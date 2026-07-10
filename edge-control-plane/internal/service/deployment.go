package service

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/provenance"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// IsValidAppName returns true if the app name is safe for use in paths.
// Rejects empty strings and strings containing path traversal characters.
func IsValidAppName(name string) bool {
	if name == "" {
		return false
	}
	return !strings.ContainsAny(name, "/\\..")
}

// IsValidDeploymentAppName enforces the public-facing app name format
// `^[a-z0-9][a-z0-9-]{0,62}$` for endpoints that accept an explicit
// `app_name` from the client (currently `POST /api/migrate-tree`).
//
// Distinct from `IsValidAppName`, which is a path-safety guard for
// internal callers. The regex is mirrored in edge-migrate-lib's
// `is_valid_deployment_app_name` and tested in lockstep — see
// `edge-migrate/edge-migrate-lib/src/patterns.rs` and
// `service/migration_test.go::TestIsValidDeploymentAppName`.
func IsValidDeploymentAppName(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	for i, r := range name {
		isLower := r >= 'a' && r <= 'z'
		isDigit := r >= '0' && r <= '9'
		isHyphen := r == '-'
		if i == 0 {
			if !isLower && !isDigit {
				return false
			}
		} else {
			if !isLower && !isDigit && !isHyphen {
				return false
			}
		}
	}
	return true
}

// regionPattern constrains a region string to the charset used by
// NATS subjects (which forbid `*`, `>`, `.`, whitespace, and path-
// separator-like characters). The 64-char cap is a defensive ceiling;
// AWS/GCP region codes are all <20 chars. See `IsValidRegion`.
var regionPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

// IsValidRegion returns true if the region string is safe to use as a
// NATS subject segment and as a deployment audit field. Rejects:
//   - empty strings,
//   - strings longer than 64 chars,
//   - characters outside `[a-z0-9-]` (uppercase, whitespace, `.`, `/`,
//     NATS wildcards, etc. — all of which would either break the
//     subject or invite injection).
//
// Modeled on `IsValidAppName`. The service layer rejects invalid
// regions before they reach the DB or the publisher.
func IsValidRegion(r string) bool {
	return regionPattern.MatchString(r)
}

// MaxArtifactSize is the maximum allowed artifact size in bytes (100 MiB).
//
// The canonical constant lives in internal/storage so the read-side cap
// (used by ArtifactStore.Open) and the upload-side cap (used by the
// migration handler) stay colocated. This alias preserves the existing
// in-package reference for upload-side callers.
const MaxArtifactSize = storage.MaxArtifactSize

// MaxRegionsPerDeployment caps the number of regions a single deployment
// can target. Defensive ceiling against fan-out abuse; realistic tenants
// want ≤10 regions. Operators can raise this constant if needed.
const MaxRegionsPerDeployment = 16

// PreviewDefaultTTL is the default expiry applied to preview
// deployments (issue #308) when the HTTP request omits ?preview-ttl=.
// 168h = 7 days, matching the LOG_RETENTION default in CLAUDE.md so
// the operator's mental model is "abandoned previews get reclaimed
// in a week." Per-deploy overridable via ?preview-ttl=24h. Operators
// can change the global default by editing this constant (no env var
// wired today — the GC interval + retention are env-tunable in
// preview_gc.go, but the default TTL on a new preview row is a code
// constant so the service can decide on a per-Deploy basis without
// a config round-trip).
const PreviewDefaultTTL = 168 * time.Hour

// PreviewOpts is the bundle of preview metadata the HTTP handler
// hands to DeploymentService.Deploy when the request includes
// ?preview-id= / ?preview-pr-number= / ?preview-ttl= (issue #308).
// Nil means "this is not a preview" — the service preserves the
// pre-#308 behavior (no preview columns stamped, no GC expiry).
//
// All three fields are populated together when present: a non-nil
// PreviewOpts with a zero-value PreviewPRNumber means "preview with
// no PR linkage" (a non-CI user running `edge deploy --preview`
// from a laptop). The handler validates this shape before
// constructing the struct.
//
// PreviewID is the hex suffix the runtime uses as the store-scope
// key (`<EDGE_KV_STORE_PATH>/{tenant_id}/preview-{id}/`). It also
// appears in the published TaskMessage so the worker can forward it
// to edge-runtime::RuntimeState. See mintPreviewID for the
// server-side default.
//
// PreviewPRNumber is the GitHub PR number the composite action
// forwards via ?preview-pr-number=. Optional; nil for non-CI users.
//
// ExpiresAt is the absolute TIMESTAMPTZ the PreviewGCService
// compares against NOW() on each sweep. The handler resolves
// `?preview-ttl=` to an absolute time before constructing this
// struct; the service layer trusts the value.
type PreviewOpts struct {
	PreviewID       string
	PreviewPRNumber *int
	ExpiresAt       time.Time
}

// mintPreviewID returns a 12-character lowercase-hex string for
// server-side preview-id generation (issue #308). Used as a fallback
// when the HTTP request didn't carry ?preview-id= (the CLI mints its
// own; the server falls back when the request is from a tool that
// doesn't). Cryptographic-random so two parallel requests can't
// collide; 12 hex chars = 48 bits = ~280 trillion values, plenty of
// headroom for any realistic PR throughput.
//
// Returns a non-empty string on success. crypto/rand.Read failure
// returns empty — the caller is expected to fall back to its own
// identifier in that case (the caller is the handler, which uses
// uuid.New() as the broader fallback).
func mintPreviewID() string {
	var b [6]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}

// ptrToTime returns &t for t, used at call sites that need a
// *time.Time column write. Issue #440: ActivationAttemptStartedAt on
// active_deployments takes a non-nil pointer so the disable path's
// wait loop sees a recent timestamp.
func ptrToTime(t time.Time) *time.Time {
	return &t
}

// Sentinel errors.
//
// The handler matches ErrInvalidRegion and ErrTooManyRegions via
// errors.Is and maps them to 400. Quota errors stay 429.
// ErrNoLastGood is returned by RollbackDeployment when the active row
// exists but last_good_deployment_id is NULL — the tenant has no
// previous deployment to roll back to. Handler maps to HTTP 409.
var (
	ErrMaxDeploymentsQuotaExceeded = fmt.Errorf("max deployments reached for tenant")
	ErrInvalidRegion               = errors.New("invalid region")
	ErrTooManyRegions              = errors.New("too many regions")
	// ErrInvalidWasm is returned by Deploy when the artifact's first
	// 4 bytes aren't the wasm magic (`\0asm`). Streaming keeps us
	// from validating the full module here, but the magic-byte
	// check is enough to reject obviously-non-wasm inputs before
	// the bytes hit disk. Handler maps to HTTP 400.
	ErrInvalidWasm = errors.New("invalid wasm artifact")
	ErrNoLastGood  = fmt.Errorf("no previous deployment to roll back to")
	// ErrDeploymentNotFound is returned by PromoteDeployment when the
	// deployment doesn't exist or belongs to a different tenant.
	ErrDeploymentNotFound = fmt.Errorf("deployment not found")
	// ErrNoActiveDeployment is returned by RollbackDeployment when there
	// is no active-deployment row for this app (user never activated any
	// deployment). Distinct from ErrAppNotFound (which is for the app
	// row in the apps table) so handlers can map to HTTP 404 without
	// false matches. Handler maps to HTTP 404.
	ErrNoActiveDeployment = fmt.Errorf("no active deployment")
	// ErrPublishFailed is returned by ActivateDeployment /
	// RollbackDeployment when the post-commit NATS publish of the
	// TaskMessage failed. The DB transaction may have already
	// committed, so workers may still be serving the prior
	// deployment; the client should treat this as a transient
	// infrastructure failure. Handler maps to HTTP 502. The wrapped
	// cause (using Go 1.20+ multi-%w) is preserved for logs.
	//
	// On issue #127 step 6, ActivateDeployment / RollbackDeployment
	// additionally wrap this sentinel inside a *PublishError so the
	// HTTP layer can surface the exact per-region breakdown via
	// errors.As. errors.Is(err, ErrPublishFailed) continues to
	// work — Unwrap() preserves the sentinel match.
	ErrPublishFailed = fmt.Errorf("publishing task update failed")
	// ErrAutoRollbackDisabled is returned by RollbackDeployment (via
	// the repo's ResetStableSinceForRollback SQL guard) when the
	// active_deployments row has auto_rollback_enabled=false. The
	// string MUST stay in sync with `errAutoRollbackDisabledSentinel`
	// in `internal/repository/active_deployment.go`; the handler
	// matches it via errors.Is. Returned only by the worker-driven
	// auto-rollback path (POST /api/internal/apps/{appName}/auto-rollback);
	// the manual `edge rollback` path bypasses this guard because a
	// tenant should always be able to manually reverse their own
	// activation, even if they opted out of auto-rollback.
	ErrAutoRollbackDisabled = errors.New("auto-rollback disabled")
	// ErrTenantDisabled is returned by ActivateDeployment /
	// RollbackDeployment / PromoteDeployment when the tenants row
	// has disabled_at IS NOT NULL at the moment the activate tx takes
	// its SELECT … FOR UPDATE on (tenants.id) — issue #440. The
	// row-level lock serializes the activate tx against
	// WorkerService.disableTenantAtomically, so a racing tenant
	// disable either blocks the activate (we then read disabled_at
	// non-nil and abort the publish) or commits ahead of us (the
	// disable path's post-commit active-deployments diff then sees
	// our row and skips publishing an empty task_update that would
	// otherwise kill the just-activated app). Handler maps to
	// HTTP 409 Conflict (RFC 9110 §15.5.10 — "request can't be
	// processed in current resource state"); match the conventional
	// ErrNoLastGood / ErrAlreadyActivated mappings at the top of
	// handler/deployment.go.
	ErrTenantDisabled = errors.New("tenant is disabled (issue #440); re-enable via POST /api/v1/admin/tenants/.../enable or wait for the quota-billing cycle to reset")
)

// PublishError carries the per-region outcome of a fan-out
// publish. Returned by ActivateDeployment / RollbackDeployment
// when at least one region's NATS publish failed; the wrapped
// Err is always ErrPublishFailed so existing
// errors.Is(err, ErrPublishFailed) checks keep working.
//
// The HTTP layer matches via errors.As and writes the
// regions_published / regions_failed arrays in the 502 body so the
// operator can see exactly which regions got the message and which
// are still pending retry. See handler/deployment.go for the
// envelope shape.
//
// Published is the deduped set of regions whose publish succeeded
// on THIS activation attempt (NOT the cumulative regions_published
// column on the active_deployments row — that column is updated by
// AppendRegionsPublished after the loop). Failed is the regions
// whose publish failed this attempt; those are merged into
// regions_failed by AppendRegionsFailed.
//
// Zero value is unusable; always construct via `&PublishError{...}`
// so Unwrap is set.
type PublishError struct {
	Published []string
	Failed    []string
	// CachedSucceeded, CachedSkipped, and CacheFailed (issue #332,
	// PR 2 follow-up) carry the per-region outcome of the optional
	// per-region artifact-cache push that runs before the NATS
	// TaskMessage publish. The two Cached* slices are disjoint:
	// CachedSucceeded is the regions where the push returned 2xx;
	// CachedSkipped is the regions where the row's RegionsCached
	// already contained the region (no push attempted); CacheFailed
	// is the regions where the push errored. Always populated (as
	// empty slices, not nil, when the cache feature is disabled) so
	// the 502 envelope handler doesn't have to nil-check.
	//
	// Pre-PR-2 callers that read the Cached field should migrate to
	// CachedSucceeded (the union of the two is the same data).
	CachedSucceeded []string
	CachedSkipped   []string
	CacheFailed     []string
	Err             error
}

// Error renders the publish error in a stable human-readable form.
// Includes the per-region breakdown so log lines are diagnostic
// without needing to inspect the struct fields. Cache fields are
// appended when non-empty so the no-cache-configured case is
// identical to the pre-#332 wire shape.
func (e *PublishError) Error() string {
	if e == nil {
		return "<nil PublishError>"
	}
	msg := fmt.Sprintf("%s (published=%v, failed=%v)",
		e.Err.Error(), e.Published, e.Failed)
	if len(e.CachedSucceeded) > 0 || len(e.CachedSkipped) > 0 || len(e.CacheFailed) > 0 {
		msg += fmt.Sprintf(" (cached_succeeded=%v, cached_skipped=%v, cache_failed=%v)",
			e.CachedSucceeded, e.CachedSkipped, e.CacheFailed)
	}
	return msg
}

// Unwrap returns the wrapped sentinel so errors.Is(err, ErrPublishFailed)
// keeps matching. This is the contract that handler/deployment.go
// (and the pre-step-6 string-error path) relies on — both the
// wrapped sentinel AND the typed error must be reachable from the
// returned value.
func (e *PublishError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Package-level interfaces for testability. The concrete
// *repository.* types satisfy these interfaces structurally.

// deploymentRepoInterface is the subset of *repository.DeploymentRepository
// methods used by DeploymentService.
type deploymentRepoInterface interface {
	WithTx(tx *sqlx.Tx) *repository.DeploymentRepository
	GetByID(ctx context.Context, id string) (*domain.Deployment, error)
	ListByApp(ctx context.Context, tenantID, appName string) ([]domain.Deployment, error)
	CountByApp(ctx context.Context, tenantID, appName string) (int, error)
	ListByAppPaginated(ctx context.Context, tenantID, appName string, limit, offset int) ([]domain.Deployment, error)
	Create(ctx context.Context, deployment *domain.Deployment) error
	DeleteByID(ctx context.Context, id string) error
}

// deployActiveRepoInterface is the subset of *repository.ActiveDeploymentRepository
// methods used by DeploymentService (distinct from worker.go's
// activeRepoInterface which targets the stability-window evaluator).
type deployActiveRepoInterface interface {
	WithTx(tx *sqlx.Tx) *repository.ActiveDeploymentRepository
	Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error)
	GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error)
	Set(ctx context.Context, ad *domain.ActiveDeployment) error
	ClearStableSince(ctx context.Context, tenantID, appName string) error
	ListByTenant(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error)
	AppendRegionsPublished(ctx context.Context, tenantID, appName string, regions []string, attemptID string, ts time.Time) error
	AppendRegionsFailed(ctx context.Context, tenantID, appName string, regions []string, attemptID string, ts time.Time) error
	AppendRegionsCacheState(ctx context.Context, tenantID, appName string, succeeded, failed []string, ts time.Time) error
}

// tenantRepoForDeploymentSvc is the subset of *repository.TenantRepository
// methods used by DeploymentService.
type tenantRepoForDeploymentSvc interface {
	WithTx(tx *sqlx.Tx) *repository.TenantRepository
	GetByID(ctx context.Context, id string) (*domain.Tenant, error)
}

// quotaRepoForDeploymentSvc is the subset of *repository.QuotaRepository
// methods used by DeploymentService.
type quotaRepoForDeploymentSvc interface {
	WithTx(tx *sqlx.Tx) *repository.QuotaRepository
	GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error)
}

// appEnvRepoForDeploymentSvc is the subset of *repository.AppEnvRepository
// methods used by DeploymentService.
type appEnvRepoForDeploymentSvc interface {
	WithTx(tx *sqlx.Tx) *repository.AppEnvRepository
	List(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error)
}

// DeploymentService handles deployment business logic.
type DeploymentService struct {
	db             *sqlx.DB
	deploymentRepo deploymentRepoInterface
	activeRepo     deployActiveRepoInterface
	appEnvRepo     appEnvRepoForDeploymentSvc
	quotaRepo      quotaRepoForDeploymentSvc
	tenantRepo     tenantRepoForDeploymentSvc
	artifactStore  storage.ArtifactStore
	publisher      nats.Publisher
	appSvc         *AppService
	envSvc         *EnvService     // injected for decryption at publish
	webhookSvc     *WebhookService // injected for webhook events
	// cachePusher pushes the activation artifact bytes to a per-region
	// edge-artifact-cache binary before the NATS TaskMessage publish
	// (issue #332). Optional; when nil, the cache-push step is skipped
	// and the existing pull-from-CP behavior is unchanged. Set via
	// SetCachePusher so existing tests/constructors that don't care
	// about caches don't need to thread an extra argument.
	cachePusher artifactCachePusher
	// regionArtifactCaches is the per-region URL map from config. When
	// a region's URL is unset (or the region is not in the map), the
	// cache-push step is skipped for that region — the worker continues
	// to pull from the CP's /api/internal/download/. Set via
	// SetRegionArtifactCaches.
	regionArtifactCaches map[string]string
	// keyring signs every new deployment's artifact (issue #307 PR1;
	// was a single `*signing.Signer` before PR1). Required — set by
	// the constructor; a nil keyring would cause `Deploy` to return
	// an error. The signature + signing_key_id are stamped onto the
	// row before the INSERT, then copied onto the
	// `AppConfig.DeploymentSignature` / `AppConfig.SigningKeyID`
	// fields of the published TaskMessage so workers can verify
	// before instantiation.
	keyring *signing.Keyring
	// defaultRegion is this control plane's own region. Used as the
	// fallback `regions` list for deployments that don't explicitly
	// target any region — both in `Deploy` (when the HTTP request
	// omits `?regions=`) and in `ActivateDeployment` (when a
	// pre-migration-008 row has an empty `regions` array). Set via
	// the constructor; never nil/empty (the config layer defaults to
	// "global" when unset).
	defaultRegion string
}

func NewDeploymentService(
	db *sqlx.DB,
	deploymentRepo *repository.DeploymentRepository,
	activeRepo *repository.ActiveDeploymentRepository,
	appEnvRepo *repository.AppEnvRepository,
	quotaRepo *repository.QuotaRepository,
	tenantRepo *repository.TenantRepository,
	artifactStore storage.ArtifactStore,
	publisher nats.Publisher,
	defaultRegion string,
	keyring *signing.Keyring,
) *DeploymentService {
	// Defensive: never let the service run with an empty default
	// region. A blank region would build a NATS subject like
	// `edgecloud.tasks.` which is malformed and the publish would
	// fail opaquely. The config layer already defaults to "global",
	// but a misconfigured test harness or a future refactor that
	// bypasses the config layer would otherwise crash here.
	if defaultRegion == "" {
		defaultRegion = "global"
	}
	return &DeploymentService{
		db:             db,
		deploymentRepo: deploymentRepo,
		activeRepo:     activeRepo,
		appEnvRepo:     appEnvRepo,
		quotaRepo:      quotaRepo,
		tenantRepo:     tenantRepo,
		artifactStore:  artifactStore,
		publisher:      publisher,
		defaultRegion:  defaultRegion,
		keyring:        keyring,
	}
}

// SetCachePusher injects the per-region artifact-cache pusher. When
// nil, the cache-push step in publishSwap is a no-op (workers
// continue to pull from the CP's /api/internal/download/ endpoint).
// Optional injection so existing tests and wiring code that don't
// care about caches don't need to thread an extra arg.
func (s *DeploymentService) SetCachePusher(p artifactCachePusher) {
	s.cachePusher = p
}

// SetRegionArtifactCaches injects the per-region URL map. When a
// region's URL is absent (or empty), the cache-push step for that
// region is skipped. Combined with SetCachePusher: the pusher must
// be set AND the region must be in the map for a push to occur.
func (s *DeploymentService) SetRegionArtifactCaches(m map[string]string) {
	s.regionArtifactCaches = m
}

// SetAppService sets the AppService dependency for auto-creating apps on deploy.
func (s *DeploymentService) SetAppService(appSvc *AppService) {
	s.appSvc = appSvc
}

// SetEnvService injects the EnvService used for decrypting env vars at publish.
func (s *DeploymentService) SetEnvService(envSvc *EnvService) {
	s.envSvc = envSvc
}

func (s *DeploymentService) SetWebhookService(webhookSvc *WebhookService) {
	s.webhookSvc = webhookSvc
}

// Deploy creates a new deployment and stores the artifact.
//
// `regions` is the list of regions the deployment is targeted at. Pass
// nil/empty to use the control plane's default region (preserves the
// pre-#82 single-region behavior). Each region is validated against
// `IsValidRegion`; the first invalid entry fails the call before any
// DB or storage I/O.
//
// `previewOpts` is the preview-metadata bundle (issue #308). Nil
// preserves the pre-#308 behavior — no preview columns stamped, no
// GC expiry. Non-nil stamps preview_id + preview_pr_number + preview_expires_at
// onto the deployments row and (via ActivateDeployment) onto the
// active row, so the worker can scope per-preview persistent stores
// and stamp `EDGE_PREVIEW_PR_NUMBER` into the guest env. See
// PreviewOpts for the per-field contract.
//
// After the deployment row is written, the activate path will publish
// one `TaskMessage` per region to `edgecloud.tasks.<region>`. (See
// `ActivateDeployment`.)
func (s *DeploymentService) Deploy(ctx context.Context, tenantID, appName string, r io.Reader, regions []string, autoRollback bool, desiredReplicas int, buildMeta *provenance.CLISideMetadata, previewOpts *PreviewOpts) (*domain.Deployment, error) {
	// Validate appName to prevent path traversal (defense-in-depth)
	if !IsValidAppName(appName) {
		return nil, fmt.Errorf("invalid app name")
	}

	// Validate every region before any side effect. An invalid
	// region string would either break the NATS subject or
	// (worse) inject a wildcard into a subject. The empty
	// `regions` slice is NOT an error — it means "use default".
	// Return on the first invalid entry so the error message
	// names the offending region (the old shape fell through the
	// loop and reported only the LAST invalid entry, a pre-existing
	// minor bug fixed in the #116 review follow-up).
	//
	// %w wraps the sentinel so handlers can match via errors.Is;
	// the message also embeds the failing region for the operator.
	for _, r := range regions {
		if !IsValidRegion(r) {
			return nil, fmt.Errorf("%w %q: must match [a-z0-9-]{1,64}", ErrInvalidRegion, r)
		}
	}
	if len(regions) > MaxRegionsPerDeployment {
		return nil, fmt.Errorf("%w: %d (max %d)", ErrTooManyRegions, len(regions), MaxRegionsPerDeployment)
	}

	// Auto-create the app record if it doesn't already exist (backward compatible).
	if s.appSvc != nil {
		if err := s.appSvc.CreateIfNotExists(ctx, tenantID, appName); err != nil {
			return nil, fmt.Errorf("creating app: %w", err)
		}
	}

	// Check quota
	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("getting quota: %w", err)
	}

	count, err := s.deploymentRepo.CountByApp(ctx, tenantID, appName)
	if err != nil {
		return nil, fmt.Errorf("counting deployments: %w", err)
	}
	if count >= quota.MaxDeployments {
		return nil, ErrMaxDeploymentsQuotaExceeded
	}

	// Note: we no longer buffer the entire artifact in memory here.
	// The streaming SaveAndHash below does hash + write in a single
	// pass; the only bytes we touch up front are the 4-byte wasm
	// magic (peeked inside the tx callback at line ~311) so we can
	// reject non-wasm blobs cheaply before any disk I/O happens.
	// The handler enforces the MaxArtifactSize cap via
	// http.MaxBytesReader, so the inner read never sees more than
	// MaxArtifactSize bytes.

	// Resolve the effective regions list: explicit > default. We
	// keep the empty `regions` slice distinct from nil so the repo
	// layer can pass an empty array to the NOT NULL column without
	// ambiguity. Pre-#82 behavior (no regions field) is preserved
	// when the caller passes nil/empty.
	effectiveRegions := regions
	if len(effectiveRegions) == 0 {
		effectiveRegions = []string{s.defaultRegion}
	}

	deployment := &domain.Deployment{
		ID:       "d_" + uuid.New().String(),
		TenantID: tenantID,
		AppName:  appName,
		Status:   domain.StatusDeployed,
		// Hash is populated after SaveAndHash returns the SHA-256
		// of the streamed bytes. Streaming keeps the bytes from
		// ever sitting in RAM as a single buffer.
		Hash:      "",
		Regions:   domain.StringArrayFrom(effectiveRegions),
		CreatedAt: time.Now(),
		// Persist the tenant opt-in on the artifact row so audit
		// endpoints (`edge deployments --app foo`) can show which
		// deployments opted in. The flag is copied onto the
		// active_deployments row by ActivateDeployment.
		AutoRollbackEnabled: autoRollback,
		// Persist the desired replica count (issue #316). 0 means
		// "no threshold" — the reconcile loop won't warn about
		// under-replication.
		DesiredReplicas: desiredReplicas,
	}
	// Preview metadata (issue #308). Stamped onto the deployment
	// row so PreviewGCService can find expired previews via
	// preview_expires_at < NOW() (partial index migration 021)
	// AND so the published TaskMessage can carry preview_id +
	// preview_pr_number to the worker. When previewOpts is nil
	// the three fields stay nil and the columns persist as SQL
	// NULL — the legacy non-preview path is unchanged.
	if previewOpts != nil {
		previewID := previewOpts.PreviewID
		if previewID == "" {
			// Fallback for handlers that pass a non-nil
			// PreviewOpts without an explicit id (e.g., a
			// future "preview mode" toggle that doesn't carry
			// one). mintPreviewID uses crypto/rand; on the
			// effectively-unreachable failure path, fall
			// back to a UUID-derived hex so the row is still
			// unique. The TTL still fires; only the store-
			// scope key becomes opaque-but-unique.
			previewID = mintPreviewID()
			if previewID == "" {
				previewID = uuid.New().String()[:12]
			}
		}
		deployment.PreviewID = &previewID
		deployment.PreviewPRNumber = previewOpts.PreviewPRNumber
		expires := previewOpts.ExpiresAt
		if expires.IsZero() {
			// Defensive default — handler should always set
			// this, but if a future caller forgets, fall
			// back to PreviewDefaultTTL so the preview is
			// still reclaimable.
			expires = time.Now().Add(PreviewDefaultTTL)
		}
		deployment.PreviewExpiresAt = &expires
	}

	// Wrap the row insert and the artifact save in a transaction
	// so a failed SaveAndHash rolls the deployment row back
	// atomically (we don't end up with a row pointing at no
	// artifact). The artifact store is filesystem, so the tx only
	// protects the row; if SaveAndHash succeeds and the tx commit
	// fails, the blob is left on disk but no row references it
	// (operator-cleanable). SaveAndHash is atomic on disk via the
	// temp-rename pattern, so a failed write never leaves partial
	// bytes at the final path.
	//
	// The 4-byte wasm magic peek runs INSIDE the tx callback so
	// a non-wasm artifact is rejected before any disk I/O. The
	// remaining stream is then handed to SaveAndHash, which hashes
	// and writes in a single pass.
	//
	// When s.db is nil (the test path, where a sqlmock or
	// in-memory harness wires repos but not a *sqlx.DB), fall
	// back to the no-tx path so the call doesn't segfault on
	// `db.BeginTxx(nil)`. Production callers always have s.db.
	if s.db != nil {
		err = repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
			magic := make([]byte, 4)
			if _, err := io.ReadFull(r, magic); err != nil {
				return fmt.Errorf("reading wasm magic: %w", err)
			}
			if !bytes.Equal(magic, []byte{0x00, 0x61, 0x73, 0x6d}) {
				return ErrInvalidWasm
			}
			// The 4 bytes we just consumed must be re-attached to
			// the stream that SaveAndHash will hash and write —
			// otherwise the stored artifact is 4 bytes short
			// (worker can't parse it, the supervisor's
			// `detect_execution_model_from_bytes` substring
			// scan finds no exports, and the SHA-256 in `deployments.hash`
			// is the digest of the truncated bytes, so a later
			// operator re-verifying the file with a real wasm
			// tool gets a confusing mismatch). MultiReader glues
			// the buffered prefix back in front of the still-to-read
			// remainder.
			full := io.MultiReader(bytes.NewReader(magic), r)
			hash, saveErr := s.artifactStore.SaveAndHash(ctx, tenantID, appName, deployment.ID, full)
			if saveErr != nil {
				return saveErr
			}
			deployment.Hash = hex.EncodeToString(hash)
			// Sign the artifact (issue #307). Signature is over
			// `sha256(artifact) || deployment.ID`; binding to the
			// id prevents DB-replay. Sign happens inside the tx so
			// a signing failure rolls the row back alongside the
			// artifact (the temp file is unlinked by SaveAndHash
			// on its own error path; the row insert is the only
			// state we own here).
			if s.keyring == nil {
				return fmt.Errorf("signing is not configured (deployment service requires a keyring at construction)")
			}
			sig, kid, signErr := s.keyring.Sign(deployment.Hash, deployment.ID)
			if signErr != nil {
				return fmt.Errorf("signing artifact: %w", signErr)
			}
			deployment.Signature = sig
			deployment.SigningKeyID = kid
			// PR2.6 — Build the SLSA L1 in-toto Statement envelope
			// inside the tx so an envelope-build failure rolls the
			// row + artifact back atomically. The envelope is
			// persisted onto the deployment row verbatim (see
			// domain.Deployment.BuildAttestation).
			if attachErr := s.attachBuildAttestation(deployment, buildMeta); attachErr != nil {
				return fmt.Errorf("attaching build attestation: %w", attachErr)
			}
			if err := s.deploymentRepo.WithTx(tx).Create(ctx, deployment); err != nil {
				return fmt.Errorf("creating deployment: %w", err)
			}
			return nil
		})
		// Apps-row cleanup: CreateIfNotExists above inserted the
		// apps row OUTSIDE the tx (nesting a tx inside another tx
		// isn't supported by the sqlx layer), so a failed tx only
		// rolls back the deployment row — not the apps row.
		// Without this cleanup, a failed first deploy of an app
		// would orphan the apps row forever, counting against the
		// tenant's max_apps quota. The NOT EXISTS guard in
		// DeleteIfNoDeployments makes this safe under concurrent
		// deploys: if a parallel deploy succeeded for the same
		// app, NOT EXISTS is FALSE and the apps row stays.
		if err != nil && s.appSvc != nil {
			if _, delErr := s.appSvc.DeleteIfNoDeployments(ctx, tenantID, appName); delErr != nil {
				log.Printf("rollback apps-row cleanup failed after tx failure: tenant_id=%s app_name=%s error=%v", tenantID, appName, delErr)
			}
		}
	} else {
		// No-tx fallback path (test harnesses that wire repos but
		// not s.db; production callers always go through the tx
		// branch above). Use unique names for inner errors so we
		// don't shadow the outer `err` via the if-init
		// `err := ...` form.
		magic := make([]byte, 4)
		if _, readErr := io.ReadFull(r, magic); readErr != nil {
			err = fmt.Errorf("reading wasm magic: %w", readErr)
		} else if !bytes.Equal(magic, []byte{0x00, 0x61, 0x73, 0x6d}) {
			err = ErrInvalidWasm
		} else if hash, saveErr := s.artifactStore.SaveAndHash(ctx, tenantID, appName, deployment.ID, io.MultiReader(bytes.NewReader(magic), r)); saveErr != nil {
			// Compensate in the same order as the tx branch's
			// equivalent: apps-row cleanup BEFORE deployment-row
			// cleanup, so the NOT EXISTS guard on
			// DeleteIfNoDeployments still sees the deployment row
			// to decide whether to drop the apps row.
			if s.appSvc != nil {
				if _, delErr := s.appSvc.DeleteIfNoDeployments(ctx, tenantID, appName); delErr != nil {
					log.Printf("rollback apps-row cleanup failed after artifact save error: tenant_id=%s app_name=%s error=%v", tenantID, appName, delErr)
				}
			}
			if delErr := s.deploymentRepo.DeleteByID(ctx, deployment.ID); delErr != nil {
				log.Printf("compensating DeleteByID failed after artifact save error (no-tx path): deployment_id=%s error=%v", deployment.ID, delErr)
			}
			err = saveErr
		} else {
			deployment.Hash = hex.EncodeToString(hash)
			// Sign the artifact (issue #307). Same logic as the
			// tx branch above; here it happens after the row is
			// about to be inserted, so a signing error returns
			// without inserting.
			if s.keyring == nil {
				err = fmt.Errorf("signing is not configured (deployment service requires a keyring at construction)")
			} else if sig, kid, signErr := s.keyring.Sign(deployment.Hash, deployment.ID); signErr != nil {
				err = fmt.Errorf("signing artifact: %w", signErr)
			} else {
				deployment.Signature = sig
				deployment.SigningKeyID = kid
				// PR2.6 — same envelope construction as the tx
				// branch. No-tx path is test-only; production goes
				// through the tx branch above.
				if attachErr := s.attachBuildAttestation(deployment, buildMeta); attachErr != nil {
					err = fmt.Errorf("attaching build attestation: %w", attachErr)
				} else if createErr := s.deploymentRepo.Create(ctx, deployment); createErr != nil {
					err = fmt.Errorf("creating deployment: %w", createErr)
				}
			}
		}
	}
	if err != nil {
		return nil, err
	}

	if s.webhookSvc != nil {
		s.webhookSvc.PublishEvent(context.Background(), deployment.TenantID, deployment.AppName, "deploy", map[string]string{
			"deployment_id": deployment.ID,
			"hash":          deployment.Hash,
		})
	}

	return deployment, nil
}

func (s *DeploymentService) GetDeployment(ctx context.Context, tenantID, id string) (*domain.Deployment, error) {
	deployment, err := s.deploymentRepo.GetByID(ctx, id)
	if err != nil || deployment == nil {
		return nil, err
	}
	if tenantID != "*" && tenantID != "" && deployment.TenantID != tenantID {
		return nil, nil // not found for this tenant
	}
	return deployment, nil
}

func (s *DeploymentService) ListDeployments(ctx context.Context, tenantID, appName string) ([]domain.Deployment, error) {
	return s.deploymentRepo.ListByApp(ctx, tenantID, appName)
}

func (s *DeploymentService) ListDeploymentsPaginated(ctx context.Context, tenantID, appName string, limit, offset int) ([]domain.Deployment, error) {
	// Negative inputs are silently corrected: limit ≤ 0 becomes 20, offset < 0 becomes 0.
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	return s.deploymentRepo.ListByAppPaginated(ctx, tenantID, appName, limit, offset)
}

func (s *DeploymentService) ListDeploymentsPaginatedWithTotal(ctx context.Context, tenantID, appName string, limit, offset int) ([]domain.Deployment, int, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	total, err := s.deploymentRepo.CountByApp(ctx, tenantID, appName)
	if err != nil {
		return nil, 0, fmt.Errorf("counting deployments: %w", err)
	}
	deployments, err := s.deploymentRepo.ListByAppPaginated(ctx, tenantID, appName, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	return deployments, total, nil
}

func (s *DeploymentService) ActivateDeployment(ctx context.Context, tenantID, appName, deploymentID string) error {
	deployment, err := s.deploymentRepo.GetByID(ctx, deploymentID)
	if err != nil || deployment == nil {
		return fmt.Errorf("deployment not found")
	}
	if deployment.TenantID != tenantID || deployment.AppName != appName {
		return fmt.Errorf("deployment not found")
	}

	return s.activateDeployment(ctx, tenantID, appName, deploymentID, deployment, deployment.AutoRollbackEnabled)
}

// PromoteDeployment activates a deployment under a different app name than
// the one it was originally deployed under. This enables the preview →
// production promotion workflow: a user deploys as `myapp--pr-42` (gets a
// unique preview URL), then promotes the same artifact to `myapp`.
func (s *DeploymentService) PromoteDeployment(ctx context.Context, tenantID, targetAppName, deploymentID string) error {
	deployment, err := s.deploymentRepo.GetByID(ctx, deploymentID)
	if err != nil || deployment == nil {
		return ErrDeploymentNotFound
	}
	if deployment.TenantID != tenantID {
		return ErrDeploymentNotFound
	}
	return s.activateDeployment(ctx, tenantID, targetAppName, deploymentID, deployment, deployment.AutoRollbackEnabled)
}

// previewIDFromDeployment unwraps the *string on the deployment
// row into a value suitable for the AppConfig wire field
// (issue #308). Returns "" when the deployment is not a preview —
// the JSON tag `omitempty` on AppConfig.PreviewID then drops it
// from the wire, so pre-#308 workers ignore the field silently.
//
// Local helper rather than a method on Deployment to keep the
// "nil pointer → empty string" conversion colocated with the
// BuildAppConfig call site (the pattern repeats three times in
// this file: activate, rollback, full-sync republish).
func previewIDFromDeployment(d *domain.Deployment) string {
	if d == nil || d.PreviewID == nil {
		return ""
	}
	return *d.PreviewID
}

// previewPRNumberFromDeployment unwraps the *int on the deployment
// row into a value suitable for AppConfig.PreviewPRNumber
// (issue #308). Returns 0 when unset — the JSON tag `omitempty`
// then drops the field from the wire.
func previewPRNumberFromDeployment(d *domain.Deployment) int {
	if d == nil || d.PreviewPRNumber == nil {
		return 0
	}
	return *d.PreviewPRNumber
}

// activateDeployment is the shared inner logic for ActivateDeployment
// and PromoteDeployment. It sets the active deployment row and publishes
// a task update, without checking the deployment's original app name.
func (s *DeploymentService) activateDeployment(ctx context.Context, tenantID, appName, deploymentID string, deployment *domain.Deployment, autoRollbackEnabled bool) error {

	// Atomically move the current active id into last_good_deployment_id
	// and write the new id. Two readers can race on a non-tx read+write;
	// use a tx with FOR UPDATE so concurrent activate/rollback serialize.
	//
	// Edge cases handled:
	//   - First-ever activate: current is nil → last_good stays NULL.
	//   - Re-activate the same id: current.deployment_id == newID →
	//     last_good becomes equal to deployment_id (visually a no-op,
	//     but the row stays consistent).
	if err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		// Issue #440: take SELECT ... FOR UPDATE on the tenants row
		// inside the same tx as the active_deployments FOR UPDATE so
		// the tenant state is observed under the lock — not after a
		// post-commit GetByID race. The lock also serializes against
		// WorkerService.disableTenantAtomically (added in commit 3):
		// if disable commits ahead, our tx observes disabled_at
		// non-nil and we abort before publishing; if our tx commits
		// first, disable's post-commit active-deployments diff sees
		// the row we just wrote and skips the empty task_update
		// that would otherwise kill the just-activated app.
		txTenant, err := s.tenantRepo.WithTx(tx).GetForUpdate(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("reading tenant for update: %w", err)
		}
		if txTenant == nil {
			return fmt.Errorf("tenant not found")
		}
		if txTenant.IsDisabled() {
			return fmt.Errorf("%w: tenant=%s", ErrTenantDisabled, tenantID)
		}
		txActive := s.activeRepo.WithTx(tx)
		current, err := txActive.GetForUpdate(ctx, tenantID, appName)
		if err != nil {
			return fmt.Errorf("reading current active deployment: %w", err)
		}
		var lastGood *string
		if current != nil {
			lastGood = &current.DeploymentID
		}
		if err := txActive.Set(ctx, &domain.ActiveDeployment{
			TenantID:             tenantID,
			AppName:              appName,
			DeploymentID:         deploymentID,
			LastGoodDeploymentID: lastGood,
			// Copy the opt-in flag from the deployments row onto
			// the active slot. The worker-driven auto-rollback
			// path and the heartbeat-driven stability window
			// both read from the active row.
			AutoRollbackEnabled: deployment.AutoRollbackEnabled,
			// Copy the desired replica count (issue #316). The
			// reconcile loop uses this as a monitoring threshold.
			DesiredReplicas: deployment.DesiredReplicas,
			// Preview linkage (issue #308). When the deployment
			// was uploaded with previewOpts, copy preview_id +
			// preview_pr_number onto the active row so the
			// published TaskMessage carries them. The runtime
			// reads these to scope per-preview persistent stores
			// and stamp EDGE_PREVIEW_PR_NUMBER into the guest
			// env. Non-preview deploys leave both nil — the
			// active row has NULL columns and the runtime falls
			// back to per-tenant scoping, preserving the
			// pre-#308 behavior.
			PreviewID:       deployment.PreviewID,
			PreviewPRNumber: deployment.PreviewPRNumber,
			// Stamp the issue #440 in-flight marker (migration
			// 026). The disable path observes this column via
			// waitForActiveRowPublishes (commit 8) and waits
			// for the matching last_publish_at stamp before
			// publishing empty — closing the canonical race
			// where activate wins the tenants FOR UPDATE lock
			// first and its post-commit publishSwap runs
			// after the disable's commit.
			ActivationAttemptStartedAt: ptrToTime(time.Now()),
		}); err != nil {
			return fmt.Errorf("setting active deployment: %w", err)
		}
		// Reset the stability clock on every activate. The new
		// deployment has not been observed running yet, so
		// stable_since must be NULL — otherwise the stability
		// window could fire immediately on the next heartbeat
		// and promote the just-activated id into last_good
		// before any worker has even loaded the artifact. We
		// explicitly ClearStableSince inside the tx (rather than
		// relying on Set to write it) because Set deliberately
		// omits stable_since from its UPDATE clause — see the
		// doc comment on ActiveDeploymentRepository.Set.
		if err := txActive.ClearStableSince(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("clearing stability clock: %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("setting active deployment: %w", err)
	}

	// Publish task update
	var envMap map[string]string
	var pubErr error
	if s.envSvc != nil {
		envMap, pubErr = s.envSvc.DecryptEnvMap(ctx, tenantID, appName)
		if pubErr != nil {
			return fmt.Errorf("preparing env vars for publish: %w", pubErr)
		}
	} else {
		var envs []domain.AppEnv
		envs, pubErr = s.appEnvRepo.List(ctx, tenantID, appName)
		if pubErr != nil {
			return fmt.Errorf("listing env vars: %w", pubErr)
		}
		envMap = make(map[string]string, len(envs))
		for _, e := range envs {
			envMap[e.EnvKey] = e.EnvValue
		}
	}

	tenant, pubErr := s.tenantRepo.GetByID(ctx, tenantID)
	if pubErr != nil {
		return fmt.Errorf("getting tenant: %w", pubErr)
	}
	if tenant == nil {
		return fmt.Errorf("tenant not found")
	}
	if tenant.IsDisabled() {
		// Issue #440 belt-and-braces: the tx-time check above catches
		// the racing case under the tenants-row FOR UPDATE lock. This
		// post-commit check covers the (theoretical) case where a
		// future non-tx activation path skips that lock and observes
		// the disabled tenant only after its own write commits. Wrap
		// with ErrTenantDisabled so the handler's `errors.Is` branch
		// maps it to 409, matching the tx-time path's status.
		return fmt.Errorf("%w: tenant=%s", ErrTenantDisabled, tenantID)
	}

	quota, pubErr := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if pubErr != nil {
		return fmt.Errorf("getting quota: %w", pubErr)
	}
	maxMemoryMB := 256
	if quota != nil && quota.MaxMemoryMB > 0 {
		maxMemoryMB = quota.MaxMemoryMB
	}

	msg := &nats.TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now().UTC(),
		TenantID:  tenantID,
		Apps: map[string]nats.AppConfig{
			appName: nats.BuildAppConfig(
				deploymentID,
				deployment.Hash,
				deployment.Signature,
				deployment.SigningKeyID, // issue #307 PR1: per-key kid
				previewIDFromDeployment(deployment),
				previewPRNumberFromDeployment(deployment),
				envMap,
				tenant.AllowlistedDestinations,
				maxMemoryMB,
			),
		},
	}

	// Resolve the effective regions list to publish to via the
	// shared helper. publishSwap is also used by RollbackDeployment,
	// which previously published to "global" only — a silent
	// multi-region regression. Keeping both paths through one helper
	// guarantees they fan out identically.
	//
	// deployment.Regions is pq.StringArray on this branch; convert to
	// []string so publishSwap can iterate it directly.
	regions := domain.StringArrayTo(deployment.Regions)
	if len(regions) == 0 {
		regions = []string{s.defaultRegion}
	}
	if err := s.publishSwap(ctx, tenantID, appName, deploymentID, msg, regions); err != nil {
		return err
	}
	if s.webhookSvc != nil {
		s.webhookSvc.PublishEvent(context.Background(), tenantID, appName, "activate", map[string]string{
			"deployment_id": deploymentID,
		})
	}
	return nil
}

// publishSwap fans a TaskMessage out to every region in `regions`,
// skipping regions that have already been published for this
// (tenant, app) activation and always retrying regions in
// regions_failed. Used by ActivateDeployment and RollbackDeployment
// so they cannot drift in their region-fanout behavior (the prior
// Rollback path published to "global" only, leaving multi-region
// deployments stuck on the broken version until the next heartbeat).
//
// Idempotency (issue #127 step 6):
//   - Reads the committed active_deployments row to discover
//     regions_published (skip — already on the wire) and
//     regions_failed (always retry — never let a stale
//     regions_published mask a real failure; see Risk 3 in the
//     issue #127 plan).
//   - The publish set is (regions ∪ regions_failed) − regions_published.
//   - If empty (every region already published), returns nil
//     without touching the publisher or the DB.
//
// Failures in a single region are logged and accumulated into the
// returned *PublishError — we keep publishing to the remaining
// regions rather than aborting on the first failure, so a
// transient NATS blip in one region doesn't starve the others.
//
// On success, persists the per-region outcome via
// activeRepo.AppendRegionsPublished / AppendRegionsFailed so the
// next retry call can short-circuit. The append is best-effort:
// a DB write failure is logged but does not change the returned
// error — the publish itself already succeeded and the operator
// would rather see the per-region 502 envelope than a misleading
// 500.
//
// The DB tx has already committed by the time we get here, so
// workers may still be serving the prior deployment; the caller
// surfaces this as a transient 502 to the client.
//
// Returns nil on full success, or *PublishError{Published, Failed}
// wrapping ErrPublishFailed on any per-region failure.
// errors.Is(err, ErrPublishFailed) matches via the *PublishError's
// Unwrap, preserving the contract the pre-step-6 handler relied on.
func (s *DeploymentService) publishSwap(ctx context.Context, tenantID, appName, deploymentID string, msg *nats.TaskMessage, regions []string) error {
	// Read the committed row's publish state. The Set upsert at the
	// start of Activate / Rollback wipes regions_published and
	// regions_failed to empty (see ActiveDeploymentRepository.Set's
	// DO UPDATE clause), so on a first attempt the toPublish set
	// equals `regions`; on a retry of a partially-failed
	// activation, prior successes are skipped and prior failures
	// are included.
	current, err := s.activeRepo.Get(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("reading publish state: %w", err)
	}
	alreadyPublished := make(map[string]struct{}, len(current.RegionsPublished))
	for _, r := range current.RegionsPublished {
		alreadyPublished[r] = struct{}{}
	}
	mustRetry := make(map[string]struct{}, len(current.RegionsFailed))
	for _, r := range current.RegionsFailed {
		mustRetry[r] = struct{}{}
	}
	// alreadyCached (issue #332, Layer 3): regions whose
	// edge-artifact-cache binary already holds the artifact bytes
	// from a prior activation. The cache-push loop below skips
	// these regions (no PUT); the NATS publish loop still runs
	// for them, since the worker may not have received the prior
	// TaskMessage (NATS workqueue dedupes by message id, but the
	// two messages are different so the worker will get a refresh).
	// The skipped regions are recorded in `cached` for the 502
	// envelope; on a future re-activation with the same row, both
	// `alreadyPublished` AND `alreadyCached` keep re-publishing /
	// skipping respectively — until a new Set wipes them.
	alreadyCached := make(map[string]struct{}, len(current.RegionsCached))
	for _, r := range current.RegionsCached {
		alreadyCached[r] = struct{}{}
	}

	// toPublish = (regions ∪ regions_failed) − regions_published
	// Preserves input order for log determinism.
	seen := make(map[string]struct{}, len(regions))
	toPublish := make([]string, 0, len(regions)+len(mustRetry))
	for _, r := range regions {
		if _, ok := alreadyPublished[r]; ok {
			continue
		}
		if _, dup := seen[r]; dup {
			continue
		}
		seen[r] = struct{}{}
		toPublish = append(toPublish, r)
	}
	for r := range mustRetry {
		if _, ok := alreadyPublished[r]; ok {
			continue
		}
		if _, dup := seen[r]; dup {
			continue
		}
		seen[r] = struct{}{}
		toPublish = append(toPublish, r)
	}
	if len(toPublish) == 0 {
		// All requested regions already published. Short-circuit;
		// do not bump last_publish_at since nothing happened.
		return nil
	}

	attemptID := uuid.NewString()
	now := time.Now()

	// Per-region artifact-cache push (issue #332, Layer 3). Runs
	// BEFORE the NATS publish so the worker has the artifact in
	// the local cache by the time it receives the TaskMessage.
	//
	// Cache push failures are best-effort: a push failure does NOT
	// add the region to `failed` (the NATS publish still happens
	// and the worker falls back to pulling the artifact from the
	// control plane's /api/internal/download/). The failure is
	// recorded separately in `cacheFailed` and persisted via
	// AppendRegionsCacheState so an operator can see which regions
	// are currently failing in the row's regions_cache_failed
	// column — but the activation return value is NOT shaped by
	// cache failures. The 502 envelope is reserved for NATS publish
	// failures (worker was never notified).
	//
	// Skip conditions (region in `toPublish` is NOT pushed when):
	//   - s.cachePusher is nil (cache feature disabled at runtime)
	//   - s.regionArtifactCaches[region] is unset or empty
	//   - region is in `alreadyCached`: a Set with the same
	//     deployment_id preserves RegionsCached via the CASE WHEN
	//     in the DO UPDATE clause (PR 2 follow-up), so this branch
	//     fires on a re-activation of the same deployment where the
	//     cache is still warm from a prior activation. The cache
	//     bytes are identical, so a redundant push is wasted work.
	//
	// The third condition means cache push is best-effort +
	// idempotent: a successful push on activation A leaves
	// RegionsCached populated; activation B (same id) sees the
	// region in alreadyCached and skips — no network traffic for
	// regions whose cache already has the bytes. If the cache
	// between activations is wiped, the next activation re-pushes
	// (because the cache pusher's Push call returns an error and
	// the region ends up in cacheFailed, NOT cachedSucceeded, so
	// the row's RegionsCached is NOT updated for that region).
	var cachedSucceeded []string
	var cachedSkipped []string
	var cacheFailed []string
	if s.cachePusher != nil && len(s.regionArtifactCaches) > 0 {
		for _, region := range toPublish {
			if _, ok := alreadyCached[region]; ok {
				// Already cached from a prior activation. Record as
				// skipped so the row reflects "no push was needed";
				// don't attempt a redundant push. Persisted via
				// AppendRegionsCacheState alongside the success
				// slice below.
				cachedSkipped = append(cachedSkipped, region)
				continue
			}
			cacheURL, ok := s.regionArtifactCaches[region]
			if !ok || cacheURL == "" {
				// No cache configured for this region; the worker
				// will pull from the CP as today.
				continue
			}
			if err := s.cachePusher.Push(ctx, cacheURL, tenantID, appName, deploymentID); err != nil {
				log.Printf("artifact cache push failed for region %q (deployment %s): %v", region, deploymentID, err)
				cacheFailed = append(cacheFailed, region)
				continue
			}
			cachedSucceeded = append(cachedSucceeded, region)
		}
	}

	var published []string
	var failed []string
	for _, region := range toPublish {
		if err := s.publisher.PublishTaskUpdate(region, msg); err != nil {
			log.Printf("publishing task update failed for region %q (deployment %s): %v", region, deploymentID, err)
			failed = append(failed, region)
			continue
		}
		published = append(published, region)
	}

	// Best-effort persistence. Failures here are logged but do
	// not change the returned error — the publish itself already
	// happened, and the operator would rather see the structured
	// 502 envelope than a misleading 500 caused by an audit-log
	// write failing.
	//
	// All four appends (regions_published, regions_failed,
	// regions_cached, regions_cache_failed) share one tx so the
	// row's per-region state stays consistent even if the process
	// crashes mid-write. Within the closure, returning the first
	// error aborts the tx and Rollback discards every append — the
	// desired atomicity. PR 2 (issue #332) added `regions_cached`
	// to this same tx; PR 2 follow-up replaced it with the
	// `AppendRegionsCacheState` helper that updates both
	// regions_cached (succeeded+skipped) and regions_cache_failed
	// in a single statement.
	if len(published) > 0 || len(failed) > 0 || len(cachedSucceeded) > 0 || len(cachedSkipped) > 0 || len(cacheFailed) > 0 {
		if err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
			txRepo := s.activeRepo.WithTx(tx)
			if len(published) > 0 {
				if err := txRepo.AppendRegionsPublished(ctx, tenantID, appName, published, attemptID, now); err != nil {
					return fmt.Errorf("append regions_published: %w", err)
				}
			}
			if len(failed) > 0 {
				if err := txRepo.AppendRegionsFailed(ctx, tenantID, appName, failed, attemptID, now); err != nil {
					return fmt.Errorf("append regions_failed: %w", err)
				}
			}
			// AppendRegionsCacheState is invoked whenever the
			// cache loop ran. `succeeded` is the deduped union of
			// cachedSucceeded (real push returned 2xx) and
			// cachedSkipped (already cached from a prior
			// activation, no push attempted) — both go into
			// regions_cached so the next activation's alreadyCached
			// check fires. `failed` goes into regions_cache_failed.
			// In the no-op case (cache disabled or no region has a
			// cache URL) we skip the tx entirely via the outer
			// guard.
			if len(cachedSucceeded) > 0 || len(cachedSkipped) > 0 || len(cacheFailed) > 0 {
				mergedSucceeded := append([]string{}, cachedSucceeded...)
				mergedSucceeded = append(mergedSucceeded, cachedSkipped...)
				if err := txRepo.AppendRegionsCacheState(ctx, tenantID, appName, mergedSucceeded, cacheFailed, now); err != nil {
					return fmt.Errorf("append regions_cache_state: %w", err)
				}
			}
			return nil
		}); err != nil {
			log.Printf("warning: persisting publish state for %s/%s attempt %s: %v", tenantID, appName, attemptID, err)
		}
	}

	// Only NATS publish failures trigger the 502 envelope. Cache
	// push failures are best-effort — see the comment above the
	// cache loop — and are persisted in the row's
	// regions_cache_failed column for operator visibility. The
	// worker still receives the TaskMessage for those regions
	// (the NATS publish succeeded) and will fall back to the
	// control plane's download endpoint.
	if len(failed) > 0 {
		return &PublishError{
			Published:       published,
			Failed:          failed,
			CachedSucceeded: cachedSucceeded,
			CachedSkipped:   cachedSkipped,
			CacheFailed:     cacheFailed,
			Err:             ErrPublishFailed,
		}
	}

	// Block until active workers confirm they have started the deployment (issue #331, Layer 3).
	if err := s.waitForWorkers(ctx, tenantID, appName, deploymentID, regions); err != nil {
		log.Printf("waitForWorkers failed/timeout: %v", err)
		return &PublishError{
			Published:       published,
			Failed:          regions,
			CachedSucceeded: cachedSucceeded,
			CachedSkipped:   cachedSkipped,
			CacheFailed:     cacheFailed,
			Err:             ErrPublishFailed,
		}
	}

	return nil
}

func (s *DeploymentService) waitForWorkers(ctx context.Context, tenantID, appName, deploymentID string, regions []string) error {
	workerRepo := repository.NewWorkerRepository(s.db)

	workers, err := workerRepo.List(ctx)
	if err != nil {
		return fmt.Errorf("listing workers: %w", err)
	}

	targetRegions := make(map[string]struct{}, len(regions))
	for _, r := range regions {
		targetRegions[r] = struct{}{}
	}

	var targetWorkers []string
	now := time.Now()
	for _, w := range workers {
		if _, exists := targetRegions[w.Region]; exists {
			// A worker is active if it sent a heartbeat within the last 90 seconds.
			if now.Sub(w.LastSeen) <= 90*time.Second {
				targetWorkers = append(targetWorkers, w.ID)
			}
		}
	}

	if len(targetWorkers) == 0 {
		// No active workers in the target regions. Nothing to wait for.
		return nil
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		statuses, err := workerRepo.GetLatestStatuses(ctx, targetWorkers)
		if err != nil {
			return fmt.Errorf("getting worker statuses: %w", err)
		}

		allConfirmed := true
		for _, wID := range targetWorkers {
			ws, ok := statuses[wID]
			if !ok {
				allConfirmed = false
				break
			}

			var apps map[string]domain.AppStatus
			if err := json.Unmarshal(ws.Apps, &apps); err != nil {
				allConfirmed = false
				break
			}

			confirmed := false
			for rawKey, app := range apps {
				currAppName := rawKey
				if i := strings.IndexByte(rawKey, ':'); i >= 0 {
					currAppName = rawKey[:i]
				}

				if currAppName == appName && app.DeploymentID == deploymentID && app.Status == "running" {
					confirmed = true
					break
				}
			}

			if !confirmed {
				allConfirmed = false
				break
			}
		}

		if allConfirmed {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	return fmt.Errorf("timeout waiting for workers in regions %v to confirm deployment %s", regions, deploymentID)
}

// RollbackDeployment atomically swaps the active deployment back to the
// stored last_good_deployment_id and republishes a TaskMessage so workers
// reconcile. Returns ErrNoLastGood if the row has no last-good pointer
// (tenant has only ever activated one deployment).
//
// On success, returns the deployment_id that is now active (i.e., the
// prior last_good value). The prior current deployment_id is overwritten
// — there is no multi-step history in this minimum viable version.
func (s *DeploymentService) RollbackDeployment(ctx context.Context, tenantID, appName string) (string, error) {
	var rolledBackID string
	var deploymentHash string
	var deploymentSignature string
	var deploymentSigningKeyID string
	// Preview linkage (issue #308) — preserved across rollback so
	// the published TaskMessage keeps per-preview store scoping +
	// EDGE_PREVIEW_PR_NUMBER stamping. Sourced from the rolled-
	// back-to deployment row inside the tx (see the assignment
	// below). Defaults to zero values so a non-preview rollback
	// doesn't need to special-case.
	var rollbackPreviewID string
	var rollbackPreviewPRNumber int
	var regions []string
	var tenant *domain.Tenant
	var envs []domain.AppEnv
	var maxMemoryMB int

	if err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		// Issue #440: take the tenants row FOR UPDATE inside the
		// same tx as the active_deployments lock so the rollback
		// observes disabled_at under the lock, not after a
		// post-commit GetByID race. Without this guard, the
		// disable path could commit + publish empty AFTER the
		// rollback's tx commits but BEFORE the rollback's
		// publishSwap, killing the just-rolled-back app. The
		// handler's existing `errors.Is(err, ErrTenantDisabled)`
		// mapping at handler/deployment.go:798 was previously
		// dead code for the rollback path — this guard makes
		// it reachable.
		txTenant, err := s.tenantRepo.WithTx(tx).GetForUpdate(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("reading tenant for rollback: %w", err)
		}
		if txTenant == nil {
			return fmt.Errorf("tenant not found")
		}
		if txTenant.IsDisabled() {
			return fmt.Errorf("%w: tenant=%s", ErrTenantDisabled, tenantID)
		}

		txActive := s.activeRepo.WithTx(tx)
		current, err := txActive.GetForUpdate(ctx, tenantID, appName)
		if err != nil {
			return fmt.Errorf("reading current active deployment: %w", err)
		}
		if current == nil {
			return ErrNoActiveDeployment
		}
		if current.LastGoodDeploymentID == nil {
			return ErrNoLastGood
		}

		rolledBackID = *current.LastGoodDeploymentID

		// Confirm the target still exists. Defends against the (unlikely)
		// case where the last_good row was deleted out from under us.
		dep, err := s.deploymentRepo.WithTx(tx).GetByID(ctx, rolledBackID)
		if err != nil || dep == nil {
			return fmt.Errorf("previous deployment %s not found", rolledBackID)
		}
		if dep.TenantID != tenantID || dep.AppName != appName {
			return fmt.Errorf("previous deployment %s not found", rolledBackID)
		}
		deploymentHash = dep.Hash
		deploymentSignature = dep.Signature
		deploymentSigningKeyID = dep.SigningKeyID
		// Preview linkage (issue #308) — preserved across rollback.
		// The rolled-back-TO deployment may itself be a preview
		// (e.g., promote-then-rollback); re-publish its preview
		// fields so the worker continues to scope per-preview
		// stores and stamp EDGE_PREVIEW_PR_NUMBER. Sourced from
		// the deployment row (not the active row) because the
		// active row's preview_* was just cleared via the Set
		// call below — the deployment row is the authoritative
		// source for the artifact's metadata.
		rollbackPreviewID = previewIDFromDeployment(dep)
		rollbackPreviewPRNumber = previewPRNumberFromDeployment(dep)
		// Use the rolled-BACK-TO deployment's regions so we publish
		// to exactly the regions where this artifact was originally
		// destined. Previously this published to "global" only, which
		// silently left multi-region tenants running the broken
		// version on their non-default regions (workers there had no
		// signal to swap).
		regions = domain.StringArrayTo(dep.Regions)
		if len(regions) == 0 {
			regions = []string{s.defaultRegion}
		}

		// Clear last_good so a second rollback is a no-op (returns 409
		// rather than rolling back to whatever was active two steps ago —
		// that requires an N-step history table, out of scope for the
		// minimum viable UX). Also reset the stability clock so the
		// freshly-active deployment has to be observed running again
		// before it becomes eligible to be promoted into last_good
		// (otherwise the next heartbeat could see a stale stable_since
		// from before the rollback and immediately promote the
		// now-active deployment).
		if err := txActive.Set(ctx, &domain.ActiveDeployment{
			TenantID:             tenantID,
			AppName:              appName,
			DeploymentID:         rolledBackID,
			LastGoodDeploymentID: nil,
			// Preserve the auto-rollback flag across the rollback —
			// it's a tenant preference, not a property of any single
			// deployment, so it should survive a swap.
			AutoRollbackEnabled: current.AutoRollbackEnabled,
			// Issue #440 in-flight marker (migration 026). Same
			// rationale as ActivateDeployment: a rollback is an
			// activation event for this row, so the disable path
			// must see the row's marker stamp and wait for
			// publishSwap before publishing empty.
			ActivationAttemptStartedAt: ptrToTime(time.Now()),
		}); err != nil {
			return fmt.Errorf("swapping active deployment: %w", err)
		}
		if err := txActive.ClearStableSince(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("clearing stability clock: %w", err)
		}

		// Snapshot the publish inputs inside the tx so the message
		// reflects post-rollback state even if another activate lands
		// between commit and publish (which would itself race with this
		// publish; see plan §"Risk register"). All four reads below use
		// WithTx(tx) so they participate in the same atomic transaction
		// as the active_deployments Set above — without the wrapper the
		// reads would happen on the main connection pool and could
		// observe a different snapshot than the one we're about to
		// commit.
		envsList, err := s.appEnvRepo.WithTx(tx).List(ctx, tenantID, appName)
		if err != nil {
			return fmt.Errorf("listing env vars: %w", err)
		}
		envs = envsList
		tenant, err = s.tenantRepo.WithTx(tx).GetByID(ctx, tenantID)
		if err != nil || tenant == nil {
			return fmt.Errorf("getting tenant: %w", err)
		}
		quota, err := s.quotaRepo.WithTx(tx).GetByTenantID(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("getting quota: %w", err)
		}
		maxMemoryMB = 256
		if quota != nil && quota.MaxMemoryMB > 0 {
			maxMemoryMB = quota.MaxMemoryMB
		}
		return nil
	}); err != nil {
		return "", err
	}

	envMap := make(map[string]string, len(envs))
	if s.envSvc != nil {
		for _, e := range envs {
			v, err := s.envSvc.Decrypt(e.EnvValue)
			if err != nil {
				return "", fmt.Errorf("rollback: decrypting env %s: %w", e.EnvKey, err)
			}
			envMap[e.EnvKey] = v
		}
	} else {
		for _, e := range envs {
			envMap[e.EnvKey] = e.EnvValue
		}
	}

	msg := &nats.TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now().UTC(),
		TenantID:  tenantID,
		Apps: map[string]nats.AppConfig{
			appName: nats.BuildAppConfig(
				rolledBackID,
				deploymentHash,
				deploymentSignature,
				deploymentSigningKeyID, // issue #307 PR1: per-key kid
				rollbackPreviewID,      // issue #308: preserved across rollback
				rollbackPreviewPRNumber,
				envMap,
				tenant.AllowlistedDestinations,
				maxMemoryMB,
			),
		},
	}
	if err := s.publishSwap(ctx, tenantID, appName, rolledBackID, msg, regions); err != nil {
		return "", err
	}

	if s.webhookSvc != nil {
		s.webhookSvc.PublishEvent(context.Background(), tenantID, appName, "rollback", map[string]string{
			"deployment_id": rolledBackID,
		})
	}

	return rolledBackID, nil
}

// RepublishActiveDeployments re-sends a TaskMessage for every currently-active
// deployment belonging to tenantID. Called after an egress allowlist change so
// workers pick up the new policy without a manual re-activate.
func (s *DeploymentService) RepublishActiveDeployments(ctx context.Context, tenantID string) error {
	activeList, err := s.activeRepo.ListByTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("listing active deployments: %w", err)
	}
	if len(activeList) == 0 {
		return nil
	}

	tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("getting tenant: %w", err)
	}
	if tenant == nil {
		return fmt.Errorf("tenant not found")
	}

	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("getting quota: %w", err)
	}
	maxMemoryMB := 256
	if quota != nil && quota.MaxMemoryMB > 0 {
		maxMemoryMB = quota.MaxMemoryMB
	}

	var failedApps []string
	for _, ad := range activeList {
		deployment, err := s.deploymentRepo.GetByID(ctx, ad.DeploymentID)
		if err != nil || deployment == nil {
			log.Printf("republish: skipping app %q — deployment %s not found", ad.AppName, ad.DeploymentID)
			failedApps = append(failedApps, ad.AppName)
			continue
		}

		envs, err := s.appEnvRepo.List(ctx, tenantID, ad.AppName)
		if err != nil {
			failedApps = append(failedApps, ad.AppName)
			continue
		}
		envMap := make(map[string]string, len(envs))
		for _, e := range envs {
			v := e.EnvValue
			if s.envSvc != nil {
				var decErr error
				v, decErr = s.envSvc.Decrypt(e.EnvValue)
				if decErr != nil {
					log.Printf("republish: decrypting env %s/%s: %v", ad.AppName, e.EnvKey, decErr)
					failedApps = append(failedApps, ad.AppName)
					break
				}
			}
			envMap[e.EnvKey] = v
		}

		msg := &nats.TaskMessage{
			Type:      "task_update",
			Timestamp: time.Now().UTC(),
			TenantID:  tenantID,
			Apps: map[string]nats.AppConfig{
				ad.AppName: nats.BuildAppConfig(
					ad.DeploymentID,
					deployment.Hash,
					deployment.Signature,                      // issue #307
					deployment.SigningKeyID,                   // issue #307 PR1: per-key kid
					previewIDFromDeployment(deployment),       // issue #308
					previewPRNumberFromDeployment(deployment), // issue #308
					envMap,
					tenant.AllowlistedDestinations,
					maxMemoryMB,
				),
			},
		}

		regions := deployment.Regions
		if len(regions) == 0 {
			regions = []string{s.defaultRegion}
		}
		for _, region := range regions {
			if err := s.publisher.PublishTaskUpdate(region, msg); err != nil {
				log.Printf("republish: publishing task update failed for app %q region %q: %v", ad.AppName, region, err)
				failedApps = append(failedApps, ad.AppName)
			}
		}
	}

	if len(failedApps) > 0 {
		return fmt.Errorf("republish failed for apps: %s", strings.Join(failedApps, ", "))
	}
	return nil
}

func (s *DeploymentService) GetActiveDeployment(ctx context.Context, tenantID, appName string) (*domain.Deployment, error) {
	ad, err := s.activeRepo.Get(ctx, tenantID, appName)
	if err != nil || ad == nil {
		return nil, err
	}
	return s.deploymentRepo.GetByID(ctx, ad.DeploymentID)
}

func (s *DeploymentService) GetArtifact(ctx context.Context, tenantID, appName, deploymentID string, format string) (io.ReadCloser, error) {
	// Verify deployment belongs to this tenant
	deployment, err := s.deploymentRepo.GetByID(ctx, deploymentID)
	if err != nil || deployment == nil {
		return nil, fmt.Errorf("deployment not found")
	}
	if deployment.TenantID != tenantID || deployment.AppName != appName {
		return nil, fmt.Errorf("deployment not found")
	}
	return s.artifactStore.OpenFormat(ctx, tenantID, appName, deploymentID, format)
}

// attachBuildAttestation constructs and signs an SLSA L1 in-toto
// Statement v0.1 envelope for the freshly-saved artifact and
// stores it on the deployment row as a JSONB byte slice.
//
// Called from inside the Deploy transaction callback so a
// signing error rolls the row + artifact back atomically; the
// pre-existing PR2.5 handler contract (build_metadata part of the
// multipart envelope is optional) means buildMeta may be nil —
// in that case we still build an envelope with "unknown"
// toolchain fields so downstream audit pipelines always get a
// well-formed attestation, just with a partial provenance
// picture. Future EDGE_PROVENANCE_REQUIRED=true will tighten
// this contract; for now nil is best-effort.
//
// Returns the canonical JSON bytes of the DSSE wrapper — the
// service stores them verbatim on the deployments.build_attestation
// JSONB column. The struct-marshal round-trip is avoided so the
// bytes that go to disk are bit-for-bit identical to the bytes
// the verifier will recompute.
func (s *DeploymentService) attachBuildAttestation(deployment *domain.Deployment, buildMeta *provenance.CLISideMetadata) error {
	if s.keyring == nil {
		return fmt.Errorf("signing is not configured (deployment service requires a keyring at construction)")
	}

	// Populate toolchain entries from buildMeta. Optional fields
	// stay empty — that matches the "unknown" toolchain story in
	// the function docstring above.
	var tools []provenance.ToolEntry
	if buildMeta != nil {
		if buildMeta.ToolchainRustc != "" {
			tools = append(tools, provenance.ToolEntry{Name: "rustc", Version: buildMeta.ToolchainRustc})
		}
		if buildMeta.ToolchainCargo != "" {
			tools = append(tools, provenance.ToolEntry{Name: "cargo", Version: buildMeta.ToolchainCargo})
		}
		if buildMeta.ToolchainClang != "" {
			tools = append(tools, provenance.ToolEntry{Name: "clang", Version: buildMeta.ToolchainClang})
		}
	}

	// Subject path is the on-disk artifact path so audit
	// consumers can correlate the envelope back to the bytes.
	artifactPath := fmt.Sprintf("/registry/%s/%s/%s.wasm",
		deployment.TenantID, deployment.AppName, deployment.ID)

	now := time.Now()
	stmt, stmtErr := provenance.NewStatement(provenance.BuildOptions{
		ArtifactSHA256:  deployment.Hash,
		ArtifactPath:    artifactPath,
		BuildStartedOn:  now,
		BuildFinishedOn: now,
		Tools:           tools,
	})
	if stmtErr != nil {
		return fmt.Errorf("build statement: %w", stmtErr)
	}

	envelope, _, signErr := provenance.SignStatement(stmt, s.keyring)
	if signErr != nil {
		return fmt.Errorf("sign statement: %w", signErr)
	}

	envelopeBytes, marshalErr := json.Marshal(envelope)
	if marshalErr != nil {
		return fmt.Errorf("marshal envelope: %w", marshalErr)
	}
	deployment.BuildAttestation = json.RawMessage(envelopeBytes)
	return nil
}
