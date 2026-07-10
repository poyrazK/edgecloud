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
	"github.com/lib/pq"
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
	// ErrPaymentRequired is returned by Deploy when the deploy would
	// violate a billing boundary (issue #420). The handler maps this
	// to HTTP 402 PAYMENT_REQUIRED. Reasons are emitted as
	// `subscription_past_due`, `subscription_canceled`, `free_tier_exceeded`,
	// `quota_will_be_exceeded` — all surface in the JSON error envelope
	// so the client can route the user to the right upgrade path.
	// Distinct from ErrMaxDeploymentsQuotaExceeded (429) which still
	// signals a backoff-and-retry cap on static deployment counts.
	ErrPaymentRequired = errors.New("payment required")
	// ErrInvalidWasm is returned by Deploy when the artifact's first
	// ErrInvalidWasm is returned by Deploy when the artifact's first
	// 4 bytes aren't the wasm magic (`\0asm`). Streaming keeps us
	// from validating the full module here, but the magic-byte
	// check is enough to reject obviously-non-wasm inputs before
	// the bytes hit disk. Handler maps to HTTP 400.
	ErrInvalidWasm = errors.New("invalid wasm artifact")
	ErrNoLastGood  = fmt.Errorf("no previous deployment to roll back to")

	// ErrIdempotencyKeyMismatch (issue #52) — the caller reused
	// an Idempotency-Key against a request body whose SHA-256
	// differs from the one stored alongside the original row.
	// The handler maps this to 422: the conflict is a client
	// bug (a key by definition is supposed to identify a
	// unique request), not a retry on the same request.
	ErrIdempotencyKeyMismatch = errors.New("idempotency key reused with a different request body")
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

// PaymentRequiredError wraps ErrPaymentRequired with a structured
// reason code that the handler echoes in the JSON error envelope.
// The reason is a stable, machine-readable string the client can
// route on (e.g. "subscription_past_due" → show "update billing"
// CTA, "quota_will_be_exceeded" → show "upgrade plan" CTA). The
// sentinel stays in the error chain via Unwrap() so handlers can
// still use errors.Is(err, ErrPaymentRequired) for the 402 mapping.
type PaymentRequiredError struct {
	Reason string
}

func (e *PaymentRequiredError) Error() string {
	return fmt.Sprintf("%s: %s", ErrPaymentRequired.Error(), e.Reason)
}
func (e *PaymentRequiredError) Unwrap() error { return ErrPaymentRequired }

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
	VerifyUnderCap(ctx context.Context, tenantID string, projectedRequests, projectedOutboundBytes int64) (bool, error)
}

// billingRepoForDeploymentSvc is the subset of *repository.BillingRepository
// methods used by DeploymentService. Added in issue #420 — the deploy-time
// gate reads billing_subscriptions.status to enforce past_due / canceled
// boundaries without widening the seam to the full billing surface.
type billingRepoForDeploymentSvc interface {
	GetSubscriptionStatus(ctx context.Context, tenantID string) (domain.SubscriptionStatus, error)
}

// appEnvRepoForDeploymentSvc is the subset of *repository.AppEnvRepository
// methods used by DeploymentService.
type appEnvRepoForDeploymentSvc interface {
	WithTx(tx *sqlx.Tx) *repository.AppEnvRepository
	List(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error)
}

// idempotencyRepoForDeploymentSvc is the subset of
// *repository.IdempotencyKeyRepo used by DeploymentService for the
// Idempotency-Key replay cache (issue #52). Identical surface area
// to the concrete repo; defined here so service-level tests can
// substitute a stub without spinning up sqlx.
type idempotencyRepoForDeploymentSvc interface {
	Lookup(ctx context.Context, tenantID, key string) (*domain.IdempotencyKey, error)
	Insert(ctx context.Context, k *domain.IdempotencyKey) error
}

// outboxRepoForDeploymentSvc is the subset of *repository.OutboxRepository
// methods used by DeploymentService. The interface exists so tests can
// inject a sqlmock-backed fake without depending on the tx machinery.
type outboxRepoForDeploymentSvc interface {
	WithTx(tx *sqlx.Tx) *repository.OutboxRepository
}

// DeploymentService handles deployment business logic.
type DeploymentService struct {
	db             *sqlx.DB
	deploymentRepo deploymentRepoInterface
	activeRepo     deployActiveRepoInterface
	appEnvRepo     appEnvRepoForDeploymentSvc
	quotaRepo      quotaRepoForDeploymentSvc
	billingRepo    billingRepoForDeploymentSvc
	tenantRepo     tenantRepoForDeploymentSvc
	// idempotencyRepo is the (tenant_id, key) -> deployment_id
	// replay cache (issue #52). Optional — when nil, the replay
	// check is skipped and every Deploy mints a fresh row. Set
	// via SetIdempotencyRepo after construction so existing
	// test harnesses that don't care about the cache don't need
	// to thread an extra arg.
	idempotencyRepo idempotencyRepoForDeploymentSvc
	outboxRepo      outboxRepoForDeploymentSvc
	artifactStore   storage.ArtifactStore
	publisher       nats.Publisher
	appSvc          *AppService
	envSvc          *EnvService     // injected for decryption at publish
	webhookSvc      *WebhookService // injected for webhook events
	// cachePusher pushes the activation artifact bytes to a per-region
	// edge-artifact-cache binary before the NATS TaskMessage publish
	// (issue #332). Optional; when nil, the cache-push step is skipped
	// and the existing pull-from-CP behavior is unchanged. Set via
	// SetCachePusher so existing tests/constructors that don't care
	// about caches don't need to thread an extra argument.
	cachePusher artifactCachePusher
	// regionArtifactCaches is the per-region URL map from config. When
	// a region's URL is unset (or the region is not in the map), the
	// cache-push step for that region is skipped — the worker continues
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
	outboxRepo *repository.OutboxRepository,
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
		outboxRepo:     outboxRepo,
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

// SetBillingRepo injects the billing repo used by the deploy-time
// quota gate (issue #420). Optional injection — Deploy is the only
// caller, and existing tests that don't care about billing can leave
// it nil. When nil, the deploy-time 402 path treats a tenant as
// "no paid subscription" (subscription_status defaults to active),
// which preserves the pre-#420 behavior for tests that don't wire it.
func (s *DeploymentService) SetBillingRepo(b billingRepoForDeploymentSvc) {
	s.billingRepo = b
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

// SetIdempotencyRepo injects the Idempotency-Key replay cache
// (issue #52). When nil (the default), Deploy short-circuits
// the replay check and behaves exactly as before — every call
// mints a fresh deployment_id. Set via a setter (mirroring
// SetAppService / SetEnvService) so test harnesses that don't
// care about idempotency don't have to thread an extra
// constructor arg.
func (s *DeploymentService) SetIdempotencyRepo(r idempotencyRepoForDeploymentSvc) {
	s.idempotencyRepo = r
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
func (s *DeploymentService) Deploy(ctx context.Context, tenantID, appName string, r io.Reader, regions []string, autoRollback bool, desiredReplicas int, buildMeta *provenance.CLISideMetadata, previewOpts *PreviewOpts, idempotencyKey string, requestSHA256 [32]byte) (deployment *domain.Deployment, fromCache bool, err error) {
	// Validate appName to prevent path traversal (defense-in-depth)
	if !IsValidAppName(appName) {
		return nil, false, fmt.Errorf("invalid app name")
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
			return nil, false, fmt.Errorf("%w %q: must match [a-z0-9-]{1,64}", ErrInvalidRegion, r)
		}
	}
	if len(regions) > MaxRegionsPerDeployment {
		return nil, false, fmt.Errorf("%w: %d (max %d)", ErrTooManyRegions, len(regions), MaxRegionsPerDeployment)
	}

	// Idempotency-Key replay check (issue #52). When the caller
	// supplies a key AND the repo is wired, look up the cache
	// before doing any of the heavy lifting (artifact hash,
	// disk I/O, signature, row insert). A non-nil hit
	// short-circuits the function with the cached row +
	// fromCache=true; the handler converts that into 200 vs 201.
	//
	// The body-hash mismatch path (same key, different body)
	// returns ErrIdempotencyKeyMismatch so the handler maps it
	// to 422. This is the "you reused a key by mistake" guard
	// rail — the table's request_sha256 column is the source
	// of truth.
	//
	// Lookup errors other than sql.ErrNoRows bubble up; the
	// handler converts them to 500 via the existing
	// InternalErrorCtx path.
	if idempotencyKey != "" && s.idempotencyRepo != nil {
		cached, lookupErr := s.idempotencyRepo.Lookup(ctx, tenantID, idempotencyKey)
		if lookupErr != nil {
			return nil, false, fmt.Errorf("idempotency lookup: %w", lookupErr)
		}
		if cached != nil {
			// Verify the request body matches what produced
			// the cached row. A mismatch means the caller
			// reused a key against a different artifact —
			// refuse with 422 rather than silently replaying
			// a stale row against the wrong body.
			if cached.RequestSHA256 != requestSHA256 {
				return nil, false, ErrIdempotencyKeyMismatch
			}
			existing, getErr := s.deploymentRepo.GetByID(ctx, cached.DeploymentID)
			if getErr != nil {
				return nil, false, fmt.Errorf("idempotency replay fetch: %w", getErr)
			}
			if existing == nil {
				// The deployments row was deleted out from
				// under us (operator-initiated GC, FK
				// cascade taking a beat, etc.). Treat as
				// a miss — caller will write a fresh row
				// with the same key (ON CONFLICT DO NOTHING).
				// Falling through is the right behavior.
			} else {
				return existing, true, nil
			}
		}
	}

	// Auto-create the app record if it doesn't already exist (backward compatible).
	if s.appSvc != nil {
		if err := s.appSvc.CreateIfNotExists(ctx, tenantID, appName); err != nil {
			return nil, false, fmt.Errorf("creating app: %w", err)
		}
	}

	// Check quota (issue #420: deploy-time enforcement gate).
	//
	// Three pre-checks run before the static-cap check; any one failing
	// returns 402 PAYMENT_REQUIRED. Order matters — we test the cheapest,
	// most likely reason first (subscription state) before falling
	// through to the row-locking cap test.
	//
	//  1. subscription_status in {past_due, canceled} → 402
	//     (the merchant's billing provider has reported a stuck card
	//     or an explicit cancel; the tenant's payment method is bad
	//     and the next deploy won't fix it).
	//  2. tenants.disabled_at IS NOT NULL (free-tier lockdown active) → 402
	//     (reuses the existing disabled_at mechanism from #155; the
	//     cap row may still allow, but the tenant is locked from
	//     launching new apps until cleared).
	//  3. tenants.overage_allowed_until > now() → skip the cap check
	//     (paid tenant with an admin-granted grace window).
	//  4. VerifyUnderCap → 402 if the next-request-burst projection
	//     would push the tenant over their monthly cap.
	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		return nil, false, fmt.Errorf("getting quota: %w", err)
	}

	// Pre-check 1: subscription status. billingRepo is optional (set
	// via SetBillingRepo); nil preserves the pre-#420 behavior of
	// "no paid subscription → treat as active" so tests that don't
	// wire it don't have to mock it.
	if s.billingRepo != nil {
		status, err := s.billingRepo.GetSubscriptionStatus(ctx, tenantID)
		if err != nil {
			return nil, false, fmt.Errorf("getting subscription status: %w", err)
		}
		switch status {
		case domain.SubscriptionPastDue:
			return nil, false, &PaymentRequiredError{Reason: "subscription_past_due"}
		case domain.SubscriptionCanceled:
			return nil, false, &PaymentRequiredError{Reason: "subscription_canceled"}
		}
	}

	// Pre-check 2: free-tier lockdown via tenants.disabled_at. We
	// read the tenant row separately so the deploy-time gate can
	// also honour Pre-check 3 (overage grace). tenantRepo is wired
	// by the constructor; nil would skip both checks and degrade to
	// the pre-#420 behavior (cheap tests don't need to mock it).
	if s.tenantRepo != nil {
		tenant, err := s.tenantRepo.GetByID(ctx, tenantID)
		if err != nil {
			return nil, false, fmt.Errorf("getting tenant: %w", err)
		}
		if tenant != nil && tenant.IsDisabled() {
			// Free-tier lockdown is a billing cliff — apps should
			// stop, deploys should be blocked. The grace column
			// (quota_lock_grace_until) is checked separately below
			// so the request-time gate can still serve 402 only
			// after grace expires.
			return nil, false, &PaymentRequiredError{Reason: "free_tier_exceeded"}
		}
		// Pre-check 3: admin overage grace. The grace is a
		// per-tenant bypass for the cap check only — it does NOT
		// clear a free-tier lockdown (that's a separate admin
		// lever). If now < overage_allowed_until we skip VerifyUnderCap
		// below. Past timestamps are equivalent to NULL: the
		// comparison is strict.
		if tenant != nil && tenant.OverageAllowedUntil != nil &&
			!tenant.OverageAllowedUntil.IsZero() &&
			time.Now().UTC().Before(*tenant.OverageAllowedUntil) {
			// Skip cap verification but still fall through to the
			// static MaxDeployments count check below — the grace
			// is a quota cap bypass, not an "all checks off" flag.
			quota = nil // mark as "skip VerifyUnderCap"
		}
	}

	// Pre-check 4: projected cap verification. Skipped when the
	// overage grace is active (quota==nil from the pre-check above).
	// Default projection: 1 request for the deploy's first inbound
	// call, 0 outbound bytes (we don't know the response size).
	// Tunable later if we learn tenants abuse by deploying then
	// hammering — the heartbeat path moves the actual counter.
	if quota != nil {
		ok, err := s.quotaRepo.VerifyUnderCap(ctx, tenantID, 1, 0)
		if err != nil {
			return nil, false, fmt.Errorf("verifying cap: %w", err)
		}
		if !ok {
			return nil, false, &PaymentRequiredError{Reason: "quota_will_be_exceeded"}
		}
	}

	count, err := s.deploymentRepo.CountByApp(ctx, tenantID, appName)
	if err != nil {
		return nil, false, fmt.Errorf("counting deployments: %w", err)
	}
	if count >= quota.MaxDeployments {
		return nil, false, ErrMaxDeploymentsQuotaExceeded
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

	deployment = &domain.Deployment{
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
			// Idempotency-Key record (issue #52). Inserted
			// after the deployments row writes — same tx so
			// a signing / attestation failure rolls the cache
			// row back alongside the row it points at.
			// ON CONFLICT (tenant_id, key) DO NOTHING absorbs
			// concurrent retries with the same key; the first
			// writer's deployment_id is the one future
			// lookups return. The audit-naming asymmetry
			// ("deployment row first, then cache row") doesn't
			// matter because the cache FK cascades the row
			// away if the deployments row is rolled back, and
			// a successful deployment row is the only signal a
			// caller will ever see.
			//
			// requestSHA256 is the body hash the handler
			// computed from the multipart body BEFORE parsing
			// the parts, not the artifact hash from
			// SaveAndHash — same-key/different-body reuses
			// compare this against the stored hash.
			if idempotencyKey != "" && s.idempotencyRepo != nil {
				if iErr := s.idempotencyRepo.Insert(ctx, &domain.IdempotencyKey{
					TenantID:      tenantID,
					Key:           idempotencyKey,
					DeploymentID:  deployment.ID,
					RequestSHA256: requestSHA256,
				}); iErr != nil {
					return fmt.Errorf("recording idempotency key: %w", iErr)
				}
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
				} else if idempotencyKey != "" && s.idempotencyRepo != nil {
					// No-tx path (test only): record the
					// idempotency key after the deployments
					// row is created. ON CONFLICT DO NOTHING
					// absorbs concurrent retries with the same
					// key, mirroring the tx path's behavior.
					if iErr := s.idempotencyRepo.Insert(ctx, &domain.IdempotencyKey{
						TenantID:      tenantID,
						Key:           idempotencyKey,
						DeploymentID:  deployment.ID,
						RequestSHA256: requestSHA256,
					}); iErr != nil {
						err = fmt.Errorf("recording idempotency key: %w", iErr)
					}
				}
			}
		}
	}
	if err != nil {
		return nil, false, err
	}

	if s.webhookSvc != nil {
		s.webhookSvc.PublishEvent(context.Background(), deployment.TenantID, deployment.AppName, "deploy", map[string]string{
			"deployment_id": deployment.ID,
			"hash":          deployment.Hash,
		})
	}

	return deployment, false, nil
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
// and PromoteDeployment. It sets the active deployment row and enqueues
// a durable NATS publish via the transactional outbox (issue #42),
// without checking the deployment's original app name.
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
	//
	// Phase 1 (transactional): the active_deployments upsert +
	// ClearStableSince + the outbox row are written in a single tx.
	// The outbox row carries the marshaled TaskMessage so the
	// background OutboxDrainer can publish it durably after commit;
	// before #42 this publish ran synchronously here, with a
	// process/NATS crash between commit and publish leaving the row
	// active but no worker ever receiving the TaskMessage (until the
	// reconcile loop's 5-minute FullSync safety net).
	//
	// buildPublishPayload reads env / tenant / quota via the same tx
	// so the payload reflects post-commit state even if another
	// activate lands before this one drains. The reads happen against
	// the tx's connection so they see a consistent snapshot.
	regions := domain.StringArrayTo(deployment.Regions)
	if len(regions) == 0 {
		regions = []string{s.defaultRegion}
	}
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

		// Build the TaskMessage payload inside the tx so env /
		// tenant / quota reads participate in the same atomic
		// snapshot as the active_deployments mutation. Decryption
		// (CPU-only, no DB) is fine inside the closure. The
		// dedupe_key is `<tenant>:<app>:<attempt_id>` — UNIQUE in
		// the outbox schema so a buggy re-enqueue surfaces as a
		// constraint violation rather than a double-publish.
		payload, err := s.buildPublishPayload(ctx, tx, tenantID, appName,
			deploymentID, deployment, regions)
		if err != nil {
			return fmt.Errorf("building publish payload: %w", err)
		}
		attemptID := uuid.NewString()
		if err := s.outboxRepo.WithTx(tx).Enqueue(ctx, &repository.OutboxRow{
			TenantID:  tenantID,
			AppName:   appName,
			Kind:      "task_update",
			Payload:   payload,
			Regions:   pq.StringArray(regions),
			DedupeKey: tenantID + ":" + appName + ":" + attemptID,
		}); err != nil {
			return fmt.Errorf("enqueueing outbox row: %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("setting active deployment: %w", err)
	}

	// Phase 2 (post-tx, fire-and-forget): the optional per-region
	// artifact cache push. Cache failures are best-effort (logged,
	// not returned) — the worker falls back to /api/internal/download
	// when the cache misses. The NATS TaskMessage is durably queued
	// in the outbox; the OutboxDrainer will publish it independently
	// of this handler.
	if err := s.publishSwap(ctx, tenantID, appName, deploymentID, regions); err != nil {
		// publishSwap returns nil on cache-push errors today
		// (best-effort), but defensive: a future regression that
		// returns an error here should not break activate.
		log.Printf("activate: cache-push post-state failed for %s/%s/%s: %v", tenantID, appName, deploymentID, err)
	}
	if s.webhookSvc != nil {
		s.webhookSvc.PublishEvent(context.Background(), tenantID, appName, "activate", map[string]string{
			"deployment_id": deploymentID,
		})
	}
	return nil
}

// buildPublishPayload assembles the marshaled TaskMessage that
// accompanies the active_deployments mutation. Runs INSIDE the
// caller's tx so env / tenant / quota reads participate in the same
// snapshot (the on-wire message reflects post-commit state). Decryption
// is CPU-only and safe inside the closure.
//
// Takes pre-resolved regions (so the activate path's default-region
// fallback and the rollback path's deployment-regions resolution
// aren't duplicated) plus the deployment row (for hash / signature /
// signing_key_id / preview linkage). Returns the marshaled JSON
// payload ready to be stored on the outbox row.
func (s *DeploymentService) buildPublishPayload(ctx context.Context, tx *sqlx.Tx, tenantID, appName, deploymentID string, deployment *domain.Deployment, regions []string) ([]byte, error) {
	envsList, err := s.appEnvRepo.WithTx(tx).List(ctx, tenantID, appName)
	if err != nil {
		return nil, fmt.Errorf("listing env vars: %w", err)
	}
	envMap := make(map[string]string, len(envsList))
	for _, e := range envsList {
		if s.envSvc != nil {
			// envSvc decrypts the ciphertext values stored in
			// app_env (when secrets encryption is enabled).
			// Decryption is CPU-only and safe to do inside the tx
			// closure. A decrypt error is fatal for the publish:
			// silently falling through with raw ciphertext would
			// publish a payload the worker can't use and is hard
			// to diagnose downstream. The pre-#42 rollback path
			// surfaced this as an error; the activate path used
			// log-and-continue, but in the outbox world a failed
			// enqueue is cheaper than publishing a broken message.
			plain, derr := s.envSvc.Decrypt(e.EnvValue)
			if derr != nil {
				return nil, fmt.Errorf("decrypting env %s: %w", e.EnvKey, derr)
			}
			envMap[e.EnvKey] = plain
			continue
		}
		envMap[e.EnvKey] = e.EnvValue
	}

	tenant, err := s.tenantRepo.WithTx(tx).GetByID(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("getting tenant: %w", err)
	}
	if tenant == nil {
		return nil, fmt.Errorf("tenant not found")
	}
	if tenant.IsDisabled() {
		// Issue #440 belt-and-braces: the tx-time check above catches
		// the racing case under the tenants-row FOR UPDATE lock. This
		// post-commit check covers the (theoretical) case where a
		// future non-tx activation path skips that lock and observes
		// the disabled tenant only after its own write commits. Wrap
		// with ErrTenantDisabled so the handler's `errors.Is` branch
		// maps it to 409, matching the tx-time path's status.
		return nil, fmt.Errorf("%w: tenant=%s", ErrTenantDisabled, tenantID)
	}

	quota, err := s.quotaRepo.WithTx(tx).GetByTenantID(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("getting quota: %w", err)
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
	return json.Marshal(msg)
}

// publishSwap is the post-commit cache-push step. Pre-#42 this also
// fanned out the NATS TaskMessage; that responsibility now lives on
// the OutboxDrainer. publishSwap only handles:
//
//   - reading the committed active row to discover regions_published
//     (skip — already on the wire) and regions_cached (skip — push
//     already happened, the cache-skip-on-activation optimization
//     from issue #332 Layer 3),
//   - pushing the artifact bytes to per-region edge-artifact-cache
//     binaries,
//   - persisting the cache-state outcome via AppendRegionsCacheState.
//
// The function never returns an error today: cache-push failures are
// best-effort (logged, not returned). The signature keeps the
// `error` return so a future regression that surfaces a cache error
// doesn't break the caller — log-and-continue stays the default.
func (s *DeploymentService) publishSwap(ctx context.Context, tenantID, appName, deploymentID string, regions []string) error {
	// Cache push only — pre-#42 this function also fanned out the
	// NATS TaskMessage via s.publisher.PublishTaskUpdate. That
	// responsibility now lives on the OutboxDrainer
	// (internal/service/outbox_drainer.go). The handler-side
	// activate/rollback no longer publishes NATS directly; the
	// outbox row carries the TaskMessage and the drainer relays
	// it. publishSwap is now strictly cache-push + cache-state
	// post-write.
	//
	// Best-effort: a cache push error does NOT bubble up as a 502
	// to the client (the activation succeeded — the worker will
	// pull from /api/internal/download when its cache miss is
	// detected). The error is logged and the per-region state is
	// persisted to regions_cache_failed for operator visibility.
	//
	// Skip conditions:
	//   - s.cachePusher is nil (cache feature disabled at runtime)
	//   - s.regionArtifactCaches[region] is unset or empty
	//   - region is in alreadyCached (issue #332, Layer 3): a Set
	//     with the same deployment_id preserves RegionsCached via
	//     the CASE WHEN in the DO UPDATE clause, so this branch
	//     fires on a re-activation of the same deployment where
	//     the cache is still warm from a prior activation.
	if s.cachePusher == nil || len(s.regionArtifactCaches) == 0 {
		return nil
	}

	// Read the committed row's cache state so the alreadyCached
	// skip-on-activation optimization fires correctly.
	current, err := s.activeRepo.Get(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("reading cache state: %w", err)
	}
	alreadyCached := make(map[string]struct{}, len(current.RegionsCached))
	for _, r := range current.RegionsCached {
		alreadyCached[r] = struct{}{}
	}

	// Build the per-region toPush set, deduping against the
	// alreadyCached map.
	seen := make(map[string]struct{}, len(regions))
	var cachedSucceeded []string
	var cachedSkipped []string
	var cacheFailed []string
	for _, region := range regions {
		if _, ok := alreadyCached[region]; ok {
			cachedSkipped = append(cachedSkipped, region)
			continue
		}
		if _, dup := seen[region]; dup {
			continue
		}
		seen[region] = struct{}{}
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

	// Persist cache-state outcome. Best-effort — a DB error is
	// logged but does not change the function's return value.
	if len(cachedSucceeded) > 0 || len(cachedSkipped) > 0 || len(cacheFailed) > 0 {
		now := time.Now()
		if err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
			txRepo := s.activeRepo.WithTx(tx)
			mergedSucceeded := append([]string{}, cachedSucceeded...)
			mergedSucceeded = append(mergedSucceeded, cachedSkipped...)
			if err := txRepo.AppendRegionsCacheState(ctx, tenantID, appName, mergedSucceeded, cacheFailed, now); err != nil {
				return fmt.Errorf("append regions_cache_state: %w", err)
			}
			return nil
		}); err != nil {
			log.Printf("warning: persisting cache state for %s/%s/%s: %v", tenantID, appName, deploymentID, err)
		}
	}
	return nil
}

// Issue #42 removed (*DeploymentService).waitForWorkers. The
// synchronous worker-confirmation wait is redundant now that the
// outbox makes publish durable — workers pick up the new active
// deployment on their next heartbeat-driven reconcile, and
// publishSwap's only post-tx work is the per-region cache push.

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

		// Build the TaskMessage payload inside the tx (issue #42) so
		// env / tenant / quota reads participate in the same atomic
		// snapshot as the active_deployments mutation. The drainer
		// will relay the marshaled payload after commit.
		deploymentForPayload := &domain.Deployment{
			Hash:            deploymentHash,
			Signature:       deploymentSignature,
			SigningKeyID:    deploymentSigningKeyID,
			PreviewID:       nil,
			PreviewPRNumber: nil,
		}
		if rollbackPreviewID != "" {
			pid := rollbackPreviewID
			deploymentForPayload.PreviewID = &pid
			pr := rollbackPreviewPRNumber
			deploymentForPayload.PreviewPRNumber = &pr
		}
		payload, err := s.buildPublishPayload(ctx, tx, tenantID, appName,
			rolledBackID, deploymentForPayload, regions)
		if err != nil {
			return fmt.Errorf("building publish payload: %w", err)
		}
		attemptID := uuid.NewString()
		if err := s.outboxRepo.WithTx(tx).Enqueue(ctx, &repository.OutboxRow{
			TenantID:  tenantID,
			AppName:   appName,
			Kind:      "task_update",
			Payload:   payload,
			Regions:   pq.StringArray(regions),
			DedupeKey: tenantID + ":" + appName + ":" + attemptID,
		}); err != nil {
			return fmt.Errorf("enqueueing outbox row: %w", err)
		}
		return nil
	}); err != nil {
		return "", err
	}

	// Post-tx: cache push only (the outbox row above carries the
	// TaskMessage; the drainer relays it). publishSwap never errors
	// for the rollback path today — log defensively if a future
	// regression surfaces a cache error.
	if err := s.publishSwap(ctx, tenantID, appName, rolledBackID, regions); err != nil {
		log.Printf("rollback: cache-push post-state failed for %s/%s/%s: %v", tenantID, appName, rolledBackID, err)
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
