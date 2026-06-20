package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
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
const MaxArtifactSize = 100 * 1024 * 1024

// MaxRegionsPerDeployment caps the number of regions a single deployment
// can target. Defensive ceiling against fan-out abuse; realistic tenants
// want ≤10 regions. Operators can raise this constant if needed.
const MaxRegionsPerDeployment = 16

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
	ErrNoLastGood                  = fmt.Errorf("no previous deployment to roll back to")
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
	ErrPublishFailed = fmt.Errorf("publishing task update failed")
)

// DeploymentService handles deployment business logic.
type DeploymentService struct {
	db             *sqlx.DB
	deploymentRepo *repository.DeploymentRepository
	activeRepo     *repository.ActiveDeploymentRepository
	appEnvRepo     *repository.AppEnvRepository
	quotaRepo      *repository.QuotaRepository
	tenantRepo     *repository.TenantRepository
	artifactStore  *storage.ArtifactStore
	publisher      nats.Publisher
	appSvc         *AppService
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
	artifactStore *storage.ArtifactStore,
	publisher nats.Publisher,
	defaultRegion string,
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
	}
}

// SetAppService sets the AppService dependency for auto-creating apps on deploy.
func (s *DeploymentService) SetAppService(appSvc *AppService) {
	s.appSvc = appSvc
}

// Deploy creates a new deployment and stores the artifact.
//
// `regions` is the list of regions the deployment is targeted at. Pass
// nil/empty to use the control plane's default region (preserves the
// pre-#82 single-region behavior). Each region is validated against
// `IsValidRegion`; the first invalid entry fails the call before any
// DB or storage I/O.
//
// After the deployment row is written, the activate path will publish
// one `TaskMessage` per region to `edgecloud.tasks.<region>`. (See
// `ActivateDeployment`.)
func (s *DeploymentService) Deploy(ctx context.Context, tenantID, appName string, r io.Reader, regions []string) (*domain.Deployment, error) {
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

	// Read artifact and compute hash (bounded to prevent memory exhaustion)
	data, err := io.ReadAll(io.LimitReader(r, MaxArtifactSize+1))
	if err != nil {
		return nil, fmt.Errorf("reading artifact: %w", err)
	}
	if int64(len(data)) > MaxArtifactSize {
		return nil, fmt.Errorf("artifact exceeds maximum size of %d bytes", MaxArtifactSize)
	}

	// Reject non-wasm artifacts before persisting them. Without this guard a
	// non-wasm file would be stored, hashed, and shipped to workers, where
	// it would fail only at execution time. Magic bytes are the cheapest
	// first-line check — full module validation is wasmtime's job.
	if !validateWasm(data) {
		return nil, fmt.Errorf("invalid wasm artifact: missing magic bytes (\\0asm)")
	}

	hash := sha256.Sum256(data)

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
		ID:        "d_" + uuid.New().String(),
		TenantID:  tenantID,
		AppName:   appName,
		Status:    domain.StatusDeployed,
		Hash:      hex.EncodeToString(hash[:]),
		Regions:   domain.StringArrayFrom(effectiveRegions),
		CreatedAt: time.Now(),
	}

	if err := s.deploymentRepo.Create(ctx, deployment); err != nil {
		return nil, fmt.Errorf("creating deployment: %w", err)
	}

	// Save artifact
	if err := s.artifactStore.Save(tenantID, appName, deployment.ID, bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("saving artifact: %w", err)
	}

	return deployment, nil
}

func (s *DeploymentService) GetDeployment(ctx context.Context, tenantID, id string) (*domain.Deployment, error) {
	deployment, err := s.deploymentRepo.GetByID(ctx, id)
	if err != nil || deployment == nil {
		return nil, err
	}
	if deployment.TenantID != tenantID {
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
		txActive := s.activeRepo.WithTx(tx)
		current, err := txActive.GetForUpdate(ctx, tenantID, appName)
		if err != nil {
			return fmt.Errorf("reading current active deployment: %w", err)
		}
		var lastGood *string
		if current != nil {
			lastGood = &current.DeploymentID
		}
		return txActive.Set(ctx, &domain.ActiveDeployment{
			TenantID:             tenantID,
			AppName:              appName,
			DeploymentID:         deploymentID,
			LastGoodDeploymentID: lastGood,
		})
	}); err != nil {
		return fmt.Errorf("setting active deployment: %w", err)
	}

	// Publish task update
	envs, err := s.appEnvRepo.List(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("listing env vars: %w", err)
	}
	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.EnvKey] = e.EnvValue
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

	msg := &nats.TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now(),
		TenantID:  tenantID,
		Apps: map[string]nats.AppConfig{
			appName: {
				DeploymentID:   deploymentID,
				DeploymentHash: deployment.Hash,
				Env:            envMap,
				Allowlist:      domain.StringArrayTo(tenant.AllowlistedDestinations),
				MaxMemoryMB:    maxMemoryMB,
			},
		},
	}

	// Resolve the effective regions list to publish to:
	//   - deployment.Regions if non-empty (post-migration-008 row),
	//   - otherwise the control plane's default region (handles
	//     pre-migration rows AND any caller that wrote the row via a
	//     path that left Regions nil).
	regions := domain.StringArrayTo(deployment.Regions)
	if len(regions) == 0 {
		regions = []string{s.defaultRegion}
	}

	// Fan out: publish one TaskMessage per region. Failures are
	// logged but do NOT abort the loop — if a tenant activated to
	// three regions and one publish fails (e.g. NATS blip), the
	// other two should still get the message. The error returned
	// here is a summary wrapped with ErrPublishFailed so the HTTP
	// layer can match via errors.Is and surface a 502 — the DB row
	// has already committed by this point, so workers may still be
	// serving the prior deployment; the client should treat this
	// as a transient infrastructure failure and retry the failed
	// regions.
	var failedRegions []string
	for _, region := range regions {
		if err := s.publisher.PublishTaskUpdate(region, msg); err != nil {
			log.Printf("publishing task update failed for region %q (deployment %s): %v", region, deploymentID, err)
			failedRegions = append(failedRegions, region)
		}
	}
	if len(failedRegions) > 0 {
		return fmt.Errorf("%w: %d region(s) failed: %s", ErrPublishFailed, len(failedRegions), strings.Join(failedRegions, ","))
	}

	return nil
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
	var tenant *domain.Tenant
	var envs []domain.AppEnv
	var maxMemoryMB int

	if err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
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
		dep, err := s.deploymentRepo.GetByID(ctx, rolledBackID)
		if err != nil || dep == nil {
			return fmt.Errorf("previous deployment %s not found", rolledBackID)
		}
		if dep.TenantID != tenantID || dep.AppName != appName {
			return fmt.Errorf("previous deployment %s not found", rolledBackID)
		}
		deploymentHash = dep.Hash

		// Clear last_good so a second rollback is a no-op (returns 409
		// rather than rolling back to whatever was active two steps ago —
		// that requires an N-step history table, out of scope for the
		// minimum viable UX).
		if err := txActive.Set(ctx, &domain.ActiveDeployment{
			TenantID:             tenantID,
			AppName:              appName,
			DeploymentID:         rolledBackID,
			LastGoodDeploymentID: nil,
		}); err != nil {
			return fmt.Errorf("swapping active deployment: %w", err)
		}

		// Snapshot the publish inputs inside the tx so the message
		// reflects post-rollback state even if another activate lands
		// between commit and publish (which would itself race with this
		// publish; see plan §"Risk register").
		envsList, err := s.appEnvRepo.List(ctx, tenantID, appName)
		if err != nil {
			return fmt.Errorf("listing env vars: %w", err)
		}
		envs = envsList
		tenant, err = s.tenantRepo.GetByID(ctx, tenantID)
		if err != nil || tenant == nil {
			return fmt.Errorf("getting tenant: %w", err)
		}
		quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
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

	envMap := make(map[string]string)
	for _, e := range envs {
		envMap[e.EnvKey] = e.EnvValue
	}

	msg := &nats.TaskMessage{
		Type:      "task_update",
		Timestamp: time.Now(),
		TenantID:  tenantID,
		Apps: map[string]nats.AppConfig{
			appName: {
				DeploymentID:   rolledBackID,
				DeploymentHash: deploymentHash,
				Env:            envMap,
				Allowlist:      tenant.AllowlistedDestinations,
				MaxMemoryMB:    maxMemoryMB,
			},
		},
	}
	if err := s.publisher.PublishTaskUpdate("global", msg); err != nil {
		return "", fmt.Errorf("%w: %w", ErrPublishFailed, err)
	}

	return rolledBackID, nil
}

func (s *DeploymentService) GetActiveDeployment(ctx context.Context, tenantID, appName string) (*domain.Deployment, error) {
	ad, err := s.activeRepo.Get(ctx, tenantID, appName)
	if err != nil || ad == nil {
		return nil, err
	}
	return s.deploymentRepo.GetByID(ctx, ad.DeploymentID)
}

func (s *DeploymentService) GetArtifact(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error) {
	// Verify deployment belongs to this tenant
	deployment, err := s.deploymentRepo.GetByID(ctx, deploymentID)
	if err != nil || deployment == nil {
		return nil, fmt.Errorf("deployment not found")
	}
	if deployment.TenantID != tenantID || deployment.AppName != appName {
		return nil, fmt.Errorf("deployment not found")
	}
	return s.artifactStore.Open(tenantID, appName, deploymentID)
}
