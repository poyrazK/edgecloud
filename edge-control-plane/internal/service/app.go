package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// appRepoInterface defines the app repository methods used by AppService.
type appRepoInterface interface {
	Create(ctx context.Context, app *domain.App) error
	Get(ctx context.Context, tenantID, appName string) (*domain.App, error)
	List(ctx context.Context, tenantID string, limit int, afterName string) ([]domain.App, error)
	CountByTenant(ctx context.Context, tenantID string) (int, error)
	AtomicDelete(ctx context.Context, tenantID, appName string) (bool, error)
	InsertIfNotExists(ctx context.Context, app *domain.App) (bool, error)
	Update(ctx context.Context, app *domain.App) error
	// GetForUpdate locks the (tenant, app) row with `SELECT … FOR UPDATE`.
	// Added for the v2 quota-race fix in AddDomain (issue #83 second-pass
	// review); the lock is held for the caller's tx lifetime.
	GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.App, error)
	DeleteIfNoDeployments(ctx context.Context, tenantID, appName string) (bool, error)
	// L4 port allocation (issue #548). See repository/app.go for the
	// atomicity story — AllocateL4Port serializes across racing ingress
	// instances via `l4_public_port IS NULL` guard, so two callers that
	// pick different ports converge to the same persisted value.
	GetL4Port(ctx context.Context, tenantID, appName string) (uint16, error)
	AllocateL4Port(ctx context.Context, tenantID, appName string, port uint16) (uint16, error)
	ReleaseL4Port(ctx context.Context, tenantID, appName string) error
	AllocatedL4Ports(ctx context.Context) (map[uint16]struct{}, error)
	// WithTx returns a tx-bound repository so the cascade delete
	// (issue #60) can run inside `repository.Transaction` — the parent
	// delete, child deletes, and outbox enqueue share one transaction.
	WithTx(tx *sqlx.Tx) *repository.AppRepository
}

// AppService handles app business logic.
type AppService struct {
	db               *sqlx.DB
	appRepo          appRepoInterface
	appEnvRepo       *repository.AppEnvRepository
	activeRepo       *repository.ActiveDeploymentRepository
	deployRepo       *repository.DeploymentRepository
	trafficSplitRepo *repository.TrafficSplitRepository
	artifactStore    storage.ArtifactStore
	quotaRepo        quotaRepoInterface
	outboxRepo       *repository.OutboxRepository
	defaultRegion    string
	// l4PortRangeStart and l4PortRangeEnd bound the L4/TCP port
	// range used by AllocateL4Port (issue #548). Configured via
	// cfg.L4.PortRangeStart / cfg.L4.PortRangeEnd and threaded into
	// NewAppService from app.New. Must be 1..65535 with start <= end;
	// validated by config.validateL4Config at startup.
	l4PortRangeStart uint16
	l4PortRangeEnd   uint16
}

func NewAppService(
	db *sqlx.DB,
	appRepo *repository.AppRepository,
	deploymentRepo *repository.DeploymentRepository,
	activeRepo *repository.ActiveDeploymentRepository,
	appEnvRepo *repository.AppEnvRepository,
	trafficSplitRepo *repository.TrafficSplitRepository,
	artifactStore storage.ArtifactStore,
	quotaRepo *repository.QuotaRepository,
	outboxRepo *repository.OutboxRepository,
	defaultRegion string,
	l4PortRangeStart uint16,
	l4PortRangeEnd uint16,
) *AppService {
	if defaultRegion == "" {
		defaultRegion = "global"
	}
	return &AppService{
		db:               db,
		appRepo:          appRepo,
		activeRepo:       activeRepo,
		appEnvRepo:       appEnvRepo,
		deployRepo:       deploymentRepo,
		trafficSplitRepo: trafficSplitRepo,
		artifactStore:    artifactStore,
		quotaRepo:        quotaRepo,
		outboxRepo:       outboxRepo,
		defaultRegion:    defaultRegion,
		l4PortRangeStart: l4PortRangeStart,
		l4PortRangeEnd:   l4PortRangeEnd,
	}
}

// Sentinel errors.
var ErrAppAlreadyExists = fmt.Errorf("app already exists")
var ErrMaxAppsQuotaExceeded = fmt.Errorf("max apps reached for tenant")

// ErrInvalidLimit is returned by AppService.List when the handler
// forwards a non-positive limit. The handler is expected to clamp
// before calling; this is a defense-in-depth guard so a future
// handler regression can't silently trigger a SELECT with LIMIT 0
// (which would return zero rows and look like "no apps" to a
// caller). Issue #58.
var ErrInvalidLimit = fmt.Errorf("invalid limit")

// Create creates a new app. Returns ErrAppAlreadyExists if it already exists.
// Returns ErrMaxAppsQuotaExceeded if the tenant has reached their MaxApps limit.
func (s *AppService) Create(ctx context.Context, tenantID, appName string, req *domain.CreateAppRequest) (*domain.App, error) {
	if !IsValidAppName(appName) {
		return nil, fmt.Errorf("invalid app name: %s", appName)
	}

	// Serialize concurrent creates for the same tenant via SELECT FOR UPDATE.
	// This prevents a TOCTOU race where two simultaneous creates both count N
	// apps, both pass the quota check, and the tenant ends up with N+1 apps.
	if s.db != nil {
		tx, err := s.db.BeginTxx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("beginning transaction: %w", err)
		}
		defer func() {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				log.Printf("app service: failed to rollback transaction: %v", rollbackErr)
			}
		}()

		// Lock the tenant row for the duration of this transaction.
		// Acquire a row lock on the tenant row. The boolean result is unused;
		// only the lock (held until tx.Commit) matters for serializing concurrent creates.
		var tenantExists bool
		err = tx.GetContext(ctx, &tenantExists, `SELECT true FROM tenants WHERE id = $1 FOR UPDATE`, tenantID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("tenant not found")
			}
			return nil, fmt.Errorf("locking tenant: %w", err)
		}

		// Check MaxApps quota while holding the lock.
		quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
		if err != nil {
			return nil, fmt.Errorf("getting quota: %w", err)
		}
		count, err := s.appRepo.CountByTenant(ctx, tenantID)
		if err != nil {
			return nil, fmt.Errorf("counting apps: %w", err)
		}
		if quota != nil && count >= quota.MaxApps {
			return nil, ErrMaxAppsQuotaExceeded
		}

		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("committing transaction: %w", err)
		}
	} else {
		// No DB available (test path) — check quota without the lock.
		quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
		if err != nil {
			return nil, fmt.Errorf("getting quota: %w", err)
		}
		count, err := s.appRepo.CountByTenant(ctx, tenantID)
		if err != nil {
			return nil, fmt.Errorf("counting apps: %w", err)
		}
		if quota != nil && count >= quota.MaxApps {
			return nil, ErrMaxAppsQuotaExceeded
		}
	}

	var desc *string
	if req.Description != "" {
		desc = &req.Description
	}

	// Use InsertIfNotExists for an atomic check-and-insert — no TOCTOU race.
	app := &domain.App{
		ID:          "a_" + uuid.New().String(),
		TenantID:    tenantID,
		Name:        appName,
		Description: desc,
		CreatedAt:   time.Now(),
	}
	inserted, err := s.appRepo.InsertIfNotExists(ctx, app)
	if err != nil {
		return nil, fmt.Errorf("creating app: %w", err)
	}
	if !inserted {
		return nil, ErrAppAlreadyExists
	}
	return app, nil
}

// Get returns an app by name, or nil if not found.
func (s *AppService) Get(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	return s.appRepo.Get(ctx, tenantID, appName)
}

// GetForUpdate locks the (tenant, app) row with `SELECT … FOR UPDATE`
// for the lifetime of the surrounding tx and returns it. Used by
// `DomainService.AddDomain` to serialize concurrent domain inserts
// against the same parent app so the per-app quota
// (MaxDomainsPerApp) cannot be overshot. Pass-through to
// `AppRepository.GetForUpdate` — the lock is held until the caller's
// tx commits or rolls back. Returns (nil, nil) when no app exists.
func (s *AppService) GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	return s.appRepo.GetForUpdate(ctx, tenantID, appName)
}

// AppListPage is the wire shape the apps handler returns to callers.
// Issue #58 hard-cut to cursor pagination: no offset, no total — just
// the page slice, the limit, and an opaque next_cursor (nil on the
// final page). The handler turns this into JSON.
type AppListPage struct {
	Apps       []domain.App
	Limit      int
	NextCursor *string
}

// List returns one page of apps for a tenant via keyset pagination
// (issue #58). The handler passes the previously-returned NextCursor
// back in via afterCursor; the empty string means "first page".
//
// Limit/offset semantics:
//
//   - limit is the page size requested by the caller.
//   - The service fetches limit+1 rows internally to detect whether
//     a next page exists without a separate COUNT(*) query. If the
//     repository returns exactly limit+1 rows, the trailing row is
//     dropped from the response and a NextCursor is encoded from the
//     last visible row's name (the row at index limit-1).
//   - The handler caps limit at 500 (appsLimitCap); this service
//     trusts the handler's cap.
//
// Errors:
//
//   - ErrInvalidAppCursor / ErrUnsupportedAppCursorVersion: the
//     supplied cursor is malformed or from a newer server. Handler
//     maps to 400.
//   - repository errors bubble up unchanged.
func (s *AppService) List(ctx context.Context, tenantID string, limit int, afterCursor string) (*AppListPage, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	afterName := ""
	if afterCursor != "" {
		var err error
		afterName, err = decodeAppCursor(afterCursor)
		if err != nil {
			return nil, err
		}
	}

	// Fetch limit+1 to detect "has more" without a separate count.
	rows, err := s.appRepo.List(ctx, tenantID, limit+1, afterName)
	if err != nil {
		return nil, err
	}

	page := &AppListPage{
		Limit: limit,
	}
	if len(rows) > limit {
		rows = rows[:limit]
		cursor, err := encodeAppCursor(rows[len(rows)-1].Name)
		if err != nil {
			// encodeAppCursor only fails on an empty name — which
			// would mean the repo returned an empty Name, a schema
			// violation we cannot recover from at this layer. The
			// caller will see a 500.
			return nil, err
		}
		page.NextCursor = &cursor
	}
	page.Apps = rows
	return page, nil
}

// Update updates mutable fields of an existing app.
// Returns ErrAppNotFound if the app does not exist.
// Currently only description is mutable; add fields to the
// UpdateAppRequest and this method as needed.
func (s *AppService) Update(ctx context.Context, tenantID, appName string, req *domain.UpdateAppRequest) (*domain.App, error) {
	app, err := s.appRepo.Get(ctx, tenantID, appName)
	if err != nil {
		return nil, fmt.Errorf("getting app: %w", err)
	}
	if app == nil {
		return nil, ErrAppNotFound
	}

	if req.Description != nil {
		app.Description = req.Description
	}

	if err := s.appRepo.Update(ctx, app); err != nil {
		return nil, fmt.Errorf("updating app: %w", err)
	}
	return app, nil
}

// Delete deletes an app and all its associated data atomically.
// Returns ErrAppNotFound if the app does not exist.
//
// Atomicity: the parent apps row delete, the child deletes
// (app_env, active_deployments, app_traffic_splits, deployments),
// and the task_purge outbox enqueue all run inside one PostgreSQL
// transaction. Either everything commits or everything rolls back
// — so the invariant "a task_purge row was enqueued iff the parent
// apps row was removed" is preserved. Compare with the previous
// shape, which committed the parent delete first and only ran the
// child work in a second transaction; a cascade failure there
// silently orphaned env/active/deployment rows and never enqueued
// the worker's purge tombstone. See issue #60.
//
// Artifact cleanup (.wasm and .cwasm) runs only after a successful
// DB commit. External object storage cannot participate in a
// PostgreSQL transaction, so the artifact pass is best-effort and
// post-commit errors are aggregated with errors.Join and returned
// to the caller; the HTTP handler maps non-ErrAppNotFound errors to
// 500. See issue #60.
var ErrAppNotFound = fmt.Errorf("app not found")

func (s *AppService) Delete(ctx context.Context, tenantID, appName string) error {
	var deployments []domain.Deployment

	err := repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		appRepo := s.appRepo.WithTx(tx)
		appEnvRepo := s.appEnvRepo.WithTx(tx)
		activeRepo := s.activeRepo.WithTx(tx)
		trafficSplitRepo := s.trafficSplitRepo.WithTx(tx)
		deployRepo := s.deployRepo.WithTx(tx)
		outboxRepo := s.outboxRepo.WithTx(tx)

		// 1. Delete parent. Returning `false` (no rows matched) is
		//    ErrAppNotFound — the closure returns it and Transaction
		//    rolls back without touching anything else.
		deleted, err := appRepo.AtomicDelete(ctx, tenantID, appName)
		if err != nil {
			return fmt.Errorf("deleting app: %w", err)
		}
		if !deleted {
			return ErrAppNotFound
		}

		// 2. Capture deployment IDs before deleting the rows so the
		//    post-commit artifact cleanup can target each .wasm and
		//    .cwasm file. Runs inside the tx for a consistent
		//    snapshot — if a concurrent insert lands between here
		//    and step 6, the tx's row snapshot excludes it.
		deployments, err = deployRepo.ListByApp(ctx, tenantID, appName)
		if err != nil {
			return fmt.Errorf("listing deployments for artifact cleanup: %w", err)
		}

		// 3. Child deletes in dependency order:
		//    - app_traffic_splits MUST run before deployments because
		//      traffic_splits.deployment_id has no ON DELETE CASCADE.
		if err := appEnvRepo.DeleteByApp(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting app env: %w", err)
		}
		if err := activeRepo.Delete(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting active deployment: %w", err)
		}
		if err := trafficSplitRepo.DeleteAllForApp(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting traffic splits: %w", err)
		}
		if err := deployRepo.DeleteByApp(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting deployments: %w", err)
		}

		// 4. Enqueue the worker task_purge tombstone inside the same
		//    tx. Issue #569: the worker drops per-app KV / cache /
		//    scheduling state when it receives this. If any step above
		//    errors and rolls back, no row is enqueued — the worker
		//    never receives a phantom purge for a still-existing app.
		payload, err := json.Marshal(nats.PurgePayload{
			Type:      nats.TaskMessageKindTaskPurge,
			Timestamp: time.Now().UTC(),
			TenantID:  tenantID,
			AppName:   appName,
			Reason:    nats.PurgeReasonAppDeleted,
		})
		if err != nil {
			return fmt.Errorf("marshaling purge payload: %w", err)
		}
		if err := outboxRepo.Enqueue(ctx, &repository.OutboxRow{
			TenantID:  tenantID,
			AppName:   appName,
			Kind:      nats.TaskMessageKindTaskPurge,
			Payload:   payload,
			Regions:   pq.StringArray{s.defaultRegion},
			DedupeKey: "purge:" + tenantID + ":" + appName + ":" + uuid.NewString(),
		}); err != nil {
			return fmt.Errorf("enqueueing task_purge: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 5. Post-commit artifact cleanup. Failure here leaves the DB
	//    deletion intact but orphan files in storage — the caller is
	//    told via 500 so a follow-up `edge apps delete` can retry
	//    (the second attempt finds no deployments to clean and returns
	//    nil). errors.Join aggregates per-deployment failures so a
	//    caller sees every problematic deployment, not just the last.
	if s.artifactStore == nil {
		return nil
	}
	var delErr error
	for _, d := range deployments {
		if err := s.artifactStore.Delete(ctx, tenantID, appName, d.ID); err != nil {
			delErr = errors.Join(delErr,
				fmt.Errorf("wasm artifact cleanup failed for %s: %w", d.ID, err))
		}
		if err := s.artifactStore.DeleteFormat(ctx, tenantID, appName, d.ID, "cwasm"); err != nil {
			delErr = errors.Join(delErr,
				fmt.Errorf("cwasm artifact cleanup failed for %s: %w", d.ID, err))
		}
	}
	return delErr
}

// CreateIfNotExists creates an app if it doesn't already exist.
// Idempotent — safe to call multiple times.
// Returns ErrMaxAppsQuotaExceeded if the tenant has reached their MaxApps limit.
func (s *AppService) CreateIfNotExists(ctx context.Context, tenantID, appName string) error {
	if !IsValidAppName(appName) {
		return fmt.Errorf("invalid app name: %s", appName)
	}

	var err error // outer-scope for InsertIfNotExists call below

	// Serialize concurrent creates for the same tenant via SELECT FOR UPDATE.
	// This prevents a TOCTOU race where two simultaneous creates both count N
	// apps, both pass the quota check, and the tenant ends up with N+1 apps.
	if s.db != nil {
		tx, err := s.db.BeginTxx(ctx, nil)
		if err != nil {
			return fmt.Errorf("beginning transaction: %w", err)
		}
		defer func() {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				log.Printf("app service: failed to rollback transaction: %v", rollbackErr)
			}
		}()

		// Acquire a row lock on the tenant row. The boolean result is unused;
		// only the lock (held until tx.Commit) matters for serializing concurrent creates.
		var tenantExists bool
		err = tx.GetContext(ctx, &tenantExists, `SELECT true FROM tenants WHERE id = $1 FOR UPDATE`, tenantID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("tenant not found")
			}
			return fmt.Errorf("locking tenant: %w", err)
		}

		quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("getting quota: %w", err)
		}
		count, err := s.appRepo.CountByTenant(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("counting apps: %w", err)
		}
		if quota != nil && count >= quota.MaxApps {
			return ErrMaxAppsQuotaExceeded
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("committing transaction: %w", err)
		}
	} else {
		// No DB available (test path) — check quota without the lock.
		quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("getting quota: %w", err)
		}
		count, err := s.appRepo.CountByTenant(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("counting apps: %w", err)
		}
		if quota != nil && count >= quota.MaxApps {
			return ErrMaxAppsQuotaExceeded
		}
	}

	app := &domain.App{
		ID:          "a_" + uuid.New().String(),
		TenantID:    tenantID,
		Name:        appName,
		Description: nil,
		CreatedAt:   time.Now(),
	}
	// InsertIfNotExists uses INSERT ... ON CONFLICT DO NOTHING — inherently idempotent.
	_, err = s.appRepo.InsertIfNotExists(ctx, app)
	if err != nil {
		return fmt.Errorf("creating app: %w", err)
	}
	return nil
}

// DeleteIfNoDeployments removes the apps row for (tenantID, appName)
// only when zero deployments exist for that app. Used by
// DeploymentService.Deploy as a compensating write when the first
// deploy of an app fails at the artifact save step — CreateIfNotExists
// has already inserted the apps row, and we don't want to leave it
// orphaned with zero deployments.
//
// The conditional DELETE is safe to call concurrently with other
// deploys: if any deployment exists (succeeded, failed, or in
// flight), the call is a no-op. Best-effort — callers should log
// the error and proceed, not fail the request.
func (s *AppService) DeleteIfNoDeployments(ctx context.Context, tenantID, appName string) (bool, error) {
	return s.appRepo.DeleteIfNoDeployments(ctx, tenantID, appName)
}

// Sentinel errors for L4 port allocation (issue #548).
var (
	// ErrL4PortRangeExhausted is returned by AllocateL4Port when every
	// port in [l4PortRangeStart, l4PortRangeEnd] is already allocated.
	// The operator should widen the range or reduce concurrent L4 apps.
	ErrL4PortRangeExhausted = fmt.Errorf("L4 public port range exhausted")
)

// GetL4Port returns the persisted L4 public port for (tenantID,
// appName), or (0, ErrAppNotFound) if the app does not exist.
// Returns (0, nil) when the app exists but has no allocated port
// yet (e.g. HTTP-only app, or L4 app that hasn't been activated
// since the l4_public_port column was added).
//
// Issue #548. The handler layer maps (0, ErrAppNotFound) to 404 and
// (port, nil) to {"public_port": port}; (0, nil) maps to 404 too
// because there's nothing meaningful to advertise.
func (s *AppService) GetL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	if !IsValidAppName(appName) {
		return 0, fmt.Errorf("invalid app name: %s", appName)
	}
	port, err := s.appRepo.GetL4Port(ctx, tenantID, appName)
	if err == sql.ErrNoRows {
		return 0, ErrAppNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("getting L4 port: %w", err)
	}
	return port, nil
}

// AllocateL4Port atomically assigns a free port from
// [l4PortRangeStart, l4PortRangeEnd] to (tenantID, appName) and
// persists it on the apps row. If the app already has a port, the
// existing port is returned unchanged (idempotent — calling twice
// for the same app is safe and does not hand out a second port).
// Returns ErrAppNotFound when the app does not exist and
// ErrL4PortRangeExhausted when no free port is available.
//
// Allocation strategy: walk the range from l4PortRangeStart upward,
// skipping ports already taken (queried once via
// AllocatedL4Ports). The result is persisted via AllocateL4Port
// which uses an UPDATE … WHERE l4_public_port IS NULL guard, so
// two concurrent Allocate calls converge to the same winner (the
// one whose UPDATE matched; the loser re-reads and returns the
// winner's port).
//
// Issue #548.
func (s *AppService) AllocateL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	if !IsValidAppName(appName) {
		return 0, fmt.Errorf("invalid app name: %s", appName)
	}
	// Fast path: app already has a port.
	existing, err := s.appRepo.GetL4Port(ctx, tenantID, appName)
	if err == sql.ErrNoRows {
		return 0, ErrAppNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("checking existing L4 port: %w", err)
	}
	if existing != 0 {
		return existing, nil
	}

	// Slow path: pick a free port from the configured range.
	allocated, err := s.appRepo.AllocatedL4Ports(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing allocated L4 ports: %w", err)
	}
	start := s.l4PortRangeStart
	end := s.l4PortRangeEnd
	if start == 0 {
		start = 31000
	}
	if end == 0 || end < start {
		end = start + 999
	}
	// Walk from start..end. The range is bounded so the loop
	// terminates with ErrL4PortRangeExhausted on a saturated fleet.
	for port := start; ; port++ {
		if port > end {
			return 0, ErrL4PortRangeExhausted
		}
		p := uint16(port)
		if _, taken := allocated[p]; taken {
			continue
		}
		persisted, err := s.appRepo.AllocateL4Port(ctx, tenantID, appName, p)
		if err == sql.ErrNoRows {
			// App was deleted between the GetL4Port and AllocateL4Port
			// calls — bail out.
			return 0, ErrAppNotFound
		}
		if err != nil {
			return 0, fmt.Errorf("allocating L4 port %d: %w", p, err)
		}
		// persisted is the port that ended up in the column. It
		// matches `p` when we won the race; it can be a different
		// port (from a racing caller) when we lost. Either way,
		// it's the persisted value to advertise.
		return persisted, nil
	}
}

// ReleaseL4Port clears the L4 public port assignment for
// (tenantID, appName). Idempotent — safe to call when no port
// was ever allocated or when the apps row no longer exists
// (the repo returns nil in both cases).
//
// Issue #548. Called by the deploy path's compensating-write
// sequence and by future app-delete flows that need to free the
// port for the next app without leaving an orphan allocation.
func (s *AppService) ReleaseL4Port(ctx context.Context, tenantID, appName string) error {
	if !IsValidAppName(appName) {
		return fmt.Errorf("invalid app name: %s", appName)
	}
	return s.appRepo.ReleaseL4Port(ctx, tenantID, appName)
}
