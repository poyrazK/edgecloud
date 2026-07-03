package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// appRepoInterface defines the app repository methods used by AppService.
type appRepoInterface interface {
	Create(ctx context.Context, app *domain.App) error
	Get(ctx context.Context, tenantID, appName string) (*domain.App, error)
	List(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error)
	CountByTenant(ctx context.Context, tenantID string) (int, error)
	AtomicDelete(ctx context.Context, tenantID, appName string) (bool, error)
	InsertIfNotExists(ctx context.Context, app *domain.App) (bool, error)
	Update(ctx context.Context, app *domain.App) error
	// GetForUpdate locks the (tenant, app) row with `SELECT … FOR UPDATE`.
	// Added for the v2 quota-race fix in AddDomain (issue #83 second-pass
	// review); the lock is held for the caller's tx lifetime.
	GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.App, error)
	DeleteIfNoDeployments(ctx context.Context, tenantID, appName string) (bool, error)
}

// AppService handles app business logic.
type AppService struct {
	db            *sqlx.DB
	appRepo       appRepoInterface
	appEnvRepo    *repository.AppEnvRepository
	activeRepo    *repository.ActiveDeploymentRepository
	deployRepo    *repository.DeploymentRepository
	artifactStore storage.ArtifactStore
	quotaRepo     quotaRepoInterface
}

func NewAppService(
	db *sqlx.DB,
	appRepo *repository.AppRepository,
	deploymentRepo *repository.DeploymentRepository,
	activeRepo *repository.ActiveDeploymentRepository,
	appEnvRepo *repository.AppEnvRepository,
	artifactStore storage.ArtifactStore,
	quotaRepo *repository.QuotaRepository,
) *AppService {
	return &AppService{
		db:            db,
		appRepo:       appRepo,
		activeRepo:    activeRepo,
		appEnvRepo:    appEnvRepo,
		deployRepo:    deploymentRepo,
		artifactStore: artifactStore,
		quotaRepo:     quotaRepo,
	}
}

// Sentinel errors.
var ErrAppAlreadyExists = fmt.Errorf("app already exists")
var ErrMaxAppsQuotaExceeded = fmt.Errorf("max apps reached for tenant")

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

// List returns apps for a tenant with pagination.
func (s *AppService) List(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error) {
	return s.appRepo.List(ctx, tenantID, limit, offset)
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
var ErrAppNotFound = fmt.Errorf("app not found")

func (s *AppService) Delete(ctx context.Context, tenantID, appName string) error {
	deleted, err := s.appRepo.AtomicDelete(ctx, tenantID, appName)
	if err != nil {
		return fmt.Errorf("deleting app: %w", err)
	}
	if !deleted {
		return ErrAppNotFound
	}

	// Delete artifact files before cascade — os.Remove is idempotent (no error if absent).
	// Collect errors so caller knows if cleanup failed.
	var delErr error
	if s.artifactStore != nil {
		deployments, err := s.deployRepo.ListByApp(ctx, tenantID, appName)
		if err != nil {
			log.Printf("warning: failed to list deployments for artifact cleanup: %v", err)
		} else {
			for _, d := range deployments {
				if err := s.artifactStore.Delete(ctx, tenantID, appName, d.ID); err != nil {
					delErr = fmt.Errorf("artifact cleanup failed for %s: %w", d.ID, err)
				}
			}
		}
	}

	// Cascade deletes run in a transaction so they either all succeed or all fail.
	err = repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		appEnvRepo := s.appEnvRepo.WithTx(tx)
		activeRepo := s.activeRepo.WithTx(tx)
		deployRepo := s.deployRepo.WithTx(tx)

		if err := appEnvRepo.DeleteByApp(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting app env: %w", err)
		}
		if err := activeRepo.Delete(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting active deployment: %w", err)
		}
		if err := deployRepo.DeleteByApp(ctx, tenantID, appName); err != nil {
			return fmt.Errorf("deleting deployments: %w", err)
		}
		return nil
	})
	if err != nil {
		// Log but don't fail — app row already deleted above.
		// In production, consider a background reconciler to clean orphaned rows.
		log.Printf("warning: cascade delete partially failed after app deletion: %v", err)
	}
	// Surface artifact deletion errors to the caller. DB deletion has already committed.
	if delErr != nil {
		return delErr
	}
	return nil
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
