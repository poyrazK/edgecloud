package repository

import (
	"context"
	"database/sql"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// AppRepository handles app data access.
type AppRepository struct {
	db DBTX
}

func NewAppRepository(db *sqlx.DB) *AppRepository {
	return &AppRepository{db: db}
}

// NewAppRepositoryFromDBTX constructs a repository bound to a DBTX
// (either *sqlx.DB or *sqlx.Tx). Used by tests that need a tx-bound
// repo without standing up a full database connection.
func NewAppRepositoryFromDBTX(dbtx DBTX) *AppRepository {
	return &AppRepository{db: dbtx}
}

// WithTx returns a new AppRepository using the provided transaction.
func (r *AppRepository) WithTx(tx *sqlx.Tx) *AppRepository {
	return &AppRepository{db: tx}
}

func (r *AppRepository) Create(ctx context.Context, app *domain.App) error {
	query := `INSERT INTO apps (id, tenant_id, name, description, created_at) VALUES ($1, $2, $3, $4, $5)`
	_, err := r.db.ExecContext(ctx, query, app.ID, app.TenantID, app.Name, app.Description, app.CreatedAt)
	return err
}

func (r *AppRepository) Get(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	var app domain.App
	query := `SELECT id, tenant_id, name, description, created_at FROM apps WHERE tenant_id = $1 AND name = $2`
	err := r.db.GetContext(ctx, &app, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &app, err
}

// GetForUpdate locks the (tenant_id, name) row with `SELECT … FOR UPDATE`
// and returns the row. The lock is held for the lifetime of the
// surrounding transaction; concurrent writers targeting the same
// (tenant, app) block until the transaction commits or rolls back.
//
// Used by service.DomainService.AddDomain to serialize concurrent
// domain inserts against the same app so the per-app quota
// (MaxDomainsPerApp) cannot be overshot by a count-then-insert race.
// The same pattern is used by service.DeploymentService.ActivateDeployment
// on the active_deployments row.
//
// Returns (nil, nil) when no app exists for (tenantID, appName) —
// the service layer maps that to ErrAppNotFound.
func (r *AppRepository) GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	var app domain.App
	query := `SELECT id, tenant_id, name, description, created_at FROM apps WHERE tenant_id = $1 AND name = $2 FOR UPDATE`
	err := r.db.GetContext(ctx, &app, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &app, err
}

func (r *AppRepository) List(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error) {
	var apps []domain.App
	query := `SELECT id, tenant_id, name, description, created_at FROM apps WHERE tenant_id = $1 ORDER BY name LIMIT $2 OFFSET $3`
	err := r.db.SelectContext(ctx, &apps, query, tenantID, limit, offset)
	return apps, err
}

func (r *AppRepository) Delete(ctx context.Context, tenantID, appName string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM apps WHERE tenant_id = $1 AND name = $2`, tenantID, appName)
	return err
}

func (r *AppRepository) Exists(ctx context.Context, tenantID, appName string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM apps WHERE tenant_id = $1 AND name = $2)`
	err := r.db.GetContext(ctx, &exists, query, tenantID, appName)
	return exists, err
}

// AtomicDelete deletes an app atomically and returns whether a row was deleted.
func (r *AppRepository) AtomicDelete(ctx context.Context, tenantID, appName string) (bool, error) {
	var deleted bool
	err := r.db.GetContext(ctx, &deleted,
		`DELETE FROM apps WHERE tenant_id = $1 AND name = $2 RETURNING true`,
		tenantID, appName)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return deleted, err
}

// DeleteIfNoDeployments removes the apps row for (tenantID, appName)
// only if it has zero deployments. Used as the compensating write
// when DeploymentService.Deploy's artifact save fails on the very
// first deploy of an app: CreateIfNotExists has just inserted an
// apps row, the deployments row was created next, and then the
// artifact save failed — both rows have been rolled back, but the
// apps row remains orphaned unless we also delete it conditionally
// on "no deployments ever succeeded for this app."
//
// The NOT EXISTS subquery makes the operation safe to call
// concurrently with other deploys: if a concurrent successful
// deploy exists (or appears during the call), the DELETE is a
// no-op. Returns whether a row was deleted.
func (r *AppRepository) DeleteIfNoDeployments(ctx context.Context, tenantID, appName string) (bool, error) {
	var deleted bool
	err := r.db.GetContext(ctx, &deleted,
		`DELETE FROM apps
		 WHERE tenant_id = $1 AND name = $2
		   AND NOT EXISTS (
		       SELECT 1 FROM deployments
		       WHERE tenant_id = $1 AND app_name = $2
		   )
		 RETURNING true`,
		tenantID, appName)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return deleted, err
}

// Update updates mutable fields of an existing app.
func (r *AppRepository) Update(ctx context.Context, app *domain.App) error {
	query := `UPDATE apps SET description = $1 WHERE id = $2 AND tenant_id = $3`
	_, err := r.db.ExecContext(ctx, query, app.Description, app.ID, app.TenantID)
	return err
}

// CountByTenant returns the number of apps for a tenant.
func (r *AppRepository) CountByTenant(ctx context.Context, tenantID string) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM apps WHERE tenant_id = $1`
	err := r.db.GetContext(ctx, &count, query, tenantID)
	return count, err
}

// InsertIfNotExists inserts an app only if no app with this tenant+name exists.
// Returns true if a row was inserted, false if it already existed.
func (r *AppRepository) InsertIfNotExists(ctx context.Context, app *domain.App) (bool, error) {
	var inserted bool
	query := `
		INSERT INTO apps (id, tenant_id, name, description, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, name) DO NOTHING
		RETURNING (xmax = 0)`
	err := r.db.GetContext(ctx, &inserted, query, app.ID, app.TenantID, app.Name, app.Description, app.CreatedAt)
	return inserted, err
}

// GetRateLimit returns the per-app rate limit override for (tenantID, appName).
// Returns (nil, nil) when the app does not exist.
func (r *AppRepository) GetRateLimit(ctx context.Context, tenantID, appName string) (*domain.AppRateLimit, error) {
	var rl domain.AppRateLimit
	query := `SELECT rate_limit_rps AS rps, rate_limit_burst AS burst FROM apps WHERE tenant_id = $1 AND name = $2`
	err := r.db.GetContext(ctx, &rl, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &rl, err
}
