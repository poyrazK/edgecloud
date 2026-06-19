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
