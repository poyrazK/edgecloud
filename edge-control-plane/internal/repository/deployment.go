package repository

import (
	"context"
	"database/sql"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// DeploymentRepository handles deployment data access.
type DeploymentRepository struct {
	db DBTX
}

func NewDeploymentRepository(db *sqlx.DB) *DeploymentRepository {
	return &DeploymentRepository{db: db}
}

// WithTx returns a new DeploymentRepository using the provided transaction.
func (r *DeploymentRepository) WithTx(tx *sqlx.Tx) *DeploymentRepository {
	return &DeploymentRepository{db: tx}
}

func (r *DeploymentRepository) Create(ctx context.Context, d *domain.Deployment) error {
	// `regions` is `NOT NULL DEFAULT '{}'` in the schema (migration 008).
	// sqlx/pq maps a Go nil slice to SQL NULL, which would violate the
	// constraint — so the service layer is responsible for passing a
	// non-nil slice (it does, via the regions-with-default path in
	// `Deploy` and `ActivateDeployment`). Defensive: treat nil as
	// empty here so a future caller that forgets the invariant gets
	// `[]` on disk, not a constraint error. The field is
	// pq.StringArray (which is []string underneath) so the nil check
	// and the pq.Array() marshal both work as for a plain slice.
	regions := d.Regions
	if regions == nil {
		regions = pq.StringArray{}
	}
	query := `INSERT INTO deployments (id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	_, err := r.db.ExecContext(ctx, query, d.ID, d.TenantID, d.AppName, d.Status, d.Hash, pq.Array(regions), d.CreatedAt, d.AutoRollbackEnabled)
	return err
}

func (r *DeploymentRepository) GetByID(ctx context.Context, id string) (*domain.Deployment, error) {
	var d domain.Deployment
	query := `SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled FROM deployments WHERE id = $1`
	err := r.db.GetContext(ctx, &d, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &d, err
}

func (r *DeploymentRepository) ListByApp(ctx context.Context, tenantID, appName string) ([]domain.Deployment, error) {
	var deployments []domain.Deployment
	query := `SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled FROM deployments WHERE tenant_id = $1 AND app_name = $2 ORDER BY created_at DESC`
	err := r.db.SelectContext(ctx, &deployments, query, tenantID, appName)
	return deployments, err
}

func (r *DeploymentRepository) ListByAppPaginated(ctx context.Context, tenantID, appName string, limit, offset int) ([]domain.Deployment, error) {
	var deployments []domain.Deployment
	query := `SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled FROM deployments WHERE tenant_id = $1 AND app_name = $2 ORDER BY created_at DESC LIMIT $3 OFFSET $4`
	err := r.db.SelectContext(ctx, &deployments, query, tenantID, appName, limit, offset)
	return deployments, err
}

func (r *DeploymentRepository) CountByApp(ctx context.Context, tenantID, appName string) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM deployments WHERE tenant_id = $1 AND app_name = $2`
	err := r.db.GetContext(ctx, &count, query, tenantID, appName)
	return count, err
}

func (r *DeploymentRepository) UpdateStatus(ctx context.Context, id, status string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE deployments SET status = $2 WHERE id = $1`, id, status)
	return err
}

func (r *DeploymentRepository) DeleteByApp(ctx context.Context, tenantID, appName string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM deployments WHERE tenant_id = $1 AND app_name = $2`, tenantID, appName)
	return err
}

// DeleteByID removes a deployment row by its ID. Idempotent on missing
// row: returns nil if no row was deleted. Used as the compensating
// write in the Create-then-Save services (Migrate, MigrateTree,
// Deploy) when the artifact save fails after the row was inserted.
func (r *DeploymentRepository) DeleteByID(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM deployments WHERE id = $1`, id)
	return err
}
