package repository

import (
	"context"
	"database/sql"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
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
	query := `INSERT INTO deployments (id, tenant_id, app_name, status, hash, created_at) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := r.db.ExecContext(ctx, query, d.ID, d.TenantID, d.AppName, d.Status, d.Hash, d.CreatedAt)
	return err
}

func (r *DeploymentRepository) GetByID(ctx context.Context, id string) (*domain.Deployment, error) {
	var d domain.Deployment
	query := `SELECT id, tenant_id, app_name, status, hash, created_at FROM deployments WHERE id = $1`
	err := r.db.GetContext(ctx, &d, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &d, err
}

func (r *DeploymentRepository) ListByApp(ctx context.Context, tenantID, appName string) ([]domain.Deployment, error) {
	var deployments []domain.Deployment
	query := `SELECT id, tenant_id, app_name, status, hash, created_at FROM deployments WHERE tenant_id = $1 AND app_name = $2 ORDER BY created_at DESC`
	err := r.db.SelectContext(ctx, &deployments, query, tenantID, appName)
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
