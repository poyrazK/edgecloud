package repository

import (
	"context"
	"database/sql"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// ActiveDeploymentRepository handles active deployment mappings.
type ActiveDeploymentRepository struct {
	db DBTX
}

func NewActiveDeploymentRepository(db *sqlx.DB) *ActiveDeploymentRepository {
	return &ActiveDeploymentRepository{db: db}
}

// WithTx returns a new ActiveDeploymentRepository using the provided transaction.
func (r *ActiveDeploymentRepository) WithTx(tx *sqlx.Tx) *ActiveDeploymentRepository {
	return &ActiveDeploymentRepository{db: tx}
}

func (r *ActiveDeploymentRepository) Set(ctx context.Context, ad *domain.ActiveDeployment) error {
	query := `INSERT INTO active_deployments (tenant_id, app_name, deployment_id, last_good_deployment_id) VALUES ($1, $2, $3, $4) ON CONFLICT (tenant_id, app_name) DO UPDATE SET deployment_id = $3, last_good_deployment_id = $4`
	_, err := r.db.ExecContext(ctx, query, ad.TenantID, ad.AppName, ad.DeploymentID, ad.LastGoodDeploymentID)
	return err
}

func (r *ActiveDeploymentRepository) Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	var ad domain.ActiveDeployment
	query := `SELECT tenant_id, app_name, deployment_id, last_good_deployment_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`
	err := r.db.GetContext(ctx, &ad, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &ad, err
}

// GetForUpdate reads the active_deployments row for (tenant, app) inside a
// transaction with a row-level lock so the caller can swap
// deployment_id ↔ last_good_deployment_id atomically. Pair with WithTx.
func (r *ActiveDeploymentRepository) GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	var ad domain.ActiveDeployment
	query := `SELECT tenant_id, app_name, deployment_id, last_good_deployment_id FROM active_deployments WHERE tenant_id = $1 AND app_name = $2 FOR UPDATE`
	err := r.db.GetContext(ctx, &ad, query, tenantID, appName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &ad, err
}

func (r *ActiveDeploymentRepository) Delete(ctx context.Context, tenantID, appName string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM active_deployments WHERE tenant_id = $1 AND app_name = $2`, tenantID, appName)
	return err
}

func (r *ActiveDeploymentRepository) ListByTenant(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error) {
	var ads []domain.ActiveDeployment
	query := `SELECT tenant_id, app_name, deployment_id, last_good_deployment_id FROM active_deployments WHERE tenant_id = $1`
	err := r.db.SelectContext(ctx, &ads, query, tenantID)
	return ads, err
}
