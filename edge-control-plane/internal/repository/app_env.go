package repository

import (
	"context"
	"database/sql"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// AppEnvRepository handles app environment variable data access.
type AppEnvRepository struct {
	db DBTX
}

func NewAppEnvRepository(db *sqlx.DB) *AppEnvRepository {
	return &AppEnvRepository{db: db}
}

// WithTx returns a new AppEnvRepository using the provided transaction.
func (r *AppEnvRepository) WithTx(tx *sqlx.Tx) *AppEnvRepository {
	return &AppEnvRepository{db: tx}
}

func (r *AppEnvRepository) Set(ctx context.Context, env *domain.AppEnv) error {
	query := `INSERT INTO app_env (tenant_id, app_name, env_key, env_value) VALUES ($1, $2, $3, $4) ON CONFLICT (tenant_id, app_name, env_key) DO UPDATE SET env_value = $4`
	_, err := r.db.ExecContext(ctx, query, env.TenantID, env.AppName, env.EnvKey, env.EnvValue)
	return err
}

func (r *AppEnvRepository) Get(ctx context.Context, tenantID, appName, key string) (*domain.AppEnv, error) {
	var env domain.AppEnv
	query := `SELECT tenant_id, app_name, env_key, env_value FROM app_env WHERE tenant_id = $1 AND app_name = $2 AND env_key = $3`
	err := r.db.GetContext(ctx, &env, query, tenantID, appName, key)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &env, err
}

func (r *AppEnvRepository) List(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
	var envs []domain.AppEnv
	query := `SELECT tenant_id, app_name, env_key, env_value FROM app_env WHERE tenant_id = $1 AND app_name = $2 ORDER BY env_key`
	err := r.db.SelectContext(ctx, &envs, query, tenantID, appName)
	return envs, err
}

func (r *AppEnvRepository) Delete(ctx context.Context, tenantID, appName, key string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM app_env WHERE tenant_id = $1 AND app_name = $2 AND env_key = $3`, tenantID, appName, key)
	return err
}
