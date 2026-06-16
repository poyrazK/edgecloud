package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// APIKeyRepository handles API key data access.
type APIKeyRepository struct {
	db DBTX
}

func NewAPIKeyRepository(db *sqlx.DB) *APIKeyRepository {
	return &APIKeyRepository{db: db}
}

// WithTx returns a new APIKeyRepository using the provided transaction.
func (r *APIKeyRepository) WithTx(tx *sqlx.Tx) *APIKeyRepository {
	return &APIKeyRepository{db: tx}
}

func (r *APIKeyRepository) Create(ctx context.Context, k *domain.APIKey) error {
	query := `INSERT INTO api_keys (id, tenant_id, name, key_hash, role, created_at, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err := r.db.ExecContext(ctx, query, k.ID, k.TenantID, k.Name, k.KeyHash, k.Role, k.CreatedAt, k.ExpiresAt)
	return err
}

func (r *APIKeyRepository) GetByID(ctx context.Context, id string) (*domain.APIKey, error) {
	var k domain.APIKey
	query := `SELECT id, tenant_id, name, key_hash, role, created_at, last_used, expires_at FROM api_keys WHERE id = $1`
	err := r.db.GetContext(ctx, &k, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &k, err
}

func (r *APIKeyRepository) GetByHash(ctx context.Context, hash string) (*domain.APIKey, error) {
	var k domain.APIKey
	query := `SELECT id, tenant_id, name, key_hash, role, created_at, last_used, expires_at FROM api_keys WHERE key_hash = $1`
	err := r.db.GetContext(ctx, &k, query, hash)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &k, err
}

func (r *APIKeyRepository) ListByTenant(ctx context.Context, tenantID string) ([]domain.APIKey, error) {
	var keys []domain.APIKey
	query := `SELECT id, tenant_id, name, key_hash, role, created_at, last_used, expires_at FROM api_keys WHERE tenant_id = $1 ORDER BY created_at DESC`
	err := r.db.SelectContext(ctx, &keys, query, tenantID)
	return keys, err
}

func (r *APIKeyRepository) Delete(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM api_keys WHERE id = $1`, id)
	return err
}

func (r *APIKeyRepository) UpdateLastUsed(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE api_keys SET last_used = $2 WHERE id = $1`, id, time.Now())
	return err
}
