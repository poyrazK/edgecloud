package repository

import (
	"context"
	"database/sql"
	"fmt"
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
	if k.HashAlgorithm == "" {
		// Programming error: callers must set HashAlgorithm explicitly.
		// A silent default would mask bugs (e.g. a future migration that
		// wants to insert SHA-256 rows would silently write argon2id).
		return fmt.Errorf("api_key %s: HashAlgorithm must be set (use %q or %q)",
			k.ID, domain.HashAlgorithmArgon2ID, domain.HashAlgorithmSHA256)
	}
	if k.LookupHash == "" {
		// Programming error: callers must compute the SHA-256 lookup hash
		// from the raw key. A row without LookupHash is invisible to
		// AuthenticateRawKey, and the partial UNIQUE index tolerates NULLs
		// so duplicates could pile up unnoticed.
		return fmt.Errorf("api_key %s: LookupHash must be set (SHA-256 hex of raw key)", k.ID)
	}
	query := `INSERT INTO api_keys (id, tenant_id, name, key_hash, lookup_hash, role, created_at, expires_at, hash_algorithm) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	_, err := r.db.ExecContext(ctx, query, k.ID, k.TenantID, k.Name, k.KeyHash, k.LookupHash, k.Role, k.CreatedAt, k.ExpiresAt, k.HashAlgorithm)
	return err
}

func (r *APIKeyRepository) GetByID(ctx context.Context, id string) (*domain.APIKey, error) {
	var k domain.APIKey
	query := `SELECT id, tenant_id, name, key_hash, lookup_hash, role, created_at, last_used, expires_at, hash_algorithm FROM api_keys WHERE id = $1`
	err := r.db.GetContext(ctx, &k, query, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &k, err
}

// GetByLookupHash fetches the key row by its stable SHA-256 lookup hash.
// This is the path AuthenticateRawKey uses to find candidate rows before
// dispatching to the algorithm-specific verifier. (See migration 006.)
func (r *APIKeyRepository) GetByLookupHash(ctx context.Context, lookupHash string) (*domain.APIKey, error) {
	var k domain.APIKey
	query := `SELECT id, tenant_id, name, key_hash, lookup_hash, role, created_at, last_used, expires_at, hash_algorithm FROM api_keys WHERE lookup_hash = $1`
	err := r.db.GetContext(ctx, &k, query, lookupHash)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &k, err
}

func (r *APIKeyRepository) ListByTenant(ctx context.Context, tenantID string) ([]domain.APIKey, error) {
	var keys []domain.APIKey
	query := `SELECT id, tenant_id, name, key_hash, lookup_hash, role, created_at, last_used, expires_at, hash_algorithm FROM api_keys WHERE tenant_id = $1 ORDER BY created_at DESC`
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

// UpdateHashIfAlgorithm atomically overwrites key_hash and hash_algorithm only
// if the row's current hash_algorithm matches currentAlgo. Returns the number
// of rows updated so the caller can detect "another auth won the race".
//
// Used by the lazy-rehash path: only the request whose CAS guard matches
// "sha256" actually writes the new argon2id hash. Concurrent requests whose
// CAS loses silently observe the row in its upgraded state and skip the
// overwrite, avoiding the random-salt ping-pong that would otherwise happen.
func (r *APIKeyRepository) UpdateHashIfAlgorithm(
	ctx context.Context, id, currentAlgo, newHash, newAlgo string,
) (int64, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE api_keys SET key_hash = $3, hash_algorithm = $4 WHERE id = $1 AND hash_algorithm = $2`,
		id, currentAlgo, newHash, newAlgo,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Update updates an existing API key's mutable fields (name, role, expires_at).
func (r *APIKeyRepository) Update(ctx context.Context, k *domain.APIKey) error {
	query := `UPDATE api_keys SET name = $2, role = $3, expires_at = $4 WHERE id = $1`
	_, err := r.db.ExecContext(ctx, query, k.ID, k.Name, k.Role, k.ExpiresAt)
	return err
}
