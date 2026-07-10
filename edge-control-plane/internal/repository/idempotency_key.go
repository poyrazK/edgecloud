package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// IdempotencyTTL is the lifetime of an idempotency key. Rows
// older than this are treated as expired by Lookup and the
// caller computes fresh-deploy semantics. 24h matches
// Stripe / Square's "standard" idempotency window; it's long
// enough to absorb any plausible CLI retry budget (a few hours
// of flaky network at most) and short enough to bound stale
// replays after major WASM API changes.
const IdempotencyTTL = 24 * time.Hour

// IdempotencyKeyRepo persists the (tenant_id, key) ->
// deployment_id replay cache (migration 026_idempotency_keys,
// issue #52).
//
// The repository is the only writer; reads happen from
// DeploymentService.Deploy at the start of every CLI
// invocation carrying the Idempotency-Key header.
type IdempotencyKeyRepo struct {
	db DBTX
}

func NewIdempotencyKeyRepo(db *sqlx.DB) *IdempotencyKeyRepo {
	return &IdempotencyKeyRepo{db: db}
}

// WithTx returns a new IdempotencyKeyRepo using the supplied
// transaction so the row's INSERT and the deployments row
// INSERT can be tied together (used in commit 2 where the
// insert becomes part of the deployment transactional path).
func (r *IdempotencyKeyRepo) WithTx(tx *sqlx.Tx) *IdempotencyKeyRepo {
	return &IdempotencyKeyRepo{db: tx}
}

// Lookup returns the cached key for the given (tenant_id, key)
// pair, treating rows older than IdempotencyTTL as expired and
// returning (nil, nil) so the caller computes fresh-deploy
// semantics. A genuine miss (no row) and an expired row are
// indistinguishable on the wire — both are "no replay".
//
// Errors:
//   - sql.ErrNoRows is converted to (nil, nil); anything else
//     bubbles up unaltered.
func (r *IdempotencyKeyRepo) Lookup(ctx context.Context, tenantID, key string) (*domain.IdempotencyKey, error) {
	var k domain.IdempotencyKey
	// TTL is enforced via `NOW() - make_interval(secs => $3)` so
	// the cutoff is recomputed per-query instead of frozen at
	// repo boot. Passing IdempotencyTTL as seconds keeps the
	// SQL stable across deploys of a TTL config change.
	query := `
        SELECT tenant_id, key, deployment_id, request_sha256, created_at
          FROM idempotency_keys
         WHERE tenant_id = $1 AND key = $2
           AND created_at > NOW() - make_interval(secs => $3)`
	err := r.db.GetContext(ctx, &k, query, tenantID, key, int64(IdempotencyTTL.Seconds()))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &k, nil
}

// Insert records the (tenant, key) -> deployment_id mapping.
// Idempotent via ON CONFLICT (tenant_id, key) DO NOTHING —
// concurrent retries with the same key won't error, and the
// first winner's deployment_id wins (subsequent retry hits
// the original row on its next lookup).
//
// Called by DeploymentService.Deploy after the deployments
// row INSERT commits.
func (r *IdempotencyKeyRepo) Insert(ctx context.Context, k *domain.IdempotencyKey) error {
	query := `
        INSERT INTO idempotency_keys (tenant_id, key, deployment_id, request_sha256, created_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (tenant_id, key) DO NOTHING`
	_, err := r.db.ExecContext(ctx, query, k.TenantID, k.Key, k.DeploymentID, k.RequestSHA256)
	return err
}
