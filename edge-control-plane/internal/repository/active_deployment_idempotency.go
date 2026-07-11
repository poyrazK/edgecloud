package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// ActiveDeploymentIdempotencyKeyRepo persists the
// (tenant_id, idempotency_key) -> (app_name, deployment_id) replay
// cache for the activate / promote / rollback paths (migration 031,
// issue #439).
//
// The repository is the only writer; reads happen from
// DeploymentService.activateDeployment (and the symmetric
// RollbackDeployment) at the start of every CLI invocation
// carrying the Idempotency-Key header on those endpoints.
type ActiveDeploymentIdempotencyKeyRepo struct {
	db DBTX
}

func NewActiveDeploymentIdempotencyKeyRepo(db *sqlx.DB) *ActiveDeploymentIdempotencyKeyRepo {
	return &ActiveDeploymentIdempotencyKeyRepo{db: db}
}

// WithTx returns a new ActiveDeploymentIdempotencyKeyRepo using
// the supplied transaction so the row's INSERT can be tied to the
// same tx as the active_deployments mutation + outbox INSERT that
// produced it. Used by DeploymentService.activateDeployment in
// commit 2 where the cache row is part of the activate
// transactional path.
func (r *ActiveDeploymentIdempotencyKeyRepo) WithTx(tx *sqlx.Tx) *ActiveDeploymentIdempotencyKeyRepo {
	return &ActiveDeploymentIdempotencyKeyRepo{db: tx}
}

// Lookup returns the cached (app_name, deployment_id) for the
// given (tenant_id, idempotency_key) pair, treating rows older
// than IdempotencyTTL as expired and returning (nil, nil) so the
// caller computes fresh-publish semantics. A genuine miss (no
// row) and an expired row are indistinguishable on the wire —
// both are "no replay".
//
// Errors:
//   - sql.ErrNoRows is converted to (nil, nil); anything else
//     bubbles up unaltered.
func (r *ActiveDeploymentIdempotencyKeyRepo) Lookup(ctx context.Context, tenantID, key string) (*domain.ActiveDeploymentIdempotencyKey, error) {
	var k domain.ActiveDeploymentIdempotencyKey
	// TTL is enforced via `NOW() - make_interval(secs => $3)` so
	// the cutoff is recomputed per-query instead of frozen at
	// repo boot. Passing IdempotencyTTL as seconds keeps the SQL
	// stable across deploys of a TTL config change. The TTL
	// constant is shared with IdempotencyKeyRepo so both caches
	// expire in lockstep.
	query := `
        SELECT tenant_id, idempotency_key, app_name, deployment_id, created_at
          FROM active_deployment_idempotency_keys
         WHERE tenant_id = $1 AND idempotency_key = $2
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

// Insert records the (tenant, key) -> (app_name, deployment_id)
// mapping. Idempotent via ON CONFLICT (tenant_id, idempotency_key)
// DO NOTHING — concurrent retries with the same key won't error,
// and the first winner's (app_name, deployment_id) wins
// (subsequent retries hit the original row on their next lookup).
//
// Called by DeploymentService.activateDeployment AFTER the outbox
// row INSERT commits (inside the same tx) so a rollback of any
// earlier statement also rolls back the cache row.
func (r *ActiveDeploymentIdempotencyKeyRepo) Insert(ctx context.Context, k *domain.ActiveDeploymentIdempotencyKey) error {
	query := `
        INSERT INTO active_deployment_idempotency_keys (tenant_id, idempotency_key, app_name, deployment_id, created_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`
	_, err := r.db.ExecContext(ctx, query, k.TenantID, k.IdempotencyKey, k.AppName, k.DeploymentID)
	return err
}

// DeleteOlderThan deletes cache rows whose created_at is older than
// `now - age`. The cutoff is computed server-side via
// `NOW() - make_interval(secs => $1)` so the DB clock — not the Go
// process clock — is the time authority. Used by
// service.IdempotencyKeyGCService (issue #439 follow-up) to bound
// table growth over a deployment's lifetime: the Lookup-side TTL
// filter makes aged-out rows invisible to the replay path, but they
// still occupy disk and are visited by every INSERT's index update
// without a sweeper.
//
// Returns the number of rows deleted. The caller logs that count for
// operator visibility (mirrors log_gc.go's deleted-row log line).
func (r *ActiveDeploymentIdempotencyKeyRepo) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	query := `DELETE FROM active_deployment_idempotency_keys WHERE created_at <= NOW() - make_interval(secs => $1)`
	res, err := r.db.ExecContext(ctx, query, int64(age.Seconds()))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}
