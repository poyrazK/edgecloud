package domain

import "time"

// IdempotencyKey is one row of the idempotency_keys table
// (migration 026, issue #52). It maps an authenticated tenant's
// Idempotency-Key header value to the deployment_id that should be
// returned on replay.
//
// Lifecycle:
//   - INSERTed by DeploymentService.Deploy after the deployments
//     row INSERT commits, so a duplicate (tenant, key) is a no-op
//     via ON CONFLICT.
//   - Read by DeploymentService.Deploy at the start of the same
//     method to short-circuit a retry on a transient network
//     error, returning the original deployment_id with the same
//     body bytes.
//   - Cascades on DELETE deployments(id), so operator-initiated
//     cleanup of a deployment also clears its replay cache.
//
// Lookup TTL (24h) is enforced by the repository — see
// IdempotencyKeyRepo.Lookup — not via a sweeper job. Rows older
// than the TTL return as "fresh" (nil, nil), so a row sitting
// past its expiry never replays an ancient deploy.
type IdempotencyKey struct {
	TenantID      string    `db:"tenant_id"`
	Key           string    `db:"key"`
	DeploymentID  string    `db:"deployment_id"`
	RequestSHA256 [32]byte  `db:"request_sha256"`
	CreatedAt     time.Time `db:"created_at"`
}

// ActiveDeploymentIdempotencyKey is one row of the
// active_deployment_idempotency_keys table (migration 031, issue #439).
// It maps an authenticated tenant's Idempotency-Key header value to
// the (app_name, deployment_id) tuple that was originally activated,
// so a retry of the activate / promote / rollback request can be
// short-circuited without enqueueing a second task_update outbox
// row.
//
// This is the activate-side mirror of IdempotencyKey above — the
// difference is that issue #52's cache pins the deployment row
// that *was created* by Deploy, while this cache pins the
// TaskMessage that *was published* by Activate. A second activate
// under the same key must return the same publish semantics as the
// first call (200 OK, no duplicate task_update); a second activate
// under the same key but with a different (app_name, deployment_id)
// target is a caller bug and returns 422 ErrIdempotencyKeyMismatch.
//
// Lifecycle:
//   - INSERTed by DeploymentService.activateDeployment (and the
//     symmetric RollbackDeployment) AFTER the outbox row INSERT,
//     inside the same tx, so a tx rollback also rolls back the
//     cache row. ON CONFLICT DO NOTHING makes a concurrent retry
//     with the same key a no-op once the first writer wins.
//   - Read by the same methods AFTER lockTenantForUpdate (so a
//     replay against a disabled tenant still returns 409) and
//     BEFORE txActive.GetForUpdate (so the replay path skips the
//     row-level FOR UPDATE — replays don't contend with fresh
//     activates on the active_deployments row).
//   - Cascades on DELETE deployments(id), so an operator-initiated
//     deployment GC also clears any dangling cache rows.
//
// Lookup TTL (24h) mirrors IdempotencyTTL in
// repository/idempotency_key.go and is enforced inside the
// repository's Lookup, not via a sweeper job.
type ActiveDeploymentIdempotencyKey struct {
	TenantID       string    `db:"tenant_id"`
	IdempotencyKey string    `db:"idempotency_key"`
	AppName        string    `db:"app_name"`
	DeploymentID   string    `db:"deployment_id"`
	CreatedAt      time.Time `db:"created_at"`
}
