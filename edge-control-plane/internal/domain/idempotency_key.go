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
