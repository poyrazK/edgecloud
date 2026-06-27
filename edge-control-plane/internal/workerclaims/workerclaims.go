// Package workerclaims defines the JWT claims shape for worker
// tokens. Lives in its own package (not `middleware` or `service`)
// so both can depend on it without creating an import cycle: the
// minting code (`service`) needs the type to construct tokens, and
// the verifying code (`middleware`) needs it to parse them.
package workerclaims

import "github.com/golang-jwt/jwt/v5"

// WorkerClaims is the JWT claims shape carried by worker tokens.
// Wire-compatible with `edge_worker::auth::WorkerClaims` on the Rust
// side; the two structures must stay in lockstep (the Rust
// `serde(rename_all)` defaults match the JSON tags here).
//
// `iss`/`exp`/`iat`/`jti` are standard JWT claims. `worker_id`,
// `tenant_id`, `region`, and `apps` are worker-specific. The Go
// control plane reads worker_id, tenant_id, and apps; `region`
// and `jti` are informational and ignored by `VerifyWorkerJWT`.
// `jti` (random per-token) gives replay protection and guarantees
// each Mint produces a unique token even within the same second.
type WorkerClaims struct {
	jwt.RegisteredClaims
	WorkerID string   `json:"worker_id"`
	TenantID string   `json:"tenant_id"`
	Region   string   `json:"region"`
	Apps     []string `json:"apps"`
}
