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
//
// `role` distinguishes the bearer: per-worker tokens carry
// `RoleWorker` (or empty, for backward compatibility with tokens
// minted before this field existed); the long-lived ingress
// service token carries `RoleIngest` so the control plane can
// gate cross-tenant reads (ListDomains, TlsAllowed,
// UpdateDomainStatus) to the poller only — a per-worker JWT
// would otherwise see every tenant's domain mapping. The
// `omitempty` keeps the JSON shape backward-compatible with
// pre-Role tokens.
//
// PR #200 review finding H2: this struct is the single source of
// truth. The verifier (`internal/middleware`) and the minter
// (`internal/service`) both reference it; `middleware.WorkerClaims`
// is a type alias to it so existing call sites
// (`claims := middleware.WorkerClaims{...}`) keep compiling without
// edits. Adding a new claim here automatically threads through both
// minter and verifier, eliminating the silent-drift risk of two
// separately-defined structs.
type WorkerClaims struct {
	jwt.RegisteredClaims
	WorkerID string   `json:"worker_id"`
	TenantID string   `json:"tenant_id"`
	Region   string   `json:"region"`
	Apps     []string `json:"apps"`
	Role     string   `json:"role,omitempty"`
}

// Role constants for the Role claim. Lives on WorkerClaims so the
// minter, verifier, and ingress token issuer all reference the same
// canonical strings.
const (
	// RoleWorker is the default for any worker-issued JWT. May run
	// per-worker endpoints (e.g. /api/internal/download/*,
	// /api/internal/workers/*) but NOT the cross-tenant domain
	// endpoints.
	RoleWorker = "worker"
	// RoleIngest is for the long-lived ingress service token
	// (cmd/api/mint.go). Allows access to the cross-tenant domain
	// endpoints used by the FQDN poller and the v2 Caddy event
	// hook (issue #83).
	RoleIngest = "ingest"
)
