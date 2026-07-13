package domain

// TenantRateLimitRequest is the admin PUT body for
// PUT /api/v1/admin/tenants/{tenantID}/rate-limit (issue #305). All
// fields are optional in the sense that 0 means "unset / admin-cleared"
// (skips the cap check); the handler validates that all values are
// non-negative before reaching the repo. Negative values are rejected
// at the handler boundary — the renderer treats < 0 as the
// "unlimited" sentinel, which is meaningful for tenants on the
// enterprise tier and would otherwise be silently dropped on a 0
// input.
//
// Field layout matches the new columns on the quotas table added in
// migration 032_tenant_rate_limits.up.sql — same names, same sentinel
// semantics, so a successful admin PUT round-trips through the
// internal read endpoint without surprise.
type TenantRateLimitRequest struct {
	// RPS is the per-tenant request-per-second cap. Renders today
	// as a per-tenant rate_limit route in Caddy JSON
	// (sub-feature #1).
	RPS int32 `json:"rps"`
	// Burst is the per-tenant burst allowance paired with RPS. 0
	// falls back to RPS at the ingress renderer.
	Burst int32 `json:"burst"`
	// ConcurrentLimit caps in-flight requests per tenant
	// (sub-feature #2). Render layer deferred.
	ConcurrentLimit int32 `json:"concurrent_limit"`
	// BandwidthBPS caps per-tenant bytes/sec (sub-feature #3).
	// Render layer deferred — needs Caddy 2.8+ bandwidth field.
	BandwidthBPS int64 `json:"bandwidth_bps"`
}

// TenantRateLimitResponse is the wire shape returned by the ingress
// fetcher's GET /api/v1/internal/rate-limit/{tenantID}. Mirrors the
// columns on the quotas row so the ingress TenantRateLimitCache can
// populate without re-deriving any sentinel semantics. All-zero values
// mean "no caps on any axis"; the cache treats those rows as
// "feature disabled for this tenant" and the renderer skips emitting
// a rate_limit route (fail-open — same shape as the quota 402 cache
// at issue #420).
//
// `db:` tags mirror the underlying column names so QuotaRepository.GetRateLimit
// can scan the SELECT result directly into this struct via sqlx.GetContext.
// Without them, sqlx errors with "missing destination name tenant_id".
type TenantRateLimitResponse struct {
	TenantID        string `db:"tenant_id"        json:"tenant_id"`
	RPS             int32  `db:"tenant_rate_limit_rps"     json:"rps"`
	Burst           int32  `db:"tenant_rate_limit_burst"   json:"burst"`
	ConcurrentLimit int32  `db:"tenant_concurrent_limit"   json:"concurrent_limit"`
	BandwidthBps    int64  `db:"tenant_bandwidth_bps"      json:"bandwidth_bps"`
}
