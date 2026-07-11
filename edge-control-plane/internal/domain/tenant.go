package domain

import (
	"time"

	"github.com/lib/pq"
)

// Tenant represents a platform customer.
//
// JSON tags are snake_case to match the OpenAPI schema in
// docs/api/openapi.yaml. Without json tags the response would emit
// PascalCase keys ("Plan", "Name") which the schema does not declare.
// The wire shape changed from PascalCase to snake_case in billing v0.
type Tenant struct {
	ID   string `db:"id"   json:"id"`
	Name string `db:"name" json:"name"`
	Plan string `db:"plan" json:"plan"`
	// AllowlistedDestinations is a TEXT[] column. Typed as
	// pq.StringArray (which is []string underneath) so the column
	// scans correctly via lib/pq's Scanner — a bare []string does NOT
	// implement sql.Scanner and would fail on SELECT. The JSON wire
	// format is unchanged because pq.StringArray marshals identically
	// to []string. The repo also wraps writes in pq.Array() for the
	// same reason on the encoding side.
	AllowlistedDestinations pq.StringArray `db:"allowlisted_destinations" json:"allowlisted_destinations"`
	CreatedAt               time.Time      `db:"created_at"               json:"created_at"`
	// DisabledAt is set when the tenant exceeds their outbound bandwidth
	// quota (issue #155). When non-nil, the control plane skips publishing
	// task updates for this tenant and rejects new deployments/activations.
	// Cleared when the billing period resets or an operator manually
	// re-enables the tenant.
	DisabledAt *time.Time `db:"disabled_at" json:"disabled_at,omitempty"`
	// OverageAllowedUntil is set by the admin quota-override endpoint
	// (issue #420) to give a paid tenant a bounded grace window
	// during which the deploy-time cap check is skipped. NULL means
	// "no override" — the regular cap check applies. Past timestamps
	// are equivalent to NULL: the deploy-time check only treats
	// "now < OverageAllowedUntil" as the override-active condition.
	OverageAllowedUntil *time.Time `db:"overage_allowed_until" json:"overage_allowed_until,omitempty"`
}

// IsDisabled returns true if the tenant is currently disabled (disabled_at
// is set in the past).
func (t *Tenant) IsDisabled() bool {
	return t.DisabledAt != nil && !t.DisabledAt.IsZero()
}

// Quota defines resource limits for a tenant.
//
// Sentinel: any Max* value < 0 means "unlimited" at the service layer.
// 0 means "unset / admin-cleared" and the service falls back to defaults.
//
// JSON tags are snake_case to match the OpenAPI schema in docs/api/openapi.yaml.
// UsedOutboundBytes and QuotaPeriodStart were previously returned as PascalCase
// keys (no json tags); the rename to snake_case is part of the billing v0
// wire-shape change.
type Quota struct {
	TenantID                   string `db:"tenant_id"                      json:"tenant_id"`
	MaxDeployments             int    `db:"max_deployments"                json:"max_deployments"`
	MaxApps                    int    `db:"max_apps"                       json:"max_apps"`
	MaxWorkers                 int    `db:"max_workers"                    json:"max_workers"`
	MaxMemoryMB                int    `db:"max_memory_mb"                  json:"max_memory_mb"`
	MaxOutboundMB              int    `db:"max_outbound_mb"                json:"max_outbound_mb"`
	MaxRequestsPerMonth        int    `db:"max_requests_per_month"         json:"max_requests_per_month"`
	MaxResidentSecondsPerMonth int    `db:"max_resident_seconds_per_month" json:"max_resident_seconds_per_month"`
	// MaxComputeMsPerMonth is the fourth metered dimension's cap
	// (issue #555). Bounds the FaaS request-duration total per
	// month — the GB-ms-style axis the Lambda-comparison narrative
	// for #484/#485/#555 hangs on. Sentinel < 0 (e.g. enterprise)
	// means unlimited; == 0 means hard-deny; > 0 is the per-tenant
	// millisecond budget. Backfilled in migration 031 with the
	// same per-plan defaults as MaxResidentSecondsPerMonth but
	// scaled to ms (free=2_592_000_000, pro=7_776_000_000,
	// business=31_104_000_000, enterprise=-1).
	MaxComputeMsPerMonth int   `db:"max_compute_ms_per_month" json:"max_compute_ms_per_month"`
	UsedOutboundBytes    int64 `db:"used_outbound_bytes"      json:"used_outbound_bytes"`
	UsedRequestCount     int64 `db:"used_request_count"       json:"used_request_count"`
	// UsedMemoryMB is the aggregate memory (MiB) currently consumed
	// by the tenant's active deployments (issue #44, part 2).
	// Incremented on activate / promote, decremented on rollback.
	// Unlike UsedOutboundBytes / UsedRequestCount, this counter is
	// NOT month-bounded — the cap is per-tenant-aggregate, not
	// per-month, so the lazy-rollover CASE against QuotaPeriodStart
	// would be wrong. The deploy-time gate rejects a new deploy when
	// UsedMemoryMB + perAppMemory > MaxMemoryMB (with MaxMemoryMB == 0
	// or < 0 falling through to the per-instance hint path).
	UsedMemoryMB        int64 `db:"used_memory_mb"        json:"used_memory_mb"`
	UsedResidentSeconds int64 `db:"used_resident_seconds" json:"used_resident_seconds"`
	// UsedComputeMs accumulates the Handler (FaaS) request-duration
	// total in milliseconds (issue #555, fourth metered dimension).
	// Updated by WorkerService.checkComputeMs from the
	// duration_ms_total heartbeat field — LongRunning apps stamp 0
	// (their resident-time axis is resident_seconds), so this counter
	// advances only when Handler dispatch stamps. Lazy-rollover
	// semantics against QuotaPeriodStart mirror the existing three
	// axes so a tenant whose month just rolled over starts fresh.
	UsedComputeMs    int64     `db:"used_compute_ms" json:"used_compute_ms"`
	QuotaPeriodStart time.Time `db:"quota_period_start"     json:"quota_period_start"`
	// QuotaLockGraceUntil is set by applyTenantDelta on free-tier
	// first-cross of a monthly cap (issue #420). It bounds the
	// request-time 402 — deploys are blocked immediately, but the
	// edge still serves requests until this timestamp expires. After
	// expiry the worker's next heartbeat calls tenantRepo.SetDisabledAt
	// to flip the blast-radius lever, which kills all running apps.
	// Operators clear it via the admin quota-override endpoint.
	QuotaLockGraceUntil *time.Time `db:"quota_lock_grace_until" json:"quota_lock_grace_until,omitempty"`
}

// UsagePct returns the highest usage percentage across the three monthly caps
// (outbound bytes, request count, resident seconds) as a 0–100 value. Returns
// nil when all caps are unlimited (sentinel < 0). The caller is expected to
// wrap this into a response shape with omitempty so unlimited tenants don't
// get a misleading 0.
//
// Resident-seconds was added in issue #485 as the third metered dimension.
// Handler (FaaS) apps do not contribute (the worker stamps
// resident_seconds=None; the CP translates None to 0). The cap check
// fires on LongRunning apps that exceed the monthly uptime budget.
func (q Quota) UsagePct() *float64 {
	outCap := int64(q.MaxOutboundMB) * 1024 * 1024
	reqCap := int64(q.MaxRequestsPerMonth)
	resCap := int64(q.MaxResidentSecondsPerMonth)

	var outPct, reqPct, resPct *float64
	if outCap > 0 {
		v := float64(q.UsedOutboundBytes) / float64(outCap) * 100
		outPct = &v
	}
	if reqCap > 0 {
		v := float64(q.UsedRequestCount) / float64(reqCap) * 100
		reqPct = &v
	}
	if resCap > 0 {
		v := float64(q.UsedResidentSeconds) / float64(resCap) * 100
		resPct = &v
	}
	switch {
	case outPct == nil && reqPct == nil && resPct == nil:
		return nil
	case outPct == nil && reqPct == nil:
		return resPct
	case outPct == nil && resPct == nil:
		return reqPct
	case reqPct == nil && resPct == nil:
		return outPct
	case outPct == nil:
		if *reqPct > *resPct {
			return reqPct
		}
		return resPct
	case reqPct == nil:
		if *outPct > *resPct {
			return outPct
		}
		return resPct
	case resPct == nil:
		if *outPct > *reqPct {
			return outPct
		}
		return reqPct
	}
	best := *outPct
	if *reqPct > best {
		best = *reqPct
	}
	if *resPct > best {
		best = *resPct
	}
	return &best
}

// TenantWithQuota combines tenant and quota data.
type TenantWithQuota struct {
	Tenant
	Quota
}
