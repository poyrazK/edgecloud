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
	TenantID            string    `db:"tenant_id"              json:"tenant_id"`
	MaxDeployments      int       `db:"max_deployments"        json:"max_deployments"`
	MaxApps             int       `db:"max_apps"               json:"max_apps"`
	MaxWorkers          int       `db:"max_workers"            json:"max_workers"`
	MaxMemoryMB         int       `db:"max_memory_mb"          json:"max_memory_mb"`
	MaxOutboundMB       int       `db:"max_outbound_mb"        json:"max_outbound_mb"`
	MaxRequestsPerMonth int       `db:"max_requests_per_month" json:"max_requests_per_month"`
	UsedOutboundBytes   int64     `db:"used_outbound_bytes"    json:"used_outbound_bytes"`
	UsedRequestCount    int64     `db:"used_request_count"     json:"used_request_count"`
	QuotaPeriodStart    time.Time `db:"quota_period_start"     json:"quota_period_start"`
	// QuotaLockGraceUntil is set by applyTenantDelta on free-tier
	// first-cross of a monthly cap (issue #420). It bounds the
	// request-time 402 — deploys are blocked immediately, but the
	// edge still serves requests until this timestamp expires. After
	// expiry the worker's next heartbeat calls tenantRepo.SetDisabledAt
	// to flip the blast-radius lever, which kills all running apps.
	// Operators clear it via the admin quota-override endpoint.
	QuotaLockGraceUntil *time.Time `db:"quota_lock_grace_until" json:"quota_lock_grace_until,omitempty"`
}

// UsagePct returns the highest usage percentage across the two monthly caps
// (outbound bytes and request count) as a 0–100 value. Returns nil when both
// caps are unlimited (sentinel < 0). The caller is expected to wrap this into
// a response shape with omitempty so unlimited tenants don't get a misleading 0.
func (q Quota) UsagePct() *float64 {
	outCap := int64(q.MaxOutboundMB) * 1024 * 1024
	reqCap := int64(q.MaxRequestsPerMonth)

	var outPct, reqPct *float64
	if outCap > 0 {
		v := float64(q.UsedOutboundBytes) / float64(outCap) * 100
		outPct = &v
	}
	if reqCap > 0 {
		v := float64(q.UsedRequestCount) / float64(reqCap) * 100
		reqPct = &v
	}
	switch {
	case outPct == nil && reqPct == nil:
		return nil
	case outPct == nil:
		return reqPct
	case reqPct == nil:
		return outPct
	}
	if *outPct > *reqPct {
		return outPct
	}
	return reqPct
}

// TenantWithQuota combines tenant and quota data.
type TenantWithQuota struct {
	Tenant
	Quota
}
