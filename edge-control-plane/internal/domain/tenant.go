package domain

import (
	"time"

	"github.com/lib/pq"
)

// Tenant represents a platform customer.
type Tenant struct {
	ID   string `db:"id"`
	Name string `db:"name"`
	Plan string `db:"plan"`
	// AllowlistedDestinations is a TEXT[] column. Typed as
	// pq.StringArray (which is []string underneath) so the column
	// scans correctly via lib/pq's Scanner — a bare []string does NOT
	// implement sql.Scanner and would fail on SELECT. The JSON wire
	// format is unchanged because pq.StringArray marshals identically
	// to []string. The repo also wraps writes in pq.Array() for the
	// same reason on the encoding side.
	AllowlistedDestinations pq.StringArray `db:"allowlisted_destinations"`
	CreatedAt               time.Time      `db:"created_at"`
}

// Quota defines resource limits for a tenant.
type Quota struct {
	TenantID       string `db:"tenant_id"`
	MaxDeployments int    `db:"max_deployments"`
	MaxApps        int    `db:"max_apps"`
	MaxWorkers     int    `db:"max_workers"`
	MaxMemoryMB    int    `db:"max_memory_mb"`
	MaxOutboundMB  int    `db:"max_outbound_mb"`
}

// DefaultQuota returns free-tier defaults.
func DefaultQuota(tenantID string) Quota {
	return Quota{
		TenantID:       tenantID,
		MaxDeployments: 10,
		MaxApps:        5,
		MaxWorkers:     3,
		MaxMemoryMB:    256,
		MaxOutboundMB:  1000,
	}
}

// TenantWithQuota combines tenant and quota data.
type TenantWithQuota struct {
	Tenant
	Quota
}
