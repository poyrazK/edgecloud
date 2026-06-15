package domain

import (
	"time"
)

// Tenant represents a platform customer.
type Tenant struct {
	ID                     string    `db:"id"`
	Name                   string    `db:"name"`
	Plan                   string    `db:"plan"`
	AllowlistedDestinations []string `db:"allowlisted_destinations"`
	CreatedAt              time.Time `db:"created_at"`
}

// Quota defines resource limits for a tenant.
type Quota struct {
	TenantID        string `db:"tenant_id"`
	MaxDeployments  int    `db:"max_deployments"`
	MaxApps         int    `db:"max_apps"`
	MaxWorkers      int    `db:"max_workers"`
	MaxMemoryMB     int    `db:"max_memory_mb"`
	MaxOutboundMB   int    `db:"max_outbound_mb"`
}

// DefaultQuota returns free-tier defaults.
func DefaultQuota(tenantID string) Quota {
	return Quota{
		TenantID:        tenantID,
		MaxDeployments:  10,
		MaxApps:         5,
		MaxWorkers:      3,
		MaxMemoryMB:     256,
		MaxOutboundMB:   1000,
	}
}

// TenantWithQuota combines tenant and quota data.
type TenantWithQuota struct {
	Tenant
	Quota
}
