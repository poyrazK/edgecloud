package domain

import (
	"errors"
	"fmt"
)

// ErrUnknownPlan is returned by QuotaForPlan when the requested plan name does
// not match any known tier. Callers should translate this into HTTP 400.
var ErrUnknownPlan = errors.New("unknown plan")

// PlanSpec describes a billable plan tier and its associated quota ceilings.
//
// Sentinel convention: every Max* field uses the literal -1 to mean
// "unlimited" at the service layer (see e.g. service.WorkerService handling
// of MaxWorkers). Distinct from 0, which means "unset / admin-cleared".
type PlanSpec struct {
	Name                string
	DisplayName         string
	PricePerMonthCents  int
	MaxDeployments      int
	MaxApps             int
	MaxWorkers          int
	MaxMemoryMB         int
	MaxOutboundMB       int
	MaxRequestsPerMonth int
}

// planTiers is the single source of truth for plan→quota mapping.
// Values mirror whitepaper §11 (whitepaper.md:720-735) verbatim.
//
// The same plan→cap values are duplicated in
// migrations/013_quotas_used_requests.up.sql for the backfill UPDATE.
// Keep both in sync when adding tiers or adjusting caps.
var planTiers = map[string]PlanSpec{
	"free": {
		Name: "free", DisplayName: "Free", PricePerMonthCents: 0,
		MaxDeployments: 10, MaxApps: 5, MaxWorkers: 3,
		MaxMemoryMB: 256, MaxOutboundMB: 1000, MaxRequestsPerMonth: 100_000,
	},
	"pro": {
		Name: "pro", DisplayName: "Pro", PricePerMonthCents: 2500,
		MaxDeployments: 50, MaxApps: 20, MaxWorkers: 10,
		MaxMemoryMB: 512, MaxOutboundMB: 10_000, MaxRequestsPerMonth: 5_000_000,
	},
	"business": {
		Name: "business", DisplayName: "Business", PricePerMonthCents: 10000,
		MaxDeployments: 200, MaxApps: 50, MaxWorkers: 30,
		MaxMemoryMB: 1024, MaxOutboundMB: 100_000, MaxRequestsPerMonth: 50_000_000,
	},
	"enterprise": {
		// All Max* = -1 (sentinel for "unlimited"). Pricing is negotiated out
		// of band; PricePerMonthCents stays 0 and a future booking flow
		// derives the actual price from the contract.
		Name: "enterprise", DisplayName: "Enterprise", PricePerMonthCents: 0,
		MaxDeployments: -1, MaxApps: -1, MaxWorkers: -1,
		MaxMemoryMB: -1, MaxOutboundMB: -1, MaxRequestsPerMonth: -1,
	},
}

// Plans returns the canonical list of plan tiers. Order is fixed (free, pro,
// business, enterprise) so callers can present them in a stable sequence.
func Plans() []PlanSpec {
	return []PlanSpec{
		planTiers["free"],
		planTiers["pro"],
		planTiers["business"],
		planTiers["enterprise"],
	}
}

// IsValidPlan reports whether plan matches a known tier name.
func IsValidPlan(plan string) bool {
	_, ok := planTiers[plan]
	return ok
}

// QuotaForPlan returns the quota row that should be inserted when a tenant is
// created with the given plan. Returns ErrUnknownPlan for any plan string not
// present in the planTiers table.
func QuotaForPlan(plan string) (Quota, error) {
	spec, ok := planTiers[plan]
	if !ok {
		return Quota{}, fmt.Errorf("%w: %q", ErrUnknownPlan, plan)
	}
	return Quota{
		MaxDeployments:      spec.MaxDeployments,
		MaxApps:             spec.MaxApps,
		MaxWorkers:          spec.MaxWorkers,
		MaxMemoryMB:         spec.MaxMemoryMB,
		MaxOutboundMB:       spec.MaxOutboundMB,
		MaxRequestsPerMonth: spec.MaxRequestsPerMonth,
	}, nil
}