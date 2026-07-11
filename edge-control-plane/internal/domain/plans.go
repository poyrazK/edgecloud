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
	Name                       string
	DisplayName                string
	PricePerMonthCents         int
	MaxDeployments             int
	MaxApps                    int
	MaxWorkers                 int
	MaxMemoryMB                int
	MaxOutboundMB              int
	MaxRequestsPerMonth        int
	MaxResidentSecondsPerMonth int
	// MaxComputeMsPerMonth is the FaaS request-duration ceiling
	// (issue #555, fourth metered dimension). Same sentinel
	// convention as the rest: -1 unlimited, 0 hard-deny, >0 the
	// per-tenant millisecond budget. Scaled from
	// MaxResidentSecondsPerMonth by 1_000 — free=2_592_000_000
	// (≈30 days × 1 app × 86_400_000 ms/day), pro=7_776_000_000
	// (≈90 days), business=31_104_000_000 (≈360 days). Caps are
	// policy and tunable via TenantService.UpdateTenant + plan
	// changes — same path as #485's resident-seconds caps.
	MaxComputeMsPerMonth int
}

// planTiers is the single source of truth for plan→quota mapping.
// Values mirror whitepaper §11 (whitepaper.md:720-735) verbatim.
//
// The same plan→cap values are duplicated in
// migrations/013_quotas_used_requests.up.sql + 029_quotas_resident_seconds.up.sql
// + 031_quotas_compute_ms.up.sql for the backfill statements. Keep all
// four in sync when adding tiers or adjusting caps.
var planTiers = map[string]PlanSpec{
	"free": {
		Name: "free", DisplayName: "Free", PricePerMonthCents: 0,
		MaxDeployments: 10, MaxApps: 5, MaxWorkers: 3,
		MaxMemoryMB: 256, MaxOutboundMB: 1000, MaxRequestsPerMonth: 100_000,
		MaxResidentSecondsPerMonth: 2_592_000,     // 30 days of 1 LR app
		MaxComputeMsPerMonth:       2_592_000_000, // ≈ 30 days × 86_400_000 ms/day (issue #555)
	},
	"pro": {
		Name: "pro", DisplayName: "Pro", PricePerMonthCents: 2500,
		MaxDeployments: 50, MaxApps: 20, MaxWorkers: 10,
		MaxMemoryMB: 512, MaxOutboundMB: 10_000, MaxRequestsPerMonth: 5_000_000,
		MaxResidentSecondsPerMonth: 7_776_000,     // 90 days of 1 LR app
		MaxComputeMsPerMonth:       7_776_000_000, // ≈ 90 days × 86_400_000 ms/day
	},
	"business": {
		Name: "business", DisplayName: "Business", PricePerMonthCents: 10000,
		MaxDeployments: 200, MaxApps: 50, MaxWorkers: 30,
		MaxMemoryMB: 1024, MaxOutboundMB: 100_000, MaxRequestsPerMonth: 50_000_000,
		MaxResidentSecondsPerMonth: 31_104_000,     // 360 days of 1 LR app
		MaxComputeMsPerMonth:       31_104_000_000, // ≈ 360 days × 86_400_000 ms/day
	},
	"enterprise": {
		// All Max* = -1 (sentinel for "unlimited"). Pricing is negotiated out
		// of band; PricePerMonthCents stays 0 and a future booking flow
		// derives the actual price from the contract.
		Name: "enterprise", DisplayName: "Enterprise", PricePerMonthCents: 0,
		MaxDeployments: -1, MaxApps: -1, MaxWorkers: -1,
		MaxMemoryMB: -1, MaxOutboundMB: -1, MaxRequestsPerMonth: -1,
		MaxResidentSecondsPerMonth: -1,
		MaxComputeMsPerMonth:       -1,
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
		MaxDeployments:             spec.MaxDeployments,
		MaxApps:                    spec.MaxApps,
		MaxWorkers:                 spec.MaxWorkers,
		MaxMemoryMB:                spec.MaxMemoryMB,
		MaxOutboundMB:              spec.MaxOutboundMB,
		MaxRequestsPerMonth:        spec.MaxRequestsPerMonth,
		MaxResidentSecondsPerMonth: spec.MaxResidentSecondsPerMonth,
		MaxComputeMsPerMonth:       spec.MaxComputeMsPerMonth,
	}, nil
}
