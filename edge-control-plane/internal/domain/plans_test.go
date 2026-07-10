package domain

import (
	"errors"
	"testing"
)

func TestQuotaForPlan_KnownPlans(t *testing.T) {
	cases := []struct {
		plan   string
		expect Quota
	}{
		{"free", Quota{MaxDeployments: 10, MaxApps: 5, MaxWorkers: 3, MaxMemoryMB: 256, MaxOutboundMB: 1000, MaxRequestsPerMonth: 100_000, MaxResidentSecondsPerMonth: 2_592_000}},
		{"pro", Quota{MaxDeployments: 50, MaxApps: 20, MaxWorkers: 10, MaxMemoryMB: 512, MaxOutboundMB: 10_000, MaxRequestsPerMonth: 5_000_000, MaxResidentSecondsPerMonth: 7_776_000}},
		{"business", Quota{MaxDeployments: 200, MaxApps: 50, MaxWorkers: 30, MaxMemoryMB: 1024, MaxOutboundMB: 100_000, MaxRequestsPerMonth: 50_000_000, MaxResidentSecondsPerMonth: 31_104_000}},
		{"enterprise", Quota{MaxDeployments: -1, MaxApps: -1, MaxWorkers: -1, MaxMemoryMB: -1, MaxOutboundMB: -1, MaxRequestsPerMonth: -1, MaxResidentSecondsPerMonth: -1}},
	}
	for _, tc := range cases {
		got, err := QuotaForPlan(tc.plan)
		if err != nil {
			t.Errorf("QuotaForPlan(%q) returned err: %v", tc.plan, err)
			continue
		}
		if got != tc.expect {
			t.Errorf("QuotaForPlan(%q) = %+v, want %+v", tc.plan, got, tc.expect)
		}
	}
}

func TestQuotaForPlan_UnknownPlan(t *testing.T) {
	_, err := QuotaForPlan("platinum")
	if err == nil {
		t.Fatalf("expected error for unknown plan")
	}
	if !errors.Is(err, ErrUnknownPlan) {
		t.Errorf("expected ErrUnknownPlan, got %v", err)
	}
}

func TestIsValidPlan(t *testing.T) {
	for _, p := range []string{"free", "pro", "business", "enterprise"} {
		if !IsValidPlan(p) {
			t.Errorf("IsValidPlan(%q) = false, want true", p)
		}
	}
	for _, p := range []string{"", "Platinum", "starter", "team"} {
		if IsValidPlan(p) {
			t.Errorf("IsValidPlan(%q) = true, want false", p)
		}
	}
}

func TestPlanSentinel_UnlimitedValues(t *testing.T) {
	q, err := QuotaForPlan("enterprise")
	if err != nil {
		t.Fatalf("QuotaForPlan(enterprise): %v", err)
	}
	if q.MaxDeployments >= 0 || q.MaxApps >= 0 || q.MaxWorkers >= 0 ||
		q.MaxMemoryMB >= 0 || q.MaxOutboundMB >= 0 || q.MaxRequestsPerMonth >= 0 ||
		q.MaxResidentSecondsPerMonth >= 0 {
		t.Errorf("enterprise tier should have all Max* = -1 (unlimited), got %+v", q)
	}
}

func TestPlans_ReturnsAllFour(t *testing.T) {
	plans := Plans()
	if len(plans) != 4 {
		t.Fatalf("Plans() returned %d plans, want 4", len(plans))
	}
	wantNames := []string{"free", "pro", "business", "enterprise"}
	for i, p := range plans {
		if p.Name != wantNames[i] {
			t.Errorf("Plans()[%d].Name = %q, want %q", i, p.Name, wantNames[i])
		}
	}
}

func TestQuota_UsagePct_BothUnlimited(t *testing.T) {
	q, _ := QuotaForPlan("enterprise")
	// TenantID doesn't affect UsagePct.
	pct := q.UsagePct()
	if pct != nil {
		t.Errorf("UsagePct() with both caps unlimited = %v, want nil", *pct)
	}
}

func TestQuota_UsagePct_PicksMax(t *testing.T) {
	// 50% outbound (50MB of 100MB cap), 80% requests (8000 of 10000 cap).
	// Should pick 80.
	q := Quota{
		MaxOutboundMB: 100, MaxRequestsPerMonth: 10_000,
		UsedOutboundBytes: 50 * 1024 * 1024, UsedRequestCount: 8000,
	}
	pct := q.UsagePct()
	if pct == nil {
		t.Fatalf("UsagePct() = nil, want non-nil")
	}
	if got, want := *pct, 80.0; got != want {
		t.Errorf("UsagePct() = %v, want %v", got, want)
	}

	// Reverse: outbound now 80%, requests 50%. Should pick 80.
	q.UsedOutboundBytes = 80 * 1024 * 1024
	q.UsedRequestCount = 5000
	if got := *q.UsagePct(); got != 80.0 {
		t.Errorf("UsagePct() = %v, want 80.0", got)
	}
}

func TestQuota_UsagePct_OneUnlimited(t *testing.T) {
	q := Quota{
		MaxRequestsPerMonth: -1, // unlimited
		MaxOutboundMB:       100,
		UsedOutboundBytes:   25 * 1024 * 1024, // 25%
	}
	pct := q.UsagePct()
	if pct == nil {
		t.Fatalf("UsagePct() = nil, want 25.0")
	}
	if *pct != 25.0 {
		t.Errorf("UsagePct() = %v, want 25.0", *pct)
	}
}

// TestQuota_UsagePct_ResidentSecondsHighest covers the third metered
// dimension (issue #485): when resident-seconds is the highest axis,
// UsagePct picks it. Mirrors TestQuota_UsagePct_PicksMax but exercises
// the new branch.
func TestQuota_UsagePct_ResidentSecondsHighest(t *testing.T) {
	q := Quota{
		MaxOutboundMB:              100,
		MaxRequestsPerMonth:        10_000,
		MaxResidentSecondsPerMonth: 1_000,            // 1000s cap
		UsedOutboundBytes:          10 * 1024 * 1024, // 10%
		UsedRequestCount:           2_000,            // 20%
		UsedResidentSeconds:        900,              // 90%
	}
	pct := q.UsagePct()
	if pct == nil {
		t.Fatalf("UsagePct() = nil, want 90.0")
	}
	if *pct != 90.0 {
		t.Errorf("UsagePct() = %v, want 90.0 (resident-seconds is the highest axis)", *pct)
	}
}

// TestQuota_UsagePct_ResidentSecondsZeroNoCap covers the rollout path:
// before operator configures a cap, MaxResidentSecondsPerMonth is 0
// (skip the cap check) — UsagePct should ignore the resident-seconds
// axis entirely. Without this guard, a tenant with used_resident_seconds=0
// would contribute a 0% that "ties" against the other axes; 0 must be
// omitted so UsagePct picks the highest non-zero axis instead.
func TestQuota_UsagePct_ResidentSecondsZeroNoCap(t *testing.T) {
	q := Quota{
		MaxOutboundMB:              100, // operator hasn't set resident cap
		MaxRequestsPerMonth:        10_000,
		MaxResidentSecondsPerMonth: 0,
		UsedOutboundBytes:          50 * 1024 * 1024, // 50%
		UsedRequestCount:           2_000,            // 20%
		UsedResidentSeconds:        0,
	}
	pct := q.UsagePct()
	if pct == nil {
		t.Fatalf("UsagePct() = nil, want 50.0")
	}
	if *pct != 50.0 {
		t.Errorf("UsagePct() = %v, want 50.0 (outbound highest, resident-seconds axis disabled)", *pct)
	}
}
