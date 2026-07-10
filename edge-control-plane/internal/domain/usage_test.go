package domain

import "testing"

// TestUpgradeOptionsForPlan_Free_ReturnsPaidTiers exercises the free-tenant
// path: every paid plan is listed with the dollar-converted price, the free
// tier itself is filtered out, and the order matches Plans() (pro, business,
// enterprise). Pinning both the order and the prices catches regressions
// in either domain.Plans() or the cents→dollars conversion.
func TestUpgradeOptionsForPlan_Free_ReturnsPaidTiers(t *testing.T) {
	got := UpgradeOptionsForPlan("free")
	want := []UpgradeOption{
		{Plan: "pro", MonthlyPriceUSD: 25},       // 2500 cents → $25
		{Plan: "business", MonthlyPriceUSD: 100}, // 10000 cents → $100
		{Plan: "enterprise", MonthlyPriceUSD: 0}, // negotiated out of band
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (got %+v)", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("[%d] = %+v, want %+v", i, got[i], w)
		}
	}
}

// TestUpgradeOptionsForPlan_NonFree_ReturnsNil covers the paid-tenant path:
// every non-free plan returns nil so the dashboard hides the upgrade CTA.
// The tenant already has a portal link to manage their plan.
func TestUpgradeOptionsForPlan_NonFree_ReturnsNil(t *testing.T) {
	for _, plan := range []string{"pro", "business", "enterprise"} {
		t.Run(plan, func(t *testing.T) {
			got := UpgradeOptionsForPlan(plan)
			if got != nil {
				t.Errorf("UpgradeOptionsForPlan(%q) = %+v, want nil", plan, got)
			}
		})
	}
}

// TestUpgradeOptionsForPlan_UnknownPlan_ReturnsNil confirms the safe
// default: a tenant whose plan name isn't in the registry (e.g. a
// legacy row from before a tier rename) gets nil rather than a panic or
// a stale list.
func TestUpgradeOptionsForPlan_UnknownPlan_ReturnsNil(t *testing.T) {
	if got := UpgradeOptionsForPlan("legacy-platinum"); got != nil {
		t.Errorf("UpgradeOptionsForPlan(legacy-platinum) = %+v, want nil", got)
	}
}

// TestBillingStatusForSubscription_NilIsActive confirms the free-tier-with-no-row
// case returns active. A brand-new tenant who has never started checkout has
// no subscription row; the dashboard still shows them as "all good" and
// surfaces the upgrade options.
func TestBillingStatusForSubscription_NilIsActive(t *testing.T) {
	if got := BillingStatusForSubscription(nil); got != BillingActive {
		t.Errorf("nil sub → %q, want %q", got, BillingActive)
	}
}

// TestBillingStatusForSubscription_StatusMapping covers every SubscriptionStatus
// value. The mapping is the customer-facing contract — "your card was
// declined" vs. "your subscription was canceled" both surface as
// action_required, while active/trialing/incomplete are folded into active.
func TestBillingStatusForSubscription_StatusMapping(t *testing.T) {
	cases := []struct {
		status SubscriptionStatus
		want   BillingStatus
	}{
		{SubscriptionActive, BillingActive},
		{SubscriptionTrialing, BillingActive},
		{SubscriptionIncomplete, BillingActive},
		{SubscriptionPastDue, BillingActionRequired},
		{SubscriptionCanceled, BillingActionRequired},
		// Unknown future statuses fall through to active (defensive
		// default — we shouldn't expose "unknown" to a customer).
		{SubscriptionStatus("future_state"), BillingActive},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			sub := &BillingSubscription{Status: tc.status}
			if got := BillingStatusForSubscription(sub); got != tc.want {
				t.Errorf("status=%q → %q, want %q", tc.status, got, tc.want)
			}
		})
	}
}
