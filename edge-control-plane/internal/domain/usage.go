package domain

import "time"

// BillingStatus is the customer-facing billing status for the usage
// dashboard endpoint (issue #421). It collapses the merchant's
// SubscriptionStatus vocabulary into two outcomes the dashboard cares
// about: "everything is fine" vs. "the tenant needs to take action".
//
// We deliberately don't expose SubscriptionStatus directly on the
// dashboard — operators recognize "past_due" / "canceled" from the
// Stripe dashboard, but a tenant-facing API should signal "your card
// was declined" rather than "your subscription is past_due".
type BillingStatus string

const (
	// BillingActive means the tenant's subscription is in good
	// standing. Covers SubscriptionActive and SubscriptionTrialing
	// (trialing is still a positive customer state) plus the
	// no-subscription-row case (a brand-new free-tier tenant has
	// nothing in billing_subscriptions yet but isn't "in trouble").
	BillingActive BillingStatus = "active"

	// BillingActionRequired means the tenant needs to update their
	// payment method or contact support. Covers SubscriptionPastDue
	// (card declined) and SubscriptionCanceled (downgraded away from
	// paid). Other "in trouble" states (SubscriptionIncomplete) are
	// folded into BillingActive for now since the dashboard only has
	// two buttons ("manage plan", "contact support"); we can split
	// later if a customer asks.
	BillingActionRequired BillingStatus = "action_required"
)

// BillingStatusForSubscription maps the merchant's SubscriptionStatus
// onto the customer-facing BillingStatus. A nil subscription (free-tier
// tenant that has never started checkout) is treated as BillingActive.
func BillingStatusForSubscription(s *BillingSubscription) BillingStatus {
	if s == nil {
		return BillingActive
	}
	switch s.Status {
	case SubscriptionPastDue, SubscriptionCanceled:
		return BillingActionRequired
	default:
		// active, trialing, incomplete, anything-else-we-haven't-named
		return BillingActive
	}
}

// CurrentPeriodUsage is the request / outbound bytes position for the
// current billing month, derived from a single quotas row.
//
// Cap fields are normalized to int64 so the response is self-describing
// without forcing the dashboard to know the MaxOutboundMB units
// convention (we expose it as bytes, not MiB). -1 / 0 sentinels
// (unlimited / unset) are passed through unchanged; the dashboard
// renders "unlimited" when the cap is < 0.
//
// UsagePct mirrors the field already produced by quotaResponse (handler/quota.go:33-36)
// so the dashboard can show the same percentage as the quotas endpoint.
// Omitted when both caps are unlimited — same omitempty contract as
// Quota.UsagePct() so the wire shape stays consistent across endpoints.
type CurrentPeriodUsage struct {
	PeriodStart       time.Time `json:"period_start"`
	PeriodEnd         time.Time `json:"period_end"`
	RequestsUsed      int64     `json:"requests_used"`
	RequestsCap       int64     `json:"requests_cap"`
	OutboundBytesUsed int64     `json:"outbound_bytes_used"`
	OutboundBytesCap  int64     `json:"outbound_bytes_cap"`
	UsagePct          *float64  `json:"usage_pct,omitempty"`
}

// UpgradeOption is one row in the upgrade_options[] array. Issued only
// for free-tier tenants — paid tenants get an empty list because they
// can already see the available tiers inside the Stripe portal.
//
// PricePerMonthUSD is in dollars (not cents) so the dashboard doesn't
// need to know the cents convention. Zero for any free tier in the
// list (defensive — the dashboard skips a zero-priced entry).
type UpgradeOption struct {
	Plan            string `json:"plan"`
	MonthlyPriceUSD int    `json:"monthly_price_usd"`
}

// UpgradeOptionsForPlan returns the upgrade_options list for a tenant
// on the given plan. Free tenants see every paid tier; non-free
// tenants get an empty list.
//
// The plan (#421) calls for showing upgrade options ONLY to free-tier
// tenants — paid tenants already see the available tiers inside the
// Stripe portal, so the dashboard doesn't need to re-list them. A
// future iteration could differentiate "upgrade" vs "downgrade"
// options for paid tenants if a customer asks.
//
// Pricing is sourced from domain.Plans() so this stays consistent
// with QuotaForPlan and the admin checkout flow (config.go:644).
func UpgradeOptionsForPlan(currentPlan string) []UpgradeOption {
	if currentPlan != "free" {
		return nil
	}
	var out []UpgradeOption
	for _, spec := range Plans() {
		if spec.Name == "free" {
			// Free is not an "upgrade" from any other tier.
			continue
		}
		// PricePerMonthCents is the source of truth in domain.Plans().
		// Convert to dollars here so the dashboard renders USD directly.
		out = append(out, UpgradeOption{
			Plan:            spec.Name,
			MonthlyPriceUSD: spec.PricePerMonthCents / 100,
		})
	}
	return out
}

// BillingEventTimelineEntry is the response-shape projection of a
// domain.BillingEvent. Drops payload_hash (an internal dedup key, not
// useful to a dashboard user) and keeps the fields a tenant would
// want to see: what happened, when, and whether the control plane
// finished processing it.
//
// The split from domain.BillingEvent is one-way: the repository
// returns []BillingEvent, the service projects it to
// []BillingEventTimelineEntry for the wire. That way we never leak
// payload_hash to the dashboard even by accident.
type BillingEventTimelineEntry struct {
	EventID     string     `db:"event_id"     json:"event_id"`
	EventType   string     `db:"event_type"   json:"event_type"`
	ReceivedAt  time.Time  `db:"received_at"  json:"received_at"`
	ProcessedAt *time.Time `db:"processed_at" json:"processed_at,omitempty"`
}

// TenantUsage is the response envelope for GET /api/v1/usage. It
// composes the current-period counters (from quotas), a window of
// subscription lifecycle events (from billing_events), and the
// upgrade options + billing portal URL when relevant.
//
// From/To are echoed back so the dashboard can render "showing
// 2026-06-10 to 2026-07-10" without parsing the request URL again.
//
// NextOffset is a placeholder for event pagination (issue #421 leaves
// it nil for now — the initial release caps at the requested limit and
// the dashboard does single-page reads). Typed as *string so future
// pagination can use a cursor-shaped value (timestamp + event_id)
// without a wire-shape break.
type TenantUsage struct {
	TenantID         string                      `json:"tenant_id"`
	BillingStatus    BillingStatus               `json:"billing_status"`
	CurrentPeriod    CurrentPeriodUsage          `json:"current_period"`
	Events           []BillingEventTimelineEntry `json:"events"`
	From             time.Time                   `json:"from"`
	To               time.Time                   `json:"to"`
	NextOffset       *string                     `json:"next_offset,omitempty"`
	UpgradeOptions   []UpgradeOption             `json:"upgrade_options"`
	BillingPortalURL *string                     `json:"billing_portal_url,omitempty"`
}