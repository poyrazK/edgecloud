// Package stripe (stripe_metering.go) — the production MeteringProvider
// implementation (issue #485). Reports every claimed
// billing_usage_events row to Stripe as a Billing Meter Event, idempotency-
// keyed on the row's idempotency_key.
//
// Lives alongside stripe.go in this sub-package deliberately: keeps the
// (large) stripe-go dep out of the rest of the CP. New merchants slot
// in as sibling sub-packages without touching this code.
//
// ===========================================================================
//   SDK-MIGRATION NOTE: the original plan (and migration 030's header
//   comment) referenced `subscriptionitem.NewUsageRecord` — the legacy
//   metered-usage endpoint that Stripe retired in 2024 in favor of the
//   "Billing Meter Events" model. stripe-go v82 no longer ships that
//   endpoint; the equivalent surface today is
//   `billing/meterevent.New(*stripe.BillingMeterEventParams)` which
//   POSTs to `/v1/billing/meter_events`. The semantic shape is the same
//   (one event per usage delta, idempotency-keyed) — the protocol just
//   moved. This file targets the v82 surface.
// ===========================================================================

package stripe

import (
	"context"
	"errors"
	"fmt"
	"time"

	stripe "github.com/stripe/stripe-go/v82"
	"github.com/stripe/stripe-go/v82/billing/meterevent"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// MeteringProvider is the Stripe MeteringProvider. Holds the
// per-process configuration (secret key, per-tenant SubscriptionItemIDs
// map keyed by (tenant, kind) → subscription_item_id).
//
// Methods are safe for concurrent use; the underlying stripe-go
// client is also concurrency-safe. The drainer is the only caller.
type MeteringProvider struct {
	cfg billing.StripeConfig
	// meterEventNames maps our domain MeterKind to the Stripe meter
	// event_name operators configured on their Stripe Billing Meter.
	// Operator-supplied via cfg.MeterEventNames; the metering call
	// must use the same event_name the meter was created with or
	// Stripe returns 400.
	meterEventNames map[domain.MeterKind]string
}

// NewMetering returns a configured Stripe MeteringProvider. Like
// New() (the BillingProvider constructor), it sets stripe.Key as a
// side effect so a subsequent test that constructs both doesn't have
// to remember which order.
//
// meterEventNames maps our MeterKind onto the operator's Stripe meter
// event_name (each meter has exactly one). Missing entries cause
// RecordUsage to fail fast with ErrNoSubscription rather than dispatch
// an event with the wrong name — that protects tenants from being
// billed under the wrong SKU if a config typo slips in.
func NewMetering(cfg billing.StripeConfig, meterEventNames map[domain.MeterKind]string) *MeteringProvider {
	if cfg.SecretKey != "" {
		stripe.Key = cfg.SecretKey
	}
	if meterEventNames == nil {
		meterEventNames = map[domain.MeterKind]string{}
	}
	return &MeteringProvider{cfg: cfg, meterEventNames: meterEventNames}
}

// Name returns the merchant identifier persisted on every
// billing_usage_events.provider row.
func (p *MeteringProvider) Name() domain.BillingProvider { return domain.ProviderStripe }

// RecordUsage dispatches one metered usage event to Stripe. The flow:
//
//  1. Resolve the SubscriptionItemID for (TenantID, Kind) from cfg.
//     A missing entry is terminal — Stripe would 400 the call, and a
//     retry can't fix a missing config. The drainer converts the
//     error into a MarkProcessed-with-error-log so the row doesn't
//     cycle forever.
//  2. Resolve the meter's event_name from meterEventNames[Kind]. Same
//     terminal posture on a missing entry.
//  3. Build a BillingMeterEventParams with:
//     - EventName = meter's event_name
//     - Identifier = idempotency_key (Stripe dedupes events with the
//     same identifier inside a rolling 24h window — equivalent
//     semantics to the legacy Idempotency-Key header, but native to
//     the meterevent resource)
//     - Timestamp = recorded_at (Unix seconds)
//     - Payload = {"stripe_customer_id": ..., "value": <quantity>}
//     The payload keys are Stripe's defaults
//     (customer_mapping.event_payload_key and
//     value_settings.event_payload_key); operators can override
//     them per-meter via the Stripe dashboard.
//  4. POST to /v1/billing/meter_events.
//
// Error translation:
//
//   - *stripe.Error with HTTPStatus < 500 → terminal (4xx means the
//     request is malformed — Stripe won't accept a retry).
//     `IdempotencyKey` collisions and bad payload shapes land here.
//   - *stripe.Error with HTTPStatus ≥ 500 → transient (5xx means
//     Stripe is having a bad day; the drainer retries).
//   - everything else (network, timeout, ctx cancelled) → transient.
//
// The drainer is the only caller and it owns the terminal/transient
// decision tree; this method just translates the SDK error shape.
func (p *MeteringProvider) RecordUsage(ctx context.Context, in billing.MeterUsage) error {
	if in.TenantID == "" {
		return fmt.Errorf("stripe: tenantID required")
	}
	if in.IdempotencyKey == "" {
		return fmt.Errorf("stripe: idempotency_key required")
	}

	subItemID, eventName, err := p.resolveMeterTarget(in.TenantID, in.Kind)
	if err != nil {
		return err
	}

	quantity := in.Quantity
	timestamp := in.RecordedAt.Unix()
	if timestamp <= 0 {
		timestamp = time.Now().Unix()
	}

	params := &stripe.BillingMeterEventParams{
		EventName:  stripe.String(eventName),
		Identifier: stripe.String(in.IdempotencyKey),
		Timestamp:  stripe.Int64(timestamp),
		Payload: map[string]string{
			"stripe_customer_id": subItemID, // populated by operator via customer mapping
			"value":              fmt.Sprintf("%d", quantity),
		},
	}
	// Belt-and-suspenders: also set the Idempotency-Key header on
	// the request. Stripe's meterevent resource dedupes on `Identifier`,
	// but the legacy header is still honored and adds a second
	// dedupe layer for free.
	params.SetIdempotencyKey(in.IdempotencyKey)

	_, err = meterevent.New(params)
	if err == nil {
		return nil
	}

	// Translate stripe-go's error type to the (terminal/transient)
	// shape the drainer consumes.
	var stripeErr *stripe.Error
	if errors.As(err, &stripeErr) {
		// 4xx is terminal: bad payload, missing meter, bad API key.
		// Retrying a 400 is a waste; the operator must fix config.
		if stripeErr.HTTPStatusCode >= 400 && stripeErr.HTTPStatusCode < 500 {
			return fmt.Errorf("%w: %v", billing.ErrTerminal, err)
		}
		// 5xx is transient: Stripe is having a bad day.
		return fmt.Errorf("stripe: transient error: %w", err)
	}
	// Network / context / unknown: transient by default.
	return fmt.Errorf("stripe: dispatch: %w", err)
}

// resolveMeterTarget looks up the Stripe subscription_item_id and
// meter event_name for a (tenant, kind) pair. Returns
// ErrNoSubscription if either is missing so the drainer can route the
// row to MarkProcessed-with-warn rather than spinning.
//
// In production the subscription_item_id is keyed on (tenant, kind)
// because each dimension (resident_seconds / request_count /
// outbound_bytes) maps to its own Stripe price item — different
// meters, different prices, different event_names. A flat
// per-tenant single-id map wouldn't model this.
func (p *MeteringProvider) resolveMeterTarget(tenantID string, kind domain.MeterKind) (string, string, error) {
	// SubscriptionItemIDs map shape: tenantID → kind → subscription_item_id
	// (matches the plan's locked design — see
	// edge-control-plane/internal/billing/metering_provider.go and
	// BillingMeteringConfig.SubscriptionItemIDs in config.go).
	if byKind, ok := p.cfg.MeterSubscriptionItemIDs[tenantID]; ok {
		if subItemID, ok := byKind[string(kind)]; ok && subItemID != "" {
			// Fall through to event_name check below.
			eventName, ok := p.meterEventNames[kind]
			if !ok || eventName == "" {
				return "", "", fmt.Errorf("%w: meter_event_name for kind %q: %w",
					billing.ErrTerminal, kind, ErrNoSubscription)
			}
			return subItemID, eventName, nil
		}
	}
	return "", "", fmt.Errorf("%w: %s/%s: %w",
		billing.ErrTerminal, tenantID, kind, ErrNoSubscription)
}

// Compile-time guard: MeteringProvider satisfies billing.MeteringProvider.
var _ billing.MeteringProvider = (*MeteringProvider)(nil)
