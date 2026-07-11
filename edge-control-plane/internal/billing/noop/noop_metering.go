// Package noop (noop_metering.go) — the dev/CI/test MeteringProvider.
// Mirrors noop.go's BillingProvider shape so the integration suite can
// exercise the metering drainer without real merchant credentials.
//
// validateBillingConfig in internal/config/config.go fail-closes when
// provider == "noop" and app.env is anything other than dev|test —
// production never sees this code path. Same posture as the
// BillingProvider counterpart.
package noop

import (
	"context"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// MeteringProvider is the noop MeteringProvider. Zero-value usable;
// every RecordUsage call is a deterministic no-op.
//
// The drainer still calls MarkProcessed on every row (the ledger is
// still durable; we just don't dispatch externally). That means
// integration tests can assert end-to-end "row → drainer tick →
// processed_at IS NOT NULL" without standing up Stripe.
type MeteringProvider struct{}

// NewMetering returns a fresh noop MeteringProvider. Constructor
// exists so app.go can write the same factory shape for every
// metering provider:
//
//	billing/stripe.NewMetering(cfg)  → *stripe.MeteringProvider
//	billing/noop.NewMetering()       → *noop.MeteringProvider
func NewMetering() *MeteringProvider { return &MeteringProvider{} }

// Name returns the merchant identifier persisted on every
// billing_usage_events.provider row. Matches the noop BillingProvider
// so a tenant can run noop-on-both-surfaces for a fully-isolated
// dev/CI environment.
func (p *MeteringProvider) Name() domain.BillingProvider { return domain.ProviderNoop }

// RecordUsage is the fire-and-record counterpart to noop's
// CreateCheckoutSession. Always returns nil; the row is consumed but
// no merchant call is made.
//
// We deliberately do NOT emit a structured log here — the drainer
// already logs every claim at INFO with the row id, tenant, and kind.
// Logging here would double-emit and bloat the dev loop's logs.
func (p *MeteringProvider) RecordUsage(ctx context.Context, in billing.MeterUsage) error {
	_ = ctx
	_ = in
	return nil
}

// Compile-time guard: MeteringProvider satisfies billing.MeteringProvider.
var _ billing.MeteringProvider = (*MeteringProvider)(nil)
