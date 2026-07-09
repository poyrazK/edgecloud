// Package noop is the dev/CI/test BillingProvider. Always succeeds so
// the integration suite can exercise /api/v1/billing/* without real
// merchant credentials. validateBillingConfig in internal/config/
// fail-closes when provider == "noop" and app.env is anything other
// than dev|test — production never sees this code path.
package noop

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// ErrNoSubscription is returned by CreatePortalSession when the tenant
// has no billing_subscriptions row yet. Mirrors the equivalent error
// from billing/stripe so the handler can translate it to HTTP 404
// without inspecting provider types.
var ErrNoSubscription = errors.New("noop: no subscription for tenant")

// ErrInvalidSignature is the noop equivalent of Stripe's
// signature-mismatch error. Returned unconditionally because the noop
// provider accepts NO webhooks — it's not wired to receive them.
var ErrInvalidSignature = errors.New("noop provider rejects all webhooks (dev/CI/test only)")

// Provider is the noop BillingProvider. Zero-value usable; all
// behavior is deterministic and side-effect-free.
type Provider struct{}

// New returns a fresh noop Provider. Constructor exists so app.go can
// write the same factory shape for every provider:
//
//	billing/stripe.New(cfg) → *stripe.Provider
//	billing/noop.New()     → *noop.Provider
func New() *Provider { return &Provider{} }

// Name returns the merchant identifier persisted on every row.
func (p *Provider) Name() domain.BillingProvider { return domain.ProviderNoop }

// CreateCheckoutSession mints a synthetic session whose URL points at
// a local /api/v1/billing/subscription placeholder. The dev loop
// closes: hit POST /api/v1/billing/checkout → follow the returned URL
// → it 200s immediately → GET /api/v1/billing/subscription returns
// the row written by the (also-noop) handler.
//
// The session ID is "noop_" + a SHA-256-derived suffix so a real
// merchant's id space never collides with a noop one if rows are
// ever migrated.
func (p *Provider) CreateCheckoutSession(ctx context.Context, in billing.CheckoutInput) (billing.CheckoutSession, error) {
	if in.TenantID == "" {
		return billing.CheckoutSession{}, fmt.Errorf("noop: tenantID required")
	}
	if in.Plan == "" {
		return billing.CheckoutSession{}, fmt.Errorf("noop: plan required")
	}
	id := "noop_" + shortHash(in.TenantID+":"+in.Plan)
	return billing.CheckoutSession{
		ID:        id,
		URL:       "/api/v1/billing/subscription?dev=noop&session=" + id,
		ExpiresAt: time.Now().Add(30 * time.Minute),
	}, nil
}

// CreatePortalSession returns ErrNoSubscription if the tenant has no
// row yet, mirroring the real provider's behavior so the handler
// surfaces a consistent 404 either way.
func (p *Provider) CreatePortalSession(ctx context.Context, tenantID string, returnURL string) (billing.PortalSession, error) {
	if tenantID == "" {
		return billing.PortalSession{}, fmt.Errorf("noop: tenantID required")
	}
	return billing.PortalSession{}, ErrNoSubscription
}

// GetSubscription returns ErrNoSubscription — the noop never writes
// real rows. A test that needs a row should construct the
// domain.BillingSubscription directly and pass it through the repo.
func (p *Provider) GetSubscription(ctx context.Context, tenantID string) (domain.BillingSubscription, error) {
	return domain.BillingSubscription{}, ErrNoSubscription
}

// VerifyWebhook rejects every call. The noop is never wired in
// production so an operator can never accidentally point a webhook at
// it; tests use a real provider (or a stub that satisfies the
// interface) for webhook path coverage.
func (p *Provider) VerifyWebhook(headers http.Header, body []byte) (domain.NormalizedEvent, error) {
	return nil, ErrInvalidSignature
}

// shortHash returns the first 16 hex chars of SHA-256(s). Used to
// mint deterministic-looking session ids without pulling in a uuid dep.
func shortHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:8])
}
