// Package billing hosts the merchant-agnostic billing seam and the
// concrete provider implementations that sit behind it.
//
// Architecture:
//
//	service.BillingService → billing.BillingProvider (this interface)
//	                              ├── billing/stripe  (real)
//	                              └── billing/noop    (dev/CI/test)
//
// The service depends only on the interface; provider sub-packages
// depend on their merchant SDK and never on the service. This keeps
// the (potentially heavy) stripe-go dep scoped to the stripe/
// sub-package so a future provider can swap in without dragging it
// along.
package billing

import (
	"context"
	"net/http"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// CheckoutInput is the merchant-agnostic input to CreateCheckoutSession.
// TenantID becomes the merchant's client_reference_id (Stripe uses this
// for webhook correlation); SuccessURL / CancelURL are merchant-specific
// redirect targets the provider threads onto its hosted checkout page.
type CheckoutInput struct {
	TenantID   string
	Plan       string // domain plan name; provider maps to its own price_id
	SuccessURL string
	CancelURL  string
}

// CheckoutSession is what every provider returns from
// CreateCheckoutSession. The handler redirects the tenant's browser to
// URL and persists the ID locally so a follow-up webhook can be
// correlated back to the tenant even after the URL is consumed.
type CheckoutSession struct {
	ID        string
	URL       string
	ExpiresAt time.Time
}

// PortalSession is what every provider returns from
// CreatePortalSession. URL is the hosted self-service portal where the
// tenant manages their subscription.
type PortalSession struct {
	URL string
}

// StripeConfig is the per-provider configuration block in
// cfg.Billing.Stripe. The factory in internal/app/app.go passes this
// through to stripe.NewProvider.
//
// PriceIDs is a plan→price_id map so the same operator can run free /
// pro / business through different Stripe products without changing
// code. ValidateBillingConfig enforces that every domain.Plans()
// entry has a matching PriceIDs entry when provider == "stripe".
type StripeConfig struct {
	SecretKey      string
	WebhookSecret  string
	PublishableKey string
	PriceIDs       map[string]string
	APIBase        string // override for stripe-mock testing; empty → default
}

// BillingProvider is the seam. Every method must be safe to call
// concurrently — the service holds a single instance for the lifetime
// of the process.
//
// Implementations:
//   - internal/billing/stripe  — wraps stripe-go; the production impl
//   - internal/billing/noop    — always-succeeds; dev/CI only
//
// Adding a new provider = a new sub-package implementing this
// interface plus a one-line switch in app.go's newBillingProvider.
type BillingProvider interface {
	// Name returns the provider identifier stored verbatim in
	// billing_subscriptions.provider / billing_events.provider. Must
	// match one of domain.ProviderStripe / domain.ProviderNoop (or a
	// new constant when adding a new provider).
	Name() domain.BillingProvider

	// CreateCheckoutSession opens a hosted checkout page for the
	// given plan. Implementations create or reuse a provider customer
	// based on the tenant's existing billing_subscriptions row, then
	// mint a Checkout Session tied to that customer.
	//
	// The returned ID is persisted on billing_subscriptions so the
	// resulting checkout.session.completed webhook can be correlated
	// back to the tenant.
	CreateCheckoutSession(ctx context.Context, in CheckoutInput) (CheckoutSession, error)

	// CreatePortalSession opens the hosted self-service portal for
	// the tenant. The caller must have already created a
	// billing_subscriptions row (StartCheckout handles that on first
	// call); providers return ErrNoSubscription when none exists.
	CreatePortalSession(ctx context.Context, tenantID string, returnURL string) (PortalSession, error)

	// GetSubscription fetches the live merchant-side subscription
	// state. The service only uses this on rare paths (manual
	// resync); the canonical read path is the local
	// billing_subscriptions row.
	GetSubscription(ctx context.Context, tenantID string) (domain.BillingSubscription, error)

	// VerifyWebhook signature-verifies and parses the inbound body in
	// one step. Returns a NormalizedEvent whose TenantID may be empty
	// if the provider couldn't derive it from the payload — the
	// service fills it from billing_subscriptions via
	// (provider, provider_customer_id) when empty.
	//
	// On signature failure the implementation returns ErrInvalidSignature
	// (or wraps it); the handler translates that to HTTP 400 and the
	// merchant retries.
	VerifyWebhook(headers http.Header, body []byte) (domain.NormalizedEvent, error)
}
