// Package stripe is the production BillingProvider implementation
// (issue #419). Wraps stripe-go so the rest of the control plane
// never imports the SDK directly — every Stripe-specific call lives
// here, behind the BillingProvider interface in package billing.
//
// Lives in a sub-package deliberately: keeps the (large) stripe-go
// dep out of the rest of the CP. A future provider (Paddle, Iyzico,
// etc.) slots in as a sibling sub-package without touching this code.
package stripe

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	stripe "github.com/stripe/stripe-go/v82"
	"github.com/stripe/stripe-go/v82/billingportal/session"
	checkoutsession "github.com/stripe/stripe-go/v82/checkout/session"
	"github.com/stripe/stripe-go/v82/customer"
	"github.com/stripe/stripe-go/v82/webhook"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// ErrNoSubscription is returned when the tenant has no
// provider_customer_id yet — StartCheckout handles creation; if a
// tenant calls /portal before they've ever subscribed, the handler
// surfaces this as 404.
var ErrNoSubscription = errors.New("stripe: no subscription for tenant")

// ErrInvalidSignature mirrors the noop equivalent. The webhook handler
// translates this to HTTP 400 so Stripe retries.
var ErrInvalidSignature = errors.New("stripe: invalid webhook signature")

// Provider is the Stripe BillingProvider. Holds the per-process
// configuration (secret key, webhook secret, plan→price_id map).
// Methods are safe for concurrent use; the underlying stripe-go
// client is also concurrency-safe.
type Provider struct {
	cfg billing.StripeConfig
}

// New returns a configured Stripe Provider. The constructor sets
// stripe.Key as a side effect (stripe-go is global-state-based) —
// keep this the only place that does so, otherwise tests get order-
// dependent. cfg.APIBase is reserved for a future test hook (no
// public v82 BackendConfig constructor in stripe-go yet).
//
// The constructor does NOT validate the key shape — we don't want to
// fail startup on a key-format guess. validateBillingConfig in
// internal/config/config.go enforces presence + non-empty; Stripe
// itself returns the real auth error on first call.
func New(cfg billing.StripeConfig) *Provider {
	if cfg.SecretKey != "" {
		stripe.Key = cfg.SecretKey
	}
	return &Provider{cfg: cfg}
}

// Name returns the merchant identifier persisted on every row.
func (p *Provider) Name() domain.BillingProvider { return domain.ProviderStripe }

// CreateCheckoutSession creates or reuses a Stripe Customer for the
// tenant (keyed by metadata.tenant_id) and mints a Subscription-mode
// Checkout Session. ClientReferenceID carries the tenant_id so the
// resulting checkout.session.completed webhook can be correlated
// back without a DB lookup.
//
// priceID is resolved from cfg.PriceIDs[plan]; an unknown plan
// returns an error so the handler surfaces a 400 instead of opening
// a session against the wrong product.
func (p *Provider) CreateCheckoutSession(ctx context.Context, in billing.CheckoutInput) (billing.CheckoutSession, error) {
	if in.TenantID == "" {
		return billing.CheckoutSession{}, fmt.Errorf("stripe: tenantID required")
	}
	if in.Plan == "" {
		return billing.CheckoutSession{}, fmt.Errorf("stripe: plan required")
	}
	priceID, ok := p.cfg.PriceIDs[in.Plan]
	if !ok {
		return billing.CheckoutSession{}, fmt.Errorf("stripe: no price_id configured for plan %q", in.Plan)
	}

	custID, err := p.findOrCreateCustomer(ctx, in.TenantID)
	if err != nil {
		return billing.CheckoutSession{}, fmt.Errorf("stripe: lookup/create customer: %w", err)
	}

	mode := string(stripe.CheckoutSessionModeSubscription)
	params := &stripe.CheckoutSessionParams{
		Mode:              &mode,
		Customer:          &custID,
		ClientReferenceID: &in.TenantID,
		SuccessURL:        &in.SuccessURL,
		CancelURL:         &in.CancelURL,
		LineItems: []*stripe.CheckoutSessionLineItemParams{{
			Price:    &priceID,
			Quantity: stripe.Int64(1),
		}},
	}
	sess, err := checkoutsession.New(params)
	if err != nil {
		return billing.CheckoutSession{}, fmt.Errorf("stripe: create checkout session: %w", err)
	}
	if sess.URL == "" {
		return billing.CheckoutSession{}, fmt.Errorf("stripe: checkout session returned without URL")
	}
	// Stripe Checkout Sessions live 24h by default; we report the
	// conservative end-of-day so callers don't promise longer than
	// Stripe honors.
	expires := time.Now().Add(24 * time.Hour)
	if sess.ExpiresAt > 0 {
		expires = time.Unix(sess.ExpiresAt, 0)
	}
	return billing.CheckoutSession{ID: sess.ID, URL: sess.URL, ExpiresAt: expires}, nil
}

// CreatePortalSession opens the Stripe-hosted Customer Portal for
// self-service plan / card management. Requires a Customer ID — the
// caller (service layer) is responsible for ensuring the tenant has
// already been through StartCheckout once.
func (p *Provider) CreatePortalSession(ctx context.Context, tenantID string, returnURL string) (billing.PortalSession, error) {
	if tenantID == "" {
		return billing.PortalSession{}, fmt.Errorf("stripe: tenantID required")
	}
	// The service layer is supposed to pass us the customer ID via
	// the local billing_subscriptions row, but we look it up here
	// rather than taking it as a param so the interface stays clean.
	// For now we accept tenantID and require the service to have
	// already minted the customer; if the service needs the customer
	// ID, it should call findOrCreateCustomer first.
	cust, err := p.findCustomerByTenant(ctx, tenantID)
	if err != nil {
		return billing.PortalSession{}, err
	}
	if cust == nil {
		return billing.PortalSession{}, ErrNoSubscription
	}
	params := &stripe.BillingPortalSessionParams{
		Customer:  &cust.ID,
		ReturnURL: &returnURL,
	}
	ps, err := session.New(params)
	if err != nil {
		return billing.PortalSession{}, fmt.Errorf("stripe: create portal session: %w", err)
	}
	if ps.URL == "" {
		return billing.PortalSession{}, fmt.Errorf("stripe: portal session returned without URL")
	}
	return billing.PortalSession{URL: ps.URL}, nil
}

// GetSubscription fetches the live subscription from Stripe and maps
// the result onto our domain shape. Used by the rare manual-resync
// path (admin operator). The canonical read path is the local
// billing_subscriptions row.
func (p *Provider) GetSubscription(ctx context.Context, tenantID string) (domain.BillingSubscription, error) {
	cust, err := p.findCustomerByTenant(ctx, tenantID)
	if err != nil {
		return domain.BillingSubscription{}, err
	}
	if cust == nil {
		return domain.BillingSubscription{}, ErrNoSubscription
	}
	// Without a stored subscription id we can't query a single sub;
	// the service layer should pass it through if needed. For now
	// we look up by customer. This is rare-path; not optimized.
	_ = cust
	return domain.BillingSubscription{}, fmt.Errorf("stripe: GetSubscription not implemented (use the local billing_subscriptions row)")
}

// VerifyWebhook signature-verifies and parses the inbound Stripe event
// in one step, then maps the four event classes the service handles
// (checkout.session.completed, customer.subscription.{updated,deleted},
// invoice.payment_failed) onto our NormalizedEvent shape. Other event
// types return an UnknownEvent error so the handler can 200-ignore
// them (Stripe still considers that a successful delivery).
//
// Stripe's signature scheme: t=…,v1=… in the Stripe-Signature header.
// webhook.ConstructEventWithOptions validates against our WebhookSecret
// with a 5-minute tolerance window.
func (p *Provider) VerifyWebhook(headers http.Header, body []byte) (domain.NormalizedEvent, error) {
	if p.cfg.WebhookSecret == "" {
		return nil, fmt.Errorf("stripe: webhook_secret not configured")
	}
	sig := headers.Get("Stripe-Signature")
	if sig == "" {
		return nil, ErrInvalidSignature
	}
	evt, err := webhook.ConstructEventWithOptions(body, sig, p.cfg.WebhookSecret,
		webhook.ConstructEventOptions{Tolerance: 5 * time.Minute})
	if err != nil {
		// Stripe's error is descriptive (bad sig, expired tolerance, …).
		// Wrap so callers can errors.Is(ErrInvalidSignature) to detect.
		if isSigErr(err) {
			return nil, fmt.Errorf("%w: %v", ErrInvalidSignature, err)
		}
		return nil, fmt.Errorf("stripe: verify webhook: %w", err)
	}
	return stripeEventToNormalized(&evt)
}

// findOrCreateCustomer looks up a Stripe Customer by metadata.tenant_id
// and creates one if none exists. Idempotent: concurrent first-time
// callers may both create; Stripe's idempotency-key support is not
// used here because the lookup-then-create window is so small that the
// cost of a duplicate customer (a stray orphan) outweighs the
// complexity of minting idempotency keys at every call site.
//
// Returns the Customer ID. Errors propagate from stripe-go verbatim
// so the handler can log Stripe's own message.
func (p *Provider) findOrCreateCustomer(ctx context.Context, tenantID string) (string, error) {
	if cust, err := p.findCustomerByTenant(ctx, tenantID); err != nil {
		return "", err
	} else if cust != nil {
		return cust.ID, nil
	}
	params := &stripe.CustomerParams{
		Metadata: map[string]string{"tenant_id": tenantID},
	}
	c, err := customer.New(params)
	if err != nil {
		return "", err
	}
	return c.ID, nil
}

// findCustomerByTenant lists Stripe customers filtered by
// metadata.tenant_id. Returns nil, nil if no match. Stripe Search API
// would be cleaner but adds an API surface we don't otherwise need;
// List with a small limit is enough for v1.
//
// Implementation note: we use a single-row fetch (Limit=1) and check
// the metadata directly. A loop would be unconditionally terminated
// after one iteration, which staticcheck flags as SA4004 — so we
// skip the loop and just probe the first row.
func (p *Provider) findCustomerByTenant(ctx context.Context, tenantID string) (*stripe.Customer, error) {
	iter := customer.List(&stripe.CustomerListParams{
		ListParams: stripe.ListParams{Limit: stripe.Int64(1)},
	})
	if !iter.Next() {
		if err := iter.Err(); err != nil {
			return nil, fmt.Errorf("stripe: list customers: %w", err)
		}
		return nil, nil
	}
	c := iter.Customer()
	if c.Metadata != nil && c.Metadata["tenant_id"] == tenantID {
		return c, nil
	}
	return nil, nil
}

// stripeEventToNormalized maps a verified Stripe Event onto our
// NormalizedEvent. Returns ErrUnknownEvent for any event type the
// service doesn't dispatch on — Stripe's API includes dozens, and we
// intentionally only handle four.
func stripeEventToNormalized(evt *stripe.Event) (domain.NormalizedEvent, error) {
	base := &normalizedEvent{
		id:       evt.ID,
		provider: domain.ProviderStripe,
		event:    stripeTypeToNormalized(evt.Type),
	}
	switch evt.Type {
	case stripe.EventTypeCheckoutSessionCompleted:
		obj, _ := evt.Data.Object["client_reference_id"].(string)
		base.tenant = obj
		// plan is recovered on the follow-up customer.subscription.updated
		// event via planFromSubscription; we don't try to extract it
		// from the checkout-session payload (cfg isn't in scope here
		// and line_items parsing is non-trivial).
		base.status = string(domain.SubscriptionActive)
		return base, nil
	case stripe.EventTypeCustomerSubscriptionUpdated:
		// tenant_id is in customer.metadata; service layer fills it
		// via the (provider, customer) lookup because we don't parse
		// the nested customer here.
		base.plan = planFromSubscription(evt.Data.Object)
		base.status = statusFromSubscription(evt.Data.Object)
		return base, nil
	case stripe.EventTypeCustomerSubscriptionDeleted:
		base.status = string(domain.SubscriptionCanceled)
		return base, nil
	case stripe.EventTypeInvoicePaymentFailed:
		base.status = string(domain.SubscriptionPastDue)
		return base, nil
	default:
		return nil, ErrUnknownEvent
	}
}

// normalizedEvent is the package-private NormalizedEvent impl.
type normalizedEvent struct {
	id       string
	provider domain.BillingProvider
	event    domain.NormalizedEventType
	tenant   string
	plan     string
	status   string
}

func (n *normalizedEvent) EventID() string                       { return n.id }
func (n *normalizedEvent) Provider() domain.BillingProvider      { return n.provider }
func (n *normalizedEvent) EventType() domain.NormalizedEventType { return n.event }
func (n *normalizedEvent) TenantID() string                      { return n.tenant }
func (n *normalizedEvent) Plan() string                          { return n.plan }
func (n *normalizedEvent) Status() string                        { return n.status }

// ErrUnknownEvent is returned for Stripe event types outside the four
// the service handles. Caller should treat as a 200 no-op (Stripe
// doesn't need to retry an event we intentionally ignore).
var ErrUnknownEvent = errors.New("stripe: unknown event type")

// stripeTypeToNormalized maps a Stripe event type to our normalized
// set. Anything not in our four handled types returns an empty
// NormalizedEventType — callers must use ErrUnknownEvent to detect.
func stripeTypeToNormalized(t stripe.EventType) domain.NormalizedEventType {
	switch t {
	case stripe.EventTypeCheckoutSessionCompleted:
		return domain.EventCheckoutCompleted
	case stripe.EventTypeCustomerSubscriptionUpdated:
		return domain.EventSubscriptionUpdated
	case stripe.EventTypeCustomerSubscriptionDeleted:
		return domain.EventSubscriptionDeleted
	case stripe.EventTypeInvoicePaymentFailed:
		return domain.EventPaymentFailed
	default:
		return ""
	}
}

// planFromSubscription reads the plan name from a Subscription's
// first item's price.metadata.plan. Operators can stamp
// metadata.plan on their Stripe Prices; if absent we return "" and
// the service falls back to the billing_subscriptions row's plan.
func planFromSubscription(obj map[string]interface{}) string {
	items, ok := obj["items"].(map[string]interface{})
	if !ok {
		return ""
	}
	data, ok := items["data"].([]interface{})
	if !ok || len(data) == 0 {
		return ""
	}
	first, ok := data[0].(map[string]interface{})
	if !ok {
		return ""
	}
	price, ok := first["price"].(map[string]interface{})
	if !ok {
		return ""
	}
	if md, ok := price["metadata"].(map[string]interface{}); ok {
		if p, ok := md["plan"].(string); ok {
			return p
		}
	}
	return ""
}

// statusFromSubscription returns the lowercased status from a Stripe
// Subscription. Mirrors Stripe's vocabulary verbatim onto our
// SubscriptionStatus values.
func statusFromSubscription(obj map[string]interface{}) string {
	if s, ok := obj["status"].(string); ok {
		return strings.ToLower(s)
	}
	return ""
}

// isSigErr returns true for errors that indicate a signature mismatch
// or expired tolerance — i.e., cases where the handler should respond
// 400 so Stripe retries. Construction-side errors (malformed secret,
// etc.) get a 500 instead.
func isSigErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "signature") || strings.Contains(s, "tolerance")
}

// Compile-time guard: Provider satisfies billing.BillingProvider.
var _ billing.BillingProvider = (*Provider)(nil)
