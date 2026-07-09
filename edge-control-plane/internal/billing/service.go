// Package billing (service.go) — the merchant-agnostic business logic
// for the billing surface. Sits between the HTTP handler
// (handler/billing.go) and the provider implementations
// (billing/stripe, billing/noop) plus the billing repository
// (repository/billing.go).
//
// The service's job is to make the database the source of truth for
// tenant↔subscription state, and treat the provider as an opaque
// "open checkout / receive webhook" pair. Every webhook delivery is
// first persisted (idempotency via PRIMARY KEY on event_id) and only
// then dispatched, so a Stripe redelivery never double-applies a
// state change.
package billing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
)

// ErrTenantNotFound is returned when a webhook event references a
// tenant we don't recognize. We do NOT auto-create a tenant from a
// webhook — onboarding must go through POST /api/v1/tenants first.
var ErrTenantNotFound = errors.New("billing: tenant not found")

// ErrNoSubscription is returned by OpenPortal when the tenant has
// never started a checkout. The handler translates to 404.
var ErrNoSubscription = errors.New("billing: no subscription for tenant")

// ErrTenantUnresolved is returned by HandleWebhook when an inbound
// event has no embedded tenant AND no matching
// (provider, provider_customer_id) row. Maps to HTTP 422.
var ErrTenantUnresolved = errors.New("billing: tenant unresolved for event")

// TenantUpdater is the subset of *service.TenantService the billing
// service depends on. Pulled out as an interface so tests can stub
// plan changes without a DB.
type TenantUpdater interface {
	UpdateTenantPlan(ctx context.Context, tenantID, newPlan string, applyQuotaDefaults bool) error
}

// BillingServiceInterface is the seam handler/billing.go depends on.
// The handler can be unit-tested against a mock that satisfies this
// interface (see handler/billing_test.go).
type BillingServiceInterface interface {
	StartCheckout(ctx context.Context, tenantID, plan string) (CheckoutSession, error)
	OpenPortal(ctx context.Context, tenantID, returnURL string) (PortalSession, error)
	GetSubscription(ctx context.Context, tenantID string) (domain.BillingSubscription, error)
	HandleWebhook(ctx context.Context, headers http.Header, body []byte) error
}

// BillingService is the merchant-agnostic business logic. Holds a
// single BillingProvider for the lifetime of the process (providers
// are concurrency-safe; see their package docs).
type BillingService struct {
	db         *sqlx.DB
	repo       *repository.BillingRepository
	provider   BillingProvider
	tenants    TenantUpdater
	successURL string
	cancelURL  string
}

// NewService wires the billing service. successURL / cancelURL are
// the operator-configured redirect targets the Stripe-hosted checkout
// page lands on; we pass them through on every CreateCheckoutSession
// call so the handler can stay URL-agnostic.
func NewService(
	db *sqlx.DB,
	repo *repository.BillingRepository,
	provider BillingProvider,
	tenants TenantUpdater,
	successURL, cancelURL string,
) *BillingService {
	return &BillingService{
		db:         db,
		repo:       repo,
		provider:   provider,
		tenants:    tenants,
		successURL: successURL,
		cancelURL:  cancelURL,
	}
}

// StartCheckout opens a hosted checkout page for the tenant on the
// configured plan. The first call against a fresh tenant also
// writes/upserts the billing_subscriptions row with a freshly-minted
// provider_customer_id, so subsequent OpenPortal calls can resolve
// the customer without a provider round-trip.
//
// The plan must be a known tier (domain.IsValidPlan). We deliberately
// allow "free" here so a noop flow can complete end-to-end in tests;
// the handler is the place that rejects "free" at the top of
// StartCheckout if the operator wants that gate.
func (s *BillingService) StartCheckout(ctx context.Context, tenantID, plan string) (CheckoutSession, error) {
	if !domain.IsValidPlan(plan) {
		return CheckoutSession{}, fmt.Errorf("%w: %q", domain.ErrUnknownPlan, plan)
	}
	// Persist or reuse the customer ID first so the provider can stamp
	// it on the checkout session. The noop provider is fine with an
	// empty row; the stripe provider looks up by metadata.tenant_id
	// and creates on demand. We upsert with the empty customer ID
	// here and let the provider's CreateCheckoutSession find-or-create
	// the real ID; the webhook handler updates the row with the
	// resolved customer ID.
	row, err := s.repo.GetByTenant(ctx, tenantID)
	if err != nil {
		return CheckoutSession{}, fmt.Errorf("billing: lookup subscription: %w", err)
	}
	if row == nil {
		row = &domain.BillingSubscription{
			TenantID:           tenantID,
			Provider:           s.provider.Name(),
			ProviderCustomerID: "", // resolved by provider on CreateCheckoutSession
			Plan:               plan,
			Status:             domain.SubscriptionIncomplete,
		}
		if err := s.repo.Upsert(ctx, row); err != nil {
			return CheckoutSession{}, fmt.Errorf("billing: seed subscription: %w", err)
		}
	}
	sess, err := s.provider.CreateCheckoutSession(ctx, CheckoutInput{
		TenantID:   tenantID,
		Plan:       plan,
		SuccessURL: s.successURL,
		CancelURL:  s.cancelURL,
	})
	if err != nil {
		return CheckoutSession{}, fmt.Errorf("billing: create checkout session: %w", err)
	}
	return sess, nil
}

// OpenPortal opens the provider's self-service portal. The tenant
// must already have a billing_subscriptions row; if not, the service
// returns ErrNoSubscription and the handler surfaces 404. We
// deliberately don't auto-create here — a tenant hitting /portal
// without ever having subscribed is a user error.
func (s *BillingService) OpenPortal(ctx context.Context, tenantID, returnURL string) (PortalSession, error) {
	row, err := s.repo.GetByTenant(ctx, tenantID)
	if err != nil {
		return PortalSession{}, fmt.Errorf("billing: lookup subscription: %w", err)
	}
	if row == nil {
		return PortalSession{}, ErrNoSubscription
	}
	ps, err := s.provider.CreatePortalSession(ctx, tenantID, returnURL)
	if err != nil {
		// Translate provider-specific ErrNoSubscription into our
		// package sentinel so the handler can errors.Is against one
		// stable error across providers.
		if errors.Is(err, providerSentinelNoSubscription(err)) {
			return PortalSession{}, ErrNoSubscription
		}
		return PortalSession{}, err
	}
	return ps, nil
}

// providerSentinelNoSubscription returns the provider-specific
// ErrNoSubscription if err is or wraps one. We sniff by string match
// (the providers' sentinels all have "no subscription for tenant" in
// the message) — see stripe/stripe.go:33 and noop/noop.go:25. This
// keeps the billing package from importing each provider's package.
func providerSentinelNoSubscription(err error) error {
	if err == nil {
		return nil
	}
	if s := err.Error(); strings.Contains(s, "no subscription for tenant") {
		return err
	}
	return nil
}

// GetSubscription returns the local billing_subscriptions row. The
// provider is NOT consulted on this read path — the local row is the
// canonical mirror, updated by webhooks. This is the right call for
// v1 because the row is current within a few hundred milliseconds of
// any provider change (webhook latency) and we want reads to stay
// off the merchant's hot path.
func (s *BillingService) GetSubscription(ctx context.Context, tenantID string) (domain.BillingSubscription, error) {
	row, err := s.repo.GetByTenant(ctx, tenantID)
	if err != nil {
		return domain.BillingSubscription{}, fmt.Errorf("billing: lookup subscription: %w", err)
	}
	if row == nil {
		return domain.BillingSubscription{}, ErrNoSubscription
	}
	return *row, nil
}

// HandleWebhook is the single ingress for provider webhook deliveries.
// The order of operations matters:
//
//  1. Verify signature via the provider. On failure: return
//     ErrInvalidSignature (handler → 400 → Stripe retries).
//  2. TryRecordEvent in a tx. PRIMARY KEY (event_id) makes this
//     race-free; on duplicate we return nil and let Stripe mark the
//     delivery successful.
//  3. Resolve tenant: prefer the event's TenantID, fall back to
//     (provider, provider_customer_id) lookup. If still unresolved:
//     422.
//  4. Dispatch by event type inside the same tx (so the dispatch
//     either fully applies or not at all).
//  5. Mark processed_at.
//
// Steps 2-5 happen inside repository.Transaction so a crash between
// TryRecordEvent and the dispatch leaves processed_at NULL and the
// operator can replay by deleting the row. Steps 2-5 are also
// idempotent at the row level: a re-run of the tx re-runs the
// upserts, which is fine because Upsert is "set to current state"
// not "transition from prior state".
func (s *BillingService) HandleWebhook(ctx context.Context, headers http.Header, body []byte) error {
	evt, err := s.provider.VerifyWebhook(headers, body)
	if err != nil {
		// Signature or parse failure — handler maps to 400.
		return err
	}
	payloadHash := sha256Hex(body)

	// Step 1: persist the event row. The transaction also encloses
	// the dispatch so the entire "first time we see this event_id"
	// sequence is atomic.
	return repository.Transaction(ctx, s.db, func(tx *sqlx.Tx) error {
		txRepo := s.repo.WithTx(tx)

		// Step 2: resolve tenant. Done OUTSIDE the tx above so the
		// lookup reads the committed billing_subscriptions row. We
		// re-resolve inside the tx for the (provider, customer)
		// case to read the current row state.
		tenantID := evt.TenantID()
		if tenantID == "" {
			// Stripe's customer.subscription.* events don't carry
			// client_reference_id. Fall back to provider_customer_id.
			// The provider object exposes the customer id when
			// available; we extract via a small adapter below.
			custID := customerIDFromEvent(evt)
			if custID == "" {
				// Roll forward: still record the event so the
				// operator can see it landed, but with a NULL
				// tenant_id. The handler then 422s. Without this
				// row the event would be lost.
				_, err := txRepo.TryRecordEvent(ctx, &domain.BillingEvent{
					EventID:     evt.EventID(),
					Provider:    evt.Provider(),
					EventType:   evt.EventType(),
					TenantID:    nil,
					PayloadHash: payloadHash,
				})
				if err != nil {
					return fmt.Errorf("billing: record event: %w", err)
				}
				return ErrTenantUnresolved
			}
			row, err := txRepo.ListByProviderCustomer(ctx, evt.Provider(), custID)
			if err != nil {
				return fmt.Errorf("billing: lookup by customer: %w", err)
			}
			if row == nil {
				_, err := txRepo.TryRecordEvent(ctx, &domain.BillingEvent{
					EventID:     evt.EventID(),
					Provider:    evt.Provider(),
					EventType:   evt.EventType(),
					TenantID:    nil,
					PayloadHash: payloadHash,
				})
				if err != nil {
					return fmt.Errorf("billing: record event: %w", err)
				}
				return ErrTenantUnresolved
			}
			tenantID = row.TenantID
		}

		// Step 3: dedup. TryRecordEvent returns (true, nil) the
		// first time, (false, nil) on every subsequent replay. We
		// capture the dedup decision AFTER resolving tenant so an
		// unresolved event still gets a row (for ops visibility).
		tenantPtr := tenantID
		recorded, err := txRepo.TryRecordEvent(ctx, &domain.BillingEvent{
			EventID:     evt.EventID(),
			Provider:    evt.Provider(),
			EventType:   evt.EventType(),
			TenantID:    &tenantPtr,
			PayloadHash: payloadHash,
		})
		if err != nil {
			return fmt.Errorf("billing: record event: %w", err)
		}
		if !recorded {
			// Already processed; nothing to do. The dispatch is
			// idempotent so re-running it is safe, but we skip
			// for log clarity.
			return nil
		}

		// Step 4: dispatch.
		if err := s.dispatch(ctx, txRepo, tenantID, evt); err != nil {
			return err
		}

		// Step 5: stamp processed_at.
		return txRepo.MarkProcessed(ctx, evt.EventID())
	})
}

// dispatch applies the effect of a NormalizedEvent onto the tenant's
// billing_subscriptions row and (for the cases that change plan)
// the tenants row. Each case is a pure upsert — re-running the same
// event with the same payload is a no-op.
func (s *BillingService) dispatch(
	ctx context.Context,
	repo *repository.BillingRepository,
	tenantID string,
	evt domain.NormalizedEvent,
) error {
	row, err := repo.GetByTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("billing: lookup subscription: %w", err)
	}
	if row == nil {
		// No row yet. The first event for a tenant is
		// checkout.session.completed; without that bootstrap the
		// tenant has no plan to mirror. Return an error so the
		// operator can investigate.
		return fmt.Errorf("%w: %s", ErrTenantNotFound, tenantID)
	}

	// Apply deltas. Plan and status come from the event when present;
	// otherwise we leave the row's prior values alone (events like
	// payment.failed only carry status, not plan).
	if p := evt.Plan(); p != "" {
		row.Plan = p
	}
	if st := evt.Status(); st != "" {
		row.Status = domain.SubscriptionStatus(st)
	}
	// SubscriptionID is only set on the first checkout.completed;
	// updates and deletes don't change it.
	if evt.EventType() == domain.EventCheckoutCompleted && row.ProviderSubscriptionID == "" {
		// The Stripe provider doesn't surface subscription_id from
		// the checkout.session.completed payload directly; the
		// follow-up customer.subscription.updated does. We leave
		// the field empty here and let the next event fill it.
	}

	row.Provider = evt.Provider()
	// updated_at is set in the repo via NOW(); we only flip the
	// user-visible fields here.
	if err := repo.Upsert(ctx, row); err != nil {
		return fmt.Errorf("billing: upsert subscription: %w", err)
	}

	// Side effect: push the plan onto tenants so the rest of the
	// system (quotas, deployments) sees the new tier. The apply-
	// quota-defaults knob is false because the merchant's checkout
	// flow doesn't recompute quotas mid-cycle.
	switch evt.EventType() {
	case domain.EventCheckoutCompleted, domain.EventSubscriptionUpdated:
		if row.Status == domain.SubscriptionActive {
			if err := s.tenants.UpdateTenantPlan(ctx, tenantID, row.Plan, false); err != nil {
				return fmt.Errorf("billing: update tenant plan: %w", err)
			}
		}
	case domain.EventSubscriptionDeleted:
		// Cancellation → drop back to free. The quota row is
		// rewritten by UpdateTenantPlan via applyQuotaDefaults=true
		// in the service layer's no-quota-change path; for the
		// downgrade case we DO want the free-tier quota to take
		// effect immediately.
		if err := s.tenants.UpdateTenantPlan(ctx, tenantID, "free", true); err != nil {
			return fmt.Errorf("billing: downgrade tenant to free: %w", err)
		}
	case domain.EventPaymentFailed:
		// No plan change; just stamp past_due (already done above).
		// A future enhancement (#300c) will read status from this
		// row to enforce suspension.
	}
	return nil
}

// customerIDFromEvent pulls the provider's customer id out of a
// NormalizedEvent when the event has one embedded. Today, only
// Stripe's checkout.session.completed carries it (in the same
// client_reference_id the tenant id is in, but the customer id is
// pulled separately). For all other event types / providers we
// return "" and let the service fall back to the
// (provider, customer_id) DB lookup — which fails open to 422 if no
// row matches.
//
// Implementations that know how to extract the customer id can wrap
// NormalizedEvent; today no provider does, so this is a forward
// extension point.
func customerIDFromEvent(_ domain.NormalizedEvent) string { return "" }

// sha256Hex returns the lowercase hex SHA-256 of body. Stored on
// billing_events.payload_hash for forensic comparison in case a
// provider replays a corrupted payload.
func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
