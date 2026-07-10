package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// BillingRepository handles persistence for billing_subscriptions
// and billing_events (issue #419). Same DBTX + WithTx shape as every
// other repository in this package — see repository/webhook.go:12-26
// for the canonical reference.
type BillingRepository struct {
	db DBTX
}

func NewBillingRepository(db *sqlx.DB) *BillingRepository {
	return &BillingRepository{db: db}
}

// WithTx returns a new BillingRepository using the provided
// transaction. Used by BillingService.HandleWebhook so the
// "record event + apply effect" sequence is atomic.
func (r *BillingRepository) WithTx(tx *sqlx.Tx) *BillingRepository {
	return &BillingRepository{db: tx}
}

// GetByTenant returns the billing_subscriptions row for a tenant, or
// (nil, nil) if none exists. Callers distinguish "no row" from
// "error" with the standard sql.ErrNoRows pattern via the second
// return.
func (r *BillingRepository) GetByTenant(ctx context.Context, tenantID string) (*domain.BillingSubscription, error) {
	var s domain.BillingSubscription
	const q = `SELECT tenant_id, provider, provider_customer_id, provider_subscription_id,
	                  plan, status, current_period_end, cancel_at_period_end, created_at, updated_at
	             FROM billing_subscriptions WHERE tenant_id = $1`
	err := r.db.GetContext(ctx, &s, q, tenantID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &s, err
}

// ListByProviderCustomer looks up a tenant by (provider, customer_id).
// Used by the webhook handler when an inbound event has no embedded
// tenant (Stripe's customer.subscription.* events don't carry
// client_reference_id).
func (r *BillingRepository) ListByProviderCustomer(ctx context.Context, provider domain.BillingProvider, customerID string) (*domain.BillingSubscription, error) {
	var s domain.BillingSubscription
	const q = `SELECT tenant_id, provider, provider_customer_id, provider_subscription_id,
	                  plan, status, current_period_end, cancel_at_period_end, created_at, updated_at
	             FROM billing_subscriptions
	            WHERE provider = $1 AND provider_customer_id = $2`
	err := r.db.GetContext(ctx, &s, q, provider, customerID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &s, err
}

// Upsert writes a billing_subscriptions row keyed on tenant_id. Used
// by StartCheckout on first call (creates the row with the provider's
// customer ID) and by every webhook handler dispatch (updates plan /
// status / period_end / subscription_id).
//
// updated_at is set to NOW() so a downstream observer can tell when
// the row last changed.
func (r *BillingRepository) Upsert(ctx context.Context, s *domain.BillingSubscription) error {
	const q = `
INSERT INTO billing_subscriptions (
    tenant_id, provider, provider_customer_id, provider_subscription_id,
    plan, status, current_period_end, cancel_at_period_end, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW(),NOW())
ON CONFLICT (tenant_id) DO UPDATE SET
    provider                 = EXCLUDED.provider,
    provider_customer_id     = EXCLUDED.provider_customer_id,
    provider_subscription_id = EXCLUDED.provider_subscription_id,
    plan                     = EXCLUDED.plan,
    status                   = EXCLUDED.status,
    current_period_end       = EXCLUDED.current_period_end,
    cancel_at_period_end     = EXCLUDED.cancel_at_period_end,
    updated_at               = NOW()
`
	_, err := r.db.ExecContext(ctx, q,
		s.TenantID, string(s.Provider), s.ProviderCustomerID, s.ProviderSubscriptionID,
		s.Plan, string(s.Status), s.CurrentPeriodEnd, s.CancelAtPeriodEnd)
	return err
}

// TryRecordEvent inserts a billing_events row. Returns (true, nil)
// when a new row was inserted (caller proceeds with dispatch) and
// (false, nil) when an existing row with the same event_id was found
// (caller treats as already-processed, no-op).
//
// The PRIMARY KEY on event_id makes this race-free: two concurrent
// webhook deliveries for the same event_id will land one INSERT
// (returns 1) and one ON CONFLICT DO NOTHING (returns 0). The
// affected-row count is the only signal we need.
func (r *BillingRepository) TryRecordEvent(ctx context.Context, e *domain.BillingEvent) (bool, error) {
	const q = `
INSERT INTO billing_events (event_id, provider, event_type, tenant_id, received_at, payload_hash)
VALUES ($1, $2, $3, $4, NOW(), $5)
ON CONFLICT (event_id) DO NOTHING
`
	res, err := r.db.ExecContext(ctx, q, e.EventID, string(e.Provider), string(e.EventType), e.TenantID, e.PayloadHash)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// MarkProcessed stamps processed_at on the event row after the
// dispatch completes. Called at the end of HandleWebhook's tx so a
// crash mid-dispatch leaves processed_at NULL — operators can grep
// for that to find stuck events.
func (r *BillingRepository) MarkProcessed(ctx context.Context, eventID string) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE billing_events SET processed_at = $2 WHERE event_id = $1`,
		eventID, time.Now().UTC())
	return err
}

// GetSubscriptionStatus returns just the status column for the tenant's
// billing_subscriptions row. Cheaper than GetByTenant (single column read
// vs full row) and avoids the temptation to leak the rest of the row out
// of the deploy-time path. Returns ("", nil) when the tenant has never
// been through StartCheckout — the caller treats that as "no paid
// subscription, fall through to the free-tier checks" rather than as an
// error. Added in issue #420.
func (r *BillingRepository) GetSubscriptionStatus(ctx context.Context, tenantID string) (domain.SubscriptionStatus, error) {
	var status string
	const q = `SELECT status FROM billing_subscriptions WHERE tenant_id = $1`
	err := r.db.GetContext(ctx, &status, q, tenantID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return domain.SubscriptionStatus(status), err
}

// ListEventsByTenant returns the billing_events rows for a tenant whose
// received_at falls within [from, to], ordered by received_at DESC, capped
// at limit. Backs the GET /api/v1/usage subscription-event timeline
// (issue #421).
//
// The query uses the partial index idx_billing_events_tenant_received
// (tenant_id, received_at DESC) WHERE tenant_id IS NOT NULL (migration
// 023:62-63) — the planner picks it because we filter on tenant_id and
// order by received_at.
//
// limit must be > 0; callers clamp before reaching the repository (the
// service layer caps at 200). An empty result is (nil, nil), NOT
// ([]BillingEvent{}, nil) — sqlx's SelectContext already returns a nil
// slice for zero rows, so callers should len()-check rather than nil-check
// if they need to distinguish "no events" from "db error".
func (r *BillingRepository) ListEventsByTenant(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]domain.BillingEvent, error) {
	const q = `SELECT event_id, provider, event_type, tenant_id, received_at, processed_at, payload_hash
	             FROM billing_events
	            WHERE tenant_id = $1
	              AND received_at >= $2 AND received_at <= $3
	            ORDER BY received_at DESC
	            LIMIT $4`
	var out []domain.BillingEvent
	if err := r.db.SelectContext(ctx, &out, q, tenantID, from, to, limit); err != nil {
		return nil, err
	}
	return out, nil
}
