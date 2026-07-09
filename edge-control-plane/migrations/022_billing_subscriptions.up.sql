-- +migrate Up
-- 022_billing_subscriptions.up.sql
-- Per-tenant billing-subscription mirror for issue #419. Holds the
-- locally-cached state of whatever external merchant (Stripe today,
-- plus future providers behind internal/billing.BillingProvider)
-- the tenant is paying through. Read by:
--
--   * GET /api/v1/billing/subscription — handler returns the row
--     directly, no merchant round-trip.
--   * The billing webhook handler — looks up tenant_id by
--     (provider, provider_customer_id) when an inbound event doesn't
--     carry the tenant directly (Stripe's customer.subscription.*
--     events don't include client_reference_id).
--   * service.TenantService — reads tenants.plan alongside this row
--     to decide plan-limits enforcement.
--
-- Schema notes:
--
--   * PRIMARY KEY on tenant_id: at most one active row per tenant.
--     If a tenant switches providers mid-cycle (future work), the
--     operator will delete the old row first; we never carry two
--     active providers for the same tenant.
--
--   * provider_customer_id is NOT NULL: every active row has been
--     through at least a CreateCustomer call. The Stripe webhook
--     handler also needs the customer_id to resolve tenant from a
--     subscription.* event.
--
--   * provider_subscription_id is NULLABLE: a row exists from the
--     moment the tenant clicks "Subscribe" (StartCheckout), before
--     checkout.session.completed lands. Without NULL we'd force an
--     extra UPDATE in the checkout flow.
--
--   * status mirrors the merchant's vocabulary one-to-one: Stripe
--     states are active | past_due | canceled | incomplete | ...
--     We don't translate them — the webhook handler stores the
--     literal Stripe status so debugging matches the dashboard.
--
--   * current_period_end is NULLABLE: Stripe sets it only on
--     active/trialing subscriptions. past_due / canceled / incomplete
--     rows may not have one yet.
--
--   * cancel_at_period_end mirrors the merchant's flag directly.
--     On true at period end, Stripe sends customer.subscription.deleted
--     and we flip plan to 'free' in the webhook handler.
--
-- ON DELETE CASCADE on tenant_id: deleting a tenant (admin path)
-- cleans up its billing row in the same transaction. No orphaned
-- billing rows survive tenant deletion.
CREATE TABLE IF NOT EXISTS billing_subscriptions (
    tenant_id                VARCHAR(64)  PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    provider                 VARCHAR(32)  NOT NULL,
    provider_customer_id     VARCHAR(128) NOT NULL,
    provider_subscription_id VARCHAR(128),
    plan                     VARCHAR(32)  NOT NULL,
    status                   VARCHAR(32)  NOT NULL,
    current_period_end       TIMESTAMPTZ,
    cancel_at_period_end     BOOLEAN      NOT NULL DEFAULT false,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Webhook hot path: customer.subscription.* events arrive without
-- the tenant id, so the handler does
--   SELECT tenant_id FROM billing_subscriptions
--    WHERE provider = $1 AND provider_customer_id = $2
-- This composite index keeps that lookup O(log n) as the fleet grows.
CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_provider_customer
    ON billing_subscriptions (provider, provider_customer_id);