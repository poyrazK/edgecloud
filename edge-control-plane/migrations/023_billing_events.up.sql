-- +migrate Up
-- 023_billing_events.up.sql
-- Webhook idempotency log for issue #419. Every inbound merchant
-- event (Stripe today, future providers behind the BillingProvider
-- interface) is recorded here on receipt. The PRIMARY KEY enforces
-- dedup at the DB layer: re-deliveries from the merchant hit
-- ON CONFLICT (event_id) DO NOTHING and the affected-row count of
-- zero tells the handler "already processed, no-op".
--
-- Schema notes:
--
--   * event_id is the merchant's event id (Stripe: evt_…). VARCHAR(128)
--     leaves room for future providers with longer ids without a
--     follow-up migration.
--
--   * provider is the merchant that emitted the event — written at
--     insert time so two providers' events never collide on event_id.
--     If a future provider recycles ids, the (provider, event_id)
--     pair is still unique.
--
--   * event_type is the normalized merchant-agnostic event type
--     (checkout.completed, subscription.updated, subscription.deleted,
--     payment.failed). Stored verbatim so an operator can grep for
--     a specific event class without joining through provider tables.
--
--   * tenant_id is NULLABLE: Stripe's customer.subscription.updated
--     event has no tenant in the payload — the handler resolves it
--     via (provider, provider_customer_id) before persisting. The
--     NOT-NULL flavor is enforced at the application layer, not the
--     DB, so a vendor-specific event that genuinely has no tenant
--     (e.g. account-level notifications) can still be logged.
--
--   * payload_hash is SHA-256 hex of the raw body. Stored so an
--     operator can verify the bytes we processed match what the
--     merchant claims it sent, even after the raw body is gone.
--     Hex form (64 chars) — VARCHAR(64) is a tight fit; VARCHAR(128)
--     buys room if we ever switch hash algorithms.
--
--   * received_at vs processed_at: received_at is set on INSERT
--     (when the webhook landed); processed_at is set after the
--     handler finishes the dispatch. A row with processed_at NULL
--     means we recorded the event but the dispatch failed
--     mid-flight (DB error, unknown event type). Operations can
--     grep for that to find stuck events.
--
-- No ON DELETE CASCADE: events survive tenant deletion so audit
-- history is preserved. A tenant that churns through plans leaves
-- a trail.
CREATE TABLE IF NOT EXISTS billing_events (
    event_id      VARCHAR(128) PRIMARY KEY,
    provider      VARCHAR(32)  NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    tenant_id     VARCHAR(64),
    received_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at  TIMESTAMPTZ,
    payload_hash  VARCHAR(128) NOT NULL
);

-- Operator hot path: "show me every event for tenant T in the last
-- 24h". The partial WHERE clause keeps the index small by excluding
-- NULL tenants (provider-level events) and processed rows.
CREATE INDEX IF NOT EXISTS idx_billing_events_tenant_received
    ON billing_events (tenant_id, received_at DESC)
    WHERE tenant_id IS NOT NULL;