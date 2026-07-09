-- +migrate Down
-- Reverse 024_billing_subscriptions_relax_customer_id. Restores the
-- NOT NULL constraint; will fail if any existing row has a NULL
-- provider_customer_id (callers must first UPDATE the column to a
-- non-null sentinel or backfill from the merchant).
ALTER TABLE billing_subscriptions
    ALTER COLUMN provider_customer_id SET NOT NULL;