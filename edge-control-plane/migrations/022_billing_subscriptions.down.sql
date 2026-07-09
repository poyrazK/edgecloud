-- +migrate Down
-- Reverse 022_billing_subscriptions. Drop the index first (Postgres
-- refuses to DROP the table while an index references it), then drop
-- the table.
DROP INDEX IF EXISTS idx_billing_subscriptions_provider_customer;

DROP TABLE IF EXISTS billing_subscriptions;