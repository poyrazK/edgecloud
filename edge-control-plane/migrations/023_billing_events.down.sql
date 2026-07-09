-- +migrate Down
-- Reverse 023_billing_events. Drop the index first, then the table.
DROP INDEX IF EXISTS idx_billing_events_tenant_received;

DROP TABLE IF EXISTS billing_events;