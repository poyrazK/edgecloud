-- +migrate Down
-- 030_billing_usage_events.down.sql
-- Drops the metering ledger table added by
-- 030_billing_usage_events.up.sql. The UNIQUE constraint and partial
-- index are dropped with the table.
DROP TABLE IF EXISTS billing_usage_events;