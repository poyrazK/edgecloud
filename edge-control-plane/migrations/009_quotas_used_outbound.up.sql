-- +migrate Up
ALTER TABLE quotas ADD COLUMN used_outbound_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE quotas ADD COLUMN quota_period_start TIMESTAMPTZ NOT NULL DEFAULT date_trunc('month', now() AT TIME ZONE 'UTC');
