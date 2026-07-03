-- +migrate Down
ALTER TABLE quotas DROP COLUMN quota_period_start;
ALTER TABLE quotas DROP COLUMN used_outbound_bytes;
