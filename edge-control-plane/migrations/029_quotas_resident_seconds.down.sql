-- +migrate Down
-- 029_quotas_resident_seconds.down.sql
-- Drops the per-tenant resident-time metering columns added by
-- 029_quotas_resident_seconds.up.sql. Reversible — the backfill in
-- the Up is idempotent on re-apply.
ALTER TABLE quotas
    DROP COLUMN IF EXISTS used_resident_seconds,
    DROP COLUMN IF EXISTS max_resident_seconds_per_month;
