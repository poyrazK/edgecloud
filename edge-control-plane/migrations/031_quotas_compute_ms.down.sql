-- +migrate Down
-- 031_quotas_compute_ms.down.sql
-- Drops the compute-ms columns added by 031_quotas_compute_ms.up.sql.
-- The 030 billing_usage_events.kind CHECK extension is its own
-- migration (it lives in 030) and is reversed by the 030 down file —
-- do NOT touch that here. The down path does not attempt to fold
-- `used_compute_ms` into `used_resident_seconds` because they are
-- distinct dimensions; rolling back loses the per-tenant compute-ms
-- history but does not corrupt the other axes.
ALTER TABLE quotas
    DROP COLUMN IF EXISTS max_compute_ms_per_month,
    DROP COLUMN IF EXISTS used_compute_ms;