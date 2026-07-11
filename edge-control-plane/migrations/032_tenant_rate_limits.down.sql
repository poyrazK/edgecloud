-- +migrate Down
-- 032_tenant_rate_limits.down.sql
-- Rollback for 032_tenant_rate_limits. Drops the partial index first
-- (Postgres refuses DROP COLUMN while a dependent index exists), then
-- drops the columns in reverse declaration order so the down migration
-- can be re-applied without transient constraint conflicts.
DROP INDEX IF EXISTS idx_quotas_tenant_rate_limit_active;

ALTER TABLE quotas
    DROP COLUMN IF EXISTS tenant_rate_limit_set_at,
    DROP COLUMN IF EXISTS tenant_bandwidth_bps,
    DROP COLUMN IF EXISTS tenant_concurrent_limit,
    DROP COLUMN IF EXISTS tenant_rate_limit_burst,
    DROP COLUMN IF EXISTS tenant_rate_limit_rps;
