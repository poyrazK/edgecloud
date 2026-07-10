-- +migrate Up
-- 029_quotas_resident_seconds.up.sql
-- Adds per-tenant resident-time metering to the quotas table (issue
-- #485). Resident time is the uptime of LongRunning apps — the third
-- metered dimension alongside outbound bytes (009) and request count
-- (013). Handler (FaaS) apps do not contribute resident time (the
-- worker stamps resident_seconds=None on their heartbeats; the CP
-- translates None to 0 contribution).
--
-- `used_resident_seconds` is the running total since the start of the
-- current `quota_period_start` month (009 already added this column
-- and the rollover logic). The lazy month-boundary rollover lives in
-- repository/quota.go's addColumn helper — same path as the other
-- two metered dimensions, so the heartbeat goroutine for
-- resident-seconds inherits dedupe + cap-trip + free-tier grace via
-- applyTenantDelta without further surgery.
--
-- `max_resident_seconds_per_month` is the per-tenant cap, populated
-- when the tenant's plan changes via service.TenantService.UpdateTenant.
-- Sentinel convention matches the other Max* columns: -1 = unlimited
-- (enterprise), 0 = "unset / admin-cleared" (skip the cap check),
-- >0 = the cap.
--
-- Plan→cap values mirror internal/domain/plans.go:17-59. Keep both
-- files in sync when adding new tiers or adjusting caps.
ALTER TABLE quotas
    ADD COLUMN IF NOT EXISTS max_resident_seconds_per_month INT   NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS used_resident_seconds           BIGINT NOT NULL DEFAULT 0;

-- Backfill existing rows from tenants.plan so paid-tier tenants don't
-- silently drop to free-tier caps after this migration. Unrecognized
-- plan strings fall back to free-tier (defensive — the handler
-- validates plan on create/update; this is the safety net for
-- historical rows). Per-tier values match plans.go exactly:
--   free       =  2_592_000  (30 days at 1 LR app)
--   pro        =  7_776_000  (90 days at 1 LR app)
--   business   = 31_104_000  (360 days at 1 LR app)
--   enterprise =         -1  (unlimited)
UPDATE quotas q
   SET max_resident_seconds_per_month = CASE t.plan
       WHEN 'free'       THEN  2592000
       WHEN 'pro'        THEN  7776000
       WHEN 'business'   THEN 31104000
       WHEN 'enterprise' THEN       -1
       ELSE 2592000
   END
  FROM tenants t
 WHERE q.tenant_id = t.id;
