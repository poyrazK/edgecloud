-- +migrate Up
-- 031_quotas_compute_ms.up.sql
-- Adds per-tenant FaaS request-duration metering to the quotas table
-- (issue #555). Compute-ms is the wall-clock time the Handler (FaaS)
-- dispatch path spent inside the guest summed across all requests
-- during the heartbeat interval — the fourth metered dimension
-- alongside outbound bytes (009), request count (013), and resident
-- seconds (029). LongRunning apps do not contribute compute-ms (the
-- worker stamps duration_ms_total=null on their heartbeats; the CP
-- translates null to 0 contribution).
--
-- `used_compute_ms` is the running total since the start of the
-- current `quota_period_start` month (009 already added this column
-- and the rollover logic). The lazy month-boundary rollover lives in
-- repository/quota.go's addColumn helper — same path as the other
-- three metered dimensions, so the heartbeat goroutine for
-- compute-ms inherits dedupe + cap-trip + free-tier grace via
-- applyTenantDelta without further surgery.
--
-- `max_compute_ms_per_month` is the per-tenant cap, populated when
-- the tenant's plan changes via service.TenantService.UpdateTenant.
-- Sentinel convention matches the other Max* columns: -1 = unlimited
-- (enterprise), 0 = "unset / admin-cleared" (skip the cap check),
-- >0 = the cap. Plan→cap values mirror
-- internal/domain/plans.go:17-65 and are the resident-seconds cap
-- scaled by 1_000 (free=2_592_000_000 = 30 days × 86_400_000 ms/day,
-- pro=7_776_000_000 = 90 days, business=31_104_000_000 = 360 days,
-- enterprise=-1). Keep both files in sync when adding new tiers or
-- adjusting caps.
--
-- The migration extends the 030 `billing_usage_events.kind` CHECK
-- constraint to include 'compute_ms' — that schema change lives in
-- 030_billing_usage_events.up.sql so all dimension-list updates are
-- colocated with the originating migration.
ALTER TABLE quotas
    ADD COLUMN IF NOT EXISTS max_compute_ms_per_month INT   NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS used_compute_ms           BIGINT NOT NULL DEFAULT 0;

-- Backfill existing rows from tenants.plan so paid-tier tenants don't
-- silently drop to free-tier caps after this migration. Unrecognized
-- plan strings fall back to free-tier (defensive — the handler
-- validates plan on create/update; this is the safety net for
-- historical rows). Per-tier values match plans.go exactly:
--   free       =  2_592_000_000  (30 days × 86_400_000 ms/day)
--   pro        =  7_776_000_000  (90 days × 86_400_000 ms/day)
--   business   = 31_104_000_000  (360 days × 86_400_000 ms/day)
--   enterprise =            -1  (unlimited)
UPDATE quotas q
   SET max_compute_ms_per_month = CASE t.plan
       WHEN 'free'       THEN  2592000000
       WHEN 'pro'        THEN  7776000000
       WHEN 'business'   THEN 31104000000
       WHEN 'enterprise' THEN          -1
       ELSE 2592000000
   END
  FROM tenants t
 WHERE q.tenant_id = t.id;