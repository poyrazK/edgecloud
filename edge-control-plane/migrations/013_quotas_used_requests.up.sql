-- 013_quotas_used_requests.up.sql
-- Adds per-tenant request-count metering to the quotas table.
-- `used_request_count` reuses the existing `quota_period_start` (added by 009)
-- for month-boundary rollover; no separate period column is needed.
-- `max_requests_per_month` is the per-tenant cap, populated when the tenant's
-- plan changes via service.TenantService.UpdateTenant.
--
-- Plan→cap values mirror internal/domain/plans.go:31-55. Keep both files in
-- sync when adding new tiers or adjusting caps.
BEGIN;

ALTER TABLE quotas
    ADD COLUMN max_requests_per_month INT   NOT NULL DEFAULT 100000,
    ADD COLUMN used_request_count     BIGINT NOT NULL DEFAULT 0;

-- Backfill existing rows from tenants.plan so paid-tier tenants don't
-- silently drop to free-tier caps after this migration. Unrecognized
-- plan strings fall back to free-tier (defensive — the handler validates
-- plan on create/update; this is the safety net for historical rows).
UPDATE quotas q
   SET max_requests_per_month = CASE t.plan
       WHEN 'free'       THEN 100000
       WHEN 'pro'        THEN 5000000
       WHEN 'business'   THEN 50000000
       WHEN 'enterprise' THEN -1
       ELSE 100000
   END
  FROM tenants t
 WHERE q.tenant_id = t.id;

COMMIT;