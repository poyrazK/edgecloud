-- +migrate Up
-- 025_quotas_grace_columns.up.sql
-- Issue #420: enforcement needs two new nullable timestamps.
--
--   - tenants.overage_allowed_until: per-tenant grace clock for paid tenants
--     who have hit cap. Set by POST /api/v1/admin/tenants/{id}/quota-override;
--     the deploy-time check skips the VerifyUnderCap test if this is in the
--     future. The intent is to give a customer who's been told "you're over
--     cap, please upgrade" a bounded runway before deploys start failing —
--     think 24 hours, not 30 days.
--
--   - quotas.quota_lock_grace_until: per-tenant grace clock for free-tier
--     first-cross. Set by applyTenantDelta on first-cross of the cap; the
--     deploy-time check still returns 402 during this window, but the
--     request-time 402 only kicks in after grace expires. This gives the
--     client a clear "your free tier is exhausted, upgrade at /billing"
--     message instead of being blocked the instant their counter ticks
--     over. Default grace length is 1 hour (operator-tunable later).
--
-- Both columns are partial-indexed (WHERE col IS NOT NULL) because the
-- vast majority of tenants have NULL there and the index is only consulted
-- when the deployment gate actually wants to check the override.
ALTER TABLE tenants ADD COLUMN overage_allowed_until TIMESTAMPTZ NULL;
CREATE INDEX idx_tenants_overage_allowed_until
    ON tenants (overage_allowed_until)
    WHERE overage_allowed_until IS NOT NULL;

ALTER TABLE quotas  ADD COLUMN quota_lock_grace_until TIMESTAMPTZ NULL;
CREATE INDEX idx_quotas_grace_until
    ON quotas (quota_lock_grace_until)
    WHERE quota_lock_grace_until IS NOT NULL;
