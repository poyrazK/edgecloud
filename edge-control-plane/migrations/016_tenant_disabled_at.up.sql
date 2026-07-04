-- +migrate Up
-- 016_tenant_disabled_at.up.sql
-- Adds a nullable disabled_at timestamp column to the tenants table so
-- the control plane can mark a tenant as suspended when they exceed
-- their outbound bandwidth quota (issue #155).
--
-- When `disabled_at IS NOT NULL`:
--   - The reconcile loop skips publishing task updates for this tenant
--   - New deployments and activations are rejected with 403
--   - Existing worker apps remain running but no new messages are sent
--   - Set to NULL when the billing period resets or plan upgrades

ALTER TABLE tenants ADD COLUMN disabled_at TIMESTAMPTZ;

-- Create an index so the reconcile loop can efficiently find active tenants
-- (disabled_at IS NULL) vs disabled ones without a full scan.
CREATE INDEX idx_tenants_disabled_at ON tenants (disabled_at);
