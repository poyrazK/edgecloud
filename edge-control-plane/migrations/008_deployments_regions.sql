-- +migrate Up
-- Add the `regions` column to `deployments` for issue #82 (multi-region
-- deploys, v1). The column stores the list of regions a deployment is
-- replicated to; the activate path loops over this list and publishes one
-- `TaskMessage` per region to `edgecloud.tasks.<region>`.
--
-- Type matches the `tenants.allowlisted_destinations TEXT[]` precedent
-- (001_init_schema.up.sql). DEFAULT '{}' is critical: existing rows
-- predate the column, and the activate-time fallback in
-- `service.ActivateDeployment` treats an empty list as "use the control
-- plane's default region", so the migration does not need a separate
-- backfill step.
--
-- This migration is intentionally artifact-replication-agnostic. Real
-- cross-region deploys require either a shared artifact store or per-region
-- control-plane federation; that work is tracked separately.

ALTER TABLE deployments ADD COLUMN regions TEXT[] NOT NULL DEFAULT '{}';

-- +migrate Down
-- Reverse migration 008: drop the `regions` column from `deployments`.
-- DESTRUCTIVE: any per-region target information on existing rows is lost.
-- Only run this as part of a planned rollback.

ALTER TABLE deployments DROP COLUMN regions;