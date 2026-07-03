-- +migrate Down
-- Reverse migration 009: drop auto-rollback columns.
-- DESTRUCTIVE: any tenant opt-in flags and observed-running timestamps
-- are lost. Only run this as part of a planned rollback.

ALTER TABLE active_deployments DROP COLUMN IF EXISTS stable_since;

ALTER TABLE active_deployments DROP COLUMN IF EXISTS auto_rollback_enabled;

ALTER TABLE deployments DROP COLUMN IF EXISTS auto_rollback_enabled;