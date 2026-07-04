-- +migrate Up
-- Add the auto-rollback feature for issue #74. Three columns, no
-- data backfill: existing rows just see sensible defaults and the
-- control plane behaves identically until a tenant opts in.
--
-- 1. deployments.auto_rollback_enabled — tenant opt-in set by
--    `edge deploy --auto-rollback`. Defaults to false so existing
--    deployments are unaffected. The flag is read at activate time
--    (see service.ActivateDeployment) and copied onto
--    active_deployments. We store it on BOTH rows so:
--      - the activate-time copy doesn't have to re-read the artifact,
--      - the artifact row retains the operator's intent for audit
--        (e.g. `edge deployments --app foo` can show which
--        deployments opted into auto-rollback).
--
-- 2. active_deployments.auto_rollback_enabled — read by the
--    worker-driven auto-rollback path (handler.AutoRollback) and by
--    the heartbeat-driven stability window
--    (service.worker.evaluateStability). Mirrors the same flag from
--    the deployments row at activate time.
--
-- 3. active_deployments.stable_since — first-heartbeat timestamp
--    for the currently-active deployment. Reset to NULL on every
--    activate/rollback/auto-rollback. The heartbeat handler sets it
--    to NOW() the first time it observes status="running" for this
--    active row; the stability window promotes
--    deployment_id → last_good_deployment_id once stable_since is
--    older than STABLE_WINDOW_SECONDS (default 30s). Nullable: NULL
--    means "not yet observed running" or "rolled back; clock
--    reset". The promote path is metadata-only — it does not
--    publish a TaskMessage because workers are still serving the
--    same deployment_id; only the safety-net pointer changes.

ALTER TABLE deployments ADD COLUMN auto_rollback_enabled BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE active_deployments ADD COLUMN auto_rollback_enabled BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE active_deployments ADD COLUMN stable_since TIMESTAMPTZ NULL;

-- +migrate Down
-- Reverse migration 009: drop auto-rollback columns.
-- DESTRUCTIVE: any tenant opt-in flags and observed-running timestamps
-- are lost. Only run this as part of a planned rollback.

ALTER TABLE active_deployments DROP COLUMN IF EXISTS stable_since;

ALTER TABLE active_deployments DROP COLUMN IF EXISTS auto_rollback_enabled;

ALTER TABLE deployments DROP COLUMN IF EXISTS auto_rollback_enabled;