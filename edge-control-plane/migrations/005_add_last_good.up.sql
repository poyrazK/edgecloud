-- +migrate Up
-- Add last_good_deployment_id column to active_deployments for issue #74
-- (CLI rollback UX). On a successful Activate, the previous deployment_id is
-- copied into this column; RollbackDeployment then swaps them back atomically.
-- Nullable so pre-existing rows (no history) read back as NULL and surface a
-- friendly 409 on rollback rather than crashing.
--
-- ON DELETE SET NULL: if the referenced deployment row is removed, the
-- pointer becomes NULL rather than failing the FK check or cascading
-- delete of the active row. The active row is per-tenant state, not a
-- child of the deployment, so CASCADE is wrong; the advisory pointer
-- is the right thing to clear, and the existing ErrNoLastGood path
-- already turns NULL into a clean 409.
ALTER TABLE active_deployments
    ADD COLUMN IF NOT EXISTS last_good_deployment_id TEXT
        REFERENCES deployments(id) ON DELETE SET NULL;
