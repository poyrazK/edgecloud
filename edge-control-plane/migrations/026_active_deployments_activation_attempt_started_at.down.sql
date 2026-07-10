-- +migrate Down
-- Issue #440: drop the activation_attempt_started_at marker added in
-- the up migration.

ALTER TABLE active_deployments
    DROP COLUMN IF EXISTS activation_attempt_started_at;