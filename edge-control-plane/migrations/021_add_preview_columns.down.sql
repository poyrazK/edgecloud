-- +migrate Down
-- Reverse 021_add_preview_columns. Drop the partial index first
-- (Postgres refuses to DROP COLUMN while an index references it),
-- then drop the columns.
DROP INDEX IF EXISTS idx_deployments_preview_expires_at;

ALTER TABLE deployments DROP COLUMN IF EXISTS preview_expires_at;
ALTER TABLE deployments DROP COLUMN IF EXISTS preview_pr_number;
ALTER TABLE deployments DROP COLUMN IF EXISTS preview_id;

ALTER TABLE active_deployments DROP COLUMN IF EXISTS preview_pr_number;
ALTER TABLE active_deployments DROP COLUMN IF EXISTS preview_id;