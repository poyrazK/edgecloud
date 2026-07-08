-- +migrate Down
-- Drop the partial index first (DROP COLUMN would fail if the index
-- referenced the column), then drop the two columns added by 017.
DROP INDEX IF EXISTS idx_deployments_signed;

ALTER TABLE deployments DROP COLUMN IF EXISTS signing_key_id;
ALTER TABLE deployments DROP COLUMN IF EXISTS signature;
