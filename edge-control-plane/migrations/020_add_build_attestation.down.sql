-- +migrate Down
-- Reverse 020_add_build_attestation. Drop the GIN index first
-- (Postgres refuses to DROP COLUMN while an index references it),
-- then drop the column.
DROP INDEX IF EXISTS idx_deployments_build_attestation;

ALTER TABLE deployments DROP COLUMN IF EXISTS build_attestation;