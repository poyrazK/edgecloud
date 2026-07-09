-- +migrate Down
-- Reverse 026_idempotency_keys. Drop the secondary index first
-- (Postgres requires the index to be gone before the table it
-- indexes can be dropped), then drop the table itself.
DROP INDEX IF EXISTS idx_idempotency_keys_deployment_id;
DROP TABLE IF EXISTS idempotency_keys;
