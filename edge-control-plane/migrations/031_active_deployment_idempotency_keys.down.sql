-- +migrate Down
DROP INDEX IF EXISTS idx_active_deployment_idempotency_keys_deployment_id;
DROP TABLE IF EXISTS active_deployment_idempotency_keys;
