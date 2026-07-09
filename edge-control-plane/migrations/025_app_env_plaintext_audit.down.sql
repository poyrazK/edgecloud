-- +migrate Down
-- Reverse 025_app_env_plaintext_audit. Silent no-op for the COMMENT
-- (Postgres preserves the prior comment string, which is null after
-- rollback since 001 set no comment). DROP VIEW IF EXISTS is idempotent.
DROP VIEW IF EXISTS app_env_plaintext_audit;
COMMENT ON COLUMN app_env.env_value IS NULL;
