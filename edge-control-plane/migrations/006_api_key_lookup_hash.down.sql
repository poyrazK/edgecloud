-- +migrate Down
DROP INDEX IF EXISTS idx_api_keys_lookup_hash;
ALTER TABLE api_keys DROP COLUMN IF EXISTS lookup_hash;
