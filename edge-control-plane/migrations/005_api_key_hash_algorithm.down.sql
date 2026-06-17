DROP INDEX IF EXISTS idx_api_keys_hash_algorithm;
ALTER TABLE api_keys DROP COLUMN IF EXISTS hash_algorithm;
