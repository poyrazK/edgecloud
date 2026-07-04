-- +migrate Down
-- Reverse 005_api_key_hash_algorithm: drop the constraint, allow NULLs,
-- then drop the column. Drop order matters: NOT NULL must be relaxed
-- before DROP COLUMN so existing rows don't fail validation.
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_hash_algorithm_check;

ALTER TABLE api_keys ALTER COLUMN hash_algorithm DROP NOT NULL;

ALTER TABLE api_keys DROP COLUMN IF EXISTS hash_algorithm;
