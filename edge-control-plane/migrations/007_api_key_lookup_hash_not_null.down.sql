-- Reverse 007_api_key_lookup_hash_not_null: allow NULLs again.
ALTER TABLE api_keys ALTER COLUMN lookup_hash DROP NOT NULL;
