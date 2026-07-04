-- +migrate Up
-- Add hash_algorithm column to api_keys so the auth path can dispatch to
-- the algorithm-specific verifier. Previously every row stored its hash
-- format implicitly in key_hash and the service code had to guess; this
-- column makes the contract explicit at the schema level.
--
-- The column is NOT NULL with a CHECK constraint that pins the allowed
-- values. Migration 006 (lookup_hash) already references this column in
-- its backfill WHERE clause; without 005 running first, 006 fails on a
-- fresh database.

ALTER TABLE api_keys ADD COLUMN hash_algorithm TEXT;

-- Backfill: detect algorithm from key_hash format.
--   sha256   → 64-char lowercase hex (the legacy format from migration 001)
--   argon2id → PHC string starting with "$argon2id$" (PHC: https://github.com/P-H-C/phc-string-format)
UPDATE api_keys SET hash_algorithm = 'sha256'
 WHERE key_hash ~ '^[0-9a-f]{64}$';

UPDATE api_keys SET hash_algorithm = 'argon2id'
 WHERE key_hash LIKE '$argon2id$%';

-- Any row with an unrecognized key_hash format is left NULL — that is a
-- pre-existing data-integrity issue, and operators must resolve it before
-- this migration can proceed. We fail loudly rather than guessing.
-- +migrate StatementBegin
DO $$
DECLARE bad_count INT;
BEGIN
    SELECT COUNT(*) INTO bad_count FROM api_keys WHERE hash_algorithm IS NULL;
    IF bad_count > 0 THEN
        RAISE EXCEPTION 'cannot backfill hash_algorithm: % api_keys rows have unrecognized key_hash format', bad_count;
    END IF;
END $$;
-- +migrate StatementEnd

ALTER TABLE api_keys ALTER COLUMN hash_algorithm SET NOT NULL;

ALTER TABLE api_keys ADD CONSTRAINT api_keys_hash_algorithm_check
    CHECK (hash_algorithm IN ('sha256', 'argon2id'));

-- +migrate Down
-- Reverse 005_api_key_hash_algorithm: drop the constraint, allow NULLs,
-- then drop the column. Drop order matters: NOT NULL must be relaxed
-- before DROP COLUMN so existing rows don't fail validation.
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_hash_algorithm_check;

ALTER TABLE api_keys ALTER COLUMN hash_algorithm DROP NOT NULL;

ALTER TABLE api_keys DROP COLUMN IF EXISTS hash_algorithm;