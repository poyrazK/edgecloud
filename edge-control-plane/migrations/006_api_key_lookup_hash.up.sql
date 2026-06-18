-- Issue (review follow-up): AuthenticateRawKey looks up rows by SHA-256 of
-- the raw key, but CreateAPIKey stores the argon2id PHC string in key_hash.
-- The two strings never match, so every newly-created API key was rejected
-- by auth.
--
-- Fix: split the column. key_hash remains the algorithm-specific encoded
-- hash (sha256 hex OR $argon2id$...); lookup_hash is a stable SHA-256 hex
-- of the raw key that AuthenticateRawKey queries against.
--
-- Backfill: SHA-256 rows already have key_hash == lookup_hash (the raw
-- key hash IS the lookup hash). argon2id rows cannot be backfilled — the
-- raw key is unrecoverable from an argon2id hash. Operators must re-issue
-- those keys. Partial UNIQUE index tolerates NULLs.
-- Wrap the column add, backfill, and partial unique index in a single
-- transaction so an interrupted migration leaves the schema atomic.
-- The partial index tolerates NULLs, so a pre-existing argon2id row
-- (unrecoverable from the hash) does not block the index creation, but
-- we still want the three statements to commit together — otherwise a
-- crash between the UPDATE and CREATE INDEX leaves rows with NULL
-- lookup_hash and no enforcing index. BEGIN/COMMIT makes that failure
-- mode all-or-nothing.
BEGIN;

ALTER TABLE api_keys ADD COLUMN lookup_hash TEXT;

UPDATE api_keys
   SET lookup_hash = key_hash
 WHERE hash_algorithm = 'sha256';

CREATE UNIQUE INDEX idx_api_keys_lookup_hash
    ON api_keys(lookup_hash) WHERE lookup_hash IS NOT NULL;

COMMIT;
