-- +migrate Up
-- Migration 006 added lookup_hash as a nullable column and backfilled
-- only sha256 rows (argon2id rows cannot be backfilled — the raw key is
-- unrecoverable from an argon2id hash, so the only path forward is for
-- operators to re-issue those keys).
--
-- Once all rows have a populated lookup_hash, this migration enforces
-- NOT NULL so future code paths cannot insert a row with an empty or NULL
-- lookup_hash and silently bypass authentication.

-- +migrate StatementBegin
DO $$
DECLARE null_count INT;
BEGIN
    SELECT COUNT(*) INTO null_count FROM api_keys WHERE lookup_hash IS NULL;
    IF null_count > 0 THEN
        RAISE EXCEPTION 'cannot set lookup_hash NOT NULL: % rows have NULL lookup_hash (re-issue those keys first; raw keys are unrecoverable from argon2id hashes)', null_count;
    END IF;
END $$;
-- +migrate StatementEnd

ALTER TABLE api_keys ALTER COLUMN lookup_hash SET NOT NULL;
