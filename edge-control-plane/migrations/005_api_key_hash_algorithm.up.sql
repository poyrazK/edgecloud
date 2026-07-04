-- +migrate Up
-- Add hash_algorithm column to api_keys so the auth path can dispatch to
-- the algorithm-specific verifier. Previously every row stored its hash
-- format implicitly in key_hash and the service code had to guess; this
-- column makes the contract explicit at the schema level.
--
-- The column has a CHECK constraint that pins the allowed values.
-- Migration 006 (lookup_hash) already references this column in its
-- backfill WHERE clause; without 005 running first, 006 fails on a
-- fresh database.

ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS hash_algorithm TEXT;

-- Backfill: detect algorithm from key_hash format.
--   sha256   → 64-char lowercase hex (the legacy format from migration 001)
--   argon2id → PHC string starting with "$argon2id$" (PHC: https://github.com/P-H-C/phc-string-format)
UPDATE api_keys SET hash_algorithm = 'sha256'
 WHERE key_hash ~ '^[0-9a-f]{64}$';

UPDATE api_keys SET hash_algorithm = 'argon2id'
 WHERE key_hash LIKE '$argon2id$%';

-- The DO $$ validation is omitted here because rubenv/sql-migrate splits
-- on semicolons and cannot parse dollar-quoted blocks. Instead, let the
-- NOT NULL constraint below fail naturally if any rows were missed.

ALTER TABLE api_keys ALTER COLUMN hash_algorithm SET NOT NULL;

-- Idempotent CHECK constraint: ADD CONSTRAINT has no IF NOT EXISTS
-- in PG, so we DROP IF EXISTS + ADD. The DROP is a no-op the first
-- time and clears the constraint on subsequent re-applies (the test
-- suite's idempotency check wipes gorp_migrations and re-runs every
-- Up section, so each constraint must tolerate re-creation).
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_hash_algorithm_check;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_hash_algorithm_check
    CHECK (hash_algorithm IN ('sha256', 'argon2id'));
