-- +migrate Down
-- Rollback of the public_key length cap. Drops the named constraint
-- added in 033_workers_public_key_length.up.sql.
ALTER TABLE workers
  DROP CONSTRAINT IF EXISTS workers_public_key_length_check;