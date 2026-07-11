-- +migrate Down
-- Rollback of issue #430. Drops the index first (the partial-index WHERE
-- clause references the column), then drops the column.
DROP INDEX IF EXISTS idx_workers_public_key;
ALTER TABLE workers DROP COLUMN IF EXISTS public_key;
