-- +migrate Down
-- Reverse migration 008: drop the `regions` column from `deployments`.
-- DESTRUCTIVE: any per-region target information on existing rows is lost.
-- Only run this as part of a planned rollback.

ALTER TABLE deployments DROP COLUMN regions;
