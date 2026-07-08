-- +migrate Down
-- Reverse 016_tenant_disabled_at: drop the index and column.

DROP INDEX IF EXISTS idx_tenants_disabled_at;

ALTER TABLE tenants DROP COLUMN IF EXISTS disabled_at;
