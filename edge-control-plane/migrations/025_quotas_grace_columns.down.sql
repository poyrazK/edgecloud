-- +migrate Down
-- Reverse 025_quotas_grace_columns: drop the partial indexes and the
-- two new columns. Restoring the table to its pre-#420 shape.
DROP INDEX IF EXISTS idx_quotas_grace_until;
ALTER TABLE quotas  DROP COLUMN IF EXISTS quota_lock_grace_until;

DROP INDEX IF EXISTS idx_tenants_overage_allowed_until;
ALTER TABLE tenants DROP COLUMN IF EXISTS overage_allowed_until;
