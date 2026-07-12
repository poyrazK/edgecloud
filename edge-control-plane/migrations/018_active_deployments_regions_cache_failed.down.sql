-- +migrate Down
-- Issue #332 (Layer 3): drop the regions_cache_failed tracking column.
-- Migration is reversible by re-running 018_..._up.sql.
ALTER TABLE active_deployments
    DROP COLUMN IF EXISTS regions_cache_failed;
