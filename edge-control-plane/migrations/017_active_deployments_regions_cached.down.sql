-- Issue #332 (Layer 3): drop the regions_cached tracking column.
-- Migration is reversible by re-running 017_..._up.sql.
ALTER TABLE active_deployments
    DROP COLUMN IF EXISTS regions_cached;
