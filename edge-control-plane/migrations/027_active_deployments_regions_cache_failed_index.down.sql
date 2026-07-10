-- +migrate Down notransaction
-- Reverse 027_..._up.sql. CONCURRENTLY drop + notransaction directive
-- mirror the up migration's constraint (CONCURRENTLY cannot run inside
-- a transaction block).
DROP INDEX CONCURRENTLY IF EXISTS idx_active_deployments_regions_cache_failed_nonempty;
