-- +migrate Up notransaction
-- Issue #501 (cache-retry sweep): support the sweep's list query.
--
-- Migration 018 added the `regions_cache_failed` column but deliberately
-- shipped without an index — at the time the column was only ever
-- read on the activation path (single-row GetForUpdate), so a
-- full-table scan was acceptable. Issue #501 turns the column into
-- the iteration target of a background sweep, which lists rows by
-- `regions_cache_failed <> '{}'`; without an index that becomes a
-- sequential scan proportional to the active-deployment count, which
-- is unacceptable at scale.
--
-- Partial btree on `tenant_id` filtered to non-empty
-- `regions_cache_failed` keeps the LIST cheap even with millions of
-- healthy (no-failed-regions) rows present: the planner scans only
-- matching rows. We index `tenant_id` rather than the empty `()`
-- because the sweep partitions the result set by (tenant_id, app_name)
-- for ORDER BY stability across runs — a future multi-replica CP
-- deployment could split sweeps by tenant.
--
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS` so the migration doesn't
-- block live writes (mirrors 002_add_indexes.up.sql). The
-- `notransaction` directive at the top is required by rubenv/sql-migrate
-- for any migration that uses CONCURRENTLY — CREATE INDEX CONCURRENTLY
-- cannot run inside a transaction block.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_active_deployments_regions_cache_failed_nonempty
    ON active_deployments (tenant_id)
    WHERE regions_cache_failed <> '{}';