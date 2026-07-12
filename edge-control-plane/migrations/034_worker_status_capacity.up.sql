-- +migrate Up
-- Issue #641: worker-level capacity signals persisted on
-- `worker_status` so the deploy-time 402 gate
-- (`SumFreeSlotsByRegion`, see DeploymentService.Deploy) can detect
-- fleet-wide saturation without scraping Prometheus / asking each
-- worker for a snapshot.
--
-- Three columns:
--   * `free_slots` INTEGER NOT NULL DEFAULT -1 — the worker's
--     `ClusterHeadroom.free_slots` value at heartbeat time. -1 is
--     the "no heartbeat yet" sentinel (distinct from 0 = "pool is
--     empty"); the deploy gate treats `free_slots >= 0` as the
--     "have data" predicate, and sums `GREATEST(free_slots, 0)`
--     across workers in a region.
--   * `cluster_headroom` JSONB — verbatim copy of the heartbeat's
--     ClusterHeadroom object, kept for future readers that want
--     cpu_pct / mem_pct / app_slots without changing the schema.
--   * `port_pool_exhausted_count` BIGINT NOT NULL DEFAULT 0 — the
--     worker's cumulative `PortPool::acquire() → None` count since
--     process boot (matches the heartbeat's
--     `port_pool_exhausted_count` field).
--   * `last_exhaustion_at` TIMESTAMPTZ — stamped by `UpsertStatus`
--     when the worker's exhaustion counter advances. Used as a
--     freshness predicate by `SumFreeSlotsByRegion` so a region
--     whose workers haven't reported recently isn't treated as
--     permanently saturated.
--
-- Partial index supports the deploy-time SUM-free-slots lookup:
-- `WHERE region = ANY($1) AND last_report > NOW() - INTERVAL '90 seconds'
--  AND free_slots >= 0`. The `free_slots >= 0` filter is what
-- excludes the "no heartbeat yet" sentinel rows from the index.
ALTER TABLE worker_status
    ADD COLUMN free_slots                INTEGER     NOT NULL DEFAULT -1,
    ADD COLUMN cluster_headroom          JSONB,
    ADD COLUMN port_pool_exhausted_count BIGINT      NOT NULL DEFAULT 0,
    ADD COLUMN last_exhaustion_at        TIMESTAMPTZ;

CREATE INDEX idx_worker_status_region_free_slots
    ON worker_status (region, free_slots)
    WHERE free_slots >= 0;