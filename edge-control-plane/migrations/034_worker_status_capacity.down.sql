-- +migrate Down
DROP INDEX IF EXISTS idx_worker_status_region_free_slots;
ALTER TABLE worker_status
    DROP COLUMN last_exhaustion_at,
    DROP COLUMN port_pool_exhausted_count,
    DROP COLUMN cluster_headroom,
    DROP COLUMN free_slots;