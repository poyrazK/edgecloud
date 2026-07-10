-- +migrate Down
-- Reverse 028_..._up.sql. The JSONB column is a leaf addition;
-- DROP COLUMN is the inverse.
ALTER TABLE active_deployments
    DROP COLUMN IF EXISTS region_cache_retry_count;
