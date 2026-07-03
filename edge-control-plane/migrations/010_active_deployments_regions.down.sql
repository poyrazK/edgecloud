-- +migrate Down
-- Reverse migration 010: drop the per-region publish state columns
-- from `active_deployments`. DESTRUCTIVE: any in-flight publish
-- history is lost. Only run this as part of a planned rollback.

ALTER TABLE active_deployments
    DROP COLUMN regions_published,
    DROP COLUMN regions_failed,
    DROP COLUMN last_publish_at,
    DROP COLUMN last_publish_attempt_id;
