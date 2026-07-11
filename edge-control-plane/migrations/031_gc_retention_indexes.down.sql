-- +migrate Down
-- 031_gc_retention_indexes.down.sql
-- Rollback of the retention-sweep indexes (issue #574). Drops are
-- guarded with IF EXISTS so a partial rollback against an older
-- schema does not error.
DROP INDEX IF EXISTS idx_audit_logs_created_at;
DROP INDEX IF EXISTS idx_webhook_deliveries_created_at;
DROP INDEX IF EXISTS idx_autoscale_events_created_at;
