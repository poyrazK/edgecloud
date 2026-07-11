-- +migrate Up
-- 031_gc_retention_indexes.up.sql
-- Issue #574: dedicated (created_at) indexes for the three append-only
-- tables that the new retention GCs (audit_logs, webhook_deliveries,
-- autoscale_events) sweep. The existing composite indexes
-- (tenant_id-led / webhook_id-led / region-led) cover tenant-scoped
-- reads; these indexes cover the time-range DELETE path used by
-- service/{audit_gc,webhook_delivery_gc,autoscale_event_gc}.go:
--
--   DELETE FROM <table> WHERE id IN (
--     SELECT id FROM <table>
--     WHERE created_at < NOW() - make_interval(secs => $1) LIMIT $2
--   )
--
-- Without these indexes the sweep becomes O(N) per tick because no
-- existing index leads with (created_at). The retention GCs run on a
-- 1h cadence by default (operator-tunable), so an unbounded seq-scan
-- will trip the loopHealth staleness alert on a multi-million-row
-- table. Each (created_at) index is the minimum cost: one B-tree per
-- table on an append-only column; INSERT-path write amplification is
-- unchanged for the (tenant_id, …) / (webhook_id, …) / (region, …)
-- indexes that already back the existing read paths.
--
-- Sister migrations: 014_audit_logs (audit_logs), 015_webhooks
-- (webhook_deliveries), 012_autoscale_events. Sister services:
-- service/audit_gc.go, service/webhook_delivery_gc.go,
-- service/autoscale_event_gc.go (issue #574).
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at
    ON audit_logs (created_at);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_created_at
    ON webhook_deliveries (created_at);
CREATE INDEX IF NOT EXISTS idx_autoscale_events_created_at
    ON autoscale_events (created_at);
