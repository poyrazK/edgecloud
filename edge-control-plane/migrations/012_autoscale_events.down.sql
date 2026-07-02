-- Reverse of 012_autoscale_events.up.sql. Drops the index first
-- (Postgres drops the index automatically when the table is dropped,
-- but being explicit matches the up migration's ordering).

DROP INDEX IF EXISTS idx_autoscale_events_region_time;
DROP TABLE IF EXISTS autoscale_events;
