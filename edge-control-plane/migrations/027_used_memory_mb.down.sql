-- +migrate Down
-- 027_used_memory_mb.down.sql
--
-- Reverse 027. Drops the aggregate memory counter; pre-#44 behavior is
-- restored (MaxMemoryMB is once again only used as a TaskMessage hint,
-- no per-tenant aggregate cap).

ALTER TABLE quotas DROP COLUMN IF EXISTS used_memory_mb;
