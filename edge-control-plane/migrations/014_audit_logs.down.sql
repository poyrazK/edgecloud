-- +migrate Down
-- 014_audit_logs.down.sql
DROP TABLE IF EXISTS audit_logs CASCADE;
