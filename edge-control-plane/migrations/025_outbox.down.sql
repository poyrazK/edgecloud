-- +migrate Down

DROP INDEX IF EXISTS outbox_failed_idx;
DROP INDEX IF EXISTS outbox_tenant_app_idx;
DROP INDEX IF EXISTS outbox_due_idx;
DROP TABLE IF EXISTS outbox;