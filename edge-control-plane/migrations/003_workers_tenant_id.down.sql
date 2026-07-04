-- +migrate Down
ALTER TABLE workers DROP COLUMN tenant_id;
