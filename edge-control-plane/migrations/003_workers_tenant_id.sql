-- +migrate Up
ALTER TABLE workers ADD COLUMN tenant_id TEXT NOT NULL REFERENCES tenants(id);
CREATE INDEX idx_workers_tenant_id ON workers(tenant_id);

-- +migrate Down
ALTER TABLE workers DROP COLUMN tenant_id;