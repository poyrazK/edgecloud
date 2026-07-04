-- +migrate Up
ALTER TABLE workers ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL REFERENCES tenants(id);
CREATE INDEX IF NOT EXISTS idx_workers_tenant_id ON workers(tenant_id);
