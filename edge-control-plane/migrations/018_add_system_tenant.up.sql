-- +migrate Up
INSERT INTO tenants (id, name, plan) VALUES ('t_system', 'System Tenant', 'enterprise') ON CONFLICT DO NOTHING;
INSERT INTO quotas (tenant_id, max_workers) VALUES ('t_system', 1000) ON CONFLICT DO NOTHING;
