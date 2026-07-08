-- +migrate Down
DELETE FROM quotas WHERE tenant_id = 't_system';
DELETE FROM tenants WHERE id = 't_system';
