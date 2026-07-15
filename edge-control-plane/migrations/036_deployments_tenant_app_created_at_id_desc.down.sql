-- +migrate Down
DROP INDEX IF EXISTS idx_deployments_tenant_app_created_at_id_desc;
