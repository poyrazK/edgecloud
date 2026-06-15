-- Rollback indexes

DROP INDEX IF EXISTS idx_deployments_tenant_app;
DROP INDEX IF EXISTS idx_deployments_tenant;
DROP INDEX IF EXISTS idx_workers_region;
DROP INDEX IF EXISTS idx_api_keys_tenant;
DROP INDEX IF EXISTS idx_active_deployments_tenant;
DROP INDEX IF EXISTS idx_app_env_tenant_app;
