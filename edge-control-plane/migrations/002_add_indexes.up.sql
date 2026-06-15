-- Add performance indexes for common query patterns

-- deployments: frequently queried by tenant + app name
CREATE INDEX CONCURRENTLY idx_deployments_tenant_app ON deployments(tenant_id, app_name);
CREATE INDEX CONCURRENTLY idx_deployments_tenant ON deployments(tenant_id);

-- workers: frequently queried by region
CREATE INDEX CONCURRENTLY idx_workers_region ON workers(region);

-- api_keys: frequently queried by tenant
CREATE INDEX CONCURRENTLY idx_api_keys_tenant ON api_keys(tenant_id);

-- active_deployments: frequently queried by tenant
CREATE INDEX CONCURRENTLY idx_active_deployments_tenant ON active_deployments(tenant_id);

-- app_env: frequently queried by tenant + app
CREATE INDEX CONCURRENTLY idx_app_env_tenant_app ON app_env(tenant_id, app_name);
