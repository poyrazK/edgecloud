-- +migrate Up
-- Tenants
CREATE TABLE tenants (
    id          TEXT PRIMARY KEY,  -- "t_<uuid>"
    name        TEXT NOT NULL,
    plan        TEXT NOT NULL DEFAULT 'free',
    allowlisted_destinations TEXT[] DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Quotas (per tenant)
CREATE TABLE quotas (
    tenant_id   TEXT PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    max_deployments  INT NOT NULL DEFAULT 10,
    max_apps        INT NOT NULL DEFAULT 5,
    max_workers     INT NOT NULL DEFAULT 3,
    max_memory_mb   INT NOT NULL DEFAULT 256,
    max_outbound_mb INT NOT NULL DEFAULT 1000
);

-- API Keys
CREATE TABLE api_keys (
    id          TEXT PRIMARY KEY,  -- "k_<uuid>"
    tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    key_hash    TEXT NOT NULL,  -- SHA-256 of raw key
    role        TEXT NOT NULL DEFAULT 'developer',  -- owner, developer, viewer
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used   TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ
);

-- Deployments
CREATE TABLE deployments (
    id          TEXT PRIMARY KEY,  -- "d_<uuid>"
    tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    app_name    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'deployed',  -- deployed, active, failed
    hash        TEXT NOT NULL,  -- SHA-256 of Wasm payload
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Active Deployment Mapping
CREATE TABLE active_deployments (
    tenant_id   TEXT NOT NULL,
    app_name    TEXT NOT NULL,
    deployment_id TEXT NOT NULL REFERENCES deployments(id),
    PRIMARY KEY (tenant_id, app_name)
);

-- App Environment Variables
CREATE TABLE app_env (
    tenant_id   TEXT NOT NULL,
    app_name    TEXT NOT NULL,
    env_key     TEXT NOT NULL,
    env_value   TEXT NOT NULL,
    PRIMARY KEY (tenant_id, app_name, env_key)
);

-- Workers (registered supervisors)
CREATE TABLE workers (
    id          TEXT PRIMARY KEY,  -- "w_<region>_<uuid>"
    region      TEXT NOT NULL,
    ip          TEXT,
    memory_mb   INT NOT NULL DEFAULT 4096,
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Worker Status Reports
CREATE TABLE worker_status (
    worker_id   TEXT PRIMARY KEY REFERENCES workers(id) ON DELETE CASCADE,
    apps        JSONB NOT NULL DEFAULT '{}',  -- { app_name: { status, exit_code, deployment_id } }
    last_report TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +migrate Down
DROP TABLE IF EXISTS worker_status;
DROP TABLE IF EXISTS workers;
DROP TABLE IF EXISTS app_env;
DROP TABLE IF EXISTS active_deployments;
DROP TABLE IF EXISTS deployments;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS quotas;
DROP TABLE IF EXISTS tenants;