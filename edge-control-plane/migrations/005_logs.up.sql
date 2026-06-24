-- Tenant log store. Populated by workers via POST /api/internal/logs (issue #76).
CREATE TABLE logs (
    id            BIGSERIAL PRIMARY KEY,
    tenant_id     VARCHAR NOT NULL,
    deployment_id VARCHAR NOT NULL,
    app_name      VARCHAR NOT NULL,
    worker_id     VARCHAR NOT NULL,
    region        VARCHAR NOT NULL,
    level         VARCHAR NOT NULL,  -- 'trace' | 'debug' | 'info' | 'warn' | 'error'
    message       TEXT    NOT NULL,
    labels        JSONB   NOT NULL DEFAULT '{}'::jsonb,
    ts            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Primary read path: "give me the latest logs for app X under tenant T".
CREATE INDEX idx_logs_tenant_app_ts ON logs (tenant_id, app_name, ts DESC);

-- GC sweep path: DELETE FROM logs WHERE ts < $1.
CREATE INDEX idx_logs_ts ON logs (ts);
