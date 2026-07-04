-- +migrate Up
-- Add `app_traffic_splits` table for issue #84 (canary/blue-green deploys).
-- Stores one row per (app, deployment, weight). Sum of weights per app = 100.
-- The primary deployment (weight=100) is also recorded here when using
-- traffic splits; the existing `active_deployments` table is NOT modified —
-- it remains the source of truth for the legacy atomic-activate path.
-- The ingress fetches this table via GET /api/v1/apps/{appName}/traffic
-- to render weighted Caddy upstreams.

CREATE TABLE app_traffic_splits (
    tenant_id       TEXT NOT NULL,
    app_name        TEXT NOT NULL,
    deployment_id   TEXT NOT NULL REFERENCES deployments(id),
    weight          INTEGER NOT NULL CHECK (weight >= 0 AND weight <= 100),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, app_name, deployment_id)
);

-- Index for fetching all splits for a given app (used by GET /traffic and
-- by the ingress on startup and after each Caddy reload).
CREATE INDEX idx_ats_tenant_app ON app_traffic_splits(tenant_id, app_name);

-- +migrate Down
DROP INDEX IF EXISTS idx_ats_tenant_app;
DROP TABLE IF EXISTS app_traffic_splits;