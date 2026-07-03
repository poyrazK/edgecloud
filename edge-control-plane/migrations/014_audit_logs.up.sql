-- +migrate Up
-- 014_audit_logs.up.sql
-- Append-only event table for tracking state-changing API operations.
-- Every CUD handler writes one row so operators and compliance tooling
-- can answer "who did what, when, and from where."
--
-- The table is INSERT-only (no UPDATE, no DELETE). A daily/weekly
-- retention job (separate PR) can drop partitions or delete rows older
-- than the retention window, modelled after log_gc.go.
BEGIN;

CREATE TABLE audit_logs (
    id          BIGSERIAL    PRIMARY KEY,
    tenant_id   VARCHAR(64)  NOT NULL DEFAULT '',
    api_key_id  VARCHAR(64)  NOT NULL DEFAULT '',
    role        VARCHAR(32)  NOT NULL DEFAULT '',
    action      VARCHAR(32)  NOT NULL,  -- create, update, delete, deploy, activate, rollback, bootstrap
    resource    VARCHAR(32)  NOT NULL,  -- tenant, api_key, app, deployment, env, egress, domain, traffic
    resource_id TEXT         NOT NULL DEFAULT '',
    details     TEXT         NOT NULL DEFAULT '',
    outcome     VARCHAR(16)  NOT NULL,  -- success, failure
    error_msg   TEXT         NOT NULL DEFAULT '',
    request_ip  VARCHAR(45)  NOT NULL DEFAULT '',  -- IPv6-capable (45 chars)
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Primary read path: recent events for a tenant (admin audit trail).
CREATE INDEX idx_audit_logs_tenant_created
    ON audit_logs (tenant_id, created_at DESC);

-- Secondary: forensics on a specific resource.
CREATE INDEX idx_audit_logs_resource
    ON audit_logs (resource, resource_id, created_at DESC);

COMMIT;
