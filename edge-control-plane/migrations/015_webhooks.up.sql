-- +migrate Up
-- 015_webhooks.up.sql
-- Tenant-managed webhook subscriptions for deployment lifecycle events.
-- Tenants register URLs that receive POST JSON payloads on deploy,
-- activate, rollback, and auto-rollback events.
CREATE TABLE IF NOT EXISTS webhooks (
    id          VARCHAR(64)  PRIMARY KEY,
    tenant_id   VARCHAR(64)  NOT NULL,
    url         TEXT         NOT NULL,
    secret      TEXT         NOT NULL,
    events      TEXT[]       NOT NULL DEFAULT '{}',
    description TEXT         NOT NULL DEFAULT '',
    enabled     BOOLEAN      NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhooks_tenant ON webhooks (tenant_id);

-- Append-only delivery log for debugging and observability.
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id            BIGSERIAL    PRIMARY KEY,
    webhook_id    VARCHAR(64)  NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    event_type    VARCHAR(32)  NOT NULL,
    status        VARCHAR(16)  NOT NULL,  -- success, failed, retrying
    status_code   INT,
    request_body  TEXT         NOT NULL DEFAULT '',
    response_body TEXT         NOT NULL DEFAULT '',
    error_msg     TEXT         NOT NULL DEFAULT '',
    attempt       INT          NOT NULL DEFAULT 1,
    max_attempts  INT          NOT NULL DEFAULT 1,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_webhook ON webhook_deliveries (webhook_id, created_at DESC);
