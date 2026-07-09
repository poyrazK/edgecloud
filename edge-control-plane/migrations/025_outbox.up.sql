-- +migrate Up

-- Durable-publish queue for NATS TaskMessages (issue #42). Written in the
-- same transaction as the `active_deployments` mutation that the message
-- accompanies; drained by `service.OutboxDrainer`
-- (internal/service/outbox_drainer.go). Rows transition
-- `pending` -> `in_flight` -> `published` (or `failed` after
-- OUTBOX_MAX_ATTEMPTS retries). `FullSync` messages from the reconcile
-- loop are NOT outboxed.

CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    app_name        TEXT NOT NULL,
    kind            TEXT NOT NULL,         -- 'task_update' today; reserved for future kinds
    payload         JSONB NOT NULL,        -- marshaled nats.TaskMessage
    regions         TEXT[] NOT NULL DEFAULT '{}',
    attempt_count   INT  NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          TEXT NOT NULL DEFAULT 'pending',  -- 'pending'|'in_flight'|'published'|'failed'
    last_error      TEXT,
    dedupe_key      TEXT NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,
    claimed_until   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_due_idx
    ON outbox (next_attempt_at)
    WHERE status IN ('pending', 'in_flight');

CREATE INDEX IF NOT EXISTS outbox_tenant_app_idx
    ON outbox (tenant_id, app_name);

CREATE INDEX IF NOT EXISTS outbox_failed_idx
    ON outbox (created_at)
    WHERE status = 'failed';