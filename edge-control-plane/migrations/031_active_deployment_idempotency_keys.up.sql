-- +migrate Up
-- Issue #439: Idempotency-Key replay cache for the activate /
-- promote / rollback paths. Mirrors issue #52's idempotency_keys
-- table (migration 026_idempotency_keys) but for TaskMessage
-- publishes keyed by (tenant, app, deployment_id) instead of
-- deployment-row creation keyed by (tenant, key, request_sha256).
--
-- Replay contract:
--   - Same key + same (app_name, deployment_id)  -> 200 OK,
--     no new outbox row, no new publish.
--   - Same key + different (app_name OR deployment_id) -> 422
--     ErrIdempotencyKeyMismatch (mirrors Deploy's body-hash
--     mismatch response).
--   - Key older than 24h (IdempotencyTTL in
--     repository/idempotency_key.go) -> treated as cache miss,
--     fresh publish semantics.
--
-- The cache is keyed on (tenant_id, idempotency_key) so cross-tenant
-- collisions are impossible: lookup is always scoped to the
-- caller's authenticated tenant_id, never by key alone. The
-- (deployment_id) FK to deployments(id) cascades so an operator-
-- initiated deployment GC also clears any dangling cache rows.
CREATE TABLE IF NOT EXISTS active_deployment_idempotency_keys (
    tenant_id       TEXT        NOT NULL,
    idempotency_key TEXT        NOT NULL,
    app_name        TEXT        NOT NULL,
    deployment_id   TEXT        NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_active_deployment_idempotency_keys_deployment_id
    ON active_deployment_idempotency_keys (deployment_id);
