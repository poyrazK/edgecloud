-- +migrate Up
-- Idempotency-Key replay cache (issue #52). Map
--   (tenant_id, key) -> deployment_id
-- so a CLI retry on a transient network error returns the original
-- record instead of minting a duplicate.
--
-- Row shape:
--   tenant_id       — owner of the key; lookup is scoped to the
--                     caller's authenticated tenant, never by key
--                     alone, so cross-tenant collisions are
--                     impossible.
--   key             — the Idempotency-Key header value, validated
--                     as [a-fA-F0-9-]{8,128} by the handler before
--                     reaching the DB.
--   deployment_id   — the replay target. FK to deployments(id) with
--                     ON DELETE CASCADE so an operator-initiated
--                     row delete cleans up the replay cache.
--   request_sha256  — SHA-256 of the multipart body that produced
--                     the deployment. The handler refuses a key
--                     reuse with a different body (422), so this
--                     doubles as a "you reused a key by mistake"
--                     guard rail.
--   created_at      — DEFAULT NOW() at insert time. The handler
--                     treats rows older than IdempotencyTTL (24h)
--                     as expired (returns nil, nil on lookup),
--                     so the table self-GCs as new keys overwrite
--                     old ones via INSERT...ON CONFLICT.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id       TEXT        NOT NULL,
    key             TEXT        NOT NULL,
    deployment_id   TEXT        NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    request_sha256  BYTEA       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, key)
);

-- Secondary index on deployment_id so future "find all replay
-- keys for a given deployment" lookups (e.g. an admin audit
-- query, or a future GC sweep) are index-only instead of
-- probing the PK in reverse. Read-only from the hot path
-- today, but cheap to keep.
CREATE INDEX IF NOT EXISTS idx_idempotency_keys_deployment_id
    ON idempotency_keys (deployment_id);
