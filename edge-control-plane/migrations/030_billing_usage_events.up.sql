-- +migrate Up
-- 030_billing_usage_events.up.sql
-- Metering ledger for issue #485. The heartbeat pipeline (worker →
-- control-plane) writes one row per (tenant, kind, quantity) per
-- 30s heartbeat batch; a drainer picks up unprocessed rows and
-- reports them to the merchant (Stripe `subscriptionitem.NewUsageRecord`
-- today, noop in dev/CI).
--
-- This table is the metering ledger. It is distinct from
-- `billing_events` (023), which is a webhook idempotency log keyed on
-- the merchant's event_id. Different concerns:
--
--   * billing_events        : per inbound webhook (small, audit-only)
--   * billing_usage_events  : per (tenant, kind, quantity) batch
--                             (larger, drives Stripe metered billing)
--
-- The two tables are joined only by tenant_id.
--
-- Schema notes:
--
--   * id BIGSERIAL PK — we synthesize the synthetic key client-side
--     from (tenant_id, idempotency_key) so a UNIQUE constraint on
--     (tenant_id, idempotency_key) collapses redeliveries to one
--     row. (Same shape as the idempotency_keys table from #52.)
--     id is BIGSERIAL anyway because every Postgres table wants a
--     numeric PK for tooling, even when application logic keys on
--     the natural (tenant_id, idempotency_key) pair.
--
--   * tenant_id NOT NULL REFERENCES tenants(id) — no ON DELETE
--     CASCADE: usage rows survive tenant deletion, matching the
--     audit-retention posture of billing_events.
--
--   * kind is constrained to the three metered dimensions the CP
--     knows about today. New dimensions require a migration.
--
--   * quantity is BIGINT (resident-seconds per heartbeat batch can
--     reach ~30, easily; bytes-per-batch can reach millions; both
--     fit comfortably in int8 with room to spare).
--
--   * idempotency_key is "<dedupe_id>:<kind>" — the heartbeat's
--     DedupeID combined with the metric kind. The same DedupeID is
--     shared across all metrics in one heartbeat (per #418), so the
--     kind suffix is what disambiguates the rows.
--
--   * provider mirrors the merchant we ultimately report to. The
--     drainer reads it to know which MeteringProvider to dispatch
--     through.
--
--   * recorded_at vs processed_at: recorded_at is set on INSERT by
--     the heartbeat path; processed_at is set after the drainer
--     successfully calls MeteringProvider.RecordUsage. NULL
--     processed_at = "queued for retry".
--
-- Indexes:
--
--   * PRIMARY KEY (id) for tooling.
--
--   * UNIQUE (tenant_id, idempotency_key) is the dedup contract.
--     Redeliveries (JetStream replay, Stripe redelivery, reconcile
--     replay) hit ON CONFLICT DO NOTHING and the affected-row count
--     of zero tells the drainer "already processed, no-op". Note
--     this collapses redelivery to "we sent the report once" — if
--     a Stripe call actually fails AFTER the ON CONFLICT absorbed
--     the redelivery, the row's processed_at will go NULL and a
--     retry will run. The drainer is responsible for not
--     double-reporting (via stripe-go's IdempotencyKey on the
--     subscriptionitem.NewUsageRecord call).
--
--   * idx_billing_usage_events_unprocessed is the drainer's hot path
--     ("SELECT … WHERE processed_at IS NULL ORDER BY recorded_at").
--     Partial index keeps it small by excluding already-processed
--     rows.
CREATE TABLE IF NOT EXISTS billing_usage_events (
    id              BIGSERIAL    PRIMARY KEY,
    tenant_id       VARCHAR(64)  NOT NULL REFERENCES tenants(id),
    kind            VARCHAR(16)  NOT NULL CHECK (kind IN ('resident_seconds','request_count','outbound_bytes')),
    quantity        BIGINT       NOT NULL CHECK (quantity >= 0),
    idempotency_key VARCHAR(128) NOT NULL,
    recorded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ,
    provider        VARCHAR(32)  NOT NULL,
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_billing_usage_events_unprocessed
    ON billing_usage_events (recorded_at)
    WHERE processed_at IS NULL;