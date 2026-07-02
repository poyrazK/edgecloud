-- Audit table for the cluster autoscaler (issue #85). Every scale
-- decision (scale_up, scale_down, noop) the autoscaler makes is
-- persisted here so operators can:
--   1. See why a worker was added/removed (`reason` column)
--   2. Tell which cloud-provider was active (`provider_kind` column)
--   3. Detect failed provision/deprovision calls (`succeeded = false`
--      rows with `error_message` set)
--
-- Column set is fixed by internal/repository/autoscale.go — every
-- INSERT/SELECT in that file enumerates these columns. If you
-- add/remove a column, update both the migration and the repository.
--
-- 1. id — BIGSERIAL. Auto-incrementing; not exposed externally.
-- 2. created_at — wall-clock at insert. Default `now()`. The
--    descending index (region, created_at DESC) makes ListRecent
--    O(log n + limit).
-- 3. region — which region this decision applies to. The autoscaler
--    is per-region, so each decision is bound to exactly one.
-- 4. action — scale_up | scale_down | noop. CHECK constraint enforces
--    the closed set so a future typo in the service layer can't
--    silently land an "scale_sideways" row.
-- 5. from_count — worker count before the decision.
-- 6. to_count — worker count after the decision (== from_count for
--    noop rows; differs by ±1 for scale rows since the autoscaler
--    scales one at a time).
-- 7. reason — short human-readable string from ComputeDecision's
--    Decision.Reason field (e.g., "free_slots=3 needed=20").
-- 8. provider_kind — `noop` | `mock` | future `hetzner` | `aws` | ….
--    Pinned per-event so a config flip doesn't retroactively relabel
--    old decisions.
-- 9. succeeded — true on a successful Provision/Deprovision call (or
--    any noop). false on cloud-provider errors; `error_message` is
--    populated when false.
-- 10. error_message — only set when succeeded = false.

CREATE TABLE autoscale_events (
    id              BIGSERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    region          TEXT        NOT NULL,
    action          TEXT        NOT NULL CHECK (action IN ('scale_up','scale_down','noop')),
    from_count      INTEGER     NOT NULL,
    to_count        INTEGER     NOT NULL,
    reason          TEXT        NOT NULL,
    provider_kind   TEXT        NOT NULL,
    succeeded       BOOLEAN     NOT NULL,
    error_message   TEXT
);

CREATE INDEX idx_autoscale_events_region_time
    ON autoscale_events (region, created_at DESC);
