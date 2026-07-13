-- +migrate Up
-- 032_tenant_rate_limits.up.sql
-- Per-tenant data-plane rate-limit columns (issue #305). Adds the storage
-- surface for the new edge-side ingress throttling that the existing
-- per-app `apps.rate_limit_rps`/`apps.rate_limit_burst` columns (added in
-- 017) could not express: a tenant-wide RPS cap that fires before any
-- per-app route, plus the future-state columns for concurrent-request and
-- bandwidth caps that the Caddy layer cannot yet render (no stock
-- Caddy module for #2; #3 needs Caddy 2.8+). Both columns land in this
-- migration so the admin endpoint, internal read endpoint, and ingress
-- cache can populate them today; the render-layer wiring is tracked in
-- follow-up issues.
--
-- Sentinel semantics match the existing quota Max* columns on this
-- table: any of these new columns < 0 means "unlimited" at the service
-- layer (mirrored on the renderer as "no cap"), 0 means "unset /
-- admin-cleared" (skip the cap check), >0 is the cap.
--
-- Column layout (issue #305):
--   tenant_rate_limit_rps     INTEGER     Per-tenant RPS cap at the ingress.
--                                         Renders today as a per-tenant
--                                         rate_limit route in Caddy JSON
--                                         (sub-feature #1).
--   tenant_rate_limit_burst   INTEGER     Per-tenant burst. 0 falls back
--                                         to tenant_rate_limit_rps in the
--                                         renderer (same shape as the
--                                         per-app columns).
--   tenant_concurrent_limit   INTEGER     Max in-flight requests per tenant.
--                                         Render deferred — stock Caddy
--                                         has no concurrency primitive.
--                                         Column + cache wired today;
--                                         follow-up issue tracked.
--   tenant_bandwidth_bps      BIGINT      Per-tenant bytes/sec cap.
--                                         Render deferred — Caddy 2.8+
--                                         rate_limit.bandwidth field,
--                                         which the floating `caddy:2`
--                                         Docker tag may or may not
--                                         surface on a given day.
--                                         Column + cache wired today;
--                                         follow-up issue tracked.
--   tenant_rate_limit_set_at  TIMESTAMPTZ Audit: when an admin last
--                                         wrote this row's rate-limit
--                                         columns via
--                                         PUT /api/v1/admin/tenants/{id}/rate-limit.
--                                         Read by audithelper.AuditLog;
--                                         not part of the public
--                                         GET /api/v1/quotas response
--                                         (json:"-" on the struct field).
--
-- Partial index lets the ingress TenantRateLimitCache fetcher scan only
-- rows that actually have a cap. Cold cache (cache empty) means
-- "no caps known" → render-time fail-open (no rate_limit route emitted)
-- which is the same shape as the existing quota 402 plumbing at issue
-- #420 (`quota_cache.is_over_cap` returns false on miss).
ALTER TABLE quotas
    ADD COLUMN IF NOT EXISTS tenant_rate_limit_rps      INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tenant_rate_limit_burst    INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tenant_concurrent_limit    INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tenant_bandwidth_bps       BIGINT      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS tenant_rate_limit_set_at   TIMESTAMPTZ;

-- Only rows with an active cap matter to the ingress fetcher. The
-- partial predicate keeps the index tiny (most tenants will keep all
-- caps at 0 / disabled) and lets a future operator-side report query
-- "which tenants have a cap today" without a table scan.
CREATE INDEX IF NOT EXISTS idx_quotas_tenant_rate_limit_active
    ON quotas (tenant_id)
    WHERE tenant_rate_limit_rps   > 0
       OR tenant_concurrent_limit > 0
       OR tenant_bandwidth_bps    > 0;
