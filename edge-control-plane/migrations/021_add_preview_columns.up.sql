-- +migrate Up
-- Preview-environment columns for issue #308. Marks a `deployments`
-- row as a preview and stamps the metadata that drives cleanup,
-- store scoping, and PR-linkage.
--
-- Column set is fixed by internal/domain/deployment.go — every
-- INSERT/SELECT in internal/repository/deployment.go enumerates
-- these columns. If you add/remove a column, update both the
-- migration and the repository.
--
-- 1. preview_id — server- or client-minted hex suffix used as the
--    store-scope key (`<EDGE_KV_STORE_PATH>/{tenant_id}/preview-{id}/`)
--    and as the SWEEP marker. Nullable: legacy (non-preview)
--    rows are NULL. The PreviewGCService's WHERE clause filters
--    on `preview_expires_at IS NOT NULL`, so this column is for
--    human-readability + cross-referencing — the GC sweep uses
--    the expiry.
--
-- 2. preview_pr_number — integer PR number the GitHub composite
--    action forwards via ?preview-pr-number=. Stored on the row
--    so an operator can correlate a deployment with the PR that
--    produced it. Optional: non-CI users (`edge deploy --preview`
--    on a laptop) don't have a PR number, so NULL is normal.
--
-- 3. preview_expires_at — TIMESTAMPTZ the PreviewGCService
--    compares against NOW() on each sweep. Defaults to
--    NOW() + 7 days when a preview deploy is uploaded; per-deploy
--    overridable via ?preview-ttl=24h. Indexed (partial, below)
--    so the GC sweep stays cheap as the deployments table grows.
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS preview_id           TEXT;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS preview_pr_number   INTEGER;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS preview_expires_at  TIMESTAMPTZ;

-- Hot path: PreviewGCService.Run SELECTs/DELETEs by expiry on every
-- tick. The partial WHERE clause keeps the index small — non-preview
-- rows (the vast majority) are excluded.
CREATE INDEX IF NOT EXISTS idx_deployments_preview_expires_at
    ON deployments (preview_expires_at)
    WHERE preview_expires_at IS NOT NULL;

-- active_deployments mirrors the same preview markers: GetForUpdate
-- and the Set INSERT/UPDATE paths (issue #308, internal/repository/
-- active_deployment.go:88,100,279) read/write preview_id +
-- preview_pr_number off this table. Adding them here closes the
-- schema/repo drift the e2e rollback test surfaced (issue #613).
ALTER TABLE active_deployments ADD COLUMN IF NOT EXISTS preview_id         TEXT;
ALTER TABLE active_deployments ADD COLUMN IF NOT EXISTS preview_pr_number INTEGER;