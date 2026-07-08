-- Issue #332 (Layer 3: Push-to-Edge Artifact Distribution) — track
-- per-region cache-push failures so a client retry can re-attempt the
-- failed cache push without re-doing the NATS publish.
--
-- PR 1 added `regions_cache_failed` to the 502 envelope and
-- `PublishError.CacheFailed`. PR 2 deliberately omitted the matching
-- DB column ("matches how regions_published works for NATS publish
-- retries"). This PR closes that wire/DB asymmetry so an operator
-- who sees "fra" in the 502 envelope's `regions_cache_failed` can
-- also see it in the row, and so a future retry that preserves the
-- active row knows which regions to re-push.
--
-- `IF NOT EXISTS` for re-run safety. Mirrors the column shape from
-- 017 (TEXT[] NOT NULL DEFAULT '{}') and 010 (regions_failed).
ALTER TABLE active_deployments
    ADD COLUMN IF NOT EXISTS regions_cache_failed TEXT[] NOT NULL DEFAULT '{}';
