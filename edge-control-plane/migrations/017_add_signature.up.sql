-- +migrate Up
-- Add Ed25519 signature columns for issue #307 (artifact signing).
--
-- `signature` holds the base64url(no-pad) Ed25519 signature over
-- `sha256(artifact_bytes) || deployment_id`, computed by the control
-- plane at Deploy / Migrate time. Nullable so pre-#307 rows (deployed
-- before the signing code shipped) read back as NULL; workers running
-- with EDGE_REQUIRE_SIGNATURE=false accept NULL signatures, workers
-- with EDGE_REQUIRE_SIGNATURE=true reject them and force a re-deploy.
--
-- `signing_key_id` is the logical key id stamped on each row at sign
-- time (env EDGE_SIGNING_KEY_ID on the CP). Including it now avoids a
-- future migration when key rotation lands — the worker / auditor can
-- later check `signing_key_id = <expected current key id>` as a
-- freshness check. Nullable for the same reason as `signature`.
--
-- Partial index on signed rows only — the common operator query is
-- "which deployments are signed by the current key id", and a partial
-- index over the small subset of rows that are signed keeps the
-- index small while making the lookup O(log n) instead of seqscan.
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS signature       TEXT;
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS signing_key_id  TEXT;

CREATE INDEX IF NOT EXISTS idx_deployments_signed
    ON deployments(signing_key_id)
    WHERE signature IS NOT NULL;
