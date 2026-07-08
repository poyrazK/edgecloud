-- +migrate Up
-- Add SLSA L1 provenance column for issue #307 PR2.
--
-- `build_attestation` holds a DSSE-wrapped, signed in-toto Statement
-- v0.1 envelope (see edge-control-plane/internal/provenance) emitted
-- at Deploy / Migrate / MigrateTree time. Persisted as JSONB so the
-- downstream audit pipeline can query structured fields (e.g.
-- `build_attestation->>'predicateType'`) without a round-trip through
-- the wire form.
--
-- Nullable for the same reason as 017's signature column: pre-PR2
-- deployments have no attestation, and a worker running with the
-- current attestation policy (default: don't require provenance)
-- accepts them as-is. A future `EDGE_PROVENANCE_REQUIRED=true` switch
-- will be the operator-facing gate; the column shape itself is
-- unchanged.
--
-- GIN index on the JSONB predicate subtree lets the audit query
-- `build_attestation @> '{"predicateType":"https://slsa.dev/provenance/v1"}'`
-- hit the index instead of seqscan. The `WHERE build_attestation IS
-- NOT NULL` partial keeps the index small — legacy rows are excluded.
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS build_attestation JSONB;

CREATE INDEX IF NOT EXISTS idx_deployments_build_attestation
    ON deployments USING GIN (build_attestation jsonb_path_ops)
    WHERE build_attestation IS NOT NULL;