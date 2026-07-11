-- +migrate Up
-- Length cap on workers.public_key (issue #430 review follow-up).
--
-- The EnrollWorker endpoint validates that req.PublicKey is exactly
-- 64 lowercase hex chars (32 bytes — the Ed25519 public-key size)
-- before persisting it (see internal/handler/internal.go:838-845),
-- but the column itself has no length constraint. An attacker who
-- somehow reaches the write path with a megabyte-sized value would
-- succeed at the application layer (the json.Decode would accept it
-- if validation regressed) and pollute the row.
--
-- 64 hex chars = 32 raw bytes. The cap is intentionally tight: the
-- application contract is exactly that size, and any future migration
-- that broadens the key format (e.g. P-256 for ECDSA, ~91 base64 chars)
-- should be paired with raising this cap and updating the validation.
-- A cap of 256 chars leaves headroom for ~4x the current size without
-- enabling abuse.
--
-- Sister constraint: see EnrollWorker (internal/handler/internal.go) and
-- repository/worker.go::SetPublicKey, which both gate on the 64-hex
-- shape today; this CHECK is the defense-in-depth that catches a
-- future code regression before it lands in production.
ALTER TABLE workers
  ADD CONSTRAINT workers_public_key_length_check
  CHECK (public_key IS NULL OR length(public_key) <= 256);