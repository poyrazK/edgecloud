-- +migrate Up
-- 025_app_env_plaintext_audit.up.sql
-- Issue #441 follow-up to PR #462: document the app_env.env_value
-- encryption contract and provide a SQL-only shape audit so operators
-- can verify the legacy-plaintext migration is complete without
-- booting the control plane.
--
-- The active encryption key lives in env-vars / keyring, never in the
-- DB. SQL cannot auto-encrypt legacy plaintext rows. The actual fix-up
-- is the runtime path:
--
--   * POST /api/v1/admin/secrets/re-encrypt
--       Re-encrypts existing cipher rows under the active key. Skips
--       plaintext rows (plaintext_skipped field in the response).
--   * POST /api/v1/apps/<app>/env
--       Setting a plaintext env value through the API Encrypts it
--       on insert.
--
-- The view below is a *shape* check, not an integrity check. A row with
-- a valid kid prefix but a tampered GCM tag will appear as a "cipher"
-- here and surface as ErrCiphertextMismatch at Decrypt time — the
-- shape check is for triage, the integrity check is for action.

COMMENT ON COLUMN app_env.env_value IS
    'Envelope-encrypted secret value. New-format shape is '
    '"<kid>:<hex-nonce>:<hex-ciphertext+tag>" where <kid> is the active '
    'SecretEncryptor key id (any string; keyring lookup at Decrypt time), '
    'hex-nonce is the AES-GCM nonce (12 bytes, hex-encoded), and '
    'hex-ciphertext+tag is the AES-GCM ciphertext with appended 16-byte '
    'tag. Legacy pre-keyring format "<hex-nonce>:<hex-ct>" is also '
    'accepted (Decrypt tries every key in the keyring). Values that '
    'match neither shape are rejected at Decrypt time with '
    'ErrPlaintextEnvNotAllowed; values with a known kid whose GCM tag '
    'fails are rejected with ErrCiphertextMismatch. The control plane '
    'refuses to boot when plaintext rows are present unless '
    'EDGE_ALLOW_LEGACY_PLAINTEXT_ENV=true is set (logged WARNING, not '
    'enforced). For SQL-only auditing of legacy plaintext rows, see the '
    'app_env_plaintext_audit view created in this migration. See issue '
    '#441 and PR #462.';

-- app_env_plaintext_audit: one row per legacy plaintext app_env value.
-- The regex matches the new-format shape only — a 3-part value with a
-- kid-shaped prefix and two hex segments. The kid charset is a subset
-- of what the runtime accepts (the runtime accepts any string the
-- operator put in secrets.active_key_id, since it does a map lookup
-- rather than a regex match); the stricter character class here
-- produces false negatives only for operator-defined kids that contain
-- characters outside [A-Za-z0-9_-], which is acceptable for a triage
-- view. The runtime is still the source of truth — count discrepancies
-- between this view and EnvService.CountPlaintextRows should be
-- reported as a bug in the audit, not the runtime.
--
-- The view deliberately does NOT verify the kid against the keyring
-- (the keyring lives outside the DB) and does NOT verify the GCM tag
-- (would require re-encrypting with a held key). It is a shape
-- classifier, same semantics as SecretEncryptor.LooksLikeCipher.
--
-- The `value_snippet` column is the first 80 characters of env_value
-- to help operators identify the row without leaking the full secret
-- to psql history / terminal scrollback. The full value is available
-- via a direct SELECT against app_env if the operator already has DB
-- read access (same trust level as the audit itself).
--
-- CREATE OR REPLACE keeps the migration idempotent (the
-- MigrationsAreIdempotent subtest of roundtrip_test.go wipes
-- gorp_migrations and re-runs every *.up.sql).
CREATE OR REPLACE VIEW app_env_plaintext_audit AS
    SELECT
        tenant_id,
        app_name,
        env_key,
        LEFT(env_value, 80) AS value_snippet
    FROM app_env
    WHERE env_value !~ '^[A-Za-z0-9_-]+:[0-9a-f]+:[0-9a-f]+$';
