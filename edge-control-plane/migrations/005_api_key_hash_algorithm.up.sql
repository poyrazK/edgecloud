-- Issue #36: track which hash algorithm each API key uses so we can migrate
-- from SHA-256 to argon2id without forcing all existing keys to be re-issued.
--
-- Existing rows default to 'sha256' — they keep verifying until their next
-- successful auth, at which point AuthMiddleware lazily upgrades them to
-- argon2id.
ALTER TABLE api_keys
    ADD COLUMN hash_algorithm TEXT NOT NULL DEFAULT 'sha256';

-- Index supports future analytics / cleanup jobs ("find legacy SHA-256 keys").
CREATE INDEX idx_api_keys_hash_algorithm ON api_keys(hash_algorithm);
