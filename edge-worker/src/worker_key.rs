//! Worker identity keypair (issue #430).
//!
//! Each worker holds an Ed25519 signing keypair that authenticates the
//! bootstrap enrollment handshake against the control plane. The
//! control plane's `EnrollWorker` handler:
//!
//! 1. Stores `public_key_hex` on the `workers` row (migration 032).
//! 2. Returns the per-worker derived HS256 secret
//!    (`HKDF-SHA256(JWT_SECRET, salt=public_key, info="worker-v1|...")`)
//!    plus the `kid` (`"wkr_" + hex(sha256(pubkey))[:8]`).
//! 3. Verifies any inbound JWT whose `kid` starts with `wkr_` by
//!    re-deriving the same secret from the stored public_key — so the
//!    cluster `JWT_SECRET` never leaves the control plane.
//!
//! On the worker side the keypair is loaded from disk if present
//! (default `.worker-cache/identity.key`, mode 0600) and generated
//! freshly on first boot otherwise. Reusing the same key across
//! restarts lets the worker skip the bootstrap handshake on warm
//! reboots — see `bootstrap::BootstrapClient::run` and the
//! `EDGE_WORKER_REENROLL_ON_BOOT` escape hatch.
//!
//! File format: 32 raw bytes — the Ed25519 seed. No headers, no JSON.
//! Operators who want a different format can supply one via
//! `EDGE_WORKER_KEY` (inline lowercase hex) — see `load_or_create`.

use std::path::Path;

use anyhow::Context;
use ed25519_dalek::{Signer, SigningKey, SECRET_KEY_LENGTH};
use rand_core::{OsRng, RngCore};

/// Length of an Ed25519 seed in bytes.
const ED25519_SEED_LEN: usize = SECRET_KEY_LENGTH;

/// Worker identity keypair. Cheap to clone (no heap allocation beyond
/// the seed + cached hex string) and `Send + Sync` (the underlying
/// `SigningKey` is; we never expose a `Keypair` directly).
#[derive(Clone)]
pub struct WorkerIdentity {
    signing_key: SigningKey,
    public_key_hex: String,
}

impl std::fmt::Debug for WorkerIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't leak the private seed in `dbg!` output.
        f.debug_struct("WorkerIdentity")
            .field("public_key_hex", &self.public_key_hex)
            .finish_non_exhaustive()
    }
}

impl WorkerIdentity {
    /// Load the identity keypair from `path` if present; otherwise
    /// generate a fresh keypair, persist it to `path` with mode 0600,
    /// and return it.
    ///
    /// `EDGE_WORKER_KEY` (inline lowercase-hex seed) overrides the
    /// on-disk path — useful for containers / immutable images where
    /// mounting a per-pod secret file is impractical. The inline path
    /// does NOT persist a file; the inline value is the source of
    /// truth. Mirrors the `EDGE_SIGNING_KEYRING_PATH` /
    /// `EDGE_SIGNING_KEYRING` precedence at `config.rs:185-189`.
    pub fn load_or_create(path: &Path) -> anyhow::Result<Self> {
        // Inline override wins (matches the rest of the worker's
        // secret-resolution precedence).
        if let Ok(hex_seed) = std::env::var("EDGE_WORKER_KEY") {
            let seed = parse_hex_seed(&hex_seed)
                .context("EDGE_WORKER_KEY must be 64 lowercase hex chars")?;
            return Ok(Self::from_seed(seed));
        }

        if path.exists() {
            Self::load(path)
        } else {
            Self::generate_and_persist(path)
        }
    }

    /// Load a previously-persisted identity keypair from `path`. The
    /// file must be exactly 32 bytes of raw seed; `EDGE_WORKER_KEY` is
    /// NOT consulted (callers should use `load_or_create` for the
    /// precedence-aware path).
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading worker identity key from {}", path.display()))?;
        if bytes.len() != ED25519_SEED_LEN {
            anyhow::bail!(
                "worker identity key at {} has wrong length: got {} bytes, expected {}",
                path.display(),
                bytes.len(),
                ED25519_SEED_LEN
            );
        }
        let mut seed = [0u8; ED25519_SEED_LEN];
        seed.copy_from_slice(&bytes);
        Ok(Self::from_seed(seed))
    }

    /// Generate a fresh identity keypair and persist the raw seed to
    /// `path` with mode 0600. Creates the parent directory if needed.
    pub fn generate_and_persist(path: &Path) -> anyhow::Result<Self> {
        let mut seed = [0u8; ED25519_SEED_LEN];
        fill_random_seed(&mut seed).context("reading random bytes for worker identity key")?;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "creating parent dir for worker identity key: {}",
                        parent.display()
                    )
                })?;
            }
        }
        write_secret_file(path, &seed)
            .with_context(|| format!("writing worker identity key to {}", path.display()))?;
        tracing::info!(
            path = %path.display(),
            "generated and persisted new worker identity key (mode 0600)"
        );
        Ok(Self::from_seed(seed))
    }

    /// Build an identity from a raw 32-byte seed (test helper + the
    /// `EDGE_WORKER_KEY` path).
    pub fn from_seed(seed: [u8; ED25519_SEED_LEN]) -> Self {
        let signing_key = SigningKey::from_bytes(&seed);
        let public_key = signing_key.verifying_key().to_bytes();
        let public_key_hex = hex::encode(public_key);
        Self {
            signing_key,
            public_key_hex,
        }
    }

    /// Lowercase hex of the 32-byte Ed25519 public key. The control
    /// plane stores this verbatim on the `workers.public_key` column
    /// and uses it as the HKDF salt for `DeriveWorkerSecret`.
    pub fn public_key_hex(&self) -> &str {
        &self.public_key_hex
    }

    /// Sign an arbitrary message. Returns the 64-byte Ed25519
    /// signature. Used by the bootstrap enrollment handshake to
    /// prove possession of the private key over the challenge issued
    /// in phase 1.
    pub fn sign(&self, msg: &[u8]) -> [u8; 64] {
        let sig = self.signing_key.sign(msg);
        sig.to_bytes()
    }
}

/// Parse a 64-char lowercase hex string into a 32-byte seed.
fn parse_hex_seed(s: &str) -> anyhow::Result<[u8; ED25519_SEED_LEN]> {
    let trimmed = s.trim();
    if trimmed.len() != ED25519_SEED_LEN * 2 {
        anyhow::bail!(
            "expected {} hex chars, got {}",
            ED25519_SEED_LEN * 2,
            trimmed.len()
        );
    }
    let bytes = hex::decode(trimmed).context("decoding hex seed")?;
    if bytes.len() != ED25519_SEED_LEN {
        anyhow::bail!(
            "decoded seed has wrong length: {} bytes, expected {}",
            bytes.len(),
            ED25519_SEED_LEN
        );
    }
    let mut seed = [0u8; ED25519_SEED_LEN];
    seed.copy_from_slice(&bytes);
    Ok(seed)
}

/// Read random bytes into a buffer using the OS CSPRNG.
///
/// Wraps `rand_core::OsRng` (already a transitive dep of
/// `ed25519-dalek` 2.x). `OsRng::try_fill_bytes` returns
/// `Result<(), rand_core::Error>`; we translate the rare failure
/// into an `anyhow::Error` so the caller can keep using `?`.
fn fill_random_seed(out: &mut [u8]) -> anyhow::Result<()> {
    OsRng.try_fill_bytes(out).context("OsRng failed")
}

/// Write `bytes` to `path` atomically with mode 0600 on Unix.
///
/// The atomic shape is: write to `<path>.tmp`, fsync, rename onto
/// `path`. If the rename target already exists (it does — we're
/// updating an existing identity), rename replaces it. The 0600
/// permission is set BEFORE the rename so a crashed worker never
/// leaves a world-readable key on disk.
///
/// On non-Unix platforms the chmod call is a no-op; macOS is Unix,
/// Linux CI is Unix, and the operator runbook targets Linux.
fn write_secret_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let tmp = path.with_extension("key.tmp");
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    // Belt-and-suspenders: some filesystems can ignore the mode arg
    // when the target already exists (the file is being replaced by
    // rename). Re-chmod the final path explicitly.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path)?.permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(path, perms)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn worker_key_generates_when_absent() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("identity.key");
        // Ensure the env override doesn't leak across tests.
        // SAFETY: tests in this module don't run in parallel with
        // other tests that mutate EDGE_WORKER_KEY.
        let prev = std::env::var("EDGE_WORKER_KEY").ok();
        unsafe { std::env::remove_var("EDGE_WORKER_KEY") };

        let id = WorkerIdentity::load_or_create(&path).expect("load_or_create");
        assert!(
            path.exists(),
            "identity.key should be created on first boot"
        );
        assert_eq!(id.public_key_hex().len(), 64);
        assert!(id.public_key_hex().chars().all(|c| c.is_ascii_hexdigit()));

        // Cleanup env override.
        match prev {
            Some(v) => unsafe { std::env::set_var("EDGE_WORKER_KEY", v) },
            None => unsafe { std::env::remove_var("EDGE_WORKER_KEY") },
        }
    }

    #[test]
    fn worker_key_loads_when_present() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("identity.key");
        let prev = std::env::var("EDGE_WORKER_KEY").ok();
        unsafe { std::env::remove_var("EDGE_WORKER_KEY") };

        let id1 = WorkerIdentity::load_or_create(&path).expect("first load");
        let id2 = WorkerIdentity::load_or_create(&path).expect("second load");
        assert_eq!(
            id1.public_key_hex(),
            id2.public_key_hex(),
            "second load must return the same key"
        );

        match prev {
            Some(v) => unsafe { std::env::set_var("EDGE_WORKER_KEY", v) },
            None => unsafe { std::env::remove_var("EDGE_WORKER_KEY") },
        }
    }

    /// Two separate on-disk keys must produce two separate identities.
    /// This is the negative mirror of the load-when-present test and
    /// guards against accidental reuse of an in-memory cache.
    #[test]
    fn worker_key_distinct_paths_produce_distinct_identities() {
        let dir = tempdir().expect("tempdir");
        let prev = std::env::var("EDGE_WORKER_KEY").ok();
        unsafe { std::env::remove_var("EDGE_WORKER_KEY") };

        let id1 = WorkerIdentity::load_or_create(&dir.path().join("a.key")).expect("a");
        let id2 = WorkerIdentity::load_or_create(&dir.path().join("b.key")).expect("b");
        assert_ne!(id1.public_key_hex(), id2.public_key_hex());

        match prev {
            Some(v) => unsafe { std::env::set_var("EDGE_WORKER_KEY", v) },
            None => unsafe { std::env::remove_var("EDGE_WORKER_KEY") },
        }
    }

    /// `EDGE_WORKER_KEY` (inline hex) wins over the on-disk path. The
    /// inline value is NOT written to disk — operators mounting a
    /// secret into the container as an env var don't want a copy on
    /// the persistent volume.
    #[test]
    fn worker_key_inline_env_overrides_disk() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("identity.key");
        // Pre-populate the on-disk path so we'd notice if the env
        // override didn't actually win.
        let _disk = WorkerIdentity::load_or_create(&path).expect("disk seed");
        let disk_pubkey = std::fs::read_to_string("/dev/null").ok(); // unused; just demonstrating the file exists

        // Build a known seed and use it inline.
        let seed_hex = "11".repeat(32);
        let prev = std::env::var("EDGE_WORKER_KEY").ok();
        unsafe { std::env::set_var("EDGE_WORKER_KEY", &seed_hex) };

        let id = WorkerIdentity::load_or_create(&path).expect("inline load");
        let expected = {
            let seed = parse_hex_seed(&seed_hex).expect("seed parse");
            let sk = SigningKey::from_bytes(&seed);
            hex::encode(sk.verifying_key().to_bytes())
        };
        assert_eq!(id.public_key_hex(), expected);

        // Restore env.
        match prev {
            Some(v) => unsafe { std::env::set_var("EDGE_WORKER_KEY", v) },
            None => unsafe { std::env::remove_var("EDGE_WORKER_KEY") },
        }
        let _ = disk_pubkey;
    }

    /// Round-trip: signing with the private key produces a signature
    /// that verifies against the corresponding public key. This is
    /// the actual round-trip the bootstrap enrollment handshake
    /// depends on (the CP re-derives the verification pubkey from
    /// the hex-encoded `public_key` field).
    #[test]
    fn worker_key_sign_challenge_round_trips() {
        let dir = tempdir().expect("tempdir");
        let prev = std::env::var("EDGE_WORKER_KEY").ok();
        unsafe { std::env::remove_var("EDGE_WORKER_KEY") };

        let id = WorkerIdentity::load_or_create(&dir.path().join("id.key")).expect("id");
        let msg = b"hello, edgecloud worker";
        let sig_bytes = id.sign(msg);
        assert_eq!(sig_bytes.len(), 64);

        // Rebuild the verifying key from the cached hex pubkey and
        // verify the signature.
        use ed25519_dalek::Verifier;
        let pk_bytes = hex::decode(id.public_key_hex()).expect("hex decode");
        let mut pk_arr = [0u8; 32];
        pk_arr.copy_from_slice(&pk_bytes);
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&pk_arr).expect("vk");
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        vk.verify(msg, &sig).expect("signature must verify");

        match prev {
            Some(v) => unsafe { std::env::set_var("EDGE_WORKER_KEY", v) },
            None => unsafe { std::env::remove_var("EDGE_WORKER_KEY") },
        }
    }

    /// The identity file must be written with mode 0600 so an
    /// operator who leaks a backup of `.worker-cache/` doesn't
    /// hand an attacker the worker's private key.
    ///
    /// Skipped on Windows (no Unix mode bits). CI runs Linux so the
    /// default path always exercises this assertion.
    #[cfg(unix)]
    #[test]
    fn worker_key_uses_0600_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("identity.key");
        let prev = std::env::var("EDGE_WORKER_KEY").ok();
        unsafe { std::env::remove_var("EDGE_WORKER_KEY") };

        let _id = WorkerIdentity::load_or_create(&path).expect("generate");
        let meta = std::fs::metadata(&path).expect("stat");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "identity.key must be mode 0600, got {mode:o}");

        match prev {
            Some(v) => unsafe { std::env::set_var("EDGE_WORKER_KEY", v) },
            None => unsafe { std::env::remove_var("EDGE_WORKER_KEY") },
        }
    }

    #[test]
    fn worker_key_load_rejects_short_file() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("bad.key");
        std::fs::write(&path, [0u8; 16]).expect("write");
        let err = WorkerIdentity::load(&path).expect_err("must reject short file");
        assert!(
            err.to_string().contains("wrong length"),
            "error must mention length; got: {err}"
        );
    }

    #[test]
    fn worker_key_inline_rejects_bad_hex() {
        let prev = std::env::var("EDGE_WORKER_KEY").ok();
        unsafe { std::env::set_var("EDGE_WORKER_KEY", "not-hex") };
        let err = WorkerIdentity::load_or_create(Path::new("/tmp/never-read"))
            .expect_err("must reject bad hex");
        assert!(
            err.to_string().contains("EDGE_WORKER_KEY"),
            "error must name the env var; got: {err}"
        );
        match prev {
            Some(v) => unsafe { std::env::set_var("EDGE_WORKER_KEY", v) },
            None => unsafe { std::env::remove_var("EDGE_WORKER_KEY") },
        }
    }
}
