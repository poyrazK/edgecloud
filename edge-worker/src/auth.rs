//! Worker JWT signing — produces bearer tokens for outbound calls to the
//! control plane's `WorkerAuth`-protected endpoints.
//!
//! The Go control plane's `internal/middleware/worker.go` accepts HMAC-SHA256
//! JWTs with `iss` (issuer), `exp` (expiry), `worker_id`, `tenant_id`, and
//! `apps`. We match that wire format and add a `region` claim (the control
//! plane ignores unknown fields).
//!
//! Tokens are signed once and cached for the lifetime of the worker; they are
//! refreshed when within 5 minutes of expiry. This keeps signing off the
//! hot path of every HTTP request while staying well ahead of the clock
//! skew between worker and control plane.

use anyhow::Context;
use jsonwebtoken::{encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// How long before the cached token's `exp` we re-sign. 5 minutes is well
/// above typical NTP drift and gives a comfortable margin if a request
/// stalls at the control plane right as the old token crosses `exp`.
const REFRESH_LEAD: Duration = Duration::from_secs(5 * 60);

/// Default token TTL. Matches the Go control plane's `JWTConfig.TTL` default
/// (24h) and the whitepaper's §9.3 internal endpoint spec.
const DEFAULT_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Worker JWT claims — wire-compatible with `middleware.WorkerClaims` (Go).
///
/// `iss`/`exp`/`iat`/`jti` are standard JWT claims. `worker_id`, `tenant_id`,
/// `region`, and `apps` are worker-specific. The Go control plane reads
/// worker_id, tenant_id, and apps; `region` and `jti` are informational and
/// ignored — but `jti` (random per-token) gives us replay protection and
/// guarantees each `sign()` produces a unique token even within the same
/// second.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerClaims {
    pub iss: String,
    pub exp: usize,
    pub iat: usize,
    pub jti: String,
    pub worker_id: String,
    pub tenant_id: String,
    pub region: String,
    #[serde(default)]
    pub apps: Vec<String>,
}

/// Thread-safe JWT signer with token caching.
///
/// `sign()` returns the same token for repeated calls as long as the cached
/// token's expiry is more than `REFRESH_LEAD` away. The mutex is held only
/// for the read/write of the (token, expires_at) tuple — JWT encoding
/// (potentially expensive) happens outside the lock.
///
/// When `kid` is `Some(...)`, the JWT header includes a `kid` field so the
/// control plane can select the correct verification key during rotation.
/// Issue #430 added the per-worker `wkr_` kid namespace; `set_secret`
/// swaps the kid + secret together during the bootstrap enrollment
/// handshake.
pub struct WorkerJwtSigner {
    secret: Mutex<Vec<u8>>,
    kid: Mutex<Option<String>>,
    issuer: String,
    worker_id: String,
    region: String,
    tenant_id: String,
    ttl: Duration,
    cache: Mutex<Option<CachedToken>>,
}

struct CachedToken {
    token: String,
    /// When the cached token expires (Instant, not wall clock — Instant is
    /// monotonic and immune to NTP step adjustments).
    expires_at: Instant,
}

impl WorkerJwtSigner {
    pub fn new(
        secret: impl Into<Vec<u8>>,
        kid: Option<String>,
        issuer: impl Into<String>,
        worker_id: impl Into<String>,
        region: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            secret: Mutex::new(secret.into()),
            kid: Mutex::new(kid),
            issuer: issuer.into(),
            worker_id: worker_id.into(),
            region: region.into(),
            tenant_id: tenant_id.into(),
            ttl: DEFAULT_TTL,
            cache: Mutex::new(None),
        })
    }

    /// Returns a current bearer token. Returns the cached value if it is
    /// still fresh; otherwise encodes a fresh one and updates the cache.
    pub fn sign(&self) -> String {
        let now = Instant::now();

        // Fast path: cached token is still fresh (more than REFRESH_LEAD
        // before expiry). Hold the lock only for the bool check.
        // `unwrap_or_else(|e| e.into_inner())` recovers from poisoning — if a
        // previous holder panicked we still want to issue a token.
        {
            let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ct) = cache.as_ref() {
                if ct.expires_at.saturating_duration_since(now) > REFRESH_LEAD {
                    return ct.token.clone();
                }
            }
        }

        // Slow path: encode a fresh token outside the lock, then swap it in.
        let token = self.encode();
        let expires_at = now + self.ttl;

        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        *cache = Some(CachedToken {
            token: token.clone(),
            expires_at,
        });
        token
    }

    /// Force the next `sign()` to re-encode. Useful in tests that want to
    /// assert the cache is invalidated on expiry.
    #[cfg(test)]
    pub fn expire_cache_for_test(&self) {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        *cache = None;
    }

    /// Replace the signing secret, invalidate the token cache, and
    /// (if provided) update the `kid` header. Used by the bootstrap
    /// handshake (issue #104 + #430) to set the per-worker derived
    /// secret + kid after enrollment without recreating the signer.
    /// The next call to `sign()` will re-encode with the new secret
    /// and `kid`.
    ///
    /// `new_kid` semantics:
    /// - `Some(kid)` → overwrite the current kid (use this to set the
    ///   per-worker `wkr_` kid after a successful enrollment).
    /// - `None` → leave the existing kid untouched.
    ///
    /// The split exists so a future "rotate just the secret, keep the
    /// same kid" call site doesn't have to know the current kid value.
    pub fn set_secret(&self, new_secret: impl Into<Vec<u8>>, new_kid: Option<String>) {
        *self.secret.lock().unwrap_or_else(|e| e.into_inner()) = new_secret.into();
        if let Some(kid) = new_kid {
            *self.kid.lock().unwrap_or_else(|e| e.into_inner()) = Some(kid);
        }
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        *cache = None;
    }

    /// Construct a signer with no secret or kid preloaded. The
    /// resulting signer signs with an empty secret until
    /// `set_secret` is called. Used by `main.rs` when the JWT
    /// secret comes from the post-#430 bootstrap enrollment path
    /// (where the secret + kid are produced together at runtime)
    /// — the `new` constructor doesn't fit that flow because it
    /// takes both as static arguments.
    pub fn empty(
        issuer: impl Into<String>,
        worker_id: impl Into<String>,
        region: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            secret: Mutex::new(Vec::new()),
            kid: Mutex::new(None),
            issuer: issuer.into(),
            worker_id: worker_id.into(),
            region: region.into(),
            tenant_id: tenant_id.into(),
            ttl: DEFAULT_TTL,
            cache: Mutex::new(None),
        })
    }

    fn encode(&self) -> String {
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs() as usize;

        let claims = WorkerClaims {
            iss: self.issuer.clone(),
            exp: now_unix + self.ttl.as_secs() as usize,
            iat: now_unix,
            jti: Uuid::new_v4().to_string(),
            worker_id: self.worker_id.clone(),
            tenant_id: self.tenant_id.clone(),
            region: self.region.clone(),
            apps: Vec::new(),
        };

        let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
        let kid_snapshot = self.kid.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(ref kid) = kid_snapshot {
            header.kid = Some(kid.clone());
        }

        let secret = self.secret.lock().unwrap_or_else(|e| e.into_inner());
        encode(&header, &claims, &EncodingKey::from_secret(&secret))
            .expect("HS256 signing should not fail")
    }
}

/// TEST-ONLY: do not call from production code paths; production
/// workers use `sign()` which always produces valid output.
///
/// Parses and validates a worker JWT with the given secret. Used by the
/// `signed_token_parses_with_correct_claims` and
/// `verify_rejects_wrong_secret` unit tests AND by
/// `tests/integration_tests.rs::signed_token_round_trips` to round-trip
/// the signed token back through the same wire format the control plane
/// expects.
///
/// `expected_iss` is the issuer this code path is willing to accept; the
/// `jsonwebtoken` crate's `Validation::set_issuer` pins it. This is
/// defense-in-depth on top of the control plane's middleware check
/// (see `edge-control-plane/internal/middleware/worker.go`) — if the
/// signer drifts away from the canonical issuer, the round-trip test
/// fails here too, before the request ever hits the wire.
///
/// `#[doc(hidden)]` keeps the function compiled into the lib (the
/// integration test target is a separate `[[test]]` and doesn't enable
/// the production build's #[cfg(test)] gates) but signals "not for
/// public use" to anyone reading the generated docs. The `verify`
/// name is too tempting on a hot path: a future maintainer could
/// accidentally call it on a hot path and inherit a weaker
/// validation set than the control plane (no `aud` check, no
/// `exp_required`). The rename to `verify_for_test_only` makes the
/// intent unambiguous.
#[doc(hidden)]
#[allow(dead_code)]
pub fn verify_for_test_only(
    secret: &[u8],
    expected_iss: &str,
    token: &str,
) -> anyhow::Result<WorkerClaims> {
    let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.validate_aud = false;
    validation.set_issuer(&[expected_iss]);

    let data = jsonwebtoken::decode::<WorkerClaims>(
        token,
        &DecodingKey::from_secret(secret),
        &validation,
    )?;
    Ok(data.claims)
}

/// Persisted per-worker signing secret (issue #430).
///
/// After a successful `/worker-bootstrap/enroll` handshake the worker
/// writes `{kid, secret, public_key_hex}` to disk (mode 0600) so
/// subsequent restarts skip the bootstrap. The format is a tiny
/// length-prefixed binary record — not JSON — to keep parsing
/// allocation-free at boot.
///
/// Layout (all big-endian):
/// - u32: magic = `b"EWIS"` (`0x45574953`)
/// - u8:  version (= 1)
/// - u32: kid_len
/// - [u8; kid_len]: kid bytes
/// - u32: secret_len
/// - [u8; secret_len]: raw HS256 secret bytes
/// - u32: pubkey_len
/// - [u8; pubkey_len]: lowercase hex public_key
pub const IDENTITY_RECORD_MAGIC: u32 = 0x45574953;
pub const IDENTITY_RECORD_VERSION: u8 = 1;

/// Persisted identity (kid + secret + pubkey). Owned by the caller;
/// the on-disk format is rebuilt via `to_bytes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedIdentity {
    pub kid: String,
    pub secret: Vec<u8>,
    pub public_key_hex: String,
}

impl PersistedIdentity {
    pub fn to_bytes(&self) -> Vec<u8> {
        let kid = self.kid.as_bytes();
        let pk = self.public_key_hex.as_bytes();
        let mut out =
            Vec::with_capacity(4 + 1 + 4 + kid.len() + 4 + self.secret.len() + 4 + pk.len());
        out.extend_from_slice(&IDENTITY_RECORD_MAGIC.to_be_bytes());
        out.push(IDENTITY_RECORD_VERSION);
        out.extend_from_slice(&(kid.len() as u32).to_be_bytes());
        out.extend_from_slice(kid);
        out.extend_from_slice(&(self.secret.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.secret);
        out.extend_from_slice(&(pk.len() as u32).to_be_bytes());
        out.extend_from_slice(pk);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(bytes.len() > 4, "identity record too short for header");
        let magic = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        anyhow::ensure!(
            magic == IDENTITY_RECORD_MAGIC,
            "identity record magic mismatch (got {magic:#x}, expected {:#x})",
            IDENTITY_RECORD_MAGIC
        );
        let version = bytes[4];
        anyhow::ensure!(
            version == IDENTITY_RECORD_VERSION,
            "identity record version {version} not supported (expected {IDENTITY_RECORD_VERSION})"
        );
        let mut cur = 5usize;
        let kid = read_length_prefixed(bytes, &mut cur, "kid")?;
        let secret = read_length_prefixed(bytes, &mut cur, "secret")?;
        let pk = read_length_prefixed(bytes, &mut cur, "public_key_hex")?;
        anyhow::ensure!(
            cur == bytes.len(),
            "identity record has trailing bytes ({} extra)",
            bytes.len() - cur
        );
        let kid = std::str::from_utf8(&kid)
            .context("kid is not valid utf-8")?
            .to_string();
        let public_key_hex = std::str::from_utf8(&pk)
            .context("public_key_hex is not valid utf-8")?
            .to_string();
        Ok(Self {
            kid,
            secret,
            public_key_hex,
        })
    }
}

fn read_length_prefixed(bytes: &[u8], cur: &mut usize, field: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        bytes.len() >= *cur + 4,
        "identity record truncated reading {field} length"
    );
    let len = u32::from_be_bytes([
        bytes[*cur],
        bytes[*cur + 1],
        bytes[*cur + 2],
        bytes[*cur + 3],
    ]) as usize;
    *cur += 4;
    anyhow::ensure!(
        bytes.len() >= *cur + len,
        "identity record truncated reading {field} body (wanted {len} bytes, have {})",
        bytes.len() - *cur
    );
    let out = bytes[*cur..*cur + len].to_vec();
    *cur += len;
    Ok(out)
}

/// Persist the worker's per-worker signing secret to `path` with
/// mode 0600. Used by `main.rs` immediately after the bootstrap
/// enrollment handshake. Overwrites any existing file.
///
/// The atomic shape matches `worker_key::write_secret_file`:
/// write-to-tmp + fsync + rename, then explicit chmod so a crashed
/// worker can't leave a world-readable secret on disk.
pub fn persist_identity(
    path: &std::path::Path,
    identity: &PersistedIdentity,
) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "creating parent dir for persisted identity: {}",
                    parent.display()
                )
            })?;
        }
    }
    let tmp = path.with_extension("identity.tmp");
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(&identity.to_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path)?.permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(path, perms)?;
    }
    Ok(())
}

/// Load a previously-persisted per-worker signing secret from
/// `path`. Returns `Ok(None)` if the file does not exist (the common
/// first-boot case). Returns `Err` for malformed records — a corrupt
/// identity file must NOT silently fall through to bootstrap because
/// that would let an attacker who can write to the cache directory
/// forge a worker identity.
pub fn load_persisted_identity(
    path: &std::path::Path,
) -> anyhow::Result<Option<PersistedIdentity>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(PersistedIdentity::from_bytes(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::Error::new(e).context(format!(
            "reading persisted identity from {}",
            path.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer() -> Arc<WorkerJwtSigner> {
        WorkerJwtSigner::new(
            "test-secret",
            Some("test-kid".to_string()),
            "edgecloud",
            "w_fra_abc123",
            "fra",
            "t_tenant1",
        )
    }

    #[test]
    fn sign_produces_a_token() {
        let s = signer();
        let t = s.sign();
        assert!(!t.is_empty());
        // A JWT has 3 dot-separated segments.
        assert_eq!(t.matches('.').count(), 2);
    }

    #[test]
    fn sign_is_deterministic_while_cached() {
        let s = signer();
        let t1 = s.sign();
        let t2 = s.sign();
        assert_eq!(
            t1, t2,
            "second sign() within cache window must return same token"
        );
    }

    #[test]
    fn sign_refreshes_after_cache_invalidated() {
        let s = signer();
        let t1 = s.sign();
        s.expire_cache_for_test();
        let t2 = s.sign();
        assert_ne!(
            t1, t2,
            "after expire_cache_for_test() the next token must be fresh"
        );
    }

    #[test]
    fn signed_token_parses_with_correct_claims() {
        let s = signer();
        let t = s.sign();
        let claims =
            verify_for_test_only(b"test-secret", "edgecloud", &t).expect("verify should succeed");
        assert_eq!(claims.iss, "edgecloud");
        assert_eq!(claims.worker_id, "w_fra_abc123");
        assert_eq!(claims.tenant_id, "t_tenant1");
        assert_eq!(claims.region, "fra");
        // jti must be present (it's the source of per-token uniqueness).
        assert!(!claims.jti.is_empty(), "jti must be non-empty");
        // exp must be after iat (sanity check on TTL wiring).
        assert!(claims.exp > claims.iat);
        // exp - iat must equal the TTL (24h).
        assert_eq!(claims.exp - claims.iat, DEFAULT_TTL.as_secs() as usize);
    }

    /// Issue #430: the JWT header carries a `kid` so the control
    /// plane can route the verification key. This test parses the
    /// raw header (the jsonwebtoken crate hides it from `decode`)
    /// and asserts the kid round-trips.
    #[test]
    fn signed_token_includes_kid_header() {
        let s = signer();
        let t = s.sign();
        let header_b64 = t.split('.').next().expect("header segment");
        let header_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            header_b64,
        )
        .expect("header b64");
        let header_json: serde_json::Value =
            serde_json::from_slice(&header_bytes).expect("header json");
        assert_eq!(
            header_json["kid"].as_str(),
            Some("test-kid"),
            "JWT header must carry the configured kid"
        );
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let s = signer();
        let t = s.sign();
        assert!(verify_for_test_only(b"wrong-secret", "edgecloud", &t).is_err());
    }

    /// `verify_for_test_only` pins the issuer via `Validation::set_issuer`.
    /// A token whose `iss` does not match the expected value must be
    /// rejected — this is the Rust-side mirror of the control plane's
    /// middleware check (Commit 1) and catches issuer drift in the signer.
    #[test]
    fn verify_rejects_wrong_issuer() {
        let s = signer(); // mints with iss = "edgecloud"
        let t = s.sign();
        let err = verify_for_test_only(b"test-secret", "some-other-issuer", &t)
            .expect_err("verify with wrong expected_iss must fail");
        assert!(
            err.to_string().to_lowercase().contains("issuer")
                || err.to_string().to_lowercase().contains("iss"),
            "error should mention issuer, got: {}",
            err
        );
    }

    // ── set_secret (issue #430) ────────────────────────────────────
    //
    // The bootstrap enrollment handshake lands here after a
    // successful /worker-bootstrap/enroll call. set_secret must:
    //   (1) atomically swap the secret AND (when supplied) the kid,
    //   (2) invalidate the cached token so the next sign() re-encodes,
    //   (3) leave the kid untouched when called with `kid = None`.

    /// Swapping the secret invalidates the token cache so the next
    /// sign() re-encodes under the new secret. Tokens minted before
    /// the swap must NOT verify under the new secret.
    #[test]
    fn set_secret_invalidates_cache_and_rotates_token() {
        let s = signer();
        let before = s.sign();
        s.set_secret(b"rotated-secret".to_vec(), None);
        let after = s.sign();
        assert_ne!(
            before, after,
            "sign() must produce a fresh token after set_secret"
        );
        assert!(
            verify_for_test_only(b"rotated-secret", "edgecloud", &after).is_ok(),
            "after-secret token must verify with the rotated secret"
        );
        assert!(
            verify_for_test_only(b"test-secret", "edgecloud", &after).is_err(),
            "after-secret token must NOT verify with the old secret"
        );
    }

    /// Passing `Some(kid)` updates the JWT header's `kid` claim
    /// starting with the next encoded token.
    #[test]
    fn set_secret_updates_kid_header() {
        let s = signer();
        // Pre-swap token carries kid="test-kid" (from signer()).
        let before = s.sign();
        let before_kid = extract_kid(&before);
        assert_eq!(before_kid.as_deref(), Some("test-kid"));

        s.set_secret(b"rotated-secret".to_vec(), Some("wkr_deadbeef".to_string()));
        let after = s.sign();
        let after_kid = extract_kid(&after);
        assert_eq!(
            after_kid.as_deref(),
            Some("wkr_deadbeef"),
            "set_secret must propagate the new kid to the JWT header"
        );
    }

    /// Passing `kid = None` leaves the existing kid in place. This
    /// supports a future "rotate only the secret" call site without
    /// having to know the current kid.
    #[test]
    fn set_secret_without_kid_preserves_existing_kid() {
        let s = signer();
        s.set_secret(b"rotated-secret".to_vec(), None);
        let after = s.sign();
        let after_kid = extract_kid(&after);
        assert_eq!(
            after_kid.as_deref(),
            Some("test-kid"),
            "kid must NOT change when set_secret is called with new_kid=None"
        );
    }

    /// `empty()` constructs a signer with no secret or kid, and
    /// set_secret brings it to a working state. This is the
    /// bootstrap-then-set_secret flow that `main.rs` uses when
    /// `EDGE_WORKER_REENROLL_ON_BOOT=true` and no persisted secret
    /// exists on disk.
    #[test]
    fn empty_then_set_secret_produces_valid_tokens() {
        let s = WorkerJwtSigner::empty("edgecloud", "w_fra_abc", "fra", "t_tenant1");
        // Before set_secret, the signer signs with an empty secret.
        let t = s.sign();
        assert!(
            verify_for_test_only(b"", "edgecloud", &t).is_ok(),
            "empty signer must produce tokens verifying with an empty secret"
        );
        // After set_secret, the new secret takes over.
        s.set_secret(b"new-secret".to_vec(), Some("wkr_cafef00d".to_string()));
        let t2 = s.sign();
        assert!(verify_for_test_only(b"new-secret", "edgecloud", &t2).is_ok());
        assert_eq!(extract_kid(&t2).as_deref(), Some("wkr_cafef00d"));
    }

    /// Pull the `kid` claim out of a JWT's header segment. Used by
    /// the set_secret tests above to assert the header rotates.
    fn extract_kid(token: &str) -> Option<String> {
        let header_b64 = token.split('.').next()?;
        let header_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            header_b64,
        )
        .ok()?;
        let v: serde_json::Value = serde_json::from_slice(&header_bytes).ok()?;
        v["kid"].as_str().map(|s| s.to_string())
    }

    // ── PersistedIdentity round-trip (issue #430) ─────────────────
    //
    // The disk-persistence helpers (`persist_identity`,
    // `load_persisted_identity`) drive the "skip bootstrap on warm
    // restart" path. Their tests are co-located here because the
    // format is auth-specific — a different module would own this
    // record in a larger crate.

    #[test]
    fn persisted_identity_round_trips() {
        let id = PersistedIdentity {
            kid: "wkr_deadbeef".to_string(),
            secret: b"\x01\x02\x03\x04\x05\x06\x07\x08".to_vec(),
            public_key_hex: "abcd".repeat(16),
        };
        let bytes = id.to_bytes();
        let back = PersistedIdentity::from_bytes(&bytes).expect("parse");
        assert_eq!(back, id);
    }

    #[test]
    fn persisted_identity_rejects_bad_magic() {
        let mut bytes = PersistedIdentity {
            kid: "k".to_string(),
            secret: vec![1, 2, 3],
            public_key_hex: "ab".to_string(),
        }
        .to_bytes();
        bytes[0] = 0;
        let err = PersistedIdentity::from_bytes(&bytes).expect_err("bad magic");
        assert!(err.to_string().contains("magic"));
    }

    #[test]
    fn persisted_identity_rejects_unknown_version() {
        let mut bytes = PersistedIdentity {
            kid: "k".to_string(),
            secret: vec![1, 2, 3],
            public_key_hex: "ab".to_string(),
        }
        .to_bytes();
        bytes[4] = 99; // version
        let err = PersistedIdentity::from_bytes(&bytes).expect_err("bad version");
        assert!(err.to_string().contains("version"));
    }

    #[test]
    fn persisted_identity_rejects_truncated_body() {
        let bytes = PersistedIdentity {
            kid: "k".to_string(),
            secret: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            public_key_hex: "ab".to_string(),
        }
        .to_bytes();
        let truncated = &bytes[..bytes.len() - 4];
        let err = PersistedIdentity::from_bytes(truncated).expect_err("truncated");
        assert!(
            err.to_string().contains("trailing") || err.to_string().contains("truncated"),
            "error must describe truncation: {err}"
        );
    }

    #[test]
    fn persist_and_load_round_trips_to_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("identity.key");
        let id = PersistedIdentity {
            kid: "wkr_1234abcd".to_string(),
            secret: vec![0xAA; 32],
            public_key_hex: "11".repeat(32),
        };
        persist_identity(&path, &id).expect("persist");
        let loaded = load_persisted_identity(&path)
            .expect("load")
            .expect("file exists");
        assert_eq!(loaded, id);
    }

    #[test]
    fn load_persisted_returns_none_when_absent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nope.key");
        let got = load_persisted_identity(&path).expect("missing file");
        assert!(got.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn persist_identity_uses_0600_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("identity.key");
        let id = PersistedIdentity {
            kid: "wkr_x".to_string(),
            secret: vec![0xBB; 32],
            public_key_hex: "22".repeat(32),
        };
        persist_identity(&path, &id).expect("persist");
        let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "identity record must be 0600, got {mode:o}");
    }
}
