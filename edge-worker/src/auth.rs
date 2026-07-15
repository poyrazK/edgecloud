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
//!
//! ## Atomic snapshot (issue #504)
//!
//! Pre-#504 the signer held separate `Mutex<Vec<u8>>` (secret) + `Mutex<Option<String>>` (kid)
//!     + `Mutex<Option<CachedToken>>` (cache). A concurrent `sign()` + rotation
//!     could read the **new** cache under the **old** secret (or vice versa) and
//!     mint a token that verifies under neither. Post-#504 one `RwLock<Arc<TokenSnapshot>>`
//!     holds the (token, kid, secret, expires_at, generation) tuple; refresh
//!     builds a fresh snapshot OUTSIDE the lock and atomically swaps the
//!     `Arc`. Read paths clone the `Arc` and access the immutable snapshot
//!     without holding any further lock — true lock-free reads.
//!
//! The `generation` counter is the load-bearing piece for the reactive 401
//! helper (`with_token_refresh`): it lets the helper compare-before-invalidate
//! and never clobber a concurrent successful refresh with a stale 401.

use anyhow::Context;
use jsonwebtoken::{encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
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

/// Thread-safe JWT signer with token caching + secret rotation.
///
/// `sign()` returns the same token for repeated calls as long as the cached
/// token's expiry is more than `REFRESH_LEAD` away. The read path clones an
/// `Arc<TokenSnapshot>` and never blocks on the writer (no further lock
/// acquired). JWT encoding for the slow path happens outside any lock.
///
/// When `kid` is `Some(...)`, the JWT header includes a `kid` field so the
/// control plane can select the correct verification key during rotation.
/// Issue #430 added the per-worker `wkr_` kid namespace; the snapshot
/// swap path swaps secret + kid + token together (atomic swap of the
/// `Arc<TokenSnapshot>`).
///
/// ## Atomic snapshot (issue #504)
///
/// ALL mutable state — secret, kid, cached token, expires_at, generation
/// — lives inside one immutable `TokenSnapshot`, stored behind a single
/// `RwLock<Arc<TokenSnapshot>>`. Concurrent `sign()` + `install_snapshot()`
/// CANNOT produce a torn observation: a reader either sees the old Arc
/// (with the old secret+token+kid triple) or the new Arc (with the new
/// triple), never a mix.
pub struct WorkerJwtSigner {
    state: RwLock<Arc<TokenSnapshot>>,
    issuer: String,
    worker_id: String,
    region: String,
    tenant_id: String,
    ttl: Duration,
}

/// Single atomic snapshot of the signer's state. Replaces the pre-#504
/// `CachedToken { token, expires_at }` + split `Mutex<Option<String>>`
/// for kid + split `Mutex<Vec<u8>>` for secret (now eliminated).
///
/// All five fields travel together under one `Arc`, so once installed a
/// reader is guaranteed to see them in lock-step until the next
/// `install_snapshot` swap.
#[derive(Debug, Clone)]
pub struct TokenSnapshot {
    pub token: String,
    pub kid: Option<String>,
    /// Raw HS256 secret bytes. Held only inside the snapshot — never
    /// reaches `sign()`. `sign()` reads the already-encoded `token`
    /// from the snapshot directly.
    pub secret: Vec<u8>,
    /// When the cached token expires (Instant, not wall clock — Instant is
    /// monotonic and immune to NTP step adjustments).
    pub expires_at: Instant,
    /// Monotonic counter, bumped each time `install_snapshot` swaps a
    /// fresh snapshot in. Lets `with_token_refresh` distinguish a snapshot
    /// it installed from one a concurrent refresh installed.
    pub generation: u64,
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
        let secret_bytes: Vec<u8> = secret.into();
        let s = Arc::new(Self {
            state: RwLock::new(Arc::new(TokenSnapshot {
                token: String::new(),
                kid: kid.clone(),
                secret: secret_bytes.clone(),
                expires_at: Instant::now(), // immediately stale → triggers first encode
                generation: 0,
            })),
            issuer: issuer.into(),
            worker_id: worker_id.into(),
            region: region.into(),
            tenant_id: tenant_id.into(),
            ttl: DEFAULT_TTL,
        });
        // Eagerly encode so `sign()` callers see a valid token on the
        // first call (matches pre-#504 behavior). The initial snapshot
        // is overwritten in one write lock with the encoded token +
        // bumped `expires_at`.
        s.install_initial_snapshot(&secret_bytes, kid);
        s
    }

    /// Returns a current bearer token. Returns the cached value if it is
    /// still fresh; otherwise encodes a fresh one and updates the cache.
    pub fn sign(&self) -> String {
        let now = Instant::now();

        // Fast path: cached token is still fresh (more than REFRESH_LEAD
        // before expiry). Clone the Arc — no further lock acquired.
        // `unwrap_or_else(|e| e.into_inner())` recovers from poisoning — if a
        // previous holder panicked we still want to issue a token.
        let snap = self.snapshot_arc();
        if snap.expires_at.saturating_duration_since(now) > REFRESH_LEAD && !snap.token.is_empty() {
            return snap.token.clone();
        }

        // Slow path: encode a fresh token using the snapshot's secret
        // bytes (no separate secret lock to take — the secret lives
        // in the snapshot). Install the new snapshot atomically.
        let token = self.encode(&snap);
        let new_snap = TokenSnapshot {
            token: token.clone(),
            kid: snap.kid.clone(),
            secret: snap.secret.clone(),
            expires_at: now + self.ttl,
            // generation is NOT bumped here — `sign()` re-encoding is
            // idempotent for callers (same secret, same kid); only
            // `install_snapshot` (and the refresh path it wraps) bump it.
            generation: snap.generation,
        };
        self.swap(new_snap);
        token
    }

    /// Force the next `sign()` to re-encode. Useful in tests that want to
    /// assert the cache is invalidated on expiry.
    #[cfg(test)]
    pub fn expire_cache_for_test(&self) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        let snap = Arc::make_mut(&mut state);
        // Drop the token to empty so `sign()` re-encodes; expires_at
        // stays monotonic so the REFRESH_LEAD check still fires.
        snap.token = String::new();
    }

    /// Atomically install a new snapshot. Used by the bootstrap handshake
    /// (issue #104 + #430) to rotate the per-worker derived secret + kid
    /// and the freshly-encoded token together. The whole tuple is rewritten
    /// in one `Arc` swap — readers see either the old snapshot or the new
    /// one, never a mix.
    ///
    /// Empty-secret guard: if `new_secret` is `Vec::new()` (or otherwise
    /// empty), the call refuses to silently wipe the signer and returns
    /// without changing state. This closes Defect 4 from the #504 review:
    /// a `Noop`/`Skipped` refresh outcome used to overwrite the secret
    /// with an empty Vec.
    pub fn install_snapshot(&self, new_secret: impl Into<Vec<u8>>, new_kid: Option<String>) {
        let secret_bytes: Vec<u8> = new_secret.into();
        let cur = self.snapshot();
        let kid = match new_kid {
            Some(k) => Some(k),
            None => cur.kid.clone(),
        };
        if secret_bytes.is_empty() {
            tracing::warn!(
                generation = cur.generation,
                "install_snapshot called with empty secret bytes; ignoring"
            );
            return;
        }
        let token = encode_with(&secret_bytes, kid.as_deref(), &self.claims());
        let new_snap = TokenSnapshot {
            token,
            kid,
            secret: secret_bytes,
            expires_at: Instant::now() + self.ttl,
            generation: cur.generation.wrapping_add(1),
        };
        self.swap(new_snap);
    }

    /// Test-only constructor that accepts a custom TTL instead of the
    /// hardcoded 24h default. See Issue #504 review-fix #6: this is
    /// retained for the in-process unit tests that pin a non-default
    /// TTL; the WireMock integration test that depends on a real
    /// `tokio` time driver is deferred (issue #710 follow-up).
    #[doc(hidden)]
    pub fn with_ttl(
        secret: impl Into<Vec<u8>>,
        kid: Option<String>,
        issuer: impl Into<String>,
        worker_id: impl Into<String>,
        region: impl Into<String>,
        tenant_id: impl Into<String>,
        ttl: Duration,
    ) -> Arc<Self> {
        let secret_bytes: Vec<u8> = secret.into();
        let s = Arc::new(Self {
            state: RwLock::new(Arc::new(TokenSnapshot {
                token: String::new(),
                kid: kid.clone(),
                secret: secret_bytes.clone(),
                expires_at: Instant::now(),
                generation: 0,
            })),
            issuer: issuer.into(),
            worker_id: worker_id.into(),
            region: region.into(),
            tenant_id: tenant_id.into(),
            ttl,
        });
        s.install_initial_snapshot(&secret_bytes, kid);
        s
    }

    /// Construct a signer with no secret or kid preloaded. The
    /// resulting signer signs with an empty secret until
    /// `install_snapshot` is called. Used by `main.rs` when the JWT
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
            state: RwLock::new(Arc::new(TokenSnapshot {
                token: String::new(),
                kid: None,
                secret: Vec::new(),
                expires_at: Instant::now(),
                generation: 0,
            })),
            issuer: issuer.into(),
            worker_id: worker_id.into(),
            region: region.into(),
            tenant_id: tenant_id.into(),
            ttl: DEFAULT_TTL,
        })
    }

    /// Read the current snapshot under the read lock. Exposed for the
    /// reactive 401 helper and the proactive refresh metrics so they
    /// can inspect generation / expires_at without taking `sign()`'s
    /// slow path.
    pub fn snapshot(&self) -> TokenSnapshot {
        // Returning `TokenSnapshot` (not the inner Arc) preserves the
        // v1 call-site API for tests; the clone happens here once.
        (**self.state.read().unwrap_or_else(|e| e.into_inner())).clone()
    }

    /// Clone the inner `Arc<TokenSnapshot>` for true lock-free reads.
    /// Used by the hot path of `sign()` and by the metrics
    /// `tick_worker_gauges` to read the expiry gauge without taking
    /// a writer.
    pub fn snapshot_arc(&self) -> Arc<TokenSnapshot> {
        self.state.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Install a freshly-built snapshot. Acquires the write lock exactly
    /// once; the swap is `Arc`-atomic for all readers. Bumps `generation`
    /// by 1 if the caller hasn't already (the helper bakes the bump
    /// into the snapshot it builds, so this is just the publish step).
    fn swap(&self, new_snap: TokenSnapshot) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        *state = Arc::new(new_snap);
    }

    /// Eagerly encode the initial token in `new()` / `with_ttl()` so the
    /// first `sign()` doesn't pay the encoding cost. Mirrors the
    /// pre-#504 "first call signs, subsequent calls hit cache" shape.
    fn install_initial_snapshot(&self, secret_bytes: &[u8], kid: Option<String>) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        let snap = Arc::make_mut(&mut state);
        // Use the secret the caller provided, not the one already in
        // the snapshot (the empty Vec<u8> case applies to `empty()`,
        // not `new()` / `with_ttl()`).
        snap.secret = secret_bytes.to_vec();
        snap.token = encode_with(secret_bytes, kid.as_deref(), &self.claims());
        snap.expires_at = Instant::now() + self.ttl;
        snap.kid = kid;
        snap.generation = 0;
    }

    /// Build the worker claims without encoding. The token's `iat` /
    /// `jti` / `exp` are recomputed at encode time (the jti must be
    /// unique per encoded token).
    fn claims(&self) -> WorkerClaims {
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs() as usize;
        WorkerClaims {
            iss: self.issuer.clone(),
            exp: now_unix + self.ttl.as_secs() as usize,
            iat: now_unix,
            jti: Uuid::new_v4().to_string(),
            worker_id: self.worker_id.clone(),
            tenant_id: self.tenant_id.clone(),
            region: self.region.clone(),
            apps: Vec::new(),
        }
    }

    /// Encode a token using the secret + kid from `snap`. The caller
    /// already holds a reference to the snapshot; this method makes no
    /// further lock acquisitions.
    fn encode(&self, snap: &TokenSnapshot) -> String {
        encode_with(&snap.secret, snap.kid.as_deref(), &self.claims())
    }
}

/// Encode a JWT with the given secret + kid. Standalone (not a method on
/// `WorkerJwtSigner`) so `install_initial_snapshot` can use it during
/// construction before `self.claims()` is callable from inside the
/// same-instance method chain. Always non-failing (HS256 + valid claims).
fn encode_with(secret: &[u8], kid: Option<&str>, claims: &WorkerClaims) -> String {
    let mut header = Header::new(jsonwebtoken::Algorithm::HS256);
    if let Some(k) = kid {
        header.kid = Some(k.to_string());
    }
    encode(&header, claims, &EncodingKey::from_secret(secret))
        .expect("HS256 signing should not fail")
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

    // ── install_snapshot (issue #504 review-fix #3) ───────────────
    //
    // install_snapshot is the rotation entry point: it atomically
    // installs a fresh (token, kid, secret, expires_at, generation)
    // tuple as a single `Arc<TokenSnapshot>` swap. Pre-#504 the same
    // critical section held three separate Mutex<>'s, which produced
    // a TOCTOU window.

    /// Swapping the secret invalidates the token cache so the next
    /// sign() re-encodes under the new secret. Tokens minted before
    /// the swap must NOT verify under the new secret.
    #[test]
    fn install_snapshot_invalidates_cache_and_rotates_token() {
        let s = signer();
        let before = s.sign();
        s.install_snapshot(b"rotated-secret".to_vec(), None);
        let after = s.sign();
        assert_ne!(
            before, after,
            "sign() must produce a fresh token after install_snapshot"
        );
        assert!(
            verify_for_test_only(b"rotated-secret", "edgecloud", &after).is_ok(),
            "after-rotation token must verify with the rotated secret"
        );
        assert!(
            verify_for_test_only(b"test-secret", "edgecloud", &after).is_err(),
            "after-rotation token must NOT verify with the old secret"
        );
    }

    /// Passing `Some(kid)` updates the JWT header's `kid` claim
    /// starting with the next encoded token.
    #[test]
    fn install_snapshot_updates_kid_header() {
        let s = signer();
        // Pre-swap token carries kid="test-kid" (from signer()).
        let before = s.sign();
        let before_kid = extract_kid(&before);
        assert_eq!(before_kid.as_deref(), Some("test-kid"));

        s.install_snapshot(b"rotated-secret".to_vec(), Some("wkr_deadbeef".to_string()));
        let after = s.sign();
        let after_kid = extract_kid(&after);
        assert_eq!(
            after_kid.as_deref(),
            Some("wkr_deadbeef"),
            "install_snapshot must propagate the new kid to the JWT header"
        );
    }

    /// Passing `kid = None` leaves the existing kid in place.
    #[test]
    fn install_snapshot_without_kid_preserves_existing_kid() {
        let s = signer();
        s.install_snapshot(b"rotated-secret".to_vec(), None);
        let after = s.sign();
        let after_kid = extract_kid(&after);
        assert_eq!(
            after_kid.as_deref(),
            Some("test-kid"),
            "kid must NOT change when install_snapshot is called with new_kid=None"
        );
    }

    /// Calling `install_snapshot` with an empty secret refuses to wipe
    /// the existing signer state. Pre-#504 this same scenario silently
    /// cloned an empty `Vec` and produced an unusable signer (Defect
    /// 4 in the review). The fail-closed posture keeps the previous
    /// snapshot serving requests.
    #[test]
    fn install_snapshot_with_empty_secret_is_a_noop() {
        let s = signer();
        let pre = s.sign();
        s.install_snapshot(Vec::<u8>::new(), None);
        // Snapshot should be unchanged: same token bytes (since the
        // token is cached, not the secret), same kid.
        let post = s.sign();
        assert_eq!(
            pre, post,
            "install_snapshot with empty secret must NOT alter the cached token"
        );
        assert_eq!(extract_kid(&post).as_deref(), Some("test-kid"));
    }

    /// `empty()` constructs a signer with no secret or kid, and
    /// install_snapshot brings it to a working state.
    #[test]
    fn empty_then_install_snapshot_produces_valid_tokens() {
        let s = WorkerJwtSigner::empty("edgecloud", "w_fra_abc", "fra", "t_tenant1");
        // Before install_snapshot, the signer signs with an empty secret.
        let t = s.sign();
        assert!(
            verify_for_test_only(b"", "edgecloud", &t).is_ok(),
            "empty signer must produce tokens verifying with an empty secret"
        );
        // After install_snapshot, the new secret takes over.
        s.install_snapshot(b"new-secret".to_vec(), Some("wkr_cafef00d".to_string()));
        let t2 = s.sign();
        assert!(verify_for_test_only(b"new-secret", "edgecloud", &t2).is_ok());
        assert_eq!(extract_kid(&t2).as_deref(), Some("wkr_cafef00d"));
    }

    /// Pull the `kid` claim out of a JWT's header segment. Used by
    /// the install_snapshot tests above to assert the header rotates.
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
        let path = dir.path().join("identity.bin");
        let id = PersistedIdentity {
            kid: "wkr_x".to_string(),
            secret: vec![0xBB; 32],
            public_key_hex: "22".repeat(32),
        };
        persist_identity(&path, &id).expect("persist");
        let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "identity record must be 0600, got {mode:o}");
    }

    // ── Arc<TokenSnapshot> swap (issue #504 review-fix #3) ─────────

    /// `install_snapshot` must atomically install the new (token, kid,
    /// secret) tuple. A reader that arrives after the swap sees
    /// ONLY the new token — never a token signed with the OLD
    /// secret OR the new kid header with the OLD token.
    #[test]
    fn install_snapshot_atomically_swaps_token_and_kid() {
        let s = WorkerJwtSigner::new(
            "old-secret",
            Some("old-kid".to_string()),
            "edgecloud",
            "w_x",
            "fra",
            "t_test",
        );
        let before = s.sign();
        let before_kid = extract_kid(&before);
        assert_eq!(before_kid.as_deref(), Some("old-kid"));

        s.install_snapshot(b"new-secret".to_vec(), Some("new-kid".to_string()));
        let after = s.sign();
        assert_eq!(extract_kid(&after).as_deref(), Some("new-kid"));
        // After-token must verify with new secret AND not old.
        assert!(
            verify_for_test_only(b"new-secret", "edgecloud", &after).is_ok(),
            "after-swap token must verify with the new secret"
        );
        assert!(
            verify_for_test_only(b"old-secret", "edgecloud", &after).is_err(),
            "after-swap token must NOT verify with the old secret"
        );
    }

    /// The `generation` counter is bumped on every successful
    /// `install_snapshot`. The reactive 401 helper
    /// (`with_token_refresh`, land in Commit 5) reads it BEFORE its
    /// retry and compares AFTER — if a concurrent refresh succeeded
    /// in between, the older 401 path won't clobber the newer
    /// snapshot.
    #[test]
    fn generation_counter_advances_on_install_snapshot() {
        let s = WorkerJwtSigner::new(
            "s",
            Some("k1".to_string()),
            "edgecloud",
            "w_x",
            "fra",
            "t_test",
        );
        let g0 = s.snapshot().generation;
        s.install_snapshot(b"s2".to_vec(), Some("k2".to_string()));
        let g1 = s.snapshot().generation;
        s.install_snapshot(b"s3".to_vec(), Some("k3".to_string()));
        let g2 = s.snapshot().generation;
        assert!(g1 > g0, "first install_snapshot must bump generation");
        assert!(g2 > g1, "second install_snapshot must bump generation");
    }

    /// `sign()` does NOT bump generation (signing is idempotent for
    /// the same secret+kid).
    #[test]
    fn sign_does_not_advance_generation() {
        let s = WorkerJwtSigner::new(
            "s",
            Some("k1".to_string()),
            "edgecloud",
            "w_x",
            "fra",
            "t_test",
        );
        let g0 = s.snapshot().generation;
        // Sign 10 times; generation should stay put.
        for _ in 0..10 {
            let _ = s.sign();
        }
        let g1 = s.snapshot().generation;
        assert_eq!(g0, g1, "pure sign() (no rotation) must NOT bump generation");
    }

    /// `snapshot_arc()` clones the inner `Arc<TokenSnapshot>` without
    /// taking the writer. The returned snapshot is independent of the
    /// signer's later swaps, exactly as `install_snapshot` intends.
    #[test]
    fn snapshot_arc_is_lock_free_and_independent_of_swaps() {
        let s = WorkerJwtSigner::new(
            "s",
            Some("k1".to_string()),
            "edgecloud",
            "w_x",
            "fra",
            "t_test",
        );
        let arc_before = s.snapshot_arc();
        let gen_before = arc_before.generation;
        s.install_snapshot(b"s2".to_vec(), Some("k2".to_string()));
        // The cloned Arc still observes the pre-swap snapshot.
        assert_eq!(
            arc_before.generation, gen_before,
            "snapshots handed out before the swap must not see the new generation"
        );
        // But a fresh Arc observes the post-swap state.
        let arc_after = s.snapshot_arc();
        assert!(
            arc_after.generation > gen_before,
            "fresh snapshot_arc() after the swap must see the bumped generation"
        );
    }

    /// `with_ttl(test-constructor)` lets tests drive a 1-second
    /// token lifetime without waiting 24h.
    #[test]
    fn with_ttl_constructor_respects_supplied_ttl() {
        let s = WorkerJwtSigner::with_ttl(
            "s",
            Some("k".to_string()),
            "edgecloud",
            "w_x",
            "fra",
            "t_test",
            Duration::from_secs(60),
        );
        let token = s.sign();
        let claims = verify_for_test_only(b"s", "edgecloud", &token).expect("verify");
        assert_eq!(
            claims.exp - claims.iat,
            60,
            "with_ttl must encode the supplied TTL into exp - iat"
        );
    }

    /// 10 concurrent `sign()` calls on a freshly-constructed
    /// signer (cache miss) all serialize correctly through the
    /// `RwLock`. None of them crash, and every caller gets a
    /// well-formed token. The invariant is weaker than "exactly
    /// one encode" — multiple encodes are fine; the property is
    /// "no panic, no torn snapshot."
    #[test]
    fn concurrent_signs_do_not_panic_or_torn_snapshot() {
        use std::thread;
        let s = WorkerJwtSigner::new(
            "s",
            Some("k1".to_string()),
            "edgecloud",
            "w_x",
            "fra",
            "t_test",
        );
        let s = Arc::new(s);
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let s = s.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        let token = s.sign();
                        assert!(verify_for_test_only(b"s", "edgecloud", &token).is_ok());
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("thread should not panic");
        }
    }
}
