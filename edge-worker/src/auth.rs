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
//! Two ways to provision the signing secret (Phase 4):
//!
//! - **Static** (legacy / fallback): a fixed `Vec<u8>` shared with the
//!   control plane via the `WORKER_JWT_SECRET` env var. Used for the
//!   deprecation fallback so existing deployments keep working.
//! - **Callback** (recommended): the signer holds an
//!   `Arc<dyn Fn() -> Result<JwtBundle>>` that, on cache miss, calls
//!   `bootstrap::fetch_token` against the control plane's
//!   `POST /api/internal/auth/token` endpoint and returns a fresh JWT.
//!   Production workers (Phase 4) use this path; the cached bundle also
//!   persists to disk via `bootstrap::save_to_disk` so a restart skips
//!   re-bootstrapping as long as the JWT is still fresh.
//!
//! `sign()` returns `anyhow::Result<String>` rather than `String` because
//! the callback path can fail (network error, server 401). Callers
//! (Downloader, LogForwarder) propagate the error via `?`.

use jsonwebtoken::{encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::bootstrap::{self, JwtBundle};

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

/// Where the signing material comes from. The two variants correspond to
/// the two provisioning paths described in the module-level doc comment.
pub enum TokenSource {
    /// Fixed secret (the legacy `WORKER_JWT_SECRET` path). New code
    /// should use `Callback` instead — Phase 4 keeps this variant for
    /// the deprecation fallback so existing deployments don't break.
    Static(Vec<u8>),
    /// Lazy callback invoked on cache miss. The callback is expected to
    /// produce a fresh `JwtBundle` from the control plane's bootstrap
    /// endpoint. Holding an `Arc<dyn Fn>` rather than the bundle itself
    /// lets the signer re-bootstrap on cache expiry without needing a
    /// `&mut self` API at the call sites.
    Callback(Arc<dyn Fn() -> anyhow::Result<JwtBundle> + Send + Sync>),
}

/// Thread-safe JWT signer with token caching.
///
/// `sign()` returns the same token for repeated calls as long as the cached
/// token's expiry is more than `REFRESH_LEAD` away. The mutex is held only
/// for the read/write of the (token, expires_at) tuple — JWT encoding
/// (potentially expensive) happens outside the lock.
pub struct WorkerJwtSigner {
    source: TokenSource,
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
    /// Legacy constructor: signs with a fixed secret loaded from the
    /// `WORKER_JWT_SECRET` env var. **Deprecated** in Phase 4 — new
    /// code should use `new_with_callback` and the bootstrap path.
    /// The `#[cfg_attr(not(test), deprecated)]` attribute keeps every
    /// existing test-only call site (5 in `auth.rs::tests` +
    /// `edge-test-helpers/src/supervisor.rs`) warning-free while still
    /// surfacing the deprecation in production builds.
    #[cfg_attr(
        not(test),
        deprecated(note = "use new_with_callback for bootstrap support")
    )]
    pub fn new(
        secret: impl Into<Vec<u8>>,
        issuer: impl Into<String>,
        worker_id: impl Into<String>,
        region: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> Arc<Self> {
        Self::from_source(
            TokenSource::Static(secret.into()),
            issuer.into(),
            worker_id.into(),
            region.into(),
            tenant_id.into(),
        )
    }

    /// Production constructor (Phase 4): the signer invokes `fetch` on
    /// every cache miss. `fetch` is expected to call
    /// `bootstrap::fetch_token` against the control plane's
    /// `POST /api/internal/auth/token` endpoint and return a fresh
    /// `JwtBundle`. The callback's `Arc` lets multiple signers (or a
    /// signer + a separate caller) share one closure without cloning
    /// the captured environment.
    pub fn new_with_callback<F>(
        issuer: impl Into<String>,
        worker_id: impl Into<String>,
        region: impl Into<String>,
        tenant_id: impl Into<String>,
        fetch: F,
    ) -> Arc<Self>
    where
        F: Fn() -> anyhow::Result<JwtBundle> + Send + Sync + 'static,
    {
        Self::from_source(
            TokenSource::Callback(Arc::new(fetch)),
            issuer.into(),
            worker_id.into(),
            region.into(),
            tenant_id.into(),
        )
    }

    fn from_source(
        source: TokenSource,
        issuer: String,
        worker_id: String,
        region: String,
        tenant_id: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            source,
            issuer,
            worker_id,
            region,
            tenant_id,
            ttl: DEFAULT_TTL,
            cache: Mutex::new(None),
        })
    }

    /// Seed the cache from a JWT bundle loaded at startup (typically
    /// from `bootstrap::load_from_disk`). The first `sign()` returns
    /// the seeded token without invoking the callback; only after
    /// expiry does the signer re-bootstrap.
    pub fn with_seeded_token(self: Arc<Self>, bundle: JwtBundle) -> Arc<Self> {
        let Some(expires_at) = bootstrap::derive_expires_at_instant(bundle.expires_at_unix) else {
            // Already expired — don't seed. The next sign() will fall
            // through to the callback path.
            return self;
        };
        // Scope the lock guard so it's dropped before we move `self`
        // back to the caller; otherwise the MutexGuard outlives the
        // Arc clone we hand back (E0505).
        {
            let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            *cache = Some(CachedToken {
                token: bundle.token,
                expires_at,
            });
        }
        self
    }

    /// Returns a current bearer token. Returns the cached value if it is
    /// still fresh; otherwise re-encodes (Static) or re-bootstraps
    /// (Callback) and updates the cache.
    ///
    /// Returns `Err` if the callback path fails (network error, server
    /// 401). Callers should propagate; the Downloader / LogForwarder
    /// path turns this into an HTTP error.
    pub fn sign(&self) -> anyhow::Result<String> {
        let now = Instant::now();

        // Fast path: cached token is still fresh (more than REFRESH_LEAD
        // before expiry). Hold the lock only for the bool check.
        // `unwrap_or_else(|e| e.into_inner())` recovers from poisoning — if a
        // previous holder panicked we still want to issue a token.
        {
            let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ct) = cache.as_ref() {
                if ct.expires_at.saturating_duration_since(now) > REFRESH_LEAD {
                    return Ok(ct.token.clone());
                }
            }
        }

        // Slow path: acquire a fresh token. Two sources:
        //   - Static:  encode with the fixed secret (no I/O).
        //   - Callback: invoke the closure, then store the bundle's
        //     token + expiry. Errors propagate.
        let (token, expires_at) = match &self.source {
            TokenSource::Static(_secret) => {
                let token = self.encode()?;
                let expires_at = now + self.ttl;
                (token, expires_at)
            }
            TokenSource::Callback(fetch) => {
                let bundle = fetch()?;
                let token: String = bundle.token;
                let expires_at = bootstrap::derive_expires_at_instant(bundle.expires_at_unix)
                    .unwrap_or(now + self.ttl);
                (token, expires_at)
            }
        };

        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        *cache = Some(CachedToken {
            token: token.clone(),
            expires_at,
        });
        drop(cache);
        Ok(token)
    }

    /// Force the next `sign()` to re-encode. Useful in tests that want to
    /// assert the cache is invalidated on expiry.
    #[cfg(test)]
    pub fn expire_cache_for_test(&self) {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        *cache = None;
    }

    fn encode(&self) -> anyhow::Result<String> {
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

        // The Static variant is the only path that uses `self.secret`
        // — extracted here so the Callback path doesn't borrow it.
        let secret = match &self.source {
            TokenSource::Static(s) => s.as_slice(),
            TokenSource::Callback(_) => {
                return Err(anyhow::anyhow!(
                    "WorkerJwtSigner::encode called on Callback-backed signer; \
                     use the callback path instead"
                ));
            }
        };

        Ok(encode(
            &Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .expect("HS256 signing should not fail"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::JwtBundle;

    #[allow(deprecated)]
    fn signer() -> Arc<WorkerJwtSigner> {
        WorkerJwtSigner::new(
            "test-secret",
            "edgecloud",
            "w_fra_abc123",
            "fra",
            "t_tenant1",
        )
    }

    #[test]
    fn sign_produces_a_token() {
        let s = signer();
        let t = s.sign().expect("sign");
        assert!(!t.is_empty());
        // A JWT has 3 dot-separated segments.
        assert_eq!(t.matches('.').count(), 2);
    }

    #[test]
    fn sign_is_deterministic_while_cached() {
        let s = signer();
        let t1 = s.sign().expect("sign1");
        let t2 = s.sign().expect("sign2");
        assert_eq!(
            t1, t2,
            "second sign() within cache window must return same token"
        );
    }

    #[test]
    fn sign_refreshes_after_cache_invalidated() {
        let s = signer();
        let t1 = s.sign().expect("sign1");
        s.expire_cache_for_test();
        let t2 = s.sign().expect("sign2");
        assert_ne!(
            t1, t2,
            "after expire_cache_for_test() the next token must be fresh"
        );
    }

    #[test]
    fn signed_token_parses_with_correct_claims() {
        let s = signer();
        let t = s.sign().expect("sign");
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

    #[test]
    fn verify_rejects_wrong_secret() {
        let s = signer();
        let t = s.sign().expect("sign");
        assert!(verify_for_test_only(b"wrong-secret", "edgecloud", &t).is_err());
    }

    /// `verify_for_test_only` pins the issuer via `Validation::set_issuer`.
    /// A token whose `iss` does not match the expected value must be
    /// rejected — this is the Rust-side mirror of the control plane's
    /// middleware check (Commit 1) and catches issuer drift in the signer.
    #[test]
    fn verify_rejects_wrong_issuer() {
        let s = signer(); // mints with iss = "edgecloud"
        let t = s.sign().expect("sign");
        let err = verify_for_test_only(b"test-secret", "some-other-issuer", &t)
            .expect_err("verify with wrong expected_iss must fail");
        assert!(
            err.to_string().to_lowercase().contains("issuer")
                || err.to_string().to_lowercase().contains("iss"),
            "error should mention issuer, got: {}",
            err
        );
    }

    // -----------------------------------------------------------------
    // Callback-path tests (Phase 4)
    // -----------------------------------------------------------------

    use std::sync::atomic::{AtomicU32, Ordering};

    fn far_future_unix() -> u64 {
        // 2099-01-01 — far enough in the future that any reasonable test
        // execution time can't push it past expiry.
        4_070_448_000
    }

    /// Builds a `new_with_callback` signer whose callback increments a
    /// shared counter on every call and returns a fixed bundle. Tests
    /// assert the counter to verify the cache short-circuited the
    /// callback.
    fn callback_signer(counter: Arc<AtomicU32>) -> Arc<WorkerJwtSigner> {
        WorkerJwtSigner::new_with_callback(
            "edgecloud",
            "w_fra_abc123",
            "fra",
            "t_tenant1",
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(JwtBundle {
                    token: format!("callback-token-{}", counter.load(Ordering::SeqCst)),
                    expires_at_unix: far_future_unix(),
                })
            },
        )
    }

    #[test]
    fn new_with_callback_uses_callback_on_first_sign() {
        let counter = Arc::new(AtomicU32::new(0));
        let s = callback_signer(counter.clone());
        let t = s.sign().expect("sign");
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(t, "callback-token-1");
    }

    #[test]
    fn new_with_callback_uses_cached_token_on_second_sign() {
        let counter = Arc::new(AtomicU32::new(0));
        let s = callback_signer(counter.clone());
        let t1 = s.sign().expect("sign1");
        let t2 = s.sign().expect("sign2");
        assert_eq!(counter.load(Ordering::SeqCst), 1, "callback must fire once");
        assert_eq!(t1, t2);
    }

    #[test]
    fn new_with_callback_refreshes_after_cache_invalidated() {
        let counter = Arc::new(AtomicU32::new(0));
        let s = callback_signer(counter.clone());
        let _ = s.sign().expect("sign1");
        s.expire_cache_for_test();
        let _ = s.sign().expect("sign2");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn new_with_callback_propagates_callback_error() {
        let s = WorkerJwtSigner::new_with_callback(
            "edgecloud",
            "w_fra_abc123",
            "fra",
            "t_tenant1",
            || Err(anyhow::anyhow!("synthetic bootstrap failure")),
        );
        let err = s.sign().expect_err("must error on callback failure");
        assert!(err.to_string().contains("synthetic bootstrap failure"));
    }

    #[test]
    fn with_seeded_token_skips_callback_on_first_sign() {
        let counter = Arc::new(AtomicU32::new(0));
        let s = callback_signer(counter.clone());
        let bundle = JwtBundle {
            token: "seeded-token".to_string(),
            expires_at_unix: far_future_unix(),
        };
        let s = s.with_seeded_token(bundle);
        let t = s.sign().expect("sign");
        assert_eq!(t, "seeded-token");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "callback must NOT fire when seeded bundle is fresh"
        );
    }

    #[test]
    fn with_seeded_token_does_not_seed_when_expired() {
        let counter = Arc::new(AtomicU32::new(0));
        let s = callback_signer(counter.clone());
        let bundle = JwtBundle {
            token: "expired-token".to_string(),
            // 1970 — long expired.
            expires_at_unix: 1_000,
        };
        let s = s.with_seeded_token(bundle);
        let t = s.sign().expect("sign");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "expired seed must not stick; callback must fire"
        );
        assert_eq!(t, "callback-token-1");
    }
}
