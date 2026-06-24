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
pub struct WorkerJwtSigner {
    secret: Vec<u8>,
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
        issuer: impl Into<String>,
        worker_id: impl Into<String>,
        region: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            secret: secret.into(),
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

        encode(
            &Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(&self.secret),
        )
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
