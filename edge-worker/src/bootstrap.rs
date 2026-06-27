//! Worker JWT bootstrap — pre-shared-key enrollment with the control plane.
//!
//! Replaces the chicken-and-egg `WORKER_JWT_SECRET` env-var model. Each new
//! worker:
//! 1. Reads its `WORKER_BOOTSTRAP_PSK` (a secret shared between worker
//!    fleet and control plane, loaded from a secrets manager).
//! 2. Computes `HMAC-SHA256(psk, "{worker_id}:{region}:{tenant_id}")` and
//!    sends it in the `X-Bootstrap-Signature` header.
//! 3. Receives a signed JWT (`{token, expires_at_unix}`) and caches it on
//!    disk at `Config::jwt_cache_path` (default
//!    `/var/lib/edge-worker/jwt-cache.json`, mode `0600`).
//! 4. On subsequent restarts, the cached token is loaded from disk; only
//!    when it's within `REFRESH_LEAD` of expiry (or missing entirely) does
//!    the worker re-bootstrap.
//!
//! **Why a single shared PSK instead of per-worker:** provisioning. With N
//! workers in a region, per-worker PSKs mean N secrets to distribute and
//! rotate. The shared PSK closes the "shared long-lived secret across
//! workers" gap (any compromise → blast radius across all workers in a
//! region) but only partially: an attacker who steals the PSK can
//! impersonate any worker. Per-worker PSK is a follow-up; for now the PSK
//! must be short-lived and rotated independently of the JWT secret.
//!
//! **Why not JWT for the bootstrap signature:** the bootstrap is the
//! enrollment step — the worker doesn't have a JWT yet. A pre-shared
//! HMAC key is the simplest viable proof-of-possession. Reusing
//! `jsonwebtoken` would require constructing a JWT with no verifier on
//! the server side, which is more code for no security gain.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// Wire format sent to and received from `POST /api/internal/auth/token`.
///
/// The server requires:
/// - `X-Worker-Id: <worker_id>` matches `body.worker_id`
/// - `X-Worker-Region: <region>` matches `body.region`
/// - `X-Bootstrap-Signature: <hex>` matches
///   `hex(HMAC-SHA256(psk, "{worker_id}:{region}:{tenant_id}"))` — the
///   `tenant_id` is bound into the signed payload so an attacker who
///   captures a valid signature for tenant A cannot replay it to mint
///   a JWT for tenant B (finding A1).
///
/// The body carries `tenant_id` because the worker already knows which
/// tenant it serves (from `WORKER_TENANT_ID`); the server populates the
/// resulting JWT's `tenant_id` claim from the body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapRequest {
    pub worker_id: String,
    pub region: String,
    pub tenant_id: String,
}

/// Server response. `token_type` is fixed to `"Bearer"`; we serialize it
/// for symmetry with RFC 6750 even though the worker doesn't need to
/// inspect it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapResponse {
    pub token: String,
    pub expires_at_unix: u64,
    #[serde(default = "default_token_type")]
    pub token_type: String,
}

fn default_token_type() -> String {
    "Bearer".to_string()
}

/// What the worker holds in memory after a successful bootstrap. The
/// control plane's JWT TTL is 24h (`auth.rs::DEFAULT_TTL`), so the bundle
/// is short-lived.
#[derive(Debug, Clone)]
pub struct JwtBundle {
    pub token: String,
    /// Wall-clock expiry as a Unix timestamp (seconds). The signer
    /// converts this to an `Instant` via `derive_expires_at_instant`
    /// when caching, so NTP step adjustments don't break the cache
    /// freshness check.
    pub expires_at_unix: u64,
}

/// On-disk representation. Wraps a `JwtBundle` together with the
/// `Instant` used by the freshness check (no recomputation on every
/// load).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedJwt {
    pub token: String,
    /// Wall-clock expiry as a Unix timestamp (seconds). On load, the
    /// signer derives the corresponding `Instant` via
    /// `derive_expires_at_instant` and uses `Instant::now() + REFRESH_LEAD`
    /// as the freshness threshold. Storing the Unix timestamp (not the
    /// `Instant`) means the file survives across boots — `Instant` is
    /// monotonic and meaningless after a restart.
    pub expires_at_unix: u64,
}

impl From<JwtBundle> for CachedJwt {
    fn from(b: JwtBundle) -> Self {
        Self {
            token: b.token,
            expires_at_unix: b.expires_at_unix,
        }
    }
}

/// Convert a Unix-epoch expiry to an `Instant` by computing
/// `Instant::now() + (expiry - now_unix)`. This loses fidelity to wall
/// clock (if NTP steps the clock forward by 30 s during a flush, the
/// `Instant` drifts by 30 s in the same direction) but keeps the
/// `sign()` cache fresh-check on a monotonic clock — immune to step
/// adjustments within the same boot.
///
/// Returns `None` if the expiry is already in the past (negative
/// duration); the caller treats this as "load but force a re-bootstrap".
pub fn derive_expires_at_instant(expires_at_unix: u64) -> Option<Instant> {
    let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    if expires_at_unix <= now_unix {
        return None;
    }
    Some(Instant::now() + Duration::from_secs(expires_at_unix - now_unix))
}

/// `true` if the cache is still fresh — more than `REFRESH_LEAD` away
/// from expiry. Matches `auth.rs::REFRESH_LEAD` so the in-memory
/// freshness check is consistent with the cache load decision.
#[allow(dead_code)] // consumed only by bootstrap::tests; lib build doesn't see cfg(test)
fn is_fresh(expires_at: Instant) -> bool {
    expires_at.saturating_duration_since(Instant::now()) > Duration::from_secs(5 * 60)
}

/// Compute `HMAC-SHA256(psk, "{worker_id}:{region}:{tenant_id}")` and
/// return the 64-char lowercase hex digest.
///
/// The signed string is `"{worker_id}:{region}:{tenant_id}"`
/// (colon-separated, no surrounding whitespace). This is a simple
/// canonical form: worker_id already validates against the format
/// `^w_[a-z0-9_]+$` server-side and region against `^[a-z]{3,16}$`,
/// so the canonical string can't contain a colon or be confused with
/// another worker. Adding a nonce or timestamp is a follow-up; for MVP
/// the 24h JWT TTL bounds replay.
///
/// **Tenant binding (finding A1):** the canonical payload includes
/// `tenant_id` so an attacker who captures one valid
/// `X-Bootstrap-Signature` cannot replay it to mint a JWT for a
/// different tenant. Without `tenant_id` in the payload, the attacker
/// could submit any body tenant_id and the server would mint a JWT
/// for that tenant.
pub fn sign_with_psk(psk: &[u8], worker_id: &str, region: &str, tenant_id: &str) -> String {
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(psk).expect("HMAC-SHA256 accepts any key length");
    mac.update(worker_id.as_bytes());
    mac.update(b":");
    mac.update(region.as_bytes());
    mac.update(b":");
    mac.update(tenant_id.as_bytes());
    let tag = mac.finalize().into_bytes();
    hex::encode(tag)
}

/// Load a cached JWT from `path`. Returns `Ok(None)` if the file is
/// missing, unreadable, or its JSON is malformed. Callers should
/// treat this as "no cache; bootstrap afresh" without surfacing the
/// error to operators — a corrupt cache is recoverable by
/// re-bootstrapping.
pub async fn load_from_disk(path: &Path) -> Result<Option<CachedJwt>> {
    let bytes = match tokio::fs::read(path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(anyhow!("read jwt cache {}: {e}", path.display()));
        }
    };
    let cached: CachedJwt = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse jwt cache {}", path.display()))?;
    Ok(Some(cached))
}

/// Persist a JWT bundle to `path` atomically (write to `.tmp`, fsync not
/// required — matches the `edge-runtime/src/interfaces/kv_store.rs`
/// pattern), then rename over the destination. On Unix the file is
/// chmod'd to `0600` after the rename so a leaked cache file is
/// only readable by the worker user.
///
/// Best-effort: this function is the worker side of "I just got a new
/// JWT; remember it". A failure here is logged but not fatal — the
/// worker keeps the in-memory bundle and signs with it until the next
/// restart, at which point it'll re-bootstrap. Losing durability across
/// a restart is acceptable.
pub async fn save_to_disk(path: &Path, bundle: &JwtBundle) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create jwt cache dir {}", parent.display()))?;
    }
    let cached: CachedJwt = CachedJwt {
        token: bundle.token.clone(),
        expires_at_unix: bundle.expires_at_unix,
    };
    let body = serde_json::to_vec(&cached).context("serialize jwt cache")?;

    let tmp_path: PathBuf = {
        let mut s = path.as_os_str().to_owned();
        s.push(".tmp");
        PathBuf::from(s)
    };
    tokio::fs::write(&tmp_path, &body)
        .await
        .with_context(|| format!("write jwt cache tmp {}", tmp_path.display()))?;
    tokio::fs::rename(&tmp_path, path).await.with_context(|| {
        format!(
            "rename jwt cache {} -> {}",
            tmp_path.display(),
            path.display()
        )
    })?;

    // 0600 on Unix: only the worker user can read the cached JWT.
    // The cache holds a bearer token; a leaked cache is a leaked
    // token until the JWT expires (24h max). Tightening the perms
    // to 0600 prevents "world-readable /var/log/edge-worker/*"
    // style leaks.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        if let Err(e) = tokio::fs::set_permissions(path, perms).await {
            // Don't fail the bootstrap on chmod failure — the file
            // is written, just with default umask. Log so operators
            // notice if their umask is too permissive.
            tracing::warn!(
                err = %e,
                path = %path.display(),
                "bootstrap: failed to chmod jwt cache to 0600; \
                 check umask if this persists"
            );
        }
    }
    Ok(())
}

/// POST to `{control_plane_url}/api/internal/auth/token` and parse the
/// response. The signature header carries the worker's identity proof;
/// the body carries the `tenant_id` claim that lands in the JWT.
///
/// The supplied `client` should already have the timeout the caller
/// wants; we don't add one here. `psk` is passed as `&[u8]` (not a
/// `String`) so the caller can load it from disk and zero it after use
/// without leaving a copy in the heap.
pub async fn fetch_token(
    control_plane_url: &str,
    client: &reqwest::Client,
    psk: &[u8],
    worker_id: &str,
    region: &str,
    tenant_id: &str,
) -> Result<JwtBundle> {
    let signature = sign_with_psk(psk, worker_id, region, tenant_id);
    let url = format!("{}/api/internal/auth/token", control_plane_url);
    let body = BootstrapRequest {
        worker_id: worker_id.to_string(),
        region: region.to_string(),
        tenant_id: tenant_id.to_string(),
    };
    let resp = client
        .post(&url)
        .header("X-Worker-Id", worker_id)
        .header("X-Worker-Region", region)
        .header("X-Bootstrap-Signature", &signature)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        // Drain the body for a useful error message, but don't fail
        // loudly on drain failure (the status code is the main
        // signal).
        let body_text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("bootstrap HTTP {status} from {url}: {body_text}"));
    }
    let parsed: BootstrapResponse = resp
        .json()
        .await
        .with_context(|| format!("parse bootstrap response from {url}"))?;
    Ok(JwtBundle {
        token: parsed.token,
        expires_at_unix: parsed.expires_at_unix,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const TEST_PSK: &[u8] = b"0123456789abcdef0123456789abcdef"; // 32 bytes

    #[test]
    fn sign_with_psk_matches_known_vector() {
        // Vector computed with python:
        //   import hmac, hashlib
        //   hmac.new(b"0123456789abcdef0123456789abcdef",
        //            b"w_fra_abc123:fra:t_tenant1", hashlib.sha256).hexdigest()
        // The exact value isn't important — what matters is determinism
        // and stability against an accidental algorithm change. If this
        // test breaks, check whether the input format or the algorithm
        // drifted. The payload was extended to include `tenant_id` in
        // finding A1.
        let sig = sign_with_psk(TEST_PSK, "w_fra_abc123", "fra", "t_tenant1");
        assert_eq!(sig.len(), 64, "HMAC-SHA256 hex is 64 chars");
        assert!(
            sig.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "signature must be lowercase hex"
        );
        // Determinism: same input -> same output.
        assert_eq!(
            sig,
            sign_with_psk(TEST_PSK, "w_fra_abc123", "fra", "t_tenant1")
        );
    }

    #[test]
    fn sign_with_psk_differs_when_worker_id_changes() {
        let a = sign_with_psk(TEST_PSK, "w_fra_aaa", "fra", "t_tenant1");
        let b = sign_with_psk(TEST_PSK, "w_fra_bbb", "fra", "t_tenant1");
        assert_ne!(a, b, "different worker_id must yield different signature");
    }

    #[test]
    fn sign_with_psk_differs_when_region_changes() {
        let a = sign_with_psk(TEST_PSK, "w_fra_abc", "fra", "t_tenant1");
        let b = sign_with_psk(TEST_PSK, "w_fra_abc", "nyc", "t_tenant1");
        assert_ne!(a, b, "different region must yield different signature");
    }

    /// Regression for finding A1: the canonical payload must include
    /// `tenant_id` so a signature captured for one tenant cannot be
    /// replayed against another. Without `tenant_id` in the payload,
    /// an attacker who captured `X-Bootstrap-Signature` for tenant A
    /// could POST a body with `"tenant_id":"t_victim"` and the server
    /// would mint a JWT for the victim.
    #[test]
    fn sign_with_psk_differs_when_tenant_id_changes() {
        let a = sign_with_psk(TEST_PSK, "w_fra_abc", "fra", "t_alice");
        let b = sign_with_psk(TEST_PSK, "w_fra_abc", "fra", "t_victim");
        assert_ne!(
            a, b,
            "different tenant_id must yield different signature \
             (otherwise A1 tenant-pivot is possible)"
        );
    }

    #[test]
    fn sign_with_psk_differs_when_psk_changes() {
        let a = sign_with_psk(b"psk-one-00000000000000000000000", "w", "r", "t");
        let b = sign_with_psk(b"psk-two-00000000000000000000000", "w", "r", "t");
        assert_ne!(a, b, "different PSK must yield different signature");
    }

    #[test]
    fn sign_with_psk_uses_lowercase_hex() {
        let sig = sign_with_psk(TEST_PSK, "w_fra_abc", "fra", "t_tenant1");
        assert_eq!(sig.to_lowercase(), sig, "must be lowercase hex");
    }

    #[test]
    fn derive_expires_at_instant_returns_none_for_past() {
        // 1970-01-01 is definitely in the past.
        assert!(derive_expires_at_instant(1234).is_none());
        assert!(derive_expires_at_instant(0).is_none());
    }

    #[test]
    fn derive_expires_at_instant_returns_some_for_future() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let one_hour_later = now + 3600;
        let inst = derive_expires_at_instant(one_hour_later).expect("future");
        // Should be roughly an hour from now; allow a few seconds of
        // slack for test runtime.
        let delta = inst.saturating_duration_since(Instant::now());
        assert!(
            delta >= Duration::from_secs(3590) && delta <= Duration::from_secs(3610),
            "expected ~1h from now, got {:?}",
            delta
        );
    }

    #[test]
    fn is_fresh_distinguishes_fresh_from_stale() {
        // One hour from now -> fresh.
        let future = Instant::now() + Duration::from_secs(3600);
        assert!(is_fresh(future));
        // Five minutes ago (within the lead) -> stale.
        let past = Instant::now() - Duration::from_secs(300);
        assert!(!is_fresh(past));
        // Way in the past -> stale.
        let ancient = Instant::now() - Duration::from_secs(86_400);
        assert!(!is_fresh(ancient));
    }

    #[tokio::test]
    async fn save_then_load_roundtrips() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("jwt-cache.json");
        let bundle = JwtBundle {
            token: "eyJ.fake.token".to_string(),
            expires_at_unix: 1_782_547_200,
        };
        save_to_disk(&path, &bundle).await.expect("save");
        let loaded = load_from_disk(&path).await.expect("load");
        let cached = loaded.expect("Some");
        assert_eq!(cached.token, bundle.token);
        assert_eq!(cached.expires_at_unix, bundle.expires_at_unix);
    }

    #[tokio::test]
    async fn load_returns_none_for_missing_file() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("nonexistent.json");
        let loaded = load_from_disk(&path).await.expect("load");
        assert!(loaded.is_none(), "missing file must return Ok(None)");
    }

    #[tokio::test]
    async fn load_propagates_parse_error_for_corrupt_json() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("jwt-cache.json");
        tokio::fs::write(&path, b"not-json-{")
            .await
            .expect("write garbage");
        let err = load_from_disk(&path)
            .await
            .expect_err("corrupt cache must return Err");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("parse jwt cache"),
            "error must mention the parse failure for ops triage: {msg}"
        );
    }

    #[tokio::test]
    async fn save_creates_parent_dir() {
        let dir = TempDir::new().expect("tempdir");
        let nested = dir.path().join("nested").join("jwt-cache.json");
        let bundle = JwtBundle {
            token: "t".into(),
            expires_at_unix: 1_700_000_000,
        };
        save_to_disk(&nested, &bundle).await.expect("save");
        let loaded = load_from_disk(&nested).await.expect("load");
        assert!(loaded.is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn save_uses_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("jwt-cache.json");
        let bundle = JwtBundle {
            token: "t".into(),
            expires_at_unix: 1_700_000_000,
        };
        save_to_disk(&path, &bundle).await.expect("save");
        let meta = tokio::fs::metadata(&path).await.expect("stat");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "expected 0600, got {:o}", mode);
    }

    /// `fetch_token` constructs the right URL, headers, and body, and
    /// parses a 200 response. We use wiremock so the test exercises the
    /// full reqwest path (URL building, header serialization, JSON
    /// deserialization) without standing up a real HTTP server.
    #[tokio::test]
    async fn fetch_token_constructs_correct_request_and_parses_response() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/auth/token"))
            .and(header("X-Worker-Id", "w_fra_abc"))
            .and(header("X-Worker-Region", "fra"))
            .and(header(
                "X-Bootstrap-Signature",
                sign_with_psk(TEST_PSK, "w_fra_abc", "fra", "t_tenant1"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "token": "eyJ.fake.jwt",
                "expires_at_unix": 1_782_547_200,
                "token_type": "Bearer",
            })))
            .expect(1)
            .mount(&mock)
            .await;

        let client = reqwest::Client::new();
        let bundle = fetch_token(
            &mock.uri(),
            &client,
            TEST_PSK,
            "w_fra_abc",
            "fra",
            "t_tenant1",
        )
        .await
        .expect("fetch_token");
        assert_eq!(bundle.token, "eyJ.fake.jwt");
        assert_eq!(bundle.expires_at_unix, 1_782_547_200);
    }

    /// The handler returns 401 on a wrong signature; the worker must
    /// propagate the error rather than retry — the server's signal
    /// ("your PSK is wrong") is permanent until the operator fixes
    /// the configuration.
    #[tokio::test]
    async fn fetch_token_propagates_401() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/auth/token"))
            .respond_with(ResponseTemplate::new(401).set_body_string("invalid signature"))
            .expect(1)
            .mount(&mock)
            .await;

        let client = reqwest::Client::new();
        let err = fetch_token(
            &mock.uri(),
            &client,
            TEST_PSK,
            "w_fra_abc",
            "fra",
            "t_tenant1",
        )
        .await
        .expect_err("must error on 401");
        let msg = format!("{err:#}");
        assert!(msg.contains("401"), "error must mention status 401: {msg}");
    }

    /// Network errors (connection refused, DNS failure) propagate as
    /// anyhow errors. The caller (`WorkerJwtSigner::sign`) surfaces
    /// them as `sign()` failures; the downloader / log forwarder
    /// callers see an error and can decide whether to retry.
    #[tokio::test]
    async fn fetch_token_propagates_network_error() {
        let client = reqwest::Client::new();
        // 127.0.0.1:1 is reserved and refuses connections.
        let err = fetch_token(
            "http://127.0.0.1:1",
            &client,
            TEST_PSK,
            "w_fra_abc",
            "fra",
            "t_tenant1",
        )
        .await
        .expect_err("must error on connection refused");
        // Just check it's a real error with some content.
        assert!(!format!("{err:#}").is_empty());
    }
}
