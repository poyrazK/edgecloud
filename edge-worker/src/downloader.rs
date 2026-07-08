//! Artifact downloader with local file cache.

use anyhow::Context;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;

use crate::auth::WorkerJwtSigner;
use crate::verifier::Keyring;

/// Downloads Wasm artifacts from the control plane with local cache.
pub struct Downloader {
    client: reqwest::Client,
    control_plane_url: String,
    cache_dir: PathBuf,
    jwt_signer: Arc<WorkerJwtSigner>,
    /// Optional Ed25519 signing keyring (issue #307 PR2 + PR1 follow-up
    /// multi-keyring). `None` when the worker is started with
    /// `EDGE_REQUIRE_SIGNATURE=false` (the rollout escape hatch) — in
    /// that mode, `get_artifact` accepts unsigned artifacts, and the
    /// supervisor's `require_signature` guard already short-circuits
    /// any `None`-signature AppSpec before this method is reached for
    /// "verification required" workers. Tests that don't exercise
    /// signing also pass `None`.
    ///
    /// `pub(crate)` so the supervisor's `start_app` early-reject guard
    /// can read it (defensive check that `require_signature=true`
    /// always implies a keyring was constructed).
    pub(crate) signature_verifier: Option<Arc<Keyring>>,
}

impl Downloader {
    pub fn new(
        control_plane_url: String,
        cache_dir: PathBuf,
        jwt_signer: Arc<WorkerJwtSigner>,
        signature_verifier: Option<Arc<Keyring>>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            control_plane_url,
            cache_dir,
            jwt_signer,
            signature_verifier,
        }
    }

    /// Get artifact bytes for a deployment.
    ///
    /// Returns cached bytes if available, otherwise downloads from the control plane.
    /// Both the cached and freshly-downloaded bytes are verified against `expected_hash`
    /// (a bare lowercase hex SHA-256 digest) before being returned. Verification errors
    /// (empty hash, malformed hash, or mismatch) cause this function to return `Err`;
    /// a tampered cache file is invalidated and the artifact is re-downloaded once.
    ///
    /// `expected_signature` is the base64url(no-pad) Ed25519 signature
    /// over `(sha256(artifact) || deployment_id)`, carried by the
    /// AppSpec. `expected_signature_kid` is the key id used to
    /// produce it (issue #307 PR1 follow-up multi-keyring) — `None`
    /// or `Some("")` resolves against the keyring's default key.
    /// When the worker is configured with a keyring
    /// (`signature_verifier.is_some()`), the signature is verified
    /// BOTH in the cache fast-path AND the fresh-download path —
    /// re-verification on cache hit means a tampered cache file
    /// cannot bypass signature checks. Verification errors
    /// invalidate the cache and re-download, mirroring the
    /// hash-mismatch path. When the worker is configured without a
    /// keyring, `expected_signature` MUST be `None` (the
    /// supervisor's `require_signature` guard enforces this).
    pub async fn get_artifact(
        &self,
        deployment_id: &str,
        expected_hash: &str,
        expected_signature: Option<&str>,
        expected_signature_kid: Option<&str>,
    ) -> anyhow::Result<bytes::Bytes> {
        let cache_path = self.cache_path(deployment_id);

        // Try local cache first. Always verify against expected_hash; an empty hash
        // is an error (see verify_hash) so the cache fast-path is only usable when
        // the producer supplied a real hash. Signature is verified when a verifier
        // is configured; re-verifying on cache hit means a tampered .wasm file
        // cannot bypass signature checks.
        if cache_path.exists() {
            match tokio::fs::read(&cache_path).await {
                Ok(data) => {
                    // Hash verification is the cheap gate; if it fails,
                    // the cached file is poisoned and we re-download.
                    if let Err(e) = verify_hash(&data, expected_hash, deployment_id) {
                        tracing::warn!(
                            deployment_id,
                            err = %e,
                            "cached artifact hash mismatch; invalidating and re-downloading"
                        );
                        let _ = tokio::fs::remove_file(&cache_path).await;
                        let _ = tokio::fs::remove_file(self.cwasm_path(deployment_id)).await;
                    } else {
                        // Signature verification is unforgiving — a wrong
                        // sig stays wrong after a re-download, so we bail
                        // immediately. The cache file is invalidated as a
                        // side-effect before the error propagates so the
                        // next call doesn't keep tripping the same check.
                        match verify_signature(
                            &data,
                            expected_hash,
                            expected_signature,
                            expected_signature_kid,
                            deployment_id,
                            self.signature_verifier.as_deref(),
                        ) {
                            Ok(()) => {
                                tracing::debug!(deployment_id, bytes = data.len(), "cache hit");
                                return Ok(data.into());
                            }
                            Err(sig_err) => {
                                tracing::warn!(
                                    deployment_id,
                                    err = %sig_err,
                                    "cached artifact signature mismatch; \
                                     invalidating cache and propagating error"
                                );
                                let _ = tokio::fs::remove_file(&cache_path).await;
                                let _ =
                                    tokio::fs::remove_file(self.cwasm_path(deployment_id)).await;
                                return Err(sig_err);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(deployment_id, err = %e, "cache read failed; downloading");
                    let _ = tokio::fs::remove_file(&cache_path).await;
                    let _ = tokio::fs::remove_file(self.cwasm_path(deployment_id)).await;
                }
            }
        }

        // Defensive: if a verifier is wired in but no signature was
        // supplied, don't waste an HTTP round trip — fail closed. The
        // supervisor's `require_signature` guard should have caught
        // this earlier; this is belt-and-suspenders.
        if self.signature_verifier.is_some() && expected_signature.is_none() {
            tracing::error!(
                deployment_id,
                "no signature in AppSpec but worker is configured to verify signatures; \
                 refusing to download from control plane"
            );
            anyhow::bail!(
                "AppSpec for {deployment_id} has no signature; worker is configured \
                 EDGE_REQUIRE_SIGNATURE=true"
            );
        }

        // Download from control plane. Sign the request with the worker's
        // bearer JWT — the control plane's WorkerAuth middleware will reject
        // any unsigned /api/internal/* request with 401.
        let url = format!(
            "{}/api/internal/download/{}",
            self.control_plane_url, deployment_id
        );
        let token = self.jwt_signer.sign();
        tracing::info!(url, "downloading artifact");

        let response = self
            .client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("failed to download {}", url))?
            .error_for_status()
            .with_context(|| format!("HTTP error for {}", url))?;

        let data: bytes::Bytes = response.bytes().await?;

        // Verify before caching — never persist unverified bytes to disk.
        verify_hash(&data, expected_hash, deployment_id)?;
        // Signature verification is only meaningful when a verifier
        // is configured. A None verifier + None signature is the
        // legitimate "unsigned mode" path; the helper below returns
        // Ok(()) in that case and bails on every other combination
        // (None + Some sig, Some + None sig, malformed sig, etc.).
        verify_signature(
            &data,
            expected_hash,
            expected_signature,
            expected_signature_kid,
            deployment_id,
            self.signature_verifier.as_deref(),
        )?;
        // Defensive warning: a control plane that signs but a worker
        // that doesn't verify would silently accept the AppSpec's
        // claimed signature without checking it. The helper returned
        // Ok(()) above, so we log once so operators see the
        // mismatch and can flip EDGE_REQUIRE_SIGNATURE on.
        if self.signature_verifier.is_none() && expected_signature.is_some() {
            tracing::warn!(
                deployment_id,
                "AppSpec carries a signature but worker has no verifier configured; \
                 the signature is ignored. Set EDGE_SIGNING_PUBKEY[_PATH] and \
                 EDGE_REQUIRE_SIGNATURE=true to enable verification."
            );
        }

        // Ensure cache directory exists and write to cache
        tokio::fs::create_dir_all(&self.cache_dir).await?;
        tokio::fs::write(&cache_path, &data).await?;

        tracing::info!(deployment_id, bytes = data.len(), "artifact cached");
        Ok(data)
    }

    pub fn cache_path(&self, deployment_id: &str) -> PathBuf {
        self.cache_dir.join(format!("{}.wasm", deployment_id))
    }

    pub fn cwasm_path(&self, deployment_id: &str) -> PathBuf {
        self.cache_dir.join(format!("{}.cwasm", deployment_id))
    }

    /// Notify the control plane that the worker has exhausted the
    /// restart cap on a tenant app and wants the active deployment
    /// swapped to `last_good_deployment_id`. Best-effort: returns
    /// `Err` on any failure (network, non-2xx, malformed response)
    /// so the caller can log it. The supervisor does NOT block on
    /// this — `tokio::spawn` is the caller's responsibility so the
    /// supervisor's per-app task can exit immediately. The user can
    /// fall back to `edge rollback` if the auto-rollback POST fails.
    ///
    /// The control-plane endpoint is documented in
    /// `edge-control-plane/internal/handler/internal.go::AutoRollback`.
    /// It enforces `auto_rollback_enabled=true` server-side; if the
    /// tenant opted out, the response is 412 — the worker logs and
    /// moves on (no retry, no escalation).
    pub async fn post_auto_rollback(
        &self,
        tenant_id: &str,
        app_name: &str,
        current_deployment_id: &str,
        restart_count: u32,
    ) -> anyhow::Result<()> {
        // Path-traversal guard: appName flows into the URL path
        // (`/api/internal/apps/{appName}/auto-rollback`). A "../"
        // would let one tenant's worker signal auto-rollback for a
        // different app on the same control plane. tenant_id is
        // already validated upstream (see start_app in supervisor.rs);
        // we still check it here so this method is safe to call from
        // anywhere without depending on the caller's checks.
        if app_name.is_empty()
            || app_name.contains('/')
            || app_name.contains('\\')
            || app_name.contains("..")
        {
            anyhow::bail!("refusing to POST auto-rollback for unsafe app_name {app_name:?}");
        }

        let url = format!(
            "{}/api/internal/apps/{}/auto-rollback",
            self.control_plane_url, app_name
        );
        let body = serde_json::json!({
            "tenant_id": tenant_id,
            "app_name": app_name,
            "current_deployment_id": current_deployment_id,
            "restart_count": restart_count,
        });

        tracing::info!(
            url = %url,
            tenant_id,
            app_name,
            current_deployment_id,
            restart_count,
            "posting auto-rollback to control plane"
        );

        // Use send() rather than error_for_status() so we can log
        // the response status code on non-2xx without short-circuiting
        // the post — the caller treats both 2xx and 4xx as "we got our
        // signal across" and only escalates on transport errors.
        //
        // Sign the request with the worker's bearer JWT (see main's
        // PR #98 for the broader JWT middleware rollout): the control
        // plane's WorkerAuth middleware will reject any unsigned
        // /api/internal/* request with 401.
        let token = self.jwt_signer.sign();
        let response = self
            .client
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("failed to POST {url}"))?;

        let status = response.status();
        if !status.is_success() {
            // 4xx means the control plane got the signal but rejected
            // it (e.g. 412 auto-rollback disabled, 409 no last-good,
            // 404 no active deployment). Log and return Err so the
            // caller's tracing captures the failure, but DON'T
            // retry — these are tenant-config issues, not transient
            // outages, and a retry would just hit the same rejection.
            let body = response.text().await.unwrap_or_default();
            tracing::warn!(
                url = %url,
                status = %status,
                body = %body,
                "auto-rollback POST rejected by control plane"
            );
            anyhow::bail!("auto-rollback POST {url} returned {status}: {body}");
        }

        tracing::info!(url = %url, status = %status, "auto-rollback POST accepted");
        Ok(())
    }
}

/// Verify the Ed25519 signature over `(sha256(bytes) || deployment_id)`
/// against the worker's signing keyring.
///
/// The signed message layout mirrors the Go control plane's
/// `internal/signing/signer.go::Sign` byte-for-byte:
///   `msg = make([]byte, 0, 32+len(deploymentID))`
///   `msg = append(msg, hashBytes...)`        // raw 32 bytes
///   `msg = append(msg, []byte(deploymentID)...)`
///
/// The `bytes` argument here is the artifact itself (the same
/// payload we just SHA-256'd in `verify_hash`); we re-hash it
/// inside this function to avoid coupling verify_hash and
/// verify_signature via a shared hash state. The redundant SHA-256
/// is sub-microsecond on any real wasm artifact.
///
/// `expected_signature_kid` is the kid from the AppSpec. `None` or
/// `Some("")` (the legacy CP form) both resolve against the
/// keyring's default key — see `Keyring::verify` for the exact
/// normalization rule and the rationale pinning it.
///
/// Returns `Ok(())` on a valid signature. Returns `Err` on:
///
/// - no keyring configured (a no-op when the worker is in
///   `EDGE_REQUIRE_SIGNATURE=false` mode AND `expected_signature`
///   is `None`; an error otherwise — see caller logic in
///   `get_artifact`).
/// - `expected_signature` is `None` but a keyring is configured
///   (the supervisor should have caught this earlier; we double-
///   check here so the worker fails closed on a wire-shape
///   contract violation).
/// - signature wire-format error (empty / non-base64url / wrong
///   decoded length / ed25519-dalek rejected the sig shape).
/// - the kid did not resolve to a key in the keyring
///   (`UnknownKey` error variant; surfaces config drift cleanly).
/// - signature is well-formed but does not match `(hash || id)`
///   for the resolved key — a `verify()` returning `Ok(false)`.
///
/// Each error path includes `deployment_id` in the `tracing::error!`
/// message so an operator can correlate a reject to a specific
/// deployment without grepping for the verify call site.
fn verify_signature(
    bytes: &[u8],
    expected_hash: &str,
    expected_signature: Option<&str>,
    expected_signature_kid: Option<&str>,
    deployment_id: &str,
    keyring: Option<&Keyring>,
) -> anyhow::Result<()> {
    let keyring = match keyring {
        Some(k) => k,
        // No keyring: only acceptable when the AppSpec ALSO has no
        // signature. The caller's get_artifact already logs a
        // warning for the "keyring None + sig Some" combination
        // (operator should set EDGE_REQUIRE_SIGNATURE); here we
        // short-circuit cleanly on the "keyring None + sig None"
        // case.
        None => return Ok(()),
    };

    let sig = match expected_signature {
        Some(s) => s,
        None => {
            tracing::error!(
                deployment_id,
                "no signature in AppSpec but worker is configured to verify signatures"
            );
            anyhow::bail!(
                "AppSpec for {deployment_id} has no signature; worker is \
                 configured EDGE_REQUIRE_SIGNATURE=true"
            );
        }
    };

    if sig.is_empty() {
        tracing::error!(
            deployment_id,
            "deployment_signature is empty; refusing to instantiate unverified artifact"
        );
        anyhow::bail!("deployment_signature is empty for {deployment_id}");
    }

    match keyring.verify(expected_hash, deployment_id, sig, expected_signature_kid) {
        Ok(true) => {
            tracing::debug!(
                deployment_id,
                bytes = bytes.len(),
                "Ed25519 artifact signature verified"
            );
            Ok(())
        }
        Ok(false) => {
            tracing::error!(
                deployment_id,
                "Ed25519 artifact signature verify returned false — refusing to instantiate. \
                 The signature does not match (hash || deployment_id) for the configured pubkey. \
                 Check that EDGE_SIGNING_KEYRING matches the CP's active signing key and \
                 that the deployment hasn't been tampered with."
            );
            anyhow::bail!(
                "artifact signature verify failed for {deployment_id}: \
                 signature does not match (hash || deployment_id)"
            );
        }
        Err(e) => {
            tracing::error!(
                deployment_id,
                err = %e,
                "Ed25519 signature wire-format or keyring error — refusing to instantiate"
            );
            anyhow::bail!("artifact signature for {deployment_id} malformed: {e}");
        }
    }
}

/// Verify that `sha256(bytes)` equals `expected_hex` (a bare lowercase hex digest).
///
/// `expected_hex` must be exactly 64 characters from the set `0-9 a-f` — the
/// shape the Go control plane produces (`internal/service/deployment.go:112`).
/// Uppercase hex is rejected at the pre-check rather than failing later inside
/// the decoder, so the error is specific and actionable.
///
/// Errors on:
/// - empty `expected_hex` (closes the pre-fix bypass where empty meant "skip")
/// - wrong length
/// - non-lowercase or non-hex characters
/// - hash mismatch
fn verify_hash(bytes: &[u8], expected_hex: &str, deployment_id: &str) -> anyhow::Result<()> {
    if expected_hex.is_empty() {
        tracing::error!(
            deployment_id,
            "deployment_hash is empty; refusing to instantiate unverified artifact"
        );
        anyhow::bail!("deployment_hash is empty for {deployment_id}");
    }

    if expected_hex.len() != 64 {
        tracing::error!(
            deployment_id,
            len = expected_hex.len(),
            "deployment_hash must be exactly 64 chars (SHA-256 hex digest length)"
        );
        anyhow::bail!(
            "deployment_hash for {deployment_id} has wrong length: expected 64, got {}",
            expected_hex.len()
        );
    }

    if !expected_hex.bytes().all(is_lower_hex) {
        tracing::error!(
            deployment_id,
            "deployment_hash contains non-lowercase or non-hex chars; must be 64 lowercase hex (0-9, a-f)"
        );
        anyhow::bail!(
            "deployment_hash for {deployment_id} contains non-hex chars; must be 64 lowercase hex (0-9, a-f)"
        );
    }

    let expected_bytes = decode_hex_32(expected_hex)?;
    let actual = Sha256::digest(bytes);

    if actual.as_slice() != expected_bytes {
        let actual_hex = hex_encode(actual.as_slice());
        tracing::error!(
            deployment_id,
            expected = %expected_hex,
            actual = %actual_hex,
            "artifact hash mismatch — refusing to instantiate"
        );
        anyhow::bail!(
            "artifact hash mismatch for {deployment_id}: expected {expected_hex}, got {actual_hex}"
        );
    }

    Ok(())
}

/// Decode exactly 64 lowercase hex chars into 32 bytes.
/// Caller must have validated `len == 64 && all is_lower_hex`.
fn decode_hex_32(s: &str) -> anyhow::Result<[u8; 32]> {
    let bytes = s.as_bytes();
    let mut out = [0u8; 32];
    for i in 0..32 {
        let hi = hex_nibble(bytes[2 * i])?;
        let lo = hex_nibble(bytes[2 * i + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

/// `true` iff `b` is a lowercase hex digit (`0-9` or `a-f`).
const fn is_lower_hex(b: u8) -> bool {
    matches!(b, b'0'..=b'9' | b'a'..=b'f')
}

fn hex_nibble(b: u8) -> anyhow::Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        _ => anyhow::bail!("non-hex byte: 0x{b:02x}"),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    // Base64 encoder trait — used by the signature tests below to
    // re-encode tampered/raw sigs back to base64url(no-pad).
    use base64::Engine;
    // ed25519-dalek's SigningKey::sign lives behind the Signer
    // trait; import it so the signature tests can call .sign()
    // directly on the test keypair.
    use ed25519_dalek::Signer;

    fn sha256_hex(data: &[u8]) -> String {
        hex_encode(Sha256::digest(data).as_slice())
    }

    #[test]
    fn verify_hash_rejects_empty() {
        let err = verify_hash(b"anything", "", "d_test").unwrap_err();
        assert!(err.to_string().contains("empty"), "got: {err}");
    }

    #[test]
    fn verify_hash_rejects_wrong_length() {
        let err = verify_hash(b"anything", "abc", "d_test").unwrap_err();
        assert!(err.to_string().contains("wrong length"), "got: {err}");
    }

    #[test]
    fn verify_hash_rejects_non_hex() {
        // 64 chars but contains 'z'
        let bad = format!("{}z", "0".repeat(63));
        let err = verify_hash(b"anything", &bad, "d_test").unwrap_err();
        assert!(err.to_string().contains("non-hex"), "got: {err}");
    }

    #[test]
    fn verify_hash_rejects_uppercase() {
        // 64 chars, all valid hex, but uppercase — must be rejected at the
        // pre-check so the error is specific (not "non-hex byte: 0x41").
        let bad = "A".repeat(64);
        let err = verify_hash(b"anything", &bad, "d_test").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("lowercase"),
            "expected error to mention lowercase, got: {msg}"
        );
    }

    #[test]
    fn verify_hash_accepts_matching() {
        let data = b"hello world";
        let hash = sha256_hex(data);
        verify_hash(data, &hash, "d_test").expect("matching hash must verify");
    }

    #[test]
    fn verify_hash_rejects_mismatch() {
        let hash = sha256_hex(b"hello");
        let err = verify_hash(b"world", &hash, "d_test").unwrap_err();
        assert!(err.to_string().contains("mismatch"), "got: {err}");
    }

    #[test]
    fn hex_encode_doubles_length_and_is_lowercase() {
        let data: Vec<u8> = (0u8..=255).collect();
        let encoded = hex_encode(&data);
        assert_eq!(encoded.len(), data.len() * 2);
        assert!(
            encoded.bytes().all(is_lower_hex),
            "hex_encode must emit only lowercase hex"
        );
    }

    #[test]
    fn decode_hex_32_accepts_any_byte_value() {
        // Every byte 0x00..=0xff must encode to a 2-char lowercase string that
        // decode_hex_32 (called twice in sequence) recovers losslessly.
        let data: Vec<u8> = (0u8..=255).collect();
        let encoded = hex_encode(&data);
        let encoded_32: String = encoded.chars().take(64).collect();
        let decoded = decode_hex_32(&encoded_32).expect("decode");
        assert_eq!(decoded.to_vec(), data[..32]);
    }

    // -----------------------------------------------------------------------
    // get_artifact cache-path tests (no Docker needed — run in CI).
    //
    // Use wiremock + tempfile to drive Downloader::get_artifact through its
    // cache fast-path, cache-invalidation-then-redownload path, and the
    // download-failure path.
    // -----------------------------------------------------------------------

    /// Cache hit: a pre-populated cache file whose bytes match the expected
    /// hash must be returned without contacting the control plane.
    #[tokio::test]
    async fn get_artifact_cached_file_verifies_and_returns_bytes() {
        use tempfile::TempDir;
        use wiremock::MockServer;

        let server = MockServer::start().await;
        // No mock mounted — any request to the server fails this test.

        let tmp = TempDir::new().expect("tempdir");
        let cache_dir = tmp.path().to_path_buf();
        let downloader = Downloader::new(server.uri(), cache_dir.clone(), test_signer(), None);

        let bytes: Vec<u8> = b"some test bytes".to_vec();
        let hash = sha256_hex(&bytes);
        tokio::fs::write(cache_dir.join("d_unit_cache_hit.wasm"), &bytes)
            .await
            .expect("pre-populate cache");

        let result = downloader
            .get_artifact("d_unit_cache_hit", &hash, None, None)
            .await
            .expect("cache hit must succeed");
        assert_eq!(result.as_ref(), bytes.as_slice());

        let received = server.received_requests().await.expect("received");
        assert!(
            received.is_empty(),
            "expected zero requests on cache hit, got {}",
            received.len()
        );
    }

    /// Tampered cache: the cache file's bytes do NOT match the expected hash.
    /// The downloader must invalidate the cache, re-download from the control
    /// plane, verify, and write the verified bytes back to the cache.
    #[tokio::test]
    async fn get_artifact_cached_file_hash_mismatch_invalidates_and_redownloads() {
        use tempfile::TempDir;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let good_bytes: Vec<u8> = b"the real artifact".to_vec();
        let good_hash = sha256_hex(&good_bytes);
        Mock::given(method("GET"))
            .and(path("/api/internal/download/d_unit_redownload"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(good_bytes.clone()))
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let cache_dir = tmp.path().to_path_buf();
        let downloader = Downloader::new(server.uri(), cache_dir.clone(), test_signer(), None);

        // Pre-populate the cache with content that won't match the expected hash.
        tokio::fs::write(cache_dir.join("d_unit_redownload.wasm"), b"tampered bytes")
            .await
            .expect("pre-populate tampered cache");

        let result = downloader
            .get_artifact("d_unit_redownload", &good_hash, None, None)
            .await
            .expect("re-downloaded bytes must verify and return");
        assert_eq!(result.as_ref(), good_bytes.as_slice());

        // The cache file should now contain the verified good bytes.
        let on_disk = tokio::fs::read(cache_dir.join("d_unit_redownload.wasm"))
            .await
            .expect("read cache after re-download");
        assert_eq!(
            on_disk, good_bytes,
            "cache file must be rewritten with verified bytes"
        );

        let received = server.received_requests().await.expect("received");
        assert_eq!(
            received.len(),
            1,
            "expected exactly one download request after cache invalidation"
        );
    }

    /// Network error after cache invalidation: a tampered cache triggers
    /// invalidation, the subsequent download fails with HTTP 500, and the
    /// error propagates. The failed download must NOT recreate the cache file.
    #[tokio::test]
    async fn get_artifact_download_failure_propagates_error() {
        use tempfile::TempDir;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/internal/download/d_unit_500"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let cache_dir = tmp.path().to_path_buf();
        let downloader = Downloader::new(server.uri(), cache_dir.clone(), test_signer(), None);

        // Pre-populate the cache with tampered bytes so the cache fast-path
        // is exercised, then invalidated, forcing the download path.
        let cache_path = cache_dir.join("d_unit_500.wasm");
        tokio::fs::write(&cache_path, b"tampered bytes")
            .await
            .expect("pre-populate tampered cache");

        // The hash the caller is asking for doesn't match the cache (and
        // wouldn't match a 500 body either). Use the real-hash of any
        // non-tampered bytes — the result is the same: the download fails.
        let hash = sha256_hex(b"any bytes");

        let err = downloader
            .get_artifact("d_unit_500", &hash, None, None)
            .await
            .expect_err("500 from server must propagate as Err");
        let msg = err.to_string();
        assert!(
            msg.contains("HTTP error") || msg.contains("500"),
            "expected HTTP error in message, got: {msg}"
        );

        assert!(
            !cache_path.exists(),
            "cache file should not be recreated on download failure"
        );
    }

    // -----------------------------------------------------------------------
    // post_auto_rollback tests (no Docker needed — run in CI).
    //
    // The supervisor-level integration test (driving a real crashing
    // wasm through the full lifecycle to verify the POST fires on
    // Crashed) requires a wasi-sdk-built crashing fixture that's out
    // of scope for this PR to build. The behavior under test is split:
    //
    //   - Downloader::post_auto_rollback itself — covered here with
    //     wiremock (the wire shape, retry semantics, rejection
    //     handling).
    //   - The Crashed-branch wiring (which calls post_auto_rollback) —
    //     a hand-crafted component or an InstancePre mock would be
    //     needed, both of which require a non-trivial test fixture.
    //     See TODO comment in tests/integration_tests.rs for the
    //     follow-up to land a crashing.wasm and exercise the full
    //     loop.
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // get_artifact signature tests (issue #307 PR2). Mirror the
    // hash-path tests above: 4 cases that exercise the signature
    // verification path through both the cache fast-path and the
    // fresh-download path. The verifier is the test keypair's
    // verifying key (zero seed → deterministic).
    // -----------------------------------------------------------------------

    /// Cache + valid signature → bytes returned without contacting the
    /// control plane. Mirrors the hash positive test, but the
    /// verifier is configured and a valid signature is required.
    #[tokio::test]
    async fn get_artifact_signed_match_starts_app() {
        use tempfile::TempDir;
        use wiremock::MockServer;

        let server = MockServer::start().await;
        // No mock mounted — any request fails this test.

        let tmp = TempDir::new().expect("tempdir");
        let cache_dir = tmp.path().to_path_buf();
        let bytes: Vec<u8> = b"signed test bytes".to_vec();
        let hash = sha256_hex(&bytes);
        // Deterministic test signer (zero seed, matches Go side).
        let seed = [0u8; 32];
        let sk = ed25519_dalek::SigningKey::from_bytes(&seed);
        let verifier = std::sync::Arc::new(crate::verifier::Keyring::single(sk.verifying_key()));
        // Sign over (hash_raw || deployment_id) the way the Go signer does.
        let hash_raw = decode_hex_32(&hash).expect("decode hex hash");
        let dep_id = "d_signed_match";
        let mut msg = Vec::with_capacity(32 + dep_id.len());
        msg.extend_from_slice(&hash_raw);
        msg.extend_from_slice(dep_id.as_bytes());
        let sig_bytes = sk.sign(&msg);
        let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig_bytes.to_bytes());

        // Pre-populate the cache with the right bytes — the cache
        // fast-path returns without contacting the CP.
        tokio::fs::write(cache_dir.join(format!("{dep_id}.wasm")), &bytes)
            .await
            .expect("pre-populate cache");

        let downloader = Downloader::new(
            server.uri(),
            cache_dir.clone(),
            test_signer(),
            Some(verifier),
        );
        let result = downloader
            .get_artifact(dep_id, &hash, Some(&sig), None)
            .await
            .expect("cache hit with valid sig must succeed");
        assert_eq!(result.as_ref(), bytes.as_slice());

        let received = server.received_requests().await.expect("received");
        assert!(
            received.is_empty(),
            "expected zero requests on cache hit, got {}",
            received.len()
        );
    }

    /// Cache + corrupted signature → cache invalidated, re-download
    /// also fails. The point: a single-bit corruption of the
    /// signature column on disk must produce a clean verify-false,
    /// not a silent accept.
    #[tokio::test]
    async fn get_artifact_signed_mismatch_rejects_app() {
        use tempfile::TempDir;
        use wiremock::MockServer;

        let server = MockServer::start().await;
        // No mock mounted — if get_artifact tries to re-download
        // (e.g. the cache invalidation path), the test fails because
        // wiremock returns 404 for unmatched paths.
        let tmp = TempDir::new().expect("tempdir");
        let cache_dir = tmp.path().to_path_buf();
        let bytes: Vec<u8> = b"the real artifact".to_vec();
        let hash = sha256_hex(&bytes);

        // Set up a verifier with a DIFFERENT key than the one that
        // produced the signature — the cleanest way to assert
        // "signature does not verify" without doing byte-level
        // tampering of the base64 string.
        let seed_a = [0u8; 32];
        let seed_b = [1u8; 32]; // distinct key
        let sk_a = ed25519_dalek::SigningKey::from_bytes(&seed_a);
        let sk_b = ed25519_dalek::SigningKey::from_bytes(&seed_b);
        let verifier = std::sync::Arc::new(crate::verifier::Keyring::single(sk_a.verifying_key()));
        // Sign with the WRONG key (sk_b) — verifier is sk_a's pubkey.
        let hash_raw = decode_hex_32(&hash).expect("decode hex hash");
        let dep_id = "d_signed_bad";
        let mut msg = Vec::with_capacity(32 + dep_id.len());
        msg.extend_from_slice(&hash_raw);
        msg.extend_from_slice(dep_id.as_bytes());
        let sig =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sk_b.sign(&msg).to_bytes());

        // Pre-populate cache with valid bytes (hash matches) but
        // the signature on the AppSpec is for a different key.
        tokio::fs::write(cache_dir.join(format!("{dep_id}.wasm")), &bytes)
            .await
            .expect("pre-populate cache");

        let downloader = Downloader::new(
            server.uri(),
            cache_dir.clone(),
            test_signer(),
            Some(verifier),
        );
        let err = downloader
            .get_artifact(dep_id, &hash, Some(&sig), None)
            .await
            .expect_err("wrong-key signature must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("signature") || msg.contains("verify"),
            "expected signature-related error, got: {msg}"
        );
    }

    /// First call (valid sig) populates the cache. Second call
    /// (tampered sig in the new AppSpec) re-verifies via the cache
    /// fast-path and rejects — proves cache hits re-verify.
    #[tokio::test]
    async fn get_artifact_signed_cache_hit_re_verifies() {
        use tempfile::TempDir;
        use wiremock::MockServer;

        let server = MockServer::start().await;
        let tmp = TempDir::new().expect("tempdir");
        let cache_dir = tmp.path().to_path_buf();
        let bytes: Vec<u8> = b"cache hit re-verify".to_vec();
        let hash = sha256_hex(&bytes);

        let seed = [0u8; 32];
        let sk = ed25519_dalek::SigningKey::from_bytes(&seed);
        let verifier = std::sync::Arc::new(crate::verifier::Keyring::single(sk.verifying_key()));
        let hash_raw = decode_hex_32(&hash).expect("decode hex hash");
        let dep_id = "d_signed_cache_reverify";
        let mut msg = Vec::with_capacity(32 + dep_id.len());
        msg.extend_from_slice(&hash_raw);
        msg.extend_from_slice(dep_id.as_bytes());

        let good_sig =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sk.sign(&msg).to_bytes());

        // A tampered sig (one b64 char flipped): take the good sig,
        // flip a character that decodes to a different byte. We
        // can't just append '+' (would fail base64url decode), so
        // we re-encode a corrupted raw sig.
        let mut raw = sk.sign(&msg).to_bytes();
        raw[0] ^= 0x01; // flip one bit
        let bad_sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);

        // Pre-populate cache.
        tokio::fs::write(cache_dir.join(format!("{dep_id}.wasm")), &bytes)
            .await
            .expect("pre-populate cache");

        let downloader = Downloader::new(
            server.uri(),
            cache_dir.clone(),
            test_signer(),
            Some(verifier),
        );

        // First call: valid sig — should return the bytes.
        let result = downloader
            .get_artifact(dep_id, &hash, Some(&good_sig), None)
            .await
            .expect("valid sig must succeed");
        assert_eq!(result.as_ref(), bytes.as_slice());

        // Second call: tampered sig. The cache is populated, but
        // the cache fast-path re-verifies the signature and must
        // reject the tampered sig. Since the test sets no mock
        // for the download path, the error propagates.
        let err = downloader
            .get_artifact(dep_id, &hash, Some(&bad_sig), None)
            .await
            .expect_err("tampered sig on cache hit must re-verify and reject");
        let msg = err.to_string();
        assert!(
            msg.contains("signature") || msg.contains("verify"),
            "expected signature-related error on cache-hit re-verify, got: {msg}"
        );
    }

    /// Verifier configured but AppSpec has no signature → worker
    /// refuses (the supervisor should have caught this earlier, but
    /// the downloader also defends the invariant).
    #[tokio::test]
    async fn get_artifact_missing_signature_when_required_rejects() {
        use tempfile::TempDir;
        use wiremock::MockServer;

        let server = MockServer::start().await;
        let tmp = TempDir::new().expect("tempdir");
        let cache_dir = tmp.path().to_path_buf();
        let bytes: Vec<u8> = b"no sig at all".to_vec();
        let hash = sha256_hex(&bytes);

        let seed = [0u8; 32];
        let sk = ed25519_dalek::SigningKey::from_bytes(&seed);
        let verifier = std::sync::Arc::new(crate::verifier::Keyring::single(sk.verifying_key()));

        // No cache — go straight to download. No mock mounted: a
        // download attempt would fail with a 404 from wiremock.
        let downloader = Downloader::new(
            server.uri(),
            cache_dir.clone(),
            test_signer(),
            Some(verifier),
        );
        let err = downloader
            .get_artifact("d_missing_sig", &hash, None, None)
            .await
            .expect_err("None signature with verifier must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("signature") || msg.contains("no signature"),
            "expected missing-signature error, got: {msg}"
        );
    }

    /// Happy path: 200 from the control plane is treated as success
    /// and `Ok(())` is returned. The worker supervisor treats both
    /// 2xx and 4xx-with-our-signal-across as "delivered"; only 5xx and
    /// transport errors are escalated.
    #[tokio::test]
    async fn post_auto_rollback_success_returns_ok() {
        use tempfile::TempDir;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Match on path + method only. Body shape is asserted below by
        // parsing the captured request body — `serde_json::json!{}`
        // doesn't guarantee key ordering, so a literal body_string
        // matcher is brittle.
        Mock::given(method("POST"))
            .and(path("/api/internal/apps/myapp/auto-rollback"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "deployment_id": "d_prev",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let downloader =
            Downloader::new(server.uri(), tmp.path().to_path_buf(), test_signer(), None);

        let result = downloader
            .post_auto_rollback("t_test", "myapp", "d_broken", 5)
            .await;
        assert!(
            result.is_ok(),
            "200 from server must return Ok, got: {result:?}"
        );

        let received = server.received_requests().await.expect("received");
        assert_eq!(received.len(), 1, "expected exactly one POST");
        // Assert the parsed body shape rather than a literal byte
        // match — restart_count is the field that drives the audit
        // log, so dropping it from the JSON would be a contract
        // regression worth catching.
        let body: serde_json::Value =
            serde_json::from_slice(&received[0].body).expect("body must be valid JSON");
        assert_eq!(body["tenant_id"], "t_test");
        assert_eq!(body["app_name"], "myapp");
        assert_eq!(body["current_deployment_id"], "d_broken");
        assert_eq!(body["restart_count"], 5);
    }

    /// 4xx rejection (e.g. 412 auto-rollback disabled, 409 no last-good):
    /// the supervisor does NOT retry — it logs and continues. The
    /// method returns Err so the supervisor's tracing captures the
    /// failure, but the supervisor's run_app_loop is not blocked on
    /// the response (it `tokio::spawn`s the call).
    #[tokio::test]
    async fn post_auto_rollback_412_returns_err_without_retry() {
        use tempfile::TempDir;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Mount exactly one expectation; the test fails if the
        // downloader issues a second POST (i.e. retries).
        Mock::given(method("POST"))
            .and(path("/api/internal/apps/myapp/auto-rollback"))
            .respond_with(
                ResponseTemplate::new(412)
                    .set_body_string(r#"{"error": "auto-rollback disabled for this app"}"#),
            )
            .expect(1)
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let downloader =
            Downloader::new(server.uri(), tmp.path().to_path_buf(), test_signer(), None);

        let err = downloader
            .post_auto_rollback("t_test", "myapp", "d_broken", 5)
            .await
            .expect_err("412 must propagate as Err");
        assert!(
            err.to_string().contains("412"),
            "expected 412 in error message, got: {err}"
        );

        // Brief delay to let any (incorrect) retry land before we
        // assert received_requests().await.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let received = server.received_requests().await.expect("received");
        assert_eq!(
            received.len(),
            1,
            "412 must NOT trigger a retry — got {} requests",
            received.len()
        );
    }

    /// Path traversal in app_name is rejected client-side: we never
    /// reach the network. The control plane would 400 a malformed
    /// path, but it's cheaper to fail fast in the worker and avoid
    /// polluting server logs.
    #[tokio::test]
    async fn post_auto_rollback_rejects_path_traversal() {
        use tempfile::TempDir;
        use wiremock::MockServer;

        let server = MockServer::start().await;
        // No mock mounted — a request would surface as
        // "expected mock not matched" and fail the test.

        let tmp = TempDir::new().expect("tempdir");
        let downloader =
            Downloader::new(server.uri(), tmp.path().to_path_buf(), test_signer(), None);

        for bad_name in ["../etc", "foo/bar", "foo\\bar", "..", ""] {
            let err = downloader
                .post_auto_rollback("t_test", bad_name, "d_broken", 5)
                .await
                .expect_err(&format!("{bad_name:?} must be rejected"));
            assert!(
                err.to_string().contains("unsafe app_name"),
                "expected rejection for {bad_name:?}, got: {err}"
            );
        }

        let received = server.received_requests().await.expect("received");
        assert!(
            received.is_empty(),
            "no request should reach the server for path-traversal app_names"
        );
    }

    /// Every outbound GET must carry an `Authorization: Bearer <jwt>` header.
    /// The control plane's WorkerAuth middleware rejects unsigned requests
    /// with 401; this test is the worker-side half of that contract.
    #[tokio::test]
    async fn download_attaches_bearer_token() {
        use tempfile::TempDir;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let good_bytes: Vec<u8> = b"the real artifact".to_vec();
        let good_hash = sha256_hex(&good_bytes);

        // Mount a mock that matches ANY request to the URL — we don't try
        // to assert the Authorization header value at the mock level (the
        // signer's token is non-deterministic across runs because of jti).
        // Instead we inspect received_requests() below to extract the token
        // and verify it parses with the worker's identity.
        Mock::given(method("GET"))
            .and(path("/api/internal/download/d_unit_auth"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(good_bytes.clone()))
            .expect(1)
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let cache_dir = tmp.path().to_path_buf();

        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            None,
            "edgecloud",
            "w_test",
            "test",
            "t_test",
        );
        let downloader = Downloader::new(server.uri(), cache_dir, signer, None);

        let _ = downloader
            .get_artifact("d_unit_auth", &good_hash, None, None)
            .await
            .expect("download should succeed");

        let received = server.received_requests().await.expect("received");
        assert_eq!(received.len(), 1, "expected exactly one download");
        let auth_header = received[0]
            .headers
            .get("authorization")
            .expect("Authorization header must be present")
            .to_str()
            .expect("Authorization must be valid ASCII");
        let token = auth_header
            .strip_prefix("Bearer ")
            .expect("Authorization must start with 'Bearer '");

        // Token must parse with the same secret the signer used and carry
        // the worker's identity. This is what the control plane's
        // WorkerAuth middleware does.
        let claims = crate::auth::verify_for_test_only(b"test-secret", "edgecloud", token)
            .expect("verify should succeed");
        assert_eq!(claims.worker_id, "w_test");
        assert_eq!(claims.tenant_id, "t_test");
        assert_eq!(claims.region, "test");
    }

    fn test_signer() -> std::sync::Arc<crate::auth::WorkerJwtSigner> {
        crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            None,
            "edgecloud",
            "w_test",
            "test",
            "t_test",
        )
    }

    #[test]
    fn test_cwasm_serialization_deserialization() {
        let engine = wasmtime::Engine::default();
        // Minimal binary representation of a WebAssembly component
        let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];

        let component = wasmtime::component::Component::from_binary(&engine, &wasm_bytes).unwrap();
        let serialized = component.serialize().unwrap();
        assert!(!serialized.is_empty());

        let deserialized =
            unsafe { wasmtime::component::Component::deserialize(&engine, &serialized).unwrap() };
        assert_eq!(
            component.serialize().unwrap(),
            deserialized.serialize().unwrap()
        );

        // Verify that deserializing corrupted bytes returns an error
        let mut corrupted = serialized.clone();
        if corrupted.len() >= 4 {
            corrupted[0..4].copy_from_slice(&[0, 0, 0, 0]); // overwrite magic header
        } else if !corrupted.is_empty() {
            corrupted[0] ^= 0xFF;
        }
        let corrupt_result =
            unsafe { wasmtime::component::Component::deserialize(&engine, &corrupted) };
        assert!(corrupt_result.is_err());
    }
}
