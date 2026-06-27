//! Artifact downloader with local file cache.

use anyhow::Context;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;

use crate::auth::WorkerJwtSigner;

/// Downloads Wasm artifacts from the control plane with local cache.
pub struct Downloader {
    client: reqwest::Client,
    control_plane_url: String,
    cache_dir: PathBuf,
    jwt_signer: Arc<WorkerJwtSigner>,
}

impl Downloader {
    pub fn new(
        control_plane_url: String,
        cache_dir: PathBuf,
        jwt_signer: Arc<WorkerJwtSigner>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            control_plane_url,
            cache_dir,
            jwt_signer,
        }
    }

    /// Get artifact bytes for a deployment.
    ///
    /// Returns cached bytes if available, otherwise downloads from the control plane.
    /// Both the cached and freshly-downloaded bytes are verified against `expected_hash`
    /// (a bare lowercase hex SHA-256 digest) before being returned. Verification errors
    /// (empty hash, malformed hash, or mismatch) cause this function to return `Err`;
    /// a tampered cache file is invalidated and the artifact is re-downloaded once.
    pub async fn get_artifact(
        &self,
        deployment_id: &str,
        expected_hash: &str,
    ) -> anyhow::Result<bytes::Bytes> {
        let cache_path = self.cache_path(deployment_id);

        // Try local cache first. Always verify against expected_hash; an empty hash
        // is an error (see verify_hash) so the cache fast-path is only usable when
        // the producer supplied a real hash.
        if cache_path.exists() {
            match tokio::fs::read(&cache_path).await {
                Ok(data) => match verify_hash(&data, expected_hash, deployment_id) {
                    Ok(()) => {
                        tracing::debug!(deployment_id, bytes = data.len(), "cache hit");
                        return Ok(data.into());
                    }
                    Err(e) => {
                        tracing::warn!(
                            deployment_id,
                            err = %e,
                            "cached artifact failed verification; invalidating and re-downloading"
                        );
                        let _ = tokio::fs::remove_file(&cache_path).await;
                    }
                },
                Err(e) => {
                    tracing::warn!(deployment_id, err = %e, "cache read failed; downloading");
                    let _ = tokio::fs::remove_file(&cache_path).await;
                }
            }
        }

        // Download from control plane. Sign the request with the worker's
        // bearer JWT — the control plane's WorkerAuth middleware will reject
        // any unsigned /api/internal/* request with 401.
        let url = format!(
            "{}/api/internal/download/{}",
            self.control_plane_url, deployment_id
        );
        let token = self
            .jwt_signer
            .sign()
            .context("signing JWT for outbound request")?;
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

        // Ensure cache directory exists and write to cache
        tokio::fs::create_dir_all(&self.cache_dir).await?;
        tokio::fs::write(&cache_path, &data).await?;

        tracing::info!(deployment_id, bytes = data.len(), "artifact cached");
        Ok(data)
    }

    fn cache_path(&self, deployment_id: &str) -> PathBuf {
        self.cache_dir.join(format!("{}.wasm", deployment_id))
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
        let downloader = Downloader::new(server.uri(), cache_dir.clone(), test_signer());

        let bytes: Vec<u8> = b"some test bytes".to_vec();
        let hash = sha256_hex(&bytes);
        tokio::fs::write(cache_dir.join("d_unit_cache_hit.wasm"), &bytes)
            .await
            .expect("pre-populate cache");

        let result = downloader
            .get_artifact("d_unit_cache_hit", &hash)
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
        let downloader = Downloader::new(server.uri(), cache_dir.clone(), test_signer());

        // Pre-populate the cache with content that won't match the expected hash.
        tokio::fs::write(cache_dir.join("d_unit_redownload.wasm"), b"tampered bytes")
            .await
            .expect("pre-populate tampered cache");

        let result = downloader
            .get_artifact("d_unit_redownload", &good_hash)
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
        let downloader = Downloader::new(server.uri(), cache_dir.clone(), test_signer());

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
            .get_artifact("d_unit_500", &hash)
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
            "edgecloud",
            "w_test",
            "test",
            "t_test",
        );
        let downloader = Downloader::new(server.uri(), cache_dir, signer);

        let _ = downloader
            .get_artifact("d_unit_auth", &good_hash)
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
            "edgecloud",
            "w_test",
            "test",
            "t_test",
        )
    }
}
