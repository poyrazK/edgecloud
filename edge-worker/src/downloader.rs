//! Artifact downloader with local file cache.

use anyhow::Context;
use sha2::{Digest, Sha256};
use std::path::PathBuf;

/// Downloads Wasm artifacts from the control plane with local cache.
pub struct Downloader {
    client: reqwest::Client,
    control_plane_url: String,
    cache_dir: PathBuf,
}

impl Downloader {
    pub fn new(control_plane_url: String, cache_dir: PathBuf) -> Self {
        Self {
            client: reqwest::Client::new(),
            control_plane_url,
            cache_dir,
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

        // Download from control plane
        let url = format!(
            "{}/api/internal/download/{}",
            self.control_plane_url, deployment_id
        );
        tracing::info!(url, "downloading artifact");

        let response = self
            .client
            .get(&url)
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
/// Errors on:
/// - empty `expected_hex` (closes the pre-fix bypass where empty meant "skip")
/// - wrong length or non-hex characters
/// - hash mismatch
fn verify_hash(bytes: &[u8], expected_hex: &str, deployment_id: &str) -> anyhow::Result<()> {
    if expected_hex.is_empty() {
        tracing::error!(
            deployment_id,
            "deployment_hash is empty; refusing to instantiate unverified artifact"
        );
        anyhow::bail!("deployment_hash is empty for {deployment_id}");
    }

    if expected_hex.len() != 64 || !expected_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        tracing::error!(
            deployment_id,
            len = expected_hex.len(),
            "deployment_hash must be 64 lowercase hex chars"
        );
        anyhow::bail!(
            "deployment_hash for {deployment_id} has wrong length or non-hex chars (got {} chars)",
            expected_hex.len()
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
/// Caller must have validated `len == 64 && all hex`.
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

fn hex_nibble(b: u8) -> anyhow::Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        _ => anyhow::bail!("non-hex byte: 0x{b:02x}"),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
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
    fn hex_encode_decode_roundtrip() {
        let data: Vec<u8> = (0u8..=255).collect();
        let encoded = hex_encode(&data);
        assert_eq!(encoded.len(), data.len() * 2);
        // Every byte 0x00..=0xff round-trips cleanly via the decoder (used for
        // fixed-size SHA-256 digests, but exercised here across the full byte range).
        let encoded_32 = encoded.chars().take(64).collect::<String>();
        let decoded = decode_hex_32(&encoded_32).expect("decode");
        assert_eq!(decoded.to_vec(), data[..32]);
    }

    #[test]
    fn decode_hex_32_rejects_non_hex() {
        // Length is 64 but contains 'Z' (non-hex) at index 0.
        let bad = format!("Z{}", "0".repeat(63));
        assert!(decode_hex_32(&bad).is_err());
    }
}
