//! Artifact downloader with local file cache.

use anyhow::Context;
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
    /// If `expected_hash` is non-empty and a cached file exists, the cache is invalidated
    /// and the artifact is re-downloaded. This ensures deployments that change on the
    /// control plane are picked up without manual cache clearing.
    pub async fn get_artifact(
        &self,
        deployment_id: &str,
        expected_hash: &str,
    ) -> anyhow::Result<bytes::Bytes> {
        let cache_path = self.cache_path(deployment_id);

        // Check local cache first; skip cache if a hash is provided (cache invalidation).
        if cache_path.exists() && expected_hash.is_empty() {
            let data = tokio::fs::read(&cache_path).await?;
            tracing::debug!(deployment_id, bytes = data.len(), "cache hit");
            return Ok(data.into());
        }

        // Invalidate cache if hash is provided and file exists
        if cache_path.exists() && !expected_hash.is_empty() {
            tracing::debug!(deployment_id, "cache invalidated by hash mismatch");
            tokio::fs::remove_file(&cache_path).await.ok();
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
