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
    /// The `expected_hash` is used for cache invalidation in a future version.
    pub async fn get_artifact(
        &self,
        deployment_id: &str,
        _expected_hash: &str,
    ) -> anyhow::Result<bytes::Bytes> {
        let cache_path = self.cache_path(deployment_id);

        // Check local cache first
        if cache_path.exists() {
            let data = tokio::fs::read(&cache_path).await?;
            tracing::debug!(deployment_id, bytes = data.len(), "cache hit");
            return Ok(data.into());
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
