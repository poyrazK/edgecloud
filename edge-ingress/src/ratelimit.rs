//! Per-app rate limit cache — fetched from the control plane API.
//!
//! The ingress periodically fetches per-app rate limit overrides from the
//! control plane and caches them. The cache is consulted at render time
//! to override the global default (from Config) with the per-app limit.
//!
//! Resolution order:
//!   1. Per-app override from RateLimitCache (fetched from CP)
//!   2. RouteEntry.rate_limit_rps/burst (set via heartbeat / upsert)
//!   3. Config.rate_limit_rps_default/burst_default
//!   4. No rate limiting (all values 0/absent)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::debug;

/// A single per-app rate limit override.
#[derive(Debug, Clone, Copy, Default)]
pub struct RateLimitEntry {
    pub rps: u32,
    pub burst: u32,
}

/// All cached per-app rate limit overrides, keyed by (tenant_id, app_name).
#[derive(Default)]
pub struct RateLimitCache {
    inner: HashMap<(String, String), RateLimitEntry>,
}

impl RateLimitCache {
    /// Get the rate limit for a specific app. Returns `None` if no
    /// per-app override has been fetched from the control plane.
    pub fn get(&self, tenant_id: &str, app_name: &str) -> Option<RateLimitEntry> {
        self.inner
            .get(&(tenant_id.to_string(), app_name.to_string()))
            .copied()
    }

    /// Update the cache with a rate limit override for an app.
    pub fn update(&mut self, tenant_id: String, app_name: String, entry: RateLimitEntry) {
        self.inner.insert((tenant_id, app_name), entry);
    }

    /// Number of cached entries.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if no entries are cached.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Shared handle to the rate limit cache.
pub type SharedRateLimitCache = Arc<RwLock<RateLimitCache>>;

/// Response shape from the control plane's rate limit endpoint.
#[derive(serde::Deserialize)]
struct RateLimitResponse {
    #[serde(default)]
    rps: u32,
    #[serde(default)]
    burst: u32,
}

/// Fetch per-app rate limit override from the control plane API.
/// Returns `None` on 404 (no override), `Some(entry)` on success, and
/// an error on transient failures.
async fn fetch_rate_limit(
    http: &reqwest::Client,
    api_url: &str,
    tenant_id: &str,
    app_name: &str,
    internal_token: Option<&str>,
) -> Result<Option<RateLimitEntry>, String> {
    let url = format!(
        "{}/api/v1/internal/rate-limits/{}/{}",
        api_url, tenant_id, app_name
    );

    let mut req = http.get(&url);
    if let Some(token) = internal_token {
        req = req.header("X-Internal-Token", token);
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => return Err(format!("network: {e}")),
    };
    let status = resp.status();

    // 404 = no per-app override.
    if status == 404 {
        return Ok(None);
    }
    if !status.is_success() {
        return Err(format!("http {status}"));
    }
    let body: RateLimitResponse = match resp.json().await {
        Ok(b) => b,
        Err(e) => return Err(format!("parse: {e}")),
    };
    // Both 0 means "no override" (same as 404).
    if body.rps == 0 && body.burst == 0 {
        return Ok(None);
    }
    Ok(Some(RateLimitEntry {
        rps: body.rps,
        burst: body.burst,
    }))
}

/// Spawn a background task that periodically fetches per-app rate limits
/// for all known apps from the control plane.
///
/// On transient failures, logs a warning and keeps the previous cached
/// value (stale-but-safe). On 404, removes any previously cached override
/// (the app was configured back to "use default").
///
/// The loop runs every `fetch_interval` (default 60s, configurable via
/// `RATE_LIMIT_FETCH_INTERVAL`). When `fetch_interval` is zero, the
/// fetcher is disabled entirely.
pub fn spawn_rate_limit_fetcher(
    http: reqwest::Client,
    api_url: String,
    cache: SharedRateLimitCache,
    internal_token: Option<String>,
    table: Arc<crate::routing::RoutingTable>,
    fetch_interval: Duration,
) {
    if fetch_interval.is_zero() {
        debug!("rate limit fetcher disabled (RATE_LIMIT_FETCH_INTERVAL=0)");
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(fetch_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;

            // Derive the app list from the routing table.
            let apps: Vec<(String, String)> = {
                let snap = table.snapshot().await;
                let mut seen = std::collections::HashSet::new();
                for entry in snap {
                    seen.insert((entry.tenant_id, entry.app_name));
                }
                seen.into_iter().collect()
            };

            for (tenant_id, app_name) in apps {
                match fetch_rate_limit(
                    &http,
                    &api_url,
                    &tenant_id,
                    &app_name,
                    internal_token.as_deref(),
                )
                .await
                {
                    Ok(Some(entry)) => {
                        debug!(
                            tenant = %tenant_id,
                            app = %app_name,
                            rps = entry.rps,
                            burst = entry.burst,
                            "fetched per-app rate limit override"
                        );
                        cache.write().await.update(tenant_id, app_name, entry);
                    }
                    Ok(None) => {
                        // No override — app uses global defaults.
                        // Remove any stale override so render_routes
                        // falls through to RouteEntry/Config defaults.
                        cache.write().await.inner.remove(&(tenant_id, app_name));
                    }
                    Err(e) => {
                        // Transient failure — keep cached value.
                        debug!(
                            tenant = %tenant_id,
                            app = %app_name,
                            err = %e,
                            "transient rate-limit fetch error; keeping cached value"
                        );
                    }
                }
            }
        }
    });
}
