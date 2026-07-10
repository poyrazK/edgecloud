//! Quota state cache — fetched from the control plane API (issue #420).
//!
//! The ingress periodically fetches per-tenant quota state (over_cap
//! boolean + locked_until timestamp) and caches it. The Caddyfile-JSON
//! renderer consults the cache to decide whether to inject a
//! `static_response` 402 block before the reverse_proxy route for the
//! tenant's apps. Without this, free-tier tenants could keep consuming
//! worker capacity after their monthly cap is reached; the worker's
//! `applyTenantDelta` calls `SetDisabledAt` to kill the apps, but that
//! path is per-region and takes a heartbeat to propagate. The
//! Caddy-level 402 is the user-facing backstop and the 30s poll
//! interval keeps it close to real-time.
//!
//! Mirrors the structure of `traffic.rs` (cache + fetch + spawn). The
//! cache is per-tenant (NOT per-(tenant, app)) because the cap is
//! tenant-wide.

use crate::routing::RouteEntry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Per-tenant quota state. The Caddy renderer looks at `over_cap` to
/// decide whether to inject a 402 `static_response` block.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QuotaState {
    /// true when used_* is at or above max_* on either axis. The
    /// deploy-time gate is the source of truth for billing boundaries;
    /// this is the user-facing backstop.
    pub over_cap: bool,
    /// Free-tier grace clock — the request-time 402 only kicks in
    /// after this timestamp. `None` means "no grace active, 402 is
    /// in effect immediately".
    pub locked_until: Option<String>,
    /// When this state was last refreshed. Used to detect staleness
    /// and decide whether to re-fetch.
    pub fetched_at: Option<Instant>,
}

/// Shared cache type for use across `spawn_quota_fetcher` (writer) and
/// the Caddy renderer (reader). Mirrors the `SharedCache` alias for
/// `traffic.rs`.
pub type SharedQuotaCache = Arc<RwLock<QuotaCache>>;

/// All cached quota states, keyed by tenant_id.
#[derive(Default)]
pub struct QuotaCache {
    inner: HashMap<String, QuotaState>,
}

impl QuotaCache {
    /// Get the quota state for a tenant. Returns `None` if the tenant
    /// is not in the cache (the renderer should treat `None` as
    /// "no quota info → do NOT inject 402" — fail-open is the right
    /// default for a deployment that's never seen the tenant).
    #[allow(dead_code)] // exposed for future introspection / metrics.
    pub fn get(&self, tenant_id: &str) -> Option<&QuotaState> {
        self.inner.get(tenant_id)
    }

    /// Update the cache for a tenant.
    pub fn update(&mut self, tenant_id: String, state: QuotaState) {
        self.inner.insert(tenant_id, state);
    }

    /// List the tenants currently in the cache.
    #[allow(dead_code)] // used in tests; future metric surface.
    pub fn known_tenants(&self) -> Vec<String> {
        self.inner.keys().cloned().collect()
    }

    /// Returns true if the tenant is over cap. The renderer's hot
    /// path: a single `HashMap` lookup, no allocations.
    pub fn is_over_cap(&self, tenant_id: &str) -> bool {
        self.inner
            .get(tenant_id)
            .map(|s| s.over_cap)
            .unwrap_or(false)
    }
}

/// Default quota fetch interval (issue #420). 30s matches
/// `RATE_LIMIT_FETCH_INTERVAL` (60s) and the heartbeat tick (30s) so
/// the ingress reacts to a free-tier lockdown within one tick of the
/// worker's `applyTenantDelta` call.
pub const QUOTA_FETCH_INTERVAL: Duration = Duration::from_secs(30);

/// Fetch quota state for a single tenant from the control plane.
/// Returns `None` for transient errors (network/5xx/parse) so the
/// caller can keep the last-known state. Returns `Some(state)` on 200;
/// the cache is NOT updated on transient failures (see `process_tick`).
pub async fn fetch_quota_state(
    http: &reqwest::Client,
    api_url: &str,
    tenant_id: &str,
    internal_token: Option<&str>,
) -> Option<QuotaState> {
    let url = format!("{}/api/v1/internal/quota/{}", api_url, tenant_id);
    let mut req = http.get(&url);
    if let Some(tok) = internal_token {
        req = req.header("X-Internal-Token", tok);
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!("quota: fetch {} failed: {}", tenant_id, e);
            return None;
        }
    };
    if !resp.status().is_success() {
        debug!(
            "quota: fetch {} returned {}; keeping last-known state",
            tenant_id,
            resp.status()
        );
        return None;
    }
    match resp.json::<QuotaApiResponse>().await {
        Ok(body) => Some(QuotaState {
            over_cap: body.over_cap,
            locked_until: body.locked_until,
            fetched_at: Some(Instant::now()),
        }),
        Err(e) => {
            warn!("quota: parse {} response failed: {}", tenant_id, e);
            None
        }
    }
}

/// Wire shape for `GET /api/v1/internal/quota/{tenantID}`. Kept in
/// sync with `quotaInternalResponse` in
/// `edge-control-plane/internal/handler/quota.go`. The struct is
/// private because only this module consumes it.
#[derive(Debug, serde::Deserialize)]
struct QuotaApiResponse {
    #[serde(default)]
    over_cap: bool,
    #[serde(default)]
    locked_until: Option<String>,
    // Other fields (max_*, used_*, period_start) are ignored — the
    // ingress only needs the derived `over_cap` + `locked_until` for
    // 402-render decisions.
}

/// One tick of the quota fetcher: walk the routing table snapshot,
/// refresh the cache for each tenant. Mirrors the `process_tick`
/// pattern in `traffic.rs:559`.
pub async fn process_tick(
    http: &reqwest::Client,
    api_url: &str,
    snapshot: &[RouteEntry],
    cache: &SharedQuotaCache,
    internal_token: Option<&str>,
) -> (usize, usize) {
    let mut tenants: HashSet<String> = HashSet::new();
    for entry in snapshot {
        if !entry.tenant_id.is_empty() {
            tenants.insert(entry.tenant_id.clone());
        }
    }
    let tenants: Vec<String> = tenants.into_iter().collect();
    let mut fetched = 0usize;
    let mut failed = 0usize;
    for tenant in tenants {
        if let Some(state) = fetch_quota_state(http, api_url, &tenant, internal_token).await {
            let mut w = cache.write().await;
            w.update(tenant, state);
            fetched += 1;
        } else {
            failed += 1;
        }
    }
    (fetched, failed)
}

/// Spawn the quota fetcher background task. Mirrors `spawn_fetcher`
/// in `traffic.rs:523`. Polls every `QUOTA_FETCH_INTERVAL` until the
/// shutdown token fires.
pub fn spawn_quota_fetcher(
    http: reqwest::Client,
    api_url: String,
    cache: SharedQuotaCache,
    internal_token: Option<String>,
    routing_table: Arc<crate::routing::RoutingTable>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(QUOTA_FETCH_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("quota fetcher: shutdown received");
                    return;
                }
                _ = ticker.tick() => {
                    let snap = routing_table.snapshot().await;
                    let (fetched, failed) = process_tick(
                        &http,
                        &api_url,
                        &snap,
                        &cache,
                        internal_token.as_deref(),
                    ).await;
                    if fetched > 0 || failed > 0 {
                        debug!(
                            "quota: tick fetched={} failed={}",
                            fetched, failed
                        );
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RouteEntry;

    fn route_entry(tenant: &str, app: &str) -> RouteEntry {
        RouteEntry {
            tenant_id: tenant.to_string(),
            app_name: app.to_string(),
            deployment_id: None,
            weight: 100,
            worker_addr: "1.2.3.4".to_string(),
            port: 8081,
            rate_limit_rps: None,
            rate_limit_burst: None,
            last_seen: Instant::now(),
        }
    }

    #[test]
    fn cache_is_over_cap_default_false() {
        let cache = QuotaCache::default();
        // Unknown tenant → not over cap (fail-open).
        assert!(!cache.is_over_cap("t_unknown"));
    }

    #[test]
    fn cache_update_then_lookup() {
        let mut cache = QuotaCache::default();
        cache.update(
            "t_1".to_string(),
            QuotaState {
                over_cap: true,
                locked_until: Some("2026-08-01T00:00:00Z".to_string()),
                fetched_at: Some(Instant::now()),
            },
        );
        assert!(cache.is_over_cap("t_1"));
        assert!(!cache.is_over_cap("t_2"));
    }

    #[tokio::test]
    async fn fetch_quota_state_parses_over_cap() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/quota/t_abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "over_cap": true,
                "locked_until": "2026-08-01T00:00:00Z",
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let state = fetch_quota_state(&http, &server.uri(), "t_abc", Some("tok")).await;
        assert!(state.is_some());
        let s = state.unwrap();
        assert!(s.over_cap);
        assert_eq!(s.locked_until.as_deref(), Some("2026-08-01T00:00:00Z"));
    }

    #[tokio::test]
    async fn fetch_quota_state_sends_internal_token_header() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/quota/t_abc"))
            .and(header("X-Internal-Token", "s3cret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "over_cap": false,
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let state = fetch_quota_state(&http, &server.uri(), "t_abc", Some("s3cret")).await;
        assert!(state.is_some());
    }

    #[tokio::test]
    async fn fetch_quota_state_keeps_last_known_on_500() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/quota/t_abc"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let state = fetch_quota_state(&http, &server.uri(), "t_abc", Some("tok")).await;
        assert!(state.is_none(), "503 must return None to keep last-known");
    }

    #[tokio::test]
    async fn process_tick_walks_snapshot() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/quota/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "over_cap": false,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/quota/t2"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let cache: SharedQuotaCache = Default::default();
        let snap = vec![route_entry("t1", "app1"), route_entry("t2", "app2")];
        let (fetched, failed) =
            process_tick(&http, &server.uri(), &snap, &cache, Some("tok")).await;
        assert_eq!(fetched, 1);
        assert_eq!(failed, 1);
        let r = cache.read().await;
        assert!(!r.is_over_cap("t1"));
        // t2 was 404 → not in cache → fail-open default.
        assert!(!r.is_over_cap("t2"));
    }
}
