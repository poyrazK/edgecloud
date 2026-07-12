//! Per-tenant data-plane rate limit cache — fetched from the control
//! plane API (issue #305).
//!
//! The ingress periodically polls every tenant with a configured rate
//! limit row and caches the four tenant-level caps. The Caddyfile-JSON
//! renderer consults the cache to inject per-tenant `rate_limit` routes
//! (sub-feature #1) and a single global `rate_limit` route keyed on
//! `remote_ip: 0.0.0.0/0` (sub-feature #4). Without this, a noisy
//! tenant could saturate worker resources and DoS other tenants — the
//! control-plane rate-limit only throttles management API calls, not
//! the data plane.
//!
//! Sub-feature scope in this module: read-only fetcher + cache. The
//! renderer's per-tenant + global route emission lives in `caddy.rs`
//! (commit 4). Sub-features #2 (concurrent cap) and #3 (bandwidth) are
//! cached here but not rendered — the schema + admin endpoint + cache
//! are wired so the render layer is a follow-up that only touches
//! `caddy.rs`.
//!
//! Mirrors the structure of `quota.rs` (cache + fetch + spawn). The
//! cache is per-tenant (NOT per-(tenant, app)) because all five caps
//! are tenant-wide — sub-feature #1 wraps every `<tenant>-*` host
//! pattern under one rate_limit route.

use crate::routing::RouteEntry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Per-tenant rate-limit state. The Caddy renderer looks at `rps` (and
/// the matching `burst`) to decide whether to inject a `rate_limit`
/// route. `concurrent_limit` and `bandwidth_bps` are cached for
/// follow-up render work (sub-features #2 + #3) — neither is consumed
/// by the renderer in this PR.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TenantRateLimitState {
    /// Per-tenant RPS cap. 0 = no cap (renderer skips emitting a route).
    pub rps: u32,
    /// Per-tenant burst paired with `rps`. 0 = renderer falls back to
    /// `rps` (matches the per-app rate_limit handler semantics at
    /// `caddy.rs:380-404`).
    pub burst: u32,
    /// Concurrent-request cap per tenant (sub-feature #2). Cached but
    /// not rendered in this PR — needs a custom Caddy module.
    pub concurrent_limit: u32,
    /// Bandwidth cap per tenant (sub-feature #3). Cached but not
    /// rendered in this PR — needs Caddy 2.8+ `rate_limit.bandwidth`.
    pub bandwidth_bps: u64,
    /// When this state was last refreshed. Used to detect staleness
    /// and decide whether to re-fetch.
    pub fetched_at: Option<Instant>,
}

/// Shared cache type for use across `spawn_tenant_rate_limit_fetcher`
/// (writer) and the Caddy renderer (reader). Mirrors the
/// `SharedQuotaCache` alias for `quota.rs`.
pub type SharedTenantRateLimitCache = Arc<RwLock<TenantRateLimitCache>>;

/// All cached per-tenant rate-limit states, keyed by tenant_id.
#[derive(Default)]
pub struct TenantRateLimitCache {
    inner: HashMap<String, TenantRateLimitState>,
}

impl TenantRateLimitCache {
    /// Get the rate-limit state for a tenant. Returns `None` if the
    /// tenant is not in the cache (the renderer should treat `None`
    /// as "no caps known → emit no rate_limit route" — fail-open
    /// matches the quota 402 cache at issue #420).
    #[allow(dead_code)] // exposed for future introspection / metrics.
    pub fn get(&self, tenant_id: &str) -> Option<&TenantRateLimitState> {
        self.inner.get(tenant_id)
    }

    /// Update the cache for a tenant.
    pub fn update(&mut self, tenant_id: String, state: TenantRateLimitState) {
        self.inner.insert(tenant_id, state);
    }

    /// Remove a tenant from the cache (used when the CP returns 404
    /// or when every cap is zero — "no caps configured, skip the
    /// route").
    pub fn remove(&mut self, tenant_id: &str) {
        self.inner.remove(tenant_id);
    }

    /// List the tenants currently in the cache.
    #[allow(dead_code)] // used in tests + future metric surface.
    pub fn known_tenants(&self) -> Vec<String> {
        self.inner.keys().cloned().collect()
    }

    /// Iterate `(tenant_id, state)` for every tenant with `rps > 0`.
    /// The renderer's hot path: skip zero-rps rows so the route table
    /// only carries tenants that need a route.
    #[allow(dead_code)] // consumed by the renderer in issue #305 commit 4.
    pub fn active_caps(&self) -> impl Iterator<Item = (&String, &TenantRateLimitState)> {
        self.inner.iter().filter(|(_, s)| s.rps > 0)
    }
}

/// Default tenant rate-limit fetch interval (issue #305). 30s
/// matches `QUOTA_FETCH_INTERVAL` so both caches refresh on the same
/// beat; an admin write to the rate-limit endpoint propagates within
/// one tick.
pub const TENANT_RATE_LIMIT_FETCH_INTERVAL: Duration = Duration::from_secs(30);

/// Fetch per-tenant rate-limit state for a single tenant from the
/// control plane. Returns `None` for transient errors (network/5xx/
/// parse) so the caller can keep the last-known state. Returns
/// `Some(None)` on 404 (tenant has no quotas row) or on a 200 with
/// all-zero caps — the cache treats both as "no caps configured,
/// skip emitting a route" (fail-open). Returns `Some(Some(state))`
/// on a 200 with at least one non-zero cap.
///
/// The `Some(Option<...>)` shape is deliberate: the caller needs to
/// distinguish "transient fetch failure → keep last-known" from
/// "200 with zero caps → delete cache entry" (a follow-up admin
/// write that zeroes all caps must clear the route immediately).
pub async fn fetch_tenant_rate_limit(
    http: &reqwest::Client,
    api_url: &str,
    tenant_id: &str,
    internal_token: Option<&str>,
) -> Option<Option<TenantRateLimitState>> {
    let url = format!("{}/api/v1/internal/rate-limit/{}", api_url, tenant_id);
    let mut req = http.get(&url);
    if let Some(tok) = internal_token {
        req = req.header("X-Internal-Token", tok);
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!("tenant_ratelimit: fetch {} failed: {}", tenant_id, e);
            return None;
        }
    };
    let status = resp.status();
    if status == 404 {
        // No quotas row — feature not enabled for this tenant.
        debug!(
            "tenant_ratelimit: fetch {} returned 404; clearing cache",
            tenant_id
        );
        return Some(None);
    }
    if !status.is_success() {
        debug!(
            "tenant_ratelimit: fetch {} returned {}; keeping last-known state",
            tenant_id, status
        );
        return None;
    }
    match resp.json::<TenantRateLimitApiResponse>().await {
        Ok(body) => {
            // rps is the only field the renderer consumes; the rest
            // are cached for follow-up render work. We still treat
            // "all zero" as "delete the cache entry" so an admin
            // write that zeroes all caps clears the route
            // immediately.
            let state = TenantRateLimitState {
                rps: body.rps.max(0) as u32,
                burst: body.burst.max(0) as u32,
                concurrent_limit: body.concurrent_limit.max(0) as u32,
                bandwidth_bps: body.bandwidth_bps.max(0) as u64,
                fetched_at: Some(Instant::now()),
            };
            if state.rps == 0
                && state.burst == 0
                && state.concurrent_limit == 0
                && state.bandwidth_bps == 0
            {
                Some(None)
            } else {
                Some(Some(state))
            }
        }
        Err(e) => {
            warn!(
                "tenant_ratelimit: parse {} response failed: {}",
                tenant_id, e
            );
            None
        }
    }
}

/// Wire shape for `GET /api/v1/internal/rate-limit/{tenantID}`. Kept
/// in sync with `TenantRateLimitResponse` in
/// `edge-control-plane/internal/domain/ratelimit.go`. The struct is
/// private because only this module consumes it.
///
/// Field types are `i32` / `i64` to mirror the Go JSON wire shape
/// exactly (the Go response emits ints without bounds); the parser
/// clamps to `u32` / `u64` because the renderer only consumes
/// non-negative values and the admin endpoint validates `>= -1`.
#[derive(Debug, serde::Deserialize)]
struct TenantRateLimitApiResponse {
    #[serde(default)]
    rps: i32,
    #[serde(default)]
    burst: i32,
    #[serde(default)]
    concurrent_limit: i32,
    #[serde(default)]
    bandwidth_bps: i64,
}

/// One tick of the tenant rate-limit fetcher: walk the routing table
/// snapshot, refresh the cache for each tenant. Mirrors
/// `process_tick` in `quota.rs:153`.
pub async fn process_tick(
    http: &reqwest::Client,
    api_url: &str,
    snapshot: &[RouteEntry],
    cache: &SharedTenantRateLimitCache,
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
        match fetch_tenant_rate_limit(http, api_url, &tenant, internal_token).await {
            Some(Some(state)) => {
                let mut w = cache.write().await;
                w.update(tenant, state);
                fetched += 1;
            }
            Some(None) => {
                // 404 or all-zero caps → drop any stale cache entry
                // so the renderer immediately stops emitting a
                // route. This is what makes the follow-up admin
                // write that zeroes all caps take effect within one
                // tick instead of waiting for the 30s reconcile.
                let mut w = cache.write().await;
                w.remove(&tenant);
                fetched += 1;
            }
            None => {
                // Transient failure → keep last-known state.
                failed += 1;
            }
        }
    }
    (fetched, failed)
}

/// Spawn the tenant rate-limit fetcher background task. Mirrors
/// `spawn_quota_fetcher` at `quota.rs:184`. Polls every
/// `TENANT_RATE_LIMIT_FETCH_INTERVAL` until the shutdown token
/// fires. `fetch_interval == 0` disables the fetcher entirely.
pub fn spawn_tenant_rate_limit_fetcher(
    http: reqwest::Client,
    api_url: String,
    cache: SharedTenantRateLimitCache,
    internal_token: Option<String>,
    routing_table: Arc<crate::routing::RoutingTable>,
    fetch_interval: Duration,
    shutdown: CancellationToken,
) {
    if fetch_interval.is_zero() {
        debug!("tenant_ratelimit fetcher disabled (TENANT_RATE_LIMIT_FETCH_INTERVAL=0)");
        return;
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(fetch_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("tenant_ratelimit fetcher: shutdown received");
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
                            "tenant_ratelimit: tick fetched={} failed={}",
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
    fn cache_unknown_tenant_is_none() {
        let cache = TenantRateLimitCache::default();
        assert!(cache.get("t_unknown").is_none());
    }

    #[test]
    fn cache_update_then_lookup() {
        let mut cache = TenantRateLimitCache::default();
        cache.update(
            "t_1".to_string(),
            TenantRateLimitState {
                rps: 100,
                burst: 200,
                concurrent_limit: 50,
                bandwidth_bps: 5_000_000,
                fetched_at: Some(Instant::now()),
            },
        );
        let s = cache.get("t_1").expect("must be in cache");
        assert_eq!(s.rps, 100);
        assert_eq!(s.burst, 200);
        assert_eq!(s.concurrent_limit, 50);
        assert_eq!(s.bandwidth_bps, 5_000_000);
    }

    #[test]
    fn cache_active_caps_skips_zero_rps() {
        let mut cache = TenantRateLimitCache::default();
        cache.update(
            "t_active".into(),
            TenantRateLimitState {
                rps: 100,
                ..Default::default()
            },
        );
        cache.update("t_zero".into(), TenantRateLimitState::default());
        let mut ids: Vec<&String> = cache.active_caps().map(|(k, _)| k).collect();
        ids.sort();
        assert_eq!(ids, vec!["t_active"]);
    }

    #[test]
    fn cache_remove_clears_entry() {
        let mut cache = TenantRateLimitCache::default();
        cache.update(
            "t_1".into(),
            TenantRateLimitState {
                rps: 100,
                ..Default::default()
            },
        );
        assert!(cache.get("t_1").is_some());
        cache.remove("t_1");
        assert!(cache.get("t_1").is_none());
    }

    #[tokio::test]
    async fn fetch_tenant_rate_limit_parses_caps() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/rate-limit/t_abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "rps": 100,
                "burst": 200,
                "concurrent_limit": 50,
                "bandwidth_bps": 5_000_000_i64,
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let out = fetch_tenant_rate_limit(&http, &server.uri(), "t_abc", Some("tok")).await;
        assert!(matches!(out, Some(Some(_))));
        let state = out.unwrap().unwrap();
        assert_eq!(state.rps, 100);
        assert_eq!(state.burst, 200);
        assert_eq!(state.concurrent_limit, 50);
        assert_eq!(state.bandwidth_bps, 5_000_000);
    }

    #[tokio::test]
    async fn fetch_tenant_rate_limit_404_clears_cache() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/rate-limit/t_missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let out = fetch_tenant_rate_limit(&http, &server.uri(), "t_missing", Some("tok")).await;
        assert!(matches!(out, Some(None)), "404 must yield Some(None)");
    }

    #[tokio::test]
    async fn fetch_tenant_rate_limit_all_zero_yields_none_inner() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/rate-limit/t_zero"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "rps": 0,
                "burst": 0,
                "concurrent_limit": 0,
                "bandwidth_bps": 0,
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let out = fetch_tenant_rate_limit(&http, &server.uri(), "t_zero", Some("tok")).await;
        assert!(
            matches!(out, Some(None)),
            "all-zero caps must yield Some(None) so cache entry clears"
        );
    }

    #[tokio::test]
    async fn fetch_tenant_rate_limit_503_keeps_last_known() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/rate-limit/t_abc"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let out = fetch_tenant_rate_limit(&http, &server.uri(), "t_abc", Some("tok")).await;
        assert!(
            out.is_none(),
            "503 must yield None to keep last-known cache entry"
        );
    }

    #[tokio::test]
    async fn process_tick_walks_snapshot_and_clears_zero_caps() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/rate-limit/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "rps": 100, "burst": 200, "concurrent_limit": 0, "bandwidth_bps": 0,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/rate-limit/t2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "rps": 0, "burst": 0, "concurrent_limit": 0, "bandwidth_bps": 0,
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let cache: SharedTenantRateLimitCache = Default::default();
        // Pre-populate t2 with a stale entry to confirm the tick
        // clears it on the all-zero response.
        cache.write().await.update(
            "t2".into(),
            TenantRateLimitState {
                rps: 999,
                ..Default::default()
            },
        );
        let snap = vec![route_entry("t1", "app1"), route_entry("t2", "app2")];
        let (fetched, failed) =
            process_tick(&http, &server.uri(), &snap, &cache, Some("tok")).await;
        assert_eq!(fetched, 2);
        assert_eq!(failed, 0);
        let r = cache.read().await;
        assert_eq!(r.get("t1").unwrap().rps, 100);
        assert!(
            r.get("t2").is_none(),
            "all-zero caps must clear the cache entry"
        );
    }
}
