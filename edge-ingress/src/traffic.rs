//! Traffic split cache — fetched from the control plane API.
//!
//! The ingress periodically fetches traffic splits for all known
//! `(tenant_id, app_name)` pairs and caches them. The cache is consulted
//! at render time to override the heartbeat-derived weight with the
//! authoritative split from the control plane DB.

use crate::routing::RouteEntry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, error, warn};

/// A traffic split for one app: deployment_id → weight.
pub type DeploymentWeights = HashMap<String, u8>;

/// Key identifying a traffic split scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppKey {
    pub tenant_id: String,
    pub app_name: String,
}

/// All cached traffic splits, keyed by (tenant_id, app_name).
#[derive(Default)]
pub struct TrafficSplitCache {
    /// Cached splits, updated periodically from the control plane.
    inner: HashMap<AppKey, DeploymentWeights>,
    /// When each entry was last fetched (for TTL eviction).
    fetched_at: HashMap<AppKey, Instant>,
}

/// TTL for cached splits before we re-fetch.
const CACHE_TTL: Duration = Duration::from_secs(30);

impl TrafficSplitCache {
    /// Get the weight for a specific deployment within an app's split.
    /// Returns `None` if the split is not cached or the deployment is not found.
    pub fn weight(&self, tenant_id: &str, app_name: &str, deployment_id: &str) -> Option<u8> {
        let key = AppKey {
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
        };
        self.inner.get(&key)?.get(deployment_id).copied()
    }

    /// Returns true if the cache has a split for this app and it's not stale.
    pub fn has_split(&self, tenant_id: &str, app_name: &str) -> bool {
        let key = AppKey {
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
        };
        matches!(
            self.fetched_at.get(&key),
            Some(instant) if instant.elapsed() < CACHE_TTL
        )
    }

    /// Update the cache with a new set of splits for an app.
    pub fn update(&mut self, tenant_id: String, app_name: String, weights: DeploymentWeights) {
        let key = AppKey {
            tenant_id,
            app_name,
        };
        self.inner.insert(key.clone(), weights);
        self.fetched_at.insert(key, Instant::now());
    }

    /// Remove stale entries (TTL expired).
    pub fn evict_stale(&mut self) {
        let stale: Vec<AppKey> = self
            .fetched_at
            .iter()
            .filter(|(_, instant)| instant.elapsed() >= CACHE_TTL)
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale {
            self.inner.remove(&key);
            self.fetched_at.remove(&key);
        }
    }

    /// Get the list of all known (tenant_id, app_name) pairs in the cache.
    pub fn known_apps(&self) -> Vec<(String, String)> {
        self.inner
            .keys()
            .map(|k| (k.tenant_id.clone(), k.app_name.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weight_known_deployment() {
        let mut cache = TrafficSplitCache::default();
        cache.update(
            "t1".into(),
            "app".into(),
            HashMap::from([("d1".into(), 80u8)]),
        );
        assert_eq!(cache.weight("t1", "app", "d1"), Some(80));
    }

    #[test]
    fn test_weight_unknown_app() {
        let cache = TrafficSplitCache::default();
        assert_eq!(cache.weight("t1", "unknown", "d1"), None);
    }

    #[test]
    fn test_weight_unknown_deployment() {
        let mut cache = TrafficSplitCache::default();
        cache.update(
            "t1".into(),
            "app".into(),
            HashMap::from([("d1".into(), 50u8)]),
        );
        assert_eq!(cache.weight("t1", "app", "missing"), None);
    }

    #[test]
    fn test_has_split_fresh() {
        let mut cache = TrafficSplitCache::default();
        cache.update("t1".into(), "app".into(), HashMap::new());
        assert!(cache.has_split("t1", "app"));
    }

    #[test]
    fn test_has_split_unknown() {
        let cache = TrafficSplitCache::default();
        assert!(!cache.has_split("unknown", "app"));
    }

    #[test]
    fn test_update_replaces() {
        let mut cache = TrafficSplitCache::default();
        cache.update(
            "t1".into(),
            "app".into(),
            HashMap::from([("d1".into(), 100u8)]),
        );
        cache.update(
            "t1".into(),
            "app".into(),
            HashMap::from([("d1".into(), 50u8)]),
        );
        assert_eq!(cache.weight("t1", "app", "d1"), Some(50));
    }

    #[test]
    fn test_update_also_sets_has_split() {
        let mut cache = TrafficSplitCache::default();
        cache.update("t1".into(), "app".into(), HashMap::new());
        assert!(cache.has_split("t1", "app"));
    }

    #[test]
    fn test_evict_stale_keeps_fresh() {
        let mut cache = TrafficSplitCache::default();
        cache.update(
            "t1".into(),
            "app".into(),
            HashMap::from([("d1".into(), 80u8)]),
        );
        cache.evict_stale();
        // Fresh entry (30s TTL, test runs in microseconds) survives
        assert_eq!(cache.weight("t1", "app", "d1"), Some(80));
    }

    #[test]
    fn test_known_apps_returns_all() {
        let mut cache = TrafficSplitCache::default();
        cache.update("t1".into(), "a".into(), HashMap::new());
        cache.update("t2".into(), "b".into(), HashMap::new());
        let mut apps = cache.known_apps();
        apps.sort();
        assert_eq!(
            apps,
            vec![
                ("t1".to_string(), "a".to_string()),
                ("t2".to_string(), "b".to_string())
            ]
        );
    }

    #[test]
    fn test_known_apps_empty() {
        let cache = TrafficSplitCache::default();
        assert!(cache.known_apps().is_empty());
    }

    #[tokio::test]
    async fn fetch_app_split_returns_weights() {
        let mock_server = wiremock::MockServer::start().await;

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v1/internal/traffic/t1/app1"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
                serde_json::json!({ "splits": [
                    {"deployment_id": "d1", "weight": 80},
                    {"deployment_id": "d2", "weight": 20}
                ]}),
            ))
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::new();
        let outcome = fetch_app_split(&http, &mock_server.uri(), "t1", "app1", None).await;

        match outcome {
            FetchOutcome::Ok(weights) => {
                assert_eq!(weights.get("d1"), Some(&80));
                assert_eq!(weights.get("d2"), Some(&20));
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_app_split_returns_none_on_404() {
        let mock_server = wiremock::MockServer::start().await;

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(
                "/api/v1/internal/traffic/t1/no-such-app",
            ))
            .respond_with(wiremock::ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::new();
        let outcome = fetch_app_split(&http, &mock_server.uri(), "t1", "no-such-app", None).await;

        match outcome {
            FetchOutcome::Transient(reason) => {
                assert!(
                    reason.contains("404"),
                    "expected 404 in transient reason, got {reason}"
                );
            }
            other => panic!("expected Transient(404), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_app_split_returns_unauthorized_on_401() {
        let mock_server = wiremock::MockServer::start().await;

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v1/internal/traffic/t1/app1"))
            .respond_with(wiremock::ResponseTemplate::new(401))
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::new();
        let outcome = fetch_app_split(&http, &mock_server.uri(), "t1", "app1", None).await;

        assert!(matches!(outcome, FetchOutcome::Unauthorized));
    }

    #[tokio::test]
    async fn fetch_app_split_sends_internal_token_header() {
        let mock_server = wiremock::MockServer::start().await;

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v1/internal/traffic/t1/app1"))
            .and(wiremock::matchers::header("X-Internal-Token", "s3cret"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "splits": [] })),
            )
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::new();
        let outcome =
            fetch_app_split(&http, &mock_server.uri(), "t1", "app1", Some("s3cret")).await;

        assert!(matches!(outcome, FetchOutcome::Ok(_)));
    }

    // ── process_tick tests ───────────────────────────────────────────

    fn route_entry(tenant: &str, app: &str, addr: &str, port: u16) -> RouteEntry {
        RouteEntry {
            tenant_id: tenant.to_string(),
            app_name: app.to_string(),
            deployment_id: None,
            weight: 100,
            worker_addr: addr.to_string(),
            port,
            rate_limit_rps: None,
            rate_limit_burst: None,
            last_seen: std::time::Instant::now(),
        }
    }

    #[tokio::test]
    async fn process_tick_empty_table() {
        let cache: SharedCache = Default::default();
        let (fetched, unauthorized) = process_tick(
            &reqwest::Client::new(),
            "http://localhost:1",
            &[],
            &cache,
            None,
        )
        .await;
        assert_eq!(fetched, 0);
        assert_eq!(unauthorized, 0);
    }

    #[tokio::test]
    async fn process_tick_success() {
        let mock = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v1/internal/traffic/t1/app1"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
                serde_json::json!({ "splits": [
                    {"deployment_id": "d1", "weight": 80},
                    {"deployment_id": "d2", "weight": 20}
                ]}),
            ))
            .mount(&mock)
            .await;

        let cache: SharedCache = Default::default();
        let snap = vec![route_entry("t1", "app1", "1.2.3.4", 8081)];
        let (fetched, unauthorized) =
            process_tick(&reqwest::Client::new(), &mock.uri(), &snap, &cache, None).await;
        assert_eq!(fetched, 1);
        assert_eq!(unauthorized, 0);

        // Cache should now have the weights.
        let cache_r = cache.read().await;
        assert_eq!(cache_r.weight("t1", "app1", "d1"), Some(80));
    }

    #[tokio::test]
    async fn process_tick_unauthorized() {
        let mock = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v1/internal/traffic/t1/app1"))
            .respond_with(wiremock::ResponseTemplate::new(401))
            .mount(&mock)
            .await;

        let cache: SharedCache = Default::default();
        let snap = vec![route_entry("t1", "app1", "1.2.3.4", 8081)];
        let (fetched, unauthorized) =
            process_tick(&reqwest::Client::new(), &mock.uri(), &snap, &cache, None).await;
        assert_eq!(fetched, 0);
        assert_eq!(unauthorized, 1);
    }

    #[tokio::test]
    async fn process_tick_transient() {
        let mock = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v1/internal/traffic/t1/app1"))
            .respond_with(wiremock::ResponseTemplate::new(503))
            .mount(&mock)
            .await;

        let cache: SharedCache = Default::default();
        let snap = vec![route_entry("t1", "app1", "1.2.3.4", 8081)];
        // Pre-populate cache to verify it's NOT cleared on transient error.
        cache.write().await.update("t1".into(), "app1".into(), {
            let mut h = std::collections::HashMap::new();
            h.insert("d1".into(), 100u8);
            h
        });

        let (fetched, unauthorized) =
            process_tick(&reqwest::Client::new(), &mock.uri(), &snap, &cache, None).await;
        assert_eq!(fetched, 0);
        assert_eq!(unauthorized, 0);

        // Previous cache value survives.
        let cache_r = cache.read().await;
        assert_eq!(cache_r.weight("t1", "app1", "d1"), Some(100));
    }

    #[tokio::test]
    async fn process_tick_mixed() {
        let mock = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v1/internal/traffic/t1/app1"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
                serde_json::json!({ "splits": [{"deployment_id": "d1", "weight": 100}]}),
            ))
            .mount(&mock)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v1/internal/traffic/t2/app2"))
            .respond_with(wiremock::ResponseTemplate::new(401))
            .mount(&mock)
            .await;

        let cache: SharedCache = Default::default();
        let snap = vec![
            route_entry("t1", "app1", "1.2.3.4", 8081),
            route_entry("t2", "app2", "5.6.7.8", 8082),
        ];
        let (fetched, unauthorized) =
            process_tick(&reqwest::Client::new(), &mock.uri(), &snap, &cache, None).await;
        // t1/app1 succeeds, t2/app2 unauthorized.
        assert_eq!(fetched, 1);
        assert_eq!(unauthorized, 1);

        // Only t1/app1 is in the cache.
        let cache_r = cache.read().await;
        assert_eq!(cache_r.weight("t1", "app1", "d1"), Some(100));
        assert_eq!(cache_r.weight("t2", "app2", "d1"), None);
    }
}

/// Outcome of a single traffic-split fetch. The spawn_fetcher loop uses
/// `Unauthorized` to flag `EDGE_INTERNAL_TOKEN` misconfiguration: the
/// ingress can't talk to the control plane's internal endpoints at all,
/// every canary is silently falling back to single-deployment routing.
/// Surfacing this distinctly from network/5xx errors means operators see
/// the cause on the first failed render, not after hours of "the canary
/// isn't taking effect" debugging.
#[derive(Debug)]
enum FetchOutcome {
    Ok(DeploymentWeights),
    /// 401/403 — the shared secret is missing or wrong. Almost always a
    /// deploy misconfiguration (forgot EDGE_INTERNAL_TOKEN, mismatched
    /// values across control-plane and ingress).
    Unauthorized,
    /// Network error, 5xx, parse error, or any other transient failure.
    /// Transient — log once and keep retrying.
    Transient(String),
}

/// Fetch traffic splits for a specific app from the control plane API.
async fn fetch_app_split(
    http: &reqwest::Client,
    api_url: &str,
    tenant_id: &str,
    app_name: &str,
    internal_token: Option<&str>,
) -> FetchOutcome {
    // /api/v1/internal/traffic/{tenantID}/{appName} is mounted under the
    // control plane's `internalAuth` middleware, which gates on
    // `X-Internal-Token`. The tenant is in the URL path because the
    // ingress isn't tenant-authenticated — it's a service-to-service
    // caller. The CLI's `edge traffic show` uses a different
    // tenant-authenticated endpoint.
    let url = format!(
        "{}/api/v1/internal/traffic/{}/{}",
        api_url, tenant_id, app_name
    );
    #[derive(serde::Deserialize)]
    struct SplitEntry {
        deployment_id: String,
        weight: u8,
    }
    #[derive(serde::Deserialize)]
    struct TrafficResponse {
        splits: Vec<SplitEntry>,
    }

    let mut req = http.get(&url);
    if let Some(token) = internal_token {
        req = req.header("X-Internal-Token", token);
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(tenant = %tenant_id, app = %app_name, err = %e, "failed to fetch traffic split");
            return FetchOutcome::Transient(format!("network: {e}"));
        }
    };
    let status = resp.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        // Don't include `app_name` in this log — the same misconfiguration
        // affects every app, so a per-app line per fetch tick is log spam.
        // The first 401 in a tick is enough signal; the loop suppresses
        // the rest for the same tick.
        return FetchOutcome::Unauthorized;
    }
    if !status.is_success() {
        warn!(tenant = %tenant_id, app = %app_name, status = %status, "traffic split fetch returned non-2xx");
        return FetchOutcome::Transient(format!("http {status}"));
    }
    let body: TrafficResponse = match resp.json().await {
        Ok(b) => b,
        Err(e) => {
            warn!(tenant = %tenant_id, app = %app_name, err = %e, "failed to parse traffic split response");
            return FetchOutcome::Transient(format!("parse: {e}"));
        }
    };
    let weights: DeploymentWeights = body
        .splits
        .into_iter()
        .map(|s| (s.deployment_id, s.weight))
        .collect();
    debug!(tenant = %tenant_id, app = %app_name, count = %weights.len(), "fetched traffic split");
    FetchOutcome::Ok(weights)
}

/// Shared handle to the traffic split cache.
pub type SharedCache = Arc<RwLock<TrafficSplitCache>>;

/// Spawn a background task that periodically re-fetches traffic splits for
/// all known apps. It also periodically removes stale cache entries.
///
/// The loop tracks a `consecutive_unauthorized` counter across apps in a
/// single tick — the first 401 fires an ERROR log with a stable marker
/// (`reason="internal_token_unauthorized"`) that operators can grep for or
/// alert on. Subsequent 401s in the same tick are suppressed to avoid
/// per-app log spam; the next tick resets and re-emits if the issue
/// persists. The fetch never panics, never aborts, never recurses — the
/// ingress still serves traffic, just without canary weights.
pub fn spawn_fetcher(
    http: reqwest::Client,
    api_url: String,
    cache: SharedCache,
    internal_token: Option<String>,
    table: Arc<crate::routing::RoutingTable>,
) {
    tokio::spawn(async move {
        let fetch_interval = Duration::from_secs(30);
        let mut ticker = tokio::time::interval(fetch_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let snap = table.snapshot().await;
            process_tick(&http, &api_url, &snap, &cache, internal_token.as_deref()).await;
        }
    });
}

/// Process one tick of the traffic-split fetcher: derive the app list from
/// the routing table snapshot, evict stale cache entries, fetch each app's
/// traffic split from the control plane API, and log aggregate stats.
///
/// Returns the number of successful fetches and the number of unauthorized
/// responses so callers can surface high-level health.
///
/// This is `pub(crate)` so unit tests can exercise the tick logic directly
/// with a wiremock control plane and synthetic routing snapshots.
pub(crate) async fn process_tick(
    http: &reqwest::Client,
    api_url: &str,
    table_snap: &[RouteEntry],
    cache: &SharedCache,
    internal_token: Option<&str>,
) -> (usize, usize) {
    // Derive the app list from the routing table, not the cache.
    // The cache starts empty and is only populated by this loop,
    // so relying on cache.known_apps() creates a chicken-and-egg
    // bug where no apps are ever fetched (issue #152).
    let apps: Vec<(String, String)> = {
        let mut seen = std::collections::HashSet::new();
        for entry in table_snap {
            seen.insert((entry.tenant_id.clone(), entry.app_name.clone()));
        }
        seen.into_iter().collect()
    };

    cache.write().await.evict_stale();

    let mut fetch_count = 0usize;
    let mut unauthorized_count = 0usize;
    let mut tick_unauthorized_logged = false;

    for (tenant_id, app_name) in apps {
        let outcome = fetch_app_split(http, api_url, &tenant_id, &app_name, internal_token).await;
        match outcome {
            FetchOutcome::Ok(weights) => {
                metrics::counter!("ingress.traffic_fetch.total", "status" => "success")
                    .increment(1);
                fetch_count += 1;
                let mut cache = cache.write().await;
                cache.update(tenant_id, app_name, weights);
            }
            FetchOutcome::Unauthorized => {
                metrics::counter!("ingress.traffic_fetch.total", "status" => "unauthorized")
                    .increment(1);
                unauthorized_count += 1;
                if !tick_unauthorized_logged {
                    error!(
                        reason = "internal_token_unauthorized",
                        api_url = %api_url,
                        canary_routing_degraded = true,
                        "control plane rejected internal token; ingress is serving single-deployment weights only — set EDGE_INTERNAL_TOKEN on the control plane and edge-ingress"
                    );
                    tick_unauthorized_logged = true;
                }
            }
            FetchOutcome::Transient(reason) => {
                metrics::counter!("ingress.traffic_fetch.total", "status" => "failure")
                    .increment(1);
                // Per-app warn is fine here — transient is
                // expected on network blips and a per-app line
                // helps correlate with operator reports.
                debug!(
                    tenant = %tenant_id,
                    app = %app_name,
                    err = %reason,
                    "transient traffic-split fetch error; will retry on next tick"
                );
            }
        }
    }

    (fetch_count, unauthorized_count)
}
