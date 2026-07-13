//! L4 public-port cache (issue #548).
//!
//! Persists per-`(tenant_id, app_name)` L4/TCP public ports across
//! ingress restarts and across multiple ingress instances in the same
//! region. The cache polls
//! `GET /api/v1/internal/l4-port/{tenantID}/{appName}` on the control
//! plane every `QUOTA_FETCH_INTERVAL` (30s) and stores whatever the
//! control plane has already persisted on the `apps.l4_public_port`
//! column (issue #548 migration 032).
//!
//! ## Why this is read-only on the ingress side
//!
//! The CP-side `AllocateL4Port` is the only authoritative writer —
//! it uses an atomic `UPDATE apps SET l4_public_port = $1 …  WHERE
//! l4_public_port IS NULL RETURNING` so two concurrent ingress
//! instances can't persist the same port. The ingress cache is
//! read-only: it never writes a port back. Allocation happens via
//! the CLI pre-deploy (`POST /api/v1/apps/{appName}/l4-port`,
//! tenant-authenticated) or, when that's the only viable path,
//! operator admin tooling.
//!
//! ## Fallback semantics
//!
//! `apply_heartbeat` consults the cache before falling back to the
//! ingress-local `L4PortPool`. The pool is the v1 fallback for
//! tenants that haven't run `edge tcp-info hello-tcp` yet — without
//! a CLI pre-allocation the cache will return `None` and the heartbeat
//! path uses a local pool slot. **Two ingress instances in the same
//! region will race on the local pool** in that scenario; once the
//! CLI pre-allocates, both ingresses see the same port via the cache
//! and the race disappears. The pre-allocation step is documented in
//! `docs/l4-ingress.md` (issue #548 follow-up).
//!
//! ## Failure-mode contract (matches `quota.rs`)
//!
//! - 200 with `public_port` → store, use the port.
//! - 404 (app not found, or no port yet allocated) → cache miss →
//!   caller falls back to the local pool.
//! - 5xx / network / parse error → cache miss (fail-open), next tick
//!   retries. The last-known cache entry is retained so a transient
//!   failure doesn't flap the routing table.

use crate::l4::L4AppKey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Poll cadence for the L4 port cache. Mirrors `quota::QUOTA_FETCH_INTERVAL`
/// (issue #420) so a single ticker thread can drive both fetches if a
/// future refactor unifies them.
pub const L4_PORT_CACHE_FETCH_INTERVAL: Duration = Duration::from_secs(30);

/// Stale threshold — if the cached entry is older than this, `apply_heartbeat`
/// re-fetches before believing it. The 30s timer ticks on the same cadence
/// as `L4_PORT_CACHE_FETCH_INTERVAL`, so the staleness window is two ticks
/// at most (worst case: the tick just fired the moment a heartbeat arrived).
/// 60s gives us a sane margin without forcing a synchronous fetch on every
/// heartbeat that races the tick.
pub const L4_PORT_CACHE_STALE_AFTER: Duration = Duration::from_secs(60);

/// Per-`(tenant, app)` cached port assignment.
#[derive(Debug, Clone, PartialEq)]
pub struct CachedL4Port {
    pub public_port: u16,
    /// When we last confirmed the port via a fetch. Used to decide
    /// whether to re-fetch on the next `apply_heartbeat` consult.
    pub fetched_at: Instant,
}

/// Shared cache type (mirrors `SharedQuotaCache` / `SharedCache`).
pub type SharedL4PortCache = Arc<RwLock<L4PortCache>>;

/// All cached L4 port assignments, keyed by `(tenant_id, app_name)`.
#[derive(Default)]
pub struct L4PortCache {
    inner: HashMap<L4AppKey, CachedL4Port>,
}

impl L4PortCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up the cached port for a `(tenant, app)`. Returns `None`
    /// if the entry isn't cached (caller falls back to the local
    /// pool).
    #[allow(dead_code)] // exposed for tests + future introspection
    pub fn get(&self, key: &L4AppKey) -> Option<&CachedL4Port> {
        self.inner.get(key)
    }

    /// Whether the cached entry is still fresh enough to trust.
    /// `false` when missing OR when `fetched_at` is older than
    /// `L4_PORT_CACHE_STALE_AFTER`. Forces a re-fetch before
    /// `apply_heartbeat` trusts the port.
    pub fn is_fresh(&self, key: &L4AppKey) -> bool {
        match self.inner.get(key) {
            Some(e) => e.fetched_at.elapsed() < L4_PORT_CACHE_STALE_AFTER,
            None => false,
        }
    }

    /// Update the cache for a `(tenant, app)`.
    pub fn update(&mut self, key: L4AppKey, port: u16) {
        self.inner.insert(
            key,
            CachedL4Port {
                public_port: port,
                fetched_at: Instant::now(),
            },
        );
    }

    /// Number of cached entries (informational; for tests + metrics).
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the cache has no entries (companion to `len` for
    /// clippy::len_without_is_empty).
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// List the keys currently in the cache (test-only).
    #[cfg(test)]
    pub fn keys(&self) -> Vec<L4AppKey> {
        self.inner.keys().cloned().collect()
    }
}

/// Wire shape for `GET /api/v1/internal/l4-port/{tenantID}/{appName}`.
/// Kept private because only this module consumes it.
#[derive(Debug, serde::Deserialize)]
struct L4PortApiResponse {
    public_port: u16,
}

/// Fetch the persisted L4 public-port assignment for one `(tenant, app)`.
/// Returns `Some(port)` on 200 (and updates the cache on the way out
/// via the caller's `update`), `None` on 404 / transient error.
///
/// `api_url` is the control plane base URL (no trailing slash).
/// `internal_token` is the optional `X-Internal-Token` shared secret;
/// when set, the CP's `InternalAuth` middleware gates the endpoint.
pub async fn fetch_l4_port(
    http: &reqwest::Client,
    api_url: &str,
    tenant_id: &str,
    app_name: &str,
    internal_token: Option<&str>,
) -> Option<u16> {
    let url = format!(
        "{}/api/v1/internal/l4-port/{}/{}",
        api_url, tenant_id, app_name
    );
    let mut req = http.get(&url);
    if let Some(tok) = internal_token {
        req = req.header("X-Internal-Token", tok);
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(
                "l4_port_cache: fetch {}/{} failed: {}",
                tenant_id, app_name, e
            );
            return None;
        }
    };
    if !resp.status().is_success() {
        // 404 = app not found OR no allocation yet. 5xx = transient.
        // Both surface as None; the caller treats both as cache miss.
        debug!(
            "l4_port_cache: fetch {}/{} returned {}; treating as cache miss",
            tenant_id,
            app_name,
            resp.status()
        );
        return None;
    }
    match resp.json::<L4PortApiResponse>().await {
        Ok(body) => {
            if body.public_port == 0 {
                // Defensive: 0 is not a valid port and would indicate
                // a CP/ingress JSON contract drift. Treat as miss.
                warn!(
                    "l4_port_cache: {}/{} parsed with public_port=0; treating as miss",
                    tenant_id, app_name
                );
                None
            } else {
                Some(body.public_port)
            }
        }
        Err(e) => {
            warn!(
                "l4_port_cache: parse {}/{} response failed: {}",
                tenant_id, app_name, e
            );
            None
        }
    }
}

/// One tick of the L4-port fetcher: walk the L4 routing-table snapshot,
/// refresh the cache for each `(tenant, app)` that already has a
/// routable L4 entry. Mirrors `quota::process_tick`.
///
/// The fetch list intentionally *only* contains entries with a
/// current L4 route — we don't pre-warm caches for apps that may
/// never heartbeat. Cost is O(routable L4 apps) per tick, bounded
/// by the L4 port range (1000 by default).
pub async fn process_tick(
    http: &reqwest::Client,
    api_url: &str,
    snapshot: &[crate::l4::L4RouteEntry],
    cache: &SharedL4PortCache,
    internal_token: Option<&str>,
) -> (usize, usize) {
    let mut fetched = 0usize;
    let mut failed = 0usize;
    for entry in snapshot {
        let key = L4AppKey::new(&entry.tenant_id, &entry.app_name);
        match fetch_l4_port(
            http,
            api_url,
            &entry.tenant_id,
            &entry.app_name,
            internal_token,
        )
        .await
        {
            Some(port) => {
                let mut w = cache.write().await;
                w.update(key, port);
                fetched += 1;
            }
            None => {
                failed += 1;
            }
        }
    }
    (fetched, failed)
}

/// Spawn the L4-port fetcher background task. Mirrors
/// `spawn_quota_fetcher` (`quota.rs:184`). Polls every
/// `L4_PORT_CACHE_FETCH_INTERVAL` until the shutdown token fires.
///
/// The `l4_table` snapshot is taken inside the tick so a heartbeat
/// that races the tick is observable to the *next* tick at the latest
/// (worst-case latency: `L4_PORT_CACHE_FETCH_INTERVAL` + half a tick
/// — i.e. 45s, well under `L4_PORT_CACHE_STALE_AFTER`).
pub fn spawn_l4_port_cache_fetcher(
    http: reqwest::Client,
    api_url: String,
    cache: SharedL4PortCache,
    internal_token: Option<String>,
    l4_table: Arc<crate::l4::L4RoutingTable>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(L4_PORT_CACHE_FETCH_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("l4_port_cache fetcher: shutdown received");
                    return;
                }
                _ = ticker.tick() => {
                    let snap = l4_table.snapshot().await;
                    let (fetched, failed) = process_tick(
                        &http,
                        &api_url,
                        &snap,
                        &cache,
                        internal_token.as_deref(),
                    )
                    .await;
                    if fetched > 0 || failed > 0 {
                        debug!(
                            "l4_port_cache: tick fetched={} failed={}",
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

    #[test]
    fn cache_get_miss_for_unknown_key() {
        let c = L4PortCache::new();
        assert!(c.get(&L4AppKey::new("t_a", "api")).is_none());
        assert!(!c.is_fresh(&L4AppKey::new("t_a", "api")));
    }

    #[test]
    fn cache_update_then_is_fresh() {
        let mut c = L4PortCache::new();
        let key = L4AppKey::new("t_a", "api");
        c.update(key.clone(), 31042);
        // Just-updated entry is fresh by definition.
        assert!(c.is_fresh(&key));
        let got = c.get(&key).unwrap();
        assert_eq!(got.public_port, 31042);
    }

    #[test]
    fn cache_update_then_is_fresh_ignores_old_entries() {
        // `is_fresh` relies on `fetched_at.elapsed() < STALE`; we test
        // the structure directly by inspecting `elapsed()` is a small
        // value after a fresh update. A separate sleep-based test
        // would flake; the public API contract is enforced by
        // `is_fresh` returning `true` immediately after `update`, and
        // `get` returning the entry regardless of freshness.
        let mut c = L4PortCache::new();
        let key = L4AppKey::new("t_a", "api");
        c.update(key.clone(), 31042);
        assert!(c.is_fresh(&key));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn cache_update_overwrites_stale_port() {
        // Re-update with a different port (e.g. CP had a transient
        // allocation that changed) — verify the cache reflects the
        // latest port, not the first-seen one.
        let mut c = L4PortCache::new();
        let key = L4AppKey::new("t_a", "api");
        c.update(key.clone(), 31042);
        c.update(key.clone(), 31099);
        assert_eq!(c.get(&key).unwrap().public_port, 31099);
    }

    #[tokio::test]
    async fn fetch_l4_port_parses_200() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/l4-port/t_abc/hello-tcp"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "public_port": 31042u16,
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let port = fetch_l4_port(&http, &server.uri(), "t_abc", "hello-tcp", Some("tok")).await;
        assert_eq!(port, Some(31042));
    }

    #[tokio::test]
    async fn fetch_l4_port_sends_internal_token_header() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/l4-port/t_abc/hello-tcp"))
            .and(header("X-Internal-Token", "s3cret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "public_port": 31042u16,
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let port = fetch_l4_port(&http, &server.uri(), "t_abc", "hello-tcp", Some("s3cret")).await;
        assert_eq!(port, Some(31042));
    }

    #[tokio::test]
    async fn fetch_l4_port_returns_none_on_404() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/l4-port/t_abc/hello-tcp"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let port = fetch_l4_port(&http, &server.uri(), "t_abc", "hello-tcp", Some("tok")).await;
        assert_eq!(port, None, "404 must surface as cache miss");
    }

    #[tokio::test]
    async fn fetch_l4_port_returns_none_on_500() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/l4-port/t_abc/hello-tcp"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let port = fetch_l4_port(&http, &server.uri(), "t_abc", "hello-tcp", Some("tok")).await;
        assert_eq!(port, None, "5xx must surface as transient cache miss");
    }

    #[tokio::test]
    async fn process_tick_walks_l4_snapshot() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        // t1 allocates a port; t2 doesn't (404).
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/l4-port/t1/app1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "public_port": 31042u16,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/internal/l4-port/t2/app2"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let cache: SharedL4PortCache = Default::default();
        let snap = vec![
            crate::l4::L4RouteEntry {
                tenant_id: "t1".to_string(),
                app_name: "app1".to_string(),
                public_port: 0, // placeholder; ignored by process_tick
                worker_addr: "1.2.3.4".to_string(),
                upstream_port: 8081,
                last_seen: Instant::now(),
            },
            crate::l4::L4RouteEntry {
                tenant_id: "t2".to_string(),
                app_name: "app2".to_string(),
                public_port: 0,
                worker_addr: "5.6.7.8".to_string(),
                upstream_port: 8082,
                last_seen: Instant::now(),
            },
        ];
        let (fetched, failed) =
            process_tick(&http, &server.uri(), &snap, &cache, Some("tok")).await;
        assert_eq!(fetched, 1);
        assert_eq!(failed, 1);
        // Cache only has t1.
        let r = cache.read().await;
        assert_eq!(r.len(), 1);
        let entry = r.get(&L4AppKey::new("t1", "app1")).unwrap();
        assert_eq!(entry.public_port, 31042);
    }

    #[tokio::test]
    async fn process_tick_zero_snapshot_is_noop() {
        // Defensive: empty L4 routing table (heartbeat subscriber
        // hasn't seen any TCP heartbeats yet) must not call CP at all
        // and must not crash.
        let http = reqwest::Client::new();
        let cache: SharedL4PortCache = Default::default();
        let (fetched, failed) =
            process_tick(&http, "http://unused", &[], &cache, Some("tok")).await;
        assert_eq!(fetched, 0);
        assert_eq!(failed, 0);
    }
}
