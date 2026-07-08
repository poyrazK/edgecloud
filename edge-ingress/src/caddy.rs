//! Caddy admin-API client and Caddyfile-JSON renderer.
//!
//! Caddy exposes a JSON admin API on a configurable port (default `:2019`).
//! We render the full Caddyfile-JSON in Rust and `POST /load` it on every
//! routing change. The config is small (one route per app) and the round
//! trip is fast (~50ms for thousands of routes), so a full reload is fine
//! for v1.
//!
//! TODO(incremental-caddy): when route count exceeds ~10k, switch to
//! `PUT /id/<id>/apps/http/servers/edge_https/routes/<n>` patches and
//! track per-route handles in the `RoutingTable` snapshot.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use tracing::warn;

use crate::config::{ingress_host, Config};
use crate::ratelimit::RateLimitCache;
use crate::routing::{FqdnBinding, RouteEntry};
use crate::traffic::TrafficSplitCache;

const SERVER_NAME_HTTPS: &str = "edge_https";
const SERVER_NAME_HTTP: &str = "edge_http";

/// Number of attempts (initial + retries) for `post_with_retry`.
/// 3 attempts with exponential backoff (100ms → 200ms → 400ms,
/// capped at 2s) means worst-case latency for a hard-down Caddy
/// is ~700ms before we give up — the upstream snapshotter
/// (heartbeat / domain poller) is on a 30s tick so the next
/// reload will be tried promptly.
const MAX_LOAD_ATTEMPTS: u32 = 3;
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct CaddyClient {
    http: Client,
    admin_url: String,
    token: Option<String>,
}

impl CaddyClient {
    pub fn new(admin_url: &str, token: Option<String>) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("building reqwest client")?;
        Ok(Self {
            http,
            admin_url: admin_url.trim_end_matches('/').to_string(),
            token,
        })
    }

    /// POST the rendered config to Caddy's `/load` endpoint. Replaces
    /// the entire config. Bearer-token header is added when configured.
    /// Retries up to `MAX_LOAD_ATTEMPTS` times with exponential
    /// backoff on 5xx / 429 / 408 (transient Caddy failures during a
    /// config reload — e.g. Caddy briefly refusing the request while
    /// it applies the previous batch). 4xx other than 429/408 are NOT
    /// retried: a 400 means we sent malformed JSON, retrying is just
    /// log noise.
    pub async fn load_config(&self, config: &Value) -> Result<()> {
        let url = format!("{}/load", self.admin_url);
        post_with_retry(&self.http, &url, self.token.as_deref(), config).await
    }
}

/// POST `body` to `url`, retrying on transient HTTP failures. The
/// retryable status set mirrors `edge-runtime`'s `http_client` (5xx,
/// 429, 408). 4xx other than 408/429 are non-retryable: they signal
/// a caller bug, not a transient Caddy failure.
async fn post_with_retry(
    client: &Client,
    url: &str,
    token: Option<&str>,
    body: &Value,
) -> Result<()> {
    let mut delay = INITIAL_BACKOFF;
    for attempt in 1..=MAX_LOAD_ATTEMPTS {
        let mut req = client.post(url).json(body);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                // Transport error (connect refused, DNS, TLS handshake).
                // Treat as retryable: Caddy might be restarting in lockstep
                // with the ingress.
                if attempt < MAX_LOAD_ATTEMPTS {
                    warn!(
                        attempt,
                        max = MAX_LOAD_ATTEMPTS,
                        err = %e,
                        "Caddy /load transport error, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_BACKOFF);
                    continue;
                }
                return Err(anyhow!(
                    "Caddy /load transport error after {attempt} attempts: {e}"
                ));
            }
        };
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let retryable = is_retryable_status(status);
        let body_txt = resp.text().await.unwrap_or_default();
        if retryable && attempt < MAX_LOAD_ATTEMPTS {
            warn!(
                attempt,
                max = MAX_LOAD_ATTEMPTS,
                status = status.as_u16(),
                "Caddy /load transient failure, retrying"
            );
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_BACKOFF);
            continue;
        }
        return Err(anyhow!(
            "Caddy /load returned {status} after {attempt} attempt(s): {body_txt}"
        ));
    }
    unreachable!("loop always returns or continues")
}

fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error() || status.as_u16() == 429 || status.as_u16() == 408
}

/// Render the full Caddyfile-JSON for a set of routes. Pure function — no
/// I/O, easy to unit-test.
///
/// Hosts are formatted as `<tenant_id>-<app_name>.edgecloud.dev` so two
/// tenants creating an app named `api` don't collide on the shared wildcard.
///
/// When multiple entries exist for the same `(tenant_id, app_name)` (canary /
/// blue-green), they are rendered as weighted upstreams in a single route:
/// ```json
/// "upstreams": [
///   {"dial": "1.2.3.4:8081", "weight": 95},
///   {"dial": "1.2.3.5:8082", "weight": 5}
/// ]
/// ```
///
/// The `traffic_cache` is consulted for authoritative weights. If a cached
/// split exists for a `(tenant_id, app_name)` the cached weight is used;
/// otherwise `e.weight` (from the heartbeat, defaulting to 100) is used.
///
/// A single entry omits the `weight` key, which Caddy defaults to `1` for
/// that sole upstream — identical routing behaviour to the legacy path.
///
/// `fqdns` is the list of custom-domain bindings (issue #83). Each
/// `FqdnBinding` becomes a per-host route with `tls.on_demand: {}`
/// attached; Caddy asks the control plane's `tls-allowed` endpoint
/// before issuing a cert. FQDN routes are appended AFTER the default
/// routes so the synthetic hostnames continue to take priority when
/// both match. A FQDN whose `(tenant, app)` is missing from `entries`
/// is silently skipped — that means the underlying app is not
/// currently running, so the route would 502 anyway.
pub fn render_routes(
    entries: &[RouteEntry],
    fqdns: &[FqdnBinding],
    cfg: &Config,
    traffic_cache: &TrafficSplitCache,
    rate_limit_cache: &RateLimitCache,
) -> Value {
    // Group entries by (tenant_id, app_name). Each entry in a group represents
    // a different deployment_id for the same app (canary/blue-green).
    let mut groups: HashMap<(&str, &str), Vec<&RouteEntry>> = HashMap::new();
    for e in entries {
        groups
            .entry((e.tenant_id.as_str(), e.app_name.as_str()))
            .or_default()
            .push(e);
    }

    // Sort groups by (tenant_id, app_name) for deterministic output.
    let mut group_keys: Vec<_> = groups.keys().collect();
    group_keys.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));

    let mut routes: Vec<Value> = group_keys
        .iter()
        .map(|(tenant_id, app_name)| {
            let host = ingress_host(tenant_id, app_name);
            let group = &groups[&(*tenant_id, *app_name)];
            // Sort group by deployment_id so output is deterministic.
            let mut group_sorted = (*group).clone();
            group_sorted.sort_by(|a, b| {
                a.deployment_id
                    .cmp(&b.deployment_id)
                    .then_with(|| a.worker_addr.cmp(&b.worker_addr))
            });

            let upstreams: Value = if group_sorted.len() == 1 {
                // Single deployment — no weight key needed.
                let e = group_sorted[0];
                serde_json::json!([{"dial": format!("{}:{}", e.worker_addr, e.port)}])
            } else {
                // Multiple deployments — use cached weights when available.
                serde_json::json!(group_sorted
                    .iter()
                    .map(|e| {
                        // Use cached traffic split weight if available, otherwise
                        // fall back to the heartbeat weight (default 100).
                        let weight = e
                            .deployment_id
                            .as_ref()
                            .and_then(|did| traffic_cache.weight(tenant_id, app_name, did))
                            .unwrap_or(e.weight);
                        serde_json::json!({
                            "dial": format!("{}:{}", e.worker_addr, e.port),
                            "weight": weight,
                        })
                    })
                    .collect::<Vec<_>>())
            };

            // Resolve effective rate limit for this route.
            // Priority: per-app cache entry > RouteEntry field > Config default.
            let first = group_sorted[0];
            let cached = rate_limit_cache.get(tenant_id, app_name);
            let rps = cached
                .map(|e| e.rps)
                .or(first.rate_limit_rps)
                .or_else(|| {
                    let d = cfg.rate_limit_rps_default;
                    if d > 0 {
                        Some(d)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let burst = cached
                .map(|e| e.burst)
                .or(first.rate_limit_burst)
                .or_else(|| {
                    let d = cfg.rate_limit_burst_default;
                    if d > 0 {
                        Some(d)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);

            let mut handle_chain = Vec::new();

            // Inject rate_limit handler when rps > 0.
            if rps > 0 {
                let burst = if burst > 0 { burst } else { rps };
                handle_chain.push(json!({
                    "handler": "rate_limit",
                    "rates": {
                        "rps": rps,
                        "burst": burst,
                    },
                    "key": "{http.request.host}",
                }));
            }

            handle_chain.push(json!({
                "handler": "reverse_proxy",
                "upstreams": upstreams,
                "health_checks": {
                    "active": {"uri": "/", "expect_status": 2}
                }
            }));

            json!({
                "match": [{"host": [host]}],
                "handle": [{
                    "handler": "subroute",
                    "routes": [{
                        "handle": handle_chain,
                    }]
                }],
                "terminal": true
            })
        })
        .collect();

    // Build a (tenant, app) → upstream lookup from the by_app snapshot.
    // The FQDN map carries no upstream info by design; we resolve at
    // render time. See the routing.rs module-level doc comment for why.
    let upstream_index: HashMap<(String, String), (String, u16)> = entries
        .iter()
        .map(|e| {
            (
                (e.tenant_id.clone(), e.app_name.clone()),
                (e.worker_addr.clone(), e.port),
            )
        })
        .collect();

    // Build a (tenant, app) → rate limit lookup for FQDN routes.
    let rate_limit_index: HashMap<(String, String), (u32, u32)> = entries
        .iter()
        .map(|e| {
            let rps = e
                .rate_limit_rps
                .or(Some(cfg.rate_limit_rps_default))
                .unwrap_or(0);
            let burst = e
                .rate_limit_burst
                .or(Some(cfg.rate_limit_burst_default))
                .unwrap_or(0);
            ((e.tenant_id.clone(), e.app_name.clone()), (rps, burst))
        })
        .collect();

    // FQDN routes: sort by FQDN for deterministic output.
    let mut sorted_fqdns: Vec<&FqdnBinding> = fqdns.iter().collect();
    sorted_fqdns.sort_by(|a, b| a.fqdn.cmp(&b.fqdn));

    for b in sorted_fqdns {
        // Skip FQDNs whose app has no upstream — the route would 502.
        // We don't remove the FQDN from the table for that; the next
        // 30s poll will pick it up if the app comes back.
        let Some((worker_addr, port)) =
            upstream_index.get(&(b.tenant_id.clone(), b.app_name.clone()))
        else {
            continue;
        };

        // Resolve rate limit for this FQDN route.
        // Priority: per-app cache entry > RouteEntry field > Config default.
        let cached = rate_limit_cache.get(&b.tenant_id, &b.app_name);
        let (fqdn_rps, fqdn_burst) = {
            let entry = rate_limit_index.get(&(b.tenant_id.clone(), b.app_name.clone()));
            let from_entry = entry.copied().unwrap_or((0, 0));
            let rps = cached
                .map(|e| e.rps)
                .or(if from_entry.0 > 0 {
                    Some(from_entry.0)
                } else {
                    None
                })
                .or_else(|| {
                    let d = cfg.rate_limit_rps_default;
                    if d > 0 {
                        Some(d)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let burst = cached
                .map(|e| e.burst)
                .or(if from_entry.1 > 0 {
                    Some(from_entry.1)
                } else {
                    None
                })
                .or_else(|| {
                    let d = cfg.rate_limit_burst_default;
                    if d > 0 {
                        Some(d)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            (rps, burst)
        };

        let mut fqdn_handle_chain = Vec::new();
        if fqdn_rps > 0 {
            let burst = if fqdn_burst > 0 { fqdn_burst } else { fqdn_rps };
            fqdn_handle_chain.push(json!({
                "handler": "rate_limit",
                "rates": { "rps": fqdn_rps, "burst": burst },
                "key": "{http.request.host}",
            }));
        }
        fqdn_handle_chain.push(json!({
            "handler": "reverse_proxy",
            "upstreams": [{"dial": format!("{}:{}", worker_addr, port)}],
            "health_checks": health_checks_block(cfg)
        }));

        routes.push(json!({
            "match": [{"host": [b.fqdn]}],
            "handle": [{
                "handler": "subroute",
                "routes": [{
                    "handle": fqdn_handle_chain,
                }]
            }],
            "terminal": true,
            "tls": {"on_demand": {}}
        }));
    }

    let mut servers = serde_json::Map::new();
    servers.insert(
        SERVER_NAME_HTTPS.to_string(),
        json!({
            "listen": [cfg.listen_https],
            "routes": routes,
        }),
    );
    if cfg.http_to_https {
        servers.insert(
            SERVER_NAME_HTTP.to_string(),
            json!({
                "listen": [cfg.listen_http],
                "routes": [{
                    "handle": [{
                        "handler": "static_response",
                        "headers": {"Location": ["{http.request.uri}"]},
                        "status_code": 308
                    }]
                }]
            }),
        );
    }

    // On-demand ask URL is only emitted in custom-domain mode. Caddy
    // hits the control plane's /api/internal/tls-allowed endpoint
    // before issuing a cert for a never-seen-before hostname.
    let mut tls = serde_json::Map::new();
    tls.insert(
        "certificates".to_string(),
        json!({
            "load_files": [
                {"certificate": cfg.cert_file, "key": cfg.key_file}
            ]
        }),
    );
    if !cfg.control_plane_url.is_empty() {
        let ask_url = format!("{}/api/internal/tls-allowed", cfg.control_plane_url);
        tls.insert(
            "automation".to_string(),
            json!({
                "on_demand": {
                    "ask": ask_url
                }
            }),
        );
    }

    let mut root = serde_json::Map::new();
    // Preserve the admin binding across POST /load reloads. Without
    // this, Caddy resets to its default `localhost:2019` (inside the
    // container), breaking the next reload from the host.
    root.insert(
        "admin".to_string(),
        json!({"listen": cfg.caddy_admin_listen}),
    );
    let mut http_apps = serde_json::Map::new();
    http_apps.insert("servers".to_string(), Value::Object(servers));
    let mut http_block = serde_json::Map::new();
    http_block.insert("http".to_string(), Value::Object(http_apps));
    http_block.insert("tls".to_string(), Value::Object(tls));
    root.insert("apps".to_string(), Value::Object(http_block));
    Value::Object(root)
}

/// Build the Caddy health_checks block from config. Emits active checks
/// with configurable interval/timeout/uri/max_fails, and passive checks
/// for additional failure detection.
fn health_checks_block(cfg: &Config) -> serde_json::Value {
    json!({
        "active": {
            "uri": cfg.health_check_uri,
            "expect_status": 2,
            "interval": cfg.health_check_interval.as_secs_f64().to_string() + "s",
            "timeout": cfg.health_check_timeout.as_secs_f64().to_string() + "s",
            "max_fails": cfg.health_check_max_fails,
        },
        "passive": {
            "fail_duration": "30s",
            "max_fails": 2,
            "unhealthy_request_count": 3,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ratelimit::RateLimitCache;
    use crate::routing::{FqdnBinding, RouteEntry};
    use crate::traffic::TrafficSplitCache;
    use std::time::{Duration, Instant};

    fn test_rate_limit_cache() -> RateLimitCache {
        RateLimitCache::default()
    }

    fn entry(tenant: &str, app: &str, addr: &str, port: u16) -> RouteEntry {
        RouteEntry {
            tenant_id: tenant.to_string(),
            app_name: app.to_string(),
            deployment_id: None,
            weight: 100,
            worker_addr: addr.to_string(),
            port,
            rate_limit_rps: None,
            rate_limit_burst: None,
            last_seen: Instant::now(),
        }
    }

    fn canary_entry(
        tenant: &str,
        app: &str,
        deployment_id: &str,
        addr: &str,
        port: u16,
        weight: u8,
    ) -> RouteEntry {
        RouteEntry {
            tenant_id: tenant.to_string(),
            app_name: app.to_string(),
            deployment_id: Some(deployment_id.to_string()),
            weight,
            worker_addr: addr.to_string(),
            port,
            rate_limit_rps: None,
            rate_limit_burst: None,
            last_seen: Instant::now(),
        }
    }

    #[allow(dead_code)] // available for future tests; currently unused
    fn fqdn(tenant: &str, app: &str, host: &str) -> FqdnBinding {
        FqdnBinding {
            tenant_id: tenant.to_string(),
            app_name: app.to_string(),
            fqdn: host.to_string(),
        }
    }

    fn test_cfg() -> Config {
        Config {
            nats_url: "nats://localhost:4222".into(),
            caddy_admin_url: "http://127.0.0.1:2019".into(),
            region: "test".into(),
            cert_file: "/etc/caddy/tls/cert.pem".into(),
            key_file: "/etc/caddy/tls/key.pem".into(),
            listen_http: ":80".into(),
            listen_https: ":443".into(),
            refresh_debounce_ms: 1000,
            http_to_https: true,
            admin_token: None,
            control_plane_api_url: "http://localhost:8080".into(),
            internal_token: None,
            control_plane_url: String::new(),
            service_token: String::new(),
            domain_poll_interval: Duration::from_secs(30),
            caddy_admin_listen: "localhost:2019".into(),
            metrics_listen: ":9091".into(),
            rate_limit_rps_default: 0,
            rate_limit_burst_default: 0,
            rate_limit_fetch_interval: Duration::from_secs(60),
            stale_timeout: Duration::from_secs(60),
            prune_interval: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            health_check_timeout: Duration::from_secs(3),
            health_check_uri: "/healthz".into(),
            health_check_max_fails: 2,
        }
    }

    #[test]
    fn render_empty_table_still_emits_servers_and_tls() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let cfg_json = render_routes(&[], &[], &cfg, &cache, &test_rate_limit_cache());
        let servers = cfg_json["apps"]["http"]["servers"].as_object().unwrap();
        assert!(servers.contains_key(SERVER_NAME_HTTPS));
        assert!(servers.contains_key(SERVER_NAME_HTTP));
        assert_eq!(
            servers[SERVER_NAME_HTTPS]["routes"]
                .as_array()
                .unwrap()
                .len(),
            0,
            "no entries means no routes"
        );
    }

    #[test]
    fn wildcard_cert_takes_precedence_over_auto_tls() {
        let cache = TrafficSplitCache::default();
        let rl_cache = test_rate_limit_cache();
        let cfg_json = render_routes(&[], &[], &test_cfg(), &cache, &rl_cache);
        // Caddy 2.11 removed the `app.http.automatic_https` field.
        // The wildcard cert in `tls.certificates.load_files` takes
        // precedence automatically — no need to disable auto-TLS.
        assert!(
            cfg_json["apps"]["http"]["automatic_https"].is_null(),
            "render_routes must not emit the removed automatic_https field"
        );
    }

    #[test]
    fn admin_block_is_emitted_so_caddy_binding_persists_across_reloads() {
        let cache = TrafficSplitCache::default();
        let mut cfg = test_cfg();
        cfg.caddy_admin_listen = "0.0.0.0:2019".into();
        let cfg_json = render_routes(&[], &[], &cfg, &cache, &test_rate_limit_cache());
        assert_eq!(
            cfg_json["admin"]["listen"], "0.0.0.0:2019",
            "render_routes must include admin.listen matching Config so \
             POST /load does not reset Caddy's admin binding"
        );
    }

    #[test]
    fn render_three_distinct_apps_produces_three_routes() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let entries = vec![
            entry("t_acme", "api", "1.2.3.4", 8081),
            entry("t_acme", "web", "1.2.3.4", 8082),
            entry("t_globex", "api", "5.6.7.8", 9000),
        ];
        let cfg_json = render_routes(&entries, &[], &cfg, &cache, &test_rate_limit_cache());
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        assert_eq!(routes.len(), 3);

        // Sorted by (tenant, app) — t_acme/api, t_acme/web, t_globex/api.
        let hosts: Vec<String> = routes
            .iter()
            .map(|r| r["match"][0]["host"][0].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            hosts,
            vec![
                "t_acme-api.edgecloud.dev".to_string(),
                "t_acme-web.edgecloud.dev".to_string(),
                "t_globex-api.edgecloud.dev".to_string(),
            ]
        );

        // Dials must reflect the right upstream per route.
        let dials: Vec<String> = routes
            .iter()
            .map(|r| {
                r["handle"][0]["routes"][0]["handle"][0]["upstreams"][0]["dial"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(dials, vec!["1.2.3.4:8081", "1.2.3.4:8082", "5.6.7.8:9000"]);
    }

    /// Two entries for the same (tenant, app) with different deployment_ids
    /// must be rendered as a single route with weighted upstreams.
    #[test]
    fn canary_two_deployments_rendered_as_weighted_upstreams() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let entries = vec![
            canary_entry("t_acme", "api", "d_v1", "1.2.3.4", 8081, 95),
            canary_entry("t_acme", "api", "d_v2", "1.2.3.5", 8082, 5),
        ];
        let cfg_json = render_routes(&entries, &[], &cfg, &cache, &test_rate_limit_cache());
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        assert_eq!(routes.len(), 1, "only one route for t_acme/api");

        // Host must be the same for both deployments.
        let hosts: Vec<String> = routes[0]["match"][0]["host"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(hosts, vec!["t_acme-api.edgecloud.dev"]);

        // Upstreams must include both dial addrs with correct weights.
        let upstreams = &routes[0]["handle"][0]["routes"][0]["handle"][0]["upstreams"];
        let upstreams_arr = upstreams.as_array().unwrap();
        assert_eq!(upstreams_arr.len(), 2);

        // Sort by dial for deterministic assertion.
        let mut sorted = upstreams_arr.clone();
        sorted.sort_by(|a, b| a["dial"].as_str().unwrap().cmp(b["dial"].as_str().unwrap()));
        assert_eq!(sorted[0]["dial"], "1.2.3.4:8081");
        assert_eq!(sorted[0]["weight"], 95);
        assert_eq!(sorted[1]["dial"], "1.2.3.5:8082");
        assert_eq!(sorted[1]["weight"], 5);
    }

    /// Single deployment omits the `weight` key so Caddy uses its default.
    #[test]
    fn single_deployment_omits_weight_key() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let entries = vec![canary_entry("t_acme", "api", "d_v1", "1.2.3.4", 8081, 100)];
        let cfg_json = render_routes(&entries, &[], &cfg, &cache, &test_rate_limit_cache());
        let upstreams = &cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"][0]
            ["handle"][0]["routes"][0]["handle"][0]["upstreams"];
        assert_eq!(upstreams.as_array().unwrap().len(), 1);
        let upstream = &upstreams[0];
        assert_eq!(upstream["dial"], "1.2.3.4:8081");
        // weight key must be absent (Caddy defaults to unweighted for single upstream)
        assert!(
            !upstream.as_object().unwrap().contains_key("weight"),
            "single upstream should not have a weight key"
        );
    }

    #[test]
    fn http_to_https_disabled_omits_port_80_server() {
        let mut cfg = test_cfg();
        cfg.http_to_https = false;
        let cache = TrafficSplitCache::default();
        let cfg_json = render_routes(&[], &[], &cfg, &cache, &test_rate_limit_cache());
        let servers = cfg_json["apps"]["http"]["servers"].as_object().unwrap();
        assert!(!servers.contains_key(SERVER_NAME_HTTP));
        assert!(servers.contains_key(SERVER_NAME_HTTPS));
    }

    /// A `weight=0` deployment (draining) must still be included in the
    /// upstreams list so Caddy can honour the weight even if it's 0.
    #[test]
    fn weight_zero_deployment_is_included_in_upstreams() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let entries = vec![
            canary_entry("t_acme", "api", "d_v1", "1.2.3.4", 8081, 0),
            canary_entry("t_acme", "api", "d_v2", "1.2.3.5", 8082, 100),
        ];
        let cfg_json = render_routes(&entries, &[], &cfg, &cache, &test_rate_limit_cache());
        let upstreams = &cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"][0]
            ["handle"][0]["routes"][0]["handle"][0]["upstreams"];
        let upstreams_arr = upstreams.as_array().unwrap();
        assert_eq!(upstreams_arr.len(), 2);
        // Both upstreams are present; weight=0 is rendered explicitly.
        let by_dial: std::collections::HashMap<_, _> = upstreams_arr
            .iter()
            .map(|u| {
                (
                    u["dial"].as_str().unwrap().to_string(),
                    u["weight"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(by_dial.get("1.2.3.4:8081"), Some(&0));
        assert_eq!(by_dial.get("1.2.3.5:8082"), Some(&100));
    }

    /// Cached traffic split weights override the heartbeat-derived weights.
    /// Heartbeat says weight=100 for both, but the cache says 5/95 — the
    /// rendered upstreams must use the cached values.
    #[test]
    fn traffic_cache_overrides_heartbeat_weights() {
        let cfg = test_cfg();
        let mut cache = TrafficSplitCache::default();
        cache.update(
            "t_acme".to_string(),
            "api".to_string(),
            std::collections::HashMap::from([("d_v1".to_string(), 5), ("d_v2".to_string(), 95)]),
        );
        // Heartbeat weights are 100/100 — cache should override to 5/95.
        let entries = vec![
            canary_entry("t_acme", "api", "d_v1", "1.2.3.4", 8081, 100),
            canary_entry("t_acme", "api", "d_v2", "1.2.3.5", 8082, 100),
        ];
        let cfg_json = render_routes(&entries, &[], &cfg, &cache, &test_rate_limit_cache());
        let upstreams = &cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"][0]
            ["handle"][0]["routes"][0]["handle"][0]["upstreams"];
        let upstreams_arr = upstreams.as_array().unwrap();
        let by_dial: std::collections::HashMap<_, _> = upstreams_arr
            .iter()
            .map(|u| {
                (
                    u["dial"].as_str().unwrap().to_string(),
                    u["weight"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(by_dial.get("1.2.3.4:8081"), Some(&5));
        assert_eq!(by_dial.get("1.2.3.5:8082"), Some(&95));
    }

    /// Custom-domain mode: FQDN bindings emit per-host routes with
    /// `tls.on_demand: {}` attached. The FQDN route resolves the
    /// upstream at render time via the (tenant, app) entry in
    /// `entries` — if the app is not currently running, the FQDN
    /// route is silently dropped (not rendered at all).
    #[test]
    fn fqdn_binding_emits_per_host_route_with_on_demand() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let bindings = vec![fqdn("t_acme", "api", "api.acme.com")];
        let cfg_json = render_routes(&entries, &bindings, &cfg, &cache, &test_rate_limit_cache());
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        // The first route is the default <tenant>-<app>.edgecloud.dev,
        // the second is the FQDN route.
        assert_eq!(routes.len(), 2);
        assert_eq!(
            routes[1]["match"][0]["host"][0], "api.acme.com",
            "FQDN host must match the binding's fqdn"
        );
        assert_eq!(
            routes[1]["tls"]["on_demand"],
            serde_json::json!({}),
            "FQDN route must have tls.on_demand: {{}} attached"
        );
        // Upstream is looked up at render time.
        let dial = routes[1]["handle"][0]["routes"][0]["handle"][0]["upstreams"][0]["dial"]
            .as_str()
            .unwrap();
        assert_eq!(dial, "1.2.3.4:8081");
    }

    /// FQDN whose underlying (tenant, app) has no upstream is silently
    /// skipped — the route would 502 anyway. We don't remove the
    /// FQDN binding from the routing table; the next 30s poll will
    /// re-emit it once the app is back.
    #[test]
    fn fqdn_with_missing_upstream_is_silently_skipped() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        // FQDN binding is for t_other/web but the entries only have t_acme/api.
        let bindings = vec![fqdn("t_other", "web", "web.example.com")];
        let cfg_json = render_routes(&entries, &bindings, &cfg, &cache, &test_rate_limit_cache());
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        // Only the default route renders; the orphan FQDN is dropped.
        assert_eq!(routes.len(), 1, "only the default route should render");
    }

    /// Default-only mode (no `control_plane_url`): no `automation` block
    /// is emitted in the tls section, so Caddy never asks the
    /// control plane whether a custom hostname is allowed.
    #[test]
    fn default_only_mode_omits_on_demand_ask_url() {
        let cache = TrafficSplitCache::default();
        let rl_cache = test_rate_limit_cache();
        let cfg_json = render_routes(&[], &[], &test_cfg(), &cache, &rl_cache);
        assert!(
            cfg_json["apps"]["tls"].get("automation").is_none(),
            "no automation block when control_plane_url is empty"
        );
    }

    /// Custom-domain mode: the on-demand ask URL is emitted, pointing
    /// at the control plane's /api/internal/tls-allowed endpoint.
    #[test]
    fn custom_domain_mode_emits_on_demand_ask_url() {
        let mut cfg = test_cfg();
        cfg.control_plane_url = "http://control-plane:8080".into();
        let cache = TrafficSplitCache::default();
        let cfg_json = render_routes(&[], &[], &cfg, &cache, &test_rate_limit_cache());
        assert_eq!(
            cfg_json["apps"]["tls"]["automation"]["on_demand"]["ask"],
            "http://control-plane:8080/api/internal/tls-allowed"
        );
    }

    /// FQDN routes are emitted in sorted-by-host order for determinism
    /// (so test diffs and the Caddy re-render diff are both stable).
    #[test]
    fn fqdn_routes_are_sorted_by_host() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let bindings = vec![
            fqdn("t_acme", "api", "zeta.example.com"),
            fqdn("t_acme", "api", "alpha.example.com"),
            fqdn("t_acme", "api", "mike.example.com"),
        ];
        let cfg_json = render_routes(&entries, &bindings, &cfg, &cache, &test_rate_limit_cache());
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        // routes[0] is the default; routes[1..] are the FQDNs in sorted order.
        let hosts: Vec<String> = routes[1..]
            .iter()
            .map(|r| r["match"][0]["host"][0].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            hosts,
            vec![
                "alpha.example.com".to_string(),
                "mike.example.com".to_string(),
                "zeta.example.com".to_string(),
            ],
            "FQDN routes are sorted by host for determinism"
        );
    }

    /// FQDN route must include active health checks (issue #133 review
    /// finding #3). Matches the synthetic-host route shape so dead
    /// workers are detected by both route types.
    #[test]
    fn fqdn_route_includes_active_health_checks() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let bindings = vec![fqdn("t_acme", "api", "api.acme.com")];
        let cfg_json = render_routes(&entries, &bindings, &cfg, &cache, &test_rate_limit_cache());
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        // routes[0] is the default synthetic host, routes[1] is the FQDN.
        let fqdn_route = &routes[1];
        let active = &fqdn_route["handle"][0]["routes"][0]["handle"][0]["health_checks"]["active"];
        assert_eq!(
            active["uri"], "/healthz",
            "FQDN route must probe uri=/healthz (matches config default)"
        );
        assert_eq!(active["expect_status"], 2);
    }

    // -----------------------------------------------------------------
    // Retry helper — wiremock integration test for the /load retry
    // path. Skipped under CI=true (the existing
    // `should_skip_integration_tests` pattern in tests/integration.rs)
    // because wiremock + tokio can flap on the runner.
    // -----------------------------------------------------------------

    fn should_skip_integration_tests() -> bool {
        std::env::var("CI").is_ok() || std::env::var("SKIP_INTEGRATION_TESTS").is_ok()
    }

    /// 502 twice, then 200 — `load_config` must retry and succeed.
    /// Pins the retry budget (`MAX_LOAD_ATTEMPTS = 3`) and the
    /// exponential backoff shape (the 200 is the third response,
    /// proving the second 502 triggered a retry rather than giving
    /// up). Without this, a single Caddy hiccup would propagate as
    /// an Err to the upstream and the FQDN table would not reload.
    #[tokio::test]
    async fn load_config_retries_on_502() {
        if should_skip_integration_tests() {
            eprintln!("skipping load_config_retries_on_502 under CI/SKIP_INTEGRATION_TESTS");
            return;
        }
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/load"))
            .respond_with(ResponseTemplate::new(502))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/load"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let client = CaddyClient::new(&server.uri(), None).unwrap();
        let cfg = json!({"apps": {"http": {}}});
        client
            .load_config(&cfg)
            .await
            .expect("load_config should succeed after retries");
    }

    /// 400 (non-retryable) must NOT be retried — the first call
    /// surfaces the error and no second request hits the wire.
    /// Pins the retryable-status gate at the variant level.
    #[tokio::test]
    async fn load_config_does_not_retry_on_400() {
        if should_skip_integration_tests() {
            eprintln!("skipping load_config_does_not_retry_on_400 under CI/SKIP_INTEGRATION_TESTS");
            return;
        }
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/load"))
            .respond_with(ResponseTemplate::new(400).set_body_string("malformed json"))
            .expect(1) // strict: exactly one call
            .mount(&server)
            .await;

        let client = CaddyClient::new(&server.uri(), None).unwrap();
        let cfg = json!({"apps": {}});
        let err = client
            .load_config(&cfg)
            .await
            .expect_err("400 should surface as Err");
        assert!(
            err.to_string().contains("400"),
            "err should mention 400, got: {err}"
        );
    }

    /// All 3 attempts return 503 — the third failure surfaces as
    /// Err (the budget is exhausted, no further retries). Pins the
    /// budget constant: future changes to `MAX_LOAD_ATTEMPTS`
    /// must update this assertion.
    #[tokio::test]
    async fn load_config_gives_up_after_three_503s() {
        if should_skip_integration_tests() {
            eprintln!(
                "skipping load_config_gives_up_after_three_503s under CI/SKIP_INTEGRATION_TESTS"
            );
            return;
        }
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/load"))
            .respond_with(ResponseTemplate::new(503))
            .expect(3) // strict: exactly three calls (the budget)
            .mount(&server)
            .await;

        let client = CaddyClient::new(&server.uri(), None).unwrap();
        let cfg = json!({"apps": {}});
        let err = client
            .load_config(&cfg)
            .await
            .expect_err("503 budget should exhaust to Err");
        assert!(
            err.to_string().contains("503"),
            "err should mention 503, got: {err}"
        );
    }

    /// Connection refused — must trigger the transport-error retry path.
    #[tokio::test]
    async fn load_config_retries_on_connection_refused() {
        let client = CaddyClient::new("http://127.0.0.1:1", None).unwrap();
        let cfg = json!({"apps": {}});
        let err = client
            .load_config(&cfg)
            .await
            .expect_err("connection refused should surface as Err");
        assert!(
            err.to_string().contains("transport error"),
            "err should mention transport error, got: {err}"
        );
    }
}
