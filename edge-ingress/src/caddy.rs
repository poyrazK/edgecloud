//! Caddy admin-API client and Caddyfile-JSON renderer.
//! Caddy admin-API client and Caddyfile-JSON renderer.
//!
//! Caddy exposes a JSON admin API on a configurable port (default `:2019`).
//! We render the full Caddyfile-JSON in Rust and `POST /load` it on every
//! routing change. For incremental updates we use `@id` annotations on
//! individual route objects and route-level `PUT /id/<id>` / `DELETE /id/<id>`
//! operations — see `CaddyClient::upsert_route` and `delete_route`.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use tracing::warn;

use crate::config::{ingress_host, Config};
use crate::l4::L4RouteEntry;
use crate::quota::QuotaCache;
use crate::ratelimit::RateLimitCache;
use crate::routing::{FqdnBinding, RouteEntry};
use crate::tenant_ratelimit::TenantRateLimitCache;
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

    /// Upsert a single route object by its `@id`. Caddy's admin API
    /// handles `PUT /id/<id>` which creates or replaces the route.
    pub async fn upsert_route(&self, route: &Value) -> Result<()> {
        let id = route
            .get("@id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("route object missing @id"))?;
        let url = format!("{}/id/{}", self.admin_url, encode_url_path(id));
        put_with_retry(&self.http, &url, self.token.as_deref(), route).await
    }

    /// Delete a single route by its `@id`.
    pub async fn delete_route(&self, id: &str) -> Result<()> {
        let url = format!("{}/id/{}", self.admin_url, encode_url_path(id));
        delete_with_retry(&self.http, &url, self.token.as_deref()).await
    }
}

/// URL-encode a route ID for use in Caddy admin API paths.
/// Caddy `/id/` endpoints expect path-safe encoding.
fn encode_url_path(id: &str) -> String {
    id.replace('%', "%25")
        .replace(':', "%3A")
        .replace('/', "%2F")
        .replace('#', "%23")
        .replace('?', "%3F")
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

/// PUT `body` to `url`, retrying on transient failures.
/// Same retry semantics as `post_with_retry`.
async fn put_with_retry(
    client: &Client,
    url: &str,
    token: Option<&str>,
    body: &Value,
) -> Result<()> {
    let mut delay = INITIAL_BACKOFF;
    for attempt in 1..=MAX_LOAD_ATTEMPTS {
        let mut req = client.put(url).json(body);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                if attempt < MAX_LOAD_ATTEMPTS {
                    warn!(
                        attempt,
                        max = MAX_LOAD_ATTEMPTS,
                        err = %e,
                        "Caddy route PUT transport error, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_BACKOFF);
                    continue;
                }
                return Err(anyhow!(
                    "Caddy route PUT transport error after {attempt} attempts: {e}"
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
                "Caddy route PUT transient failure, retrying"
            );
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_BACKOFF);
            continue;
        }
        return Err(anyhow!(
            "Caddy route PUT returned {status} after {attempt} attempt(s): {body_txt}"
        ));
    }
    unreachable!("loop always returns or continues")
}

/// DELETE `url`, retrying on transient failures.
async fn delete_with_retry(client: &Client, url: &str, token: Option<&str>) -> Result<()> {
    let mut delay = INITIAL_BACKOFF;
    for attempt in 1..=MAX_LOAD_ATTEMPTS {
        let mut req = client.delete(url);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                if attempt < MAX_LOAD_ATTEMPTS {
                    warn!(
                        attempt,
                        max = MAX_LOAD_ATTEMPTS,
                        err = %e,
                        "Caddy route DELETE transport error, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_BACKOFF);
                    continue;
                }
                return Err(anyhow!(
                    "Caddy route DELETE transport error after {attempt} attempts: {e}"
                ));
            }
        };
        let status = resp.status();
        if status.is_success() || status.as_u16() == 404 {
            // 404 means the route doesn't exist — consider that a success
            // since our goal was to ensure it's absent.
            return Ok(());
        }
        let retryable = is_retryable_status(status);
        let body_txt = resp.text().await.unwrap_or_default();
        if retryable && attempt < MAX_LOAD_ATTEMPTS {
            warn!(
                attempt,
                max = MAX_LOAD_ATTEMPTS,
                status = status.as_u16(),
                "Caddy route DELETE transient failure, retrying"
            );
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_BACKOFF);
            continue;
        }
        return Err(anyhow!(
            "Caddy route DELETE returned {status} after {attempt} attempt(s): {body_txt}"
        ));
    }
    unreachable!("loop always returns or continues")
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
    quota_cache: &QuotaCache,
    tenant_rate_limit_cache: &TenantRateLimitCache,
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

    // Issue #420: snapshot the over-cap tenant set once at the top so the
    // `routes` iterator below stays pure. We use it to (a) prepend a
    // tenant-wide `static_response` 402 block in front of every host route
    // for that tenant, and (b) skip FQDN rendering for tenants in the set
    // (the 402 block above also covers the FQDN since the FQDN is bound
    // to a (tenant, app) tuple that has its own host route — but we
    // additionally render a wildcard 402 to be safe for tenants with
    // FQDNs that don't appear in the snapshot).
    let over_cap_tenants: std::collections::HashSet<&str> = group_keys
        .iter()
        .map(|(t, _)| *t)
        .filter(|t| quota_cache.is_over_cap(t))
        .collect();

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
                "@id": ingress_host(tenant_id, app_name),
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

    // Issue #420 — quota enforcement at the edge. For each tenant that
    // the quota cache reports as over cap, prepend a tenant-wide
    // `static_response` 402 route. Caddy's matcher picks the first
    // terminal route that matches, so any host route under this tenant
    // short-circuits with 402 + Retry-After:3600 BEFORE the
    // reverse_proxy ever fires. We generate one tenant-wide block
    // (matched by host suffix on the `*.edgecloud.dev` wildcard cert)
    // rather than per-host blocks to avoid duplication — the
    // subroute under it covers all apps for the tenant.
    //
    // Fail-open semantics: when the quota cache has no entry for a
    // tenant (cache cold-start or transient CP outage), `is_over_cap`
    // returns false and we do NOT inject the 402 — the previous
    // (no-quota-block) Caddy config is preserved.
    //
    // For FQDN-bound tenants, the 402 block needs to match the FQDN
    // string, not the synthetic host. We render a per-tenant FQDN 402
    // block below when iterating `fqdns`. The synthetic-host 402
    // above covers traffic to `<tenant>-<app>.edgecloud.dev`.
    let mut quota_402_routes: Vec<Value> = Vec::new();
    for tenant_id in &over_cap_tenants {
        quota_402_routes.push(json!({
            "@id": format!("{}:quota-402-synthetic", tenant_id),
            // Tenant id is `t_<slug>` (e.g. `t_acme`) and only
            // contains `[a-z0-9_]` per the control-plane validation
            // (see `validatePathComponent`). The synthetic host
            // pattern is `<tenant>-<app>.edgecloud.dev`. Caddy's
            // `host_regexp` uses RE2; we anchor with `^` and `$`
            // to avoid matching unrelated tenants. We place this
            // block BEFORE the per-host reverse_proxy routes (see
            // `routes.prepend` below) so it short-circuits first.
            "match": [{
                "host_regexp": format!(
                    "^{}-[^.]+\\.{}$",
                    tenant_id,
                    crate::config::INGRESS_HOST_SUFFIX,
                )
            }],
            "handle": [{
                "handler": "static_response",
                "status_code": 402,
                "body": "Payment Required",
                "headers": {
                    "Retry-After": ["3600"]
                }
            }],
            "terminal": true
        }));
    }

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

    // Prepend the synthetic-host 402 blocks. They are terminal and
    // listed first so Caddy's route matcher short-circuits them
    // before evaluating per-app reverse_proxy routes below.
    let quota_402_route_count = quota_402_routes.len();
    routes = quota_402_routes.into_iter().chain(routes).collect();

    // Issue #305 sub-feature #1 — per-tenant data-plane rate limit.
    // For every tenant the TenantRateLimitCache reports as having a
    // configured cap (state.rps > 0), insert a `rate_limit` route keyed
    // by host_regexp matching the `<tenant>-<app>.edgecloud.dev`
    // synthetic-host pattern. `terminal: false` so per-app rate-limit
    // handlers layered below still apply when a request passes through.
    // The cache treats `None` (unknown tenant) and `rps == 0` as
    // "no cap" — fail-open, same shape as the quota 402 cache.
    //
    // Insertion order: AFTER quota_402_routes (so 402 short-circuits
    // first when the tenant is over cap) and BEFORE per-app routes (so
    // per-tenant caps apply before the per-app cap chain inside the
    // handle).
    //
    // TODO(issue #NNN sub-feature #2): render the concurrent_limit
    // via a custom Caddy module once the platform-side primitive
    // exists. The cache already carries concurrent_limit for that
    // follow-up.
    //
    // TODO(issue #NNN sub-feature #3): render bandwidth_bps via
    // Caddy 2.8+ `rate_limit.bandwidth` once the deployment upgrades.
    // The cache carries bandwidth_bps for that follow-up.
    let mut tenant_rl_routes: Vec<Value> = Vec::new();
    for (tenant_id, state) in tenant_rate_limit_cache.active_caps() {
        let burst = if state.burst > 0 {
            state.burst
        } else {
            state.rps
        };
        tenant_rl_routes.push(json!({
            "@id": format!("tenant-rl:{}", tenant_id),
            "match": [{
                "host_regexp": format!(
                    "^{}-[^.]+\\.{}$",
                    tenant_id,
                    crate::config::INGRESS_HOST_SUFFIX,
                )
            }],
            "handle": [{
                "handler": "rate_limit",
                "key": format!("tenant-{}", tenant_id),
                "rates": {
                    "rps": state.rps,
                    "burst": burst,
                },
            }],
            "terminal": false,
        }));
    }
    if !tenant_rl_routes.is_empty() {
        // Splice: keep the first `quota_402_route_count` entries, then
        // the tenant_rl_routes, then the rest (per-app routes).
        let per_app_start = quota_402_route_count;
        let per_app = routes.split_off(per_app_start);
        routes.extend(tenant_rl_routes);
        routes.extend(per_app);
    }

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

        // Issue #420 — quota 402 for FQDN routes. The synthetic-host
        // 402 block above does NOT match custom FQDNs (the host_regexp
        // is anchored to `<tenant>-<app>.edgecloud.dev`). When the
        // tenant is over cap, emit a per-FQDN 402 block in place of
        // the reverse_proxy and `continue` so we don't also emit a
        // dead reverse_proxy. Caddy's route-list order makes the
        // 402 the first match for this host.
        if over_cap_tenants.contains(b.tenant_id.as_str()) {
            routes.push(json!({
                "@id": format!("{}:quota-402-fqdn", b.fqdn),
                "match": [{"host": [b.fqdn.clone()]}],
                "handle": [{
                    "handler": "static_response",
                    "status_code": 402,
                    "body": "Payment Required",
                    "headers": {
                        "Retry-After": ["3600"]
                    }
                }],
                "terminal": true,
                "tls": {"on_demand": {}}
            }));
            continue;
        }

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
            "@id": b.fqdn,
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

    // Prepend a global per-IP rate limit route when configured.
    // This runs before per-app routing so a single abusive IP gets
    // 429'd regardless of which app it's hitting.
    if cfg.per_ip_rps > 0 {
        let burst = if cfg.per_ip_burst > 0 {
            cfg.per_ip_burst
        } else {
            cfg.per_ip_rps
        };
        let global_rl_route = json!({
            "match": [{"remote_ip": {"ranges": ["0.0.0.0/0"]}}],
            "handle": [{
                "handler": "rate_limit",
                "rates": { "rps": cfg.per_ip_rps, "burst": burst },
                "key": "{http.request.remote_host}",
            }],
            "terminal": false
        });
        routes.insert(0, global_rl_route);
    }

    // Issue #305 sub-feature #4 — global platform-wide RPS cap. Enforced
    // per Caddy replica: with N ingress replicas, the effective cap is
    // N × global_rate_limit_rps. Multi-replica NATS aggregation is a
    // separate follow-up. Same shape as the per-IP prepend above:
    // matches both `0.0.0.0/0` (IPv4) AND `::/0` (IPv6) — review
    // finding: a single IPv4-only range would let IPv6 traffic
    // bypass the global cap entirely (Cloudflare origin, Fly.io,
    // etc. increasingly IPv6-only). `terminal: false` so the
    // per-tenant + per-app rate_limit handlers layered below still
    // apply.
    if cfg.global_rate_limit_rps > 0 {
        let burst = if cfg.global_rate_limit_burst > 0 {
            cfg.global_rate_limit_burst
        } else {
            cfg.global_rate_limit_rps
        };
        let global_rl_route = json!({
            "@id": "global-rate-limit",
            "match": [{"remote_ip": {"ranges": ["0.0.0.0/0", "::/0"]}}],
            "handle": [{
                "handler": "rate_limit",
                "rates": { "rps": cfg.global_rate_limit_rps, "burst": burst },
                "key": "global-platform",
            }],
            "terminal": false
        });
        routes.insert(0, global_rl_route);
    }

    let mut servers = serde_json::Map::new();
    let mut edge_https = serde_json::Map::new();
    edge_https.insert("listen".to_string(), json!([cfg.listen_https]));
    edge_https.insert("routes".to_string(), json!(routes));
    if cfg.max_conns > 0 {
        edge_https.insert("max_conns".to_string(), json!(cfg.max_conns));
    }
    if cfg.max_conns_per_ip > 0 {
        edge_https.insert("max_conns_per_ip".to_string(), json!(cfg.max_conns_per_ip));
    }
    servers.insert(SERVER_NAME_HTTPS.to_string(), Value::Object(edge_https));
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
    // Issue #438 — when TLS_CERT_FILE_2 / TLS_KEY_FILE_2 are set, load
    // the multi-label `*.*.edgecloud.dev` wildcard cert alongside the
    // single-level `*.edgecloud.dev` wildcard so dotted app names like
    // `myapp.v2` resolve via TLS without depending on ACME on-demand.
    // Without the second cert, dotted hosts still route (the
    // `host_regexp` matchers catch them) but the TLS handshake falls
    // through to the on-demand issuer on first hit.
    let mut load_files = vec![json!({"certificate": cfg.cert_file, "key": cfg.key_file})];
    if let (Some(cert2), Some(key2)) = (&cfg.cert_file_2, &cfg.key_file_2) {
        load_files.push(json!({"certificate": cert2, "key": key2}));
    }
    tls.insert(
        "certificates".to_string(),
        json!({
            "load_files": load_files
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

/// Diff result for routing entries: (added with item, removed IDs, changed with item).
type DiffResult<'a> = (
    Vec<(&'a RouteEntry, serde_json::Value)>,
    Vec<String>,
    Vec<(&'a RouteEntry, serde_json::Value)>,
);

/// Compute the delta between two routing table snapshots.
///
/// Returns `(added, removed, changed)` where:
/// - `added`: routes present in `curr` but not in `prev`
/// - `removed`: route IDs present in `prev` but not in `curr`
/// - `changed`: routes present in both but with different content
///   (worker_addr, port, or weight), paired with their rendered JSON
///
/// Comparison uses `RouteEntry::route_id()` as the stable key.
/// Two entries are considered "changed" if any routing-relevant
/// field differs (worker_addr, port, weight, rate_limit_rps/burst).
pub(crate) fn diff_routes<'a>(prev: &'a [RouteEntry], curr: &'a [RouteEntry]) -> DiffResult<'a> {
    let prev_by_id: HashMap<String, &RouteEntry> = prev.iter().map(|e| (e.route_id(), e)).collect();
    let curr_by_id: HashMap<String, &RouteEntry> = curr.iter().map(|e| (e.route_id(), e)).collect();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for (id, entry) in &curr_by_id {
        if !prev_by_id.contains_key(id) {
            added.push((*entry, json!({})));
        }
    }
    for (id, prev_entry) in &prev_by_id {
        match curr_by_id.get(id.as_str()) {
            None => {
                removed.push((*id).to_string());
            }
            Some(curr_entry) => {
                if prev_entry.worker_addr != curr_entry.worker_addr
                    || prev_entry.port != curr_entry.port
                    || prev_entry.weight != curr_entry.weight
                    || prev_entry.rate_limit_rps != curr_entry.rate_limit_rps
                    || prev_entry.rate_limit_burst != curr_entry.rate_limit_burst
                {
                    changed.push((*curr_entry, json!({})));
                }
            }
        }
    }

    (added, removed, changed)
}

/// Compute the delta between two FQDN binding lists.
/// Returns `(added_fqdns, removed_fqdns)` — FQDNs are their own IDs.
pub(crate) fn diff_fqdns(prev: &[FqdnBinding], curr: &[FqdnBinding]) -> (Vec<String>, Vec<String>) {
    let prev_set: std::collections::HashSet<&str> = prev.iter().map(|b| b.fqdn.as_str()).collect();
    let curr_set: std::collections::HashSet<&str> = curr.iter().map(|b| b.fqdn.as_str()).collect();

    let added: Vec<String> = curr
        .iter()
        .filter(|b| !prev_set.contains(b.fqdn.as_str()))
        .map(|b| b.fqdn.clone())
        .collect();
    let removed: Vec<String> = prev
        .iter()
        .filter(|b| !curr_set.contains(b.fqdn.as_str()))
        .map(|b| b.fqdn.clone())
        .collect();

    (added, removed)
}

// ── L4 (raw-TCP) rendering ───────────────────────────────────────────
//
// Issue #548. Caddy's `mholt/caddy-l4` plugin (the only way to proxy
// raw TCP with Caddy) does NOT support per-id `PUT/DELETE /id/<id>`
// for layer-4 servers — only the top-level `POST /load` accepts a
// full `apps.layer4` config. So L4 routing changes always go through
// the full `render_full` path. The HTTP tree still has incremental
// per-id patches for the small-change fast path; only L4 is
// forced-full.
//
// `apps.layer4.servers.<name>` JSON shape per `mholt/caddy-l4` README:
//   listen:    [":31000"]
//   routes[0]:
//     match:    [{"subroute": "..."}]
//     handle[0]:
//       handler:   "proxy"
//       upstreams: [{"dial": "10.0.0.1:8082"}]
//
// `subroute` is left implicit by using `match: []` (matches every
// connection) and the proxy handler is the only handler.

/// Build the Caddy `apps.layer4` tree for `entries`. Returns a
/// complete Caddyfile-JSON payload (admin + apps.layer4); the HTTP
/// tree is intentionally absent so the caller can merge it in via
/// [`render_full`].
pub fn render_l4_routes(entries: &[L4RouteEntry], cfg: &Config, quota_cache: &QuotaCache) -> Value {
    let mut root = serde_json::Map::new();
    root.insert(
        "admin".to_string(),
        json!({"listen": cfg.caddy_admin_listen}),
    );

    // Group by public_port (which is also the Caddy server_id —
    // see `L4RouteEntry::server_id`). Each public port owns one
    // Caddy server in the layer4 tree.
    let mut servers = serde_json::Map::new();
    let mut sorted_entries: Vec<&L4RouteEntry> = entries.iter().collect();
    sorted_entries.sort_by_key(|a| a.public_port);

    for entry in &sorted_entries {
        // Build the per-server object. The shape is always:
        //   { listen: [":<port>"], routes: [...] }
        // For over-cap tenants, `routes` is `[]` (close-on-cap —
        // Caddy's layer4 matches nothing when routes is empty and
        // closes the connection). Otherwise `routes` contains a
        // single proxy route.
        let mut server_obj = serde_json::Map::new();
        server_obj.insert(
            "listen".to_string(),
            json!([format!(":{}", entry.public_port)]),
        );
        let routes_value = if quota_cache.is_over_cap(&entry.tenant_id) {
            // Caddy's layer4 matches nothing when `routes: []` and
            // closes the connection immediately — the close-on-cap
            // behaviour from the issue #548 design.
            Value::Array(Vec::new())
        } else {
            let dial = format!("{}:{}", entry.worker_addr, entry.upstream_port);
            json!([{
                "handle": [{
                    "handler": "proxy",
                    "upstreams": [{"dial": dial}],
                }],
            }])
        };
        server_obj.insert("routes".to_string(), routes_value);

        // Issue #548 review finding #32: the per-app DDoS caps were
        // originally emitted as a `connection_policies` block, but
        // mholt/caddy-l4 doesn't recognise the `max_conns_per_app`
        // / `max_conns_per_ip` keys we emitted — the real
        // connection-cap plugin is `caddy-l4`'s `conn_limit`
        // sub-module, which uses a different schema. Until that
        // plugin is wired in (out of v1 scope), emitting a
        // `connection_policies` block creates the illusion of
        // protection while doing nothing. Drop it; leave the
        // `l4_max_conns_*` config fields in place for the future
        // conn_limit plugin to consume without a second config
        // migration. `cfg.l4_max_conns_per_app/_per_ip` are still
        // validated by `test_cfg()` to catch typos at config-load
        // time.
        servers.insert(entry.server_id(), Value::Object(server_obj));
    }

    // If there are no L4 entries, the Caddy admin API still needs
    // an empty `apps.layer4` block (an absent `apps.layer4` means
    // the plugin is not configured and Caddy returns a 404 on
    // /apps/layer4). The block is harmless when empty.
    let mut apps_block = serde_json::Map::new();
    let mut layer4 = serde_json::Map::new();
    layer4.insert("servers".to_string(), Value::Object(servers));
    apps_block.insert("layer4".to_string(), Value::Object(layer4));
    root.insert("apps".to_string(), Value::Object(apps_block));

    Value::Object(root)
}

/// Merge the HTTP tree from [`render_routes`] with the L4 tree from
/// [`render_l4_routes`] into one Caddy admin `/load` payload. The
/// HTTP block is the pre-#548 shape verbatim; the L4 block lives
/// under `apps.layer4` per the `mholt/caddy-l4` JSON schema.
///
/// When `l4_entries` is empty, the L4 block is emitted with
/// `servers: {}` rather than omitted — Caddy's admin API tolerates
/// both, but emitting the empty block is friendlier to operators
/// debugging the live config (`curl localhost:2019/config/apps/layer4`
/// returns `{"servers":{}}` rather than a 404).
///
/// When `http_payload` is from a cache hit (a previous successful
/// push that hasn't been invalidated by an HTTP-side delta), the
/// `admin` key from `http_payload` wins — it's identical between
/// the two renderers by construction.
pub fn render_full(
    http_payload: Value,
    l4_entries: &[L4RouteEntry],
    cfg: &Config,
    quota_cache: &QuotaCache,
) -> Value {
    let l4_payload = render_l4_routes(l4_entries, cfg, quota_cache);

    // Merge the two trees. The HTTP payload is the "base" — we
    // splice the L4 `apps.layer4` block in alongside `apps.http`.
    let mut http_obj = http_payload
        .as_object()
        .cloned()
        .unwrap_or_else(serde_json::Map::new);
    let mut merged_apps = http_obj
        .remove("apps")
        .and_then(|a| a.as_object().cloned())
        .unwrap_or_else(serde_json::Map::new);

    if let Some(l4_apps) = l4_payload.get("apps").and_then(|a| a.as_object()) {
        for (k, v) in l4_apps {
            merged_apps.insert(k.clone(), v.clone());
        }
    }
    http_obj.insert("apps".to_string(), Value::Object(merged_apps));
    Value::Object(http_obj)
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

    /// Default `QuotaCache` for tests: no tenants over cap, so the
    /// renderer does NOT inject any 402 `static_response` blocks. Tests
    /// that want to exercise the 402 path construct a populated cache
    /// directly.
    fn test_quota_cache() -> crate::quota::QuotaCache {
        crate::quota::QuotaCache::default()
    }

    /// Default `TenantRateLimitCache` for tests: empty. Tests that
    /// want to exercise the per-tenant / global rate_limit route
    /// paths populate this directly (Commit 4 of issue #305).
    fn test_tenant_rate_limit_cache() -> crate::tenant_ratelimit::TenantRateLimitCache {
        crate::tenant_ratelimit::TenantRateLimitCache::default()
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
            cert_file_2: None,
            key_file_2: None,
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
            max_conns: 0,
            max_conns_per_ip: 0,
            per_ip_rps: 0,
            per_ip_burst: 0,
            rate_limit_rps_default: 0,
            rate_limit_burst_default: 0,
            rate_limit_fetch_interval: Duration::from_secs(60),
            quota_fetch_interval: Duration::from_secs(30),
            stale_timeout: Duration::from_secs(60),
            prune_interval: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            health_check_timeout: Duration::from_secs(3),
            health_check_uri: "/healthz".into(),
            health_check_max_fails: 2,
            rate_limit_rps_tenant_default: 0,
            rate_limit_burst_tenant_default: 0,
            tenant_rate_limit_fetch_interval: Duration::from_secs(30),
            global_rate_limit_rps: 0,
            global_rate_limit_burst: 0,
            l4_port_range_start: 31000,
            l4_port_range_end: 31999,
            l4_max_conns_per_app: 1000,
            l4_max_conns_per_ip: 100,
            l4_port_cooldown_secs: 60,
        }
    }

    #[test]
    fn render_empty_table_still_emits_servers_and_tls() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let cfg_json = render_routes(
            &[],
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let q_cache = test_quota_cache();
        let cfg_json = render_routes(
            &[],
            &[],
            &test_cfg(),
            &cache,
            &rl_cache,
            &q_cache,
            &test_tenant_rate_limit_cache(),
        );
        // Caddy 2.11 removed the `app.http.automatic_https` field.
        // The wildcard cert in `tls.certificates.load_files` takes
        // precedence automatically — no need to disable auto-TLS.
        assert!(
            cfg_json["apps"]["http"]["automatic_https"].is_null(),
            "render_routes must not emit the removed automatic_https field"
        );
    }

    /// Issue #438 — when `TLS_CERT_FILE_2` / `TLS_KEY_FILE_2` are set,
    /// the multi-label wildcard cert (`*.*.edgecloud.dev`) that covers
    /// dotted app names like `myapp.v2` is appended to
    /// `tls.certificates.load_files` alongside the single-level
    /// `*.edgecloud.dev` cert. Both must be present (one without the
    /// other is a configuration error).
    #[test]
    fn multi_label_wildcard_cert_is_loaded_when_cert_file_2_set() {
        let cache = TrafficSplitCache::default();
        let rl_cache = test_rate_limit_cache();
        let q_cache = test_quota_cache();
        let mut cfg = test_cfg();
        cfg.cert_file_2 = Some("/etc/caddy/tls/cert-multi.pem".into());
        cfg.key_file_2 = Some("/etc/caddy/tls/key-multi.pem".into());
        let cfg_json = render_routes(
            &[],
            &[],
            &cfg,
            &cache,
            &rl_cache,
            &q_cache,
            &test_tenant_rate_limit_cache(),
        );

        let load_files = &cfg_json["apps"]["tls"]["certificates"]["load_files"];
        let arr = load_files
            .as_array()
            .expect("load_files must be an array when cert_file_2 is set");
        assert_eq!(
            arr.len(),
            2,
            "load_files must contain both single- and multi-label wildcard certs"
        );
        assert_eq!(arr[0]["certificate"], "/etc/caddy/tls/cert.pem");
        assert_eq!(arr[0]["key"], "/etc/caddy/tls/key.pem");
        assert_eq!(arr[1]["certificate"], "/etc/caddy/tls/cert-multi.pem");
        assert_eq!(arr[1]["key"], "/etc/caddy/tls/key-multi.pem");
    }

    /// Default config (no second cert) still emits the single-level
    /// wildcard cert. Dotted hosts will fall through to per-route
    /// `tls.on_demand: {}` ACME on first hit.
    #[test]
    fn single_label_only_when_cert_file_2_unset() {
        let cache = TrafficSplitCache::default();
        let rl_cache = test_rate_limit_cache();
        let q_cache = test_quota_cache();
        let cfg_json = render_routes(
            &[],
            &[],
            &test_cfg(),
            &cache,
            &rl_cache,
            &q_cache,
            &test_tenant_rate_limit_cache(),
        );

        let load_files = &cfg_json["apps"]["tls"]["certificates"]["load_files"];
        let arr = load_files
            .as_array()
            .expect("load_files must be an array even without the multi-label cert");
        assert_eq!(
            arr.len(),
            1,
            "load_files must contain only the single-label cert when cert_file_2 is unset"
        );
    }

    #[test]
    fn admin_block_is_emitted_so_caddy_binding_persists_across_reloads() {
        let cache = TrafficSplitCache::default();
        let mut cfg = test_cfg();
        cfg.caddy_admin_listen = "0.0.0.0:2019".into();
        let cfg_json = render_routes(
            &[],
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &[],
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &entries,
            &bindings,
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &entries,
            &bindings,
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        // Only the default route renders; the orphan FQDN is dropped.
        assert_eq!(routes.len(), 1, "only the default route should render");
    }

    /// Quota cache is the source of truth for the 402 injection (issue
    /// #420). This pins the three boundary cases:
    /// * t_a: over_cap=true → a 402 `static_response` block IS emitted
    /// * t_b: over_cap=false → NO 402 block
    /// * t_c: absent from cache (cold start or transient CP outage) → NO 402 block (fail-open)
    ///
    /// We also assert the 402 block is placed BEFORE the per-app
    /// reverse_proxy route so Caddy's matcher short-circuits first.
    #[test]
    fn quota_cache_boundary_drives_402_injection() {
        let cfg = test_cfg();
        let cache = TrafficSplitCache::default();
        let entries = vec![
            entry("t_a", "api", "1.2.3.4", 8081),
            entry("t_b", "api", "5.6.7.8", 8082),
        ];
        let mut q_cache = test_quota_cache();
        q_cache.update(
            "t_a".to_string(),
            crate::quota::QuotaState {
                over_cap: true,
                locked_until: None,
                fetched_at: Some(Instant::now()),
            },
        );
        q_cache.update(
            "t_b".to_string(),
            crate::quota::QuotaState {
                over_cap: false,
                locked_until: None,
                fetched_at: Some(Instant::now()),
            },
        );
        // t_c intentionally not in the cache.

        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &q_cache,
            &test_tenant_rate_limit_cache(),
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();

        // t_a's 402 block must be present and have the right shape.
        let t_a_block = routes
            .iter()
            .find(|r| r["@id"] == "t_a:quota-402-synthetic")
            .expect("t_a quota 402 block must be emitted for over_cap=true");
        assert_eq!(t_a_block["handle"][0]["status_code"], 402);
        assert_eq!(t_a_block["terminal"], true);
        let host_regex = t_a_block["match"][0]["host_regexp"].as_str().unwrap();
        assert!(
            host_regex.starts_with("^t_a-"),
            "host_regexp anchored to tenant id, got {host_regex}"
        );

        // t_b's 402 block must NOT be present (over_cap=false).
        let t_b_block = routes
            .iter()
            .find(|r| r["@id"] == "t_b:quota-402-synthetic");
        assert!(
            t_b_block.is_none(),
            "t_b must not emit a 402 block when over_cap=false"
        );

        // t_c absent from cache → fail-open, NO 402 block.
        let t_c_block = routes
            .iter()
            .find(|r| r["@id"] == "t_c:quota-402-synthetic");
        assert!(
            t_c_block.is_none(),
            "t_c must fail open — not in cache, no 402 block"
        );

        // The 402 block must come BEFORE the per-app reverse_proxy route
        // so Caddy short-circuits the request. routes[0] is the default
        // route; the 402 block follows it; the per-app reverse_proxy
        // routes follow.
        let pos_402 = routes
            .iter()
            .position(|r| r["@id"] == "t_a:quota-402-synthetic")
            .unwrap();
        let app_id = crate::config::ingress_host("t_a", "api");
        let pos_app = routes
            .iter()
            .position(|r| r["@id"] == app_id)
            .unwrap_or_else(|| panic!("per-app reverse_proxy route {app_id} must exist"));
        assert!(
            pos_402 < pos_app,
            "402 block at index {pos_402} must precede reverse_proxy route at {pos_app}"
        );
    }

    /// Default-only mode (no `control_plane_url`): no `automation` block
    /// is emitted in the tls section, so Caddy never asks the
    /// control plane whether a custom hostname is allowed.
    #[test]
    fn default_only_mode_omits_on_demand_ask_url() {
        let cache = TrafficSplitCache::default();
        let rl_cache = test_rate_limit_cache();
        let q_cache = test_quota_cache();
        let cfg_json = render_routes(
            &[],
            &[],
            &test_cfg(),
            &cache,
            &rl_cache,
            &q_cache,
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &[],
            &[],
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &entries,
            &bindings,
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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
        let cfg_json = render_routes(
            &entries,
            &bindings,
            &cfg,
            &cache,
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
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

    // ── DDoS / abuse protection tests ───────────────────────────────

    /// Global per-IP rate limit route is prepended when configured.
    #[test]
    fn global_per_ip_rate_limit_prepended_when_configured() {
        let mut cfg = test_cfg();
        cfg.per_ip_rps = 50;
        cfg.per_ip_burst = 100;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        // First route must be the global per-IP rate limit.
        let first = &routes[0];
        assert_eq!(
            first["handle"][0]["handler"], "rate_limit",
            "first route must be rate_limit handler"
        );
        assert_eq!(first["handle"][0]["rates"]["rps"], 50);
        assert_eq!(first["handle"][0]["rates"]["burst"], 100);
        assert_eq!(
            first["handle"][0]["key"], "{http.request.remote_host}",
            "per-IP rate limit must key on remote_host"
        );
        assert!(
            first["match"][0]["remote_ip"]["ranges"]
                .as_array()
                .is_some(),
            "must have remote_ip match"
        );
        // The app route should still be there as the second route.
        let second = &routes[1];
        assert_eq!(second["match"][0]["host"][0], "t_acme-api.edgecloud.dev");
    }

    /// Global per-IP rate limit is omitted when zero.
    #[test]
    fn global_per_ip_rate_limit_omitted_when_zero() {
        let mut cfg = test_cfg();
        cfg.per_ip_rps = 0;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        // First route must be the app, not a rate limit handler.
        let first = &routes[0];
        assert_ne!(
            first["handle"][0]["handler"].as_str().unwrap_or(""),
            "rate_limit",
            "first route must not be rate_limit when per_ip_rps=0"
        );
        assert_eq!(routes.len(), 1, "only the app route should exist");
    }

    // ── Issue #305 sub-feature #1 (per-tenant RL) + #4 (global RL) ──

    /// Per-tenant rate limit route is emitted when the cache has an
    /// entry with `rps > 0` (issue #305 sub-feature #1).
    #[test]
    fn tenant_rl_cache_injects_per_tenant_route() {
        let mut cfg = test_cfg();
        cfg.global_rate_limit_rps = 0; // disable sub-feature #4 for this test
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let mut tenant_rl_cache = TenantRateLimitCache::default();
        tenant_rl_cache.update(
            "t_acme".into(),
            crate::tenant_ratelimit::TenantRateLimitState {
                rps: 50,
                burst: 100,
                ..Default::default()
            },
        );
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &tenant_rl_cache,
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        // The tenant-rl route should be present, with a host_regexp
        // matching the synthetic-host pattern for t_acme.
        let tenant_rl = routes
            .iter()
            .find(|r| r["@id"] == "tenant-rl:t_acme")
            .expect("per-tenant rate_limit route must exist");
        assert_eq!(tenant_rl["handle"][0]["handler"], "rate_limit");
        assert_eq!(tenant_rl["handle"][0]["rates"]["rps"], 50);
        assert_eq!(tenant_rl["handle"][0]["rates"]["burst"], 100);
        assert_eq!(
            tenant_rl["handle"][0]["key"], "tenant-t_acme",
            "per-tenant rate_limit must key on tenant id"
        );
        let re = tenant_rl["match"][0]["host_regexp"].as_str().unwrap();
        assert_eq!(re, "^t_acme-[^.]+\\.edgecloud.dev$");
        assert_eq!(
            tenant_rl["terminal"], false,
            "per-tenant rate_limit must NOT be terminal so per-app caps layer below"
        );
    }

    /// Per-tenant rate limit route is omitted when the cache is empty
    /// (fail-open: unknown tenant → no route).
    #[test]
    fn tenant_rl_cache_empty_no_route_emitted() {
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let cfg_json = render_routes(
            &entries,
            &[],
            &test_cfg(),
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        assert!(
            routes
                .iter()
                .all(|r| r["@id"].as_str().unwrap_or("") != "tenant-rl:t_acme"),
            "empty tenant-rl cache must NOT emit a per-tenant route"
        );
    }

    /// Per-tenant rate limit route's burst falls back to rps when burst=0.
    #[test]
    fn tenant_rl_route_burst_falls_back_to_rps() {
        let mut cfg = test_cfg();
        cfg.global_rate_limit_rps = 0;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let mut tenant_rl_cache = TenantRateLimitCache::default();
        tenant_rl_cache.update(
            "t_acme".into(),
            crate::tenant_ratelimit::TenantRateLimitState {
                rps: 50,
                burst: 0, // explicitly zero
                ..Default::default()
            },
        );
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &tenant_rl_cache,
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        let tenant_rl = routes
            .iter()
            .find(|r| r["@id"] == "tenant-rl:t_acme")
            .expect("per-tenant rate_limit route must exist");
        assert_eq!(tenant_rl["handle"][0]["rates"]["rps"], 50);
        assert_eq!(
            tenant_rl["handle"][0]["rates"]["burst"], 50,
            "burst must fall back to rps when 0"
        );
    }

    /// Per-tenant RL route is NOT emitted when the cache row has rps=0
    /// (admin-cleared caps must immediately drop the route).
    #[test]
    fn tenant_rl_route_skipped_when_rps_zero() {
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let mut tenant_rl_cache = TenantRateLimitCache::default();
        tenant_rl_cache.update(
            "t_acme".into(),
            crate::tenant_ratelimit::TenantRateLimitState::default(), // all zero
        );
        let cfg_json = render_routes(
            &entries,
            &[],
            &test_cfg(),
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &tenant_rl_cache,
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        assert!(
            routes
                .iter()
                .all(|r| r["@id"].as_str().unwrap_or("") != "tenant-rl:t_acme"),
            "all-zero caps must NOT emit a per-tenant route"
        );
    }

    /// Issue #305 sub-feature #4 — global platform-wide RPS cap is
    /// prepended when configured. Per-replica semantics.
    #[test]
    fn global_rate_limit_prepended_when_configured() {
        let mut cfg = test_cfg();
        cfg.global_rate_limit_rps = 1000;
        cfg.global_rate_limit_burst = 2000;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        let first = &routes[0];
        assert_eq!(first["@id"], "global-rate-limit");
        assert_eq!(first["handle"][0]["handler"], "rate_limit");
        assert_eq!(first["handle"][0]["rates"]["rps"], 1000);
        assert_eq!(first["handle"][0]["rates"]["burst"], 2000);
        assert_eq!(
            first["handle"][0]["key"], "global-platform",
            "global rate limit must key on a static string"
        );
        assert_eq!(
            first["match"][0]["remote_ip"]["ranges"][0], "0.0.0.0/0",
            "global rate limit must match IPv4"
        );
        assert_eq!(
            first["match"][0]["remote_ip"]["ranges"][1], "::/0",
            "global rate limit must match IPv6 (review finding: \
             0.0.0.0/0 alone lets IPv6 traffic bypass the cap)"
        );
        assert_eq!(
            first["terminal"], false,
            "global rate_limit must NOT be terminal so per-tenant/per-app layers apply"
        );
    }

    /// Global RPS matches both IPv4 and IPv6. Review finding:
    /// `0.0.0.0/0` is IPv4-only — IPv6 traffic from Cloudflare
    /// origins, Fly.io, etc. would bypass the global cap entirely
    /// without `::/0` alongside.
    #[test]
    fn global_rate_limit_matches_ipv4_and_ipv6() {
        let mut cfg = test_cfg();
        cfg.global_rate_limit_rps = 100;
        cfg.global_rate_limit_burst = 100;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        let ranges = routes[0]["match"][0]["remote_ip"]["ranges"]
            .as_array()
            .unwrap();
        assert_eq!(
            ranges.len(),
            2,
            "global rate_limit matcher must carry both IPv4 and IPv6 ranges"
        );
        let has_v4 = ranges.iter().any(|v| v == "0.0.0.0/0");
        let has_v6 = ranges.iter().any(|v| v == "::/0");
        assert!(has_v4, "missing 0.0.0.0/0 (IPv4)");
        assert!(has_v6, "missing ::/0 (IPv6)");
    }

    /// Global RPS is omitted when zero (no cap → no route).
    #[test]
    fn global_rate_limit_omitted_when_zero() {
        let mut cfg = test_cfg();
        cfg.global_rate_limit_rps = 0;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        assert!(
            routes
                .iter()
                .all(|r| r["@id"].as_str().unwrap_or("") != "global-rate-limit"),
            "global rate_limit route must NOT be emitted when global_rate_limit_rps=0"
        );
    }

    /// Global RPS burst falls back to rps when burst=0.
    #[test]
    fn global_rate_limit_burst_falls_back_to_rps() {
        let mut cfg = test_cfg();
        cfg.global_rate_limit_rps = 100;
        cfg.global_rate_limit_burst = 0;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        let first = &routes[0];
        assert_eq!(first["handle"][0]["rates"]["rps"], 100);
        assert_eq!(
            first["handle"][0]["rates"]["burst"], 100,
            "burst must fall back to rps when 0"
        );
    }

    /// Per-tenant route + global route + per-app route can coexist.
    /// Pins the insertion order: global first, then per-tenant, then
    /// per-app (terminal=true with its own RL chain).
    #[test]
    fn tenant_global_per_app_coexist_with_correct_order() {
        let mut cfg = test_cfg();
        cfg.global_rate_limit_rps = 1000;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let mut tenant_rl_cache = TenantRateLimitCache::default();
        tenant_rl_cache.update(
            "t_acme".into(),
            crate::tenant_ratelimit::TenantRateLimitState {
                rps: 50,
                burst: 100,
                ..Default::default()
            },
        );
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &tenant_rl_cache,
        );
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        assert_eq!(routes.len(), 3, "global + tenant + app routes");
        assert_eq!(routes[0]["@id"], "global-rate-limit");
        assert_eq!(routes[1]["@id"], "tenant-rl:t_acme");
        assert_eq!(routes[2]["@id"], "t_acme-api.edgecloud.dev");
    }

    /// Connection caps are injected into the server block when configured.
    #[test]
    fn max_conns_injected_into_server_block() {
        let mut cfg = test_cfg();
        cfg.max_conns = 1000;
        cfg.max_conns_per_ip = 50;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let server = &cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS];
        assert_eq!(server["max_conns"], 1000);
        assert_eq!(server["max_conns_per_ip"], 50);
    }

    /// Connection caps are absent from the server block when zero.
    #[test]
    fn max_conns_omitted_when_zero() {
        let mut cfg = test_cfg();
        cfg.max_conns = 0;
        cfg.max_conns_per_ip = 0;
        let entries = vec![entry("t_acme", "api", "1.2.3.4", 8081)];
        let cfg_json = render_routes(
            &entries,
            &[],
            &cfg,
            &Default::default(),
            &test_rate_limit_cache(),
            &test_quota_cache(),
            &test_tenant_rate_limit_cache(),
        );
        let server = &cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS];
        assert!(
            server.get("max_conns").is_none(),
            "max_conns must be absent when zero"
        );
        assert!(
            server.get("max_conns_per_ip").is_none(),
            "max_conns_per_ip must be absent when zero"
        );
    }

    // ── diff_routes / diff_fqdns tests ───────────────────────────────

    #[test]
    fn diff_routes_empty_both() {
        let (added, removed, changed) = diff_routes(&[], &[]);
        assert!(added.is_empty());
        assert!(removed.is_empty());
        assert!(changed.is_empty());
    }

    #[test]
    fn diff_routes_no_changes() {
        let snap = vec![entry("t_a", "api", "1.2.3.4", 8081)];
        let (added, removed, changed) = diff_routes(&snap, &snap);
        assert!(added.is_empty());
        assert!(removed.is_empty());
        assert!(changed.is_empty());
    }

    #[test]
    fn diff_routes_added() {
        let prev = vec![];
        let curr = vec![entry("t_a", "api", "1.2.3.4", 8081)];
        let (added, removed, changed) = diff_routes(&prev, &curr);
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].0.app_name, "api");
        assert!(removed.is_empty());
        assert!(changed.is_empty());
    }

    #[test]
    fn diff_routes_removed() {
        let prev = vec![entry("t_a", "api", "1.2.3.4", 8081)];
        let curr = vec![];
        let (added, removed, changed) = diff_routes(&prev, &curr);
        assert!(added.is_empty());
        assert_eq!(removed.len(), 1);
        assert!(changed.is_empty());
    }

    #[test]
    fn diff_routes_changed() {
        let prev = vec![entry("t_a", "api", "1.2.3.4", 8081)];
        let curr = vec![entry("t_a", "api", "5.6.7.8", 8082)];
        let (added, removed, changed) = diff_routes(&prev, &curr);
        assert!(added.is_empty());
        assert!(removed.is_empty());
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0.worker_addr, "5.6.7.8");
    }

    #[test]
    fn diff_routes_combined() {
        let prev = vec![
            entry("t_a", "api", "1.2.3.4", 8081), // changes
            entry("t_a", "web", "1.2.3.4", 8082), // removed
        ];
        let curr = vec![
            entry("t_a", "api", "5.6.7.8", 8082),     // changed
            entry("t_b", "blog", "9.10.11.12", 9000), // added
        ];
        let (added, removed, changed) = diff_routes(&prev, &curr);
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].0.app_name, "blog");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0], "t_a:web");
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0.app_name, "api");
    }

    #[test]
    fn diff_fqdns_empty_both() {
        let (added, removed) = diff_fqdns(&[], &[]);
        assert!(added.is_empty());
        assert!(removed.is_empty());
    }

    #[test]
    fn diff_fqdns_no_changes() {
        let bindings = vec![
            fqdn("t_a", "api", "api.acme.com"),
            fqdn("t_a", "web", "web.acme.com"),
        ];
        let (added, removed) = diff_fqdns(&bindings, &bindings);
        assert!(added.is_empty());
        assert!(removed.is_empty());
    }

    #[test]
    fn diff_fqdns_changes() {
        let prev = vec![
            fqdn("t_a", "api", "api.acme.com"),
            fqdn("t_a", "web", "web.acme.com"),
        ];
        let curr = vec![
            fqdn("t_a", "api", "api.acme.com"),
            fqdn("t_b", "blog", "blog.acme.com"),
        ];
        let (added, removed) = diff_fqdns(&prev, &curr);
        assert_eq!(added, vec!["blog.acme.com"]);
        assert_eq!(removed, vec!["web.acme.com"]);
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

    // ── L4 rendering tests (issue #548) ──────────────────────────────

    use crate::l4::L4RouteEntry;

    fn l4_test_cfg() -> Config {
        Config {
            nats_url: "nats://localhost:4222".into(),
            caddy_admin_url: "http://localhost:2019".into(),
            region: "test".into(),
            cert_file: "/tmp/cert.pem".into(),
            key_file: "/tmp/key.pem".into(),
            cert_file_2: None,
            key_file_2: None,
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
            max_conns: 0,
            max_conns_per_ip: 0,
            per_ip_rps: 0,
            per_ip_burst: 0,
            rate_limit_rps_default: 0,
            rate_limit_burst_default: 0,
            rate_limit_fetch_interval: Duration::from_secs(60),
            quota_fetch_interval: Duration::from_secs(30),
            stale_timeout: Duration::from_secs(60),
            prune_interval: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            health_check_timeout: Duration::from_secs(3),
            health_check_uri: "/healthz".into(),
            health_check_max_fails: 2,
            rate_limit_rps_tenant_default: 0,
            rate_limit_burst_tenant_default: 0,
            tenant_rate_limit_fetch_interval: Duration::from_secs(30),
            global_rate_limit_rps: 0,
            global_rate_limit_burst: 0,
            l4_port_range_start: 31000,
            l4_port_range_end: 31999,
            l4_max_conns_per_app: 1000,
            l4_max_conns_per_ip: 100,
            l4_port_cooldown_secs: 60,
        }
    }

    fn empty_quota_cache() -> QuotaCache {
        QuotaCache::default()
    }

    fn l4_entry(tenant: &str, app: &str, public_port: u16) -> L4RouteEntry {
        L4RouteEntry {
            tenant_id: tenant.to_string(),
            app_name: app.to_string(),
            public_port,
            worker_addr: "10.0.0.1".to_string(),
            upstream_port: 8082,
            last_seen: Instant::now(),
        }
    }

    #[test]
    fn render_l4_routes_empty_emits_empty_servers_block() {
        let cfg = l4_test_cfg();
        let payload = render_l4_routes(&[], &cfg, &empty_quota_cache());
        let servers = payload
            .pointer("/apps/layer4/servers")
            .and_then(|v| v.as_object())
            .expect("layer4/servers object");
        assert!(servers.is_empty(), "no entries → empty servers map");
    }

    #[test]
    fn render_l4_routes_single_entry_emits_proxy() {
        let cfg = l4_test_cfg();
        let entry = l4_entry("t_a", "redis", 31000);
        let payload = render_l4_routes(&[entry], &cfg, &empty_quota_cache());
        let server = payload
            .pointer("/apps/layer4/servers/l4_31000")
            .expect("l4_31000 server");
        assert_eq!(
            server.pointer("/listen").and_then(|v| v.as_array()),
            Some(&vec![json!(":31000")])
        );
        let proxy = server.pointer("/routes/0/handle/0").expect("proxy handler");
        assert_eq!(proxy.get("handler"), Some(&json!("proxy")));
        assert_eq!(
            proxy.pointer("/upstreams/0/dial"),
            Some(&json!("10.0.0.1:8082"))
        );
    }

    #[test]
    fn render_l4_routes_omits_connection_policies_until_conn_limit_plugin_wired() {
        // Issue #548 review finding #32: the previous `connection_policies`
        // emission created the illusion of DDoS protection while
        // doing nothing (mholt/caddy-l4 doesn't recognise the
        // `max_conns_per_app` / `max_conns_per_ip` keys — the real
        // cap is the `conn_limit` sub-plugin with a different
        // schema). Until that plugin is added, the rendered
        // `apps.layer4` payload MUST NOT carry a `connection_policies`
        // block — operators reading Caddy config shouldn't be
        // misled into thinking protection is active.
        let cfg = l4_test_cfg();
        let entry = l4_entry("t_a", "redis", 31000);
        let payload = render_l4_routes(&[entry], &cfg, &empty_quota_cache());
        let server = payload
            .pointer("/apps/layer4/servers/l4_31000")
            .expect("server block");
        assert!(
            server.get("connection_policies").is_none(),
            "rendered L4 server must not carry connection_policies until caddy-l4 conn_limit plugin is wired; got: {:?}",
            server
        );
    }

    #[test]
    fn render_l4_routes_over_cap_tenant_renders_empty_routes() {
        let cfg = l4_test_cfg();
        let entry = l4_entry("t_a", "redis", 31000);
        let mut quota_cache = empty_quota_cache();
        quota_cache.update(
            "t_a".to_string(),
            crate::quota::QuotaState {
                over_cap: true,
                ..Default::default()
            },
        );
        let payload = render_l4_routes(&[entry], &cfg, &quota_cache);
        let routes = payload
            .pointer("/apps/layer4/servers/l4_31000/routes")
            .and_then(|v| v.as_array())
            .expect("routes array");
        assert!(routes.is_empty(), "over-cap → empty routes (close-on-cap)");
    }

    #[test]
    fn render_full_merges_http_and_l4_trees() {
        let cfg = l4_test_cfg();
        let http_payload = json!({
            "admin": {"listen": "localhost:2019"},
            "apps": {
                "http": {"servers": {"srv": {"listen": [":443"]}}},
                "tls":  {"automation": {"policies": []}}
            }
        });
        let l4_entry = l4_entry("t_a", "redis", 31000);
        let merged = render_full(http_payload, &[l4_entry], &cfg, &empty_quota_cache());
        // HTTP tree preserved
        assert!(merged.pointer("/apps/http/servers/srv/listen/0").is_some());
        assert!(merged.pointer("/apps/tls").is_some());
        // L4 tree appended
        assert!(merged
            .pointer("/apps/layer4/servers/l4_31000/routes/0/handle")
            .is_some());
        // admin block preserved (HTTP wins)
        assert_eq!(
            merged.pointer("/admin/listen"),
            Some(&json!("localhost:2019"))
        );
    }

    #[test]
    fn render_full_with_empty_l4_still_emits_layer4_block() {
        let cfg = l4_test_cfg();
        let http_payload = json!({
            "apps": {"http": {"servers": {}}}
        });
        let merged = render_full(http_payload, &[], &cfg, &empty_quota_cache());
        let servers = merged
            .pointer("/apps/layer4/servers")
            .and_then(|v| v.as_object())
            .expect("layer4 present even when empty");
        assert!(servers.is_empty());
    }
}
