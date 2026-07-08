//! NATS heartbeat subscriber → routing table → debounced Caddy reload.
//!
//! On boot we connect to NATS, subscribe to `edgecloud.heartbeats.<region>`
//! (plain push, no JetStream — matches the worker's pattern), and feed every
//! payload into the routing table. A 60s tick prunes entries that haven't
//! been refreshed in 180s (3 missed heartbeats). After every table change we
//! notify a single renderer task that drains after a debounce and pushes the
//! rendered Caddyfile-JSON to the local Caddy admin API.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use tokio::sync::Notify;
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};

use crate::caddy::{render_routes, CaddyClient};
use crate::config::Config;
use crate::messages::HeartbeatMessage;
use crate::ratelimit::SharedRateLimitCache;
use crate::routing::{FqdnBinding, RouteEntry, RoutingTable};
use crate::traffic::{spawn_fetcher, SharedCache};
use reqwest::Client;

/// Connect to NATS, subscribe, and pump heartbeats into the routing table.
/// Returns when the subscription ends (e.g., NATS disconnect). The caller
/// is expected to re-invoke this in a loop with backoff, mirroring the
/// worker's main loop.
///
/// `render_notify` is the shared `Notify` that the Caddy renderer awaits.
/// It is passed in (rather than created here) so the domain poller in
/// `main.rs` can signal the same channel — see PR #133 review finding #1.
pub async fn run(
    cfg: Config,
    table: Arc<RoutingTable>,
    caddy: Arc<CaddyClient>,
    render_notify: Arc<Notify>,
) -> Result<()> {
    let client = async_nats::connect(&cfg.nats_url)
        .await
        .with_context(|| format!("connecting to NATS at {}", cfg.nats_url))?;
    info!(url = %cfg.nats_url, region = %cfg.region, "connected to NATS");

    // Spawn the renderer + the periodic pruner. Both share the same
    // `Notify` flag (passed in by the caller) so we collapse bursts of
    // heartbeats into a single Caddy reload.

    // Traffic-split cache shared between the fetcher and the renderer.
    let traffic_cache: SharedCache = Default::default();
    // Rate-limit cache shared between the fetcher and the renderer.
    let rate_limit_cache: SharedRateLimitCache = Default::default();
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("reqwest Client must build");
    let traffic_cache_for_renderer = traffic_cache.clone();
    let traffic_cache_for_push = traffic_cache.clone();
    let rate_limit_cache_for_renderer = rate_limit_cache.clone();
    let rate_limit_cache_for_push = rate_limit_cache.clone();
    spawn_fetcher(
        http_client.clone(),
        cfg.control_plane_api_url.clone(),
        traffic_cache.clone(),
        cfg.internal_token.clone(),
        table.clone(),
    );
    crate::ratelimit::spawn_rate_limit_fetcher(
        http_client,
        cfg.control_plane_api_url.clone(),
        rate_limit_cache.clone(),
        cfg.internal_token.clone(),
        table.clone(),
        cfg.rate_limit_fetch_interval,
    );
    spawn_renderer(
        cfg.clone(),
        table.clone(),
        caddy.clone(),
        traffic_cache_for_renderer,
        rate_limit_cache_for_renderer,
        render_notify.clone(),
    );
    spawn_pruner(table.clone(), render_notify.clone(), cfg.clone());

    // Push the initial empty config so Caddy's admin API has a known state
    // before the first heartbeat lands. (Otherwise Caddy might still be
    // serving its default config, e.g. `:2019` admin only.)
    let mut boot_previous: Option<PreviousState> = None;
    if let Err(e) = push_now(
        &cfg,
        &table,
        &caddy,
        &traffic_cache_for_push,
        &rate_limit_cache_for_push,
        &mut boot_previous,
    )
    .await
    {
        warn!(err = %e, "initial Caddy load failed (will retry on first heartbeat)");
    }

    let subject = format!("edgecloud.heartbeats.{}", cfg.region);
    let mut subscription = client.subscribe(subject.clone()).await?;
    info!(%subject, "subscribed to heartbeats");

    while let Some(msg) = subscription.next().await {
        match serde_json::from_slice::<HeartbeatMessage>(&msg.payload) {
            Ok(hb) => {
                metrics::counter!("ingress.heartbeats.received", "region" => cfg.region.clone())
                    .increment(1);
                metrics::counter!("ingress.heartbeats.apps_total").increment(hb.apps.len() as u64);
                if apply_heartbeat(&table, &hb).await {
                    metrics::counter!("ingress.routes.changed").increment(1);
                    render_notify.notify_one();
                }
            }
            Err(e) => {
                metrics::counter!("ingress.heartbeats.parse_failed").increment(1);
                warn!(err = %e, "failed to parse heartbeat; ignoring");
            }
        }
    }

    Err(anyhow::anyhow!("NATS subscription stream ended"))
}

/// Returns `true` iff at least one app caused a table mutation. The caller
/// uses this to skip the Caddy reload notify when the heartbeat carried no
/// usable routing data (e.g. a legacy worker that hasn't published
/// `worker_addr` yet — pre-#70 workers that don't carry a port will fail
/// to deserialize at the `serde_json::from_slice` call site above, which
/// is the intended hard-cutover behaviour).
///
/// `pub` so cross-crate integration tests can drive the same code path
/// that the NATS subscription loop drives — that's the only way to prove
/// the wire-shape contract (Step 1) actually produces the routing-table
/// mutations the rest of the system expects. Without `pub`, integration
/// tests would have to either duplicate `apply_heartbeat`'s logic (drift
/// risk) or skip the cross-wire assertion entirely.
pub async fn apply_heartbeat(table: &RoutingTable, hb: &HeartbeatMessage) -> bool {
    let worker_addr = hb.worker_addr.as_deref().unwrap_or("");
    if worker_addr.is_empty() {
        metrics::counter!("ingress.heartbeats.no_addr").increment(1);
        warn!("heartbeat has no worker_addr; cannot route any apps from it");
        return false;
    }

    // Empty apps map signals a final heartbeat (worker shutting down).
    // Remove all routes for this worker immediately instead of waiting
    // for the stale pruner.
    if hb.apps.is_empty() {
        let removed = table.remove_worker(worker_addr).await;
        if !removed.is_empty() {
            warn!(
                worker_addr = %worker_addr,
                ?removed,
                "worker sent empty heartbeat — removed all routes"
            );
            return true;
        }
        return false;
    }

    let mut changed = false;
    for (key, app) in &hb.apps {
        let (app_name, deployment_id) = match key.split_once(':') {
            Some((name, id)) => (name, Some(id)),
            None => (key.as_str(), None),
        };
        let port = app.port;
        debug!(
            app = %app_name,
            deployment_id = %deployment_id.unwrap_or("(none)"),
            tenant = %app.tenant_id,
            worker_addr,
            port,
            status = %app.status,
            "updating route"
        );
        // Weight is not in the heartbeat — the ingress fetches traffic splits
        // from the control plane API at render time. Default to 100 so a
        // single deployment always gets full traffic.
        table
            .upsert(
                &app.tenant_id,
                app_name,
                deployment_id,
                100,
                worker_addr,
                port,
                &app.status,
            )
            .await;
        changed = true;

        // If the heartbeat carries a WebSocket port, insert a second
        // route entry so Caddy can route Upgrade: websocket requests
        // to the correct upstream port (issue #312). Caddy's
        // reverse_proxy natively handles WebSocket transparently.
        if let Some(ws_port) = app.ws_port {
            let ws_app_name = format!("{}-ws", app_name);
            let ws_deployment_id = deployment_id.map(|d| format!("{}-ws", d));
            table
                .upsert(
                    &app.tenant_id,
                    &ws_app_name,
                    ws_deployment_id.as_deref(),
                    100,
                    worker_addr,
                    ws_port,
                    &app.status,
                )
                .await;
        }
    }
    changed
}

/// Snapshot of the routing table and FQDN bindings from the last
/// successful Caddy config push. Used to compute incremental diffs.
struct PreviousState {
    route_entries: Vec<RouteEntry>,
    fqdn_bindings: Vec<FqdnBinding>,
}

fn spawn_renderer(
    cfg: Config,
    table: Arc<RoutingTable>,
    caddy: Arc<CaddyClient>,
    traffic_cache: SharedCache,
    rate_limit_cache: SharedRateLimitCache,
    notify: Arc<Notify>,
) {
    tokio::spawn(async move {
        let mut previous: Option<PreviousState> = None;
        loop {
            notify.notified().await;
            // Coalesce bursty notifications: sleep the debounce, then push.
            sleep(Duration::from_millis(cfg.refresh_debounce_ms)).await;
            if let Err(e) = push_now(
                &cfg,
                &table,
                &caddy,
                &traffic_cache,
                &rate_limit_cache,
                &mut previous,
            )
            .await
            {
                error!(err = %e, "Caddy reload failed");
                // On error reset previous state so the next push is a full reload.
                previous = None;
            } else {
                debug!("Caddy config reloaded");
            }
        }
    });
}

fn spawn_pruner(table: Arc<RoutingTable>, notify: Arc<Notify>, cfg: Config) {
    tokio::spawn(async move {
        let mut ticker = interval(cfg.prune_interval);
        // Skip the first immediate tick.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let removed = table.remove_stale(cfg.stale_timeout).await;
            if !removed.is_empty() {
                metrics::counter!("ingress.pruner.removed_total").increment(removed.len() as u64);
                warn!(?removed, "pruned stale routes");
                notify.notify_one();
            }
        }
    });
}

async fn push_now(
    cfg: &Config,
    table: &RoutingTable,
    caddy: &CaddyClient,
    traffic_cache: &SharedCache,
    rate_limit_cache: &SharedRateLimitCache,
    previous: &mut Option<PreviousState>,
) -> Result<()> {
    let snap: Vec<RouteEntry> = table.snapshot().await;
    let fqdns = table.fqdn_snapshot().await;
    let traffic_cache_guard = traffic_cache.read().await;
    let rate_limit_cache_guard = rate_limit_cache.read().await;

    // Set gauges from current state.
    metrics::gauge!("ingress.routes.active").set(snap.len() as f64);
    metrics::gauge!("ingress.fqdns.active").set(fqdns.len() as f64);

    match previous.take() {
        None => {
            // Boot push — full config POST /load.
            let render_start = std::time::Instant::now();
            let json = render_routes(
                &snap,
                &fqdns,
                cfg,
                &traffic_cache_guard,
                &rate_limit_cache_guard,
            );
            let render_dur = render_start.elapsed();
            metrics::histogram!("ingress.caddy.render_duration_seconds")
                .record(render_dur.as_secs_f64());

            let load_start = std::time::Instant::now();
            let result = caddy.load_config(&json).await;
            let load_dur = load_start.elapsed();
            metrics::histogram!("ingress.caddy.reload_duration_seconds")
                .record(load_dur.as_secs_f64());

            match &result {
                Ok(()) => {
                    metrics::counter!("ingress.caddy.reload_total", "status" => "success")
                        .increment(1);
                    *previous = Some(PreviousState {
                        route_entries: snap,
                        fqdn_bindings: fqdns,
                    });
                }
                Err(_) => {
                    metrics::counter!("ingress.caddy.reload_total", "status" => "failure")
                        .increment(1);
                }
            }
            result
        }
        Some(prev) => {
            // Incremental — compute diffs and apply per-route patches.
            let (added, removed_ids, changed) =
                crate::caddy::diff_routes(&prev.route_entries, &snap);
            let (fqdn_added, fqdn_removed) = crate::caddy::diff_fqdns(&prev.fqdn_bindings, &fqdns);
            let total_changes = added.len()
                + removed_ids.len()
                + changed.len()
                + fqdn_added.len()
                + fqdn_removed.len();
            let total = snap.len().max(prev.route_entries.len()).max(1);

            // Heuristic: if >20% of routes changed, fall back to full reload.
            if total_changes * 5 > total {
                let render_start = std::time::Instant::now();
                let json = render_routes(
                    &snap,
                    &fqdns,
                    cfg,
                    &traffic_cache_guard,
                    &rate_limit_cache_guard,
                );
                let render_dur = render_start.elapsed();
                metrics::histogram!("ingress.caddy.render_duration_seconds")
                    .record(render_dur.as_secs_f64());

                let load_start = std::time::Instant::now();
                let result = caddy.load_config(&json).await;
                let load_dur = load_start.elapsed();
                metrics::histogram!("ingress.caddy.reload_duration_seconds")
                    .record(load_dur.as_secs_f64());

                match &result {
                    Ok(()) => {
                        metrics::counter!("ingress.caddy.reload_total", "status" => "success")
                            .increment(1);
                        *previous = Some(PreviousState {
                            route_entries: snap,
                            fqdn_bindings: fqdns,
                        });
                    }
                    Err(_) => {
                        metrics::counter!("ingress.caddy.reload_total", "status" => "failure")
                            .increment(1);
                    }
                }
                return result;
            }

            // Apply per-route diffs.
            let mut ops_ok = true;
            let load_start = std::time::Instant::now();

            // Delete removed routes.
            for id in &removed_ids {
                if let Err(e) = caddy.delete_route(id).await {
                    tracing::warn!(err = %e, route_id = %id, "failed to delete route");
                    ops_ok = false;
                    break;
                }
            }

            // Delete removed FQDN routes.
            if ops_ok {
                for fqdn in &fqdn_removed {
                    if let Err(e) = caddy.delete_route(fqdn).await {
                        tracing::warn!(err = %e, fqdn = %fqdn, "failed to delete FQDN route");
                        ops_ok = false;
                        break;
                    }
                }
            }

            // Upsert added + changed routes.
            if ops_ok {
                for (entry, _) in added.iter().chain(changed.iter()) {
                    let route = render_single_route(
                        entry,
                        cfg,
                        &traffic_cache_guard,
                        &rate_limit_cache_guard,
                    );
                    if let Err(e) = caddy.upsert_route(&route).await {
                        tracing::warn!(
                            err = %e, tenant = %entry.tenant_id, app = %entry.app_name,
                            "failed to upsert route"
                        );
                        ops_ok = false;
                        break;
                    }
                }
            }

            // Upsert added FQDN routes.
            if ops_ok {
                for fqdn_str in &fqdn_added {
                    // Find the FqdnBinding by fqdn.
                    if let Some(binding) = fqdns.iter().find(|b| b.fqdn == *fqdn_str) {
                        let route = render_fqdn_route(binding, &snap, cfg, &rate_limit_cache_guard);
                        if let Some(route) = route {
                            if let Err(e) = caddy.upsert_route(&route).await {
                                tracing::warn!(err = %e, fqdn = %fqdn_str, "failed to upsert FQDN route");
                                ops_ok = false;
                                break;
                            }
                        }
                    }
                }
            }

            let load_dur = load_start.elapsed();
            metrics::histogram!("ingress.caddy.reload_duration_seconds")
                .record(load_dur.as_secs_f64());

            if ops_ok {
                metrics::counter!("ingress.caddy.reload_total", "status" => "success").increment(1);
                *previous = Some(PreviousState {
                    route_entries: snap,
                    fqdn_bindings: fqdns,
                });
                Ok(())
            } else {
                metrics::counter!("ingress.caddy.reload_total", "status" => "failure").increment(1);
                // Reset previous on error so next push does a full reload.
                Err(anyhow::anyhow!("incremental push failed"))
            }
        }
    }
}

/// Render a single route for a (tenant, app) group.
fn render_single_route(
    entry: &RouteEntry,
    _cfg: &Config,
    _traffic_cache: &crate::traffic::TrafficSplitCache,
    _rate_limit_cache: &crate::ratelimit::RateLimitCache,
) -> serde_json::Value {
    let host = crate::config::ingress_host(&entry.tenant_id, &entry.app_name);
    let dial = format!("{}:{}", entry.worker_addr, entry.port);
    let mut handle_chain = Vec::new();
    handle_chain.push(serde_json::json!({
        "handler": "reverse_proxy",
        "upstreams": [{"dial": dial}],
        "health_checks": {
            "active": {"uri": "/", "expect_status": 2}
        }
    }));
    serde_json::json!({
        "@id": host,
        "match": [{"host": [host]}],
        "handle": [{
            "handler": "subroute",
            "routes": [{
                "handle": handle_chain,
            }]
        }],
        "terminal": true
    })
}

/// Render a single FQDN route.
fn render_fqdn_route(
    binding: &FqdnBinding,
    entries: &[RouteEntry],
    _cfg: &Config,
    _rate_limit_cache: &crate::ratelimit::RateLimitCache,
) -> Option<serde_json::Value> {
    let upstream = entries
        .iter()
        .find(|e| e.tenant_id == binding.tenant_id && e.app_name == binding.app_name)?;
    let dial = format!("{}:{}", upstream.worker_addr, upstream.port);
    let handle_chain = vec![serde_json::json!({
        "handler": "reverse_proxy",
        "upstreams": [{"dial": dial}],
        "health_checks": {
            "active": {"uri": "/", "expect_status": 2}
        }
    })];
    Some(serde_json::json!({
        "@id": binding.fqdn,
        "match": [{"host": [binding.fqdn]}],
        "handle": [{
            "handler": "subroute",
            "routes": [{
                "handle": handle_chain,
            }]
        }],
        "terminal": true,
        "tls": {"on_demand": {}}
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use edge_worker::messages::AppStatus;
    use std::collections::HashMap;

    /// Helper to build a minimal AppStatus with the given tenant, status,
    /// and port. All other fields get sensible defaults.
    fn app_status(tenant_id: &str, status: &str, port: u16) -> AppStatus {
        AppStatus {
            deployment_id: "d_test".to_string(),
            status: status.to_string(),
            exit_code: None,
            request_count: 0,
            outbound_bytes: 0,
            tenant_id: tenant_id.to_string(),
            port,
            ws_port: None,
            observer_metrics: vec![],
        }
    }

    /// Helper to build a HeartbeatMessage with a worker_addr and apps.
    fn hb_with_addr(worker_addr: &str, apps: HashMap<String, AppStatus>) -> HeartbeatMessage {
        HeartbeatMessage {
            msg_type: "heartbeat".to_string(),
            timestamp: "2026-06-19T00:00:00Z".to_string(),
            worker_id: "w_fra_abc".to_string(),
            region: "fra".to_string(),
            worker_addr: Some(worker_addr.to_string()),
            tenant_id: None,
            apps,
            cluster_headroom: None,
        }
    }

    fn hb_no_addr(apps: HashMap<String, AppStatus>) -> HeartbeatMessage {
        HeartbeatMessage {
            msg_type: "heartbeat".to_string(),
            timestamp: "2026-06-19T00:00:00Z".to_string(),
            worker_id: "w_fra_abc".to_string(),
            region: "fra".to_string(),
            worker_addr: None,
            tenant_id: None,
            apps,
            cluster_headroom: None,
        }
    }

    // ── Existing tests ────────────────────────────────────────────────

    /// A heartbeat with `worker_addr: None` must NOT mutate the routing
    /// table, and `apply_heartbeat` must return `false` so the caller skips
    /// the Caddy-reload notify.
    #[tokio::test]
    async fn handle_one_skips_when_worker_addr_is_none() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        let hb = hb_no_addr(apps);

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(!changed);
        assert_eq!(table.len().await, 0);
    }

    /// Same expectation for an empty-string `worker_addr`.
    #[tokio::test]
    async fn handle_one_skips_when_worker_addr_is_empty_string() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        let hb = HeartbeatMessage {
            msg_type: "heartbeat".to_string(),
            timestamp: "2026-06-19T00:00:00Z".to_string(),
            worker_id: "w_fra_abc".to_string(),
            region: "fra".to_string(),
            worker_addr: Some(String::new()),
            tenant_id: None,
            apps,
            cluster_headroom: None,
        };

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(!changed);
        assert_eq!(table.len().await, 0);
    }

    /// Happy-path: with a valid `worker_addr`, `apply_heartbeat` inserts
    /// one route per app and returns `true`.
    #[tokio::test]
    async fn handle_one_inserts_route_when_worker_addr_present() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].worker_addr, "203.0.113.10");
        assert_eq!(snap[0].port, 8081);
        assert_eq!(snap[0].tenant_id, "t_a");
        assert_eq!(snap[0].app_name, "api");
        assert_eq!(snap[0].deployment_id, None);
        assert_eq!(snap[0].weight, 100);
    }

    // ── New apply_heartbeat tests ─────────────────────────────────────

    /// Key with `:` separator sets the deployment_id (canary support).
    #[tokio::test]
    async fn apply_heartbeat_with_canary_key() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        apps.insert("api:v2".to_string(), app_status("t_a", "running", 8081));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].app_name, "api");
        assert_eq!(snap[0].deployment_id, Some("v2".to_string()));
    }

    /// Non-"running" status removes the entry.
    #[tokio::test]
    async fn apply_heartbeat_with_non_running_status() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "crashed", 8081));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(changed);
        // The "crashed" app causes an upsert with that status, which
        // the routing table interprets as "remove".
        assert_eq!(table.len().await, 0);
    }

    /// "draining" status keeps the route with weight=0.
    #[tokio::test]
    async fn apply_heartbeat_with_draining_status() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "draining", 8081));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].weight, 0, "draining apps get weight=0");
        assert_eq!(snap[0].app_name, "api");
        assert_eq!(snap[0].port, 8081);
    }

    /// Empty apps map removes all routes for that worker.
    #[tokio::test]
    async fn apply_heartbeat_empty_apps_removes_worker_routes() {
        let table = Arc::new(RoutingTable::new());
        // First insert a route via normal heartbeat.
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        let hb1 = hb_with_addr("203.0.113.10", apps);
        assert!(apply_heartbeat(&table, &hb1).await);
        assert_eq!(table.len().await, 1);

        // Empty heartbeat from same worker removes all routes.
        let hb2 = hb_with_addr("203.0.113.10", HashMap::new());
        let changed = apply_heartbeat(&table, &hb2).await;
        assert!(changed);
        assert_eq!(table.len().await, 0);
    }

    /// Unknown worker with empty apps is a no-op.
    #[tokio::test]
    async fn apply_heartbeat_empty_apps_unknown_worker_noop() {
        let table = Arc::new(RoutingTable::new());
        let hb = hb_with_addr("203.0.113.10", HashMap::new());
        let changed = apply_heartbeat(&table, &hb).await;
        assert!(!changed);
        assert_eq!(table.len().await, 0);
    }

    /// Multiple apps in a single heartbeat — both upserted.
    #[tokio::test]
    async fn apply_heartbeat_with_multiple_apps() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        apps.insert("worker".to_string(), app_status("t_a", "running", 8082));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 2);
    }

    /// Empty apps map — returns false, no mutation.
    #[tokio::test]
    async fn apply_heartbeat_with_empty_apps() {
        let table = Arc::new(RoutingTable::new());
        let hb = hb_with_addr("203.0.113.10", HashMap::new());

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(!changed);
        assert_eq!(table.len().await, 0);
    }

    /// WebSocket port creates a second route entry with `-ws` suffix.
    #[tokio::test]
    async fn apply_heartbeat_with_ws_port_creates_second_route() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.ws_port = Some(9091);
        apps.insert("api".to_string(), status);
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 2);
        assert!(snap.iter().any(|e| e.app_name == "api" && e.port == 8081));
        assert!(snap
            .iter()
            .any(|e| e.app_name == "api-ws" && e.port == 9091));
    }

    /// WebSocket port with canary key — ws entry gets `{deployment_id}-ws`.
    #[tokio::test]
    async fn apply_heartbeat_with_ws_port_and_canary_key() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.ws_port = Some(9091);
        apps.insert("api:v2".to_string(), status);
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 2);
        // Normal route: "api" with deployment_id "v2"
        let api_entry = snap.iter().find(|e| e.app_name == "api").unwrap();
        assert_eq!(api_entry.deployment_id, Some("v2".to_string()));
        // WS route: "api-ws" with deployment_id "v2-ws"
        let ws_entry = snap.iter().find(|e| e.app_name == "api-ws").unwrap();
        assert_eq!(ws_entry.deployment_id, Some("v2-ws".to_string()));
        assert_eq!(ws_entry.port, 9091);
    }

    /// Mixed statuses: one running, one crashed. Only running survives.
    #[tokio::test]
    async fn apply_heartbeat_with_mixed_statuses() {
        let table = Arc::new(RoutingTable::new());
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        apps.insert("cron".to_string(), app_status("t_a", "crashed", 8082));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].app_name, "api");
    }

    // ── push_now tests ────────────────────────────────────────────────

    fn test_config(admin_url: &str) -> Config {
        Config {
            nats_url: "nats://localhost:4222".into(),
            caddy_admin_url: admin_url.to_string(),
            region: "test".into(),
            cert_file: "/tmp/cert.pem".into(),
            key_file: "/tmp/key.pem".into(),
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
            stale_timeout: Duration::from_secs(60),
            prune_interval: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            health_check_timeout: Duration::from_secs(3),
            health_check_uri: "/healthz".into(),
            health_check_max_fails: 2,
        }
    }

    #[tokio::test]
    async fn push_now_sends_config_to_caddy() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/load"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let cfg = Config {
            refresh_debounce_ms: 1, // fast debounce for tests
            ..test_config(&server.uri())
        };
        let table = Arc::new(RoutingTable::new());
        let caddy = Arc::new(CaddyClient::new(&server.uri(), None).unwrap());
        let cache: SharedCache = Default::default();
        let rl_cache: SharedRateLimitCache = Default::default();

        push_now(&cfg, &table, &caddy, &cache, &rl_cache, &mut None)
            .await
            .expect("push_now should succeed");
    }

    #[tokio::test]
    async fn push_now_propagates_caddy_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/load"))
            .respond_with(ResponseTemplate::new(502))
            .up_to_n_times(3)
            .mount(&server)
            .await;

        let cfg = test_config(&server.uri());
        let table = Arc::new(RoutingTable::new());
        let caddy = Arc::new(CaddyClient::new(&server.uri(), None).unwrap());
        let cache: SharedCache = Default::default();
        let rl_cache: SharedRateLimitCache = Default::default();

        let err = push_now(&cfg, &table, &caddy, &cache, &rl_cache, &mut None)
            .await
            .expect_err("push_now should fail with 502");
        assert!(
            err.to_string().contains("502"),
            "err should mention 502, got: {err}"
        );
    }

    // ── spawn_renderer tests ─────────────────────────────────────────

    /// Notify triggers a Caddy reload. Verifies the spawn→notify→debounce→push
    /// chain works end-to-end with a real debounce delay.
    #[tokio::test]
    async fn spawn_renderer_reloads_caddy_on_notify() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/load"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1..)
            .mount(&server)
            .await;

        let cfg = Config {
            refresh_debounce_ms: 1,
            ..test_config(&server.uri())
        };
        let table = Arc::new(RoutingTable::new());
        let caddy = Arc::new(CaddyClient::new(&server.uri(), None).unwrap());
        let cache: SharedCache = Default::default();
        let rl_cache: SharedRateLimitCache = Default::default();
        let notify = Arc::new(Notify::new());

        // Notify BEFORE spawn: tokio::sync::Notify stores a pending
        // notification when no task is waiting, so the spawned task's
        // first notified().await will observe it immediately.
        notify.notify_one();

        spawn_renderer(cfg, table, caddy, cache, rl_cache, notify.clone());

        // Wait for debounce (1ms) + push to complete.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ── spawn_pruner / pruner_tick tests ─────────────────────────────

    #[tokio::test]
    async fn pruner_tick_removes_stale_entries() {
        let table = Arc::new(RoutingTable::new());
        let _notify = Arc::new(Notify::new());

        // Insert an entry with a recent timestamp (should not be pruned).
        table
            .upsert("t_a", "api", None, 100, "1.2.3.4", 8081, "running")
            .await;
        assert_eq!(table.len().await, 1);

        // remove_stale with zero duration prunes everything.
        let removed = table.remove_stale(Duration::from_secs(0)).await;
        assert_eq!(removed.len(), 1, "should prune the entry");
        assert_eq!(table.len().await, 0);
    }

    #[tokio::test]
    async fn pruner_tick_skips_fresh_entries() {
        let table = Arc::new(RoutingTable::new());
        let _notify = Arc::new(Notify::new());

        table
            .upsert("t_a", "api", None, 100, "1.2.3.4", 8081, "running")
            .await;

        // remove_stale with a very long duration keeps everything.
        let removed = table.remove_stale(Duration::from_secs(9999)).await;
        assert!(removed.is_empty());
        assert_eq!(table.len().await, 1);
    }
}
