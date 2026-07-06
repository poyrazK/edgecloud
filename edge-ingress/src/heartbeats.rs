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
use crate::routing::{RouteEntry, RoutingTable};
use crate::traffic::{spawn_fetcher, SharedCache};
use reqwest::Client;

const STALE_AFTER: Duration = Duration::from_secs(180);
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

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
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("reqwest Client must build");
    let traffic_cache_for_renderer = traffic_cache.clone();
    let traffic_cache_for_push = traffic_cache.clone();
    spawn_fetcher(
        http_client,
        cfg.control_plane_api_url.clone(),
        traffic_cache.clone(),
        cfg.internal_token.clone(),
        table.clone(),
    );
    spawn_renderer(
        cfg.clone(),
        table.clone(),
        caddy.clone(),
        traffic_cache_for_renderer,
        render_notify.clone(),
    );
    spawn_pruner(table.clone(), render_notify.clone());

    // Push the initial empty config so Caddy's admin API has a known state
    // before the first heartbeat lands. (Otherwise Caddy might still be
    // serving its default config, e.g. `:2019` admin only.)
    if let Err(e) = push_now(&cfg, &table, &caddy, &traffic_cache_for_push).await {
        warn!(err = %e, "initial Caddy load failed (will retry on first heartbeat)");
    }

    let subject = format!("edgecloud.heartbeats.{}", cfg.region);
    let mut subscription = client.subscribe(subject.clone()).await?;
    info!(%subject, "subscribed to heartbeats");

    while let Some(msg) = subscription.next().await {
        match serde_json::from_slice::<HeartbeatMessage>(&msg.payload) {
            Ok(hb) => {
                if apply_heartbeat(&table, &hb).await {
                    render_notify.notify_one();
                }
            }
            Err(e) => {
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
        warn!("heartbeat has no worker_addr; cannot route any apps from it");
        return false;
    }
    let mut changed = false;
    for (key, app) in &hb.apps {
        // Heartbeat key is now "app_name:deployment_id" to support canary
        // (multiple concurrent deployments of the same app). Split to recover
        // the two parts. AppStatus.deployment_id must match the key suffix.
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

fn spawn_renderer(
    cfg: Config,
    table: Arc<RoutingTable>,
    caddy: Arc<CaddyClient>,
    traffic_cache: SharedCache,
    notify: Arc<Notify>,
) {
    tokio::spawn(async move {
        loop {
            notify.notified().await;
            // Coalesce bursty notifications: sleep the debounce, then push.
            // If more heartbeats arrive during the debounce, `Notify` will
            // hold a permit and the next `notified().await` returns
            // immediately, so we push again — with one extra reload per
            // burst. That's acceptable for v1; if it becomes a problem,
            // switch to a trailing-edge debounce using a watch channel.
            sleep(Duration::from_millis(cfg.refresh_debounce_ms)).await;
            if let Err(e) = push_now(&cfg, &table, &caddy, &traffic_cache).await {
                error!(err = %e, "Caddy reload failed");
            } else {
                debug!("Caddy config reloaded");
            }
        }
    });
}

fn spawn_pruner(table: Arc<RoutingTable>, notify: Arc<Notify>) {
    tokio::spawn(async move {
        let mut ticker = interval(PRUNE_INTERVAL);
        // Skip the first immediate tick.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let removed = table.remove_stale(STALE_AFTER).await;
            if !removed.is_empty() {
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
) -> Result<()> {
    let snap: Vec<RouteEntry> = table.snapshot().await;
    let fqdns = table.fqdn_snapshot().await;
    let traffic_cache = traffic_cache.read().await;
    let json = render_routes(&snap, &fqdns, cfg, &traffic_cache);
    caddy.load_config(&json).await
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
        assert!(snap.iter().any(|e| e.app_name == "api-ws" && e.port == 9091));
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

        push_now(&cfg, &table, &caddy, &cache)
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

        let err = push_now(&cfg, &table, &caddy, &cache)
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
        let notify = Arc::new(Notify::new());

        // Notify BEFORE spawn: tokio::sync::Notify stores a pending
        // notification when no task is waiting, so the spawned task's
        // first notified().await will observe it immediately.
        notify.notify_one();

        spawn_renderer(cfg, table, caddy, cache, notify.clone());

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
