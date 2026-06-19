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

const STALE_AFTER: Duration = Duration::from_secs(180);
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

/// Connect to NATS, subscribe, and pump heartbeats into the routing table.
/// Returns when the subscription ends (e.g., NATS disconnect). The caller
/// is expected to re-invoke this in a loop with backoff, mirroring the
/// worker's main loop.
pub async fn run(cfg: Config, table: Arc<RoutingTable>, caddy: Arc<CaddyClient>) -> Result<()> {
    let client = async_nats::connect(&cfg.nats_url)
        .await
        .with_context(|| format!("connecting to NATS at {}", cfg.nats_url))?;
    info!(url = %cfg.nats_url, region = %cfg.region, "connected to NATS");

    // Spawn the renderer + the periodic pruner. Both share a `Notify` flag
    // so we collapse bursts of heartbeats into a single Caddy reload.
    let render_notify = Arc::new(Notify::new());
    spawn_renderer(
        cfg.clone(),
        table.clone(),
        caddy.clone(),
        render_notify.clone(),
    );
    spawn_pruner(table.clone(), render_notify.clone());

    // Push the initial empty config so Caddy's admin API has a known state
    // before the first heartbeat lands. (Otherwise Caddy might still be
    // serving its default config, e.g. `:2019` admin only.)
    if let Err(e) = push_now(&cfg, &table, &caddy).await {
        warn!(err = %e, "initial Caddy load failed (will retry on first heartbeat)");
    }

    let subject = format!("edgecloud.heartbeats.{}", cfg.region);
    let mut subscription = client.subscribe(subject.clone()).await?;
    info!(%subject, "subscribed to heartbeats");

    while let Some(msg) = subscription.next().await {
        match serde_json::from_slice::<HeartbeatMessage>(&msg.payload) {
            Ok(hb) => {
                if handle_one(&table, &hb).await {
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
async fn handle_one(table: &RoutingTable, hb: &HeartbeatMessage) -> bool {
    let worker_addr = hb.worker_addr.as_deref().unwrap_or("");
    if worker_addr.is_empty() {
        warn!("heartbeat has no worker_addr; cannot route any apps from it");
        return false;
    }
    let mut changed = false;
    for (app_name, app) in &hb.apps {
        let port = app.port;
        debug!(
            app = %app_name,
            tenant = %app.tenant_id,
            worker_addr,
            port,
            status = %app.status,
            "updating route"
        );
        table
            .upsert(&app.tenant_id, app_name, worker_addr, port, &app.status)
            .await;
        changed = true;
    }
    changed
}

fn spawn_renderer(
    cfg: Config,
    table: Arc<RoutingTable>,
    caddy: Arc<CaddyClient>,
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
            if let Err(e) = push_now(&cfg, &table, &caddy).await {
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

async fn push_now(cfg: &Config, table: &RoutingTable, caddy: &CaddyClient) -> Result<()> {
    let snap: Vec<RouteEntry> = table.snapshot().await;
    let json = render_routes(&snap, cfg);
    caddy.load_config(&json).await
}
