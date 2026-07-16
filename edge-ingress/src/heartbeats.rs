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
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::caddy::{render_full, render_routes, CaddyClient};
use crate::config::Config;
use crate::global_rps::{spawn_global_rps_reader, SharedGlobalRpsCache};
use crate::l4::{L4AppKey, L4PortPool, L4RouteEntry, L4RoutingTable};
use crate::l4_cache::{spawn_l4_port_cache_fetcher, SharedL4PortCache};
use crate::messages::HeartbeatMessage;
use crate::quota::{spawn_quota_fetcher, SharedQuotaCache};
use crate::ratelimit::SharedRateLimitCache;
use crate::routing::{FqdnBinding, RouteEntry, RoutingTable};
use crate::tenant_ratelimit::{spawn_tenant_rate_limit_fetcher, SharedTenantRateLimitCache};
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
    l4_table: Arc<L4RoutingTable>,
    l4_ports: Arc<tokio::sync::Mutex<L4PortPool>>,
    caddy: Arc<CaddyClient>,
    render_notify: Arc<Notify>,
    shutdown: CancellationToken,
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
    // Quota-state cache shared between the quota fetcher and the renderer.
    // Issue #420 — driving the Caddy `static_response` 402 inject.
    let quota_cache: SharedQuotaCache = Default::default();
    // Per-tenant data-plane rate-limit cache (issue #305). Polls
    // `GET /api/v1/internal/rate-limit/{tenantID}` every
    // `cfg.tenant_rate_limit_fetch_interval` (default 30s) so the
    // renderer can inject per-tenant `rate_limit` routes (sub-feature
    // #1) and a single global `rate_limit` route (sub-feature #4).
    // Disabled when `tenant_rate_limit_fetch_interval` is zero — see
    // `spawn_tenant_rate_limit_fetcher`.
    let tenant_rate_limit_cache: SharedTenantRateLimitCache = Default::default();
    // L4 public-port cache (issue #548). Polls the control plane
    // every `L4_PORT_CACHE_FETCH_INTERVAL` (30s) so two ingress
    // instances in the same region see the same `(tenant, app) →
    // public_port` mapping. `apply_heartbeat` consults this before
    // falling back to the ingress-local `L4PortPool` on the cold
    // path.
    let l4_port_cache: SharedL4PortCache = Default::default();
    // Cross-replica global RPS cache (issue #665 PR D). The
    // UDS reader task below parses datagrams from the sidecar
    // and writes the latest `local_cap` here; the renderer reads
    // it to emit the cross-replica `rate_limit` route. Fail-closed
    // by default (cold cache → no route emitted).
    let global_rps_cache: SharedGlobalRpsCache = Default::default();
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("reqwest Client must build");
    let traffic_cache_for_renderer = traffic_cache.clone();
    let traffic_cache_for_push = traffic_cache.clone();
    let rate_limit_cache_for_renderer = rate_limit_cache.clone();
    let rate_limit_cache_for_push = rate_limit_cache.clone();
    let quota_cache_for_renderer = quota_cache.clone();
    let quota_cache_for_push = quota_cache.clone();
    let tenant_rate_limit_cache_for_renderer = tenant_rate_limit_cache.clone();
    let tenant_rate_limit_cache_for_push = tenant_rate_limit_cache.clone();
    let global_rps_cache_for_renderer = global_rps_cache.clone();
    let global_rps_cache_for_push = global_rps_cache.clone();

    // Spawn background tasks with the shutdown token.
    let fetcher_shutdown = shutdown.clone();
    spawn_fetcher(
        http_client.clone(),
        cfg.control_plane_api_url.clone(),
        traffic_cache.clone(),
        cfg.internal_token.clone(),
        table.clone(),
        fetcher_shutdown,
    );
    let rl_fetcher_shutdown = shutdown.clone();
    // Make a third clone to keep `http_client` alive for the quota
    // fetcher below (issue #420). The rate-limit fetcher takes
    // ownership, so we explicitly clone here rather than relying on
    // the compiler to see through `.clone()` later in the function.
    let http_client_for_quota = http_client.clone();
    crate::ratelimit::spawn_rate_limit_fetcher(
        http_client,
        cfg.control_plane_api_url.clone(),
        rate_limit_cache.clone(),
        cfg.internal_token.clone(),
        table.clone(),
        cfg.rate_limit_fetch_interval,
        rl_fetcher_shutdown,
    );
    // Quota fetcher (issue #420). Polls `GET /api/v1/internal/quota/{tenant}`
    // every `cfg.quota_fetch_interval` (default 30s) so the renderer can
    // inject a Caddy `static_response` 402 when any tenant crosses cap.
    // Disabled when `quota_fetch_interval` is zero — see spawn_quota_fetcher.
    let quota_fetcher_shutdown = shutdown.clone();
    let http_client_for_l4 = http_client_for_quota.clone();
    spawn_quota_fetcher(
        http_client_for_quota.clone(),
        cfg.control_plane_api_url.clone(),
        quota_cache.clone(),
        cfg.internal_token.clone(),
        table.clone(),
        quota_fetcher_shutdown,
    );
    // Tenant rate-limit fetcher (issue #305). Polls every tenant in
    // the routing table for `GET /api/v1/internal/rate-limit/{tenant}`
    // so the renderer can emit per-tenant + global rate_limit routes.
    // Disabled when `tenant_rate_limit_fetch_interval` is zero — see
    // `spawn_tenant_rate_limit_fetcher`. The fetcher is intentionally
    // launched after the quota fetcher so the http_client clone
    // chain stays obvious to readers (quota -> tenant_rl -> renderer).
    let tenant_rl_fetcher_shutdown = shutdown.clone();
    spawn_tenant_rate_limit_fetcher(
        http_client_for_quota,
        cfg.control_plane_api_url.clone(),
        tenant_rate_limit_cache.clone(),
        cfg.internal_token.clone(),
        table.clone(),
        cfg.tenant_rate_limit_fetch_interval,
        tenant_rl_fetcher_shutdown,
    );
    // L4 public-port cache fetcher (issue #548). Polls
    // `GET /api/v1/internal/l4-port/{tenant}/{app}` every 30s for every
    // routable L4 app; the cache is consulted by `apply_heartbeat` on
    // the cold path before we fall back to the ingress-local pool.
    // Disabled when `control_plane_api_url` is empty — same mode as
    // the quota/traffic/rate-limit fetchers.
    let l4_cache_shutdown = shutdown.clone();
    spawn_l4_port_cache_fetcher(
        http_client_for_l4,
        cfg.control_plane_api_url.clone(),
        l4_port_cache.clone(),
        cfg.internal_token.clone(),
        l4_table.clone(),
        l4_cache_shutdown,
    );
    // Issue #665 PR D — spawn the cross-replica global RPS UDS
    // reader. Opt-in via `INGRESS_RATE_LIMIT_AGGREGATION=true`;
    // when false, the cache stays empty and the renderer never
    // emits the cross-replica route (fail-closed). The reader
    // owns the bind + chmod + unlink on `cfg.global_rps_uds_path`
    // — see `edge-ingress-sidecar/src/expose.rs` module docs for
    // the UDS ownership rationale.
    let global_rps_reader_shutdown = shutdown.clone();
    let global_rps_cache_for_reader = global_rps_cache.clone();
    let global_rps_uds_path = cfg.global_rps_uds_path.clone();
    if cfg.ingress_rate_limit_aggregation {
        match spawn_global_rps_reader(
            global_rps_uds_path,
            global_rps_cache_for_reader,
            global_rps_reader_shutdown,
        ) {
            Ok(_handle) => {
                info!(
                    path = %cfg.global_rps_uds_path.display(),
                    "global_rps: UDS reader spawned (cross-replica aggregation enabled)"
                );
            }
            Err(e) => {
                warn!(
                    err = %e,
                    path = %cfg.global_rps_uds_path.display(),
                    "global_rps: UDS reader spawn failed; cross-replica route will be omitted \
                     (fail-closed). Check that the path's parent directory exists and is writable."
                );
            }
        }
    } else {
        debug!("global_rps: INGRESS_RATE_LIMIT_AGGREGATION=false; cross-replica route disabled");
    }
    let renderer_shutdown = shutdown.clone();
    spawn_renderer(
        cfg.clone(),
        table.clone(),
        l4_table.clone(),
        caddy.clone(),
        traffic_cache_for_renderer,
        rate_limit_cache_for_renderer,
        quota_cache_for_renderer,
        tenant_rate_limit_cache_for_renderer,
        global_rps_cache_for_renderer,
        render_notify.clone(),
        renderer_shutdown,
    );
    let pruner_shutdown = shutdown.clone();
    spawn_pruner(
        table.clone(),
        l4_table.clone(),
        l4_ports.clone(),
        render_notify.clone(),
        cfg.clone(),
        pruner_shutdown,
    );

    // Push the initial empty config so Caddy's admin API has a known state
    // before the first heartbeat lands. (Otherwise Caddy might still be
    // serving its default config, e.g. `:2019` admin only.)
    let mut boot_previous: Option<PreviousState> = None;
    if let Err(e) = push_now(
        &cfg,
        &table,
        &l4_table,
        &caddy,
        &traffic_cache_for_push,
        &rate_limit_cache_for_push,
        &quota_cache_for_push,
        &tenant_rate_limit_cache_for_push,
        &global_rps_cache_for_push,
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
                let mut l4_ports_guard = l4_ports.lock().await;
                if apply_heartbeat(&table, &l4_table, &mut l4_ports_guard, &l4_port_cache, &hb)
                    .await
                {
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
///
/// `l4_port_cache` carries the CP-persisted `(tenant, app) → public_port`
/// mapping (issue #548). On a cold TCP heartbeat (no entry in `l4_table`
/// yet), we consult the cache before falling back to the ingress-local
/// `L4PortPool` — so two ingress instances in the same region that
/// both see a fresh TCP heartbeat for the same app agree on the public
/// port by reading it from the cache, instead of independently
/// allocating from each local pool.
pub async fn apply_heartbeat(
    table: &RoutingTable,
    l4_table: &L4RoutingTable,
    l4_ports: &mut L4PortPool,
    l4_port_cache: &SharedL4PortCache,
    hb: &HeartbeatMessage,
) -> bool {
    let worker_addr = hb.worker_addr.as_deref().unwrap_or("");
    if worker_addr.is_empty() {
        metrics::counter!("ingress.heartbeats.no_addr").increment(1);
        warn!("heartbeat has no worker_addr; cannot route any apps from it");
        return false;
    }

    // Empty apps map signals a final heartbeat (worker shutting down).
    // Remove all routes for this worker immediately instead of waiting
    // for the stale pruner. Both the HTTP and L4 tables are scanned —
    // a worker can host a mix of HTTP and L4 apps, and we want
    // symmetric eviction on shutdown. For L4 entries, the public port
    // is also released so it enters the cooldown window rather than
    // being held until the next l4_ports reap tick.
    if hb.apps.is_empty() {
        // Snapshot the L4 table BEFORE removal so we can find the
        // public_port for each evicted (tenant_id, app_name).
        let l4_before = l4_table.snapshot().await;
        let removed_http = table.remove_worker(worker_addr).await;
        let removed_l4 = l4_table.remove_worker(worker_addr).await;
        for entry in l4_before.iter() {
            if entry.worker_addr == worker_addr {
                l4_ports.release(entry.public_port);
            }
        }
        if !removed_http.is_empty() || !removed_l4.is_empty() {
            warn!(
                worker_addr = %worker_addr,
                http_removed = removed_http.len(),
                l4_removed = removed_l4.len(),
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
        let protocol = if app.protocol.is_empty() {
            "http"
        } else {
            app.protocol.as_str()
        };
        debug!(
            app = %app_name,
            deployment_id = %deployment_id.unwrap_or("(none)"),
            tenant = %app.tenant_id,
            worker_addr,
            port,
            protocol,
            status = %app.status,
            "updating route"
        );

        match protocol {
            "tcp" => {
                // L4 path (issue #548). Resolution order on the cold
                // path (no existing L4 entry yet):
                //   1. `l4_table` snapshot — re-heartbeats re-use the
                //      port they already picked.
                //   2. `l4_port_cache` — CP-persisted allocation from
                //      `apps.l4_public_port`. Wins when the CLI
                //      pre-allocated via `POST /api/v1/apps/{app}/l4-port`
                //      or when a previous ingress instance already
                //      ran `AllocateL4Port` and persisted it.
                //   3. `L4PortPool::acquire()` — local fallback for
                //      tenants that haven't pre-allocated. Two
                //      ingresses in one region will *race* on this
                //      branch; the CP persistence layer eliminates
                //      the race the moment a port is allocated via
                //      the API.
                let existing = l4_table.lookup(&app.tenant_id, app_name).await;
                // `Some(entry)` means the public port is already
                // mapped, but if the worker_addr on the cached entry
                // differs from the current heartbeat's worker_addr,
                // the existing route is stale (worker restarted with
                // a new IP / moved region) and we must re-resolve via
                // the cache + pool path below. Without this guard,
                // traffic would keep flowing to the OLD worker_addr
                // for up to STALE_TIMEOUT (60s) until pruner
                // catches up. See issue #548 review finding #29.
                let existing = existing.filter(|e| e.worker_addr == worker_addr);
                let public_port = match existing {
                    Some(entry) => entry.public_port,
                    None => {
                        // Step 2: consult the CP-persisted cache.
                        // `is_fresh` returns false on miss or on a
                        // stale entry (older than `L4_PORT_CACHE_STALE_AFTER`);
                        // both fall through to the local pool.
                        let cache_key = L4AppKey::new(&app.tenant_id, app_name);
                        let cached: Option<u16> = {
                            let r = l4_port_cache.read().await;
                            if r.is_fresh(&cache_key) {
                                r.get(&cache_key).map(|e| e.public_port)
                            } else {
                                None
                            }
                        };
                        match cached {
                            Some(p) => p,
                            None => match l4_ports.acquire() {
                                Some(p) => p,
                                None => {
                                    warn!(
                                        tenant = %app.tenant_id,
                                        app = %app_name,
                                        "no L4 public port available — pool exhausted; \
                                         not registering route. Operator action required: \
                                         widen L4_PORT_RANGE_END or reduce concurrent L4 apps."
                                    );
                                    continue;
                                }
                            },
                        }
                    }
                };
                l4_table
                    .upsert(
                        &app.tenant_id,
                        app_name,
                        public_port,
                        worker_addr,
                        port,
                        &app.status,
                    )
                    .await;
                changed = true;
            }
            _ => {
                // HTTP / WebSocket path — the pre-#548 default.
                // Weight is not in the heartbeat — the ingress
                // fetches traffic splits from the control plane
                // API at render time. Default to 100 so a single
                // deployment always gets full traffic.
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

                // If the heartbeat carries a WebSocket port, insert
                // a second route entry so Caddy can route
                // `Upgrade: websocket` requests to the correct
                // upstream port (issue #312). Caddy's reverse_proxy
                // natively handles WebSocket transparently.
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
        }
    }
    changed
}

/// Snapshot of the routing table and FQDN bindings from the last
/// successful Caddy config push. Used to compute incremental diffs.
///
/// `l4_entries` is tracked separately (issue #548) so the diff
/// logic can compare the last-pushed L4 snapshot against the
/// current one (`prev.l4_entries != l4_snap`). When L4 changes,
/// the renderer falls back to the full `render_full` path
/// (caddy-l4 only supports `POST /load`, no incremental L4 patch
/// path). Without this field the renderer would always take the
/// full path even when only HTTP routes moved, doubling the
/// rendered payload size on every HTTP-only delta.
struct PreviousState {
    route_entries: Vec<RouteEntry>,
    fqdn_bindings: Vec<FqdnBinding>,
    /// Last-pushed L4 entries (issue #548). Compared against the
    /// current `l4_table.snapshot()` on every render tick; if the
    /// sets differ, `render_full` re-emits the entire `apps.layer4`
    /// block.
    l4_entries: Vec<L4RouteEntry>,
    /// Last-pushed cross-replica global RPS cap (issue #665 PR D).
    /// `None` ⇒ no global route was emitted on the last render
    /// (cold cache, sidecar disabled, or stale). When this flips
    /// between `None` and `Some(cap)` the renderer must take the
    /// full `render_full` path — the incremental route-id patch
    /// can't add or remove a brand-new `@id` from Caddy's config.
    /// Without this field the `None → Some(7_500)` transition (cold
    /// start of the sidecar) would never propagate to Caddy until
    /// the next heartbeat-driven render — operator-visible
    /// regression. Compared by value; cap drift also forces a full
    /// reload so the new `rps` reaches Caddy without waiting for
    /// a >20% HTTP delta.
    global_rps_cap: Option<u32>,
}

#[allow(clippy::too_many_arguments)]
fn spawn_renderer(
    cfg: Config,
    table: Arc<RoutingTable>,
    l4_table: Arc<L4RoutingTable>,
    caddy: Arc<CaddyClient>,
    traffic_cache: SharedCache,
    rate_limit_cache: SharedRateLimitCache,
    quota_cache: SharedQuotaCache,
    tenant_rate_limit_cache: SharedTenantRateLimitCache,
    global_rps_cache: SharedGlobalRpsCache,
    notify: Arc<Notify>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut previous: Option<PreviousState> = None;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("renderer: shutdown signal received; performing final push");
                    let _ = push_now(&cfg, &table, &l4_table, &caddy, &traffic_cache, &rate_limit_cache, &quota_cache, &tenant_rate_limit_cache, &global_rps_cache, &mut previous).await;
                    break;
                }
                _ = notify.notified() => {
                    // Coalesce bursty notifications: sleep the debounce, then push.
                    sleep(Duration::from_millis(cfg.refresh_debounce_ms)).await;
                    if let Err(e) = push_now(
                        &cfg,
                        &table,
                        &l4_table,
                        &caddy,
                        &traffic_cache,
                        &rate_limit_cache,
                        &quota_cache,
                        &tenant_rate_limit_cache,
                        &global_rps_cache,
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
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn spawn_pruner(
    table: Arc<RoutingTable>,
    l4_table: Arc<L4RoutingTable>,
    l4_ports: Arc<tokio::sync::Mutex<L4PortPool>>,
    notify: Arc<Notify>,
    cfg: Config,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut ticker = interval(cfg.prune_interval);
        // Skip the first immediate tick.
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("pruner: shutdown signal received, stopping");
                    break;
                }
                _ = ticker.tick() => {
                    let removed_http = table.remove_stale(cfg.stale_timeout).await;
                    // Issue #548: prune the L4 table in lock-step
                    // with the HTTP table. CRITICAL: snapshot the
                    // L4 table BEFORE remove_stale runs — once
                    // remove_stale mutates the table, snapshot()
                    // returns only the SURVIVING entries, so any
                    // port-release loop over the post-remove snapshot
                    // would never see the entries we just removed
                    // and the public ports would leak (the pool's
                    // cooldown window never fires → no slot is
                    // reclaimable until restart). The bug previously
                    // left L4 ports stuck forever after a worker
                    // disappeared, masking as "ports got tight but
                    // the worker came back".
                    let l4_snapshot_before = l4_table.snapshot().await;
                    let removed_l4 = l4_table.remove_stale(cfg.stale_timeout).await;
                    let mut l4_ports_guard = l4_ports.lock().await;
                    for entry in l4_snapshot_before.iter() {
                        if removed_l4
                            .iter()
                            .any(|k| k.tenant_id == entry.tenant_id && k.app_name == entry.app_name)
                        {
                            l4_ports_guard.release(entry.public_port);
                        }
                    }
                    drop(l4_ports_guard);
                    if !removed_http.is_empty() || !removed_l4.is_empty() {
                        metrics::counter!("ingress.pruner.removed_total")
                            .increment((removed_http.len() + removed_l4.len()) as u64);
                        warn!(
                            http_removed = removed_http.len(),
                            l4_removed = removed_l4.len(),
                            "pruned stale routes"
                        );
                        notify.notify_one();
                    }
                }
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn push_now(
    cfg: &Config,
    table: &RoutingTable,
    l4_table: &L4RoutingTable,
    caddy: &CaddyClient,
    traffic_cache: &SharedCache,
    rate_limit_cache: &SharedRateLimitCache,
    quota_cache: &SharedQuotaCache,
    tenant_rate_limit_cache: &SharedTenantRateLimitCache,
    global_rps_cache: &SharedGlobalRpsCache,
    previous: &mut Option<PreviousState>,
) -> Result<()> {
    let snap: Vec<RouteEntry> = table.snapshot().await;
    let fqdns = table.fqdn_snapshot().await;
    let l4_snap = l4_table.snapshot().await;
    let traffic_cache_guard = traffic_cache.read().await;
    let rate_limit_cache_guard = rate_limit_cache.read().await;
    let quota_cache_guard = quota_cache.read().await;
    let tenant_rate_limit_cache_guard = tenant_rate_limit_cache.read().await;
    let global_rps_cache_guard = global_rps_cache.read().await;
    // Snapshot the current cross-replica cap BEFORE taking the
    // `previous` match — we need this both to feed the renderer and
    // to record into `PreviousState` so the next render's diff logic
    // can detect a `None ↔ Some(cap)` transition (see PreviousState
    // doc on `global_rps_cap`).
    let current_global_rps_cap =
        global_rps_cache_guard.current_local_cap(2 * cfg.global_rps_tick_interval);

    // Set gauges from current state.
    metrics::gauge!("ingress.routes.active").set(snap.len() as f64);
    metrics::gauge!("ingress.fqdns.active").set(fqdns.len() as f64);
    metrics::gauge!("ingress.l4.routes_active").set(l4_snap.len() as f64);

    match previous.take() {
        None => {
            // Boot push — full config POST /load. Issue #548:
            // always include both HTTP + L4 trees; the HTTP tree
            // comes from `render_routes` (unchanged) and the L4
            // tree from `render_l4_routes` (via `render_full`).
            let render_start = std::time::Instant::now();
            let http_payload = render_routes(
                &snap,
                &fqdns,
                cfg,
                &traffic_cache_guard,
                &rate_limit_cache_guard,
                &quota_cache_guard,
                &tenant_rate_limit_cache_guard,
                &global_rps_cache_guard,
            );
            let json = render_full(http_payload, &l4_snap, cfg, &quota_cache_guard);
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
                        l4_entries: l4_snap,
                        global_rps_cap: current_global_rps_cap,
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
            // Issue #548: ANY L4 delta always forces a full reload —
            // `apps.layer4` only supports `POST /load`, not the
            // per-id patch path used by the HTTP tree. The HTTP
            // >20% heuristic above is the *additional* gate that
            // can trigger a full reload on the HTTP side too.
            //
            // Issue #665 PR D: ANY change in `global_rps_cap` also
            // forces a full reload — the incremental route-id patch
            // can't add or remove a brand-new `@id` from Caddy's
            // config, so the `None → Some(cap)` cold-start transition
            // (and the `Some(cap1) → Some(cap2)` cap-drift transition
            // that lets the new rps reach Caddy without waiting for
            // an HTTP delta) both require the full `render_full`
            // path.
            let l4_changed = prev.l4_entries != l4_snap;
            let global_rps_changed = prev.global_rps_cap != current_global_rps_cap;
            if l4_changed || global_rps_changed || total_changes * 5 > total {
                let render_start = std::time::Instant::now();
                let http_payload = render_routes(
                    &snap,
                    &fqdns,
                    cfg,
                    &traffic_cache_guard,
                    &rate_limit_cache_guard,
                    &quota_cache_guard,
                    &tenant_rate_limit_cache_guard,
                    &global_rps_cache_guard,
                );
                let json = render_full(http_payload, &l4_snap, cfg, &quota_cache_guard);
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
                            l4_entries: l4_snap,
                            global_rps_cap: current_global_rps_cap,
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
                    l4_entries: l4_snap,
                    global_rps_cap: current_global_rps_cap,
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
///
/// Mirrors the per-app handle chain in `caddy.rs:408-429` so the
/// incremental-push path emits the same per-app `rate_limit`
/// handler when the bulk `render_routes` does. Without this, an app
/// updated via `upsert_route` (route changed in `diff_routes`) would
/// land WITHOUT a `rate_limit` handler even though the equivalent
/// entry on a full reload would have one — that's the asymmetry
/// this fix removes. See issue #305 commit 5 for the design.
fn render_single_route(
    entry: &RouteEntry,
    cfg: &Config,
    traffic_cache: &crate::traffic::TrafficSplitCache,
    rate_limit_cache: &crate::ratelimit::RateLimitCache,
) -> serde_json::Value {
    let host = crate::config::ingress_host(&entry.tenant_id, &entry.app_name);
    let dial = format!("{}:{}", entry.worker_addr, entry.port);

    // Resolve the per-app weight from the traffic-split cache when
    // multiple deployments exist for this (tenant, app); the bulk
    // path at `caddy.rs:355-377` already does this for the full
    // reload. Mirroring it here keeps the incremental route shape
    // identical to the bulk shape so an admin-driven weight update
    // (canary split) takes effect whether the change rides an
    // upsert or a full reload.
    let weight = entry
        .deployment_id
        .as_ref()
        .and_then(|did| traffic_cache.weight(&entry.tenant_id, &entry.app_name, did))
        .unwrap_or(entry.weight);

    // Resolve effective per-app rate limit (priority: per-app cache
    // > RouteEntry field > Config default). Same priority chain as
    // `caddy.rs:382-406`. Keeping this duplicate free of the
    // multi-deployment grouping logic — the incremental path only
    // touches a single entry at a time.
    let cached = rate_limit_cache.get(&entry.tenant_id, &entry.app_name);
    let rps = cached
        .map(|e| e.rps)
        .or(entry.rate_limit_rps)
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
        .or(entry.rate_limit_burst)
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
    if rps > 0 {
        let burst = if burst > 0 { burst } else { rps };
        handle_chain.push(serde_json::json!({
            "handler": "rate_limit",
            "rates": { "rps": rps, "burst": burst },
            "key": "{http.request.host}",
        }));
    }

    handle_chain.push(serde_json::json!({
        "handler": "reverse_proxy",
        "upstreams": [{"dial": dial, "weight": weight}],
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
///
/// Mirrors `render_single_route` so the incremental FQDN upsert
/// path emits the same per-app `rate_limit` handler chain as the
/// bulk `render_routes` path. Without this, an admin-driven FQDN
/// binding update would land WITHOUT a `rate_limit` handler on the
/// incremental path even though the bulk path would have one.
/// See issue #305 commit 5 for the design.
fn render_fqdn_route(
    binding: &FqdnBinding,
    entries: &[RouteEntry],
    cfg: &Config,
    rate_limit_cache: &crate::ratelimit::RateLimitCache,
) -> Option<serde_json::Value> {
    let upstream = entries
        .iter()
        .find(|e| e.tenant_id == binding.tenant_id && e.app_name == binding.app_name)?;
    let dial = format!("{}:{}", upstream.worker_addr, upstream.port);

    // Resolve effective per-app rate limit. Same priority chain as
    // `render_single_route`. The FQDN variant skips the traffic-split
    // weight lookup because FQDN bindings resolve to a single
    // (tenant, app) upstream — the bulk path at `caddy.rs:345-432`
    // groups by app_name before applying traffic weight, so a single
    // upstream carries `weight: 100` (the heartbeat default) and the
    // per-app rate limit is what matters here.
    let cached = rate_limit_cache.get(&upstream.tenant_id, &upstream.app_name);
    let rps = cached
        .map(|e| e.rps)
        .or(upstream.rate_limit_rps)
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
        .or(upstream.rate_limit_burst)
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
    if rps > 0 {
        let burst = if burst > 0 { burst } else { rps };
        handle_chain.push(serde_json::json!({
            "handler": "rate_limit",
            "rates": { "rps": rps, "burst": burst },
            "key": "{http.request.host}",
        }));
    }

    handle_chain.push(serde_json::json!({
        "handler": "reverse_proxy",
        "upstreams": [{"dial": dial}],
        "health_checks": {
            "active": {"uri": "/", "expect_status": 2}
        }
    }));
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

    /// Fresh in-memory HTTP table + L4 routing table + L4 port pool,
    /// with a 1000-port range. The pool is returned bare (not
    /// wrapped in `Mutex`) so tests can drive `apply_heartbeat`
    /// directly; production wires it through a `Mutex` because the
    /// heartbeat loop holds a `&mut` reference for the duration of
    /// every message. Tests are single-message so no contention
    /// arises.
    fn fresh_tables() -> (
        Arc<RoutingTable>,
        Arc<L4RoutingTable>,
        L4PortPool,
        SharedL4PortCache,
    ) {
        (
            Arc::new(RoutingTable::new()),
            Arc::new(L4RoutingTable::new()),
            L4PortPool::new(31000, 31999, 60),
            Default::default(),
        )
    }

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
            dedupe_id: None,
            resident_seconds: None,
            observer_metrics: vec![],
            last_error: None,
            // Issue #548: HTTP-by-default for the existing test suite.
            // The L4 branch (protocol="tcp") is covered by a follow-up
            // commit's `apply_heartbeat_tcp_protocol` test once the
            // L4RoutingTable lands.
            protocol: "http".to_string(),
            duration_ms_total: 0,
            // Issue #84 ask 6/7: per-deployment 5xx counter mirrored
            // from the worker's heartbeat AppStatus. Ingress doesn't
            // act on it — it just needs to deserialize the wire shape
            // without a missing-field error.
            status_5xx_count: 0,
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
            port_pool_exhausted_count: 0,
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
            port_pool_exhausted_count: 0,
        }
    }

    // ── Existing tests ────────────────────────────────────────────────

    /// A heartbeat with `worker_addr: None` must NOT mutate the routing
    /// table, and `apply_heartbeat` must return `false` so the caller skips
    /// the Caddy-reload notify.
    #[tokio::test]
    async fn handle_one_skips_when_worker_addr_is_none() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        let hb = hb_no_addr(apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(!changed);
        assert_eq!(table.len().await, 0);
    }

    /// Same expectation for an empty-string `worker_addr`.
    #[tokio::test]
    async fn handle_one_skips_when_worker_addr_is_empty_string() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
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
            port_pool_exhausted_count: 0,
        };

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(!changed);
        assert_eq!(table.len().await, 0);
    }

    /// Happy-path: with a valid `worker_addr`, `apply_heartbeat` inserts
    /// one route per app and returns `true`.
    #[tokio::test]
    async fn handle_one_inserts_route_when_worker_addr_present() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
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
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        apps.insert("api:v2".to_string(), app_status("t_a", "running", 8081));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].app_name, "api");
        assert_eq!(snap[0].deployment_id, Some("v2".to_string()));
    }

    /// Non-"running" status removes the entry.
    #[tokio::test]
    async fn apply_heartbeat_with_non_running_status() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "crashed", 8081));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(changed);
        // The "crashed" app causes an upsert with that status, which
        // the routing table interprets as "remove".
        assert_eq!(table.len().await, 0);
    }

    /// "draining" status keeps the route with weight=0.
    #[tokio::test]
    async fn apply_heartbeat_with_draining_status() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "draining", 8081));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
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
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        // First insert a route via normal heartbeat.
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        let hb1 = hb_with_addr("203.0.113.10", apps);
        assert!(apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb1).await);
        assert_eq!(table.len().await, 1);

        // Empty heartbeat from same worker removes all routes.
        let hb2 = hb_with_addr("203.0.113.10", HashMap::new());
        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb2).await;
        assert!(changed);
        assert_eq!(table.len().await, 0);
    }

    /// Unknown worker with empty apps is a no-op.
    #[tokio::test]
    async fn apply_heartbeat_empty_apps_unknown_worker_noop() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let hb = hb_with_addr("203.0.113.10", HashMap::new());
        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(!changed);
        assert_eq!(table.len().await, 0);
    }

    /// Multiple apps in a single heartbeat — both upserted.
    #[tokio::test]
    async fn apply_heartbeat_with_multiple_apps() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        apps.insert("worker".to_string(), app_status("t_a", "running", 8082));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 2);
    }

    /// Empty apps map — returns false, no mutation.
    #[tokio::test]
    async fn apply_heartbeat_with_empty_apps() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let hb = hb_with_addr("203.0.113.10", HashMap::new());

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(!changed);
        assert_eq!(table.len().await, 0);
    }

    /// WebSocket port creates a second route entry with `-ws` suffix.
    #[tokio::test]
    async fn apply_heartbeat_with_ws_port_creates_second_route() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.ws_port = Some(9091);
        apps.insert("api".to_string(), status);
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
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
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.ws_port = Some(9091);
        apps.insert("api:v2".to_string(), status);
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
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
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        apps.insert("api".to_string(), app_status("t_a", "running", 8081));
        apps.insert("cron".to_string(), app_status("t_a", "crashed", 8082));
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(changed);
        let snap = table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].app_name, "api");
    }

    // ── L4 protocol-branch tests (issue #548) ─────────────────────────

    /// `protocol = "tcp"` routes the app into the L4 table, not the
    /// HTTP table. The HTTP table stays empty.
    #[tokio::test]
    async fn apply_heartbeat_tcp_protocol_inserts_l4_route() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.protocol = "tcp".to_string();
        apps.insert("api".to_string(), status);
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(changed);
        assert_eq!(table.len().await, 0, "HTTP table stays empty");
        let snap = l4_table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(
            snap[0].public_port, 31000,
            "first allocate lands on the start port"
        );
        assert_eq!(snap[0].worker_addr, "203.0.113.10");
        assert_eq!(snap[0].upstream_port, 8081);
        assert_eq!(snap[0].tenant_id, "t_a");
        assert_eq!(snap[0].app_name, "api");
    }

    /// A re-heartbeat for the same L4 app reuses the existing
    /// public port rather than walking to the next one. This is the
    /// contract that lets restart-stable port allocation work.
    #[tokio::test]
    async fn apply_heartbeat_tcp_reuses_existing_public_port() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.protocol = "tcp".to_string();
        apps.insert("api".to_string(), status.clone());
        let hb1 = hb_with_addr("203.0.113.10", apps);
        assert!(apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb1).await);
        let first_port = l4_table.snapshot().await[0].public_port;

        // Second heartbeat for the same app from the SAME
        // worker_addr. The public_port is reused. Note (issue #548
        // review finding #29): a heartbeat from a DIFFERENT
        // worker_addr falls through to the cache + pool path and
        // gets a FRESH port — covered by
        // `apply_heartbeat_tcp_worker_addr_change_allocates_new_port`.
        let mut apps2 = HashMap::new();
        apps2.insert("api".to_string(), status);
        let hb2 = hb_with_addr("203.0.113.10", apps2);
        let _ = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb2).await;
        let snap = l4_table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(
            snap[0].public_port, first_port,
            "public port preserved across heartbeat"
        );
    }

    /// When the worker_addr changes between heartbeats (worker
    /// restart with a new IP), the lookup no longer matches —
    /// issue #548 review finding #29. The ingress allocates a FRESH
    /// port instead of reusing the stale one, so traffic
    /// immediately flows to the new worker instead of dangling on
    /// the dead address for up to STALE_TIMEOUT. The old port is
    /// released into cooldown via the subsequent remove_worker
    /// path (covered separately).
    #[tokio::test]
    async fn apply_heartbeat_tcp_worker_addr_change_allocates_new_port() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.protocol = "tcp".to_string();
        apps.insert("api".to_string(), status.clone());
        let hb1 = hb_with_addr("203.0.113.10", apps);
        assert!(apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb1).await);
        let first_port = l4_table.snapshot().await[0].public_port;

        // Second heartbeat from a DIFFERENT worker_addr. The existing
        // entry is invalidated (worker restart with new IP), so a
        // fresh port is allocated.
        let mut apps2 = HashMap::new();
        apps2.insert("api".to_string(), status);
        let hb2 = hb_with_addr("198.51.100.7", apps2);
        let _ = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb2).await;
        let snap = l4_table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_ne!(
            snap[0].public_port, first_port,
            "worker_addr change must allocate a fresh port (issue #548 #29)"
        );
        assert_eq!(snap[0].worker_addr, "198.51.100.7");
    }

    /// `protocol = ""` (missing) is treated as `"http"` — backwards
    /// compatible with pre-#548 heartbeats that don't carry the
    /// field.
    #[tokio::test]
    async fn apply_heartbeat_missing_protocol_defaults_to_http() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.protocol = String::new();
        apps.insert("api".to_string(), status);
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(changed);
        assert_eq!(table.len().await, 1, "missing protocol → HTTP route");
        assert_eq!(l4_table.len().await, 0, "L4 table untouched");
    }

    /// Mixed HTTP + TCP in a single heartbeat: each app lands in
    /// the right table based on its protocol.
    #[tokio::test]
    async fn apply_heartbeat_mixed_http_and_tcp_protocols() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        let mut apps = HashMap::new();
        apps.insert("web".to_string(), app_status("t_a", "running", 8081));
        let mut tcp_app = app_status("t_a", "running", 8082);
        tcp_app.protocol = "tcp".to_string();
        apps.insert("redis".to_string(), tcp_app);
        let hb = hb_with_addr("203.0.113.10", apps);

        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(changed);
        assert_eq!(table.len().await, 1);
        assert_eq!(table.snapshot().await[0].app_name, "web");
        assert_eq!(l4_table.len().await, 1);
        assert_eq!(l4_table.snapshot().await[0].app_name, "redis");
    }

    /// Empty heartbeat removes routes from BOTH tables for the
    /// worker. L4 ports are released back to the pool's cooldown
    /// window so they can be re-acquired after the cooldown elapses.
    #[tokio::test]
    async fn apply_heartbeat_empty_apps_clears_both_tables() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        // Seed both tables with entries for the same worker.
        // For the L4 path, acquire the port properly so the pool's
        // bookkeeping (taken set) is in the same state that
        // production reaches via `apply_heartbeat` → `l4_ports.acquire()`.
        table
            .upsert("t_a", "web", None, 100, "1.2.3.4", 8081, "running")
            .await;
        let l4_port = l4_ports.acquire().unwrap();
        l4_table
            .upsert("t_a", "redis", l4_port, "1.2.3.4", 8082, "running")
            .await;
        assert_eq!(table.len().await, 1);
        assert_eq!(l4_table.len().await, 1);

        // Empty heartbeat from the same worker → both evicted.
        let hb = hb_with_addr("1.2.3.4", HashMap::new());
        let changed = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        assert!(changed);
        assert_eq!(table.len().await, 0);
        assert_eq!(l4_table.len().await, 0);
        // The public port entered cooldown so a fresh acquire walks
        // past it until the cooldown elapses.
        assert!(l4_ports.is_in_cooldown(l4_port));
    }

    /// An L4 app with `status != "running"` removes the entry from
    /// the L4 table — same shape as HTTP non-running statuses.
    #[tokio::test]
    async fn apply_heartbeat_tcp_crashed_removes_l4_entry() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        // First insert a running L4 route.
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.protocol = "tcp".to_string();
        apps.insert("api".to_string(), status);
        let hb1 = hb_with_addr("203.0.113.10", apps);
        assert!(apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb1).await);
        assert_eq!(l4_table.len().await, 1);

        // Then update with a crashed status → entry removed.
        let mut apps2 = HashMap::new();
        let mut crashed = app_status("t_a", "crashed", 8081);
        crashed.protocol = "tcp".to_string();
        apps2.insert("api".to_string(), crashed);
        let hb2 = hb_with_addr("203.0.113.10", apps2);
        let _ = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb2).await;
        assert_eq!(l4_table.len().await, 0);
    }

    // ── L4PortCache-consult tests (issue #548 Commit 9) ───────────────

    /// Pre-populated cache: a TCP heartbeat on the cold path uses the
    /// CP-persisted public port from the cache instead of allocating
    /// from the local pool. This is the contract that lets two
    /// ingress instances in the same region agree on a public port
    /// when one operator pre-allocates via `POST /api/v1/apps/{app}/l4-port`.
    #[tokio::test]
    async fn apply_heartbeat_tcp_uses_cached_public_port() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        // Pre-seed the cache with a CP-persisted allocation that does NOT
        // match what the local pool would hand out (the pool starts at
        // 31000; cache says 31999).
        {
            let mut w = l4_port_cache.write().await;
            w.update(crate::l4::L4AppKey::new("t_a", "api"), 31999);
        }
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.protocol = "tcp".to_string();
        apps.insert("api".to_string(), status);
        let hb = hb_with_addr("203.0.113.10", apps);

        let _ = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        let snap = l4_table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(
            snap[0].public_port, 31999,
            "cache hit must win over local pool"
        );
        // The local pool must NOT have been touched — no port was acquired.
        assert_eq!(
            l4_ports.taken_count(),
            0,
            "cache hit path must skip the local pool"
        );
    }

    /// Cache miss (no entry yet) → fallback to local pool's first
    /// available port. Pre-CLI-pre-allocation tenants still get a
    /// routable port.
    #[tokio::test]
    async fn apply_heartbeat_tcp_falls_back_to_pool_on_cache_miss() {
        let (table, l4_table, mut l4_ports, l4_port_cache) = fresh_tables();
        // Cache is empty (no allocation yet).
        let mut apps = HashMap::new();
        let mut status = app_status("t_a", "running", 8081);
        status.protocol = "tcp".to_string();
        apps.insert("api".to_string(), status);
        let hb = hb_with_addr("203.0.113.10", apps);

        let _ = apply_heartbeat(&table, &l4_table, &mut l4_ports, &l4_port_cache, &hb).await;
        let snap = l4_table.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(
            snap[0].public_port, 31000,
            "cache miss path acquires the first pool port"
        );
        assert_eq!(
            l4_ports.taken_count(),
            1,
            "local pool must have allocated the port"
        );
    }

    // ── push_now tests ────────────────────────────────────────────────

    fn test_config(admin_url: &str) -> Config {
        Config {
            nats_url: "nats://localhost:4222".into(),
            caddy_admin_url: admin_url.to_string(),
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
            ingress_rate_limit_aggregation: false,
            global_rps_uds_path: std::path::PathBuf::from("/var/run/edge-ingress/global-rps.sock"),
            global_rps_tick_interval: std::time::Duration::from_secs(1),
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
        let (table, l4_table, _l4_ports, _l4_port_cache) = fresh_tables();
        let caddy = Arc::new(CaddyClient::new(&server.uri(), None).unwrap());
        let cache: SharedCache = Default::default();
        let rl_cache: SharedRateLimitCache = Default::default();
        let q_cache: SharedQuotaCache = Default::default();
        let tenant_rl_cache: SharedTenantRateLimitCache = Default::default();
        let global_rps_cache: SharedGlobalRpsCache = Default::default();

        push_now(
            &cfg,
            &table,
            &l4_table,
            &caddy,
            &cache,
            &rl_cache,
            &q_cache,
            &tenant_rl_cache,
            &global_rps_cache,
            &mut None,
        )
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
        let (table, l4_table, _l4_ports, _l4_port_cache) = fresh_tables();
        let caddy = Arc::new(CaddyClient::new(&server.uri(), None).unwrap());
        let cache: SharedCache = Default::default();
        let rl_cache: SharedRateLimitCache = Default::default();
        let q_cache: SharedQuotaCache = Default::default();
        let tenant_rl_cache: SharedTenantRateLimitCache = Default::default();
        let global_rps_cache: SharedGlobalRpsCache = Default::default();

        let err = push_now(
            &cfg,
            &table,
            &l4_table,
            &caddy,
            &cache,
            &rl_cache,
            &q_cache,
            &tenant_rl_cache,
            &global_rps_cache,
            &mut None,
        )
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
        let (table, l4_table, _l4_ports, _l4_port_cache) = fresh_tables();
        let caddy = Arc::new(CaddyClient::new(&server.uri(), None).unwrap());
        let cache: SharedCache = Default::default();
        let rl_cache: SharedRateLimitCache = Default::default();
        let q_cache: SharedQuotaCache = Default::default();
        let tenant_rl_cache: SharedTenantRateLimitCache = Default::default();
        let global_rps_cache: SharedGlobalRpsCache = Default::default();
        let notify = Arc::new(Notify::new());

        // Notify BEFORE spawn: tokio::sync::Notify stores a pending
        // notification when no task is waiting, so the spawned task's
        // first notified().await will observe it immediately.
        notify.notify_one();

        spawn_renderer(
            cfg,
            table,
            l4_table,
            caddy,
            cache,
            rl_cache,
            q_cache,
            tenant_rl_cache,
            global_rps_cache,
            notify.clone(),
            CancellationToken::new(),
        );

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

    // ── render_single_route / render_fqdn_route tests
    //    (issue #305 commit 5 — per-app rate-limit handler symmetry). ──

    use crate::ratelimit::RateLimitCache;
    use crate::ratelimit::RateLimitEntry;
    use crate::routing::FqdnBinding;

    fn route_entry_with_rl(
        tenant: &str,
        app: &str,
        rps: Option<u32>,
        burst: Option<u32>,
    ) -> RouteEntry {
        RouteEntry {
            tenant_id: tenant.to_string(),
            app_name: app.to_string(),
            deployment_id: None,
            weight: 100,
            worker_addr: "1.2.3.4".to_string(),
            port: 8081,
            rate_limit_rps: rps,
            rate_limit_burst: burst,
            last_seen: std::time::Instant::now(),
        }
    }

    /// No cap sources → no `rate_limit` handler. The incremental
    /// path stays symmetric with the bulk path (caddy.rs:411 — only
    /// inject when `rps > 0`).
    #[test]
    fn render_single_route_omits_rate_limit_when_no_caps() {
        let entry = route_entry_with_rl("t_a", "api", None, None);
        let cfg = test_config("http://localhost:2019");
        let traffic: crate::traffic::TrafficSplitCache = Default::default();
        let rl: RateLimitCache = Default::default();
        let route = render_single_route(&entry, &cfg, &traffic, &rl);

        let handle_chain = &route["handle"][0]["routes"][0]["handle"];
        assert_eq!(handle_chain.as_array().unwrap().len(), 1);
        assert_eq!(handle_chain[0]["handler"], "reverse_proxy");
    }

    /// `RouteEntry.rate_limit_rps` set → inject `rate_limit`
    /// handler before the `reverse_proxy`, with `burst` carried
    /// verbatim. Mirrors `caddy.rs:411-421`.
    #[test]
    fn render_single_route_inlines_rate_limit_handler_from_entry() {
        let entry = route_entry_with_rl("t_a", "api", Some(100), Some(200));
        let cfg = test_config("http://localhost:2019");
        let traffic: crate::traffic::TrafficSplitCache = Default::default();
        let rl: RateLimitCache = Default::default();
        let route = render_single_route(&entry, &cfg, &traffic, &rl);

        let handle_chain = &route["handle"][0]["routes"][0]["handle"];
        let arr = handle_chain.as_array().unwrap();
        assert_eq!(arr.len(), 2, "expected rate_limit + reverse_proxy");
        assert_eq!(arr[0]["handler"], "rate_limit");
        assert_eq!(arr[0]["rates"]["rps"], 100);
        assert_eq!(arr[0]["rates"]["burst"], 200);
        assert_eq!(arr[0]["key"], "{http.request.host}");
        assert_eq!(arr[1]["handler"], "reverse_proxy");
    }

    /// Cache entry wins over the RouteEntry field (priority:
    /// cached → entry → cfg default). Mirrors `caddy.rs:382-406`.
    #[test]
    fn render_single_route_cache_overrides_entry_field() {
        let entry = route_entry_with_rl("t_a", "api", Some(50), Some(60));
        let mut rl = RateLimitCache::default();
        rl.update(
            "t_a".into(),
            "api".into(),
            RateLimitEntry {
                rps: 500,
                burst: 600,
            },
        );
        let cfg = test_config("http://localhost:2019");
        let traffic: crate::traffic::TrafficSplitCache = Default::default();
        let route = render_single_route(&entry, &cfg, &traffic, &rl);

        let arr = &route["handle"][0]["routes"][0]["handle"]
            .as_array()
            .unwrap();
        assert_eq!(arr[0]["rates"]["rps"], 500);
        assert_eq!(arr[0]["rates"]["burst"], 600);
    }

    /// Config default kicks in when neither cache nor entry supply
    /// a cap. Mirrors `caddy.rs:387-393`. Without this fallback the
    /// incremental path would silently drop the default-on cap that
    /// the bulk path applies — same bug class the asymmetric fix
    /// targets.
    #[test]
    fn render_single_route_uses_config_default_when_no_caps_present() {
        let entry = route_entry_with_rl("t_a", "api", None, None);
        let mut cfg = test_config("http://localhost:2019");
        cfg.rate_limit_rps_default = 25;
        cfg.rate_limit_burst_default = 50;
        let traffic: crate::traffic::TrafficSplitCache = Default::default();
        let rl: RateLimitCache = Default::default();
        let route = render_single_route(&entry, &cfg, &traffic, &rl);

        let arr = &route["handle"][0]["routes"][0]["handle"]
            .as_array()
            .unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["rates"]["rps"], 25);
        assert_eq!(arr[0]["rates"]["burst"], 50);
    }

    /// `burst == 0` falls back to `rps`, matching the bulk
    /// `caddy.rs:412` semantics — so a cap with no explicit burst
    /// still emits a sane Burst value rather than `0` (which Caddy
    /// would treat as "reject every request").
    #[test]
    fn render_single_route_burst_falls_back_to_rps_when_zero() {
        let entry = route_entry_with_rl("t_a", "api", Some(100), Some(0));
        let cfg = test_config("http://localhost:2019");
        let traffic: crate::traffic::TrafficSplitCache = Default::default();
        let rl: RateLimitCache = Default::default();
        let route = render_single_route(&entry, &cfg, &traffic, &rl);

        let arr = &route["handle"][0]["routes"][0]["handle"]
            .as_array()
            .unwrap();
        assert_eq!(arr[0]["rates"]["rps"], 100);
        assert_eq!(
            arr[0]["rates"]["burst"], 100,
            "burst=0 should fall back to rps"
        );
    }

    /// No caps → no `rate_limit` handler in the FQDN path. Same
    /// symmetry expectation as the per-app path.
    #[test]
    fn render_fqdn_route_omits_rate_limit_when_no_caps() {
        let binding = FqdnBinding {
            fqdn: "custom.example.com".into(),
            tenant_id: "t_a".into(),
            app_name: "api".into(),
        };
        let entry = route_entry_with_rl("t_a", "api", None, None);
        let cfg = test_config("http://localhost:2019");
        let rl: RateLimitCache = Default::default();
        let route = render_fqdn_route(&binding, std::slice::from_ref(&entry), &cfg, &rl)
            .expect("render_fqdn_route returns Some");

        let handle_chain = &route["handle"][0]["routes"][0]["handle"];
        assert_eq!(handle_chain.as_array().unwrap().len(), 1);
        assert_eq!(handle_chain[0]["handler"], "reverse_proxy");
    }

    /// Caps → inject the `rate_limit` handler in the FQDN path.
    /// Same expectation as `render_single_route`.
    #[test]
    fn render_fqdn_route_inlines_rate_limit_handler() {
        let binding = FqdnBinding {
            fqdn: "custom.example.com".into(),
            tenant_id: "t_a".into(),
            app_name: "api".into(),
        };
        let entry = route_entry_with_rl("t_a", "api", Some(100), Some(200));
        let cfg = test_config("http://localhost:2019");
        let rl: RateLimitCache = Default::default();
        let route = render_fqdn_route(&binding, std::slice::from_ref(&entry), &cfg, &rl)
            .expect("render_fqdn_route returns Some");

        let arr = &route["handle"][0]["routes"][0]["handle"]
            .as_array()
            .unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["handler"], "rate_limit");
        assert_eq!(arr[0]["rates"]["rps"], 100);
        assert_eq!(arr[0]["rates"]["burst"], 200);
        // FQDN variant must carry the on_demand TLS block so ACME
        // still kicks in for unknown hosts.
        assert_eq!(route["tls"]["on_demand"], serde_json::json!({}));
    }

    /// FQDN variant cache priority: cached → entry → cfg default.
    /// Mirrors the per-app chain.
    #[test]
    fn render_fqdn_route_cache_overrides_entry_field() {
        let binding = FqdnBinding {
            fqdn: "custom.example.com".into(),
            tenant_id: "t_a".into(),
            app_name: "api".into(),
        };
        let entry = route_entry_with_rl("t_a", "api", Some(50), Some(60));
        let mut rl = RateLimitCache::default();
        rl.update(
            "t_a".into(),
            "api".into(),
            RateLimitEntry {
                rps: 999,
                burst: 1999,
            },
        );
        let cfg = test_config("http://localhost:2019");
        let route = render_fqdn_route(&binding, std::slice::from_ref(&entry), &cfg, &rl)
            .expect("render_fqdn_route returns Some");

        let arr = &route["handle"][0]["routes"][0]["handle"]
            .as_array()
            .unwrap();
        assert_eq!(arr[0]["rates"]["rps"], 999);
        assert_eq!(arr[0]["rates"]["burst"], 1999);
    }
}
