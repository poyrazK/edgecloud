//! edge-ingress — public ingress / edge proxy for edgeCloud.
//!
//! Wraps a Caddy process via its JSON admin API. Subscribes to NATS
//! heartbeats to learn which worker hosts which app, and renders a
//! Caddyfile-JSON that maps `<tenant_id>-<app_name>.edgecloud.dev` to
//! `http://<worker>:<port>`. See `edge-ingress/README.md` for the operator
//! runbook (env vars, cert provisioning, Caddy invocation).

mod caddy;
mod config;
mod domains;
mod global_rps;
pub mod heartbeats;
mod ingress_metrics;
pub mod l4;
mod l4_cache;
mod messages;
mod quota;
mod ratelimit;
mod routing;
mod tenant_ratelimit;
pub mod traffic;

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::caddy::CaddyClient;
use crate::config::Config;
use crate::l4::{L4PortPool, L4RoutingTable};
use crate::routing::RoutingTable;

#[derive(Parser, Debug)]
#[command(name = "edge-ingress", about = "Public ingress for edgeCloud")]
struct Args {
    /// Optional path to a TOML config file. Env vars always win; this is
    /// just a convenience for operators who prefer files.
    #[arg(long)]
    config: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let _args = Args::parse();

    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(err = %e, "config error");
            return ExitCode::from(2);
        }
    };
    // Review finding: log a warning when the configured rate-limit
    // caps look like operator typos (sub-10 RPS for the global or
    // per-tenant-default knobs is almost certainly a missing zero).
    // Non-fatal — operators may legitimately want a low cap — but
    // gives them a single grep target when investigating a 429 storm.
    cfg.validate();
    tracing::info!(
        region = %cfg.region,
        caddy = %cfg.caddy_admin_url,
        cert = %cfg.cert_file,
        metrics = %cfg.metrics_listen,
        "edge-ingress starting"
    );

    // Install the Prometheus metrics recorder and register descriptions.
    let metrics_addr: SocketAddr = cfg
        .metrics_listen
        .parse()
        .expect("invalid INGRESS_METRICS_LISTEN address");
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()
        .expect("failed to install Prometheus metrics recorder");
    ingress_metrics::describe_metrics();

    let table = Arc::new(RoutingTable::new());
    // Issue #548 — L4 (raw-TCP) ingress uses a parallel routing
    // table and a CP-persistent public-port pool. Both are
    // ingress-local for now (Commit 9 swaps the pool to a
    // CP-coordinated allocator via `L4PortCache`).
    let l4_table = Arc::new(L4RoutingTable::new());
    let l4_ports = Arc::new(tokio::sync::Mutex::new(L4PortPool::new(
        cfg.l4_port_range_start,
        cfg.l4_port_range_end,
        cfg.l4_port_cooldown_secs,
    )));
    let caddy = match CaddyClient::new(&cfg.caddy_admin_url, cfg.admin_token.clone()) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!(err = %e, "failed to build Caddy client");
            return ExitCode::from(1);
        }
    };

    // One shared `Notify` drives the Caddy renderer. Both the domain
    // poller (FQDN routes) and the heartbeat path (upstream routes)
    // signal on this same Notify so a Caddy reload fires regardless
    // of which side of the system observed the change.
    let render_notify = Arc::new(Notify::new());
    let shutdown = CancellationToken::new();

    // Custom-domain poller. Exits on shutdown or unrecoverable auth error.
    if !cfg.control_plane_url.is_empty() {
        let dom_cfg = cfg.clone();
        let dom_table = table.clone();
        let dom_notify = render_notify.clone();
        let dom_shutdown = shutdown.clone();
        let dom_select_shutdown = dom_shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = dom_shutdown.cancelled() => {
                    tracing::info!("domain poller: shutdown signal received, stopping");
                }
                result = domains::run(dom_cfg, dom_table, dom_notify, dom_select_shutdown) => {
                    if let Err(e) = result {
                        tracing::error!(err = %e, "domain poller exited; restarting process");
                        std::process::exit(1);
                    }
                }
            }
        });
    } else {
        tracing::info!("CONTROL_PLANE_URL unset; running in default-only mode (no custom domains)");
    }

    // Heartbeat loop with exponential backoff. Runs as a spawned task so
    // the main task can listen for shutdown signals.
    const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);
    let hb_shutdown = shutdown.clone();
    let hb_cfg = cfg.clone();
    let hb_table = table.clone();
    let hb_l4_table = l4_table.clone();
    let hb_l4_ports = l4_ports.clone();
    let hb_caddy = caddy.clone();
    let hb_notify = render_notify.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut backoff = INITIAL_BACKOFF;
        loop {
            tokio::select! {
                _ = hb_shutdown.cancelled() => {
                    tracing::info!("heartbeat loop: shutdown signal received, exiting");
                    break;
                }
                result = heartbeats::run(hb_cfg.clone(), hb_table.clone(), hb_l4_table.clone(), hb_l4_ports.clone(), hb_caddy.clone(), hb_notify.clone(), hb_shutdown.clone()) => {
                    match result {
                        Ok(()) => {
                            tracing::warn!("heartbeats::run returned cleanly; re-running");
                            backoff = INITIAL_BACKOFF;
                        }
                        Err(e) => {
                            metrics::counter!("ingress.nats.reconnects_total").increment(1);
                            tracing::error!(err = %e, delay_ms = backoff.as_millis(), "heartbeats::run failed; re-running after backoff");
                            sleep(backoff).await;
                            backoff = (backoff * 2).min(MAX_BACKOFF);
                        }
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });

    // Install signal handlers. Mirrors the worker's pattern (SIGTERM +
    // SIGINT). The `select!` is biased so SIGTERM wins if both fire at
    // the same time.
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();
    if sigterm.is_none() && sigint.is_none() {
        tracing::warn!("no signal handlers available; process must be killed externally");
        // Fall back to waiting on the heartbeat task completing (or a fatal error).
        // In this case we never signal shutdown — the process runs until killed.
        let _ = heartbeat_task.await;
        return ExitCode::SUCCESS;
    }

    let signal_name = tokio::select! {
        _ = async { sigterm.as_mut().unwrap().recv().await }, if sigterm.is_some() => "SIGTERM",
        _ = async { sigint.as_mut().unwrap().recv().await },  if sigint.is_some()  => "SIGINT",
    };
    tracing::info!(%signal_name, "received signal, initiating graceful shutdown");

    // Cancel all tasks.
    shutdown.cancel();

    // Wait for the heartbeat task to drain (with a 10s timeout).
    tokio::select! {
        _ = heartbeat_task => {
            tracing::info!("heartbeat loop drained");
        }
        _ = sleep(Duration::from_secs(10)) => {
            tracing::warn!("heartbeat loop did not drain within 10s");
        }
    }

    tracing::info!("graceful shutdown complete");
    ExitCode::SUCCESS
}
