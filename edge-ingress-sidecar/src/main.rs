//! edge-ingress-sidecar — measures Caddy's RPS, publishes per-second
//! deltas to JetStream, consumes the platform total, exposes it to
//! Caddy via a local UDS datagram socket.
//!
//! Co-located with Caddy in each ingress pod. Cross-replica aggregation
//! for the global `GLOBAL_RATE_LIMIT_RPS` cap (issue #665).
//!
//! This file is the **PR A skeleton**. Empty crates compile clean
//! without runtime wiring — the binary starts, installs the Prometheus
//! recorder, prints a startup banner, and waits for SIGTERM/SIGINT.
//! PR B fills the publisher / consumer / window-sum / UDS-expose
//! modules and replaces the placeholder loops with real work.
//!
//! See `/Users/poyrazk/.claude/plans/lets-create-imp-plan-curried-feigenbaum.md`
//! for the full design and PR breakdown.

mod aggregate;
mod caddy_metrics;
mod config;
mod expose;
mod nats_pub;
mod nats_sub;

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::config::Config;

#[derive(Parser, Debug)]
#[command(
    name = "edge-ingress-sidecar",
    about = "Cross-replica rate-limit aggregator sidecar (issue #665)"
)]
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

    tracing::info!(
        nats_url = %cfg.nats_url,
        replica_id = %cfg.replica_id,
        caddy_admin = %cfg.caddy_admin_url,
        "edge-ingress-sidecar starting (PR A skeleton; PR B wires the JetStream sum-side)"
    );

    // Install the Prometheus metrics recorder. The `/metrics` HTTP listener
    // is the operator's checkpoint: a missing endpoint tells them the
    // sidecar isn't running. PR B registers Prometheus descriptions for
    // `publish_total`, `consume_lag`, `window_sum`, `uds_writes_total`.
    let metrics_addr: SocketAddr = match cfg.metrics_listen().parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(err = %e, "invalid SIDECAR_METRICS_LISTEN address");
            return ExitCode::from(2);
        }
    };
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()
        .expect("failed to install Prometheus metrics recorder");

    let shutdown = CancellationToken::new();

    // Placeholder loop — replaced by `spawn_publisher` + `spawn_subscriber`
    // in PR B. Until then this just keeps the process alive and ensures
    // the metrics endpoint is reachable for operators to confirm the
    // pod rolled out the right image.
    let tick_shutdown = shutdown.clone();
    let tick_handle = tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(60));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick_shutdown.cancelled() => break,
                _ = tick.tick() => {
                    tracing::debug!("sidecar alive (skeleton loop; PR B wires real work)");
                }
            }
        }
    });

    // SIGTERM + SIGINT handlers (mirror edge-ingress/src/main.rs:180-211).
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();

    if sigterm.is_none() && sigint.is_none() {
        tracing::warn!("no signal handlers available; process must be killed externally");
        let _ = tick_handle.await;
        return ExitCode::SUCCESS;
    }

    let signal_name = tokio::select! {
        _ = async { sigterm.as_mut().unwrap().recv().await }, if sigterm.is_some() => "SIGTERM",
        _ = async { sigint.as_mut().unwrap().recv().await },  if sigint.is_some()  => "SIGINT",
    };
    tracing::info!(%signal_name, "received signal, initiating graceful shutdown");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), tick_handle).await;

    tracing::info!("graceful shutdown complete");
    ExitCode::SUCCESS
}
