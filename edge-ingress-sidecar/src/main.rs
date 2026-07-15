//! edge-ingress-sidecar — measures Caddy's RPS, publishes per-second
//! deltas to JetStream, consumes the platform total, exposes it to
//! Caddy via a local UDS datagram socket.
//!
//! Co-located with Caddy in each ingress pod. Cross-replica aggregation
//! for the global `GLOBAL_RATE_LIMIT_RPS` cap (issue #665).
//!
//! ## Pipeline (PR B)
//!
//! ```text
//! ┌─────────────┐  ┌───────────┐  ┌──────────────┐  ┌───────────────┐
//! │ caddy       │  │ publisher │  │ consumer     │  │ aggregator    │
//! │ admin       │─▶│ (nats_pub)│  │ (nats_sub)   │─▶│ (aggregate.rs)│
//! │ /metrics    │  │           │  │              │  │               │
//! └─────────────┘  └─────┬─────┘  └──────┬───────┘  └───────┬───────┘
//!                        ▼               │                  │
//!              edgecloud.rate-limit     │           ┌──────▼────────┐
//!              .global.delta.<rid>      │           │ UDS datagram  │
//!                                        │           │ (expose.rs)   │
//!                                        │           └──────┬────────┘
//!                                        │                  ▼
//!                                        │     /var/run/edge-ingress/
//!                                        │     global-rps.sock
//!                                        │                  │
//!                                        │           ┌──────▼────────┐
//!                                        │           │ ingress binary│
//!                                        │           │ (PR D)        │
//!                                        │           └───────────────┘
//! ```
//!
//! See issue #665 for the design doc + PR breakdown.

// All modules are `pub` so the integration test binary
// (`tests/integration_test.rs`) can drive the full pipeline:
// publisher (`nats_pub::NatsPublisher::publish_delta`) → consumer
// (`nats_sub::spawn_consumer` + freshness gate) → aggregator
// (`Aggregator::tick`) → snapshot (`Snapshot::per_replica_cap`).
// The binary keeps the same surface; only the visibility flips. The
// only consumer of these `pub` paths outside the binary is the
// integration test, which is gated behind `RUN_INTEGRATION_TESTS`.
pub mod aggregate;
pub mod caddy_metrics;
pub mod config;
pub mod expose;
pub mod nats_pub;
pub mod nats_sub;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crate::aggregate::{Aggregator, Snapshot};
use crate::caddy_metrics::{spawn_scraper, DeltaMsg};
use crate::config::Config;
use crate::expose::snapshot_to_writer;
use crate::nats_pub::{spawn_publisher, NatsPublisher};
use crate::nats_sub::spawn_consumer;

/// Channel buffer between the scraper and the NATS publisher. The
/// scraper ticks at 1 Hz; a buffer of 4 covers 4 missed publishes
/// (≈3s of measurement queue) before backpressure forces the
/// scraper to drop a tick — the same self-healing semantic the
/// publisher uses on the wire.
const SCRAPER_CHANNEL_BUFFER: usize = 4;

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
    cfg.validate();

    tracing::info!(
        nats_url = %cfg.nats_url,
        replica_id = %cfg.replica_id,
        caddy_admin = %cfg.caddy_admin_url,
        metrics_listen = %cfg.metrics_listen,
        global_rate_limit_rps = cfg.global_rate_limit_rps,
        nats_replicas = cfg.nats_replicas,
        uds_path = %cfg.uds_path,
        "edge-ingress-sidecar starting (PR B — full pipeline wired)"
    );

    // Install the Prometheus metrics recorder. The `/metrics` HTTP listener
    // is the operator's checkpoint: a missing endpoint tells them the
    // sidecar isn't running. PR B registers descriptions for
    // publish_total, consume_lag, window_sum, uds_writes_total (the
    // description call sites land in the respective modules).
    let metrics_addr: std::net::SocketAddr = match cfg.metrics_listen.parse() {
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

    // ── Connect NATS publisher + ensure the rate-limit stream ────
    //
    // PR B mirrors `edge-worker/src/nats.rs::connect + ensure_task_stream`
    // (lines 162-184). The publisher's EnsureStream is idempotent so
    // the sidecar can boot before the control plane has declared
    // the stream (PR C adds the CP-side EnsureStream — both are
    // safe to call in any order).
    let publisher: Arc<NatsPublisher> = match NatsPublisher::connect(
        &cfg.nats_url,
        cfg.nats_replicas,
    )
    .await
    {
        Ok(p) => match p.ensure_stream().await {
            Ok(()) => Arc::new(p),
            Err(e) => {
                // Exit instead of "retry on each tick" — the stream
                // declaration is the load-bearing primitive the
                // consumer subscribes against; without it the
                // consumer's `get_stream` fails forever and the
                // "retry on each tick" message misleads operators
                // into thinking the sidecar is making progress.
                // A failure here means the NATS cluster is missing
                // or permissions are wrong; both deserve operator
                // intervention, not silent retry.
                tracing::error!(
                    err = %e,
                    "ensure_stream failed; the rate-limit stream must be declared before \
                     the sidecar can subscribe. The control plane (PR C) declares this stream \
                     on startup — boot the CP first, or check NATS cluster permissions. Exiting."
                );
                return ExitCode::from(2);
            }
        },
        Err(e) => {
            tracing::error!(err = %e, "nats connect failed; exiting");
            return ExitCode::from(2);
        }
    };
    // Hand a clone of the underlying client to the consumer so it
    // can build its own jetstream context without re-connecting.
    let consumer_client = publisher.client();

    // ── Scraper → publisher channel (scrape Caddy at 1 Hz, ship) ──
    let (scraper_tx, scraper_rx) = mpsc::channel::<DeltaMsg>(SCRAPER_CHANNEL_BUFFER);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("build reqwest client");

    let scraper_handle = spawn_scraper(
        http.clone(),
        cfg.caddy_admin_url.clone(),
        cfg.replica_id.clone(),
        scraper_tx,
        shutdown.clone(),
    );

    let publisher_handle = spawn_publisher(
        publisher,
        cfg.replica_id.clone(),
        scraper_rx,
        shutdown.clone(),
    );

    // ── Consumer → aggregator channel (receive the platform-wide
    //    delta stream). ──
    let (agg_tx, agg_rx) = mpsc::channel::<DeltaMsg>(256);
    let consumer_name = format!("{}-rl-consumer", cfg.replica_id);
    let consumer_handle = spawn_consumer(consumer_client, consumer_name, agg_tx, shutdown.clone());

    // ── Aggregator → UDS datagram writer. ──
    //
    // PR B review fix: the sidecar is the SENDER, not the receiver.
    // The ingress process (PR D) owns the bind + chmod + unlink of
    // the well-known socket path; the sidecar uses an unbound
    // socket + send_to(path) per tick and treats ENOENT as
    // "receiver not bound yet; drop and retry next tick." See
    // expose.rs module docs for the full ownership rationale.
    let aggregator = Aggregator::new(cfg.replica_id.clone(), cfg.global_rate_limit_rps);
    let uds_path = Arc::<std::path::Path>::from(std::path::PathBuf::from(&cfg.uds_path));
    let on_snapshot: Arc<dyn Fn(Snapshot) + Send + Sync> = Arc::new(snapshot_to_writer(uds_path));
    let aggregator_handle =
        crate::aggregate::spawn_aggregator(aggregator, agg_rx, on_snapshot, shutdown.clone());

    // ── Signal handlers (mirror edge-ingress/src/main.rs:180-211) ──
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).ok();

    if sigterm.is_none() && sigint.is_none() {
        tracing::warn!("no signal handlers available; process must be killed externally");
        let _ = tokio::join!(
            scraper_handle,
            publisher_handle,
            consumer_handle,
            aggregator_handle
        );
        return ExitCode::SUCCESS;
    }

    let signal_name = tokio::select! {
        _ = async { sigterm.as_mut().unwrap().recv().await }, if sigterm.is_some() => "SIGTERM",
        _ = async { sigint.as_mut().unwrap().recv().await },  if sigint.is_some()  => "SIGINT",
    };
    tracing::info!(%signal_name, "received signal, initiating graceful shutdown");

    shutdown.cancel();
    // 10s mirrors `edge-ingress/src/main.rs:207`. The consumer's
    // last-per-subject ack + the publisher's in-flight publish
    // both need a small drain window; a 10s budget covers the
    // typical 1s tick + headroom for NATS reconnect.
    let drain = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            scraper_handle,
            publisher_handle,
            consumer_handle,
            aggregator_handle
        )
    })
    .await;

    if drain.is_err() {
        tracing::warn!("graceful shutdown timed out; some tasks were aborted");
    }

    tracing::info!("graceful shutdown complete");
    ExitCode::SUCCESS
}
