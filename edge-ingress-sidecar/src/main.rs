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

mod aggregate;
mod caddy_metrics;
mod config;
mod expose;
mod nats_pub;
mod nats_sub;

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
use crate::expose::Exposer;
use crate::nats_pub::{spawn_publisher, NatsPublisher};
use crate::nats_sub::spawn_consumer;

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
        // TODO(PR B review): thread `cfg.nats_replicas` from a new
        // env var. For PR B we use 1 (single-replica NATS — the
        // testcontainer-backed NATS server in PR B's integration
        // tests is single-replica by default).
        1,
    )
    .await
    {
        Ok(p) => {
            if let Err(e) = p.ensure_stream(1).await {
                tracing::error!(err = %e, "ensure_stream failed; sidecar will retry on each tick");
            }
            Arc::new(p)
        }
        Err(e) => {
            tracing::error!(err = %e, "nats connect failed; exiting");
            return ExitCode::from(2);
        }
    };
    // Hand a clone of the underlying client to the consumer so it
    // can build its own jetstream context without re-connecting.
    let consumer_client = publisher.client();

    // ── Scraper → publisher channel (scrape Caddy at 1 Hz, ship) ──
    let (scraper_tx, scraper_rx) = mpsc::channel::<DeltaMsg>(64);
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

    // ── Aggregator → UDS exposer. ──
    let aggregator = Aggregator::new(cfg.replica_id.clone(), cfg.global_rate_limit_rps);

    let exposer = match Exposer::bind(std::path::Path::new(&cfg.uds_path)) {
        Ok(e) => Arc::new(e),
        Err(e) => {
            tracing::error!(err = %e, uds_path = %cfg.uds_path, "exposer bind failed");
            return ExitCode::from(2);
        }
    };

    // The aggregator ticks once per second; on each tick we hand
    // the snapshot to the exposer, which writes one UDS datagram.
    // PR D's ingress-side cache is the eventual reader of those
    // datagrams.
    let on_snapshot: Arc<dyn Fn(Snapshot) + Send + Sync> = {
        let exposer = Arc::clone(&exposer);
        Arc::new(move |snap: Snapshot| {
            let payload = crate::expose::DatagramPayload::from(&snap);
            let exposer = Arc::clone(&exposer);
            // Fire-and-forget: the kernel socket buffer absorbs
            // backpressure at 1 Hz, and a write failure is logged
            // and dropped (the next tick republishes a fresh
            // snapshot).
            tokio::spawn(async move {
                if let Err(e) = exposer.write(&payload).await {
                    tracing::warn!(err = %e, "snapshot write failed");
                }
            });
        })
    };

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
