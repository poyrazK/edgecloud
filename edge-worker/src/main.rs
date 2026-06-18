//! edge-worker — Worker Supervisor entry point.

mod auth;
mod config;
mod downloader;
mod log_forwarder;
mod messages;
mod nats;
mod port_pool;
mod state;
mod supervisor;

use std::sync::Arc;

use tokio::signal::unix::{signal, SignalKind};

use tokio::sync::broadcast;
use tokio::time::{interval, Duration};
use tracing_subscriber::EnvFilter;

use crate::auth::WorkerJwtSigner;
use crate::config::Config;
use crate::downloader::Downloader;
use crate::log_forwarder::LogForwarder;
use crate::nats::NatsClientImpl;
use crate::port_pool::PortPool;
use crate::state::WorkerState;
use crate::supervisor::Supervisor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    tracing::info!("edge-worker starting");

    // Load configuration from environment
    let config = Config::from_env()?;
    tracing::info!(
        worker_id = %config.worker_id,
        region = %config.region,
        worker_addr = %config.worker_addr,
        queue_group = %config.queue_group,
        consumer = %config.consumer_name,
        "configuration loaded"
    );

    // Create cache directory
    tokio::fs::create_dir_all(&config.cache_dir).await?;
    tracing::info!(dir = %config.cache_dir.display(), "cache directory ready");

    // Create the shared wasmtime engine (shared for compilation caching across apps)
    let engine = edge_runtime::create_engine()?;
    tracing::info!("wasmtime engine created");

    // Initialize shared state
    let state = Arc::new(tokio::sync::RwLock::new(WorkerState::new(engine)));

    // Initialize JWT signer — signs outbound calls to the control plane's
    // /api/internal/* endpoints. Worker is per-tenant in this design; the
    // JWT carries the worker's tenant_id claim.
    let jwt_signer = WorkerJwtSigner::new(
        config.worker_jwt_secret.clone(),
        config.worker_jwt_issuer.clone(),
        config.worker_id.clone(),
        config.region.clone(),
        config.worker_tenant_id.clone(),
    );

    // Initialize downloader
    let downloader = Arc::new(Downloader::new(
        config.control_plane_url.clone(),
        config.cache_dir.clone(),
        jwt_signer.clone(),
    ));

    // Initialize port pool
    let port_pool = Arc::new(tokio::sync::Mutex::new(PortPool::new(
        config.starting_port,
        config.port_cooldown_secs,
    )));

    // Connect to NATS
    let nats = Arc::new(NatsClientImpl::connect(&config.nats_url).await?)
        as Arc<dyn crate::nats::NatsClient>;
    tracing::info!(url = %config.nats_url, "connected to NATS");

    // Create the shutdown broadcast channel for the heartbeat task.
    // Using broadcast lets us get a fresh receiver (subscription) each loop iteration.
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Initialize LogForwarder — receives tenant `emit_log` records from the
    // runtime and ships them to the control plane's POST /api/internal/logs.
    // One per worker; per-app AppLogContext travels with each record.
    let log_forwarder = LogForwarder::new(
        config.control_plane_url.clone(),
        config.worker_id.clone(),
        config.region.clone(),
        jwt_signer,
    );

    // Create the supervisor
    let supervisor = Arc::new(Supervisor {
        config: config.clone(),
        state,
        downloader,
        port_pool,
        nats: nats.clone(),
        log_forwarder: log_forwarder.clone(),
    });

    let heartbeat_supervisor = supervisor.clone();
    let heartbeat_interval = Duration::from_secs(config.heartbeat_interval_secs);

    // Start the heartbeat task — exits cleanly when it receives the shutdown signal.
    // Clone the sender so the original stays available for signal handlers.
    let shutdown_tx_for_heartbeat = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut ticker = interval(heartbeat_interval);
        // Skip the first tick which fires immediately on creation.
        ticker.tick().await;
        loop {
            // Get a fresh receiver each iteration — broadcast lets us do this.
            let mut shutdown_rx = shutdown_tx_for_heartbeat.subscribe();
            tokio::select! {
                // `biased` ensures shutdown always wins when both are ready.
                biased;
                _ = shutdown_rx.recv() => {
                    tracing::info!("heartbeat task received shutdown signal, stopping");
                    break;
                }
                _ = ticker.tick() => {
                    let heartbeat = heartbeat_supervisor.build_heartbeat().await;
                    if let Err(e) = heartbeat_supervisor
                        .nats
                        .publish_heartbeat(&heartbeat_supervisor.config.region, &heartbeat)
                        .await
                    {
                        tracing::error!(err = %e, "failed to publish heartbeat");
                    }
                }
            }
        }
    });

    tracing::info!("heartbeat task started");

    // Spawn the log-forwarder flush loop. It listens on the same shutdown
    // broadcast as the heartbeat task; on shutdown it does one final flush
    // before exiting so in-flight logs survive a clean worker shutdown.
    let shutdown_tx_for_logs = shutdown_tx.clone();
    let log_forwarder_for_loop = log_forwarder.clone();
    let logs_task = tokio::spawn(async move {
        let shutdown_rx = shutdown_tx_for_logs.subscribe();
        log_forwarder_for_loop.flush_loop(shutdown_rx).await;
    });
    tracing::info!("log forwarder task started");

    // Subscribe to task updates
    tracing::info!(region = %config.region, "subscribed to task updates");

    // Single signal handler for SIGTERM and SIGINT — whichever fires first
    // initiates graceful shutdown and consumes the log-forwarder JoinHandle.
    // The other signal is ignored (the process exits before it can fire).
    let shutdown_supervisor = supervisor.clone();
    let shutdown_tx_s = Arc::new(shutdown_tx.clone());
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        let signal_name = tokio::select! {
            _ = sigterm.recv() => "SIGTERM",
            _ = sigint.recv()  => "SIGINT",
        };
        tracing::info!(
            signal = signal_name,
            "received signal, initiating graceful shutdown"
        );
        graceful_shutdown(shutdown_tx_s, shutdown_supervisor, logs_task).await;
    });

    tracing::info!(
        region = %config.region,
        queue_group = %config.queue_group,
        "ready — waiting for task messages"
    );

    // Run the consume loop on the main task. Wrapped in a reconnect loop
    // with bounded exponential backoff so transient stream-end (consumer
    // deleted, server restart, push-consumer dropped) doesn't kill the
    // worker. Shutdown signal is observed via the broadcast receiver, so
    // Ok(()) here means "shutdown was signalled" — break and drain.
    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
    loop {
        let consume_shutdown_rx = shutdown_tx.subscribe();
        match supervisor.run_consume_loop(consume_shutdown_rx).await {
            Ok(()) => {
                tracing::info!("consume loop returned after shutdown signal");
                break;
            }
            Err(e) => {
                tracing::error!(
                    err = %e,
                    backoff_secs = backoff.as_secs(),
                    "consume loop ended unexpectedly; reconnecting"
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
            }
        }
    }

    // graceful_shutdown (spawned above) does the work — it signaled the
    // broadcast, stopped apps, published the final heartbeat, and awaited
    // the log forwarder drain. main() returns cleanly.
    Ok(())
}

/// Perform graceful shutdown: signal the heartbeat + log-forwarder to stop,
/// stop all apps, publish a final heartbeat, wait for the log forwarder to
/// drain its final flush, then exit the process.
async fn graceful_shutdown(
    shutdown_tx: Arc<broadcast::Sender<()>>,
    supervisor: Arc<Supervisor>,
    logs_task: tokio::task::JoinHandle<()>,
) {
    // Signal the heartbeat + log-forwarder tasks to stop.
    let _ = shutdown_tx.send(());

    tracing::info!("graceful shutdown: stopping all apps");
    supervisor.stop_all_apps().await;

    // Shutdown signalled. Publish one final heartbeat so the control plane
    // doesn't have to wait for the 30s heartbeat timeout to learn the
    // worker is gone.
    tracing::info!("publishing final heartbeat");
    let heartbeat = supervisor.build_heartbeat().await;
    if let Err(e) = supervisor
        .nats
        .publish_heartbeat(&supervisor.config.region, &heartbeat)
        .await
    {
        tracing::error!(err = %e, "failed to publish final heartbeat");
    }

    // Wait for the log forwarder to flush its final batch. We can't `await`
    // a broadcast signal directly, but the spawned task exits within
    // REQUEST_TIMEOUT (5s) of receiving shutdown — well under any
    // operator-imposed shutdown deadline.
    tracing::info!("awaiting log forwarder final flush");
    let _ = tokio::time::timeout(Duration::from_secs(10), logs_task).await;

    tracing::info!("shutdown complete");
}
