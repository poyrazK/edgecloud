//! edge-worker — Worker Supervisor entry point.

mod config;
mod downloader;
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

use crate::config::Config;
use crate::downloader::Downloader;
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

    // Initialize downloader
    let downloader = Arc::new(Downloader::new(
        config.control_plane_url.clone(),
        config.cache_dir.clone(),
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

    // Create the supervisor
    let supervisor = Arc::new(Supervisor {
        config: config.clone(),
        state,
        downloader,
        port_pool,
        nats: nats.clone(),
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
                    match heartbeat_supervisor
                        .nats
                        .publish_heartbeat(&heartbeat_supervisor.config.region, &heartbeat)
                        .await
                    {
                        Ok(()) => {
                            heartbeat_supervisor.reset_meters().await;
                        }
                        Err(e) => {
                            tracing::error!(err = %e, "failed to publish heartbeat");
                        }
                    }
                }
            }
        }
    });

    tracing::info!("heartbeat task started");

    // Spawn SIGTERM handler — signal shutdown and let main drain.
    let shutdown_tx_t = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        sigterm.recv().await;
        tracing::info!("received SIGTERM, initiating graceful shutdown");
        let _ = shutdown_tx_t.send(());
    });

    // Spawn SIGINT handler (Ctrl+C)
    let shutdown_tx_i = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        sigint.recv().await;
        tracing::info!("received SIGINT, initiating graceful shutdown");
        let _ = shutdown_tx_i.send(());
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

    // Stop all running apps so they release their ports and shut down
    // cleanly before the runtime drops.
    tracing::info!("stopping all apps");
    supervisor.stop_all_apps().await;

    tracing::info!("shutdown complete");
    Ok(())
}
