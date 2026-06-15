//! edge-worker — Worker Supervisor entry point.

mod config;
mod downloader;
mod messages;
mod nats;
mod port_pool;
mod state;
mod supervisor;

use std::sync::Arc;

use futures::StreamExt;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::broadcast;
use tokio::time::{interval, sleep, Duration};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::downloader::Downloader;
use crate::messages::TaskMessage;
use crate::nats::NatsClient;
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
    let nats = Arc::new(NatsClient::connect(&config.nats_url).await?);
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

    // Subscribe to task updates
    tracing::info!(region = %config.region, "subscribed to task updates");

    // Spawn SIGTERM handler for graceful shutdown
    let shutdown_supervisor = supervisor.clone();
    let shutdown_tx_s = Arc::new(shutdown_tx.clone());
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        sigterm.recv().await;
        tracing::info!("received SIGTERM, initiating graceful shutdown");
        graceful_shutdown(shutdown_tx_s, shutdown_supervisor).await;
    });

    // Spawn SIGINT handler (Ctrl+C)
    let shutdown_supervisor = supervisor.clone();
    let shutdown_tx_s = Arc::new(shutdown_tx.clone());
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        sigint.recv().await;
        tracing::info!("received SIGINT, initiating graceful shutdown");
        graceful_shutdown(shutdown_tx_s, shutdown_supervisor).await;
    });

    tracing::info!("ready — waiting for task messages");

    // Main loop: process NATS messages with automatic reconnection.
    // If the subscription stream ends (e.g., after async_nats reconnects and
    // the old mpsc channel closes), re-subscribe and continue.
    loop {
        let mut subscription = match supervisor.nats.subscribe(&config.region).await {
            Ok(sub) => sub,
            Err(e) => {
                tracing::warn!(err = %e, "subscription failed, retrying in 5s");
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        while let Some(msg) = subscription.next().await {
            let payload = msg.payload;

            let task_msg: TaskMessage = match serde_json::from_slice(&payload) {
                Ok(msg) => msg,
                Err(e) => {
                    tracing::error!(err = %e, "failed to parse task message");
                    continue;
                }
            };

            let app_count = match &task_msg {
                TaskMessage::TaskUpdate { apps, .. } => apps.len(),
            };
            tracing::debug!(app_count, "received task message");

            if let Err(e) = supervisor.handle_task_message(task_msg).await {
                tracing::error!(err = %e, "failed to handle task message");
            }
        }

        tracing::warn!("subscription stream ended, re-subscribing...");
    }
}

/// Perform graceful shutdown: signal the heartbeat to stop, stop all apps,
/// publish a final heartbeat, then exit the process.
async fn graceful_shutdown(
    shutdown_tx: Arc<broadcast::Sender<()>>,
    supervisor: Arc<Supervisor>,
) {
    // Signal the heartbeat task to stop.
    let _ = shutdown_tx.send(());

    tracing::info!("graceful shutdown: stopping all apps");
    supervisor.stop_all_apps().await;

    tracing::info!("publishing final heartbeat");
    let heartbeat = supervisor.build_heartbeat().await;
    if let Err(e) = supervisor
        .nats
        .publish_heartbeat(&supervisor.config.region, &heartbeat)
        .await
    {
        tracing::error!(err = %e, "failed to publish final heartbeat");
    }

    tracing::info!("shutdown complete");
    std::process::exit(0);
}
