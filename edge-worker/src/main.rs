//! edge-worker — Worker Supervisor entry point.

mod auth;
mod config;
mod cpu_sample;
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
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use edge_worker::tracing_layer::WorkerLogLayer;

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
    // Load configuration FIRST so we can construct the JWT signer and
    // LogForwarder before tracing init. This hoisting lets the new
    // WorkerLogLayer (wired in below) capture the JWT-secret warning
    // and every tracing call that follows.
    let config = Config::from_env()?;

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

    // Initialize LogForwarder — receives tenant `emit_log` records from the
    // runtime AND worker-side `tracing` events via WorkerLogLayer. One per
    // worker; per-app AppLogContext travels with each guest record.
    let log_forwarder = LogForwarder::new(
        config.control_plane_url.clone(),
        config.worker_id.clone(),
        config.region.clone(),
        jwt_signer.clone(),
    );

    // Without a JWT secret the worker can still run — NATS heartbeats and
    // the deployment supervisor don't need it — but every outbound call
    // to /api/internal/* will 401 until the secret is provisioned. Warn
    // loudly so an operator notices instead of discovering it from a
    // silent drop in log forwarding. A real fix needs a JWT bootstrap
    // handshake (see follow-up issue D).
    if config.worker_jwt_secret.is_empty() {
        tracing::warn!(
            "WORKER_JWT_SECRET is not set; /api/internal/* calls will return 401 \
             until the secret is provisioned. NATS heartbeats and the deployment \
             supervisor keep running — only the log forwarder and downloader are \
             affected. See follow-up issue D for the bootstrap handshake."
        );
    }

    // Initialize tracing. The Registry stack is:
    //   - EnvFilter (RUST_LOG-controlled; default info)
    //   - fmt::Layer (local stdout — preserves existing behavior)
    //   - WorkerLogLayer (ships worker-side events to the control plane
    //     via log_forwarder, with EDGE_WORKER_LOG_LEVEL controlling the
    //     threshold; default info)
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let log_layer = WorkerLogLayer::new(
        log_forwarder.clone() as Arc<dyn edge_runtime::interfaces::observe::LogSink>,
        config.forwarder_log_level(),
    );
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .with(log_layer)
        .init();

    tracing::info!("edge-worker starting");

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

// Shared HTTP client for all supervisor-initiated requests to the
    // control plane (currently: /sync fallback). Constructed once so its
    // connection pool — and any open TLS sessions to the CP — survive
    // across heartbeat ticks. A per-call Client (the previous
    // behaviour) forced a fresh TLS handshake every fallback during
    // sustained NATS outage.
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    // Create the supervisor
    let supervisor = Arc::new(Supervisor {
        config: config.clone(),
        state,
        downloader,
        port_pool,
        nats: nats.clone(),
        log_forwarder: log_forwarder.clone(),
        jwt_signer: jwt_signer.clone(),
        http,
        cpu_sample: crate::cpu_sample::CpuSample::new(),
    });

    let heartbeat_supervisor = supervisor.clone();
    let heartbeat_interval = Duration::from_secs(config.heartbeat_interval_secs);
    // Watchdog threshold for the HTTP /sync fallback (issue #53).
    // The periodic CP-side publish at RECONCILE_INTERVAL (5min default)
    // is the durable safety net; this threshold is what the worker uses
    // to decide "NATS has gone silent, fall back to HTTP". Default 60s
    // gives the periodic CP loop time to fire on a healthy cluster while
    // bounding the worst-case worker staleness on a CP-isolated network
    // partition.
    let sync_threshold = Duration::from_secs(config.worker_sync_threshold_secs);

    // Start the heartbeat task — exits cleanly when it receives the shutdown signal.
    // Clone the sender so the original stays available for signal handlers.
    let shutdown_tx_for_heartbeat = shutdown_tx.clone();
    // Subscribe ONCE, outside the loop. broadcast::Receiver::recv() only sees
    // messages sent after the subscription was created; re-subscribing inside
    // the loop loses any signal sent between the previous recv() returning
    // and the next subscribe() call. (This was the bug fix for finding #12.)
    let mut shutdown_rx_for_heartbeat = shutdown_tx_for_heartbeat.subscribe();
    tokio::spawn(async move {
        let mut ticker = interval(heartbeat_interval);
        // Skip the first tick which fires immediately on creation.
        ticker.tick().await;
        loop {
            tokio::select! {
                // `biased` ensures shutdown always wins when both are ready.
                biased;
                _ = shutdown_rx_for_heartbeat.recv() => {
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
                            heartbeat_supervisor.reset_meters_after(&heartbeat).await;
                        }
                        Err(e) => {
                            tracing::error!(err = %e, "failed to publish heartbeat");
                        }
                    }

                    // HTTP /sync fallback watchdog (issue #53). If we
                    // haven't received any TaskMessage (NAT or HTTP) in
                    // `sync_threshold`, pull the full desired state from
                    // the control plane directly. Idempotent — same
                    // diff logic as a NATS-delivered full_sync. Catches
                    // the offline-forever case where the CP's NATS
                    // stream retention expires (24h default) before
                    // reconnect.
                    let last_seen = {
                        let st = heartbeat_supervisor.state.read().await;
                        st.last_task_received_at
                            .lock()
                            .ok()
                            .and_then(|g| *g)
                    };
                    let silent_for = last_seen
                        .map(|t| t.elapsed())
                        .unwrap_or(Duration::MAX);
                    if silent_for >= sync_threshold {
                        tracing::warn!(
                            silent_for = ?silent_for,
                            threshold = ?sync_threshold,
                            "NATS silent too long; pulling /sync fallback"
                        );
                        match heartbeat_supervisor.fetch_sync().await {
                            Ok(Some(msg)) => {
                                if let Err(e) =
                                    heartbeat_supervisor.handle_task_message(msg).await
                                {
                                    tracing::error!(err = %e, "failed to apply /sync payload");
                                }
                            }
                            Ok(None) => {
                                // fetch_sync already logged the reason.
                            }
                            Err(e) => {
                                tracing::error!(err = %e, "/sync fetch failed unexpectedly");
                            }
                        }
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
    //
    // Some restricted runtimes (seccomp policies, certain sandboxed
    // environments) reject one of the signal handlers. We try both and
    // fall back to whichever is available; if neither is, we log and
    // exit the spawn task — main()'s loop keeps running and the
    // supervisor (k8s / systemd) is expected to SIGKILL the process
    // after its grace period. (This replaces the `.expect()` calls that
    // would panic the spawned task on install failure — finding #9.)
    let shutdown_supervisor = supervisor.clone();
    // broadcast::Sender is Clone + Send + Sync, so no Arc is needed.
    let shutdown_tx_s = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!(err = %e, "SIGTERM handler unavailable");
                None
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::warn!(err = %e, "SIGINT handler unavailable");
                None
            }
        };
        if sigterm.is_none() && sigint.is_none() {
            tracing::error!("no signal handlers available; cannot initiate graceful shutdown");
            return;
        }
        // `biased` so SIGTERM is preferred if both fire in the same poll.
        let signal_name = tokio::select! {
            biased;
            _ = async { sigterm.as_mut().unwrap().recv().await }, if sigterm.is_some() => "SIGTERM",
            _ = async { sigint.as_mut().unwrap().recv().await },  if sigint.is_some()  => "SIGINT",
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

/// DrainOutcome is the typed result of waiting for the log forwarder task
/// to finish after shutdown was signalled. Distinguishing the three
/// outcomes — clean, panic, timeout — lets operators see when an in-flight
/// batch is lost instead of silently dropping the JoinError or timeout.
#[derive(Debug, PartialEq, Eq)]
enum DrainOutcome {
    Clean,
    Panic,
    Timeout,
}

/// drain_logs_task waits up to `timeout` for `logs_task` to finish. Pulled
/// out of graceful_shutdown() so the three outcomes can be unit-tested
/// without a full worker setup. Logs the outcome; returns the typed result
/// for callers that want to act on it.
async fn drain_logs_task(
    timeout: Duration,
    logs_task: tokio::task::JoinHandle<()>,
) -> DrainOutcome {
    match tokio::time::timeout(timeout, logs_task).await {
        Ok(Ok(())) => {
            tracing::info!("log forwarder drained cleanly");
            DrainOutcome::Clean
        }
        Ok(Err(join_err)) => {
            tracing::error!(
                err = %join_err,
                panicked = join_err.is_panic(),
                "log forwarder task did not exit cleanly; in-flight batch may be lost"
            );
            DrainOutcome::Panic
        }
        Err(_) => {
            tracing::error!(
                timeout_secs = timeout.as_secs(),
                "log forwarder final flush exceeded timeout; in-flight batch may be lost"
            );
            DrainOutcome::Timeout
        }
    }
}

/// Perform graceful shutdown: signal the heartbeat + log-forwarder to stop,
/// stop all apps, publish a final heartbeat, wait for the log forwarder to
/// drain its final flush, then exit the process.
async fn graceful_shutdown(
    shutdown_tx: broadcast::Sender<()>,
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

    // Wait for the log forwarder to flush its final batch. drain_logs_task
    // distinguishes clean exit, panic, and timeout so operators can see
    // when a shutdown loses an in-flight batch.
    tracing::info!("awaiting log forwarder final flush");
    drain_logs_task(Duration::from_secs(10), logs_task).await;

    tracing::info!("shutdown complete");
}

#[cfg(test)]
mod shutdown_tests {
    //! Unit tests for the drain_logs_task helper. Each test exercises one
    //! of the three outcomes returned by a JoinHandle wrapped in a
    //! tokio::time::timeout — clean exit, panic, and timeout. The
    //! broadcast plumbing around the helper is covered by the integration
    //! test in tests/shutdown.rs (see follow-up issue C).

    use super::{drain_logs_task, DrainOutcome};
    use std::time::Duration;

    #[tokio::test]
    async fn drain_logs_task_returns_clean_when_task_exits_normally() {
        let handle = tokio::spawn(async {});
        let outcome = drain_logs_task(Duration::from_secs(1), handle).await;
        assert_eq!(outcome, DrainOutcome::Clean);
    }

    #[tokio::test]
    async fn drain_logs_task_returns_panic_when_task_panics() {
        let handle = tokio::spawn(async {
            panic!("synthetic log forwarder panic");
        });
        let outcome = drain_logs_task(Duration::from_secs(1), handle).await;
        assert_eq!(outcome, DrainOutcome::Panic);
    }

    #[tokio::test]
    async fn drain_logs_task_returns_timeout_when_task_runs_too_long() {
        // A task that sleeps for 10s — against a 50ms timeout, drain_logs_task
        // must hit the timeout branch and return DrainOutcome::Timeout.
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let outcome = drain_logs_task(Duration::from_millis(50), handle).await;
        assert_eq!(outcome, DrainOutcome::Timeout);
    }
}
