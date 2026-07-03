//! edge-worker — Worker Supervisor entry point.

mod auth;
mod bootstrap;
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
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use edge_spool::Spool;
use edge_worker::tracing_layer::WorkerLogLayer;

use crate::auth::WorkerJwtSigner;

/// Convert a worker-jwt-audience config string into an `Option<String>`
/// for the signer constructor. Empty string means "no audience" — the
/// signer then omits the `aud` claim from minted JWTs, matching the
/// pre-H8 behavior. PR #200 review finding H8.
fn audience_opt(audience: &str) -> Option<String> {
    if audience.is_empty() {
        None
    } else {
        Some(audience.to_string())
    }
}
use crate::bootstrap::JwtBundle;
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

    // Construct the shared reqwest::Client ONCE (finding B2). Every
    // outbound HTTP request — bootstrap POST, log forwarder flush,
    // downloader artifact fetch — reuses this client. Building a new
    // client per request (the old behavior, fixed in commit d2399f4)
    // avoids a blocking-client panic but still pays TLS pool init on
    // every cache miss and lets the runtime panic if the builder
    // returns from an async context. Holding one Arc<Client> across
    // the worker process eliminates both costs.
    let http_client: Arc<reqwest::Client> = Arc::new(
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| anyhow::anyhow!("build reqwest client: {e}"))?,
    );

    // Initialize JWT signer — signs outbound calls to the control plane's
    // /api/internal/* endpoints. Worker is per-tenant in this design; the
    // JWT carries the worker's tenant_id claim.
    //
    // Provisioning priority (Phase 4):
    //   1. Disk cache (`JWT_CACHE_PATH`) — if the cached JWT is still
    //      fresh, seed the signer without contacting the control plane.
    //   2. Bootstrap (`WORKER_BOOTSTRAP_PSK`) — on first `sign()`, fetch
    //      a JWT via `POST /api/internal/auth/token` and cache it.
    //   3. Legacy secret (`WORKER_JWT_SECRET`) — deprecated fallback so
    //      existing deployments keep working. Surfaces a deprecation
    //      warning so operators see it.
    //   4. No source configured — signer constructed with a callback
    //      that always errors; every outbound call returns 401. A
    //      startup warning makes the missing-config visible.
    let bootstrap_psk = config.worker_bootstrap_psk.clone();
    let bootstrap_control_plane_url = config.control_plane_url.clone();
    let bootstrap_worker_id = config.worker_id.clone();
    let bootstrap_region = config.region.clone();
    let bootstrap_tenant_id = config.worker_tenant_id.clone();
    let bootstrap_cache_path = config.jwt_cache_path.clone();
    let issuer = config.worker_jwt_issuer.clone();
    let jwt_signer = match bootstrap::load_from_disk(&config.jwt_cache_path).await {
        Ok(Some(cached)) => {
            // Try to seed from disk; with_seeded_token drops expired
            // caches and falls through to the bootstrap path on the
            // next `sign()`.
            let bundle = JwtBundle {
                token: cached.token,
                expires_at_unix: cached.expires_at_unix,
            };
            tracing::info!(
                path = %config.jwt_cache_path.display(),
                expires_at_unix = bundle.expires_at_unix,
                "jwt_signer: seeded from disk cache"
            );
            WorkerJwtSigner::new_with_callback(
                issuer.clone(),
                config.worker_id.clone(),
                config.region.clone(),
                config.worker_tenant_id.clone(),
                audience_opt(&config.worker_jwt_audience),
                make_bootstrap_callback(
                    http_client.clone(),
                    bootstrap_control_plane_url.clone(),
                    bootstrap_psk.clone(),
                    bootstrap_worker_id.clone(),
                    bootstrap_region.clone(),
                    bootstrap_tenant_id.clone(),
                    bootstrap_cache_path.clone(),
                ),
            )
            .with_seeded_token(bundle)
        }
        Ok(None) | Err(_) => {
            // Either no cache file, or the cache was unreadable /
            // corrupt. The corrupt-cache path is logged inside
            // `load_from_disk`; we just fall through to the bootstrap
            // path (or the env fallback).
            if let Some(psk) = &bootstrap_psk {
                tracing::info!("jwt_signer: no disk cache; will bootstrap via PSK on first sign()");
                WorkerJwtSigner::new_with_callback(
                    issuer.clone(),
                    config.worker_id.clone(),
                    config.region.clone(),
                    config.worker_tenant_id.clone(),
                    audience_opt(&config.worker_jwt_audience),
                    make_bootstrap_callback(
                        http_client.clone(),
                        bootstrap_control_plane_url.clone(),
                        Some(psk.clone()),
                        bootstrap_worker_id.clone(),
                        bootstrap_region.clone(),
                        bootstrap_tenant_id.clone(),
                        bootstrap_cache_path.clone(),
                    ),
                )
            } else if !config.worker_jwt_secret.is_empty() {
                tracing::warn!(
                    "WORKER_JWT_SECRET is set but WORKER_BOOTSTRAP_PSK is not; \
                     the secret is deprecated. Migrate to WORKER_BOOTSTRAP_PSK \
                     so the worker can self-provision a per-boot JWT."
                );
                #[allow(deprecated)]
                WorkerJwtSigner::new(
                    config.worker_jwt_secret.clone(),
                    issuer.clone(),
                    config.worker_id.clone(),
                    config.region.clone(),
                    config.worker_tenant_id.clone(),
                    audience_opt(&config.worker_jwt_audience),
                )
            } else {
                tracing::warn!(
                    "No JWT source configured (neither WORKER_BOOTSTRAP_PSK nor \
                     WORKER_JWT_SECRET is set, and no cached JWT exists); \
                     /api/internal/* calls will return 401 until provisioning \
                     is fixed. NATS heartbeats and the deployment supervisor \
                     keep running."
                );
                // Construct a signer with a callback that always errors
                // so every `sign()` returns Err and the downloader /
                // log forwarder can log the failure cleanly instead of
                // panicking.
                WorkerJwtSigner::new_with_callback(
                    issuer.clone(),
                    config.worker_id.clone(),
                    config.region.clone(),
                    config.worker_tenant_id.clone(),
                    audience_opt(&config.worker_jwt_audience),
                    || {
                        Err(anyhow::anyhow!(
                            "no JWT source configured: set WORKER_BOOTSTRAP_PSK \
                             or WORKER_JWT_SECRET"
                        ))
                    },
                )
            }
        }
    };

    // Initialize LogForwarder — receives tenant `emit_log` records from the
    // runtime AND worker-side `tracing` events via WorkerLogLayer. One per
    // worker; per-app AppLogContext travels with each guest record.
    //
    // The disk spool must be opened before the LogForwarder is
    // constructed, because the constructor drains any pending batches
    // (from a previous worker's crash or a control-plane outage).
    // Failing to open the spool is a hard error — a worker that can't
    // durably forward logs shouldn't start, since silent data loss is
    // worse than a refused boot.
    let spool = Arc::new(Spool::open(&config.spool_dir).await.map_err(|e| {
        anyhow::anyhow!("opening log spool at {}: {e}", config.spool_dir.display())
    })?);
    let log_forwarder = LogForwarder::new(
        config.control_plane_url.clone(),
        config.worker_id.clone(),
        config.region.clone(),
        jwt_signer.clone(),
        http_client.clone(),
        spool,
        config.spool_max_bytes,
    )
    .await;

    // The warnings above (deprecation + no-source) replace the old
    // "WORKER_JWT_SECRET is not set" message: with Phase 4 the
    // no-secret state is the "no source configured" case, and the
    // secret case is the "deprecated fallback" case. The combined
    // messaging keeps the visibility operators had before.

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

/// Build the closure that the `WorkerJwtSigner` invokes on cache miss
/// during the bootstrap path. The closure:
///   1. POSTs to the control plane's `POST /api/internal/auth/token`
///      with the PSK-derived HMAC signature.
///   2. Persists the resulting bundle to the JWT cache file so the
///      next restart skips the round trip.
///   3. Returns the bundle to the signer for caching in memory.
///
/// Returns a `'static + Send + Sync` closure suitable for
/// `WorkerJwtSigner::new_with_callback`. We clone the captured
/// `String`s so the closure owns them — `config` is dropped when
/// `main` returns, well before any `sign()` call would happen.
fn make_bootstrap_callback(
    client: Arc<reqwest::Client>,
    control_plane_url: String,
    psk: Option<String>,
    worker_id: String,
    region: String,
    tenant_id: String,
    cache_path: std::path::PathBuf,
) -> impl Fn() -> anyhow::Result<JwtBundle> + Send + Sync + 'static {
    move || {
        let psk = match &psk {
            Some(s) => s.as_bytes().to_vec(),
            None => {
                return Err(anyhow::anyhow!(
                    "bootstrap callback invoked but WORKER_BOOTSTRAP_PSK is unset"
                ));
            }
        };
        // Sync→async bridge for the bootstrap POST. The signer calls
        // `sign()` from inside tokio tasks (`Downloader::get_artifact`,
        // `LogForwarder::flush_now`), so we're on a tokio runtime
        // worker thread. Calling `Handle::current().block_on(...)`
        // from inside a multi-thread runtime worker panics with
        // "Cannot drop a runtime in a context where blocking is not
        // allowed" — the same anti-pattern that d2399f4 retired for
        // the reqwest sync/async bridge.
        //
        // The safe pattern (also used in `edge-test-helpers`):
        //   1. `Handle::try_current()` — succeeds if we're on a
        //      runtime worker thread; use `handle.block_on(...)`.
        //   2. Otherwise — no runtime; build a fresh single-thread
        //      runtime and call its `block_on(...)`.
        //
        // `block_on_in_runtime` is the shared helper at the bottom of
        // this file; it's used for both `fetch_token` and `save_to_disk`
        // below.
        let bundle = block_on_in_runtime(bootstrap::fetch_token(
            &control_plane_url,
            &client,
            &psk,
            &worker_id,
            &region,
            &tenant_id,
        ))?;
        // Best-effort cache write. A failure here is logged but not
        // fatal — the in-memory bundle is still valid for the rest
        // of this boot.
        if let Err(e) =
            block_on_in_runtime(bootstrap::save_to_disk(&cache_path, &bundle))
        {
            tracing::warn!(
                err = %e,
                path = %cache_path.display(),
                "jwt_signer: failed to persist bootstrap bundle to disk; \
                 next restart will re-bootstrap"
            );
        }
        Ok(bundle)
    }
}

/// Run an async future on the current tokio runtime if one exists,
/// otherwise build a fresh single-thread runtime. Safe to call from
/// inside a tokio worker thread (the original `Handle::current()` +
/// `block_on` would panic).
///
/// Used by `make_bootstrap_callback` for both the bootstrap POST and
/// the JWT cache write. Mirrors the pattern in
/// `edge-test-helpers/src/supervisor.rs::build_signer_for_config`
/// (review finding C2).
fn block_on_in_runtime<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle.block_on(future),
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build runtime for bootstrap callback");
            rt.block_on(future)
        }
    }
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
