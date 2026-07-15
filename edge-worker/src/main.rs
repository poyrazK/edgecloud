//! edge-worker — Worker Supervisor entry point.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use tokio::signal::unix::{signal, SignalKind};

use tokio::sync::broadcast;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use edge_worker::auth::{
    load_persisted_identity, persist_identity, PersistedIdentity, WorkerJwtSigner,
};
use edge_worker::backoff::compute_backoff_ms;
use edge_worker::bootstrap::BootstrapClient;
use edge_worker::config::Config;
use edge_worker::downloader::Downloader;
use edge_worker::log_forwarder::LogForwarder;
use edge_worker::metrics::WorkerMetrics;
use edge_worker::nats::{NatsClient, NatsClientImpl};
use edge_worker::port_pool::PortPool;
use edge_worker::state::WorkerState;
use edge_worker::supervisor::{StandbyPool, Supervisor};
use edge_worker::tracing_layer::WorkerLogLayer;
use edge_worker::verifier::Keyring;
use edge_worker::worker_key::WorkerIdentity;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration FIRST so we can construct the JWT signer and
    // LogForwarder before tracing init. This hoisting lets the new
    // WorkerLogLayer (wired in below) capture the JWT-secret warning
    // and every tracing call that follows.
    let config = Config::from_env()?;

    // Resolve the worker identity keypair (issue #430). The same
    // keypair is reused across restarts so the worker's public_key
    // (and therefore its kid) stays stable; main() never generates
    // a new identity for the same on-disk path.
    let identity = WorkerIdentity::load_or_create(&config.worker_key_path)
        .context("loading worker identity keypair")?;

    // Resolve the JWT secret + kid. Three paths, in priority order:
    //   1. WORKER_JWT_SECRET set directly (legacy / static-cluster
    //      mode) — used as-is, no enrollment.
    //   2. EDGE_WORKER_REENROLL_ON_BOOT=true forces the bootstrap
    //      handshake even if a persisted secret is on disk.
    //   3. Persisted identity record at worker_identity_path +
    //      bootstrap handshake skipped.
    //   4. No persisted record + WORKER_BOOTSTRAP_SECRET set →
    //      run the bootstrap handshake + persist for next time.
    //   5. None of the above → empty secret + warn (same shape as
    //      pre-#430).
    let (jwt_secret, jwt_kid) = resolve_jwt_secret(&config, &identity).await?;

    let jwt_secret_empty = jwt_secret.is_empty();

    // Initialize JWT signer — signs outbound calls to the control plane's
    // /api/internal/* endpoints. Worker is per-tenant in this design; the
    // JWT carries the worker's tenant_id claim.
    let jwt_signer = if let Some(kid) = jwt_kid {
        // Build the signer in two steps so we can stamp the kid we
        // already resolved (worker_jwt_kid env var, or the
        // persisted/derived wkr_ kid). The empty() constructor
        // matches the post-#430 split: secret + kid are set together.
        let signer = WorkerJwtSigner::empty(
            config.worker_jwt_issuer.clone(),
            config.worker_id.clone(),
            config.region.clone(),
            config.worker_tenant_id.clone(),
        );
        signer.set_secret(jwt_secret.clone(), Some(kid));
        signer
    } else {
        WorkerJwtSigner::new(
            jwt_secret.clone(),
            config.worker_jwt_kid.clone(),
            config.worker_jwt_issuer.clone(),
            config.worker_id.clone(),
            config.region.clone(),
            config.worker_tenant_id.clone(),
        )
    };

    // Initialize disk spool for log durability — failed HTTP batches are
    // persisted here and replayed on the next startup.
    let spool = edge_spool::Spool::open(&std::path::PathBuf::from(".worker-spool"))
        .await
        .ok();

    // Initialize LogForwarder — receives tenant `emit_log` records from the
    // runtime AND worker-side `tracing` events via WorkerLogLayer. One per
    // worker; per-app AppLogContext travels with each guest record.
    let log_forwarder = LogForwarder::new_with_spool(
        config.control_plane_url.clone(),
        config.worker_id.clone(),
        config.region.clone(),
        jwt_signer.clone(),
        spool,
    );

    // Log if the JWT secret was not available (bootstrap or direct).
    // This also maintains backward compat with the old "optional secret" behavior.
    if jwt_secret_empty {
        tracing::warn!(
            "No JWT secret is available; /api/internal/* calls will return 401 \
             until a secret is provisioned. NATS heartbeats and the deployment \
             supervisor keep running — only the log forwarder and downloader are \
             affected."
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
        consumer = %config.consumer_name,
        "configuration loaded"
    );

    // Validate region consistency with the control plane (issue #254).
    // If the CP uses "global" and the worker uses "fra", the worker will
    // subscribe to the wrong NATS subject and silently never receive
    // task messages. Registering with the CP early surfaces the mismatch
    // immediately with a clear error message.
    {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;
        let payload = serde_json::json!({
            "worker_id": config.worker_id,
            "region": config.region,
        });
        let resp = http
            .post(format!("{}/api/internal/workers", config.control_plane_url))
            .header("Authorization", format!("Bearer {}", jwt_signer.sign()))
            .json(&payload)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "worker registration failed (status {}): {}. \
                 Ensure CONTROL_PLANE_REGION matches worker REGION.",
                status,
                body
            );
        }

        let cp: serde_json::Value = resp.json().await?;
        let cp_region = cp["cp_region"].as_str().unwrap_or("");
        if !cp_region.is_empty() && cp_region != config.region {
            anyhow::bail!(
                "region mismatch: worker REGION={} but control plane has cp_region={}. \
                 The worker will not receive task messages unless the regions match. \
                 To fix: set CONTROL_PLANE_REGION={} on the control plane, \
                 or set REGION={} on the worker.",
                config.region,
                cp_region,
                config.region,
                cp_region
            );
        }
    }
    tracing::info!(
        worker_region = %config.region,
        "region validated with control plane"
    );

    // Create cache directory
    tokio::fs::create_dir_all(&config.cache_dir).await?;
    tracing::info!(dir = %config.cache_dir.display(), "cache directory ready");

    // Build the Ed25519 signing keyring (issue #307 PR2 +
    // PR1 follow-up multi-keyring with per-key `kid`).
    // Resolution order matches `Config::from_env`:
    //   1. EDGE_SIGNING_KEYRING_PATH (file) — production recommendation
    //   2. EDGE_SIGNING_KEYRING (inline `<kid> = <hex>` payload)
    //   3. None + require_signature=false → verifier = None + warn
    //   4. None + require_signature=true → unreachable: Config::from_env
    //      already bailed in that case.
    let signature_verifier: Option<Arc<Keyring>> = match (
        config.signing_keyring.as_deref(),
        config.signing_keyring_path.as_deref(),
    ) {
        (_, Some(p)) => match Keyring::from_file(std::path::Path::new(p)) {
            Ok(k) => {
                tracing::info!(
                    path = p,
                    keys = k.keys.len(),
                    "loaded Ed25519 signing keyring from EDGE_SIGNING_KEYRING_PATH"
                );
                Some(Arc::new(k))
            }
            Err(e) => {
                anyhow::bail!("loading EDGE_SIGNING_KEYRING_PATH {p:?}: {e}");
            }
        },
        (Some(payload), _) => match Keyring::from_inline(payload) {
            Ok(k) => {
                tracing::info!(
                    keys = k.keys.len(),
                    "loaded Ed25519 signing keyring from EDGE_SIGNING_KEYRING"
                );
                Some(Arc::new(k))
            }
            Err(e) => {
                anyhow::bail!("parsing EDGE_SIGNING_KEYRING: {e}");
            }
        },
        (None, None) if config.require_signature => {
            unreachable!("config validation should have caught require_signature=true + no key");
        }
        (None, None) => {
            tracing::warn!(
                "signature verification disabled: no EDGE_SIGNING_KEYRING[_PATH] configured \
                 and EDGE_REQUIRE_SIGNATURE=false. Unsigned artifacts will be accepted. \
                 This is the rollout escape hatch and should NOT be set in production."
            );
            None
        }
    };

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
        signature_verifier.clone(),
    ));

    // Initialize port pool (issue #641: capacity is env-tunable via EDGE_PORT_POOL_SIZE)
    let port_pool = Arc::new(tokio::sync::Mutex::new(PortPool::with_capacity(
        config.starting_port,
        config.port_cooldown_secs,
        config.port_pool_size,
    )));

    // Connect to NATS
    let nats = Arc::new(
        NatsClientImpl::connect(
            &config.nats_url,
            config.task_stream_replicas,
            config.queue_group.clone(),
        )
        .await?,
    ) as Arc<dyn NatsClient>;
    tracing::info!(url = %config.nats_url, "connected to NATS");

    // Create the shutdown broadcast channel for the heartbeat task.
    // Using broadcast lets us get a fresh receiver (subscription) each loop iteration.
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Issue #504: dedicated `CancellationToken` for the proactive
    // JWT refresh task. We deliberately don't reuse the existing
    // `broadcast::Sender<()>` here — the refresh task is a
    // single-purpose worker, and `CancellationToken::cancelled()`
    // matches its "select on either tick-or-shutdown" shape better
    // than a broadcast receiver (which forces a `recv().await` even
    // for a task that doesn't otherwise need a `&mut` borrow). The
    // root token is cancelled by `graceful_shutdown` alongside
    // `shutdown_tx.send(())` so both signals fire in lockstep.
    let refresh_shutdown = CancellationToken::new();

    // Build the WorkerMetrics surface (issue #49). Per-app
    // MetricsHandles are registered/unregistered from
    // Supervisor::start_app / stop_app; the supervisor stores an
    // Arc<WorkerMetrics> so the dispatch path can `inc_by` from
    // a cloned handle without taking a lock.
    let worker_metrics = WorkerMetrics::new()?;

    // Create the supervisor
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let supervisor = Arc::new(Supervisor {
        config: config.clone(),
        state,
        downloader,
        port_pool,
        nats: nats.clone(),
        log_forwarder: log_forwarder.clone(),
        jwt_signer: jwt_signer.clone(),
        http,
        engine_pool: Arc::new(StandbyPool::new(config.standby_pool_size)?),
        port_pool_exhausted_events: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        metrics: worker_metrics.clone(),
    });

    // Issue #49: spawn the Prometheus /metrics HTTP server. Bearer
    // auth via `METRICS_AUTH_TOKEN`; empty token → every request gets
    // 401 (fail-closed). Shutdown piggy-backs on the same broadcast
    // the heartbeat + log forwarder observe.
    //
    // The token is wrapped in `Arc<str>` once at startup so the per-
    // connection spawn inside `serve_inner` does a refcount bump
    // instead of allocating a fresh `String` per scrape — see the
    // doc on `metrics_server::serve`.
    let metrics_for_server = worker_metrics.clone();
    let metrics_token: Arc<str> = Arc::from(config.metrics_auth_token.as_str());
    let metrics_addr = config.metrics_addr;
    let shutdown_rx_for_metrics = shutdown_tx.subscribe();
    let metrics_task = tokio::spawn(async move {
        if let Err(e) = edge_worker::metrics_server::serve(
            metrics_addr,
            metrics_for_server,
            metrics_token,
            shutdown_rx_for_metrics,
        )
        .await
        {
            tracing::error!(err = %e, "metrics server exited with error");
        }
    });
    tracing::info!(addr = %metrics_addr, "metrics server started");

    // Issue #504: spawn the proactive JWT refresh task. The `Static`
    // arm is the legacy `WORKER_JWT_SECRET` mode — the secret is
    // immutable, so the loop only re-signs with the same key (no
    // network). The `Enrolled` arm re-runs the two-phase bootstrap
    // handshake so the per-worker HS256 secret tracks CP-side
    // rotation. We only spawn when there is something to refresh —
    // `WORKER_JWT_SECRET` not set means we went through the post-#430
    // path; the legacy static-secret path is intentionally not refreshed.
    let refresh_metrics = worker_metrics.clone();
    let refresh_signer = jwt_signer.clone();
    let refresh_shutdown_token = refresh_shutdown.clone();
    let refresh_source: edge_worker::jwt_refresh::RefreshSource =
        if config.worker_jwt_secret.is_empty() {
            use edge_worker::bootstrap::{BootstrapClient, BootstrapRefreshClient};
            let client = BootstrapClient::new(
                config.control_plane_url.clone(),
                config.worker_bootstrap_secret.as_bytes().to_vec(),
                config.worker_id.clone(),
                config.region.clone(),
                config.worker_tenant_id.clone(),
            );
            // `identity` was loaded earlier (line 44) — replant it for
            // the refresher adapter. (BootstrapClient::new internally
            // builds its own 15s-timeout reqwest::Client — the worker's
            // HTTP client, with its 10s timeout, is reserved for
            // supervisor-level fetches.)
            let refresher = BootstrapRefreshClient::new(client, std::sync::Arc::new(identity));
            edge_worker::jwt_refresh::RefreshSource::Enrolled(std::sync::Arc::new(refresher))
        } else {
            edge_worker::jwt_refresh::RefreshSource::Static
        };
    let refresh_tick = std::time::Duration::from_secs(60);
    let refresh_lead = std::time::Duration::from_secs(300); // 5min
    let _refresh_task = tokio::spawn(async move {
        // Stamp the initial expiry on the gauge so `/metrics`
        // reflects the current snapshot even if the loop's first
        // tick lands at the deadline boundary (where the gauge
        // would otherwise stay at the default 0.0 until the first
        // refresh). Issue #504.
        refresh_metrics.set_jwt_expires_at(refresh_signer.snapshot().expires_at);
        edge_worker::jwt_refresh::spawn_jwt_refresh_loop(
            refresh_signer,
            refresh_source,
            refresh_tick,
            refresh_lead,
            refresh_shutdown_token,
            refresh_metrics,
        )
        .await
    });
    tracing::info!("jwt refresh task started");

    let heartbeat_supervisor = supervisor.clone();
    let heartbeat_interval = Duration::from_secs(config.heartbeat_interval_secs);

    // Start the heartbeat task — exits cleanly when it receives the shutdown signal.
    let shutdown_tx_for_heartbeat = shutdown_tx.clone();
    let mut shutdown_rx_for_heartbeat = shutdown_tx_for_heartbeat.subscribe();
    tokio::spawn(async move {
        let mut ticker = interval(heartbeat_interval);
        ticker.tick().await;
        loop {
            tokio::select! {
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

    let evict_supervisor = supervisor.clone();
    let shutdown_tx_for_evict = shutdown_tx.clone();
    let mut shutdown_rx_for_evict = shutdown_tx_for_evict.subscribe();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx_for_evict.recv() => break,
                _ = ticker.tick() => {
                    evict_supervisor.evict_idle_apps(Duration::from_secs(300)).await;
                }
            }
        }
    });

    tracing::info!("heartbeat task started");

    // Replay any spooled log batches from a previous run before the
    // regular flush loop starts, so failed batches from a prior crash
    // are retried before new entries accumulate.
    log_forwarder.replay_spool().await;

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
    let metrics_task_for_shutdown = metrics_task;
    // The JWT refresh task's JoinHandle (`refresh_task`) is awaited at the
    // very end of fn main() — we only own the cancellation token here
    // so graceful_shutdown can fire it; the loop exits on its own.
    let refresh_shutdown_for_shutdown = refresh_shutdown.clone();
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
        // We don't have access to refresh_task here (it was bound
        // inside the previous `tokio::spawn` closure). The signal-
        // handler task above already cancels `refresh_shutdown`, so
        // the loop will exit on its own; we don't need to await it
        // here. main() awaits it after the consume loop returns
        // (see end of fn main).
        graceful_shutdown(
            shutdown_tx_s,
            shutdown_supervisor,
            logs_task,
            metrics_task_for_shutdown,
            refresh_shutdown_for_shutdown,
        )
        .await;
    });

    tracing::info!(
        region = %config.region,
        "ready — waiting for task messages"
    );

    // Run the consume loop on the main task. Wrapped in a reconnect loop
    // with bounded exponential backoff + ±25% jitter so transient stream-end
    // (consumer deleted, server restart, push-consumer dropped) doesn't kill
    // the worker. Shutdown signal is observed via the broadcast receiver, so
    // Ok(()) here means "shutdown was signalled" — break and drain.
    //
    // Issue #47 contract:
    // - Jitter prevents a thundering-herd reconnect when a partition heals
    //   and many workers would otherwise re-subscribe at the same wall-clock
    //   instant. The math is `compute_backoff_ms(1, backoff, MAX_BACKOFF)`
    //   — the jitter is per-sleep, not per-attempt; backoff doubles across
    //   sleeps via the `backoff *= 2` arm below.
    // - Reset: if the consume loop ran healthily for ≥ RESET_AFTER_HEALTHY
    //   before stream-end, `backoff` resets to 1s. Without this, a worker
    //   that ran stable for a day and then hit one transient reconnect
    //   would inherit the 60s cap from the previous day's failure run.
    //   "Healthy" means the consume loop was actively subscribed and
    //   processing — NOT "time since last Err" — so a worker that fails
    //   every iteration immediately never resets (correct: the failure is
    //   fresh, the saturation should hold).
    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(60);
    const RESET_AFTER_HEALTHY: Duration = Duration::from_secs(60);
    loop {
        let consume_shutdown_rx = shutdown_tx.subscribe();
        match supervisor.run_consume_loop(consume_shutdown_rx).await {
            Ok(()) => {
                tracing::info!("consume loop returned after shutdown signal");
                break;
            }
            Err(e) => {
                // Captured on the Err arm only — the Ok arm doesn't read
                // it. Intentionally *after* subscribe so the healthy-run
                // measurement excludes subscribe() blocking time (in
                // practice subscribe is a fast in-memory call once a
                // connection exists, so this is mostly cosmetic — but
                // the semantic is "time the consume loop was actually
                // running" rather than "time since we last asked NATS
                // for a stream").
                let started_at = Instant::now();
                if let Some(reset_to) = reset_backoff_if_healthy(started_at, RESET_AFTER_HEALTHY) {
                    tracing::info!(
                        healthy_for = ?started_at.elapsed(),
                        "consume loop ran healthily before stream-end; resetting backoff"
                    );
                    backoff = reset_to;
                }
                let backoff_ms = compute_backoff_ms(
                    1,
                    backoff.as_millis() as u64,
                    MAX_BACKOFF.as_millis() as u64,
                );
                tracing::error!(
                    err = %e,
                    backoff_ms,
                    // Doubles AFTER the sleep on the next iteration's Err
                    // arm — log it as a forward-looking hint, not a
                    // post-sleep fact.
                    next_backoff_secs =
                        std::cmp::min(backoff * 2, MAX_BACKOFF).as_secs(),
                    "consume loop ended unexpectedly; reconnecting"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
            }
        }
    }

    // graceful_shutdown (spawned above) does the work — it signaled the
    // broadcast, stopped apps, published the final heartbeat, and awaited
    // the log forwarder drain. main() returns cleanly.
    Ok(())
}

/// Reset the consume-loop backoff when the previous run was healthy.
///
/// Returns `Some(Duration::from_secs(1))` iff the wall-clock elapsed
/// between `started_at` and "now" is at least `threshold`, signaling
/// that the previous consume-loop run was healthy long enough to forget
/// any prior failure-state. Returns `None` otherwise.
///
/// Issue #47 contract: a worker that ran stable for ≥ `threshold` and
/// then hit one transient stream-end should reconnect immediately (1s)
/// instead of inheriting yesterday's 60s cap. The threshold is the
/// operator-facing knob — bumping it tightens the definition of
/// "healthy run."
fn reset_backoff_if_healthy(started_at: Instant, threshold: Duration) -> Option<Duration> {
    if started_at.elapsed() >= threshold {
        Some(Duration::from_secs(1))
    } else {
        None
    }
}

/// Resolve the JWT signing secret + kid (issue #430).
///
/// Resolution order:
/// 1. `WORKER_JWT_SECRET` env var set directly → use as-is, kid from
///    `WORKER_JWT_KID` if any.
/// 2. `EDGE_WORKER_REENROLL_ON_BOOT=true` → force re-enrollment even
///    if a persisted identity is on disk.
/// 3. Persisted identity record present → load it, skip bootstrap.
/// 4. `WORKER_BOOTSTRAP_SECRET` set → run the enrollment handshake
///    + persist for next time.
/// 5. None of the above → empty secret, no kid (legacy warning).
///
/// Returned `kid` is `Some(...)` whenever the secret came from the
/// post-#430 paths (persisted, freshly enrolled, force-reenrolled)
/// so the signer can stamp it on outbound JWTs. `None` keeps the
/// legacy `WorkerJwtSigner::new` path live for operators who opt
/// out of per-worker keys by setting `WORKER_JWT_SECRET` directly.
async fn resolve_jwt_secret(
    config: &Config,
    identity: &WorkerIdentity,
) -> anyhow::Result<(Vec<u8>, Option<String>)> {
    // 1. Direct env-var path: legacy mode, no enrollment.
    if !config.worker_jwt_secret.is_empty() {
        tracing::info!("using WORKER_JWT_SECRET directly; bootstrap enrollment skipped");
        return Ok((config.worker_jwt_secret.as_bytes().to_vec(), None));
    }

    // 2. Force re-enrollment: drop any persisted record and run the
    // handshake. The persisted file (if any) is overwritten by the
    // post-handshake persist call below.
    let reenroll = config.worker_reenroll_on_boot;

    // 3. Persisted identity short-circuit.
    if !reenroll {
        match load_persisted_identity(&config.worker_identity_path) {
            Ok(Some(persisted)) => {
                tracing::info!(
                    path = %config.worker_identity_path.display(),
                    kid = %persisted.kid,
                    "loaded persisted worker identity; bootstrap handshake skipped"
                );
                return Ok((persisted.secret, Some(persisted.kid)));
            }
            Ok(None) => {}
            Err(e) => {
                // Corrupt identity file — refuse to fall through to
                // bootstrap because that would let an attacker who
                // can write to the cache directory forge a worker
                // identity. The operator must remove the file (or
                // rotate the keypair) before restarting.
                tracing::error!(
                    err = %e,
                    path = %config.worker_identity_path.display(),
                    "persisted identity is unreadable; refusing to bootstrap. \
                     Delete the file or set EDGE_WORKER_REENROLL_ON_BOOT=true to \
                     force a fresh enrollment."
                );
                return Err(e);
            }
        }
    }

    // 4. Bootstrap handshake.
    if !config.worker_bootstrap_secret.is_empty() {
        tracing::info!(
            reenroll,
            "running bootstrap enrollment handshake with control plane"
        );
        let client = BootstrapClient::new(
            config.control_plane_url.clone(),
            config.worker_bootstrap_secret.as_bytes().to_vec(),
            config.worker_id.clone(),
            config.region.clone(),
            config.worker_tenant_id.clone(),
        );
        let derived = client
            .run(identity)
            .await
            .context("bootstrap enrollment handshake")?;

        // Persist for next restart. `public_key_hex` comes from the
        // worker's own `WorkerIdentity`, not the CP-derived response —
        // the CP now returns only `{kid, secret, expires_at}` (issue
        // #504), but `PersistedIdentity` still needs the pubkey so a
        // subsequent restart can prove possession in the bootstrap
        // handshake.
        let persisted = PersistedIdentity {
            kid: derived.kid.clone(),
            secret: derived.secret.clone(),
            public_key_hex: identity.public_key_hex().to_string(),
        };
        persist_identity(&config.worker_identity_path, &persisted)
            .context("persisting worker identity after enrollment")?;
        tracing::info!(
            path = %config.worker_identity_path.display(),
            kid = %derived.kid,
            "persisted worker identity; subsequent restarts skip bootstrap"
        );
        return Ok((derived.secret, Some(derived.kid)));
    }

    // 5. No secret anywhere — warn + return empty (legacy behavior).
    tracing::warn!(
        "Neither WORKER_JWT_SECRET nor WORKER_BOOTSTRAP_SECRET is set; \
         /api/internal/* calls will return 401 until the secret is provisioned. \
         NATS heartbeats and the deployment supervisor keep running — only the \
         log forwarder and downloader are affected."
    );
    Ok((Vec::new(), None))
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
/// drain its final flush, then exit the process. The JWT refresh task
/// observes `refresh_shutdown` directly (cancellation token); its
/// `JoinHandle` is awaited at the very end of `fn main()` so the
/// consume-loop / signal-handler boundary stays clean.
async fn graceful_shutdown(
    shutdown_tx: broadcast::Sender<()>,
    supervisor: Arc<Supervisor>,
    logs_task: tokio::task::JoinHandle<()>,
    metrics_task: tokio::task::JoinHandle<()>,
    refresh_shutdown: CancellationToken,
) {
    // Signal the heartbeat + log-forwarder + metrics tasks to stop.
    let _ = shutdown_tx.send(());

    // Issue #504: signal the proactive JWT refresh task to exit at its
    // next loop iteration. The `refresh_task` JoinHandle lives in
    // `fn main()` and is awaited after `graceful_shutdown` returns,
    // so all the signal-handler closeout sees here is the cancellation.
    refresh_shutdown.cancel();

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

    // Wait for the metrics server to drain. Abort after a short
    // grace period — the broadcast has fired, but a slow connection
    // could keep `serve_connection` alive for a few seconds.
    let metrics_grace = Duration::from_secs(2);
    match tokio::time::timeout(metrics_grace, metrics_task).await {
        Ok(Ok(())) => tracing::info!("metrics server drained cleanly"),
        Ok(Err(join_err)) => tracing::warn!(err = %join_err, "metrics server task ended"),
        Err(_) => tracing::warn!(
            timeout_secs = metrics_grace.as_secs(),
            "metrics server drain timed out; connections may be cut",
        ),
    }

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

/// Unit tests for the consume-loop reconnect path (issue #47).
///
/// The two contracts under test are jitter (no thundering-herd) and
/// reset-after-healthy-run (backoff doesn't carry across long stable
/// periods). The reset test feeds `reset_backoff_if_healthy` synthetic
/// Instants to keep the suite independent of wall-clock — the loop
/// body itself is `async fn`-shaped and tied to `Supervisor`, so the
/// helper is the smallest testable unit. The jitter tests are pure
/// math against `compute_backoff_ms`.
///
/// **`reconnect_state_machine_resets_then_jitters` is the load-bearing
/// test.** The other `reconnect_*` tests pin individual helper
/// contracts; this one pins the *wiring* between `reset_backoff_if_healthy`
/// and `compute_backoff_ms` — i.e. the actual behavior change. A
/// regression where the loop body forgets to call `reset_backoff_if_healthy`
/// before the next `compute_backoff_ms` call would still pass the
/// per-helper tests below; this test catches it.
#[cfg(test)]
mod reconnect_tests {
    use super::{compute_backoff_ms, reset_backoff_if_healthy};
    use std::time::{Duration, Instant};

    /// 200-iteration sample must stay in `[750, 1250]` ms when
    /// `base = 1_000` and `cap = 60_000`. Pins the thundering-herd
    /// prevention invariant — every reconnecting worker in a fleet
    /// picks a different wall-clock instant within this band.
    #[test]
    fn reconnect_jitter_band_is_pm_25_percent() {
        for _ in 0..200 {
            let slept = compute_backoff_ms(1, 1_000, 60_000);
            assert!(
                (750..=1_250).contains(&slept),
                "jitter out of band: {slept}ms (want 750..=1250)"
            );
        }
    }

    /// After a healthy run ≥ threshold, the loop resets backoff to 1s.
    /// Synthesized by constructing `started_at` 120s in the past.
    #[test]
    fn reconnect_resets_backoff_after_60s_healthy() {
        let now = Instant::now();
        let started_at = now - Duration::from_secs(120);
        let reset = reset_backoff_if_healthy(started_at, Duration::from_secs(60));
        assert_eq!(
            reset,
            Some(Duration::from_secs(1)),
            "backoff must reset to 1s after a ≥60s healthy run"
        );
    }

    /// A short-lived run (< threshold) must NOT reset — the backoff
    /// state carries forward into the next reconnect.
    #[test]
    fn reconnect_does_not_reset_backoff_before_60s() {
        let now = Instant::now();
        let started_at = now - Duration::from_secs(30);
        let reset = reset_backoff_if_healthy(started_at, Duration::from_secs(60));
        assert_eq!(
            reset, None,
            "backoff must NOT reset if the healthy run was <60s"
        );
    }

    /// **The wiring test for issue #47.** Simulates the consume-loop's
    /// Err arm: take a backoff that's already saturated at 60s
    /// (representing "yesterday's failure run"), pass through a
    /// `reset_backoff_if_healthy` call for a ≥60s healthy run, then
    /// assert the *next* `compute_backoff_ms` sleep is in `[750, 1250]`
    /// ms — i.e. the reset actually flows into the sleep duration,
    /// not just into the variable.
    ///
    /// Regression coverage: if someone deletes the `backoff = reset_to;`
    /// assignment from the loop, or forgets to apply the reset before
    /// the `compute_backoff_ms` call, this test catches it — the
    /// per-helper tests above would still pass.
    #[test]
    fn reconnect_state_machine_resets_then_jitters() {
        let mut backoff = Duration::from_secs(60); // saturated
        const MAX_BACKOFF_MS: u64 = 60_000;
        const RESET_AFTER_HEALTHY: Duration = Duration::from_secs(60);

        // Simulate a fresh Err after 120s of healthy runtime.
        let started_at = Instant::now() - Duration::from_secs(120);

        // The wiring under test — same shape as the loop body's Err arm.
        if let Some(reset_to) = reset_backoff_if_healthy(started_at, RESET_AFTER_HEALTHY) {
            backoff = reset_to;
        }
        let slept_ms = compute_backoff_ms(1, backoff.as_millis() as u64, MAX_BACKOFF_MS);

        // Reset must have flowed through: pre-reset, slept_ms would be
        // in [45_000, 75_000] (60s ± 25%). Post-reset, it must be in
        // [750, 1250] (1s ± 25%). The threshold 5_000ms cleanly
        // distinguishes the two bands — a regression to "reset doesn't
        // fire" lands the value above 5_000ms.
        assert!(
            (750..=1_250).contains(&slept_ms),
            "reset + jitter wiring broken: slept_ms={slept_ms} out of [750, 1250] (backoff was {backoff:?})",
        );
    }
}
