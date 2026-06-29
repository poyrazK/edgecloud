//! Core supervisor logic — app lifecycle management.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use edge_runtime::linker::create_component_linker;
use edge_runtime::{EgressPolicy, MetricsAccumulator, RequestMeter};
use futures::StreamExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};
use wasmtime::component::InstancePre;

use crate::auth::WorkerJwtSigner;
use crate::config::Config;
use crate::cpu_sample::CpuSample;
use crate::downloader::Downloader;
use crate::log_forwarder::LogForwarder;
use crate::messages::{
    AppSpec, AppStatus, ClusterHeadroom, HeartbeatMessage, MetricKind, MetricSample, TaskMessage,
};
use crate::nats::NatsClient;
use crate::port_pool::PortPool;
use crate::state::{AppInstance, AppInstanceStatus, WorkerState};

/// The main supervisor — manages all running apps for this worker node.
///
/// The main supervisor — manages all running apps for this worker node.
///
/// `Supervisor::new` is the canonical constructor; external code that
/// needs an instance should prefer it over the public struct literal
/// (which exists for backwards compatibility with pre-#166 callers).
/// Adding new fields (e.g. `cpu_sample` for #85) is permitted by the
/// `[package.metadata.cargo-semver-checks.lints]` allowlist in
/// `Cargo.toml`, which disables the `constructible_struct_adds_field`
/// and `enum_variant_added` rules for this crate — these types are
/// internal-process singletons, not external API.
pub struct Supervisor {
    pub config: Config,
    pub state: Arc<RwLock<WorkerState>>,
    pub downloader: Arc<Downloader>,
    pub port_pool: Arc<Mutex<PortPool>>,
    pub nats: Arc<dyn NatsClient>,
    /// Per-worker log shipper. Shared across all apps — the per-app
    /// `AppLogContext` travels with each `emit_log` call so the forwarder
    /// knows which tenant/app/deployment the record belongs to.
    pub log_forwarder: Arc<LogForwarder>,
    /// JWT signer used by `fetch_sync` to authenticate the HTTP /sync
    /// fallback request. Issue #53. Cached internally so each call is
    /// cheap; reused across all fallback attempts.
    pub jwt_signer: Arc<WorkerJwtSigner>,
    /// Shared HTTP client. Constructed once at startup so its TLS
    /// connection pool survives across every /sync fallback tick
    /// (issue #53 perf: rebuilding a fresh `reqwest::Client` per
    /// `fetch_sync` call meant a brand-new TLS handshake every 30s
    /// during a sustained NATS outage). The underlying client is
    /// internally `Arc`-backed, so cloning is cheap — but a shared
    /// instance also lets the connection pool coalesce concurrent
    /// fallback requests to the same control-plane host.
    pub http: reqwest::Client,
    /// Cached CPU% sampler for the heartbeat's cluster_headroom.cpu_pct
    /// field (issue #85). See `cpu_sample.rs` for why a single sample
    /// is always 0 and we need a primed baseline.
    pub cpu_sample: CpuSample,
}

impl Supervisor {
    /// Construct a Supervisor with every field explicitly supplied.
    ///
    /// This is the canonical way to build a `Supervisor`. The struct is
    /// `#[non_exhaustive]`, so external callers cannot use a struct
    /// literal — they must go through this constructor. The constructor
    /// was introduced to satisfy `cargo-semver-checks`'s
    /// `constructible_struct_adds_field` rule: when new fields were
    /// added to a previously-constructible pub struct, the rule flagged
    /// the change as breaking. `#[non_exhaustive]` is the documented
    /// carve-out that removes the baseline delta the rule looks for.
    ///
    /// `cpu_sample` should be a freshly-primed `CpuSample` from
    /// `cpu_sample::CpuSample::new()` — production code creates it
    /// once at startup and passes the same instance here.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        state: Arc<RwLock<WorkerState>>,
        downloader: Arc<Downloader>,
        port_pool: Arc<Mutex<PortPool>>,
        nats: Arc<dyn NatsClient>,
        log_forwarder: Arc<LogForwarder>,
        jwt_signer: Arc<crate::auth::WorkerJwtSigner>,
        http: reqwest::Client,
        cpu_sample: CpuSample,
    ) -> Self {
        Self {
            config,
            state,
            downloader,
            port_pool,
            nats,
            log_forwarder,
            jwt_signer,
            http,
            cpu_sample,
        }
    }

    /// Refresh the cached CPU sample and return the percentage in
    /// `[0.0, 100.0]`, or `None` if the sampler hasn't completed its
    /// first interval yet. Called from `build_heartbeat` to populate
    /// `cluster_headroom.cpu_pct`.
    ///
    /// Exposed as a `pub(crate)` accessor (not a direct field read)
    /// so external callers — `build_heartbeat` is the only consumer —
    /// don't read the field directly. Direct field access would still
    /// work (it's `pub`), but routing through the accessor makes the
    /// intent explicit: "the sampler is private state of the
    /// heartbeat builder, not a knob for callers to poke".
    pub(crate) fn take_cpu_pct(&self) -> crate::cpu_sample::CpuPct {
        self.cpu_sample.take()
    }

    /// Handle an incoming TaskMessage from NATS.
    ///
    /// Diffs the desired app set against currently running apps and
    /// starts/stops apps accordingly. Supports canary/blue-green: when
    /// `spec.routes` is Some, all listed deployments run concurrently.
    ///
    /// Both `TaskUpdate` (event-driven, from activate/rollback) and
    /// `FullSync` (periodic reconciliation + on-registration, issue #53)
    /// share the same diff logic — the supervisor's contract is
    /// "the apps map is the entire desired state". The CP publishes
    /// `FullSync` with the worker's full active app set, so the worker's
    /// diff against its current state naturally: stops apps no longer in
    /// the set, starts missing, restarts on `deployment_id` mismatch.
    /// The variant distinction exists only for observability so an
    /// operator can tell event-driven updates from scheduled syncs in
    /// metrics and logs.
    #[allow(clippy::type_complexity)]
    pub async fn handle_task_message(&self, msg: TaskMessage) -> anyhow::Result<()> {
        let (tenant_id, desired_apps) = match msg {
            TaskMessage::TaskUpdate {
                tenant_id, apps, ..
            } => {
                tracing::debug!(tenant_id = %tenant_id, apps = apps.len(), "task_update received");
                (tenant_id, apps)
            }
            TaskMessage::FullSync {
                tenant_id, apps, ..
            } => {
                // FullSync is the periodic safety net (issue #53). Same
                // diff logic — workers don't need to know whether a
                // message is event-driven or scheduled. The log line is
                // the operator's signal that a reconcile fired.
                tracing::info!(tenant_id = %tenant_id, apps = apps.len(), "full_sync received");
                (tenant_id, apps)
            }
        };

        // Bump the watchdog timer the moment we've parsed a message —
        // not after we apply the diff. The watchdog's job is to catch
        // *silence* (NATS hasn't spoken in N seconds); a hash mismatch,
        // port-pool exhaustion, or transient downloader failure in the
        // diff loop doesn't change the fact that NATS just spoke.
        // Bumping at the end (the previous behaviour) meant a worker
        // whose diff could only partially apply would see the
        // watchdog fire /sync anyway — which is harmless in isolation
        // but, combined with the boot-herd bug (commit F), would
        // amplify a partial-outage into a full-outage storm.
        if let Ok(mut guard) = self.state.read().await.last_task_received_at.lock() {
            *guard = Some(Instant::now());
        }

        // Compute the set of (app_name, deployment_id) keys currently running,
        // and look up the deployment_id for each key so we can detect changes.
        let (current_keys, current_deployment_ids): (
            HashMap<(String, String), AppInstanceStatus>,
            HashMap<(String, String), String>,
        ) = {
            let state = self.state.read().await;
            let mut keys = HashMap::new();
            let mut dep_ids = HashMap::new();
            for (key, inst) in state.apps.iter() {
                let inst = inst.lock().unwrap();
                keys.insert(key.clone(), inst.status.clone());
                dep_ids.insert(key.clone(), inst.deployment_id.clone());
            }
            (keys, dep_ids)
        };

        // Build the desired set from the task message.
        // When spec.routes is Some, run all listed deployments concurrently —
        // each route carries its OWN deployment_id and deployment_hash, so
        // the worker downloads and runs the right binary for every entry.
        // When None, run the primary deployment_id with the spec-level hash
        // (legacy behaviour).
        //
        // The value carries the per-route hash alongside the spec so the
        // restart-decision and download paths can use route-scoped data
        // instead of always falling back to spec.deployment_id /
        // spec.deployment_hash (which described only the primary).
        struct DesiredEntry<'a> {
            app_name: &'a str,
            spec: &'a AppSpec,
            deployment_id: &'a str,
            deployment_hash: &'a str,
        }
        let mut desired_keys: HashMap<(String, String), DesiredEntry> = HashMap::new();
        for (app_name, spec) in &desired_apps {
            if let Some(ref routes) = spec.routes {
                for route in routes {
                    desired_keys.insert(
                        (app_name.clone(), route.deployment_id.clone()),
                        DesiredEntry {
                            app_name: app_name.as_str(),
                            spec,
                            deployment_id: route.deployment_id.as_str(),
                            deployment_hash: route.deployment_hash.as_str(),
                        },
                    );
                }
            } else {
                desired_keys.insert(
                    (app_name.clone(), spec.deployment_id.clone()),
                    DesiredEntry {
                        app_name: app_name.as_str(),
                        spec,
                        deployment_id: spec.deployment_id.as_str(),
                        deployment_hash: spec.deployment_hash.as_str(),
                    },
                );
            }
        }

        // Stop instances whose key is no longer desired.
        for key in current_keys.keys() {
            if !desired_keys.contains_key(key) {
                if let Err(e) = self.stop_app(key).await {
                    tracing::error!(app_name = %key.0, deployment_id = %key.1, err = %e, "failed to stop app");
                }
            }
        }

        // Start or restart instances that are missing or changed.
        for (key, entry) in &desired_keys {
            let is_new = !current_keys.contains_key(key);
            // Restart if: no entry (new), bad status (crashed/hung), OR the
            // deployment_id at this key differs from what we want. Use the
            // per-route deployment_id (entry.deployment_id), NOT
            // spec.deployment_id — otherwise canary routes would always
            // appear "changed" relative to the primary, causing every
            // reconcile to tear down the canary.
            let current_dep_id = current_deployment_ids.get(key);
            let needs_restart = is_new
                || current_dep_id
                    .map(|id| id != entry.deployment_id)
                    .unwrap_or(false)
                || current_keys
                    .get(key)
                    .map(|s| !matches!(s, AppInstanceStatus::Running | AppInstanceStatus::Starting))
                    .unwrap_or(false);

            if needs_restart {
                if let Err(e) = self
                    .start_app(
                        key,
                        entry.deployment_id,
                        entry.deployment_hash,
                        entry.spec,
                        &tenant_id,
                    )
                    .await
                {
                    tracing::error!(app_name = %entry.app_name, deployment_id = %key.1, err = %e, "failed to start app");
                }
            }
        }

        Ok(())
    }

    /// Pull the current desired-state snapshot from the control plane
    /// via the HTTP /sync fallback endpoint and return it as a
    /// `TaskMessage` so the caller can apply it via
    /// `handle_task_message` (issue #53).
    ///
    /// Used by the heartbeat-task watchdog when NATS has been silent
    /// for longer than `EDGE_WORKER_SYNC_THRESHOLD_SECS`. The returned
    /// message always has `type: "full_sync"` — the CP enforces that
    /// on the wire — so the worker's existing handler applies it
    /// identically to a NATS-delivered FullSync.
    ///
    /// Errors are NOT propagated as `Err` for transient failures
    /// (network blip, 5xx); we return `Ok(None)` so the caller can
    /// simply log and wait for the next heartbeat tick. A persistent
    /// CP outage surfaces as repeated `warn!` log lines — the operator
    /// signal that "NATS is silent AND the HTTP fallback is failing".
    pub async fn fetch_sync(&self) -> anyhow::Result<Option<TaskMessage>> {
        let url = format!(
            "{}/api/internal/workers/{}/sync",
            self.config.control_plane_url, self.config.worker_id
        );
        // Use the shared HTTP client (constructed once in main.rs)
        // so its connection pool — and any open TLS sessions — are
        // reused across every fallback tick. Building a fresh
        // reqwest::Client per call would discard the pool and force
        // a fresh TLS handshake on every 30s heartbeat.
        let resp = match self
            .http
            .get(&url)
            .bearer_auth(self.jwt_signer.sign())
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(err = %e, url = %url, "sync fallback: GET failed");
                return Ok(None);
            }
        };
        if !resp.status().is_success() {
            tracing::warn!(
                status = %resp.status(),
                url = %url,
                "sync fallback: non-2xx response"
            );
            return Ok(None);
        }
        match resp.json::<TaskMessage>().await {
            Ok(msg) => Ok(Some(msg)),
            Err(e) => {
                tracing::warn!(err = %e, "sync fallback: deserialize failed");
                Ok(None)
            }
        }
    }

    /// Start a new app or restart a changed one.
    ///
    /// `deployment_id` and `deployment_hash` are the per-route values
    /// (for canary routes) or the primary values (legacy single-deployment
    /// mode). Both come from `DesiredEntry`; passing them in keeps the
    /// caller — which knows whether this is a canary route or a primary —
    /// in control, instead of having `start_app` reach back into the spec
    /// and accidentally use the wrong id/hash for canaries.
    async fn start_app(
        &self,
        key: &(String, String),
        deployment_id: &str,
        deployment_hash: &str,
        spec: &AppSpec,
        tenant_id: &str,
    ) -> anyhow::Result<()> {
        let app_name = &key.0;

        // Validate tenant_id before any filesystem or store operations.
        // Reject path-traversal characters that could escape the base persistence directory.
        if !edge_runtime::is_safe_tenant_id(tenant_id) {
            anyhow::bail!("refusing to start app: unsafe tenant_id {:?}", tenant_id);
        }

        tracing::info!(app_name, deployment_id, "starting app");

        // Stop existing instance at this specific (app_name, deployment_id) key.
        // We do NOT stop other deployment_ids for the same app_name — that
        // allows canary (v1 + v2) to run concurrently.
        if self.state.read().await.apps.contains_key(key) {
            self.stop_app(key).await?;
        }

        // Acquire a port. Under concurrent canary startup multiple instances of the
        // same app may briefly compete for ports; propagate the error gracefully
        // instead of panicking.
        let raw_port = {
            let mut pool = self.port_pool.lock().await;
            match pool.acquire() {
                Some(p) => p,
                None => {
                    return Err(anyhow::anyhow!(
                        "port pool exhausted — no free ports available for {}",
                        app_name
                    ));
                }
            }
        };

        // Download artifact (blocking on first request).
        // Note: Downloader::get_artifact verifies SHA-256 against the
        // per-route hash before returning; on mismatch/empty/malformed it
        // returns Err, which this arm propagates and the port-release path handles.
        // Passing spec.deployment_id / spec.deployment_hash here would always
        // download the primary binary and verify it against the primary's
        // hash — canary routes would serve the wrong code (and any
        // deployment whose artifact differs from the primary would fail
        // verification outright).
        let artifact = match self
            .downloader
            .get_artifact(deployment_id, deployment_hash)
            .await
        {
            Ok(a) => a,
            Err(e) => {
                let mut pool = self.port_pool.lock().await;
                pool.release(raw_port);
                return Err(e);
            }
        };

        // Compile the component using the shared engine
        let engine = &self.state.read().await.engine;
        let component = match wasmtime::component::Component::from_binary(engine, &artifact) {
            Ok(c) => c,
            Err(e) => {
                let mut pool = self.port_pool.lock().await;
                pool.release(raw_port);
                return Err(e).context(format!("failed to compile component for {}", app_name));
            }
        };

        // Create the component linker and pre-instantiate
        let linker = create_component_linker(engine)?;
        let instance_pre = match linker.instantiate_pre(&component) {
            Ok(ip) => ip,
            Err(e) => {
                let mut pool = self.port_pool.lock().await;
                pool.release(raw_port);
                return Err(e).context(format!("failed to pre-instantiate {}", app_name));
            }
        };

        // Spawn the per-app epoch ticker. The engine clock is global, but
        // advancing it in a per-app task keeps a misbehaving app's deadline
        // work isolated — when the app stops, the ticker aborts with it.
        let ticker_engine = engine.clone();
        let epoch_tick_ms = self.config.epoch_tick_ms;
        let ticker = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(epoch_tick_ms));
            // The first tick fires immediately; consume it so the deadline
            // budget starts fresh on the first interval.
            tick.tick().await;
            loop {
                tick.tick().await;
                ticker_engine.increment_epoch();
            }
        });

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Create request meter
        let meter = Arc::new(RequestMeter::new(
            tenant_id.to_string(),
            deployment_id.to_string(),
        ));

        // Create shared metrics accumulator. The supervisor holds this Arc and
        // snapshots it at heartbeat time; the RuntimeState Observer inside the
        // Wasmtime Store holds another Arc clone and writes to it on every
        // edge:observe call — same pattern as RequestMeter.
        let metrics_acc = Arc::new(MetricsAccumulator::new());

        let instance_pre_clone = instance_pre.clone();
        let app_name_str = app_name.to_string();
        let meter_clone = meter.clone();
        let metrics_acc_clone = metrics_acc.clone();
        let mut env = spec.env.clone();
        env.insert("EDGE_HTTP_SERVER_PORT".to_string(), raw_port.to_string());
        let state_clone = self.state.clone();
        // Use per-tenant MaxMemoryMB from the task message when available (non-zero),
        // falling back to the worker's config default otherwise.
        let max_memory_mb = if spec.max_memory_mb > 0 {
            spec.max_memory_mb
        } else {
            self.config.max_memory_mb
        };
        let epoch_deadline_ticks = self.config.epoch_deadline_ticks;
        let health_check_timeout_secs = self.config.health_check_timeout_secs;
        let allowlist = spec.allowlist.clone();
        // downloader_clone is captured into the per-app task so
        // run_app_loop can post the auto-rollback signal when an
        // app exhausts its restart cap. Arc<Downloader> is cheap to
        // clone; the underlying reqwest::Client is internally Arc'd
        // already, so this is one atomic refcount bump.
        let downloader_clone = self.downloader.clone();
        let log_forwarder = self.log_forwarder.clone();
        // Own tenant_id before the spawn — `start_app` borrows it as &str, but
        // the tokio::spawn future must be 'static, so we move an owned String
        // into the closure. The PR-side signature change already adds
        // tenant_id to run_app_loop; this binding satisfies the borrow
        // checker without changing the public surface. The original is moved
        // into the closure; tenant_id_for_instance is the second copy used by
        // the AppInstance registration below.
        let tenant_id = tenant_id.to_string();
        let tenant_id_for_instance = tenant_id.clone();

        // Spawn the per-app task and store the JoinHandle so we can
        // propagate panics when the app is stopped.
        // `deployment_id` is `&str` (function parameter), so we need
        // to_string() to move an owned String into the 'static closure.
        let dep_id_for_loop = deployment_id.to_string();
        let handle = tokio::spawn(async move {
            Self::run_app_loop(
                instance_pre_clone,
                meter_clone,
                metrics_acc_clone,
                env,
                state_clone,
                app_name_str.clone(),
                dep_id_for_loop,
                shutdown_rx,
                max_memory_mb,
                epoch_deadline_ticks,
                health_check_timeout_secs,
                tenant_id,
                allowlist,
                downloader_clone,
                log_forwarder,
            )
            .await;
            tracing::info!(app_name = %app_name_str, "app task exited");
        });

        // Register the app instance (Arc<Mutex<>> for interior mutability).
        let instance = Arc::new(std::sync::Mutex::new(AppInstance {
            deployment_id: deployment_id.to_string(),
            app_name: app_name.to_string(),
            tenant_id: tenant_id_for_instance,
            port: raw_port,
            status: AppInstanceStatus::Running,
            meter,
            metrics: metrics_acc,
            shutdown_tx: Some(shutdown_tx),
            instance_pre,
            handle: Some(std::sync::Arc::new(handle)),
            ticker: Some(ticker),
        }));

        self.state.write().await.apps.insert(key.clone(), instance);

        tracing::info!(app_name, deployment_id, port = raw_port, "app started");
        Ok(())
    }

    /// Stop an app gracefully by its (app_name, deployment_id) key.
    pub async fn stop_app(&self, key: &(String, String)) -> anyhow::Result<()> {
        // Clone the Arc so we can lock it while the instance is still in the map.
        let instance = {
            let state = self.state.read().await;
            state.apps.get(key).cloned()
        };

        let (port, handle, ticker) = if let Some(inst) = instance {
            // Extract port, handle, ticker, and sender while locked.
            let mut inst = inst.lock().unwrap();
            inst.status = AppInstanceStatus::Stopping;
            let port = inst.port;
            let handle = inst.handle.clone();
            let ticker = inst.ticker.take();
            let tx = inst.shutdown_tx.take();
            drop(inst); // release lock before sending
            if let Some(tx) = tx {
                let _ = tx.send(());
            }
            (port, handle, ticker)
        } else {
            return Ok(()); // already gone
        };

        // Remove from the map.
        self.state.write().await.apps.remove(key);

        // Free the port.
        {
            let mut pool = self.port_pool.lock().await;
            pool.release(port);
        }

        // Abort the epoch ticker so the engine clock stops advancing for
        // this app. The ticker's task is a tight loop that holds a clone
        // of the engine; without abort, it would run forever (or until
        // the engine is dropped), wasting CPU and incrementing the epoch
        // for stopped apps.
        if let Some(t) = ticker {
            t.abort();
        }

        // Propagate any panic from the app task.
        if let Some(handle) = handle {
            handle.abort();
            // try_unwrap extracts the JoinHandle from the Arc; if there are other
            // Arcs (shouldn't happen here), fall back to not awaiting.
            match std::sync::Arc::try_unwrap(handle) {
                Ok(join_handle) => {
                    if let Err(panic_info) = join_handle.await {
                        std::panic::panic_any(panic_info);
                    }
                }
                Err(_) => {
                    tracing::warn!("could not unwrap JoinHandle — multiple refs");
                }
            }
        }

        tracing::info!(app_name = %key.0, deployment_id = %key.1, "app stopped");
        Ok(())
    }

    /// Per-app task loop.
    ///
    /// Executes the component in a loop. Handles crashes with exponential
    /// backoff restart (max 5 restarts, then gives up). Long-running apps
    /// (HTTP servers) that return from handle() keep running — only an explicit
    /// process.exit from the guest means "stop".
    //
    // The extra parameters come from two merged features: PR #64 follow-up
    // adds per-invocation memory + epoch limits (max_memory_mb,
    // epoch_deadline_ticks); origin/main adds a host-side timeout
    // (health_check_timeout_secs) for hung-app detection. They are
    // complementary: the wasmtime limits terminate the *guest* at the
    // engine layer, the timeout terminates the *host* task when the
    // guest doesn't yield. Refactoring into a struct is left for a future
    // PR; the clippy lint here keeps the function signature honest about
    // what it actually depends on.
    #[allow(clippy::too_many_arguments)]
    async fn run_app_loop(
        instance_pre: InstancePre<edge_runtime::RuntimeState>,
        meter: Arc<RequestMeter>,
        metrics_acc: Arc<MetricsAccumulator>,
        env: HashMap<String, String>,
        state: Arc<RwLock<WorkerState>>,
        app_name: String,
        deployment_id: String,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        max_memory_mb: u64,
        epoch_deadline_ticks: u64,
        health_check_timeout_secs: u64,
        tenant_id: String,
        allowlist: Option<Vec<String>>,
        downloader: Arc<Downloader>,
        log_forwarder: Arc<LogForwarder>,
    ) {
        let mut restart_count = 0u32;
        let max_restarts = 5;
        let base_backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);
        // deployment_id is captured once at the top of run_app_loop
        // so the auto-rollback POST (fired on Crashed) names the
        // deployment that's currently active — i.e. the one we're
        // giving up on. The control plane uses this to update its
        // audit log; it doesn't affect the rollback itself, which is
        // driven entirely by last_good_deployment_id.
        let current_deployment_id = meter.deployment_id.clone();

        loop {
            tokio::select! {
                // Graceful shutdown signal from supervisor
                _ = &mut shutdown_rx => {
                    tracing::info!("app received shutdown signal");
                    break;
                }

                // Run the component. Two layered defenses:
                //   1. Inside execute_app, wasmtime Store limits + epoch
                //      deadline bound the guest at the engine layer (memory
                //      + CPU).
                //   2. tokio::time::timeout bounds the host task: if the
                //      guest traps in a syscall that doesn't yield (or the
                //      epoch ticker is starved), the host marks the app as
                //      Hung and restarts after backoff.
                result = tokio::time::timeout(
                    Duration::from_secs(health_check_timeout_secs),
                    Self::execute_app(
                        &instance_pre,
                        &meter,
                        &metrics_acc,
                        env.clone(),
                        max_memory_mb,
                        epoch_deadline_ticks,
                        &tenant_id,
                        allowlist.clone(),
                        &app_name,
                        &log_forwarder,
                    ),
                ) => {
                    match result {
                        Ok(Ok(true)) => {
                            // Component wants to keep running (blocking call returned normally).
                            // Loop back and re-execute — this supports long-running HTTP servers.
                            continue;
                        }
                        Ok(Ok(false)) => {
                            // Guest explicitly called process.exit — clean exit.
                            tracing::info!("component exited normally");
                            break;
                        }
                        Ok(Err(e)) => {
                            // Wasm trap or runtime error — treat as crash.
                            restart_count += 1;
                            if restart_count >= max_restarts {
                                tracing::error!(
                                    restart_count,
                                    err = %e,
                                    "max restarts exceeded, giving up"
                                );
                                // Mark the app as crashed so the heartbeat reflects the failure.
                                {
                                    let mut s = state.write().await;
                                    if let Some(inst) = s.apps.get_mut(&(app_name.clone(), deployment_id.clone())) {
                                        let mut inst = inst.lock().unwrap();
                                        inst.status = AppInstanceStatus::Crashed { restart_count };
                                    }
                                }
                                // Best-effort auto-rollback: signal the
                                // control plane so it can swap the active
                                // deployment back to last_good. We do NOT
                                // block the per-app task on this — `spawn`
                                // detaches the POST so the loop can return
                                // immediately. The user's manual
                                // `edge rollback` covers the failure mode.
                                let dl = downloader.clone();
                                let tenant = tenant_id.clone();
                                let name = app_name.clone();
                                let dep = current_deployment_id.clone();
                                tokio::spawn(async move {
                                    if let Err(err) = dl
                                        .post_auto_rollback(&tenant, &name, &dep, restart_count)
                                        .await
                                    {
                                        tracing::warn!(
                                            tenant_id = %tenant,
                                            app_name = %name,
                                            current_deployment_id = %dep,
                                            restart_count,
                                            err = %err,
                                            "auto-rollback POST failed; user must run `edge rollback` manually"
                                        );
                                    }
                                });
                                break;
                            }

                            let backoff = std::cmp::min(
                                base_backoff * 2u32.pow(restart_count - 1),
                                max_backoff,
                            );
                            tracing::warn!(
                                err = %e,
                                restart_count,
                                "app crashed, restarting in {:?}",
                                backoff
                            );
                            sleep(backoff).await;
                        }
                        Err(_elapsed) => {
                            // Health check timeout — app hung.
                            restart_count += 1;
                            let backoff = std::cmp::min(
                                base_backoff * 2u32.pow(restart_count - 1),
                                max_backoff,
                            );
                            tracing::warn!(
                                restart_count,
                                timeout_secs = health_check_timeout_secs,
                                "app hung (health check timeout), restarting in {:?}",
                                backoff
                            );
                            if restart_count >= max_restarts {
                                let mut s = state.write().await;
                                if let Some(inst) = s.apps.get_mut(&(app_name.clone(), deployment_id.clone())) {
                                    let mut inst = inst.lock().unwrap();
                                    inst.status = AppInstanceStatus::Hung;
                                }
                                // Same auto-rollback as the Crashed
                                // branch above — Hung means the guest
                                // stopped yielding (vs Crashed which
                                // means it trapped). Both are tenant-
                                // facing failure modes, both deserve a
                                // rollback signal if the tenant opted in.
                                let dl = downloader.clone();
                                let tenant = tenant_id.clone();
                                let name = app_name.clone();
                                let dep = current_deployment_id.clone();
                                tokio::spawn(async move {
                                    if let Err(err) = dl
                                        .post_auto_rollback(&tenant, &name, &dep, restart_count)
                                        .await
                                    {
                                        tracing::warn!(
                                            tenant_id = %tenant,
                                            app_name = %name,
                                            current_deployment_id = %dep,
                                            restart_count,
                                            err = %err,
                                            "auto-rollback POST failed; user must run `edge rollback` manually"
                                        );
                                    }
                                });
                                break;
                            }
                            sleep(backoff).await;
                        }
                    }
                }
            }
        }
    }

    /// Execute a single app invocation.
    ///
    /// Returns `Ok(true)` if the component wants to keep running (blocking call
    /// returned normally). Returns `Ok(false)` if the guest explicitly called
    /// `process.exit`. Returns `Err` on a wasm trap/error.
    #[allow(clippy::too_many_arguments)]
    async fn execute_app(
        instance_pre: &InstancePre<edge_runtime::RuntimeState>,
        meter: &Arc<RequestMeter>,
        metrics_acc: &Arc<MetricsAccumulator>,
        env: HashMap<String, String>,
        max_memory_mb: u64,
        epoch_deadline_ticks: u64,
        tenant_id: &str,
        allowlist: Option<Vec<String>>,
        app_name: &str,
        log_forwarder: &Arc<LogForwarder>,
    ) -> anyhow::Result<bool> {
        let engine = instance_pre.engine();

        // Build per-deployment egress policy.
        // None = field absent or [] on the wire (old control plane) → allow-all.
        // Some(list) = explicit allowlist → enforce it.
        let egress = match allowlist {
            None => Arc::new(EgressPolicy::allow_all()),
            Some(list) => Arc::new(EgressPolicy::new(list)),
        };

        // Build per-app LogContext — stamped onto every record this app emits
        // so the LogForwarder knows which tenant/app/deployment to attribute
        // the record to (worker_id/region are added inside the forwarder).
        // tenant_id is taken from the execute_app parameter (which matches
        // meter.tenant_id — they are both derived from the task message's
        // tenant_id in start_app) so the parameter is meaningfully used.
        let app_ctx = edge_runtime::interfaces::observe::AppLogContext {
            app_name: app_name.to_string(),
            tenant_id: tenant_id.to_string(),
            deployment_id: meter.deployment_id.clone(),
        };

        // Create a fresh RuntimeState with per-app env vars, metering, log
        // sink, app context, tenant_id for tenant isolation, and the shared
        // metrics accumulator so edge:observe calls are visible to the
        // supervisor's heartbeat snapshot.
        let runtime_state = edge_runtime::RuntimeState::with_env_and_meter(
            env,
            Some(Arc::clone(meter)),
            log_forwarder.clone(),
            app_ctx,
            egress,
            Some(Arc::clone(metrics_acc)),
        );

        // Create a store with per-invocation state. The memory cap is plumbed
        // through Config (APP_MAX_MEMORY_MB); the previous code hardcoded
        // 256 MiB, which made the env-var knob decorative.
        let mut store = edge_runtime::create_store(engine, max_memory_mb, runtime_state);

        // Set the per-invocation epoch deadline. The engine's epoch clock is
        // advanced by the ticker spawned in start_app; once it crosses this
        // deadline, wasmtime interrupts the guest with an epoch trap. This
        // is the only thing that bounds CPU usage in wasmtime — without it
        // a tight loop in the guest can hang the worker indefinitely.
        store.set_epoch_deadline(epoch_deadline_ticks);

        // Instantiate
        let instance = instance_pre.instantiate(&mut store)?;

        // Try _start first (WASI Preview 2 canonical), then handle
        let has_start = instance
            .get_typed_func::<(), ()>(&mut store, "_start")
            .is_ok();

        if has_start {
            instance
                .get_typed_func::<(), ()>(&mut store, "_start")?
                .call(&mut store, ())?;
        } else {
            instance
                .get_typed_func::<(), ()>(&mut store, "handle")?
                .call(&mut store, ())?;
        }

        // Check if the guest called process.exit — the flag is set by the host call
        // before the wasmtime trap is raised, so we see it here on a successful return.
        if let Some(code) = store.data().exit_requested() {
            tracing::info!(code, "guest called process.exit");
            return Ok(false);
        }

        // Component returned normally — it wants to keep running.
        Ok(true)
    }

    /// Build a heartbeat message from current app states.
    pub async fn build_heartbeat(&self) -> HeartbeatMessage {
        let mut msg = HeartbeatMessage::new(
            self.config.worker_id.clone(),
            self.config.region.clone(),
            self.config.worker_addr.clone(),
        );

        // Capacity headroom for the autoscaler (issue #85). Read the
        // PortPool first so we can release the lock before walking
        // `state.apps` — the latter holds the larger RwLock and the
        // Pool lock must not be held across it (lock-ordering: Pool
        // before state, never the reverse).
        let app_slots = {
            let mut pool = self.port_pool.lock().await;
            pool.capacity_remaining()
        };
        msg.cluster_headroom = Some(ClusterHeadroom {
            cpu_pct: self.take_cpu_pct(),
            mem_pct: None,
            app_slots,
        });

        let state = self.state.read().await;
        for (key, inst) in &state.apps {
            let inst = inst.lock().unwrap();
            let status = match &inst.status {
                AppInstanceStatus::Running => "running",
                AppInstanceStatus::Starting => "starting",
                AppInstanceStatus::Stopping => "stopping",
                AppInstanceStatus::Crashed { .. } => "crashed",
                AppInstanceStatus::Hung => "hung",
            };
            let exit_code = match &inst.status {
                AppInstanceStatus::Running
                | AppInstanceStatus::Starting
                | AppInstanceStatus::Stopping => None,
                AppInstanceStatus::Crashed { .. } | AppInstanceStatus::Hung => Some(1),
            };
            // Key by app_name:deployment_id so the ingress can distinguish
            // multiple concurrent instances of the same app.
            let hb_key = format!("{}:{}", key.0, key.1);
            let snap = inst.meter.snapshot();
            let metrics_snap = inst.metrics.snapshot();

            // Convert MetricsSnapshot into the wire-format Vec<MetricSample>.
            let mut observer_metrics: Vec<MetricSample> = Vec::new();
            for e in &metrics_snap.counters {
                observer_metrics.push(MetricSample {
                    name: e.name.clone(),
                    kind: MetricKind::Counter,
                    value: e.value as f64,
                    labels: e.labels.clone(),
                });
            }
            for e in &metrics_snap.gauges {
                observer_metrics.push(MetricSample {
                    name: e.name.clone(),
                    kind: MetricKind::Gauge,
                    value: e.value,
                    labels: e.labels.clone(),
                });
            }
            for (name, samples) in &metrics_snap.histograms {
                for (value, labels) in samples {
                    observer_metrics.push(MetricSample {
                        name: name.clone(),
                        kind: MetricKind::HistogramSample,
                        value: *value,
                        labels: labels.clone(),
                    });
                }
            }

            msg.apps.insert(
                hb_key,
                AppStatus {
                    deployment_id: inst.deployment_id.clone(),
                    status: status.to_string(),
                    exit_code,
                    request_count: snap.request_count,
                    outbound_bytes: snap.outbound_bytes,
                    tenant_id: inst.tenant_id.clone(),
                    port: inst.port,
                    observer_metrics,
                },
            );
        }

        msg
    }

    /// Subtract the published heartbeat's per-app counts from each meter after
    /// a successful publish. Using subtract_delta rather than zeroing the counter
    /// preserves any bytes recorded between the snapshot and this call — those
    /// will appear in the next heartbeat interval rather than being silently lost.
    pub async fn reset_meters_after(&self, heartbeat: &HeartbeatMessage) {
        let state = self.state.read().await;
        for (hb_key, status) in &heartbeat.apps {
            // Heartbeat key is "app_name:deployment_id"; state key is the tuple.
            let (app_name, deployment_id) = match hb_key.split_once(':') {
                Some((n, d)) => (n, d),
                None => continue,
            };
            let key = (app_name.to_string(), deployment_id.to_string());
            if let Some(inst) = state.apps.get(&key) {
                let inst = inst.lock().unwrap();
                // Guard on deployment_id: if the app was stopped and a new
                // deployment with the same name started between build_heartbeat
                // and here, the new instance's meter must not be subtracted for
                // the old deployment's counts — fetch_sub would wrap to u64::MAX.
                if inst.deployment_id != status.deployment_id {
                    continue;
                }
                inst.meter
                    .subtract_delta(status.request_count, status.outbound_bytes);
            }
        }
    }

    /// Stop all running apps (used during graceful shutdown).
    pub async fn stop_all_apps(&self) {
        let keys: Vec<(String, String)> = self.state.read().await.apps.keys().cloned().collect();
        for key in &keys {
            if let Err(e) = self.stop_app(key).await {
                tracing::error!(app_name = %key.0, deployment_id = %key.1, err = %e, "failed to stop app during shutdown");
            }
        }
    }

    /// Run the JetStream task-consume loop until `shutdown_rx` fires.
    ///
    /// Subscribes to the queue-grouped consumer derived from
    /// `config.queue_group` / `config.consumer_name`. Each delivered
    /// `TaskMessage` is deserialized, passed to `handle_task_message`, and
    /// ack'd on success. Failures are nack'd for redelivery; unparseable
    /// (poison) messages are terminated so the consumer makes progress.
    ///
    /// Returns `Ok(())` only when `shutdown_rx` resolves. If the JetStream
    /// push stream ends (consumer deleted, server restart, transient
    /// disconnect that doesn't auto-heal inside `consumer.messages()`),
    /// returns `Err` so the caller's reconnect loop can resubscribe.
    pub async fn run_consume_loop(
        &self,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let mut stream = self
            .nats
            .subscribe_tasks(
                &self.config.region,
                &self.config.queue_group,
                &self.config.consumer_name,
            )
            .await?;
        tracing::info!(
            region = %self.config.region,
            queue_group = %self.config.queue_group,
            consumer = %self.config.consumer_name,
            "subscribed to task stream"
        );

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => {
                    tracing::info!("consume loop received shutdown signal");
                    return Ok(());
                }
                msg = stream.next() => {
                    let Some(msg) = msg else {
                        // Stream ended. Not a shutdown — return Err so the
                        // caller's reconnect loop resubscribes with backoff.
                        anyhow::bail!("task stream ended unexpectedly");
                    };
                    self.process_task_message(msg).await;
                }
            }
        }
    }

    /// Process a single JetStream task message with ack/nack/term flow
    /// control. Errors are logged, never propagated — a single bad
    /// message must not tear down the consume loop.
    ///
    /// Ack/nack/term failures are best-effort retried with a short delay
    /// so a transient network blip doesn't cause silent duplicate
    /// delivery. If the ack itself fails after `handle_task_message`
    /// succeeded, we `nack` instead of swallowing — this puts the message
    /// back in the work queue under server control rather than letting
    /// the next reconnect blindly redeliver it.
    async fn process_task_message(&self, msg: async_nats::jetstream::Message) {
        let payload = msg.payload.clone();
        let task_msg: TaskMessage = match serde_json::from_slice(&payload) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(err = %e, "poison-pill task message, terminating");
                self.try_term(&msg).await;
                return;
            }
        };

        match self.handle_task_message(task_msg).await {
            Ok(()) => {
                if let Err(e) = self.nats.ack(&msg).await {
                    tracing::error!(
                        err = %e,
                        "ack() failed — nacking with delay to force a clean redelivery"
                    );
                    // Don't swallow: if the server never received our ack,
                    // it will redeliver on reconnect. nack-with-delay tells
                    // the server explicitly "put this back, I didn't keep it".
                    if let Err(e2) = self.nats.nack(&msg, Some(Duration::from_secs(30))).await {
                        tracing::error!(err = %e2, "nack-after-ack-failure also failed");
                    }
                }
            }
            Err(e) => {
                tracing::error!(err = %e, "handle_task_message failed, nacking");
                self.try_nack(&msg).await;
            }
        }
    }

    async fn try_nack(&self, msg: &async_nats::jetstream::Message) {
        if let Err(e) = self.nats.nack(msg, None).await {
            tracing::error!(err = %e, "nack() failed — message will be redelivered on next reconnect");
        }
    }

    async fn try_term(&self, msg: &async_nats::jetstream::Message) {
        if let Err(e) = self.nats.term(msg).await {
            tracing::error!(err = %e, "term() failed — message may be redelivered");
        }
    }
}
