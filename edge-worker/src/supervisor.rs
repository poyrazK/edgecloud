//! Core supervisor logic — app lifecycle management.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use edge_runtime::linker::create_component_linker;
use edge_runtime::{EgressPolicy, RequestMeter};
use futures::StreamExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};
use wasmtime::component::InstancePre;

use crate::config::Config;
use crate::downloader::Downloader;
use crate::messages::{AppSpec, AppStatus, HeartbeatMessage, TaskMessage};
use crate::nats::NatsClient;
use crate::port_pool::PortPool;
use crate::state::{AppInstance, AppInstanceStatus, WorkerState};

/// The main supervisor — manages all running apps for this worker node.
pub struct Supervisor {
    pub config: Config,
    pub state: Arc<RwLock<WorkerState>>,
    pub downloader: Arc<Downloader>,
    pub port_pool: Arc<Mutex<PortPool>>,
    pub nats: Arc<dyn NatsClient>,
}

impl Supervisor {
    /// Handle an incoming TaskMessage from NATS.
    ///
    /// Diffs the desired app set against currently running apps and
    /// starts/stops apps accordingly.
    pub async fn handle_task_message(&self, msg: TaskMessage) -> anyhow::Result<()> {
        let TaskMessage::TaskUpdate {
            tenant_id,
            apps: desired_apps,
            ..
        } = msg;

        let current_apps: HashMap<String, (String, AppInstanceStatus)> = {
            let state = self.state.read().await;
            let mut map = HashMap::new();
            for (name, inst) in state.apps.iter() {
                let inst = inst.lock().await;
                map.insert(
                    name.clone(),
                    (inst.deployment_id.clone(), inst.status.clone()),
                );
            }
            map
        };

        // Stop apps no longer in the desired set
        for app_name in current_apps.keys() {
            if !desired_apps.contains_key(app_name) {
                if let Err(e) = self.stop_app(app_name).await {
                    tracing::error!(app_name, err = %e, "failed to stop app");
                }
            }
        }

        // Start or update apps in the desired set
        for (app_name, spec) in &desired_apps {
            let is_new = !current_apps.contains_key(app_name);
            let is_changed = current_apps
                .get(app_name)
                .map(|(dep_id, _)| dep_id != &spec.deployment_id)
                .unwrap_or(false);

            if is_new || is_changed {
                if let Err(e) = self.start_app(app_name, spec, &tenant_id).await {
                    tracing::error!(app_name, err = %e, "failed to start app");
                }
            }
        }

        Ok(())
    }

    /// Start a new app or restart a changed one.
    async fn start_app(
        &self,
        app_name: &str,
        spec: &AppSpec,
        tenant_id: &str,
    ) -> anyhow::Result<()> {
        // Validate tenant_id before any filesystem or store operations.
        // Reject path-traversal characters that could escape the base persistence directory.
        if !edge_runtime::is_safe_tenant_id(tenant_id) {
            anyhow::bail!("refusing to start app: unsafe tenant_id {:?}", tenant_id);
        }

        tracing::info!(app_name, deployment_id = spec.deployment_id, "starting app");

        // Stop existing instance if present
        if self.state.read().await.apps.contains_key(app_name) {
            self.stop_app(app_name).await?;
        }

        // Acquire a port.
        let raw_port = {
            let mut pool = self.port_pool.lock().await;
            pool.acquire().expect("port pool exhausted")
        };

        // Download artifact (blocking on first request).
        // Note: Downloader::get_artifact verifies SHA-256 against
        // spec.deployment_hash before returning; on mismatch/empty/malformed it
        // returns Err, which this arm propagates and the port-release path handles.
        let artifact = match self
            .downloader
            .get_artifact(&spec.deployment_id, &spec.deployment_hash)
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
            spec.deployment_id.clone(),
        ));

        let instance_pre_clone = instance_pre.clone();
        let app_name_str = app_name.to_string();
        let tenant_id_str = tenant_id.to_string();
        let meter_clone = meter.clone();
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

        // Spawn the per-app task and store the JoinHandle so we can
        // propagate panics when the app is stopped.
        let handle = tokio::spawn(async move {
            Self::run_app_loop(
                instance_pre_clone,
                meter_clone,
                env,
                state_clone,
                app_name_str.clone(),
                shutdown_rx,
                max_memory_mb,
                epoch_deadline_ticks,
                health_check_timeout_secs,
                tenant_id_str,
                allowlist,
            )
            .await;
            tracing::info!(app_name = %app_name_str, "app task exited");
        });

        // Register the app instance (Arc<Mutex<>> for interior mutability).
        let instance = Arc::new(Mutex::new(AppInstance {
            deployment_id: spec.deployment_id.clone(),
            app_name: app_name.to_string(),
            tenant_id: tenant_id.to_string(),
            port: raw_port,
            status: AppInstanceStatus::Running,
            meter,
            shutdown_tx: Some(shutdown_tx),
            instance_pre,
            handle: Some(std::sync::Arc::new(handle)),
            ticker: Some(ticker),
        }));

        self.state
            .write()
            .await
            .apps
            .insert(app_name.to_string(), instance);

        tracing::info!(app_name, port = raw_port, "app started");
        Ok(())
    }

    /// Stop an app gracefully.
    pub async fn stop_app(&self, app_name: &str) -> anyhow::Result<()> {
        // Clone the Arc so we can lock it while the instance is still in the map.
        let instance = {
            let state = self.state.read().await;
            state.apps.get(app_name).cloned()
        };

        let (port, handle, ticker) = if let Some(inst) = instance {
            // Extract port, handle, ticker, and sender while locked.
            let mut inst = inst.lock().await;
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
        self.state.write().await.apps.remove(app_name);

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

        tracing::info!(app_name, "app stopped");
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
        env: HashMap<String, String>,
        state: Arc<RwLock<WorkerState>>,
        app_name: String,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        max_memory_mb: u64,
        epoch_deadline_ticks: u64,
        health_check_timeout_secs: u64,
        tenant_id: String,
        allowlist: Option<Vec<String>>,
    ) {
        let mut restart_count = 0u32;
        let max_restarts = 5;
        let base_backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);

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
                        env.clone(),
                        max_memory_mb,
                        epoch_deadline_ticks,
                        &tenant_id,
                        allowlist.clone(),
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
                                    if let Some(inst) = s.apps.get_mut(&app_name) {
                                        let mut inst = inst.lock().await;
                                        inst.status = AppInstanceStatus::Crashed { restart_count };
                                    }
                                }
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
                                if let Some(inst) = s.apps.get_mut(&app_name) {
                                    let mut inst = inst.lock().await;
                                    inst.status = AppInstanceStatus::Hung;
                                }
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
    async fn execute_app(
        instance_pre: &InstancePre<edge_runtime::RuntimeState>,
        meter: &Arc<RequestMeter>,
        env: HashMap<String, String>,
        max_memory_mb: u64,
        epoch_deadline_ticks: u64,
        tenant_id: &str,
        allowlist: Option<Vec<String>>,
    ) -> anyhow::Result<bool> {
        let engine = instance_pre.engine();

        // Build per-deployment egress policy.
        // None = field absent or [] on the wire (old control plane) → allow-all.
        // Some(list) = explicit allowlist → enforce it.
        let egress = match allowlist {
            None => Arc::new(EgressPolicy::allow_all()),
            Some(list) => Arc::new(EgressPolicy::new(list)),
        };

        // Create a fresh RuntimeState with per-app env vars, metering, and tenant-scoped
        // persistent stores (KV, cache, scheduling) so data never leaks across tenants.
        let runtime_state = edge_runtime::RuntimeState::with_env_and_meter(
            env,
            Some(Arc::clone(meter)),
            tenant_id.to_string(),
            egress,
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

        let state = self.state.read().await;
        for (app_name, inst) in &state.apps {
            let inst = inst.lock().await;
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
            msg.apps.insert(
                app_name.clone(),
                AppStatus {
                    deployment_id: inst.deployment_id.clone(),
                    status: status.to_string(),
                    exit_code,
                    request_count: inst.meter.snapshot().request_count,
                    tenant_id: inst.tenant_id.clone(),
                    port: inst.port,
                },
            );
        }

        msg
    }

    /// Stop all running apps (used during graceful shutdown).
    pub async fn stop_all_apps(&self) {
        let app_names: Vec<String> = self.state.read().await.apps.keys().cloned().collect();
        for app_name in &app_names {
            if let Err(e) = self.stop_app(app_name).await {
                tracing::error!(app_name, err = %e, "failed to stop app during shutdown");
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
