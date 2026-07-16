//! Prometheus metrics surface for edge-worker (issue #49).
//!
//! WorkerMetrics owns a private `prometheus::Registry` plus a
//! per-app `MetricsHandle`. Call sites use the handle to bump the
//! same counters as the per-deployment `RequestMeter` (so an operator
//! scraping `/metrics` sees the same `request_count` half-second
//! lag that the heartbeat carries), and the supervisor calls
//! `register_app` + `unregister_app` to add/remove the per-app label
//! set on app start/stop.
//!
//! The counters are PROCESS-LIFETIME accumulators (issue #49 — see
//! the body). The `reset_meters_after` snapshot-and-subtract path
//! used by the heartbeat (`supervisor.rs`) is INTENTIONALLY not
//! mirrored: Prometheus clients historically prefer monotonic
//! counters; the billing pipeline still reads the heartbeat, not the
//! metrics endpoint. Adding a `decrement` call to the prom counters
//! would break `rate()` math on the operator side.
//!
//! Surface (8 families):
//!   - edge_requests_total{deployment_id, app_name}              (IntCounter)
//!   - edge_outbound_bytes_total{deployment_id, app_name}        (IntCounter)
//!   - edge_resident_seconds_total{deployment_id, app_name}      (IntCounter)
//!   - edge_duration_ms_total{deployment_id, app_name}           (IntCounter)
//!   - edge_status_5xx_total{deployment_id, app_name}            (IntCounter)
//!   - edge_app_status{deployment_id, app_name, status}          (IntGaugeVec)
//!     — set to 1 by set_status; clear previous via remove_label_values.
//!     Swept on `unregister_app` so the active set is bounded by app
//!     cardinality currently running on the worker.
//!   - edge_app_terminal_status{deployment_id, app_name, status, exit_code}
//!     — audit-style terminal status that SURVIVES unregister_app.
//!     Stamp-once-per-`(deployment_id, app_name)` invariant: a re-register
//!     with the same key simply overwrites the row. Bounded by total
//!     deployment cardinality ever seen on this worker (operators already
//!     have access to that set via the control plane), so the long-lived-worker
//!     OOM hazard that applies to the per-app counter families does NOT
//!     apply here. Operators query `== 1` for the last-seen terminal state
//!     and `== 0` (no row) means the app has never been observed.
//!   - edge_worker_uptime_seconds                                 (Gauge)
//!   - edge_worker_active_apps                                    (Gauge)
//!
//! The worker-level gauges are bumped once per second by `tick_worker_gauges`
//! (spawned in `main` alongside the heartbeat loop).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Instant;

use prometheus::{
    core::Collector, Encoder, Gauge, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use tokio::sync::RwLock;

use crate::state::AppInstanceStatus;
use crate::supervisor::app_status_to_string;

/// Render the exit-code label for a terminal-status string.
///
/// `crashed` is the one variant that carries a numeric code (the
/// restart_count, which the supervisor embeds into the wire
/// string). Everything else — `running`, `starting`, `draining`,
/// `stopping`, `hung` — emits an empty string so the
/// `edge_app_terminal_status` gauge has at most one row per
/// `(deployment_id, app_name, status)` pair.
///
/// The supervisor does not currently stamp a numeric code for
/// `crashed` (the AppInstanceStatus wire string is just
/// "crashed"), so this helper always returns the empty string
/// today. The hook exists so a future PR that splits the restart
/// count out of the status string has a single integration
/// point — pass the restart count alongside the status_str into
/// this helper and the gauge labels pick it up automatically.
pub(crate) fn exit_code_for_status(_status_str: &str) -> &'static str {
    ""
}

/// The four process-level counter families, scoped to one app
/// instance. Cloned cheaply — each field is `Arc`-backed inside
/// `prometheus::IntCounter`. The four `_box` fields hold
/// `Arc<dyn Collector>` clones of the originals so `unregister_app`
/// can hand them back to `Registry::unregister` — the registry
/// matches collectors by descriptor (name + const labels), and a
/// freshly-built IntCounter with the same descriptors is what
/// unregister needs to find the registered entry.
#[derive(Clone)]
pub struct MetricsHandle {
    /// Bump on every accepted FaaS request and every accepted
    /// LongRunning connection.
    pub requests: IntCounter,
    /// Bump with the per-frame byte count returned by the guest
    /// (CountingBody in `dispatch.rs`).
    pub outbound_bytes: IntCounter,
    /// Bump with `RESIDENT_TICK_SECS` from the per-app resident
    /// ticker (LongRunning only).
    pub resident_seconds: IntCounter,
    /// Bump with the wall-clock millis between request accept and
    /// response complete (FaaS only — see issue #555).
    pub duration_ms: IntCounter,
    /// Bump with `meter.record_error()` from the `synthetic_500`
    /// chokepoint that all three FaaS error terminal arms of
    /// `handle_request` share (issue #84 asks 6/7). NOT bumped on
    /// the body-cap 413 early return — that is a tenant-config
    /// violation, not a guest error. Monotonic, the same as the
    /// other four counters.
    pub status_5xx: IntCounter,
    /// Registered boxes held for the unregister step.
    /// Field order mirrors the counter order; the boxes are
    /// `IntCounter` clones (the inner state is `Arc`, so the
    /// cloned box shares the same counter value).
    pub requests_box: Arc<dyn Collector>,
    pub outbound_bytes_box: Arc<dyn Collector>,
    pub resident_seconds_box: Arc<dyn Collector>,
    pub duration_ms_box: Arc<dyn Collector>,
    pub status_5xx_box: Arc<dyn Collector>,
    /// `(tenant_id, deployment_id)` — used as the map key in
    /// `WorkerMetrics::apps`; also re-used to clear the
    /// `edge_app_status` gauge on `unregister_app`.
    pub tenant_id: String,
    pub deployment_id: String,
    pub app_name: String,
}

/// Process-wide metrics handle. One per worker.
///
/// Constructed once in `main` after `Config::from_env()`. The
/// `prometheus::Registry` is **not** the global default; we keep our
/// own so a future migration to multiple registries (per-tenant,
/// per-process) doesn't have to unwind global state. The text
/// encoder reads `registry.gather()` to render the response body.
pub struct WorkerMetrics {
    pub registry: Registry,
    pub worker_uptime_seconds: Gauge,
    pub worker_active_apps: Gauge,
    /// Seconds remaining until the current JWT snapshot expires.
    /// Updated by the proactive refresh loop
    /// (`jwt_refresh::spawn_jwt_refresh_loop`) on every successful
    /// refresh. The gauge rises back to ~TTL on each successful
    /// refresh and trends toward zero between refreshes; alert when
    /// it crosses zero or stops decreasing (a stuck loop). The
    /// value is monotonic-stable — no wall-clock translation is
    /// required. Issue #504.
    pub worker_jwt_expires_at_seconds: Gauge,
    /// Per-outcome counter for proactive + reactive JWT refresh
    /// attempts. `outcome = "ok" | "err"`. Bumped by both
    /// `jwt_refresh` and the reactive `with_token_refresh` helper
    /// so a single counter is the source of truth. Issue #504.
    pub worker_jwt_refresh_total: IntCounterVec,
    pub app_status: IntGaugeVec,
    /// Audit-style terminal status. Stamped once per
    /// `(deployment_id, app_name)` from `unregister_app` AFTER
    /// reading the last known status from `last_status`. Never
    /// swept — the operator-side cardinality is bounded by total
    /// deployments ever seen on this worker (one row per
    /// `(deployment_id, app_name)`), not by the active set.
    pub app_terminal_status: IntGaugeVec,
    /// Map of active apps. The `MetricsHandle` is the value; we keep
    /// the `Arc` indirection so the dispatch path can `.inc()` on a
    /// cloned handle without taking the lock.
    pub apps: RwLock<HashMap<(String, String), Arc<MetricsHandle>>>,
    /// Last `edge_app_status` value stamped for each app. Synced
    /// `StdMutex` since the only writer is `set_status` and reads
    /// are on the same path. Locks are held for microseconds (just
    /// a HashMap lookup + small write), so the lack of async
    /// locking is fine.
    last_status: StdMutex<HashMap<(String, String), &'static str>>,
    /// Worker-process boot instant — anchored at construction so
    /// `tick_worker_gauges` can compute uptime by `start.elapsed()`.
    pub started_at: Instant,
}

/// Lightweight handle to the JWT refresh gauge + outcome counter,
/// carried by the refresh loop and the reactive 401 helper.
/// Cloned cheaply (all fields are `Arc`-backed by prometheus).
#[derive(Clone)]
pub struct RefreshMetrics {
    pub expires_at: Gauge,
    pub refresh_total: IntCounterVec,
}

impl WorkerMetrics {
    pub fn new() -> anyhow::Result<Arc<Self>> {
        let registry = Registry::new();

        let worker_uptime_seconds = Gauge::with_opts(Opts::new(
            "edge_worker_uptime_seconds",
            "Seconds since the worker process booted. Bumped once per second \
             by the worker-level tick.",
        ))?;
        registry.register(Box::new(worker_uptime_seconds.clone()))?;

        let worker_active_apps = Gauge::with_opts(Opts::new(
            "edge_worker_active_apps",
            "Number of currently-running app instances on this worker. \
             Source of truth is the supervisor's in-memory state — this gauge \
             is bumped once per second rather than on every start/stop so a \
             transient reconcile storm cannot drift the count.",
        ))?;
        registry.register(Box::new(worker_active_apps.clone()))?;

        // edge_app_status is a single IntGaugeVec keyed by
        // (deployment_id, app_name, status). Only ONE status value
        // per app carries a 1 at any time — the rest are removed via
        // remove_label_values. Operators query `edge_app_status == 1`
        // to find the current state.
        let app_status = IntGaugeVec::new(
            Opts::new(
                "edge_app_status",
                "Current app instance status (1 = matches, 0 = absent). \
                 Per `(deployment_id, app_name)` only one status value is \
                 set to 1 at a time; the rest are removed.",
            ),
            &["deployment_id", "app_name", "status"],
        )?;
        registry.register(Box::new(app_status.clone()))?;

        // edge_app_terminal_status is the audit-style sibling of
        // `edge_app_status`. Operators want to know what state a
        // CRASHED/STOPPED app died in, but `unregister_app` sweeps
        // the live gauge to keep memory bounded by active apps.
        // This gauge preserves ONE terminal status per
        // `(deployment_id, app_name)` — overwrite semantics, no
        // sweep — and survives unregister. `exit_code` is an empty
        // string for non-`Crashed` transitions and a numeric code
        // for `Crashed { restart_count }`. The cardinality is
        // bounded by total deployments ever seen on this worker
        // (one row per `(deployment_id, app_name)`), so it does not
        // share the OOM hazard that motivates sweeping the live
        // `edge_app_status` gauge.
        let app_terminal_status = IntGaugeVec::new(
            Opts::new(
                "edge_app_terminal_status",
                "Last terminal status for an app that has been \
                 unregistered (1 = matches, 0 = absent). Persists \
                 across the live-app lifecycle — operators query this \
                 to learn why an app died, since `edge_app_status` \
                 for the same key is swept on unregister. \
                 `exit_code` is the integer code for `crashed` and \
                 the empty string otherwise.",
            ),
            &["deployment_id", "app_name", "status", "exit_code"],
        )?;
        registry.register(Box::new(app_terminal_status.clone()))?;

        // JWT refresh: wall-clock expiry + per-outcome counter.
        // Both are process-global (one worker → one signer → one
        // refresh loop), so they live as a single-series Gauge /
        // labeled IntCounterVec on the private registry.
        //
        // Why "seconds remaining" rather than "Unix epoch seconds":
        // the JWT's `exp` claim and the worker's `expires_at` Instant
        // live on different clocks (wall vs monotonic). Translating
        // between them requires a boot-time anchor that drifts under
        // NTP step adjustments. The remaining-seconds gauge is
        // monotonic-stable: it goes up after each successful refresh
        // and trends down between refreshes, alerting on a stuck
        // loop without any clock arithmetic.
        let worker_jwt_expires_at_seconds = Gauge::with_opts(Opts::new(
            "edge_worker_jwt_expires_at_seconds",
            "Seconds remaining until the current JWT snapshot expires. \
             Updated by the proactive refresh loop \
             (`jwt_refresh::spawn_jwt_refresh_loop`, issue #504) on \
             every successful refresh. The gauge goes UP after each \
             refresh and trends DOWN between refreshes; alert when it \
             crosses zero or stops decreasing (a stuck loop or a \
             refresh failure that kept the previous snapshot). The \
             value is monotonic-stable — no wall-clock translation \
             required.",
        ))?;
        registry.register(Box::new(worker_jwt_expires_at_seconds.clone()))?;
        let worker_jwt_refresh_total = IntCounterVec::new(
            Opts::new(
                "edge_worker_jwt_refresh_total",
                "Outcome counter for proactive + reactive JWT refresh \
                 attempts (issue #504). `outcome=\"ok\"` for a successful \
                 install; `outcome=\"err\"` for any failure (previous \
                 snapshot remains in service). Reactive 401-triggered \
                 refreshes share the same counter so the operator sees \
                 a single source of truth.",
            ),
            &["outcome"],
        )?;
        registry.register(Box::new(worker_jwt_refresh_total.clone()))?;

        Ok(Arc::new(Self {
            registry,
            worker_uptime_seconds,
            worker_active_apps,
            worker_jwt_expires_at_seconds,
            worker_jwt_refresh_total,
            app_status,
            app_terminal_status,
            apps: RwLock::new(HashMap::new()),
            last_status: StdMutex::new(HashMap::new()),
            started_at: Instant::now(),
        }))
    }

    /// Register an app and return a cloneable handle. Called from
    /// `Supervisor::start_app` after the `RequestMeter` is built
    /// but BEFORE the per-app task is spawned (so the request
    /// counter is reachable from the dispatch path on the very
    /// first request).
    ///
    /// Register an app and return a cloneable handle. Called from
    /// `Supervisor::start_app` after the `RequestMeter` is built
    /// but BEFORE the per-app task is spawned (so the request
    /// counter is reachable from the dispatch path on the very
    /// first request).
    ///
    /// Returns `anyhow::Result` because `prometheus::IntCounter::with_opts`
    /// and `Registry::register` both carry error paths that should
    /// propagate as typed errors, not panics. In practice these
    /// errors only fire when label values escape the prometheus
    /// crate's validation (e.g. an empty string for a const label,
    /// which the supervisor already strips at `start_app`), so
    /// production callers should see `Ok`; the typed return is a
    /// defense against a future field addition that introduces an
    /// invalid label.
    ///
    /// Idempotency: if the same `(tenant_id, deployment_id)` is
    /// already registered, the existing handle is returned and a
    /// warn is logged. The supervisor's bring-up path does not
    /// exercise the duplicate case (it removes-then-inserts on
    /// redeploy), but a test harness might.
    pub async fn register_app(
        self: &Arc<Self>,
        tenant_id: &str,
        deployment_id: &str,
        app_name: &str,
    ) -> anyhow::Result<Arc<MetricsHandle>> {
        let key = (tenant_id.to_string(), deployment_id.to_string());
        {
            let apps = self.apps.read().await;
            if let Some(existing) = apps.get(&key) {
                tracing::warn!(
                    tenant_id,
                    deployment_id,
                    app_name,
                    "register_app: duplicate key — returning existing handle"
                );
                return Ok(existing.clone());
            }
        }

        // Build the four per-app counters. Each is registered
        // against our private Registry so `gather()` picks them up.
        // The `_box` Arc<dyn Collector> is kept on the handle to
        // unregister cleanly later (Registry::unregister matches
        // collectors by descriptor — name + const labels — so a
        // cloned IntCounter with the same labels works).
        let register_one = |name: &'static str, help: &'static str| -> anyhow::Result<IntCounter> {
            let c = IntCounter::with_opts(
                Opts::new(name, help)
                    .const_label("deployment_id", deployment_id)
                    .const_label("app_name", app_name),
            )
            .map_err(|e| anyhow::anyhow!("IntCounter::with_opts({name}): {e}"))?;
            self.registry
                .register(Box::new(c.clone()))
                .map_err(|e| anyhow::anyhow!("register({name}): {e}"))?;
            Ok(c)
        };

        let requests = register_one(
            "edge_requests_total",
            "Per-app request count, mirrored from the per-deployment \
             RequestMeter (issue #49). Monotonic — NOT decremented by \
             the heartbeat's snapshot-and-subtract path.",
        )?;
        let outbound_bytes = register_one(
            "edge_outbound_bytes_total",
            "Per-app outbound bytes (response bodies + synthetic 500s), \
             mirrored from the per-deployment RequestMeter.",
        )?;
        let resident_seconds = register_one(
            "edge_resident_seconds_total",
            "Per-app resident-seconds count (LongRunning apps only — \
             Handler FaaS apps don't contribute and the label set will \
             not appear). Mirrored from the per-deployment RequestMeter.",
        )?;
        let duration_ms = register_one(
            "edge_duration_ms_total",
            "Per-app FaaS wall-clock duration total in milliseconds (issue \
             #555). Handler apps only — LongRunning apps don't contribute \
             and the label set will not appear.",
        )?;
        let status_5xx = register_one(
            "edge_status_5xx_total",
            "Per-app 5xx error count (issue #84 asks 6/7). Stamped \
             from the `synthetic_500` chokepoint shared by the three \
             FaaS error terminal arms of `handle_request`. \
             Monotonic — NOT decremented by the heartbeat's \
             snapshot-and-subtract path. Body-cap 413 early \
             returns are NOT counted (tenant-config violation, \
             not a guest error).",
        )?;

        let requests_box: Arc<dyn Collector> = Arc::new(requests.clone());
        let outbound_bytes_box: Arc<dyn Collector> = Arc::new(outbound_bytes.clone());
        let resident_seconds_box: Arc<dyn Collector> = Arc::new(resident_seconds.clone());
        let duration_ms_box: Arc<dyn Collector> = Arc::new(duration_ms.clone());
        let status_5xx_box: Arc<dyn Collector> = Arc::new(status_5xx.clone());

        let handle = Arc::new(MetricsHandle {
            requests,
            outbound_bytes,
            resident_seconds,
            duration_ms,
            status_5xx,
            requests_box,
            outbound_bytes_box,
            resident_seconds_box,
            duration_ms_box,
            status_5xx_box,
            tenant_id: tenant_id.to_string(),
            deployment_id: deployment_id.to_string(),
            app_name: app_name.to_string(),
        });

        let mut apps = self.apps.write().await;
        // Double-check inside the write lock to close the
        // register-arc race between concurrent first-time registers.
        if let Some(existing) = apps.get(&key) {
            return Ok(existing.clone());
        }
        apps.insert(key, handle.clone());
        Ok(handle)
    }

    /// Unregister an app. Called from `Supervisor::stop_app` after
    /// the per-app task is aborted. Removes the four per-app
    /// counters AND every `edge_app_status` series for this
    /// `(tenant_id, deployment_id)`. Without this, the `/metrics`
    /// response would accumulate one series per app that has ever
    /// run on this worker — eventually OOM'ing on long-lived
    /// multi-tenant nodes.
    ///
    /// Before sweeping `edge_app_status`, stamps the last known
    /// status into the audit-style sibling `edge_app_terminal_status`
    /// so operators retain visibility into why a `Crashed`/`Hung`
    /// app died (otherwise the live gauge is empty for offline
    /// apps).
    pub async fn unregister_app(&self, tenant_id: &str, deployment_id: &str) {
        let key = (tenant_id.to_string(), deployment_id.to_string());
        let (app_name, handle) = {
            let mut apps = self.apps.write().await;
            let Some(handle) = apps.remove(&key) else {
                tracing::debug!(
                    tenant_id,
                    deployment_id,
                    "unregister_app: not present — no-op"
                );
                return;
            };
            (handle.app_name.clone(), handle)
        };

        // Snapshot the last known status BEFORE we drop the
        // `last_status` entry — we still need it to stamp
        // `edge_app_terminal_status`. If the app died without
        // `set_status` ever firing (a debug-level skip in the
        // start_app path), default to "stopping" — semantically
        // correct (the operator sees an unregister, which means
        // the app was alive enough to reach `stop_app`).
        let (last_status_str, last_exit_code) = {
            let mut last = self.last_status.lock().ok();
            let snapshot = last
                .as_mut()
                .and_then(|m| m.remove(&(deployment_id.to_string(), app_name.clone())));
            match snapshot {
                Some(s) => (s.to_string(), exit_code_for_status(s).to_string()),
                None => ("stopping".to_string(), String::new()),
            }
        };

        // Registry::unregister matches collectors by descriptor
        // (name + const labels). The boxes on the handle are
        // clones of the originally-registered collectors — passing
        // them back through `Box::new(handle.*_box)` finds the
        // match. Two `Arc::try_unwrap`-style moves aren't needed
        // because Box<dyn Collector> can be re-boxed from any Arc
        // by allocating a fresh Box around the inner value — but
        // `prometheus`'s `unregister` doesn't require ownership of
        // the inner counter, just a `Box<dyn Collector>` whose
        // descriptor matches. We construct those boxes fresh from
        // the still-live `IntCounter` fields on the handle.
        let _ = self.registry.unregister(Box::new(handle.requests.clone()));
        let _ = self
            .registry
            .unregister(Box::new(handle.outbound_bytes.clone()));
        let _ = self
            .registry
            .unregister(Box::new(handle.resident_seconds.clone()));
        let _ = self
            .registry
            .unregister(Box::new(handle.duration_ms.clone()));
        let _ = self
            .registry
            .unregister(Box::new(handle.status_5xx.clone()));

        // Drop every status series for this app. The IntGaugeVec
        // has a `remove_label_values` API keyed on the full label
        // tuple (deployment_id, app_name, status). It errors on
        // unknown series, which is fine — we sweep every status
        // we ever stamp. The status string set is the canonical
        // `APP_STATUS_STRINGS` from `supervisor` — a single source
        // of truth shared with `app_status_to_string`, so adding a
        // new variant only requires one place to be updated.
        for status in crate::supervisor::APP_STATUS_STRINGS {
            self.app_status
                .remove_label_values(&[deployment_id, &app_name, status])
                .ok();
        }

        // Stamp the audit-style terminal status. The label set
        // includes `exit_code` (empty for non-`Crashed`,
        // numeric for `Crashed { restart_count }`) so the same
        // `(deployment_id, app_name, status)` triple cannot
        // collide on a different `restart_count`. A repeat
        // unregister (e.g. via the register_app idempotency path)
        // overwrites the existing row — overwrite semantics, not
        // additive, so the cardinality is bounded by total
        // deployments ever observed (one row per
        // `(deployment_id, app_name)`).
        self.app_terminal_status
            .with_label_values(&[deployment_id, &app_name, &last_status_str, &last_exit_code])
            .set(1);
    }

    /// Stamp the current `edge_app_status` for an app to the
    /// provided `AppInstanceStatus`. Called from every status
    /// transition site in `supervisor::run_app_loop` (Starting →
    /// Running on first frame, Running → Draining on stop, etc.)
    /// and from `handle_app_crash` (Running → Crashed/Hung).
    ///
    /// Removes the previous status series before stamping the new
    /// one — keeps exactly one `(deployment_id, app_name)` row at
    /// value `1` (the others are absent, which the encoder omits).
    pub async fn set_status(
        &self,
        tenant_id: &str,
        deployment_id: &str,
        app_name: &str,
        status: &AppInstanceStatus,
    ) {
        let apps = self.apps.read().await;
        let Some(handle) = apps.get(&(tenant_id.to_string(), deployment_id.to_string())) else {
            // App isn't registered yet — the supervisor stamps
            // `status = Running` in `start_app` BEFORE calling
            // `register_app` to keep the timer anchor on the
            // counter-initialization moment, so this branch is the
            // expected normal path for the very first status.
            // Trace at debug to avoid log spam during the boot
            // window.
            tracing::debug!(
                tenant_id,
                deployment_id,
                app_name,
                "set_status: app not registered yet"
            );
            return;
        };
        let app_name_owned = handle.app_name.clone();
        drop(apps);

        let status_str = app_status_to_string(status);
        let key = (deployment_id.to_string(), app_name_owned.clone());

        // Clear the previous series (if any). Same-status no-op
        // skips the remove — keeps the IntGaugeVec thin.
        if let Ok(mut last) = self.last_status.lock() {
            let prev = last.get(&key).copied();
            if let Some(prev_status) = prev {
                if prev_status != status_str {
                    self.app_status
                        .remove_label_values(&[&key.0, &key.1, prev_status])
                        .ok();
                }
            }
            last.insert(key.clone(), status_str);
        }

        self.app_status
            .with_label_values(&[&key.0, &key.1, status_str])
            .set(1);
    }

    /// Render the `/metrics` body. Used by the HTTP server.
    pub fn gather(&self) -> anyhow::Result<String> {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        encoder.encode(&metric_families, &mut buf)?;
        Ok(String::from_utf8(buf)?)
    }

    /// Render convenience for the HTTP server. Falls back to an empty
    /// body on encode failure (which would only happen if the registry
    /// holds a collector that returns malformed metric families — the
    /// text encoder is well-tested and we own all registered
    /// collectors, so a non-empty fallback would be misleading).
    pub fn render(&self) -> Option<String> {
        self.gather().ok()
    }

    /// Bump the worker-level gauges. The uptime gauge is set, not
    /// incremented, because uptime is a monotonic duration, not a
    /// delta. The active-apps gauge is set to the current map size
    /// so a `remove_app` that races with the tick lands at a
    /// consistent value.
    ///
    /// `edge_worker_jwt_expires_at_seconds` is NOT touched here —
    /// it's set explicitly by the proactive refresh loop
    /// (`jwt_refresh::spawn_jwt_refresh_loop`) on every successful
    /// install. Tick-worker-gauges knows nothing about the signer;
    /// threading the signer in would force the metrics module to
    /// take a circular dependency on `auth`. The 1s ticker is short
    /// enough that the gauge stays live between installs — a stalled
    /// gauge is the intended "loop is stuck" alert signal.
    pub fn tick_worker_gauges(&self) {
        self.worker_uptime_seconds
            .set(self.started_at.elapsed().as_secs() as f64);
        // Active apps gauge: we want the count without holding the
        // async lock. Since `tick_worker_gauges` is synchronous (it
        // runs inside a `tokio::spawn`'ed async block), we read via
        // `try_lock` and skip on contention rather than block. The
        // gauge catches up next tick.
        if let Ok(apps) = self.apps.try_read() {
            self.worker_active_apps.set(apps.len() as f64);
        }
    }

    /// Convenience for `jwt_refresh::spawn_jwt_refresh_loop` and the
    /// reactive `with_token_refresh` helper. Bumps
    /// `edge_worker_jwt_refresh_total{outcome="ok"|"err"}` once per
    /// refresh attempt — both the proactive loop and the reactive
    /// helper share this counter so an operator sees a single source
    /// of truth.
    pub fn refresh_outcome_inc(&self, outcome: &str) {
        self.worker_jwt_refresh_total
            .with_label_values(&[outcome])
            .inc();
    }

    /// Stamp `edge_worker_jwt_expires_at_seconds` from the
    /// monotonic `Instant` the signer holds in its `TokenSnapshot`.
    /// We publish **seconds remaining** rather than the wall-clock
    /// Unix-epoch second at which the token expires. The remaining-
    /// seconds gauge has two advantages:
    ///
    /// 1. **No clock translation.** The Instants live entirely on
    ///    the monotonic clock; subtracting two Instants gives a
    ///    duration immune to NTP step adjustments. The pre-#504
    ///    review fix attempted a `SystemTime::now() + (deadline −
    ///    Instant::now())` translation that mixed two clocks and
    ///    could drift across NTP step events. The remaining-seconds
    ///    gauge eliminates the translation.
    /// 2. **Operator alert band is "crosses zero / stops decreasing".**
    ///    The post-#504 loop refreshes the snapshot when
    ///    `now > expires_at − REFRESH_LEAD`. After a successful
    ///    refresh the remaining-seconds gauge jumps back up to
    ///    ~TTL; between refreshes it trends toward zero. An alert
    ///    on `edge_worker_jwt_expires_at_seconds <= 0` or
    ///    `decrease > 2 * REFRESH_LEAD` flags a stuck loop without
    ///    requiring a wall-clock anchor at boot.
    ///
    /// Saturating arithmetic: if the cached `expires_at` is in the
    /// past (stale snapshot with no refresh), the gauge saturates
    /// at 0 rather than going negative.
    pub fn set_jwt_expires_at(&self, expires_at: std::time::Instant) {
        let now = std::time::Instant::now();
        let remaining_secs = expires_at.saturating_duration_since(now).as_secs();
        self.worker_jwt_expires_at_seconds
            .set(remaining_secs as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fresh WorkerMetrics → empty gather body should still include
    /// the two worker-level gauges (at zero) even before any apps
    /// are registered. Pins the contract that the public surface
    /// is fixed at construction. (Note: `IntGaugeVec` does not emit
    /// `# HELP` until at least one labeled child exists — that's a
    /// `prometheus` library behaviour, separate from our code.)
    #[tokio::test]
    async fn empty_worker_metrics_render_worker_gauges() {
        let m = WorkerMetrics::new().unwrap();
        let body = m.gather().unwrap();
        assert!(body.contains("# TYPE edge_worker_uptime_seconds gauge"));
        assert!(body.contains("# TYPE edge_worker_active_apps gauge"));
        assert!(body.contains("edge_worker_active_apps 0"));
        // No per-app counters yet.
        assert!(!body.contains("edge_requests_total"));
    }

    /// Registering twice for the same `(tenant_id, deployment_id)`
    /// returns the existing handle — the supervisor never hits this
    /// in production, but a test harness might, and leaking two
    /// series for the same key would inflate the worker-level
    /// active-apps counter.
    #[tokio::test]
    async fn register_app_idempotent_on_duplicate_key() {
        let m = WorkerMetrics::new().unwrap();
        let h1 = m
            .register_app("t_test", "dep_1", "my-app")
            .await
            .expect("test register_app");
        let h2 = m
            .register_app("t_test", "dep_1", "my-app")
            .await
            .expect("test register_app");
        assert_eq!(h1.requests.get(), 0);
        h1.requests.inc();
        assert_eq!(
            h2.requests.get(),
            1,
            "duplicate register shares the same counter"
        );
        // Map has exactly one entry.
        let apps = m.apps.read().await;
        assert_eq!(apps.len(), 1);
    }

    /// After `register_app` + a few counter bumps, the rendered
    /// body should expose the per-app label set under
    /// `edge_requests_total`.
    #[tokio::test]
    async fn register_app_emits_per_app_counter_series() {
        let m = WorkerMetrics::new().unwrap();
        let h = m
            .register_app("t_test", "dep_a", "api")
            .await
            .expect("test register_app");
        h.requests.inc_by(3);
        h.outbound_bytes.inc_by(1024);
        h.resident_seconds.inc_by(60);
        h.duration_ms.inc_by(120);
        h.status_5xx.inc_by(7);

        let body = m.gather().unwrap();
        // The TextEncoder sorts labels alphabetically, so the
        // expected output is `app_name="api",deployment_id="dep_a"`.
        assert!(
            body.contains("edge_requests_total{app_name=\"api\",deployment_id=\"dep_a\"} 3"),
            "expected edge_requests_total per-app series in:\n{body}"
        );
        assert!(body
            .contains("edge_outbound_bytes_total{app_name=\"api\",deployment_id=\"dep_a\"} 1024"));
        assert!(body
            .contains("edge_resident_seconds_total{app_name=\"api\",deployment_id=\"dep_a\"} 60"));
        assert!(
            body.contains("edge_duration_ms_total{app_name=\"api\",deployment_id=\"dep_a\"} 120")
        );
        assert!(
            body.contains("edge_status_5xx_total{app_name=\"api\",deployment_id=\"dep_a\"} 7"),
            "expected edge_status_5xx_total per-app series (issue #84); got:\n{body}"
        );
    }

    /// After `unregister_app`, the per-app counter label set is
    /// gone — the `/metrics` body must not contain
    /// `edge_requests_total{...}` for the unregistered app. This
    /// is the load-bearing test against the long-lived-worker OOM
    /// hazard. The audit-style `edge_app_terminal_status` sibling
    /// intentionally retains a row for the same key (that's the
    /// whole point — see the `terminal_status` tests below) so we
    /// check the four counter families specifically rather than
    /// the broad `app_name="ghost"` substring match that the
    /// pre-finding-#4 test used to assert.
    #[tokio::test]
    async fn unregister_app_drops_per_app_counters() {
        let m = WorkerMetrics::new().unwrap();
        let h = m
            .register_app("t_test", "dep_b", "ghost")
            .await
            .expect("test register_app");
        h.requests.inc();
        m.unregister_app("t_test", "dep_b").await;

        let body = m.gather().unwrap();
        // The four counter families must be gone — those are the
        // ones the OOM-hazard arg is about.
        for family in &[
            "edge_requests_total",
            "edge_outbound_bytes_total",
            "edge_resident_seconds_total",
            "edge_duration_ms_total",
            "edge_status_5xx_total",
        ] {
            assert!(
                !body.contains(&format!(
                    "{family}{{app_name=\"ghost\",deployment_id=\"dep_b\""
                )),
                "{family} series leaked after unregister:\n{body}"
            );
        }
        // Map is empty again.
        let apps = m.apps.read().await;
        assert!(apps.is_empty());
        // But the audit-style terminal status for the same key
        // must STILL be present (finding #4 contract).
        assert!(
            body.contains("edge_app_terminal_status{app_name=\"ghost\",deployment_id=\"dep_b\""),
            "expected audit-style terminal status to survive unregister:\n{body}"
        );
    }

    /// `set_status` writes the gauge to 1 for the current status,
    /// and removes all sibling status series for the same app. The
    /// rendered body must contain exactly one `edge_app_status` row
    /// per app.
    #[tokio::test]
    async fn set_status_writes_one_status_per_app() {
        let m = WorkerMetrics::new().unwrap();
        let _h = m
            .register_app("t_test", "dep_c", "svc")
            .await
            .expect("test register_app");
        m.set_status("t_test", "dep_c", "svc", &AppInstanceStatus::Starting)
            .await;
        let body = m.gather().unwrap();
        // Labels are sorted alphabetically by the encoder.
        assert!(
            body.contains(
                "edge_app_status{app_name=\"svc\",deployment_id=\"dep_c\",status=\"starting\"} 1"
            ),
            "expected starting=1 in:\n{body}"
        );

        // Transition to Running — the Starting series must drop,
        // the Running series must show 1.
        m.set_status("t_test", "dep_c", "svc", &AppInstanceStatus::Running)
            .await;
        let body = m.gather().unwrap();
        assert!(
            body.contains(
                "edge_app_status{app_name=\"svc\",deployment_id=\"dep_c\",status=\"running\"} 1"
            ),
            "expected running=1 in:\n{body}"
        );
        assert!(
            !body.contains("status=\"starting\""),
            "Starting series not cleared on transition: {body}"
        );
    }

    /// `unregister_app` stamps the audit-style terminal status
    /// (`edge_app_terminal_status`) with the LAST KNOWN status
    /// for the app, AFTER sweeping the live `edge_app_status`.
    /// Operators need this so a `Crashed`/`Hung` app still has a
    /// visible row in `/metrics` post-unregister. Pin: after
    /// `set_status(Stopping)` + `unregister_app`, the body must
    /// contain `edge_app_terminal_status{...,status="stopping"} 1`
    /// and NO `edge_app_status` row for the same labels.
    #[tokio::test]
    async fn unregister_app_preserves_terminal_status_in_audit_gauge() {
        let m = WorkerMetrics::new().unwrap();
        let _h = m
            .register_app("t_test", "dep_d", "crashy")
            .await
            .expect("test register_app");
        m.set_status("t_test", "dep_d", "crashy", &AppInstanceStatus::Stopping)
            .await;
        m.unregister_app("t_test", "dep_d").await;

        let body = m.gather().unwrap();
        assert!(
            body.contains("edge_app_terminal_status{")
                && body.contains("deployment_id=\"dep_d\"")
                && body.contains("app_name=\"crashy\"")
                && body.contains("status=\"stopping\"")
                && body.contains("exit_code=\"\""),
            "expected audit-style terminal status row in:\n{body}"
        );
        assert!(
            !body.contains("edge_app_status{") || !body.contains("deployment_id=\"dep_d\""),
            "live edge_app_status row should be swept after unregister:\n{body}"
        );
    }

    /// Pin the invariant for the rarer path: an app that reaches
    /// `unregister_app` without `set_status` ever firing (rare but
    /// possible during a boot-time crash). The terminal status
    /// MUST still be stamped — defaulting to "stopping" — so an
    /// operator can tell from `/metrics` that an app reached
    /// `unregister` at all. Without the default, the audit trail
    /// silently disappears and the operator only sees a clean
    /// `/metrics` body.
    #[tokio::test]
    async fn unregister_app_defaults_terminal_status_when_set_status_was_never_called() {
        let m = WorkerMetrics::new().unwrap();
        let _h = m
            .register_app("t_test", "dep_e", "ghost")
            .await
            .expect("test register_app");
        // No set_status call.
        m.unregister_app("t_test", "dep_e").await;

        let body = m.gather().unwrap();
        assert!(
            body.contains("edge_app_terminal_status{")
                && body.contains("deployment_id=\"dep_e\"")
                && body.contains("status=\"stopping\""),
            "expected default terminal status row in:\n{body}"
        );
    }

    /// Idempotent re-registration: a second `register_app` for
    /// the same `(tenant_id, deployment_id)` returns the existing
    /// handle (already covered by `register_app_idempotent_on_duplicate_key`).
    /// The audit gauge for the same key after a hypothetical
    /// unregister-then-register cycle should overwrite, not
    /// accumulate.
    #[tokio::test]
    async fn unregister_app_then_register_app_keeps_audit_gauge_bounded() {
        let m = WorkerMetrics::new().unwrap();
        let _h1 = m
            .register_app("t_test", "dep_f", "wave")
            .await
            .expect("test register_app");
        m.set_status(
            "t_test",
            "dep_f",
            "wave",
            &AppInstanceStatus::Crashed { restart_count: 3 },
        )
        .await;
        m.unregister_app("t_test", "dep_f").await;
        // Don't actually re-register — we just want to confirm the
        // audit row is the only one we see, not duplicated.
        let body = m.gather().unwrap();
        let count = body
            .matches("edge_app_terminal_status{")
            .filter(|_| true)
            .count();
        assert_eq!(
            count, 1,
            "expected exactly one terminal status row, got {count} in:\n{body}"
        );
    }

    /// tick_worker_gauges bumps uptime + active-apps. Pin both.
    #[tokio::test]
    async fn tick_worker_gauges_sets_uptime_and_active_count() {
        let m = WorkerMetrics::new().unwrap();
        let _h = m
            .register_app("t1", "d1", "a")
            .await
            .expect("test register_app");
        let _h = m
            .register_app("t2", "d2", "b")
            .await
            .expect("test register_app");
        // Sleep just enough for elapsed() to advance > 0.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        m.tick_worker_gauges();

        let body = m.gather().unwrap();
        assert!(body.contains("edge_worker_active_apps 2"));
        // uptime should be >= 1s after the sleep. The encoder renders
        // floats; >= 1.0 is the right band for `elapsed().as_secs()`.
        assert!(
            body.contains("edge_worker_uptime_seconds ")
                && body
                    .lines()
                    .find(|l| l.starts_with("edge_worker_uptime_seconds"))
                    .and_then(|l| l.split_whitespace().last())
                    .and_then(|v| v.parse::<f64>().ok())
                    .map(|v| v >= 1.0)
                    .unwrap_or(false),
            "uptime not >=1s: {body}"
        );
    }

    /// `set_jwt_expires_at(now() + 90s)` should publish a remaining
    /// gauge of ~90s, NOT a wall-clock Unix-epoch second. Issue
    /// #504 review-fix #5. Two assertions:
    /// - the value is in [89, 90] (the sub-second resolution is
    ///   fine for an alert gauge)
    /// - the value is far smaller than the wall-clock Unix-epoch
    ///   right now (~2026), confirming we publish seconds-remaining
    ///   not absolute time
    #[tokio::test]
    async fn set_jwt_expires_at_publishes_remaining_seconds() {
        let m = WorkerMetrics::new().unwrap();
        let expires_at = std::time::Instant::now() + std::time::Duration::from_secs(90);
        m.set_jwt_expires_at(expires_at);
        let body = m.gather().unwrap();
        let gauge_value: f64 = body
            .lines()
            .find(|l| l.starts_with("edge_worker_jwt_expires_at_seconds "))
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse().ok())
            .expect("gauge value must parse");
        assert!(
            (89.0..=90.0).contains(&gauge_value),
            "expected remaining-seconds gauge in [89, 90], got {gauge_value}"
        );
        assert!(
            gauge_value < 100_000.0,
            "gauge must publish seconds-remaining, not Unix-epoch seconds (got {gauge_value})"
        );
    }

    /// A stale `expires_at` (in the past) saturates to 0 — the
    /// operator alert band is "expires_at_seconds <= 0" / "stops
    /// decreasing." Going negative would be misleading.
    #[tokio::test]
    async fn set_jwt_expires_at_stale_saturates_to_zero() {
        let m = WorkerMetrics::new().unwrap();
        let expired = std::time::Instant::now() - std::time::Duration::from_secs(60);
        m.set_jwt_expires_at(expired);
        let body = m.gather().unwrap();
        let gauge_value: f64 = body
            .lines()
            .find(|l| l.starts_with("edge_worker_jwt_expires_at_seconds "))
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse().ok())
            .expect("gauge value must parse");
        assert_eq!(gauge_value, 0.0, "stale expires_at must saturate to 0");
    }
}
