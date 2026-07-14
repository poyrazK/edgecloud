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
//! Surface (7 families):
//!   - edge_requests_total{deployment_id, app_name}              (IntCounter)
//!   - edge_outbound_bytes_total{deployment_id, app_name}        (IntCounter)
//!   - edge_resident_seconds_total{deployment_id, app_name}      (IntCounter)
//!   - edge_duration_ms_total{deployment_id, app_name}           (IntCounter)
//!   - edge_app_status{deployment_id, app_name, status}          (IntGaugeVec)
//!     — set to 1 by set_status; clear previous via remove_label_values
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
    core::Collector, Encoder, Gauge, IntCounter, IntGaugeVec, Opts, Registry, TextEncoder,
};
use tokio::sync::RwLock;

use crate::state::AppInstanceStatus;
use crate::supervisor::app_status_to_string;

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
    /// Registered boxes held for the unregister step.
    /// Field order mirrors the counter order; the boxes are
    /// `IntCounter` clones (the inner state is `Arc`, so the
    /// cloned box shares the same counter value).
    pub requests_box: Arc<dyn Collector>,
    pub outbound_bytes_box: Arc<dyn Collector>,
    pub resident_seconds_box: Arc<dyn Collector>,
    pub duration_ms_box: Arc<dyn Collector>,
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
    pub app_status: IntGaugeVec,
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

        Ok(Arc::new(Self {
            registry,
            worker_uptime_seconds,
            worker_active_apps,
            app_status,
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
    ) -> Arc<MetricsHandle> {
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
                return existing.clone();
            }
        }

        // Build the four per-app counters. Each is registered
        // against our private Registry so `gather()` picks them up.
        // The `_box` Arc<dyn Collector> is kept on the handle to
        // unregister cleanly later (Registry::unregister matches
        // collectors by descriptor — name + const labels — so a
        // cloned IntCounter with the same labels works).
        let register_one = |name: &'static str, help: &'static str| -> IntCounter {
            let c = IntCounter::with_opts(
                Opts::new(name, help)
                    .const_label("deployment_id", deployment_id)
                    .const_label("app_name", app_name),
            )
            .expect("IntCounter::with_opts");
            self.registry
                .register(Box::new(c.clone()))
                .unwrap_or_else(|e| panic!("register {name}: {e}"));
            c
        };

        let requests = register_one(
            "edge_requests_total",
            "Per-app request count, mirrored from the per-deployment \
             RequestMeter (issue #49). Monotonic — NOT decremented by \
             the heartbeat's snapshot-and-subtract path.",
        );
        let outbound_bytes = register_one(
            "edge_outbound_bytes_total",
            "Per-app outbound bytes (response bodies + synthetic 500s), \
             mirrored from the per-deployment RequestMeter.",
        );
        let resident_seconds = register_one(
            "edge_resident_seconds_total",
            "Per-app resident-seconds count (LongRunning apps only — \
             Handler FaaS apps don't contribute and the label set will \
             not appear). Mirrored from the per-deployment RequestMeter.",
        );
        let duration_ms = register_one(
            "edge_duration_ms_total",
            "Per-app FaaS wall-clock duration total in milliseconds (issue \
             #555). Handler apps only — LongRunning apps don't contribute \
             and the label set will not appear.",
        );

        let requests_box: Arc<dyn Collector> = Arc::new(requests.clone());
        let outbound_bytes_box: Arc<dyn Collector> = Arc::new(outbound_bytes.clone());
        let resident_seconds_box: Arc<dyn Collector> = Arc::new(resident_seconds.clone());
        let duration_ms_box: Arc<dyn Collector> = Arc::new(duration_ms.clone());

        let handle = Arc::new(MetricsHandle {
            requests,
            outbound_bytes,
            resident_seconds,
            duration_ms,
            requests_box,
            outbound_bytes_box,
            resident_seconds_box,
            duration_ms_box,
            tenant_id: tenant_id.to_string(),
            deployment_id: deployment_id.to_string(),
            app_name: app_name.to_string(),
        });

        let mut apps = self.apps.write().await;
        // Double-check inside the write lock to close the
        // register-arc race between concurrent first-time registers.
        if let Some(existing) = apps.get(&key) {
            return existing.clone();
        }
        apps.insert(key, handle.clone());
        handle
    }

    /// Unregister an app. Called from `Supervisor::stop_app` after
    /// the per-app task is aborted. Removes the four per-app
    /// counters AND every `edge_app_status` series for this
    /// `(tenant_id, deployment_id)`. Without this, the `/metrics`
    /// response would accumulate one series per app that has ever
    /// run on this worker — eventually OOM'ing on long-lived
    /// multi-tenant nodes.
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

        // Drop the last_status tracker entry for this app so a
        // future `register_app` with the same deployment_id (rare
        // but possible after a redeploy) doesn't see a phantom
        // previous status.
        if let Ok(mut last) = self.last_status.lock() {
            last.remove(&(deployment_id.to_string(), app_name.clone()));
        }

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

        // Drop every status series for this app. The IntGaugeVec
        // has a `remove_label_values` API keyed on the full label
        // tuple (deployment_id, app_name, status). It errors on
        // unknown series, which is fine — we sweep every status
        // we ever stamp.
        for status in [
            "running",
            "starting",
            "draining",
            "stopping",
            "crashed",
            "hung",
        ] {
            self.app_status
                .remove_label_values(&[deployment_id, &app_name, status])
                .ok();
        }
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
            // counter-intialization moment, so this branch is the
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

    /// Bump the worker-level gauges. The uptime gauge is set, not
    /// incremented, because uptime is a monotonic duration, not a
    /// delta. The active-apps gauge is set to the current map size
    /// so a `remove_app` that races with the tick lands at a
    /// consistent value.
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
            .await;
        let h2 = m.register_app("t_test", "dep_1", "my-app").await;
        assert_eq!(h1.requests.get(), 0);
        h1.requests.inc();
        assert_eq!(h2.requests.get(), 1, "duplicate register shares the same counter");
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
            .await;
        h.requests.inc_by(3);
        h.outbound_bytes.inc_by(1024);
        h.resident_seconds.inc_by(60);
        h.duration_ms.inc_by(120);

        let body = m.gather().unwrap();
        // The TextEncoder sorts labels alphabetically, so the
        // expected output is `app_name="api",deployment_id="dep_a"`.
        assert!(
            body.contains("edge_requests_total{app_name=\"api\",deployment_id=\"dep_a\"} 3"),
            "expected edge_requests_total per-app series in:\n{body}"
        );
        assert!(body.contains("edge_outbound_bytes_total{app_name=\"api\",deployment_id=\"dep_a\"} 1024"));
        assert!(body.contains("edge_resident_seconds_total{app_name=\"api\",deployment_id=\"dep_a\"} 60"));
        assert!(body.contains("edge_duration_ms_total{app_name=\"api\",deployment_id=\"dep_a\"} 120"));
    }

    /// After `unregister_app`, the per-app label set is gone — the
    /// `/metrics` body must not contain `edge_requests_total{...}` for
    /// the unregistered app. This is the load-bearing test against
    /// the long-lived-worker OOM hazard.
    #[tokio::test]
    async fn unregister_app_drops_per_app_counters() {
        let m = WorkerMetrics::new().unwrap();
        let h = m.register_app("t_test", "dep_b", "ghost").await;
        h.requests.inc();
        m.unregister_app("t_test", "dep_b").await;

        let body = m.gather().unwrap();
        assert!(
            !body.contains("edge_requests_total{app_name=\"ghost\",deployment_id=\"dep_b\""),
            "edge_requests_total series leaked after unregister:\n{body}"
        );
        assert!(
            !body.contains("app_name=\"ghost\""),
            "any ghost-series still present after unregister:\n{body}"
        );
        // Map is empty again.
        let apps = m.apps.read().await;
        assert!(apps.is_empty());
    }

    /// `set_status` writes the gauge to 1 for the current status,
    /// and removes all sibling status series for the same app. The
    /// rendered body must contain exactly one `edge_app_status` row
    /// per app.
    #[tokio::test]
    async fn set_status_writes_one_status_per_app() {
        let m = WorkerMetrics::new().unwrap();
        let _h = m.register_app("t_test", "dep_c", "svc").await;
        m.set_status(
            "t_test",
            "dep_c",
            "svc",
            &AppInstanceStatus::Starting,
        )
        .await;
        let body = m.gather().unwrap();
        // Labels are sorted alphabetically by the encoder.
        assert!(
            body.contains("edge_app_status{app_name=\"svc\",deployment_id=\"dep_c\",status=\"starting\"} 1"),
            "expected starting=1 in:\n{body}"
        );

        // Transition to Running — the Starting series must drop,
        // the Running series must show 1.
        m.set_status("t_test", "dep_c", "svc", &AppInstanceStatus::Running)
            .await;
        let body = m.gather().unwrap();
        assert!(
            body.contains("edge_app_status{app_name=\"svc\",deployment_id=\"dep_c\",status=\"running\"} 1"),
            "expected running=1 in:\n{body}"
        );
        assert!(
            !body.contains("status=\"starting\""),
            "Starting series not cleared on transition: {body}"
        );
    }

    /// tick_worker_gauges bumps uptime + active-apps. Pin both.
    #[tokio::test]
    async fn tick_worker_gauges_sets_uptime_and_active_count() {
        let m = WorkerMetrics::new().unwrap();
        let _h = m.register_app("t1", "d1", "a").await;
        let _h = m.register_app("t2", "d2", "b").await;
        // Sleep just enough for elapsed() to advance > 0.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        m.tick_worker_gauges();

        let body = m.gather().unwrap();
        assert!(body.contains("edge_worker_active_apps 2"));
        // uptime should be >= 1s after the sleep. The encoder renders
        // floats; >= 1.0 is the right band for `elapsed().as_secs()`.
        assert!(
            body.contains("edge_worker_uptime_seconds ")
                && body.lines()
                    .find(|l| l.starts_with("edge_worker_uptime_seconds"))
                    .and_then(|l| l.split_whitespace().last())
                    .and_then(|v| v.parse::<f64>().ok())
                    .map(|v| v >= 1.0)
                    .unwrap_or(false),
            "uptime not >=1s: {body}"
        );
    }
}
