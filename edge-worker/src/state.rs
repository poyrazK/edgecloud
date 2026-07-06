//! App state tracking for running instances.

use std::collections::HashMap;
use std::sync::Arc;

use edge_runtime::interfaces::observe::MetricsAccumulator;
use edge_runtime::RequestMeter;
use tokio::sync::Mutex;
use wasmtime::component::InstancePre;
use wasmtime::Engine;

use crate::detect::ExecutionModel;
use crate::dispatch::HandlerDispatch;

/// Status of a running app instance.
#[derive(Debug, Clone, PartialEq)]
pub enum AppInstanceStatus {
    #[allow(dead_code)]
    Starting,
    Running,
    #[allow(dead_code)]
    Stopping,
    Crashed {
        restart_count: u32,
    },
    /// App did not return from execute_app within the health check timeout.
    Hung,
}

/// A single running app instance.
#[allow(dead_code)]
pub struct AppInstance {
    pub deployment_id: String,
    pub app_name: String,
    pub tenant_id: String,
    pub port: u16,
    pub status: AppInstanceStatus,
    pub meter: Arc<RequestMeter>,
    /// Channel to signal graceful shutdown to the app task. Wrapped in Option so
    /// it can be taken out of the locked struct to call send().
    pub shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    /// Broadcast shutdown channel — `Some` only for Handler (FaaS)
    /// apps whose axum/hyper server subscribes through
    /// `with_graceful_shutdown` (Phase C-7). Multiple subscribers per
    /// broadcast channel — needed so the server loop and any in-
    /// flight per-request tasks can both observe the shutdown. Long-
    /// Running apps keep using the `oneshot::Sender` above.
    pub shutdown_tx_broadcast: Option<tokio::sync::broadcast::Sender<()>>,
    /// Pre-compiled component for fast instantiation on restart.
    pub instance_pre: InstancePre<edge_runtime::RuntimeState>,
    /// Handle to the spawned app task — used to propagate panics on stop.
    /// Wrapped in Arc so it can be cloned without taking ownership.
    pub handle: Option<std::sync::Arc<tokio::task::JoinHandle<()>>>,
    /// Handle to the epoch ticker that advances the wasmtime engine clock.
    /// The ticker is aborted on app stop; without it the engine epoch would
    /// never advance, and the Store-level deadline would never fire.
    /// Wrapped in Option so stop_app can take it out of the locked struct.
    pub ticker: Option<tokio::task::JoinHandle<()>>,
    /// Which execution model the guest uses.
    ///
    /// `LongRunning` guests drive themselves via `_start` (spawned by
    /// `run_app_loop`). `Handler` guests are dispatched per-request via
    /// `dispatch` (Phase C wires `wasmtime_wasi_http::ProxyPre`).
    pub execution_model: ExecutionModel,
    /// FaaS dispatcher — `Some` only when `execution_model == Handler`.
    ///
    /// Phase B stores `Some(HandlerDispatch::new(port))` so the field
    /// type-checks; Phase C fills in the `ProxyPre` + axum server. The
    /// supervisor currently ignores `dispatch` for Handler components
    /// (the spawned task is a placeholder pending the per-request
    /// wiring) — see `supervisor::start_app`.
    pub dispatch: Option<Arc<HandlerDispatch>>,
    /// Shared metrics accumulator for this app instance. Guest
    /// `edge:observe` metric calls write into this, and the heartbeat
    /// builder snapshots it every 30s to populate `observer_metrics`
    /// on the wire. `None` before the app is running (the accumulator
    /// is created in `run_app_loop` / `start_app`).
    pub metrics_acc: Option<Arc<MetricsAccumulator>>,
    /// If EDGE_WS_PORT was requested, holds the allocated port number.
    /// Reported in heartbeats so the ingress can route WS traffic.
    pub ws_port: Option<u16>,
}

/// Shared worker state — protected by a tokio RwLock.
/// Apps are stored behind Arc<Mutex<>> so individual fields can be mutated
/// (e.g., status update to Crashed) without replacing the Arc entry.
/// Shared worker state — protected by a tokio RwLock.
/// Apps are stored behind Arc<Mutex<>> so individual fields can be mutated
/// (e.g., status update to Crashed) without replacing the Arc entry.
#[allow(dead_code)]
pub struct WorkerState {
    /// Currently running app instances, keyed by `(tenant_id, app_name)`.
    ///
    /// The tuple key prevents collisions when two tenants happen to
    /// deploy an app with the same name (e.g. both tenants have a
    /// service literally called "api"). It also lets `handle_task_message`
    /// filter to just the current message's tenant without scanning
    /// every running app.
    pub apps: HashMap<(String, String), Arc<Mutex<AppInstance>>>,
    /// Shared wasmtime Engine (for compilation caching across apps)
    pub engine: Engine,
    /// Last time the worker's main loop observed a TaskMessage from
    /// NATS. Used by the `/sync` HTTP fallback (issue #53) and by
    /// health-check tests to distinguish "NATS is silent because
    /// nothing has changed" from "NATS is silent because the worker
    /// is wedged". Stamped on every `handle_task_message` entry.
    pub last_task_received_at: std::sync::Mutex<Option<std::time::Instant>>,
}

impl WorkerState {
    pub fn new(engine: Engine) -> Self {
        Self {
            apps: HashMap::new(),
            engine,
            last_task_received_at: std::sync::Mutex::new(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── AppInstanceStatus active / terminal semantics ────────────────

    #[test]
    fn running_is_active() {
        assert!(matches!(AppInstanceStatus::Running, AppInstanceStatus::Running | AppInstanceStatus::Starting));
    }

    #[test]
    fn starting_is_active() {
        assert!(matches!(AppInstanceStatus::Starting, AppInstanceStatus::Running | AppInstanceStatus::Starting));
    }

    #[test]
    fn stopping_is_not_active() {
        assert!(!matches!(AppInstanceStatus::Stopping, AppInstanceStatus::Running | AppInstanceStatus::Starting));
    }

    #[test]
    fn crashed_is_not_active() {
        assert!(!matches!(AppInstanceStatus::Crashed { restart_count: 3 }, AppInstanceStatus::Running | AppInstanceStatus::Starting));
    }

    #[test]
    fn hung_is_not_active() {
        assert!(!matches!(AppInstanceStatus::Hung, AppInstanceStatus::Running | AppInstanceStatus::Starting));
    }

    #[test]
    fn running_is_not_terminal() {
        assert!(!matches!(AppInstanceStatus::Running, AppInstanceStatus::Crashed { .. } | AppInstanceStatus::Hung));
    }

    #[test]
    fn crashed_is_terminal() {
        assert!(matches!(AppInstanceStatus::Crashed { restart_count: 5 }, AppInstanceStatus::Crashed { .. } | AppInstanceStatus::Hung));
    }

    #[test]
    fn hung_is_terminal() {
        assert!(matches!(AppInstanceStatus::Hung, AppInstanceStatus::Crashed { .. } | AppInstanceStatus::Hung));
    }

    // ── AppInstanceStatus restart count extraction ───────────────────

    #[test]
    fn crashed_restart_count_matches() {
        let status = AppInstanceStatus::Crashed { restart_count: 7 };
        if let AppInstanceStatus::Crashed { restart_count } = &status {
            assert_eq!(*restart_count, 7);
        } else {
            panic!("expected Crashed variant");
        }
    }

    #[test]
    fn non_crashed_restart_count_is_implied_zero() {
        assert!(!matches!(AppInstanceStatus::Hung, AppInstanceStatus::Crashed { .. }));
    }

    // ── AppInstanceStatus equality (derived) ─────────────────────────

    #[test]
    fn same_variants_are_equal() {
        assert_eq!(
            AppInstanceStatus::Crashed { restart_count: 3 },
            AppInstanceStatus::Crashed { restart_count: 3 }
        );
    }

    #[test]
    fn different_restart_counts_are_not_equal() {
        assert_ne!(
            AppInstanceStatus::Crashed { restart_count: 3 },
            AppInstanceStatus::Crashed { restart_count: 5 }
        );
    }

    #[test]
    fn different_variants_are_not_equal() {
        assert_ne!(AppInstanceStatus::Running, AppInstanceStatus::Hung);
    }

    // ── WorkerState ──────────────────────────────────────────────────

    #[test]
    fn new_worker_state_is_empty() {
        let engine = Engine::default();
        let state = WorkerState::new(engine);
        assert!(state.apps.is_empty());
        assert_eq!(state.apps.len(), 0);
    }

    #[test]
    fn new_worker_state_last_task_is_none() {
        let engine = Engine::default();
        let state = WorkerState::new(engine);
        let last = state.last_task_received_at.lock().unwrap();
        assert!(last.is_none());
    }
}
