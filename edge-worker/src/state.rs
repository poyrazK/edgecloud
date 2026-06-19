//! App state tracking for running instances.

use std::collections::HashMap;
use std::sync::Arc;

use edge_runtime::RequestMeter;
use tokio::sync::Mutex;
use wasmtime::component::InstancePre;
use wasmtime::Engine;

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
}

/// Shared worker state — protected by a tokio RwLock.
/// Apps are stored behind Arc<Mutex<>> so individual fields can be mutated
/// (e.g., status update to Crashed) without replacing the Arc entry.
pub struct WorkerState {
    /// Currently running app instances: app_name -> AppInstance (Arc-wrapped for
    /// cheap clone, with Mutex for interior mutability of status/fields).
    pub apps: HashMap<String, Arc<Mutex<AppInstance>>>,
    /// Shared wasmtime Engine (for compilation caching across apps)
    pub engine: Engine,
}

impl WorkerState {
    pub fn new(engine: Engine) -> Self {
        Self {
            apps: HashMap::new(),
            engine,
        }
    }
}
