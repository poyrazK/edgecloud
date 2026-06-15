//! App state tracking for running instances.

use std::collections::HashMap;
use std::sync::Arc;

use edge_runtime::RequestMeter;
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
    #[allow(dead_code)]
    Crashed {
        restart_count: u32,
    },
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
    /// Channel to signal graceful shutdown to the app task.
    pub shutdown_tx: tokio::sync::oneshot::Sender<()>,
    /// Pre-compiled component for fast instantiation on restart.
    pub instance_pre: InstancePre<edge_runtime::RuntimeState>,
}

/// Shared worker state — protected by a tokio RwLock.
pub struct WorkerState {
    /// Currently running app instances: app_name -> AppInstance
    pub apps: HashMap<String, AppInstance>,
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
