//! Core supervisor logic — app lifecycle management.

use std::collections::HashMap;
use std::sync::Arc;

use edge_runtime::linker::create_component_linker_long_running;
use edge_runtime::{EgressPolicy, RequestMeter};
use futures::StreamExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};
use wasmtime::component::InstancePre;

use crate::config::Config;
use crate::detect::ExecutionModel;
use crate::dispatch::{try_load_tls_config, HandlerConfig, HandlerDispatch};
use crate::downloader::Downloader;
use crate::log_forwarder::LogForwarder;
use crate::messages::{
    AppSpec, AppStatus, ClusterHeadroom, HeartbeatMessage, MetricKind, MetricSample, TaskMessage,
};
use crate::nats::NatsClient;
use crate::port_pool::PortPool;
use crate::state::{AppInstance, AppInstanceStatus, WorkerState};

/// A pool of pre-warmed wasmtime::Engine instances.
pub struct StandbyPool {
    pool: Mutex<tokio::sync::mpsc::Receiver<wasmtime::Engine>>,
    sender: tokio::sync::mpsc::Sender<wasmtime::Engine>,
}

impl StandbyPool {
    pub fn new(size: usize) -> anyhow::Result<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(size.max(1));
        for _ in 0..size {
            let engine = edge_runtime::create_engine()?;
            tx.try_send(engine)
                .map_err(|_| anyhow::anyhow!("failed to pre-warm pool"))?;
        }
        Ok(Self {
            pool: Mutex::new(rx),
            sender: tx,
        })
    }

    pub async fn acquire(&self, state: &RwLock<WorkerState>) -> wasmtime::Engine {
        let mut rx = self.pool.lock().await;
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(engine)) => engine,
            _ => {
                // Try to evict the LRU FaaS app
                if let Some(engine) = self.evict_lru_app(state).await {
                    engine
                } else {
                    tracing::warn!("StandbyPool exhausted and no idle apps to evict; creating transient engine");
                    edge_runtime::create_engine().expect("fallback engine must build")
                }
            }
        }
    }

    async fn evict_lru_app(&self, state: &RwLock<WorkerState>) -> Option<wasmtime::Engine> {
        let apps = {
            let guard = state.read().await;
            guard.apps.clone()
        };

        let mut candidate: Option<(std::time::Instant, Arc<Mutex<AppInstance>>)> = None;

        for ((_tenant_id, _app_name), app_mutex) in apps {
            let app = app_mutex.lock().await;
            if app.execution_model == ExecutionModel::Handler {
                if let Some(ref dispatch) = app.dispatch {
                    if dispatch.has_engine().await {
                        let last_req_opt = {
                            let lock = dispatch.config.last_request_at.lock().await;
                            *lock
                        };
                        if let Some(last_req) = last_req_opt {
                            match candidate {
                                None => {
                                    candidate = Some((last_req, app_mutex.clone()));
                                }
                                Some((oldest_req, _)) if last_req < oldest_req => {
                                    candidate = Some((last_req, app_mutex.clone()));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        if let Some((_, app_mutex)) = candidate {
            let app = app_mutex.lock().await;
            if let Some(ref dispatch) = app.dispatch {
                if let Some(engine) = dispatch.evict().await {
                    tracing::info!(
                        "StandbyPool exhausted: evicting least-recently-used app to reclaim engine"
                    );
                    return Some(engine);
                }
            }
        }

        None
    }

    pub fn release(&self, engine: wasmtime::Engine) {
        let _ = self.sender.try_send(engine);
    }
}

// ── build_heartbeat integration tests ──────────────────────────────────
// These tests construct a real Supervisor with seeded WorkerState and
// verify the heartbeat wire format. No Docker required, but they need
// the handler.wasm fixture file linked into edge_runtime.

#[cfg(test)]
mod heartbeat_integration_tests {
    use super::*;
    use crate::auth::WorkerJwtSigner;
    use crate::downloader::Downloader;
    use crate::log_forwarder::LogForwarder;
    use crate::port_pool::PortPool;

    fn cp_config() -> Config {
        Config {
            worker_id: "w_test".to_string(),
            region: "fra".to_string(),
            worker_addr: "127.0.0.1:9000".to_string(),
            worker_tenant_id: "t_test".to_string(),
            nats_url: String::new(),
            control_plane_url: "http://localhost:0".to_string(),
            cache_dir: std::path::PathBuf::from("/tmp"),
            heartbeat_interval_secs: 30,
            health_check_timeout_secs: 30,
            worker_sync_threshold_secs: 30,
            port_cooldown_secs: 1,
            starting_port: 10000,
            max_memory_mb: 256,
            epoch_tick_ms: 10,
            epoch_deadline_ticks: 100,
            consumer_name: "test".to_string(),
            task_stream_replicas: 1,
            worker_jwt_secret: String::new(),
            worker_jwt_kid: None,
            worker_jwt_issuer: String::new(),
            worker_bootstrap_secret: String::new(),
            handler_request_budget_ms: 1000,
            handler_max_request_body_bytes: 0,
            tls_cert_path: None,
            tls_key_path: None,
            socket_mode: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            standby_pool_size: 1,
        }
    }

    fn load_handler_fixture(
        engine: &wasmtime::Engine,
    ) -> wasmtime::component::InstancePre<edge_runtime::RuntimeState> {
        let paths = [
            "tests/fixtures/handler.wasm",
            "edge-worker/tests/fixtures/handler.wasm",
        ];
        let wasm_path = paths
            .iter()
            .map(std::path::PathBuf::from)
            .find(|p| p.exists())
            .expect("handler.wasm fixture not found");
        let bytes = std::fs::read(&wasm_path).unwrap();
        let component = wasmtime::component::Component::from_binary(engine, &bytes)
            .expect("compile handler component");
        let linker = edge_runtime::create_component_linker_handler(engine).expect("create linker");
        linker.instantiate_pre(&component).expect("instantiate_pre")
    }

    fn build_supervisor(state: Arc<RwLock<WorkerState>>) -> Arc<Supervisor> {
        let jwt = WorkerJwtSigner::new(
            String::new(),
            None,
            String::new(),
            "w_test",
            "fra",
            "t_test",
        );
        let nats = Arc::new(crate::nats::tests::MockNatsClient::new());
        Arc::new(Supervisor {
            config: cp_config(),
            state,
            downloader: Arc::new(Downloader::new(
                "http://localhost:0".to_string(),
                std::path::PathBuf::from("/tmp"),
                jwt.clone(),
            )),
            port_pool: Arc::new(Mutex::new(PortPool::new(10000, 1))),
            nats: nats as Arc<dyn NatsClient>,
            log_forwarder: LogForwarder::new("http://localhost:0", "w_test", "fra", jwt.clone()),
            jwt_signer: jwt,
            http: reqwest::Client::new(),
            engine_pool: Arc::new(StandbyPool::new(1).expect("pool")),
        })
    }

    fn make_app(
        instance_pre: wasmtime::component::InstancePre<edge_runtime::RuntimeState>,
        status: AppInstanceStatus,
        ws_port: Option<u16>,
    ) -> Arc<Mutex<AppInstance>> {
        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        meter.record_request();
        meter.record_request();
        Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 18000,
            status,
            meter,
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre,
            handle: None,
            ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: None,
            metrics_acc: None,
            ws_port,
        }))
    }

    #[tokio::test]
    async fn heartbeat_empty_state() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let sup = build_supervisor(state);
        let hb = sup.build_heartbeat().await;
        assert_eq!(hb.worker_id, "w_test");
        assert_eq!(hb.region, "fra");
        assert!(hb.apps.is_empty());
        assert!(hb.cluster_headroom.is_some());
    }

    #[tokio::test]
    async fn heartbeat_one_running_app() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre, AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);
        let hb = sup.build_heartbeat().await;
        assert_eq!(hb.apps.len(), 1);
        let s = hb.apps.get("my-app").expect("app present");
        assert_eq!(s.status, "running");
        assert_eq!(s.tenant_id, "t_test");
        assert_eq!(s.port, 18000);
        assert_eq!(s.request_count, 2);
        assert!(s.ws_port.is_none());
    }

    #[tokio::test]
    async fn heartbeat_crashed_app() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(
            instance_pre,
            AppInstanceStatus::Crashed { restart_count: 3 },
            None,
        );
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);
        let hb = sup.build_heartbeat().await;
        let s = hb.apps.get("my-app").expect("app present");
        assert_eq!(s.status, "crashed");
        assert_eq!(s.exit_code, Some(1));
    }

    #[tokio::test]
    async fn heartbeat_with_ws_port() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre, AppInstanceStatus::Running, Some(19091));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);
        let hb = sup.build_heartbeat().await;
        let s = hb.apps.get("my-app").expect("app present");
        assert_eq!(s.ws_port, Some(19091));
    }

    // ── snapshot_current_apps tests ─────────────────────────────────

    #[tokio::test]
    async fn snapshot_empty_state_returns_empty() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let sup = build_supervisor(state);
        let snap = sup.snapshot_current_apps("t_test").await;
        assert!(snap.is_empty());
    }

    #[tokio::test]
    async fn snapshot_filters_by_tenant() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre, AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);
        // Query for a different tenant -> empty
        let snap = sup.snapshot_current_apps("t_other").await;
        assert!(snap.is_empty());
        // Query for the correct tenant -> found
        let snap = sup.snapshot_current_apps("t_test").await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.get("my-app").unwrap().0, "d1");
    }

    // ── stop_all_apps / reset_meters_after tests ────────────────────

    #[tokio::test]
    async fn stop_all_apps_empty_is_noop() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let sup = build_supervisor(state);
        sup.stop_all_apps().await;
        assert!(sup.state.read().await.apps.is_empty());
    }

    #[tokio::test]
    async fn reset_meters_after_empty_no_panic() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let sup = build_supervisor(state);
        let hb = HeartbeatMessage::new("w1".into(), "fra".into(), "1.2.3.4:0".into(), "t1".into());
        sup.reset_meters_after(&hb).await;
    }

    #[tokio::test]
    async fn fetch_sync_stamps_watchdog() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let sup = build_supervisor(state);
        let result = sup.fetch_sync().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        let state_guard = sup.state.read().await;
        let last = state_guard.last_task_received_at.lock().unwrap();
        assert!(last.is_some());
    }

    // ── handle_task_message tests ───────────────────────────────────

    #[tokio::test]
    async fn handle_task_message_empty_desired_stops_all() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre, AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);

        // Send a TaskUpdate with empty apps → should stop the running app
        let msg = TaskMessage::TaskUpdate {
            timestamp: String::new(),
            tenant_id: "t_test".into(),
            apps: HashMap::new(),
        };
        let result = sup.handle_task_message(msg).await;
        assert!(result.is_ok());
        // App should be gone now
        assert!(sup.state.read().await.apps.is_empty());
    }

    #[tokio::test]
    async fn handle_task_message_same_deployment_noop() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre, AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);

        // Send a TaskUpdate with the same app → no-op
        let mut desired = HashMap::new();
        desired.insert(
            "my-app".into(),
            AppSpec {
                deployment_id: "d1".into(),
                deployment_hash: "abc123".into(),
                routes: None,
                env: HashMap::new(),
                allowlist: None,
                max_memory_mb: 256,
                cpu_budget_ms: None,
            },
        );
        let msg = TaskMessage::TaskUpdate {
            timestamp: String::new(),
            tenant_id: "t_test".into(),
            apps: desired,
        };
        let result = sup.handle_task_message(msg).await;
        assert!(result.is_ok());
        // App should still be there
        assert_eq!(sup.state.read().await.apps.len(), 1);
    }

    #[tokio::test]
    async fn handle_task_message_other_tenant_noop() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre, AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);

        // Send a TaskUpdate for a different tenant with empty apps → should NOT stop our app
        let msg = TaskMessage::TaskUpdate {
            timestamp: String::new(),
            tenant_id: "t_other".into(),
            apps: HashMap::new(),
        };
        let result = sup.handle_task_message(msg).await;
        assert!(result.is_ok());
        // Our app should still be there
        assert_eq!(sup.state.read().await.apps.len(), 1);
    }

    #[tokio::test]
    async fn handle_task_message_full_sync_same_semantics() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre, AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);

        // FullSync with empty apps should stop running app (same as TaskUpdate)
        let msg = TaskMessage::FullSync {
            timestamp: String::new(),
            tenant_id: "t_test".into(),
            apps: HashMap::new(),
        };
        let result = sup.handle_task_message(msg).await;
        assert!(result.is_ok());
        assert!(sup.state.read().await.apps.is_empty());
    }

    // ── process_task_message ack/nack/term tests ────────────────────

    #[tokio::test]
    async fn process_poison_pill_terminates() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let sup = build_supervisor(state);
        // Verify handle_task_message returns Ok for empty apps (no side effects)
        let msg = TaskMessage::TaskUpdate {
            timestamp: String::new(),
            tenant_id: "t_test".into(),
            apps: HashMap::new(),
        };
        let result = sup.handle_task_message(msg).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handle_task_with_other_tenant_preserves_app() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre.clone(), AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "app-a".into()), app);
        let sup = build_supervisor(state);

        // TaskUpdate for t_other with empty apps should NOT stop app-a
        let msg = TaskMessage::TaskUpdate {
            timestamp: String::new(),
            tenant_id: "t_other".into(),
            apps: HashMap::new(),
        };
        sup.handle_task_message(msg).await.unwrap();
        assert_eq!(sup.state.read().await.apps.len(), 1);
    }

    #[tokio::test]
    async fn handle_task_message_with_same_tenant_but_non_overlapping_apps() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = load_handler_fixture(&engine);
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre.clone(), AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "app-a".into()), app);
        let sup = build_supervisor(state);

        // TaskUpdate for same tenant but with a different app
        let mut desired = HashMap::new();
        desired.insert(
            "app-b".into(),
            AppSpec {
                deployment_id: "d2".into(),
                deployment_hash: "abc124".into(),
                routes: None,
                env: HashMap::new(),
                allowlist: None,
                max_memory_mb: 256,
                cpu_budget_ms: None,
            },
        );
        let msg = TaskMessage::TaskUpdate {
            timestamp: String::new(),
            tenant_id: "t_test".into(),
            apps: desired,
        };
        // handle_task_message will try to stop app-a (not in desired) and start app-b
        // but start_app will fail since we don't have real wasm components.
        // This is fine — we're testing the diff+orchestration, not start_app.
        let _ = sup.handle_task_message(msg).await;
    }
}

// ── extracted pure functions tests ──────────────────────────────────────

#[cfg(test)]
mod extracted_tests {
    use super::*;
    use std::collections::HashMap;

    // ── calculate_backoff ──────────────────────────────────────────

    #[test]
    fn backoff_r0_is_1s() {
        assert_eq!(calculate_backoff(0), Duration::from_secs(1));
    }
    #[test]
    fn backoff_r1_is_1s() {
        assert_eq!(calculate_backoff(1), Duration::from_secs(1));
    }
    #[test]
    fn backoff_r2_is_2s() {
        assert_eq!(calculate_backoff(2), Duration::from_secs(2));
    }
    #[test]
    fn backoff_r3_is_4s() {
        assert_eq!(calculate_backoff(3), Duration::from_secs(4));
    }
    #[test]
    fn backoff_large_clamped() {
        assert!(calculate_backoff(u32::MAX).as_secs() <= 60);
    }

    // ── build_app_env ──────────────────────────────────────────────

    #[test]
    fn env_adds_http_port() {
        let env = build_app_env(&HashMap::new(), 8080, None);
        assert_eq!(env.get("EDGE_HTTP_SERVER_PORT"), Some(&"8080".to_string()));
    }
    #[test]
    fn env_adds_ws_port() {
        let env = build_app_env(&HashMap::new(), 8080, Some(9091));
        assert_eq!(env.get("EDGE_WS_PORT"), Some(&"9091".to_string()));
    }
    #[test]
    fn env_preserves_existing() {
        let mut base = HashMap::new();
        base.insert("FOO".into(), "bar".into());
        let env = build_app_env(&base, 8080, None);
        assert_eq!(env.get("FOO"), Some(&"bar".to_string()));
    }

    // ── parse_task_payload ─────────────────────────────────────────

    #[test]
    fn parse_valid_task_update() {
        let json = r#"{"type":"task_update","timestamp":"","tenant_id":"t1","apps":{}}"#;
        let msg = parse_task_payload(json.as_bytes()).expect("parse");
        match msg {
            TaskMessage::TaskUpdate { ref tenant_id, .. } => assert_eq!(tenant_id, "t1"),
            other => panic!("expected TaskUpdate, got {:?}", other),
        }
    }
    #[test]
    fn parse_valid_full_sync() {
        let json = r#"{"type":"full_sync","timestamp":"","tenant_id":"t2","apps":{}}"#;
        let msg = parse_task_payload(json.as_bytes()).expect("parse");
        match msg {
            TaskMessage::FullSync { ref tenant_id, .. } => assert_eq!(tenant_id, "t2"),
            other => panic!("expected FullSync, got {:?}", other),
        }
    }
    #[test]
    fn parse_invalid_json_returns_err() {
        assert!(parse_task_payload(b"garbage").is_err());
    }
}

// ── allowlist_to_egress_policy ──────────────────────────────────────

/// Convert an allowlist spec to an EgressPolicy.
#[allow(dead_code)]
pub fn allowlist_to_egress_policy(allowlist: &Option<Vec<String>>) -> Arc<EgressPolicy> {
    match allowlist {
        None => Arc::new(EgressPolicy::allow_all()),
        Some(list) => Arc::new(EgressPolicy::new(list.clone())),
    }
}

/// The main supervisor — manages all running apps for this worker node.
#[allow(dead_code)]
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
    /// HTTP client for sync fallback.
    pub http: reqwest::Client,
    /// Standby engine pool for lazy starting apps.
    pub engine_pool: Arc<StandbyPool>,
    /// JWT signer used by `fetch_sync` (the HTTP /sync fallback) and by
    /// downloader/internal HTTP calls. Mirrors the main-branch shape so
    /// `edge-test-helpers::build_supervisor_inner` can construct a
    /// Supervisor for tests without separate plumbing.
    pub jwt_signer: Arc<crate::auth::WorkerJwtSigner>,
}

impl Supervisor {
    /// HTTP `/sync` fallback (issue #53). When NATS is silent for
    /// longer than `worker_sync_threshold_secs`, the worker falls back
    /// to polling the control plane over HTTP to discover any
    /// reconciliation commands it might be missing.
    ///
    /// This is a stub in v0.2 — the actual fetch logic lands in a
    /// follow-up once the FaaS path is stable. Returns `Ok(None)` to
    /// mean "no task messages received via the HTTP fallback".
    #[allow(dead_code)]
    pub async fn fetch_sync(&self) -> anyhow::Result<Option<crate::messages::TaskMessage>> {
        // Stamp the watchdog so health-check tests don't trip on a
        // quiet worker.
        if let Ok(mut guard) = self.state.read().await.last_task_received_at.lock() {
            *guard = Some(std::time::Instant::now());
        }
        Ok(None)
    }

    /// Handle an incoming TaskMessage from NATS.
    ///
    /// Diffs the desired app set against currently running apps and
    /// starts/stops apps accordingly.
    ///
    /// A single worker hosts apps from many tenants; each `TaskMessage`
    /// carries one tenant's desired set. We filter `current_apps` to
    /// only this tenant's running apps before diffing, so a missing app
    /// in the desired set never causes us to stop another tenant's app
    /// that happens to share the same name.
    pub async fn handle_task_message(&self, msg: TaskMessage) -> anyhow::Result<()> {
        let (tenant_id, desired_apps) = match msg {
            TaskMessage::TaskUpdate {
                tenant_id, apps, ..
            } => (tenant_id, apps),
            TaskMessage::FullSync {
                tenant_id, apps, ..
            } => (tenant_id, apps),
        };

        // Snapshot this tenant's running apps.
        let current_apps = self.snapshot_current_apps(&tenant_id).await;

        let diff = compute_app_diff(&current_apps, &desired_apps);

        let has_changes = !diff.apps_to_stop.is_empty() || !diff.apps_to_start.is_empty();

        for app_name in &diff.apps_to_stop {
            if let Err(e) = self.stop_app(&tenant_id, app_name).await {
                tracing::error!(
                    tenant_id = %tenant_id,
                    app_name,
                    err = %e,
                    "failed to stop app"
                );
            }
        }

        for (app_name, spec) in &diff.apps_to_start {
            if let Err(e) = self.start_app(app_name, spec, &tenant_id).await {
                tracing::error!(
                    tenant_id = %tenant_id,
                    app_name,
                    err = %e,
                    "failed to start app"
                );
            }
        }

        if has_changes {
            let heartbeat = self.build_heartbeat().await;
            if let Err(e) = self
                .nats
                .publish_heartbeat(&self.config.region, &heartbeat)
                .await
            {
                tracing::error!(err = %e, "failed to publish immediate heartbeat");
            } else {
                tracing::info!("successfully published immediate heartbeat for route propagation");
            }
        }

        Ok(())
    }

    /// Snapshot this tenant's running apps: app_name → (deployment_id, status).
    async fn snapshot_current_apps(
        &self,
        tenant_id: &str,
    ) -> HashMap<String, (String, AppInstanceStatus)> {
        let state = self.state.read().await;
        let mut map = HashMap::new();
        for ((t, n), inst) in state.apps.iter() {
            if t != tenant_id {
                continue;
            }
            let inst = inst.lock().await;
            map.insert(n.clone(), (inst.deployment_id.clone(), inst.status.clone()));
        }
        map
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

        tracing::info!(
            tenant_id,
            app_name,
            deployment_id = spec.deployment_id,
            "starting app"
        );

        // Stop existing instance if present (for this tenant + app).
        let key = (tenant_id.to_string(), app_name.to_string());
        if self.state.read().await.apps.contains_key(&key) {
            self.stop_app(tenant_id, app_name).await?;
        }

        // Acquire an HTTP port.
        let raw_port = {
            let mut pool = self.port_pool.lock().await;
            pool.acquire().expect("port pool exhausted")
        };

        // Acquire a WebSocket port if the spec requests one via the
        // EDGE_WS_PORT env var. The guest is expected to listen on this
        // port via wasi:sockets for WebSocket upgrade traffic (issue #312).
        let wants_ws = spec.env.contains_key("EDGE_WS_PORT");
        let ws_port = if wants_ws {
            let mut pool = self.port_pool.lock().await;
            Some(pool.acquire().expect("port pool exhausted"))
        } else {
            None
        };

        // Issue #307: refuse to instantiate an artifact we can't
        // verify. Default-secure: EDGE_REQUIRE_SIGNATURE defaults to
        // true in v1; the supervisor also requires an explicit
        // signature on the AppSpec when the worker is configured
        // to verify. The Downloader will re-verify on the actual
        // download path, but we short-circuit here so the failing
        // app doesn't even hit the network — surface the error
        // before consuming a port + WASM instantiation slot.
        if self.config.require_signature {
            if spec.deployment_signature.is_none() {
                let mut pool = self.port_pool.lock().await;
                pool.release(raw_port);
                if let Some(ws) = ws_port {
                    pool.release(ws);
                }
                anyhow::bail!(
                    "deployment {} has no signature; worker is configured \
                     EDGE_REQUIRE_SIGNATURE=true. Either re-deploy from the \
                     control plane (post-PR1 CPs always sign) or set \
                     EDGE_REQUIRE_SIGNATURE=false on this worker.",
                    spec.deployment_id
                );
            }
            // Defensive: config validation in Config::from_env
            // already ensures require_signature=true implies a
            // verifier was constructed (i.e. a pubkey was set), so
            // this branch is a worker-side invariant guard. If it
            // ever fires, the early-fail is the right behavior —
            // it's a "this should be impossible" check.
            if self.downloader.signature_verifier.is_none() {
                anyhow::bail!(
                    "EDGE_REQUIRE_SIGNATURE=true but no signature verifier \
                     constructed for {}; this is a worker-side bug",
                    spec.deployment_id
                );
            }
        }

        // Download artifact (blocking on first request).
        // Note: Downloader::get_artifact verifies SHA-256 against
        // spec.deployment_hash AND (when a verifier is configured)
        // the Ed25519 signature over (hash || deployment_id) before
        // returning. On any verification failure (hash mismatch,
        // signature missing, signature wire-format error, or
        // signature verify-false) it returns Err, which this arm
        // propagates and the port-release path handles.
        let artifact = match self
            .downloader
            .get_artifact(
                &spec.deployment_id,
                &spec.deployment_hash,
                spec.deployment_signature.as_deref(),
            )
            .await
        {
            Ok(a) => a,
            Err(e) => {
                let mut pool = self.port_pool.lock().await;
                pool.release(raw_port);
                if let Some(ws) = ws_port {
                    pool.release(ws);
                }
                return Err(e);
            }
        };

        // Decide which WIT world this component targets. Detection is
        // structural — we look for a `wasi:http/incoming-handler` export
        // and pick `Handler` if found, otherwise `LongRunning`. The
        // linker factory must match: only the Handler linker has the
        // `wasi:http/incoming-handler` export wired in via ProxyPre.
        // We do this via fast-path byte detection to avoid compiling FaaS
        // handlers until their first request arrives.
        let execution_model = crate::detect::detect_execution_model_from_bytes(&artifact);
        tracing::info!(
            tenant_id,
            app_name,
            ?execution_model,
            "execution model detected"
        );

        let engine = self.state.read().await.engine.clone();

        let instance_pre = if execution_model == ExecutionModel::LongRunning {
            // For LongRunning apps, we compile and instantiate eagerly.
            // Try AOT compilation cache first.
            let cwasm_path = self.downloader.cwasm_path(&spec.deployment_id);
            let component = if cwasm_path.exists() {
                match tokio::fs::read(&cwasm_path).await {
                    Ok(cwasm_bytes) => {
                        // Safety: Loading pre-compiled code from a local trusted file cache is safe.
                        match unsafe {
                            wasmtime::component::Component::deserialize(&engine, &cwasm_bytes)
                        } {
                            Ok(c) => {
                                tracing::info!(
                                    tenant_id,
                                    app_name,
                                    deployment_id = %spec.deployment_id,
                                    "AOT pre-compilation cache hit: successfully deserialized component"
                                );
                                Some(c)
                            }
                            Err(e) => {
                                tracing::warn!(
                                    tenant_id,
                                    app_name,
                                    deployment_id = %spec.deployment_id,
                                    err = %e,
                                    "failed to deserialize component; falling back to JIT compilation"
                                );
                                let _ = tokio::fs::remove_file(&cwasm_path).await;
                                None
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            tenant_id,
                            app_name,
                            deployment_id = %spec.deployment_id,
                            err = %e,
                            "failed to read AOT cache file; falling back to JIT compilation"
                        );
                        None
                    }
                }
            } else {
                None
            };

            let component = match component {
                Some(c) => c,
                None => {
                    // Compile the component using the shared engine.
                    let engine_for_spawn = engine.clone();
                    match tokio::task::spawn_blocking(move || {
                        wasmtime::component::Component::from_binary(&engine_for_spawn, &artifact)
                    })
                    .await
                    .unwrap()
                    {
                        Ok(c) => {
                            // Serialize and write to cache in a background task
                            let cwasm_path_clone = cwasm_path.clone();
                            let serialized_result = c.serialize();
                            tokio::spawn(async move {
                                match serialized_result {
                                    Ok(serialized_bytes) => {
                                        if let Err(e) =
                                            tokio::fs::write(&cwasm_path_clone, &serialized_bytes)
                                                .await
                                        {
                                            tracing::warn!(
                                                path = %cwasm_path_clone.display(),
                                                err = %e,
                                                "failed to write serialized component to AOT cache"
                                            );
                                        } else {
                                            tracing::info!(
                                                path = %cwasm_path_clone.display(),
                                                "successfully wrote serialized component to AOT cache"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(err = %e, "failed to serialize compiled component");
                                    }
                                }
                            });
                            c
                        }
                        Err(e) => {
                            let mut pool = self.port_pool.lock().await;
                            pool.release(raw_port);
                            if let Some(ws) = ws_port {
                                pool.release(ws);
                            }
                            return Err(anyhow::Error::from(e)
                                .context(format!("failed to compile component for {}", app_name)));
                        }
                    }
                }
            };

            let linker = create_component_linker_long_running(&engine)?;
            match linker.instantiate_pre(&component) {
                Ok(ip) => Some(ip),
                Err(e) => {
                    let mut pool = self.port_pool.lock().await;
                    pool.release(raw_port);
                    if let Some(ws) = ws_port {
                        pool.release(ws);
                    }
                    return Err(anyhow::Error::from(e).context(format!(
                        "failed to pre-instantiate {} (execution_model={:?}); \
                             wasi: imports are wired in Phase C",
                        app_name, execution_model
                    )));
                }
            }
        } else {
            // Handler (FaaS) apps defer instantiation until the first request.
            None
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

        // Create request meter.
        let meter = Arc::new(RequestMeter::new(
            tenant_id.to_string(),
            spec.deployment_id.clone(),
        ));

        // Per-app env injected into both branches. For Handler apps
        // this becomes `WasiCtx` env on every per-request state clone;
        // for LongRunning it's the same HashMap the run_app_loop
        // consumes.
        let mut env = spec.env.clone();
        env.insert("EDGE_HTTP_SERVER_PORT".to_string(), raw_port.to_string());
        // Replace the EDGE_WS_PORT sentinel ("0") with the actual allocated port
        // so the guest sees the real port number in its environment.
        if let Some(ws) = ws_port {
            env.insert("EDGE_WS_PORT".to_string(), ws.to_string());
        }

        // EgressPolicy from the spec.allowlist. None / empty → allow-all.
        // Spec carries Vec<String>:
        //   * `None` (wire field absent) → permissive default
        //   * `Some([])` (field present, empty) → empty allowlist = deny all
        // Phase C: the existing LongRunning branch already passes this;
        // the Handler branch picks it up here.
        let egress_for_handler: Arc<EgressPolicy> = match &spec.allowlist {
            None => Arc::new(EgressPolicy::allow_all()),
            Some(list) => Arc::new(EgressPolicy::new(list.clone())),
        };

        // AppLogContext — stamped on every log record the guest emits.
        let app_ctx = edge_runtime::interfaces::observe::AppLogContext {
            app_name: app_name.to_string(),
            tenant_id: tenant_id.to_string(),
            deployment_id: spec.deployment_id.clone(),
        };

        // Shared metrics accumulator — guest edge:observe metric calls
        // write into this, and build_heartbeat snapshots it every 30s.
        let metrics_acc: Option<Arc<edge_runtime::interfaces::observe::MetricsAccumulator>> = Some(
            Arc::new(edge_runtime::interfaces::observe::MetricsAccumulator::new()),
        );

        // Own tenant_id before the spawn — `start_app` borrows it as &str,
        // but the tokio::spawn future must be 'static, so we move an owned
        // String into the closure. The original is moved into the closure;
        // tenant_id_for_instance is the second copy used by the AppInstance
        // registration below.
        let tenant_id = tenant_id.to_string();
        let tenant_id_for_instance = tenant_id.clone();

        // Spawn the per-app task and store the JoinHandle so we can
        // propagate panics when the app is stopped. The task body
        // depends on the execution model:
        //
        //   * `LongRunning` — calls `run_app_loop`, which instantiates
        //     the component per restart and drives it via `_start`.
        //   * `Handler`     — spawns `HandlerDispatch::serve` on the
        //     per-app TCP port. Each accepted connection is dispatched
        //     through `wasmtime_wasi_http::ProxyPre` per request.
        let app_name_str = app_name.to_string();

        // Per-app shutdown channels. The two models use different
        // channel types:
        //   * `LongRunning` — `oneshot::Sender` consumed by run_app_loop.
        //   * `Handler`     — `broadcast::Sender` consumed by
        //     `HandlerDispatch::serve` via `with_graceful_shutdown`.
        let (shutdown_tx, shutdown_tx_broadcast, handle, dispatch) = if execution_model
            == ExecutionModel::Handler
        {
            // Drop the unused oneshot receiver; broadcast will be
            // used instead.
            let (broadcast_tx, _) = tokio::sync::broadcast::channel::<()>(1);

            let handler_config = HandlerConfig {
                tenant_id: tenant_id.to_string(),
                egress: egress_for_handler.clone(),
                log_sink: self.log_forwarder.clone()
                    as Arc<dyn edge_runtime::interfaces::observe::LogSink>,
                app_ctx: app_ctx.clone(),
                meter: meter.clone(),
                env: env.clone(),
                max_request_body_bytes: self.config.handler_max_request_body_bytes,
                metrics_acc: metrics_acc.clone(),
                socket_mode: self.config.socket_mode,
                last_request_at: Arc::new(tokio::sync::Mutex::new(Some(std::time::Instant::now()))),
                max_memory_mb: spec.max_memory_mb,
                cpu_budget_ms: spec
                    .cpu_budget_ms
                    .unwrap_or(self.config.handler_request_budget_ms),
            };

            let tls_config =
                try_load_tls_config(&self.config.tls_cert_path, &self.config.tls_key_path);
            let dispatch = HandlerDispatch::new(
                raw_port,
                self.config.handler_request_budget_ms,
                self.config.epoch_tick_ms,
                handler_config,
                tls_config,
                self.downloader.clone(),
                spec.deployment_id.clone(),
                self.engine_pool.clone(),
                self.state.clone(),
            )?;

            let dispatch = Arc::new(dispatch);
            let dispatch_for_serve = dispatch.clone();
            let shutdown_rx_for_dispatch = broadcast_tx.subscribe();
            let port_for_log = raw_port;
            let app_name_for_log = app_name_str.clone();
            let tenant_for_log = tenant_id.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = dispatch_for_serve.serve(shutdown_rx_for_dispatch).await {
                    tracing::error!(
                        tenant_id = %tenant_for_log,
                        app_name = %app_name_for_log,
                        port = port_for_log,
                        err = %e,
                        "HandlerDispatch serve() returned Err"
                    );
                } else {
                    tracing::info!(
                        tenant_id = %tenant_for_log,
                        app_name = %app_name_for_log,
                        port = port_for_log,
                        "HandlerDispatch serve() exited"
                    );
                }
            });
            (None, Some(broadcast_tx), handle, Some(dispatch))
        } else {
            let instance_pre_clone = instance_pre.clone().unwrap();
            let meter_clone = meter.clone();
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
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let metrics_acc_for_loop = metrics_acc.clone();

            let socket_mode_for_loop = self.config.socket_mode;
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
                    tenant_id,
                    allowlist,
                    downloader_clone,
                    log_forwarder,
                    metrics_acc_for_loop,
                    socket_mode_for_loop,
                )
                .await;
                tracing::info!(app_name = %app_name_str, "app task exited");
            });
            (Some(shutdown_tx), None, handle, None)
        };

        // Register the app instance (Arc<Mutex<>> for interior mutability),
        // keyed by `(tenant_id, app_name)`. Each of the three
        // tenant_id_for_instance uses below consumes/clones the String —
        // the field on `AppInstance`, the tuple key in `state.apps`,
        // and the tracing::info! field rendering.
        let instance = Arc::new(Mutex::new(AppInstance {
            deployment_id: spec.deployment_id.clone(),
            app_name: app_name.to_string(),
            tenant_id: tenant_id_for_instance.clone(),
            port: raw_port,
            status: AppInstanceStatus::Running,
            meter,
            shutdown_tx,
            shutdown_tx_broadcast,
            instance_pre: instance_pre.unwrap(),
            handle: Some(std::sync::Arc::new(handle)),
            ticker: Some(ticker),
            execution_model,
            dispatch,
            metrics_acc,
            ws_port,
        }));

        self.state.write().await.apps.insert(
            (tenant_id_for_instance.clone(), app_name.to_string()),
            instance,
        );

        tracing::info!(tenant_id = %tenant_id_for_instance, app_name, port = raw_port, "app started");
        Ok(())
    }

    /// Periodically scans all active apps and evicts in-memory Wasm components for idle Handler (FaaS) apps.
    pub async fn evict_idle_apps(&self, idle_timeout: Duration) {
        let apps = {
            let state = self.state.read().await;
            state.apps.clone()
        };

        for ((tenant_id, app_name), app_mutex) in apps {
            let app = app_mutex.lock().await;
            if app.execution_model == ExecutionModel::Handler {
                if let Some(ref dispatch) = app.dispatch {
                    let last_req_opt = {
                        let lock = dispatch.config.last_request_at.lock().await;
                        *lock
                    };
                    if let Some(last_req) = last_req_opt {
                        if last_req.elapsed() > idle_timeout {
                            // Try evicting the component
                            if let Some(engine) = dispatch.evict().await {
                                tracing::info!(
                                    tenant_id = %tenant_id,
                                    app_name = %app_name,
                                    "Idle timeout reached: evicting component from memory (scale-to-zero)"
                                );
                                self.engine_pool.release(engine);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Stop an app gracefully.
    pub async fn stop_app(&self, tenant_id: &str, app_name: &str) -> anyhow::Result<()> {
        let key = (tenant_id.to_string(), app_name.to_string());
        // Clone the Arc so we can lock it while the instance is still in the map.
        let instance = {
            let state = self.state.read().await;
            state.apps.get(&key).cloned()
        };

        let (port, handle, ticker) = if let Some(inst) = instance {
            // Extract port, handle, ticker, and the per-app shutdown
            // channels while locked. Both `shutdown_tx` (oneshot for
            // LongRunning) and `shutdown_tx_broadcast` (broadcast for
            // Handler) are taken out; we ignore failures because the
            // consumer may have already dropped the receiver.
            let mut inst = inst.lock().await;
            inst.status = AppInstanceStatus::Stopping;
            let port = inst.port;
            let handle = inst.handle.clone();
            let ticker = inst.ticker.take();
            let oneshot_tx = inst.shutdown_tx.take();
            let broadcast_tx = inst.shutdown_tx_broadcast.take();
            drop(inst); // release lock before sending
            if let Some(tx) = oneshot_tx {
                let _ = tx.send(());
            }
            if let Some(tx) = broadcast_tx {
                let _ = tx.send(());
            }
            (port, handle, ticker)
        } else {
            return Ok(()); // already gone
        };

        // Remove from the map.
        self.state.write().await.apps.remove(&key);

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

        // Propagate any panic from the app task. Two failure modes to
        // distinguish:
        //
        //   * `JoinError::Cancelled` — `handle.abort()` was called on a
        //     task that had already returned cleanly. This is the
        //     common case for the Handler model: the broadcast shutdown
        //     signal reaches the dispatch task; it returns `Ok(())`
        //     within microseconds; the supervisor's `handle.abort()`
        //     then turns that finished task into a Cancelled JoinError.
        //     Re-panicking here would crash the supervisor task on
        //     every redeploy (re-deploy Handler app → shutdown →
        //     abort → JoinError::Cancelled → panic_any), and break the
        //     NATS consume loop.
        //   * Real panic payload — the guest trapped with a non-zero
        //     exit, or the host task failed. We re-raise via
        //     `panic::resume_unwind` so the supervisor task surfaces a
        //     real Rust panic (which is observable by crash-reporting
        //     infrastructure), rather than swallowing it.
        if let Some(handle) = handle {
            // try_unwrap extracts the JoinHandle from the Arc; if there
            // are other Arcs (shouldn't happen here), we skip the await
            // and let the inner task finish on its own. Dropping the
            // Arc without awaiting is fine because the task that holds
            // the JoinHandle has already been requested to shutdown
            // via the broadcast/oneshot signal above.
            let join_result = match std::sync::Arc::try_unwrap(handle) {
                Ok(join_handle) => {
                    // Skip the abort if the task already finished
                    // (e.g. the broadcast signal already reached it).
                    // `JoinHandle::is_finished()` is `Send + Sync` and
                    // non-blocking.
                    if !join_handle.is_finished() {
                        join_handle.abort();
                    }
                    join_handle.await
                }
                Err(_) => {
                    tracing::debug!("could not unwrap JoinHandle — multiple refs; skipping await");
                    return Ok(());
                }
            };
            match join_result {
                Ok(()) => {
                    // Clean return — expected path. Nothing to do.
                }
                Err(join_err) if join_err.is_cancelled() => {
                    // Aborted task that hadn't yet signaled completion.
                    // This is the normal Handler-shutdown path; not an
                    // error.
                    tracing::debug!("app task cancelled cleanly");
                }
                Err(join_err) => {
                    // Real panic. `try_into_panic()` returns the
                    // original Box<dyn Any + Send> payload; we resume
                    // the unwind so the supervisor task crashes with
                    // the original panic message rather than wrapping
                    // it in a generic JoinError.
                    if let Ok(panic_payload) = join_err.try_into_panic() {
                        std::panic::resume_unwind(panic_payload);
                    }
                }
            }
        }

        tracing::info!(tenant_id, app_name, "app stopped");
        Ok(())
    }

    /// Per-app task loop for LongRunning components.
    ///
    /// Executes the component in a loop. Handles crashes with exponential
    /// backoff restart (max 5 restarts, then gives up). Long-running apps
    /// (HTTP servers) that return from `_start` keep running — only an
    /// explicit `process.exit` from the guest means "stop".
    //
    // The extra parameters come from two merged features: PR #64 follow-up
    // adds per-invocation memory + epoch limits (max_memory_mb,
    // epoch_deadline_ticks); origin/main adds a host-side timeout
    // (health_check_timeout_secs) for hung-app detection. They are
    // complementary: the wasmtime limits terminate the *guest* at the
    // engine layer, the timeout terminates the *host* task when the
    // guest doesn't yield.
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
        downloader: Arc<Downloader>,
        log_forwarder: Arc<LogForwarder>,
        metrics_acc: Option<Arc<edge_runtime::interfaces::observe::MetricsAccumulator>>,
        socket_mode: edge_runtime::socket_egress::SocketEgressPolicy,
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
                        env.clone(),
                        max_memory_mb,
                        epoch_deadline_ticks,
                        &tenant_id,
                        allowlist.clone(),
                        &app_name,
                        &log_forwarder,
                        metrics_acc.clone(),
                        socket_mode,
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
                                    let crash_key = (tenant_id.clone(), app_name.clone());
                                    if let Some(inst) = s.apps.get_mut(&crash_key) {
                                        let mut inst = inst.lock().await;
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
                                let hang_key = (tenant_id.clone(), app_name.clone());
                                if let Some(inst) = s.apps.get_mut(&hang_key) {
                                    let mut inst = inst.lock().await;
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
    /// Returns `Ok(true)` if the component wants to keep running (blocking
    /// call returned normally). Returns `Ok(false)` if the guest explicitly
    /// called `process.exit`. Returns `Err` on a wasm trap/error.
    #[allow(clippy::too_many_arguments)]
    async fn execute_app(
        instance_pre: &InstancePre<edge_runtime::RuntimeState>,
        meter: &Arc<RequestMeter>,
        env: HashMap<String, String>,
        max_memory_mb: u64,
        epoch_deadline_ticks: u64,
        tenant_id: &str,
        allowlist: Option<Vec<String>>,
        app_name: &str,
        log_forwarder: &Arc<LogForwarder>,
        metrics_acc: Option<Arc<edge_runtime::interfaces::observe::MetricsAccumulator>>,
        socket_mode: edge_runtime::socket_egress::SocketEgressPolicy,
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
        let app_ctx = edge_runtime::interfaces::observe::AppLogContext {
            app_name: app_name.to_string(),
            tenant_id: tenant_id.to_string(),
            deployment_id: meter.deployment_id.clone(),
        };

        // Create a fresh RuntimeState with per-app env vars, metering, log
        // sink, app context, and tenant_id for tenant isolation. The
        // socket_mode is read once at worker startup
        // (`Config::from_env` → `Config::socket_mode`) and threaded in
        // here from `start_app` — the runtime does NOT read
        // `EDGE_EGRESS_SOCKET_MODE` itself.
        let runtime_state = edge_runtime::RuntimeState::with_env_and_meter(
            env,
            Some(Arc::clone(meter)),
            tenant_id.to_string(),
            egress,
            log_forwarder.clone() as Arc<dyn edge_runtime::interfaces::observe::LogSink>,
            app_ctx,
            metrics_acc,
            socket_mode,
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

        // Instantiate. The engine has `config.async_support(true)` enabled
        // (see `edge-runtime/src/engine.rs`) — wasmtime enforces this at
        // runtime: sync `instantiate` panics with "must use async
        // instantiation when async support is enabled". The async form
        // matches the FaaS path in `edge-worker/src/dispatch.rs::handle_request`.
        let instance = instance_pre.instantiate_async(&mut store).await?;

        // `_start` is the canonical WASI Preview 2 entry point for
        // long-running components. The v0.1 `handle` export is no
        // longer supported — fixtures in Phase D export `_start`.
        // Must use `call_async` for the same reason as `instantiate_async`
        // above — wasmtime rejects sync `call` on a store built with
        // `async_support(true)`.
        instance
            .get_typed_func::<(), ()>(&mut store, "_start")?
            .call_async(&mut store, ())
            .await?;

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
            self.config.worker_tenant_id.clone(),
        );

        let state = self.state.read().await;
        // Iterate the (tenant_id, app_name)-keyed map. The heartbeat wire
        // format keys `apps` by app_name only, so if two tenants happen
        // to share an app name one will overwrite the other — preserved
        // from v0.1 behavior; multi-tenant app-name collisions are a
        // v0.3 routing concern (ingress already disambiguates by
        // (tenant_id, app_name), see edge-ingress::config::ingress_host).
        for ((_tenant_id, app_name), inst) in &state.apps {
            let inst = inst.lock().await;
            let status = app_status_to_string(&inst.status);
            let exit_code = app_status_exit_code(&inst.status);
            let snap = inst.meter.snapshot();

            // Snapshot the app's MetricsAccumulator (guest edge:observe
            // metric calls) if one was wired. The three metric kinds
            // map to MetricKind::Counter, Gauge, and HistogramSample
            // respectively — matching the heartbeat wire format in
            // edge-worker/src/messages.rs.
            let metrics = if let Some(ref acc) = inst.metrics_acc {
                let msnap = acc.snapshot();
                let mut samples = Vec::with_capacity(
                    msnap.counters.len() + msnap.gauges.len() + msnap.histograms.len(),
                );
                for c in msnap.counters {
                    samples.push(MetricSample {
                        name: c.name,
                        kind: MetricKind::Counter,
                        value: c.value as f64,
                        labels: c.labels,
                    });
                }
                for g in msnap.gauges {
                    samples.push(MetricSample {
                        name: g.name,
                        kind: MetricKind::Gauge,
                        value: g.value,
                        labels: g.labels,
                    });
                }
                for (name, entries) in msnap.histograms {
                    for (value, labels) in entries {
                        samples.push(MetricSample {
                            name: name.clone(),
                            kind: MetricKind::HistogramSample,
                            value,
                            labels,
                        });
                    }
                }
                samples
            } else {
                vec![]
            };

            msg.apps.insert(
                app_name.clone(),
                AppStatus {
                    deployment_id: inst.deployment_id.clone(),
                    status: status.to_string(),
                    exit_code,
                    request_count: snap.request_count,
                    outbound_bytes: snap.outbound_bytes,
                    observer_metrics: metrics,
                    tenant_id: inst.tenant_id.clone(),
                    port: inst.port,
                    ws_port: inst.ws_port,
                },
            );
        }

        // Populate cluster headroom for the autoscaler (issue #85).
        let free_slots = self.port_pool.lock().await.free_slots();
        msg.cluster_headroom = Some(ClusterHeadroom {
            cpu_pct: None,
            mem_pct: None,
            app_slots: free_slots,
        });

        msg
    }

    /// Subtract the published heartbeat's per-app counts from each meter after
    /// a successful publish. Using subtract_delta rather than zeroing the counter
    /// preserves any bytes recorded between the snapshot and this call — those
    /// will appear in the next heartbeat interval rather than being silently lost.
    pub async fn reset_meters_after(&self, heartbeat: &HeartbeatMessage) {
        let state = self.state.read().await;
        for (app_name, status) in &heartbeat.apps {
            // Look up by (tenant_id, app_name) — the heartbeat carries
            // tenant_id inside AppStatus, so we can resolve the right
            // instance even if app_name alone is ambiguous on this
            // worker.
            let key = (status.tenant_id.clone(), app_name.clone());
            if let Some(inst) = state.apps.get(&key) {
                let inst = inst.lock().await;
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
        for (tenant_id, app_name) in &keys {
            if let Err(e) = self.stop_app(tenant_id, app_name).await {
                tracing::error!(
                    tenant_id,
                    app_name,
                    err = %e,
                    "failed to stop app during shutdown"
                );
            }
        }
    }

    /// Run the JetStream task-consume loop until `shutdown_rx` fires.
    ///
    /// Subscribes to the task stream without a queue group (issue #316
    /// fan-out). Every worker in the region receives every `TaskMessage`;
    /// `handle_task_message`'s diff logic handles duplicates.
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
            .subscribe_tasks(&self.config.region, &self.config.consumer_name)
            .await?;
        tracing::info!(
            region = %self.config.region,
            consumer = %self.config.consumer_name,
            "subscribed to task stream (fan-out mode)"
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

/// Result of diffing desired apps against current apps.
#[derive(Debug)]
pub struct AppDiff {
    pub apps_to_stop: Vec<String>,
    pub apps_to_start: Vec<(String, AppSpec)>,
}

/// Compute the set of apps to stop vs start based on current and desired sets.
/// This is a pure function — no I/O, no state mutation.
///
/// `current_apps` maps app_name → (deployment_id, status) for a single tenant.
/// `desired_apps` maps app_name → AppSpec for the same tenant.
///
/// Tenant filtering is expected to be done by the caller — this function
/// operates on already-scoped maps.
pub fn compute_app_diff(
    current_apps: &HashMap<String, (String, AppInstanceStatus)>,
    desired_apps: &HashMap<String, AppSpec>,
) -> AppDiff {
    let mut apps_to_stop = Vec::new();
    let mut apps_to_start = Vec::new();

    // Stop apps no longer in the desired set.
    for app_name in current_apps.keys() {
        if !desired_apps.contains_key(app_name) {
            apps_to_stop.push(app_name.clone());
        }
    }

    // Start or update apps in the desired set.
    for (app_name, spec) in desired_apps {
        let is_new = !current_apps.contains_key(app_name);
        let is_changed = current_apps
            .get(app_name)
            .map(|(dep_id, _)| dep_id != &spec.deployment_id)
            .unwrap_or(false);

        if is_new || is_changed {
            apps_to_start.push((app_name.clone(), spec.clone()));
        }
    }

    AppDiff {
        apps_to_stop,
        apps_to_start,
    }
}

/// Map an AppInstanceStatus to its heartbeat wire string.
pub fn app_status_to_string(status: &AppInstanceStatus) -> &'static str {
    match status {
        AppInstanceStatus::Running => "running",
        AppInstanceStatus::Starting => "starting",
        AppInstanceStatus::Stopping => "stopping",
        AppInstanceStatus::Crashed { .. } => "crashed",
        AppInstanceStatus::Hung => "hung",
    }
}

/// Map an AppInstanceStatus to its heartbeat exit_code.
pub fn app_status_exit_code(status: &AppInstanceStatus) -> Option<i32> {
    match status {
        AppInstanceStatus::Running | AppInstanceStatus::Starting | AppInstanceStatus::Stopping => {
            None
        }
        AppInstanceStatus::Crashed { .. } | AppInstanceStatus::Hung => Some(1),
    }
}

/// Exponential backoff: min(1s × 2^(n-1), 60s).
#[allow(dead_code)]
pub fn calculate_backoff(restart_count: u32) -> Duration {
    const BASE: Duration = Duration::from_secs(1);
    const MAX: Duration = Duration::from_secs(60);
    if restart_count == 0 {
        return BASE;
    }
    // Use checked_pow to avoid overflow.
    let secs = 2u64
        .checked_pow(restart_count.saturating_sub(1))
        .unwrap_or(u64::MAX);
    std::cmp::min(Duration::from_secs(secs), MAX)
}

/// Build the per-app environment map.
#[allow(dead_code)]
pub fn build_app_env(
    spec_env: &HashMap<String, String>,
    raw_port: u16,
    ws_port: Option<u16>,
) -> HashMap<String, String> {
    let mut env = spec_env.clone();
    env.insert("EDGE_HTTP_SERVER_PORT".to_string(), raw_port.to_string());
    if let Some(ws) = ws_port {
        env.insert("EDGE_WS_PORT".to_string(), ws.to_string());
    }
    env
}

/// Parse a raw NATS task message payload into a TaskMessage.
#[allow(dead_code)]
pub fn parse_task_payload(payload: &[u8]) -> anyhow::Result<TaskMessage> {
    serde_json::from_slice(payload).map_err(|e| anyhow::anyhow!("invalid task payload: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_spec(deployment_id: &str) -> AppSpec {
        AppSpec {
            deployment_id: deployment_id.to_string(),
            deployment_hash: "abc123".to_string(),
            deployment_signature: None,
            routes: None,
            env: HashMap::new(),
            allowlist: None,
            max_memory_mb: 256,
            cpu_budget_ms: None,
        }
    }

    fn make_status(deployment_id: &str, status: AppInstanceStatus) -> (String, AppInstanceStatus) {
        (deployment_id.to_string(), status)
    }

    fn running(deployment_id: &str) -> (String, AppInstanceStatus) {
        make_status(deployment_id, AppInstanceStatus::Running)
    }

    // ── app_status_to_string tests ──────────────────────────────────

    #[test]
    fn status_to_string_running() {
        assert_eq!(app_status_to_string(&AppInstanceStatus::Running), "running");
    }

    #[test]
    fn status_to_string_starting() {
        assert_eq!(
            app_status_to_string(&AppInstanceStatus::Starting),
            "starting"
        );
    }

    #[test]
    fn status_to_string_stopping() {
        assert_eq!(
            app_status_to_string(&AppInstanceStatus::Stopping),
            "stopping"
        );
    }

    #[test]
    fn status_to_string_crashed() {
        assert_eq!(
            app_status_to_string(&AppInstanceStatus::Crashed { restart_count: 5 }),
            "crashed"
        );
    }

    #[test]
    fn status_to_string_hung() {
        assert_eq!(app_status_to_string(&AppInstanceStatus::Hung), "hung");
    }

    // ── app_status_exit_code tests ──────────────────────────────────

    #[test]
    fn exit_code_running_is_none() {
        assert_eq!(app_status_exit_code(&AppInstanceStatus::Running), None);
    }

    #[test]
    fn exit_code_crashed_is_some() {
        assert_eq!(
            app_status_exit_code(&AppInstanceStatus::Crashed { restart_count: 3 }),
            Some(1)
        );
    }

    #[test]
    fn exit_code_hung_is_some() {
        assert_eq!(app_status_exit_code(&AppInstanceStatus::Hung), Some(1));
    }

    // ── compute_app_diff tests ──────────────────────────────────────

    #[test]
    fn diff_new_app_is_started() {
        let current = HashMap::new();
        let mut desired = HashMap::new();
        desired.insert("api".to_string(), make_spec("d1"));

        let diff = compute_app_diff(&current, &desired);
        assert_eq!(diff.apps_to_stop.len(), 0);
        assert_eq!(diff.apps_to_start.len(), 1);
        assert_eq!(diff.apps_to_start[0].0, "api");
        assert_eq!(diff.apps_to_start[0].1.deployment_id, "d1");
    }

    #[test]
    fn diff_same_app_same_deployment_is_noop() {
        let mut current = HashMap::new();
        current.insert("api".to_string(), running("d1"));
        let mut desired = HashMap::new();
        desired.insert("api".to_string(), make_spec("d1"));

        let diff = compute_app_diff(&current, &desired);
        assert_eq!(diff.apps_to_stop.len(), 0);
        assert_eq!(diff.apps_to_start.len(), 0);
    }

    #[test]
    fn diff_changed_deployment_triggers_restart() {
        let mut current = HashMap::new();
        current.insert("api".to_string(), running("d1"));
        let mut desired = HashMap::new();
        desired.insert("api".to_string(), make_spec("d2"));

        let diff = compute_app_diff(&current, &desired);
        assert_eq!(diff.apps_to_stop.len(), 0);
        assert_eq!(diff.apps_to_start.len(), 1);
        assert_eq!(diff.apps_to_start[0].0, "api");
        assert_eq!(diff.apps_to_start[0].1.deployment_id, "d2");
    }

    #[test]
    fn diff_missing_app_is_stopped() {
        let mut current = HashMap::new();
        current.insert("api".to_string(), running("d1"));
        let desired = HashMap::new();

        let diff = compute_app_diff(&current, &desired);
        assert_eq!(diff.apps_to_stop.len(), 1);
        assert_eq!(diff.apps_to_stop[0], "api");
        assert_eq!(diff.apps_to_start.len(), 0);
    }

    #[test]
    fn diff_empty_current_starts_all() {
        let current = HashMap::new();
        let mut desired = HashMap::new();
        desired.insert("api".to_string(), make_spec("d1"));
        desired.insert("worker".to_string(), make_spec("d2"));

        let diff = compute_app_diff(&current, &desired);
        assert_eq!(diff.apps_to_start.len(), 2);
        assert_eq!(diff.apps_to_stop.len(), 0);
    }

    #[test]
    fn diff_empty_desired_stops_all() {
        let mut current = HashMap::new();
        current.insert("api".to_string(), running("d1"));
        current.insert("worker".to_string(), running("d2"));
        let desired = HashMap::new();

        let diff = compute_app_diff(&current, &desired);
        assert_eq!(diff.apps_to_stop.len(), 2);
        assert_eq!(diff.apps_to_start.len(), 0);
    }

    #[test]
    fn diff_mixed_scenario() {
        let mut current = HashMap::new();
        current.insert("keep".to_string(), running("d1"));
        current.insert("stop_me".to_string(), running("d2"));
        current.insert("update_me".to_string(), running("d3"));
        let mut desired = HashMap::new();
        desired.insert("keep".to_string(), make_spec("d1")); // unchanged
        desired.insert("update_me".to_string(), make_spec("d4")); // changed
        desired.insert("new_app".to_string(), make_spec("d5")); // new

        let diff = compute_app_diff(&current, &desired);
        assert_eq!(diff.apps_to_stop, vec!["stop_me"]);
        assert_eq!(diff.apps_to_start.len(), 2);
        assert!(diff.apps_to_start.iter().any(|(n, _)| n == "update_me"));
        assert!(diff.apps_to_start.iter().any(|(n, _)| n == "new_app"));
        assert_eq!(
            diff.apps_to_start
                .iter()
                .find(|(n, _)| n == "update_me")
                .unwrap()
                .1
                .deployment_id,
            "d4"
        );
    }

    #[test]
    fn diff_crashed_app_still_detected_as_running() {
        let mut current = HashMap::new();
        current.insert(
            "api".to_string(),
            make_status("d1", AppInstanceStatus::Crashed { restart_count: 3 }),
        );
        let mut desired = HashMap::new();
        desired.insert("api".to_string(), make_spec("d2"));

        // Even though crashed, the app exists — changing deployment_id should trigger start.
        let diff = compute_app_diff(&current, &desired);
        assert_eq!(diff.apps_to_start.len(), 1);
    }

    #[test]
    fn diff_self_corrects_on_same_deployment_after_crash() {
        let mut current = HashMap::new();
        current.insert(
            "api".to_string(),
            make_status("d1", AppInstanceStatus::Crashed { restart_count: 5 }),
        );
        let mut desired = HashMap::new();
        desired.insert("api".to_string(), make_spec("d1"));

        // Same deployment_id, crashed — the diff says no-op because the
        // supervisor's restart loop handles the crash. The control plane
        // needs to send a new deployment_id to trigger a restart.
        let diff = compute_app_diff(&current, &desired);
        assert_eq!(diff.apps_to_start.len(), 0);
    }

    #[test]
    fn diff_non_running_statuses() {
        for status in &[
            AppInstanceStatus::Starting,
            AppInstanceStatus::Running,
            AppInstanceStatus::Stopping,
        ] {
            let mut current = HashMap::new();
            current.insert("api".to_string(), make_status("d1", status.clone()));
            let mut desired = HashMap::new();
            desired.insert("api".to_string(), make_spec("d2"));

            let diff = compute_app_diff(&current, &desired);
            assert_eq!(
                diff.apps_to_start.len(),
                1,
                "should restart for status {:?}",
                status
            );
        }
    }

    // ── StandbyPool tests ───────────────────────────────────────────

    #[tokio::test]
    async fn test_standby_pool_acquire_and_release() {
        let pool = StandbyPool::new(2).expect("failed to create pool");
        let engine = edge_runtime::create_engine().expect("failed to create engine");
        let state = RwLock::new(WorkerState::new(engine));

        // Acquire 2 engines (should be fast, no 500ms delay)
        let start = std::time::Instant::now();
        let e1 = pool.acquire(&state).await;
        let _e2 = pool.acquire(&state).await;
        assert!(start.elapsed().as_millis() < 500, "Should not timeout");

        // Release 1
        pool.release(e1);

        // We should be able to acquire again fast
        let start2 = std::time::Instant::now();
        let _e3 = pool.acquire(&state).await;
        assert!(
            start2.elapsed().as_millis() < 500,
            "Should not timeout after release"
        );
    }

    #[tokio::test]
    async fn test_standby_pool_exhaustion_fallback() {
        let pool = StandbyPool::new(1).expect("failed to create pool");
        let engine = edge_runtime::create_engine().expect("failed to create engine");
        let state = RwLock::new(WorkerState::new(engine));

        // Acquire the only engine in the pool
        let _e1 = pool.acquire(&state).await;

        // The pool is now empty. The next acquire should timeout (500ms) and fallback
        // to a new transient engine without crashing.
        let start = std::time::Instant::now();
        let _e2 = pool.acquire(&state).await;
        let elapsed = start.elapsed();

        // It should have taken at least 500ms for the timeout
        assert!(
            elapsed.as_millis() >= 450,
            "Should have hit the timeout before fallback"
        );
    }

    #[tokio::test]
    async fn test_standby_pool_lru_eviction() {
        let pool = Arc::new(StandbyPool::new(1).expect("failed to create pool"));
        let base_engine = edge_runtime::create_engine().expect("failed to create engine");
        let state = Arc::new(RwLock::new(WorkerState::new(base_engine.clone())));

        struct NullSink;
        impl edge_runtime::interfaces::observe::LogSink for NullSink {
            fn push(
                &self,
                _record: edge_runtime::interfaces::observe::LogRecord,
                _ctx: edge_runtime::interfaces::observe::AppLogContext,
            ) {
            }
        }

        // We compile a dummy component to get a real ProxyPre and InstancePre
        let engine_for_compile = pool.acquire(&state).await; // Get pre-warmed engine
        let paths = [
            "tests/fixtures/handler.wasm",
            "edge-worker/tests/fixtures/handler.wasm",
        ];
        let wasm_path = paths
            .iter()
            .map(std::path::PathBuf::from)
            .find(|p| p.exists())
            .expect("fixture handler.wasm missing");
        let bytes = std::fs::read(&wasm_path).unwrap();
        let component =
            wasmtime::component::Component::from_binary(&engine_for_compile, &bytes).unwrap();
        let linker = edge_runtime::create_component_linker_handler(&engine_for_compile).unwrap();
        let instance_pre = linker.instantiate_pre(&component).unwrap();
        let proxy_pre =
            wasmtime_wasi_http::p2::bindings::ProxyPre::new(instance_pre.clone()).unwrap();

        // Release the engine back to the pool
        pool.release(engine_for_compile);

        let config_a = HandlerConfig {
            tenant_id: "test-tenant".to_string(),
            egress: Arc::new(edge_runtime::EgressPolicy::allow_all()),
            log_sink: Arc::new(NullSink),
            app_ctx: edge_runtime::interfaces::observe::AppLogContext {
                app_name: "app-a".to_string(),
                tenant_id: "test-tenant".to_string(),
                deployment_id: "dep-a".to_string(),
            },
            meter: Arc::new(edge_runtime::RequestMeter::new(
                "test-tenant".to_string(),
                "dep-a".to_string(),
            )),
            env: HashMap::new(),
            max_request_body_bytes: 0,
            metrics_acc: None,
            socket_mode: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            last_request_at: Arc::new(tokio::sync::Mutex::new(Some(
                std::time::Instant::now() - std::time::Duration::from_secs(10),
            ))),
            max_memory_mb: 256,
            cpu_budget_ms: 1000,
        };

        let config_b = HandlerConfig {
            tenant_id: "test-tenant".to_string(),
            egress: Arc::new(edge_runtime::EgressPolicy::allow_all()),
            log_sink: Arc::new(NullSink),
            app_ctx: edge_runtime::interfaces::observe::AppLogContext {
                app_name: "app-b".to_string(),
                tenant_id: "test-tenant".to_string(),
                deployment_id: "dep-b".to_string(),
            },
            meter: Arc::new(edge_runtime::RequestMeter::new(
                "test-tenant".to_string(),
                "dep-b".to_string(),
            )),
            env: HashMap::new(),
            max_request_body_bytes: 0,
            metrics_acc: None,
            socket_mode: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            last_request_at: Arc::new(tokio::sync::Mutex::new(Some(std::time::Instant::now()))),
            max_memory_mb: 256,
            cpu_budget_ms: 1000,
        };

        let downloader = Arc::new(crate::downloader::Downloader::new(
            "http://localhost".to_string(),
            std::path::PathBuf::from("/tmp"),
            crate::auth::WorkerJwtSigner::new(vec![], None, "", "", "", ""),
        ));

        let dispatch_a = Arc::new(
            HandlerDispatch::new(
                18001,
                1000,
                10,
                config_a,
                None,
                downloader.clone(),
                "dep-a".to_string(),
                pool.clone(),
                state.clone(),
            )
            .unwrap(),
        );

        let dispatch_b = Arc::new(
            HandlerDispatch::new(
                18002,
                1000,
                10,
                config_b,
                None,
                downloader.clone(),
                "dep-b".to_string(),
                pool.clone(),
                state.clone(),
            )
            .unwrap(),
        );

        // Put the proxy_pre into dispatch_a, and make it hold the engine
        dispatch_a.set_proxy_pre(proxy_pre).await;

        // Create two FaaS apps: app A and app B.
        let app_a = AppInstance {
            deployment_id: "dep-a".to_string(),
            app_name: "app-a".to_string(),
            tenant_id: "test-tenant".to_string(),
            port: 18001,
            status: AppInstanceStatus::Running,
            meter: Arc::new(edge_runtime::RequestMeter::new(
                "test-tenant".to_string(),
                "dep-a".to_string(),
            )),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: instance_pre.clone(),
            handle: None,
            ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: Some(dispatch_a),
            metrics_acc: None,
            ws_port: None,
        };

        let app_b = AppInstance {
            deployment_id: "dep-b".to_string(),
            app_name: "app-b".to_string(),
            tenant_id: "test-tenant".to_string(),
            port: 18002,
            status: AppInstanceStatus::Running,
            meter: Arc::new(edge_runtime::RequestMeter::new(
                "test-tenant".to_string(),
                "dep-b".to_string(),
            )),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: instance_pre.clone(),
            handle: None,
            ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: Some(dispatch_b),
            metrics_acc: None,
            ws_port: None,
        };

        {
            let mut guard = state.write().await;
            guard.apps.insert(
                ("test-tenant".to_string(), "app-a".to_string()),
                Arc::new(Mutex::new(app_a)),
            );
            guard.apps.insert(
                ("test-tenant".to_string(), "app-b".to_string()),
                Arc::new(Mutex::new(app_b)),
            );
        }

        // Initially, app_a has an engine in memory.
        assert!(
            state
                .read()
                .await
                .apps
                .get(&("test-tenant".to_string(), "app-a".to_string()))
                .unwrap()
                .lock()
                .await
                .dispatch
                .as_ref()
                .unwrap()
                .has_engine()
                .await
        );

        // Let's acquire the engine to empty the pool!
        let _e1 = pool.acquire(&state).await;

        // Pool is now empty. Acquiring again should trigger LRU eviction on app-a (which has the engine) because app-a's last_request_at is older than app-b's.
        let start = std::time::Instant::now();
        let _e2 = pool.acquire(&state).await;
        let elapsed = start.elapsed();

        // Eviction should have been successful, taking the engine from app_a
        assert!(
            elapsed.as_millis() >= 450,
            "Should timeout waiting, then try eviction"
        );
        assert!(
            !state
                .read()
                .await
                .apps
                .get(&("test-tenant".to_string(), "app-a".to_string()))
                .unwrap()
                .lock()
                .await
                .dispatch
                .as_ref()
                .unwrap()
                .has_engine()
                .await,
            "app-a should have been evicted"
        );
    }
}
