//! Core supervisor logic — app lifecycle management.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    AppSpec, AppStatus, ClusterHeadroom, HeartbeatMessage, MetricKind, MetricSample, PurgeReason,
    TaskMessage,
};
use crate::metering_dedupe::dedupe_id;
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
            port_pool_size: 100,
            max_memory_mb: 256,
            epoch_tick_ms: 10,
            epoch_deadline_ticks: 100,
            consumer_name: "test".to_string(),
            queue_group: String::new(),
            task_stream_replicas: 1,
            worker_jwt_secret: String::new(),
            worker_jwt_kid: None,
            worker_jwt_issuer: String::new(),
            worker_bootstrap_secret: String::new(),
            worker_key_path: std::path::PathBuf::from("/tmp/worker-key"),
            worker_identity_path: std::path::PathBuf::from("/tmp/identity-key"),
            worker_reenroll_on_boot: false,
            handler_request_budget_ms: 1000,
            handler_max_request_body_bytes: 0,
            tls_cert_path: None,
            tls_key_path: None,
            socket_mode: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            hostname_pinning_enabled: false,
            standby_pool_size: 1,
            // Issue #307 PR2: these tests predate the signature
            // verification feature; they don't exercise signing, so
            // use the unsigned-friendly defaults. (PR1 follow-up:
            // `signing_pubkey`/`signing_pubkey_path` became
            // `signing_keyring`/`signing_keyring_path` for the
            // multi-keyring shape.)
            require_signature: false,
            signing_keyring: None,
            signing_keyring_path: None,
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
        build_supervisor_with_cooldown_secs(state, 1)
    }

    /// Same as `build_supervisor` but lets the test pick the
    /// `PortPool`'s cooldown. Used by `stop_app_releases_ws_port`
    /// (#448 regression) so released ports stay observable in
    /// `cooling_down` while the test asserts on them.
    fn build_supervisor_with_cooldown_secs(
        state: Arc<RwLock<WorkerState>>,
        cooldown_secs: u64,
    ) -> Arc<Supervisor> {
        build_supervisor_with_cooldown_and_starting_port(state, cooldown_secs, 10000)
    }

    /// Extends `build_supervisor_with_cooldown_secs` with a custom
    /// `PortPool` starting port. Used by
    /// `start_app_returns_err_when_port_pool_exhausted` (#641
    /// regression) to construct a pool near `u16::MAX` so the 1000-
    /// iteration sequential fallback wraps around quickly when all
    /// pre-populated ports are in cooldown.
    fn build_supervisor_with_cooldown_and_starting_port(
        state: Arc<RwLock<WorkerState>>,
        cooldown_secs: u64,
        starting_port: u16,
    ) -> Arc<Supervisor> {
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
                None,
            )),
            port_pool: Arc::new(Mutex::new(PortPool::new(starting_port, cooldown_secs))),
            nats: nats as Arc<dyn NatsClient>,
            log_forwarder: LogForwarder::new("http://localhost:0", "w_test", "fra", jwt.clone()),
            jwt_signer: jwt,
            http: reqwest::Client::new(),
            engine_pool: Arc::new(StandbyPool::new(1).expect("pool")),
            port_pool_exhausted_events: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    fn make_app(
        instance_pre: Option<wasmtime::component::InstancePre<edge_runtime::RuntimeState>>,
        status: AppInstanceStatus,
        ws_port: Option<u16>,
    ) -> Arc<Mutex<AppInstance>> {
        make_app_with_full_opts(
            instance_pre,
            status,
            ws_port,
            ExecutionModel::Handler,
            18000,
        )
    }

    /// Variant that lets each test pick the execution model — used
    /// by the resident_seconds build_heartbeat tests (issue #484).
    fn make_app_with_execution_model(
        execution_model: ExecutionModel,
        status: AppInstanceStatus,
    ) -> Arc<Mutex<AppInstance>> {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        make_app_with_full_opts(instance_pre, status, None, execution_model, 18000)
    }

    fn make_app_with_full_opts(
        instance_pre: Option<wasmtime::component::InstancePre<edge_runtime::RuntimeState>>,
        status: AppInstanceStatus,
        ws_port: Option<u16>,
        execution_model: ExecutionModel,
        port: u16,
    ) -> Arc<Mutex<AppInstance>> {
        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        meter.record_request();
        meter.record_request();
        Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port,
            status,
            meter,
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model,
            dispatch: None,
            metrics_acc: None,
            ws_port,
            protocol: "http".to_string(),
            last_error: None,
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
        let instance_pre = Some(load_handler_fixture(&engine));
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
        let instance_pre = Some(load_handler_fixture(&engine));
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
        let instance_pre = Some(load_handler_fixture(&engine));
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

    // ── resident_seconds build_heartbeat tests (issue #484) ───────

    /// LongRunning app: meter has accumulated 60s of resident time →
    /// heartbeat stamps `Some(60)`. The `applyTenantDelta` selector
    /// treats `Some(N)` as N resident seconds for the tenant.
    #[tokio::test]
    async fn build_heartbeat_long_running_app_stamps_resident_seconds() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app =
            make_app_with_execution_model(ExecutionModel::LongRunning, AppInstanceStatus::Running);
        // Manually accumulate 60 resident seconds (the production
        // path would do this via the per-app ticker).
        app.lock().await.meter.record_resident_seconds(60);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);
        let hb = sup.build_heartbeat().await;
        let s = hb.apps.get("my-app").expect("app present");
        assert_eq!(
            s.resident_seconds,
            Some(60),
            "LongRunning app must stamp Some(60); got {:?}",
            s.resident_seconds
        );
    }

    /// Handler (FaaS) app: meter has 0 resident time (no ticker
    /// was ever spawned) and the build_heartbeat path must stamp
    /// `None`, NOT `Some(0)`. The wire-shape distinction between
    /// "FaaS doesn't contribute" (None) and "LR just started"
    /// (Some(0)) is preserved for downstream debugging.
    #[tokio::test]
    async fn build_heartbeat_handler_app_omits_resident_seconds() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app =
            make_app_with_execution_model(ExecutionModel::Handler, AppInstanceStatus::Running);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);
        let hb = sup.build_heartbeat().await;
        let s = hb.apps.get("my-app").expect("app present");
        assert!(
            s.resident_seconds.is_none(),
            "Handler (FaaS) app must omit resident_seconds (None); got {:?}",
            s.resident_seconds
        );
    }

    /// LongRunning app that just started: meter has 0 resident time
    /// but `execution_model == LongRunning` → heartbeat stamps
    /// `Some(0)`, NOT `None`. Some(0) is the "LR ran for the whole
    /// interval but started recently" signal distinct from FaaS.
    #[tokio::test]
    async fn build_heartbeat_long_running_zero_is_some_zero_not_none() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app =
            make_app_with_execution_model(ExecutionModel::LongRunning, AppInstanceStatus::Starting);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);
        let hb = sup.build_heartbeat().await;
        let s = hb.apps.get("my-app").expect("app present");
        assert_eq!(
            s.resident_seconds,
            Some(0),
            "LongRunning just-started app must stamp Some(0); got {:?}",
            s.resident_seconds
        );
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
        let instance_pre = Some(load_handler_fixture(&engine));
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
        let instance_pre = Some(load_handler_fixture(&engine));
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
        let instance_pre = Some(load_handler_fixture(&engine));
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
                deployment_signature: None,
                signing_key_id: None,
                routes: None,
                env: HashMap::new(),
                allowlist: None,
                socket_mode: None,
                max_memory_mb: 256,
                cpu_budget_ms: None,
                preview_id: None,
                preview_pr_number: None,
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
        let instance_pre = Some(load_handler_fixture(&engine));
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
        let instance_pre = Some(load_handler_fixture(&engine));
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

    // ── task_purge (issue #569) ─────────────────────────────────────
    //
    // These tests cover the worker-side half of the per-tenant
    // cleanup wire: a `task_purge` TaskMessage carries a
    // tenant_id + optional app_name + reason. The supervisor
    // stops the affected apps (per-app or tenant-wide) and then
    // invokes `edge_runtime::purge_tenant` to clear KV / cache /
    // scheduler state. The regression guard for "stop_app must
    // NOT purge" lives in `edge-runtime::runtime::tests` so the
    // worker-side tests here exercise the wire dispatch, not the
    // store-clear invariant.

    #[tokio::test]
    async fn handle_purge_per_app_stops_only_named_app() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        // Two apps for the same tenant.
        let app_a = make_app(instance_pre.clone(), AppInstanceStatus::Running, None);
        let app_b = make_app(instance_pre, AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "app-a".into()), app_a);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "app-b".into()), app_b);
        let sup = build_supervisor(state);

        // task_purge targets app-a only.
        let msg = TaskMessage::TaskPurge {
            timestamp: String::new(),
            tenant_id: "t_test".into(),
            app_name: "app-a".into(),
            reason: PurgeReason::AppDeleted,
        };
        sup.handle_task_message(msg).await.unwrap();
        // app-b must survive a per-app purge.
        assert_eq!(sup.state.read().await.apps.len(), 1);
        assert!(sup
            .state
            .read()
            .await
            .apps
            .contains_key(&("t_test".into(), "app-b".into())));
    }

    #[tokio::test]
    async fn handle_purge_tenant_wide_stops_all_apps() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app_a = make_app(instance_pre.clone(), AppInstanceStatus::Running, None);
        let app_b = make_app(instance_pre, AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "app-a".into()), app_a);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "app-b".into()), app_b);
        let sup = build_supervisor(state);

        // task_purge with empty app_name = tenant-wide purge.
        let msg = TaskMessage::TaskPurge {
            timestamp: String::new(),
            tenant_id: "t_test".into(),
            app_name: String::new(),
            reason: PurgeReason::TenantOffboarded,
        };
        sup.handle_task_message(msg).await.unwrap();
        // All apps for the tenant must be gone.
        assert!(sup.state.read().await.apps.is_empty());
    }

    #[tokio::test]
    async fn handle_purge_idempotent_when_app_already_gone() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = make_app(instance_pre, AppInstanceStatus::Running, None);
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);

        // task_purge for an app we don't host — must be a no-op,
        // not an error. JetStream redelivery (issue #42) relies
        // on idempotence.
        let msg = TaskMessage::TaskPurge {
            timestamp: String::new(),
            tenant_id: "t_test".into(),
            app_name: "missing-app".into(),
            reason: PurgeReason::AppDeleted,
        };
        let result = sup.handle_task_message(msg).await;
        assert!(result.is_ok());
        // Existing app still there.
        assert_eq!(sup.state.read().await.apps.len(), 1);
    }

    #[tokio::test]
    async fn handle_purge_rejects_unsafe_tenant_id() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let sup = build_supervisor(state);

        // Path-traversal attempt — must be refused, not silently
        // accepted. (The runtime `purge_tenant` is the second
        // guard; here we just verify the supervisor short-circuits
        // before touching the port pool / state map.)
        let msg = TaskMessage::TaskPurge {
            timestamp: String::new(),
            tenant_id: "../etc".into(),
            app_name: String::new(),
            reason: PurgeReason::TenantOffboarded,
        };
        let result = sup.handle_task_message(msg).await;
        assert!(result.is_err(), "unsafe tenant_id must be rejected");
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
        let instance_pre = Some(load_handler_fixture(&engine));
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
        let instance_pre = Some(load_handler_fixture(&engine));
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
                deployment_signature: None,
                signing_key_id: None,
                routes: None,
                env: HashMap::new(),
                allowlist: None,
                socket_mode: None,
                max_memory_mb: 256,
                cpu_budget_ms: None,
                preview_id: None,
                preview_pr_number: None,
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

    // ── stop_app tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn stop_app_not_found() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let sup = build_supervisor(state);
        let result = sup.stop_app("nonexistent", "ghost").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn stop_app_long_running_removes_app() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let (oneshot_tx, _) = tokio::sync::oneshot::channel::<()>();
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 18000,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d1".into())),
            shutdown_tx: Some(oneshot_tx),
            shutdown_tx_broadcast: None,
            instance_pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state.clone());
        let result = sup.stop_app("t_test", "my-app").await;
        assert!(result.is_ok());
        assert!(state.read().await.apps.is_empty());
    }

    /// Issue #448 regression — `stop_app` must release BOTH the primary
    /// `port` AND the dedicated `ws_port` it allocated from the same
    /// pool. The previous code only released `port`, so `ws_port`
    /// leaked until the 60s cooldown returned it. Under bursty WS-app
    /// redeploys this exhausted the PortPool in CI.
    ///
    /// The regression assertion is structural: install an app with
    /// both `port` and `ws_port`, call `stop_app`, then assert BOTH
    /// ports are now in the pool's cooldown set. The cooldown set
    /// is the only observable signal of a `release()` — released
    /// ports don't return to `available` until the cooldown
    /// elapses; with the default 1s-cooldown test pool they re-enter
    /// `available` immediately, so the test constructs a
    /// `Supervisor` with a 60s-cooldown pool directly (the
    /// `build_supervisor` helper hard-codes 1s). The test-only
    /// `PortPool::is_in_cooldown` accessor
    /// (`edge-worker/src/port_pool.rs:115-126`) lets the test
    /// target both ports directly without exposing the internal
    /// `cooling_down` Vec.
    #[tokio::test]
    async fn stop_app_releases_ws_port() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));

        let (oneshot_tx, _) = tokio::sync::oneshot::channel::<()>();
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-ws-app".into(),
            tenant_id: "t_test".into(),
            port: 10001,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d1".into())),
            shutdown_tx: Some(oneshot_tx),
            shutdown_tx_broadcast: None,
            instance_pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: Some(10002),
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-ws-app".into()), app);

        // Inline-construct with a 60s-cooldown pool so the released
        // ports stay observable in `cooling_down` for the duration of
        // this assertion. Mirrors `build_supervisor` (line 191) with a
        // single field change. We avoid `build_supervisor`'s 1s cooldown
        // because `reap_cooled_ports` (triggered by `acquire` /
        // `free_slots`) would otherwise re-empty the cooldown set
        // between `stop_app` and our assertion.
        let sup = build_supervisor_with_cooldown_secs(state.clone(), 60);

        sup.stop_app("t_test", "my-ws-app")
            .await
            .expect("stop_app with ws_port must not error");

        // Both ports must now be in cooldown — proves `release(port)`
        // AND `release(ws_port)` ran. Before the fix, `ws_port`
        // leaked (never re-entered the cooldown set).
        let pool = sup.port_pool.lock().await;
        assert!(
            pool.is_in_cooldown(10001),
            "primary port 10001 must be in cooldown after stop_app (release(port) regression)"
        );
        assert!(
            pool.is_in_cooldown(10002),
            "ws_port 10002 must be in cooldown after stop_app (release(ws_port) regression)"
        );

        assert!(
            state.read().await.apps.is_empty(),
            "app must be removed from state after stop_app"
        );
    }

    /// Issue #641 regression: when the port pool is exhausted, `start_app`
    /// must return `Err(_)` instead of panicking. The panic would unwind
    /// the NATS consume loop and kill the worker process, taking every
    /// other app on the node down with it.
    #[tokio::test]
    async fn start_app_returns_err_when_port_pool_exhausted() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        // We use a 100-year cooldown (not `u64::MAX`, which would
        // overflow `Instant + Duration` arithmetic inside `release()`)
        // so released ports stay in `cooling_down` permanently for the
        // duration of the test. The exhausted state is constructed
        // via `drain_available_into_cooldown` (test-only) instead of
        // draining via `acquire/release` — which would increment
        // `next_port` past the cooldown range and let the sequential
        // fallback find free ports in the wrapped range.
        let huge_cooldown_secs: u64 = 100 * 365 * 24 * 60 * 60; // 100 years
        let sup = build_supervisor_with_cooldown_and_starting_port(
            state.clone(),
            huge_cooldown_secs,
            10000,
        );

        // Drain the pool. `drain_available_into_cooldown` (test-only)
        // moves the 100 pre-populated ports AND the next 1000
        // sequential-fallback ports into `cooling_down` so any
        // subsequent `acquire()` returns None. We do NOT verify
        // this with an `assert_eq!(pool.acquire(), None)` here
        // because that would itself trigger the 1000-iteration
        // sequential fallback against an 1100-entry cooldown
        // set — O(1.1M) ops, slow under debug builds. Trust the
        // helper and let `start_app` be the verification.
        {
            let mut pool = sup.port_pool.lock().await;
            let moved = pool.drain_available_into_cooldown();
            assert_eq!(moved, 100, "expected to drain 100 pre-populated ports");
        }

        // Build a minimal AppSpec (no EDGE_WS_PORT — exercises the HTTP
        // branch of the fix). The supervisor's signature check will fire
        // *after* the port-acquire block, so we don't need a real
        // signature here — but we DO need to NOT have a signature, which
        // would make the signature check fail first and mask the bug.
        // The fix is upstream of the signature check, so a missing
        // signature is fine for this test: port-exhaustion bails earlier.
        let spec = AppSpec {
            deployment_id: "d_test_641".to_string(),
            deployment_hash: "abc123".to_string(),
            deployment_signature: None,
            signing_key_id: None,
            routes: None,
            env: HashMap::new(),
            allowlist: None,
            socket_mode: None,
            max_memory_mb: 256,
            cpu_budget_ms: None,
            preview_id: None,
            preview_pr_number: None,
        };

        let result = sup.start_app("a_test_641", &spec, "t_test_641").await;

        let err = result.expect_err("start_app must return Err when pool is exhausted");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("port pool exhausted"),
            "error message should mention port pool exhaustion; got: {msg}"
        );

        // The failing app must NOT be registered in state.
        assert!(
            state.read().await.apps.is_empty(),
            "failed start_app must not register the app in state"
        );

        // Issue #641: the worker-level exhaustion counter must have
        // been bumped. The CP's deploy-time 402 gate
        // (`SumFreeSlotsByRegion`, Commit #3) reads this via the
        // persisted `worker_status.port_pool_exhausted_count` column
        // — a non-zero bump here proves the wire end-to-end path.
        let count = sup
            .port_pool_exhausted_events
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            count, 1,
            "HTTP-port exhaustion arm must bump port_pool_exhausted_events to 1"
        );
    }

    /// Issue #641 regression: when the WS-port acquire fails after the
    /// HTTP-port acquire succeeded, `start_app` must release the
    /// already-acquired HTTP port before bailing. Otherwise the leaked
    /// HTTP port sits in a 60-second cooling-down limbo during the
    /// nack-and-redeliver cycle, silently reducing pool capacity.
    #[tokio::test]
    async fn start_app_releases_raw_port_when_ws_port_acquire_fails() {
        let engine = edge_runtime::create_engine().expect("engine");
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let huge_cooldown_secs: u64 = 100 * 365 * 24 * 60 * 60; // 100 years
        let sup = build_supervisor_with_cooldown_and_starting_port(
            state.clone(),
            huge_cooldown_secs,
            10000,
        );

        // Drain the pool leaving exactly ONE port allocatable — enough
        // for the HTTP acquire to succeed but not enough for the WS
        // acquire.
        {
            let mut pool = sup.port_pool.lock().await;
            pool.drain_leaving_n_available(1);
        }

        // Capture which port the HTTP branch will consume so we can
        // assert it ends up in cooldown after the WS branch fails.
        let expected_raw_port: u16 = {
            let mut pool = sup.port_pool.lock().await;
            pool.acquire().expect("first acquire should succeed")
        };
        // Put it back — `start_app` is going to acquire it itself.
        // `release` is idempotent so it's safe even though the port
        // isn't technically cooling-down yet.
        sup.port_pool.lock().await.release(expected_raw_port);

        // Build a spec with EDGE_WS_PORT set so the WS branch runs.
        let spec = AppSpec {
            deployment_id: "d_test_641_ws".to_string(),
            deployment_hash: "abc123".to_string(),
            deployment_signature: None,
            signing_key_id: None,
            routes: None,
            env: HashMap::from([("EDGE_WS_PORT".to_string(), "1".to_string())]),
            allowlist: None,
            socket_mode: None,
            max_memory_mb: 256,
            cpu_budget_ms: None,
            preview_id: None,
            preview_pr_number: None,
        };

        let result = sup.start_app("a_test_641_ws", &spec, "t_test_641_ws").await;

        let err = result.expect_err("start_app must return Err when WS acquire fails");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("port pool exhausted"),
            "error message should mention port pool exhaustion; got: {msg}"
        );

        // The leaked HTTP port must have been released by the WS
        // branch's `pool.release(raw_port)` guard. With a 100-year
        // cooldown, the port is in `cooling_down` (not `available`).
        assert!(
            sup.port_pool.lock().await.is_in_cooldown(expected_raw_port),
            "HTTP port leaked: expected port {expected_raw_port} to be released into cooldown after WS acquire failed"
        );

        // Issue #641: the WS-port exhaustion arm must also bump the
        // worker-level counter — same observability requirement as the
        // HTTP-port branch. Both arms are "pool can't take more"
        // signals; the CP gate sees them uniformly.
        let count = sup
            .port_pool_exhausted_events
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            count, 1,
            "WS-port exhaustion arm must bump port_pool_exhausted_events to 1"
        );
    }

    #[tokio::test]
    async fn stop_app_handler_with_broadcast_sends_signal() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let (tx, _rx) = tokio::sync::broadcast::channel::<()>(1);
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 18001,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d1".into())),
            shutdown_tx: None,
            shutdown_tx_broadcast: Some(tx),
            instance_pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state.clone());
        let result = sup.stop_app("t_test", "my-app").await;
        assert!(result.is_ok());
        assert!(state.read().await.apps.is_empty());
    }

    // ── build_heartbeat with observer_metrics ──────────────────────

    #[tokio::test]
    async fn heartbeat_with_observer_metrics() {
        use edge_runtime::interfaces::observe::MetricsAccumulator;
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));

        // MetricsAccumulator starts empty — this still exercises the
        // Some(acc) branch in build_heartbeat.
        let acc = MetricsAccumulator::new();

        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        meter.record_request();
        meter.record_request();
        meter.record_outbound_bytes(512);

        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 18002,
            status: AppInstanceStatus::Running,
            meter: meter.clone(),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: None,
            metrics_acc: Some(Arc::new(acc)),
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = build_supervisor(state);
        let hb = sup.build_heartbeat().await;

        let status = hb.apps.get("my-app").expect("app present");
        assert_eq!(status.request_count, 2);
        assert_eq!(status.outbound_bytes, 512);

        // With an empty accumulator, observer_metrics is an empty vec
        assert!(
            status.observer_metrics.is_empty(),
            "empty accumulator should produce empty observer_metrics"
        );
    }

    /// Issue #45 — `stop_app` MUST NOT re-raise a panic from a
    /// panicking per-app task. Re-raising via `panic::resume_unwind`
    /// unwinds out of `stop_app` into `handle_task_message` /
    /// `run_consume_loop` and tears down the worker process,
    /// killing every other app on the same node. The fix logs
    /// the panic payload at `error!` and lets the stop sequence
    /// complete normally.
    ///
    /// The test:
    ///   1. Installs two AppInstances. App "panic-app" has a
    ///      handle that runs `panic!()` immediately. App
    ///      "survivor-app" has no handle (long-running) so it
    ///      stays in `WorkerState::apps` until redeployed.
    ///   2. Calls `sup.stop_app("t_test", "panic-app")` and
    ///      asserts the call returns `Ok(())` — i.e. did NOT
    ///      unwind through `resume_unwind`.
    ///   3. Asserts "panic-app" is removed from the state and
    ///      "survivor-app" is still present — the headline
    ///      guarantee that a single bad app does not impact any
    ///      other app on the same worker.
    #[tokio::test]
    async fn stop_app_does_not_re_panic_when_handle_panics() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));

        // App A: handle will panic.
        let (panic_tx, _) = tokio::sync::oneshot::channel::<()>();
        let panicking_handle = tokio::spawn(async {
            // Yield once so the supervisor's join awaits a
            // Ready-panic rather than a synchronous trap.
            tokio::task::yield_now().await;
            panic!("synthetic guest panic for issue #45 test");
        });
        let panic_app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d_panic".into(),
            app_name: "panic-app".into(),
            tenant_id: "t_test".into(),
            port: 18000,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d_panic".into())),
            shutdown_tx: Some(panic_tx),
            shutdown_tx_broadcast: None,
            instance_pre: instance_pre.clone(),
            handle: Some(Arc::new(panicking_handle)),
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));

        // App B: untouched. This is the "other app on the same
        // worker" the issue body requires the supervisor to keep
        // running.
        let (survivor_tx, _) = tokio::sync::oneshot::channel::<()>();
        let survivor_app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d_survivor".into(),
            app_name: "survivor-app".into(),
            tenant_id: "t_test".into(),
            port: 18001,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d_survivor".into())),
            shutdown_tx: Some(survivor_tx),
            shutdown_tx_broadcast: None,
            instance_pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));

        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "panic-app".into()), panic_app);
        state.write().await.apps.insert(
            ("t_test".into(), "survivor-app".into()),
            survivor_app.clone(),
        );

        let sup = build_supervisor(state.clone());

        // The headline assertion: this call MUST return Ok(())
        // without unwinding. Before the fix, the
        // `panic::resume_unwind(panic_payload)` inside `stop_app`
        // unwound out of the test function too, failing the test
        // with a panic message rather than an assertion error.
        let result = sup.stop_app("t_test", "panic-app").await;
        assert!(
            result.is_ok(),
            "stop_app must not propagate the app-task panic: {result:?}"
        );

        // App A torn down; App B still alive.
        let snapshot = state.read().await;
        assert!(
            !snapshot
                .apps
                .contains_key(&("t_test".into(), "panic-app".into())),
            "panic-app should be removed after stop_app"
        );
        assert!(
            snapshot
                .apps
                .contains_key(&("t_test".into(), "survivor-app".into())),
            "survivor-app must remain running — supervisor must not tear down unrelated apps"
        );
        let survivor_inst = snapshot
            .apps
            .get(&("t_test".into(), "survivor-app".into()))
            .expect("survivor-app present")
            .lock()
            .await;
        assert_eq!(survivor_inst.status, AppInstanceStatus::Running);
    }

    /// Issue #45, Site 2 (review follow-up) — `handle_app_crash` is
    /// the shared trap/panic/Hung machinery. Calling it with a
    /// `panic-in-spawn` shape (synthetic anyhow::Error wrapping the
    /// JoinError payload) must increment the restart counter, flip
    /// `AppInstance.status` to the supplied terminal variant when
    /// the cap is exceeded, and signal `terminal == true` so the
    /// caller's `break` fires. The matching `Crashed` vs `Hung`
    /// status flip is what distinguishes "guest trapped" from
    /// "guest stopped yielding" on the wire — a regression here
    /// would conflate the two failure modes for the heartbeat.
    ///
    /// The test drives `handle_app_crash` directly because
    /// `run_app_loop` requires a real `InstancePre<RuntimeState>`
    /// which can't be synthesized in a unit test. The behavior
    /// asserted here is exactly what `run_app_loop`'s trap arm,
    /// panic arm, and Hung arm all depend on.
    #[tokio::test]
    async fn handle_app_crash_flips_to_crashed_after_max_restarts() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));

        let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d_panic".into(),
            app_name: "panic-app".into(),
            tenant_id: "t_test".into(),
            port: 18002,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d_panic".into())),
            shutdown_tx: Some(_shutdown_tx),
            shutdown_tx_broadcast: None,
            instance_pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "panic-app".into()), app);

        let sup = build_supervisor(state.clone());

        // Simulate the panic-in-spawn synthetic error shape that
        // run_app_loop's `Ok(Err(join_err))` arm produces. The
        // payload itself isn't threaded into handle_app_crash
        // (the helper takes the terminal status directly) but we
        // keep it here as documentation of the call shape.
        let _synthetic_err = anyhow::anyhow!("app task panicked: synthetic");

        // Drive handle_app_crash 4 times under the cap — each call
        // should return `false` (continue-with-backoff).
        let base = Duration::from_millis(1);
        let max = Duration::from_millis(4);
        for rc in 1..5 {
            let terminal = Supervisor::handle_app_crash(
                &state,
                "t_test",
                "panic-app",
                "d_panic",
                &sup.downloader,
                rc,
                5,
                base,
                max,
                AppInstanceStatus::Crashed { restart_count: rc },
                Some("synthetic panic-in-spawn error"),
                "app crashed (panic-in-spawn)",
            )
            .await;
            assert!(
                !terminal,
                "rc={rc} under cap must NOT be terminal (handle_app_crash returned true)"
            );
        }

        // The 5th call hits the cap and must return `true`.
        let terminal = Supervisor::handle_app_crash(
            &state,
            "t_test",
            "panic-app",
            "d_panic",
            &sup.downloader,
            5,
            5,
            base,
            max,
            AppInstanceStatus::Crashed { restart_count: 5 },
            Some("synthetic panic-in-spawn error"),
            "app crashed (panic-in-spawn)",
        )
        .await;
        assert!(
            terminal,
            "rc=5 at the cap MUST be terminal (handle_app_crash returned false)"
        );

        // App status was flipped to Crashed { restart_count: 5 }.
        let snapshot = state.read().await;
        let inst = snapshot
            .apps
            .get(&("t_test".into(), "panic-app".into()))
            .expect("app present")
            .lock()
            .await;
        assert_eq!(
            inst.status,
            AppInstanceStatus::Crashed { restart_count: 5 },
            "after 5 panics, status must be Crashed {{ restart_count: 5 }}"
        );
        assert_eq!(
            inst.last_error.as_deref(),
            Some("synthetic panic-in-spawn error"),
            "after 5 panics, last_error must carry the panic payload so the \
             heartbeat can surface it (issue #45)"
        );
    }

    /// `last_error` must be stamped on every restart (not only when
    /// the cap is reached) so an operator watching the heartbeat sees
    /// the panic payload within one 30s tick — even if the app later
    /// recovers or doesn't reach the cap. Without this, the
    /// structured `tracing::error!` is the only signal and the
    /// heartbeat stays silent on `status: "running"`.
    #[tokio::test]
    async fn handle_app_crash_stamps_last_error_on_every_restart() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));

        let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d_every".into(),
            app_name: "every-app".into(),
            tenant_id: "t_test".into(),
            port: 18004,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d_every".into())),
            shutdown_tx: Some(_shutdown_tx),
            shutdown_tx_broadcast: None,
            instance_pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "every-app".into()), app);

        let sup = build_supervisor(state.clone());

        let base = Duration::from_millis(1);
        let max = Duration::from_millis(4);
        // Under the cap — must still stamp last_error.
        let _ = Supervisor::handle_app_crash(
            &state,
            "t_test",
            "every-app",
            "d_every",
            &sup.downloader,
            1,
            5,
            base,
            max,
            AppInstanceStatus::Crashed { restart_count: 1 },
            Some("first panic"),
            "app crashed (panic-in-spawn)",
        )
        .await;

        let snapshot = state.read().await;
        let inst = snapshot
            .apps
            .get(&("t_test".into(), "every-app".into()))
            .expect("app present")
            .lock()
            .await;
        assert_eq!(
            inst.last_error.as_deref(),
            Some("first panic"),
            "last_error must be stamped on the very first restart, not only at the cap"
        );
        assert!(
            matches!(inst.status, AppInstanceStatus::Running),
            "under cap, status must stay Running so the heartbeat still publishes last_error \
             against status=running (issue #45 — heartbeat visibility from tick 1)"
        );
    }

    /// Sibling assertion: `handle_app_crash` with the `Hung`
    /// terminal status must produce `AppInstanceStatus::Hung`, not
    /// `Crashed`. This is what lets the heartbeat distinguish
    /// "guest trapped" from "guest stopped yielding" on the wire
    /// (issue #45 review follow-up — both arms now route through
    /// `handle_app_crash` so the status flip can't drift).
    #[tokio::test]
    async fn handle_app_crash_flips_to_hung_for_timeout_path() {
        let engine = edge_runtime::create_engine().expect("engine");
        let instance_pre = Some(load_handler_fixture(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));

        let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d_hung".into(),
            app_name: "hung-app".into(),
            tenant_id: "t_test".into(),
            port: 18003,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d_hung".into())),
            shutdown_tx: Some(_shutdown_tx),
            shutdown_tx_broadcast: None,
            instance_pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "hung-app".into()), app);

        let sup = build_supervisor(state.clone());

        let base = Duration::from_millis(1);
        let max = Duration::from_millis(4);
        let terminal = Supervisor::handle_app_crash(
            &state,
            "t_test",
            "hung-app",
            "d_hung",
            &sup.downloader,
            5,
            5,
            base,
            max,
            AppInstanceStatus::Hung,
            None,
            "app hung (health check timeout)",
        )
        .await;
        assert!(terminal, "rc=5 at the cap MUST be terminal");

        let snapshot = state.read().await;
        let inst = snapshot
            .apps
            .get(&("t_test".into(), "hung-app".into()))
            .expect("app present")
            .lock()
            .await;
        assert_eq!(
            inst.status,
            AppInstanceStatus::Hung,
            "timeout path must produce Hung, NOT Crashed"
        );
        assert!(
            inst.last_error.is_none(),
            "Hung arm passed err_for_audit=None, so last_error must stay None \
             (no panic payload to surface — issue #45)"
        );
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

// ── socket_mode_for_spec ────────────────────────────────────────────

/// Resolve the per-app `SocketEgressPolicy` from an `AppSpec` (issue #412).
///
/// `None` on the spec falls back to the worker-wide `Config::socket_mode`
/// (the historical pre-#412 behavior). `Some(mode)` is returned
/// unconditionally — **the compose rule with the worker-wide
/// `hostname_pinning_enabled` toggle is enforced at the FaaS dispatch site
/// (`edge-worker/src/dispatch.rs::handle_request`), not here.** This keeps
/// the helper dumb: it does not inspect `hostname_pinning_enabled`. The
/// LongRunning path uses the return value directly; the FaaS path further
/// narrows the `HostnamePinned` arm through the compose rule.
#[allow(dead_code)]
pub fn socket_mode_for_spec(
    spec: &AppSpec,
    worker_default: edge_runtime::socket_egress::SocketEgressPolicy,
) -> edge_runtime::socket_egress::SocketEgressPolicy {
    spec.socket_mode.unwrap_or(worker_default)
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
    /// Worker-level exhaustion counter (issue #641). Bumped from the
    /// two `start_app` exhaustion arms (HTTP-port acquire fails,
    /// WS-port acquire fails) — covers apps that exhaust the pool on
    /// first start and never produce an AppStatus row (which is the
    /// case where a per-app counter would be invisible).
    /// Persisted into `worker_status` by the CP's heartbeat-ingest
    /// path so the deploy-time 402 gate (`SumFreeSlotsByRegion`)
    /// can ask "has any worker in this region recently exhausted?"
    /// without scraping Prometheus.
    /// Atomic so the two exhaustion arms can bump without holding the
    /// port-pool mutex (which is already locked at the bump site).
    /// Reset on worker process restart (matches `request_count`
    /// semantics — per-process-boot cumulative).
    pub port_pool_exhausted_events: Arc<std::sync::atomic::AtomicU64>,
}

impl Supervisor {
    /// HTTP `/sync` fallback (issue #53). When NATS is silent for
    /// longer than `worker_sync_threshold_secs`, the worker falls back
    /// to polling the control plane over HTTP to discover any
    /// reconciliation commands it might be missing.
    ///
    /// A non-2xx response or a malformed body surfaces as `Ok(None)`
    /// ("no task message via the HTTP fallback") rather than `Err` — a
    /// flaky /sync endpoint should not crash the watchdog loop; the
    /// worker just tries again on the next tick.
    #[allow(dead_code)]
    pub async fn fetch_sync(&self) -> anyhow::Result<Option<crate::messages::TaskMessage>> {
        // Stamp the watchdog so health-check tests don't trip on a
        // quiet worker.
        if let Ok(mut guard) = self.state.read().await.last_task_received_at.lock() {
            *guard = Some(std::time::Instant::now());
        }

        let url = format!(
            "{}/api/internal/workers/{}/sync",
            self.config.control_plane_url, self.config.worker_id
        );
        let token = self.jwt_signer.sign();
        let response = match self.http.get(&url).bearer_auth(token).send().await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!(err = %e, url, "fetch_sync request failed");
                return Ok(None);
            }
        };

        if !response.status().is_success() {
            tracing::warn!(status = %response.status(), url, "fetch_sync got non-2xx response");
            return Ok(None);
        }

        match response.json::<crate::messages::TaskMessage>().await {
            Ok(msg) => Ok(Some(msg)),
            Err(e) => {
                tracing::warn!(err = %e, url, "fetch_sync got malformed response body");
                Ok(None)
            }
        }
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
        // Stamp the watchdog on entry — "we heard from NATS", not "the
        // diff fully applied". Stamping only on success would leave the
        // timer untouched when a partial diff fails (downloader
        // rejection, hash mismatch, port exhaustion), and the
        // heartbeat-loop watchdog would then trigger the HTTP /sync
        // fallback even though NATS is healthy and delivering messages.
        if let Ok(mut guard) = self.state.read().await.last_task_received_at.lock() {
            *guard = Some(std::time::Instant::now());
        }

        // Issue #569: `task_purge` tombstones are a distinct wire shape
        // — no `apps` field, derived from the worker's in-memory state.
        // Dispatched to `handle_purge` BEFORE the task_update / full_sync
        // diff path so the stop-then-purge ordering is per-message and
        // we never race a diff against a tenant that's mid-purge.
        if let TaskMessage::TaskPurge {
            ref tenant_id,
            ref app_name,
            ref reason,
            ..
        } = msg
        {
            return self.handle_purge(tenant_id, app_name, *reason).await;
        }

        let (tenant_id, desired_apps) = match msg {
            TaskMessage::TaskUpdate {
                tenant_id, apps, ..
            } => (tenant_id, apps),
            TaskMessage::FullSync {
                tenant_id, apps, ..
            } => (tenant_id, apps),
            // Exhaustiveness: the `task_purge` arm was handled above.
            TaskMessage::TaskPurge { .. } => unreachable!("task_purge handled above"),
        };

        // Snapshot this tenant's running apps.
        let current_apps = self.snapshot_current_apps(&tenant_id).await;

        let diff = compute_app_diff(&current_apps, &desired_apps);

        let has_changes = !diff.apps_to_stop.is_empty() || !diff.apps_to_start.is_empty();

        for app_name in &diff.apps_to_stop {
            self.stop_app(&tenant_id, app_name).await?;
        }

        for (app_name, spec) in &diff.apps_to_start {
            self.start_app(app_name, spec, &tenant_id).await?;
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

    /// Issue #569: handle a `task_purge` tombstone. Stops the
    /// in-flight apps for the tenant first (per-app when `app_name`
    /// is set, every app when empty for the tenant-wide form), then
    /// calls `edge_runtime::runtime::purge_tenant` to remove the
    /// per-tenant dirs and in-memory registry entries.
    ///
    /// **Order matters, do not reorder.** Stopping apps first
    /// achieves two things:
    ///
    /// 1. Drains in-flight requests so no KV / cache / scheduler
    ///    read or write is racing the dir removal.
    /// 2. Drops every `Arc<KvStore>` / `Arc<Cache>` / `Arc<Scheduler>`
    ///    clone held by the app, so `purge_tenant`'s
    ///    `Arc::get_mut` inside `runtime.rs` succeeds and the
    ///    in-memory registries clear. If a still-running app holds
    ///    a clone when `purge_tenant` runs, `Arc::get_mut` returns
    ///    `None` and the registry entry silently survives — the
    ///    next `start_app` for the tenant then races against a
    ///    `KvStore` whose on-disk dir has already been `rm`'d,
    ///    splitting state across two instances until the old `Arc`
    ///    drops.
    ///
    /// The diff-and-reconcile path takes over from the next
    /// task_update / full_sync — a subsequent activate for the same
    /// tenant will see no apps (correct) and recreate the dirs
    /// (forward-compat).
    ///
    /// Idempotent: an unknown app is a no-op, an already-empty
    /// tenant is a no-op. JetStream redelivery (issue #42) is safe.
    async fn handle_purge(
        &self,
        tenant_id: &str,
        app_name: &str,
        reason: PurgeReason,
    ) -> anyhow::Result<()> {
        if !edge_runtime::is_safe_tenant_id(tenant_id) {
            // Mirror the start_app guard — refuse rather than
            // escape the persistence base via a malformed tenant id.
            anyhow::bail!("refusing to purge: unsafe tenant_id {:?}", tenant_id);
        }

        let current = self.snapshot_current_apps(tenant_id).await;
        let to_stop: Vec<String> = if app_name.is_empty() {
            current.keys().cloned().collect()
        } else if current.contains_key(app_name) {
            vec![app_name.to_string()]
        } else {
            tracing::info!(
                tenant_id = %tenant_id,
                app_name = %app_name,
                "task_purge for app that isn't running — no-op"
            );
            Vec::new()
        };

        for app in &to_stop {
            self.stop_app(tenant_id, app).await?;
        }

        // purge_tenant is idempotent and best-effort on dir removal;
        // a failure here is logged but does NOT propagate — the
        // tombstone has already been ack'd and a future #475 durable
        // KV tier will add a CP-side BatchDelete retry path.
        if let Err(e) = edge_runtime::purge_tenant(tenant_id) {
            tracing::error!(
                tenant_id = %tenant_id,
                err = %e,
                "purge_tenant failed (in-memory cleared; on-disk dirs may be partial)",
            );
        }

        tracing::info!(
            tenant_id = %tenant_id,
            app_name = %app_name,
            reason = ?reason,
            stopped = to_stop.len(),
            "task_purge handled",
        );
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

        // Acquire an HTTP port. Issue #641: acquire() already returns
        // Option<u16>; turn None into an Err so the consume loop can
        // nack-and-continue instead of unwinding the worker process.
        let raw_port = {
            let mut pool = self.port_pool.lock().await;
            let free = pool.free_slots();
            match pool.acquire() {
                Some(p) => p,
                None => {
                    // Bump the worker-level exhaustion counter so the
                    // CP's deploy-time 402 gate can detect saturation
                    // even for apps that exhausted the pool on first
                    // start and never produced an AppStatus row.
                    self.port_pool_exhausted_events
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::error!(
                        tenant_id,
                        app_name,
                        deployment_id = spec.deployment_id,
                        free_slots = free,
                        "port pool exhausted; refusing to start app (HTTP port unavailable)"
                    );
                    anyhow::bail!(
                        "port pool exhausted: cannot allocate HTTP port (free_slots={free})"
                    );
                }
            }
        };

        // Acquire a WebSocket port if the spec requests one via the
        // EDGE_WS_PORT env var. The guest is expected to listen on this
        // port via wasi:sockets for WebSocket upgrade traffic (issue #312).
        // Issue #641: on exhaustion we release raw_port so it isn't
        // leaked into a 60-second cooling-down limbo while the nack
        // and redeliver cycle runs.
        let wants_ws = spec.env.contains_key("EDGE_WS_PORT");
        let ws_port = if wants_ws {
            let mut pool = self.port_pool.lock().await;
            let free = pool.free_slots();
            match pool.acquire() {
                Some(p) => Some(p),
                None => {
                    // Same worker-level counter as the HTTP-port arm —
                    // both branches are "pool can't take more" signals
                    // that the CP needs to count.
                    self.port_pool_exhausted_events
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    pool.release(raw_port);
                    tracing::error!(
                        tenant_id,
                        app_name,
                        deployment_id = spec.deployment_id,
                        free_slots = free,
                        "port pool exhausted; refusing to start app (WS port unavailable, HTTP port released)"
                    );
                    anyhow::bail!(
                        "port pool exhausted: cannot allocate WS port (free_slots={free})"
                    );
                }
            }
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
                spec.signing_key_id.as_deref(),
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

        // Spawn the per-app resident-seconds ticker (issue #484).
        // LongRunning apps only — Handler (FaaS) apps don't contribute
        // resident time, so their resident_ticker stays None and the
        // build_heartbeat path stamps `resident_seconds = None`.
        // Mirrors the epoch ticker above: per-app scope (so a
        // misbehaving app can't poison another's accounting), abort
        // on stop (so the counter doesn't drift up after the app is
        // gone). The 30s cadence matches the default heartbeat
        // interval — finer granularity would 30× the atomic-op load
        // with zero new billing-grade signal.

        // Create request meter.
        let meter = Arc::new(RequestMeter::new(
            tenant_id.to_string(),
            spec.deployment_id.clone(),
        ));

        // Spawn the per-app resident-seconds ticker (issue #484).
        // LongRunning apps only — Handler (FaaS) apps don't contribute
        // resident time, so their resident_ticker stays None and the
        // build_heartbeat path stamps `resident_seconds = None`.
        // Mirrors the epoch ticker above: per-app scope (so a
        // misbehaving app can't poison another's accounting), abort
        // on stop (so the counter doesn't drift up after the app is
        // gone).
        let resident_ticker = if execution_model == ExecutionModel::LongRunning {
            let resident_meter = meter.clone();
            // TODO(metering): collapse 3 goroutines when drainer ships.
            // The resident-seconds ticker below is the third parallel
            // goroutine on the heartbeat path (alongside the existing
            // checkOutboundQuota / checkRequestCount goroutines in the
            // control plane, edge-control-plane/internal/service/worker.go).
            // Once the metering-ledger drainer replaces the per-axis
            // UPDATE path, this whole ticker can fold into the existing
            // per-app heart-beat handle.
            let resident_tick_secs = self.config.heartbeat_interval_secs;
            Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(resident_tick_secs)).await;
                    resident_meter.record_resident_seconds(resident_tick_secs);
                }
            }))
        } else {
            None
        };

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

        // Wire protocol for this app (issue #548). The control plane
        // stamps `EDGE_PROTOCOL` in `spec.env` based on `[project].protocol`
        // in the project's edge.toml: `"http"` for HTTP/WS long-running
        // and FaaS apps, `"tcp"` for raw-TCP L4 apps. We read it here
        // because the heartbeat needs the value before run_app_loop
        // starts the guest, and the env isn't visible to the supervisor
        // after we hand it to wasmtime. Defaults to "http" so legacy
        // AppSpec payloads without the env entry still work.
        let protocol = env
            .get("EDGE_PROTOCOL")
            .cloned()
            .unwrap_or_else(|| "http".to_string());

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

        // Per-app HostnamePinning cache. Constructed once per app
        // instance and shared into every FaaS dispatch via
        // HandlerConfig. Today this is dormant (the upstream
        // resolve hook hasn't merged), but the Arc layout is
        // forward-compatible: once the runtime starts populating
        // the cache during resolve_addresses, every in-flight
        // dispatch on the same app sees the entries.
        let hostname_pinning: Arc<edge_runtime::socket_egress::HostnamePinning> =
            Arc::new(edge_runtime::socket_egress::HostnamePinning::new());

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
                socket_mode_for_app: socket_mode_for_spec(spec, self.config.socket_mode),
                hostname_pinning_enabled: self.config.hostname_pinning_enabled,
                hostname_pinning: hostname_pinning.clone(),
                last_request_at: Arc::new(tokio::sync::Mutex::new(Some(std::time::Instant::now()))),
                max_memory_mb: spec.max_memory_mb,
                cpu_budget_ms: spec
                    .cpu_budget_ms
                    .unwrap_or(self.config.handler_request_budget_ms),
                // issue #308: forward preview metadata from TaskMessage so
                // the per-request RuntimeState scopes stores under
                // /preview-{id}/ and stamps EDGE_PREVIEW_PR_NUMBER.
                preview_id: spec.preview_id.clone(),
                preview_pr_number: spec.preview_pr_number,
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

            // Per-app socket mode (issue #412). The LongRunning path
            // uses the per-app value directly — it does NOT need the
            // FaaS compose rule with `hostname_pinning_enabled` because
            // the `HostnamePinned` arm stays dormant at the connect-side
            // closure for LongRunning (the upstream wasmtime-wasi resolve
            // hook that would populate the cache isn't wired here). See
            // `dispatch::handle_request` for the parallel FaaS rule.
            let socket_mode_for_loop = socket_mode_for_spec(spec, self.config.socket_mode);
            // Preview-environment IDs (issue #308) — threaded into the
            // runtime so the per-tenant persistent stores scope under
            // a `/preview-{id}/` subdirectory and the guest env sees
            // `EDGE_PREVIEW_PR_NUMBER`.
            let preview_id_for_loop = spec.preview_id.clone();
            let preview_pr_number_for_loop = spec.preview_pr_number;
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
                    preview_id_for_loop.clone(),
                    preview_pr_number_for_loop,
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
            instance_pre,
            handle: Some(std::sync::Arc::new(handle)),
            ticker: Some(ticker),
            resident_ticker,
            execution_model,
            dispatch,
            metrics_acc,
            ws_port,
            // Read from `EDGE_PROTOCOL` env stamped by the control
            // plane (issue #548). Defaults to "http" via the
            // `.unwrap_or_else` at the env-merge site above. The
            // heartbeat builder reads `inst.protocol` to stamp
            // `AppStatus.protocol` so the ingress can route HTTP vs
            // L4 apps through the correct Caddy servers.
            protocol: protocol.clone(),
            last_error: None,
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

    /// Stop an app gracefully with a drain phase.
    ///
    /// 1. Set status to `Draining` — heartbeat reports "draining" with
    ///    weight=0, ingress sends no new traffic.
    /// 2. Signal `serve()` to stop accepting new connections (broadcast).
    /// 3. Wait for in-flight requests to complete (up to 30s).
    /// 4. Set status to `Stopping`, remove from state map, free port.
    /// 5. Abort ticker and await/abort the app task.
    pub async fn stop_app(&self, tenant_id: &str, app_name: &str) -> anyhow::Result<()> {
        let key = (tenant_id.to_string(), app_name.to_string());
        // Clone the Arc so we can lock it while the instance is still in the map.
        let instance = {
            let state = self.state.read().await;
            state.apps.get(&key).cloned()
        };

        let (port, ws_port, handle, ticker, resident_ticker, _dispatch) =
            if let Some(inst) = instance {
                // Phase 1: set Draining, signal serve() to stop accepting,
                // then drain in-flight requests.
                let mut inst = inst.lock().await;
                inst.status = AppInstanceStatus::Draining;
                let port = inst.port;
                let ws_port = inst.ws_port;
                let handle = inst.handle.clone();
                let ticker = inst.ticker.take();
                // Take the resident-seconds ticker (issue #484) out of
                // the locked struct so we can abort it after the app
                // exits. Without abort, the ticker would keep firing
                // every 30s on a stopped app, drifting the
                // `meter.resident_seconds` counter past the heartbeat
                // already-published value.
                let resident_ticker = inst.resident_ticker.take();
                let broadcast_tx = inst.shutdown_tx_broadcast.take();
                let dispatch = inst.dispatch.clone();
                drop(inst);

                // Signal serve() to stop accepting new connections.
                if let Some(tx) = broadcast_tx {
                    let _ = tx.send(());
                }

                // Phase 2: wait for in-flight requests to drain (up to 30s).
                if let Some(ref d) = dispatch {
                    let drained = d.drain_in_flight(Duration::from_secs(30)).await;
                    if !drained {
                        tracing::warn!(
                            tenant_id = %tenant_id,
                            app_name = %app_name,
                            "drain timeout reached — forcing stop"
                        );
                    }
                }

                // Set stopping status after drain.
                {
                    let state = self.state.read().await;
                    if let Some(stopping_inst) = state.apps.get(&key) {
                        let mut stopping_inst = stopping_inst.lock().await;
                        stopping_inst.status = AppInstanceStatus::Stopping;
                    }
                }

                (port, ws_port, handle, ticker, resident_ticker, dispatch)
            } else {
                return Ok(()); // already gone
            };

        // Remove from the map.
        self.state.write().await.apps.remove(&key);

        // Free both the primary app port AND the dedicated WebSocket
        // listener port (issue #448 — previously only `port` was
        // released; `ws_port` leaked until the 60-second cooldown
        // eventually returned it. Burst WS-app redeploys exhausted
        // the PortPool in CI under the old behavior.).
        {
            let mut pool = self.port_pool.lock().await;
            pool.release(port);
            if let Some(ws) = ws_port {
                pool.release(ws);
            }
        }

        // Abort the epoch ticker so the engine clock stops advancing for
        // this app. The ticker's task is a tight loop that holds a clone
        // of the engine; without abort, it would run forever (or until
        // the engine is dropped), wasting CPU and incrementing the epoch
        // for stopped apps.
        if let Some(t) = ticker {
            t.abort();
        }

        // Abort the resident-seconds ticker (issue #484). Same
        // rationale as the epoch ticker above — without abort, the
        // ticker would keep firing every 30s on a stopped app and
        // drift `meter.resident_seconds` past the heartbeat delta
        // we already published.
        if let Some(t) = resident_ticker {
            t.abort();
        }

        // Surface any panic from the app task in a structured log, but
        // do NOT re-raise (issue #45). Two failure modes to
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
        //     exit, or the host task failed. We previously re-raised
        //     via `panic::resume_unwind`, but that unwound out of
        //     `stop_app` into `handle_task_message` /
        //     `run_consume_loop` and tore down the worker process,
        //     killing every other app on the same node. Now we log
        //     the panic payload at `error!` level and let the
        //     supervisor keep running.
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
                    // Real panic (issue #45). `try_into_panic()`
                    // returns the original `Box<dyn Any + Send>`
                    // payload; we LOG it (Debug form, since the
                    // payload type is unknown) and continue the
                    // stop sequence. Re-raising via
                    // `panic::resume_unwind` would unwind out of
                    // `stop_app` into `handle_task_message` /
                    // `run_consume_loop` and crash the worker
                    // process — taking every other app on this
                    // worker with it.
                    if let Ok(panic_payload) = join_err.try_into_panic() {
                        tracing::error!(
                            tenant_id = %tenant_id,
                            app_name = %app_name,
                            panic_payload = ?panic_payload,
                            "app task panicked during stop; supervisor continuing"
                        );
                    }
                }
            }
        }

        tracing::info!(tenant_id, app_name, "app stopped");
        Ok(())
    }

    /// Shared crash-handling arm for `run_app_loop` (issue #45).
    ///
    /// Called from BOTH the `Ok(Ok(Err(e)))` arm (wasm trap from the
    /// guest) and the `Ok(Err(join_err))` arm (panic inside the
    /// spawned `execute_app` task) after `restart_count` has been
    /// incremented, AND from the `Err(_elapsed)` arm (health-check
    /// timeout → Hung), which passes the terminal `AppInstanceStatus`
    /// variant via `terminal_status`. Returns `true` iff the
    /// supervisor has hit the restart cap and the caller should
    /// `break` out of `run_app_loop`: the app is flipped to the
    /// supplied terminal status, an auto-rollback POST is fired
    /// (best-effort), and the loop terminates. Returns `false` when a
    /// restart is allowed — the caller sleeps the computed backoff
    /// and loops.
    ///
    /// Splitting this out keeps the three arms in `run_app_loop`
    /// legible and prevents the auto-rollback POST + status flip
    /// from drifting between the trap, panic-in-spawn, and Hung code
    /// paths. Add new failure classes (OOM, store-limit-exceeded,
    /// etc.) by passing a different `terminal_status` and a matching
    /// `log_msg` — do NOT reintroduce inline copies.
    #[allow(clippy::too_many_arguments)]
    async fn handle_app_crash(
        state: &Arc<RwLock<WorkerState>>,
        tenant_id: &str,
        app_name: &str,
        current_deployment_id: &str,
        downloader: &Arc<Downloader>,
        restart_count: u32,
        max_restarts: u32,
        base_backoff: Duration,
        max_backoff: Duration,
        terminal_status: AppInstanceStatus,
        err_for_audit: Option<&str>,
        log_msg: &'static str,
    ) -> bool {
        if restart_count >= max_restarts {
            tracing::error!(restart_count, "max restarts exceeded, giving up");
            // Mark the app with the supplied terminal status AND
            // stamp the last error so the heartbeat can carry it
            // (issue #45 — operators see *why* the app reached
            // Crashed without grepping structured logs).
            {
                let mut s = state.write().await;
                let crash_key = (tenant_id.to_string(), app_name.to_string());
                if let Some(inst) = s.apps.get_mut(&crash_key) {
                    let mut inst = inst.lock().await;
                    inst.status = terminal_status;
                    if let Some(err) = err_for_audit {
                        inst.last_error = Some(err.to_string());
                    }
                }
            }
            // Best-effort auto-rollback: signal the control plane so
            // it can swap the active deployment back to last_good. We
            // do NOT block the per-app task on this — `spawn`
            // detaches the POST so the loop can return immediately.
            // The user's manual `edge rollback` covers the failure
            // mode if the POST fails.
            let dl = downloader.clone();
            let tenant = tenant_id.to_string();
            let name = app_name.to_string();
            let dep = current_deployment_id.to_string();
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
            true
        } else {
            // Stamp last_error on every restart too — operators
            // watching the heartbeat in real time can see the error
            // immediately, before the cap is reached.
            if let Some(err) = err_for_audit {
                let mut s = state.write().await;
                let crash_key = (tenant_id.to_string(), app_name.to_string());
                if let Some(inst) = s.apps.get_mut(&crash_key) {
                    let mut inst = inst.lock().await;
                    inst.last_error = Some(err.to_string());
                }
            }
            let backoff = std::cmp::min(base_backoff * 2u32.pow(restart_count - 1), max_backoff);
            tracing::warn!(
                restart_count,
                backoff_secs = backoff.as_secs(),
                "{log_msg}; restarting"
            );
            false
        }
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
        preview_id: Option<String>,
        preview_pr_number: Option<u32>,
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
                //
                // Issue #45: the spawn happens LAZILY inside the
                // select! future expression. This (a) avoids paying
                // the spawn cost on every loop iteration when the
                // shutdown arm wins, and (b) keeps the spawn inside
                // the future-polling context, so a panic inside
                // `execute_app` surfaces as a `JoinError` on the
                // outer future rather than unwinding out of
                // `run_app_loop` (which would crash the worker
                // process via `stop_app`'s `panic::resume_unwind`).
                // The match arms below route the new `Ok(Err(join_err))`
                // variant through the same restart-or-Crashed path
                // as a wasm trap.
                //
                // `tokio::spawn` requires `'static`, so we clone
                // every borrow into an owned form. `instance_pre`
                // is already `Clone`-able (it's a wasmtime
                // `Component`-derived pre-instance) and `meter` is
                // wrapped in `Arc`, so the cost is one Arc bump per
                // restart. `env`, `allowlist`, and `metrics_acc`
                // are owned (no borrow), so we move them into the
                // async block.
                result = tokio::time::timeout(
                    Duration::from_secs(health_check_timeout_secs),
                    tokio::spawn({
                        let instance_pre = instance_pre.clone();
                        let meter = Arc::clone(&meter);
                        let tenant_id = tenant_id.clone();
                        let app_name = app_name.clone();
                        let log_forwarder = Arc::clone(&log_forwarder);
                        let preview_id = preview_id.clone();
                        let env = env.clone();
                        let allowlist = allowlist.clone();
                        let metrics_acc = metrics_acc.clone();
                        async move {
                            Self::execute_app(
                                &instance_pre,
                                &meter,
                                env,
                                max_memory_mb,
                                epoch_deadline_ticks,
                                &tenant_id,
                                allowlist,
                                &app_name,
                                &log_forwarder,
                                metrics_acc,
                                socket_mode,
                                preview_id.as_deref(),
                                preview_pr_number,
                            )
                            .await
                        }
                    }),
                ) => {
                    match result {
                        Ok(Ok(Ok(true))) => {
                            // Component wants to keep running (blocking call returned normally).
                            // Loop back and re-execute — this supports long-running HTTP servers.
                            continue;
                        }
                        Ok(Ok(Ok(false))) => {
                            // Guest explicitly called process.exit — clean exit.
                            tracing::info!("component exited normally");
                            break;
                        }
                        Ok(Ok(Err(e))) => {
                            // Wasm trap or runtime error — treat as crash.
                            restart_count += 1;
                            let terminal = Self::handle_app_crash(
                                &state,
                                &tenant_id,
                                &app_name,
                                &current_deployment_id,
                                &downloader,
                                restart_count,
                                max_restarts,
                                base_backoff,
                                max_backoff,
                                AppInstanceStatus::Crashed { restart_count },
                                Some(&e.to_string()),
                                "app crashed (trap)",
                            )
                            .await;
                            if terminal {
                                break;
                            }
                            let backoff = std::cmp::min(
                                base_backoff * 2u32.pow(restart_count - 1),
                                max_backoff,
                            );
                            sleep(backoff).await;
                        }
                        Ok(Err(join_err)) => {
                            // Issue #45: execute_app panicked inside
                            // the spawned task. Convert to a synthetic
                            // trap-shaped error so the existing
                            // restart / Crashed arm applies uniformly.
                            // `join_err.is_panic()` disambiguates a
                            // host panic from cancellation; both
                            // routes land on the same restart counter
                            // and `Crashed` status when the cap is
                            // exceeded.
                            let payload = if join_err.is_panic() {
                                format!("app task panicked: {join_err}")
                            } else {
                                format!("app task aborted: {join_err}")
                            };
                            tracing::error!(
                                tenant_id = %tenant_id,
                                app_name = %app_name,
                                panic_payload = %payload,
                                "app task panicked inside run_app_loop; routing through restart/Crashed"
                            );
                            restart_count += 1;
                            let terminal = Self::handle_app_crash(
                                &state,
                                &tenant_id,
                                &app_name,
                                &current_deployment_id,
                                &downloader,
                                restart_count,
                                max_restarts,
                                base_backoff,
                                max_backoff,
                                AppInstanceStatus::Crashed { restart_count },
                                Some(&payload),
                                "app crashed (panic-in-spawn)",
                            )
                            .await;
                            if terminal {
                                break;
                            }
                            let backoff = std::cmp::min(
                                base_backoff * 2u32.pow(restart_count - 1),
                                max_backoff,
                            );
                            sleep(backoff).await;
                        }
                        Err(_elapsed) => {
                            // Health check timeout — app hung. Same
                            // restart-or-terminal machinery as the
                            // trap and panic arms, just with a
                            // different terminal status (Hung, not
                            // Crashed) so the heartbeat distinguishes
                            // "guest stopped yielding" from "guest
                            // trapped".
                            restart_count += 1;
                            let terminal = Self::handle_app_crash(
                                &state,
                                &tenant_id,
                                &app_name,
                                &current_deployment_id,
                                &downloader,
                                restart_count,
                                max_restarts,
                                base_backoff,
                                max_backoff,
                                AppInstanceStatus::Hung,
                                None,
                                "app hung (health check timeout)",
                            )
                            .await;
                            if terminal {
                                break;
                            }
                            let backoff = std::cmp::min(
                                base_backoff * 2u32.pow(restart_count - 1),
                                max_backoff,
                            );
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
        preview_id: Option<&str>,
        preview_pr_number: Option<u32>,
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
        let runtime_state = edge_runtime::RuntimeState::with_env_and_meter_preview(
            env,
            Some(Arc::clone(meter)),
            tenant_id.to_string(),
            app_name,
            preview_id,
            preview_pr_number,
            egress,
            log_forwarder.clone() as Arc<dyn edge_runtime::interfaces::observe::LogSink>,
            app_ctx,
            metrics_acc,
            socket_mode,
            // Dormant today (the upstream resolve hook in
            // docs/upstream-wasmtime-resolve-check.patch hasn't merged).
            // Per-RuntimeState clones share this Arc so future
            // mid-flight cache writes reach every clone.
            Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
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

        // `start` is the canonical top-level export of the
        // long-running `edge-runtime` world (wit/edge-cloud.wit:124-148).
        // Components implement it via wit-bindgen's `impl Guest for ...`
        // and the `export!` macro; the resulting component's top-level
        // export is named `start`. The previous `_start` symbol
        // came from wasi-snapshot-preview1 reactor components; the
        // canonical v0.2 world uses `start` instead. Must use
        // `call_async` for the same reason as `instantiate_async`
        // above — wasmtime rejects sync `call` on a store built with
        // `async_support(true)`.
        instance
            .get_typed_func::<(), ()>(&mut store, "start")?
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

        // Wall-clock at heartbeat time, used for the dedupe_id stamped on
        // each AppStatus (issue #418). The control plane caches these IDs
        // and skips re-applying deltas on JetStream redelivery within the
        // same bucket. Same bucket = same ID; same ID = skip.
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

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
                    // Wire protocol this app speaks (issue #548). Set
                    // by `start_app` from the `EDGE_PROTOCOL` env var
                    // (stamped by the control plane based on
                    // `[project].protocol` in edge.toml). HTTP apps
                    // stamp "http"; L4 raw-TCP apps stamp "tcp". The
                    // ingress reads this to route HTTP apps to the
                    // existing Caddy `reverse_proxy` and L4 apps to the
                    // new `apps.layer4` server. Defaults to "http" for
                    // legacy workers that don't carry the field.
                    protocol: inst.protocol.clone(),
                    // Stamp the dedupe ID at heartbeat build time so two
                    // redeliveries of the same heartbeat carry the same
                    // ID and the CP can dedupe them (issue #418).
                    dedupe_id: Some(dedupe_id(
                        &self.config.worker_id,
                        &inst.deployment_id,
                        now_unix_secs,
                    )),
                    // Surface the last error stamped by
                    // `Supervisor::handle_app_crash` on every crash /
                    // panic-in-spawn / trap (issue #45). Operators see
                    // *why* the app is `status: "crashed"` without
                    // grepping structured logs. `None` for healthy
                    // apps; persists across heartbeats until the next
                    // `start_app` clears it (currently it does not —
                    // a redeploy retains the prior error string until
                    // the first crash, which is the safer default).
                    last_error: inst.last_error.clone(),
                    // Third metered dimension (issue #484): only
                    // LongRunning apps contribute resident-seconds —
                    // the per-app resident ticker is only spawned for
                    // LR. Handler (FaaS) apps stamp None so the CP's
                    // applyTenantDelta treats them as a zero
                    // contribution without ever calling
                    // AddResidentSeconds. Reading from the same
                    // `snap` keeps the three counters atomically
                    // consistent (no TOCTOU at heartbeat time).
                    resident_seconds: if inst.execution_model == ExecutionModel::LongRunning {
                        Some(snap.resident_seconds)
                    } else {
                        None
                    },
                    // Fourth metered dimension (issue #555): Handler
                    // (FaaS) request duration in milliseconds. The
                    // dispatch path stamps
                    // `meter.record_duration(elapsed)` in each of the
                    // four terminal arms of `handle_request`. LongRunning
                    // apps leave this at 0 (the dispatch path never
                    // stamps for LR) — the control plane applies zero
                    // contribution via `checkComputeMs` either way.
                    // Reading from the same `snap` keeps the four
                    // counters atomically consistent (no TOCTOU at
                    // heartbeat time), same posture as
                    // `outbound_bytes`/`resident_seconds`.
                    duration_ms_total: snap.duration_ms,
                },
            );
        }

        // Populate cluster headroom for the autoscaler (issue #85) and the
        // deploy-time 402 gate (issue #641). `free_slots` is mirrored
        // onto both `app_slots` (autoscaler) and `free_slots` (deploy
        // gate) so the two consumers can pick the name that matches
        // their semantics without coordinating on a rename.
        let free_slots = self.port_pool.lock().await.free_slots();
        msg.cluster_headroom = Some(ClusterHeadroom {
            cpu_pct: None,
            mem_pct: None,
            app_slots: free_slots,
            free_slots,
        });

        // Stamp the worker-level exhaustion counter (issue #641) so the
        // CP can persist it onto `worker_status.port_pool_exhausted_count`.
        // Reset on worker process restart (per-process-boot cumulative).
        msg.port_pool_exhausted_count = self
            .port_pool_exhausted_events
            .load(std::sync::atomic::Ordering::Relaxed);

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
                // Third metered dimension (issue #484): subtract the
                // resident-seconds delta we just shipped. FaaS apps
                // stamp `resident_seconds = None` so the
                // `unwrap_or(0)` folds them to a no-op — same
                // shape as a LongRunning app that started within the
                // current interval.
                inst.meter
                    .subtract_resident_seconds(status.resident_seconds.unwrap_or(0));
                // Fourth metered dimension (issue #555): subtract the
                // FaaS duration-ms delta we just shipped. LongRunning
                // apps stamp `duration_ms_total = 0` (the dispatch
                // path never fires for LR), so the subtract folds
                // them to a no-op — same shape as the
                // `resident_seconds.unwrap_or(0)` call above. The
                // deployment-mismatch guard at line ~2920 already
                // short-circuits all four subtractions together when
                // a heartbeat arrives for a stale deployment.
                inst.meter.subtract_duration_ms(status.duration_ms_total);
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
        AppInstanceStatus::Draining => "draining",
        AppInstanceStatus::Stopping => "stopping",
        AppInstanceStatus::Crashed { .. } => "crashed",
        AppInstanceStatus::Hung => "hung",
    }
}

/// Map an AppInstanceStatus to its heartbeat exit_code.
pub fn app_status_exit_code(status: &AppInstanceStatus) -> Option<i32> {
    match status {
        AppInstanceStatus::Running
        | AppInstanceStatus::Starting
        | AppInstanceStatus::Draining
        | AppInstanceStatus::Stopping => None,
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
            signing_key_id: None,
            routes: None,
            env: HashMap::new(),
            allowlist: None,
            socket_mode: None,
            max_memory_mb: 256,
            cpu_budget_ms: None,
            preview_id: None,
            preview_pr_number: None,
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
    fn status_to_string_draining() {
        assert_eq!(
            app_status_to_string(&AppInstanceStatus::Draining),
            "draining"
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
            socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            last_request_at: Arc::new(tokio::sync::Mutex::new(Some(
                std::time::Instant::now() - std::time::Duration::from_secs(10),
            ))),
            max_memory_mb: 256,
            cpu_budget_ms: 1000,
            // issue #308: defaults for unit tests; specific tests override.
            preview_id: None,
            preview_pr_number: None,
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
            socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            last_request_at: Arc::new(tokio::sync::Mutex::new(Some(std::time::Instant::now()))),
            max_memory_mb: 256,
            cpu_budget_ms: 1000,
            // issue #308: defaults for unit tests; specific tests override.
            preview_id: None,
            preview_pr_number: None,
        };

        let downloader = Arc::new(crate::downloader::Downloader::new(
            "http://localhost".to_string(),
            std::path::PathBuf::from("/tmp"),
            crate::auth::WorkerJwtSigner::new(vec![], None, "", "", "", ""),
            None,
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
            instance_pre: Some(instance_pre.clone()),
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: Some(dispatch_a),
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
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
            instance_pre: Some(instance_pre.clone()),
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: Some(dispatch_b),
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
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

    // ── StandbyPool size-zero and release-to-full tests ─────────────

    #[tokio::test]
    async fn test_standby_pool_new_size_zero() {
        // size=0 should be clamped to 1
        let pool = StandbyPool::new(0).expect("pool with size 0");
        let engine = edge_runtime::create_engine().expect("engine");
        let state = RwLock::new(WorkerState::new(engine));
        let _e = pool.acquire(&state).await;
    }

    #[tokio::test]
    async fn test_standby_pool_release_to_full_pool() {
        let pool = StandbyPool::new(2).expect("pool");
        let engine = edge_runtime::create_engine().expect("engine");
        let state = RwLock::new(WorkerState::new(engine));

        // Drain both engines from the pool
        let e1 = pool.acquire(&state).await;
        let e2 = pool.acquire(&state).await;

        // Release both back — second release fills the channel, third is silent drop
        pool.release(e1);
        pool.release(e2);

        // Acquire one — should succeed without timeout since an engine was released
        let start = std::time::Instant::now();
        let _reacquired = pool.acquire(&state).await;
        assert!(
            start.elapsed().as_millis() < 450,
            "should not timeout after release"
        );
    }

    // ── allowlist_to_egress_policy tests ────────────────────────────

    #[test]
    fn allowlist_none_is_allow_all() {
        let policy = allowlist_to_egress_policy(&None);
        // allow_all policy should pass any URL
        assert!(policy.check("https://example.com").is_ok());
    }

    #[test]
    fn allowlist_some_allows_matching() {
        let policy = allowlist_to_egress_policy(&Some(vec!["example.com".into()]));
        assert!(policy.check("https://example.com/api").is_ok());
        assert!(policy.check("https://evil.com").is_err());
    }

    #[test]
    fn allowlist_empty_denies_all() {
        let policy = allowlist_to_egress_policy(&Some(vec![]));
        assert!(policy.check("https://example.com").is_err());
    }

    // ── socket_mode_for_spec (issue #412) ──────────────────────────────

    use edge_runtime::socket_egress::SocketEgressPolicy;

    /// `spec.socket_mode = Some(AllowList)` overrides the worker-wide
    /// default. The FaaS dispatch site picks up this value via
    /// `HandlerConfig::socket_mode_for_app`.
    #[test]
    fn socket_mode_for_spec_some_uses_per_app() {
        let mut spec = make_spec("d_per_app");
        spec.socket_mode = Some(SocketEgressPolicy::AllowList);
        let mode = socket_mode_for_spec(&spec, SocketEgressPolicy::BlockAll);
        assert_eq!(mode, SocketEgressPolicy::AllowList);
    }

    /// `spec.socket_mode = None` falls back to the worker-wide default.
    /// This is the pre-#412 behavior; pre-#412 control planes that
    /// don't emit the field must keep working.
    #[test]
    fn socket_mode_for_spec_none_uses_worker_default() {
        let spec = make_spec("d_default");
        assert_eq!(spec.socket_mode, None);
        let mode = socket_mode_for_spec(&spec, SocketEgressPolicy::AllowAll);
        assert_eq!(mode, SocketEgressPolicy::AllowAll);
    }

    /// The helper unconditionally returns the per-app value, including
    /// `HostnamePinned`. The compose rule with the worker-wide
    /// `hostname_pinning_enabled` toggle is enforced at the FaaS
    /// dispatch site (`dispatch::handle_request`), not here. This test
    /// pins that contract so a future refactor doesn't accidentally
    /// move the compose rule into the helper.
    #[test]
    fn socket_mode_for_spec_hostname_pinned_does_not_self_validate() {
        let mut spec = make_spec("d_pinned");
        spec.socket_mode = Some(SocketEgressPolicy::HostnamePinned);
        let mode = socket_mode_for_spec(&spec, SocketEgressPolicy::BlockAll);
        assert_eq!(
            mode,
            SocketEgressPolicy::HostnamePinned,
            "helper must return HostnamePinned unconditionally; \
             the FaaS compose rule with hostname_pinning_enabled is a separate concern"
        );
    }

    fn fixture_pre(
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

    fn make_supervisor(state: Arc<RwLock<WorkerState>>) -> Arc<Supervisor> {
        let jwt = crate::auth::WorkerJwtSigner::new(
            String::new(),
            None,
            String::new(),
            "w_test",
            "fra",
            "t_test",
        );
        let nats = Arc::new(crate::nats::tests::MockNatsClient::new());
        Arc::new(Supervisor {
            config: Config {
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
                port_pool_size: 100,
                max_memory_mb: 256,
                epoch_tick_ms: 10,
                epoch_deadline_ticks: 100,
                consumer_name: "test".to_string(),
                queue_group: String::new(),
                task_stream_replicas: 1,
                worker_jwt_secret: String::new(),
                worker_jwt_kid: None,
                worker_jwt_issuer: String::new(),
                worker_bootstrap_secret: String::new(),
                worker_key_path: std::path::PathBuf::from("/tmp/worker-key"),
                worker_identity_path: std::path::PathBuf::from("/tmp/identity-key"),
                worker_reenroll_on_boot: false,
                handler_request_budget_ms: 1000,
                handler_max_request_body_bytes: 0,
                tls_cert_path: None,
                tls_key_path: None,
                socket_mode: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
                hostname_pinning_enabled: false,
                standby_pool_size: 1,
                require_signature: false,
                signing_keyring: None,
                signing_keyring_path: None,
            },
            state,
            downloader: Arc::new(Downloader::new(
                "http://localhost".to_string(),
                std::path::PathBuf::from("/tmp"),
                jwt.clone(),
                None,
            )),
            port_pool: Arc::new(Mutex::new(PortPool::new(10000, 60))),
            nats: nats as Arc<dyn NatsClient>,
            log_forwarder: LogForwarder::new("http://localhost:0", "w_test", "fra", jwt.clone()),
            jwt_signer: jwt,
            http: reqwest::Client::new(),
            engine_pool: Arc::new(StandbyPool::new(1).expect("pool")),
            port_pool_exhausted_events: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    // ── evict_idle_apps tests ───────────────────────────────────────────

    #[tokio::test]
    async fn evict_idle_skips_long_running() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19000,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d1".into())),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = make_supervisor(state.clone());
        sup.evict_idle_apps(Duration::from_secs(1)).await;
        assert_eq!(state.read().await.apps.len(), 1);
    }

    #[tokio::test]
    async fn evict_idle_skips_no_dispatch() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19001,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d1".into())),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = make_supervisor(state.clone());
        sup.evict_idle_apps(Duration::from_secs(1)).await;
        assert_eq!(state.read().await.apps.len(), 1);
    }

    #[tokio::test]
    async fn evict_idle_skips_no_last_request() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let cfg = HandlerConfig {
            tenant_id: "t_test".into(),
            egress: Arc::new(EgressPolicy::allow_all()),
            log_sink: Arc::new(edge_runtime::interfaces::observe::NoopLogSink),
            app_ctx: edge_runtime::interfaces::observe::AppLogContext {
                app_name: "my-app".into(),
                tenant_id: "t_test".into(),
                deployment_id: "d1".into(),
            },
            meter: Arc::new(RequestMeter::new("t_test".into(), "d1".into())),
            env: HashMap::new(),
            max_request_body_bytes: 0,
            metrics_acc: None,
            socket_mode_for_app: edge_runtime::socket_egress::SocketEgressPolicy::BlockAll,
            hostname_pinning_enabled: false,
            hostname_pinning: Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
            last_request_at: Arc::new(tokio::sync::Mutex::new(None)),
            cpu_budget_ms: 1000,
            max_memory_mb: 256,
            // issue #308: defaults for unit tests; specific tests override.
            preview_id: None,
            preview_pr_number: None,
        };
        let dispatch = Arc::new(
            HandlerDispatch::new(
                19002,
                1000,
                10,
                cfg,
                None,
                Arc::new(Downloader::new(
                    "http://localhost".to_string(),
                    std::path::PathBuf::from("/tmp"),
                    crate::auth::WorkerJwtSigner::new(
                        String::new(),
                        None,
                        String::new(),
                        "w",
                        "r",
                        "t",
                    ),
                    None,
                )),
                "d1".into(),
                Arc::new(StandbyPool::new(1).expect("pool")),
                state.clone(),
            )
            .expect("HandlerDispatch::new"),
        );
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19002,
            status: AppInstanceStatus::Running,
            meter: Arc::new(RequestMeter::new("t_test".into(), "d1".into())),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: Some(dispatch),
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = make_supervisor(state.clone());
        sup.evict_idle_apps(Duration::from_secs(1)).await;
        assert_eq!(state.read().await.apps.len(), 1);
    }

    // ── reset_meters_after tests ────────────────────────────────────────

    #[tokio::test]
    async fn reset_meters_after_subtracts_delta() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        meter.record_request();
        meter.record_request();
        meter.record_outbound_bytes(100);
        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19003,
            status: AppInstanceStatus::Running,
            meter: meter.clone(),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = make_supervisor(state.clone());
        let hb = sup.build_heartbeat().await;
        sup.reset_meters_after(&hb).await;
        let snap = meter.snapshot();
        assert_eq!(snap.request_count, 0);
        assert_eq!(snap.outbound_bytes, 0);
    }

    #[tokio::test]
    async fn reset_meters_after_deployment_mismatch_skips() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        meter.record_request();
        meter.record_request();

        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(), // initial deployment
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19004,
            status: AppInstanceStatus::Running,
            meter: meter.clone(),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);

        // Build heartbeat with current state (deployment_id="d1")
        let sup = make_supervisor(state.clone());
        let hb = sup.build_heartbeat().await;

        // Now simulate a new deployment replacing the app (deployment_id changes)
        // between build_heartbeat and reset_meters_after
        {
            let mut guard = state.write().await;
            let existing = guard
                .apps
                .get_mut(&("t_test".into(), "my-app".into()))
                .unwrap();
            let mut inst = existing.lock().await;
            inst.deployment_id = "d2".into(); // deployment changed!
        }

        // Reset with the stale heartbeat (which has deployment_id="d1")
        sup.reset_meters_after(&hb).await;
        let snap = meter.snapshot();
        assert_eq!(
            snap.request_count, 2,
            "meter should not be reset due to deployment_id mismatch"
        );
    }

    /// Issue #484, third metered dimension: `reset_meters_after` must
    /// also subtract the resident-seconds delta we just shipped.
    /// Mirrors `reset_meters_after_subtracts_delta` for the new axis —
    /// the heartbeat stamps `Some(60)` for a LongRunning app that
    /// accumulated 60s, and the call must zero the meter.
    #[tokio::test]
    async fn reset_meters_after_subtracts_resident_seconds_delta() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        meter.record_request();
        meter.record_request();
        meter.record_outbound_bytes(100);
        // LongRunning apps accumulate resident time via the per-app
        // ticker. Manually pre-load the counter here to simulate the
        // ticker having fired twice (2 × heartbeat_interval_secs = 60).
        meter.record_resident_seconds(60);

        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19005,
            status: AppInstanceStatus::Running,
            meter: meter.clone(),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = make_supervisor(state.clone());
        let hb = sup.build_heartbeat().await;
        // Sanity check: build_heartbeat stamped Some(60).
        let stamped = hb.apps.get("my-app").expect("app present");
        assert_eq!(
            stamped.resident_seconds,
            Some(60),
            "build_heartbeat must stamp resident_seconds for LR app"
        );
        sup.reset_meters_after(&hb).await;
        let snap = meter.snapshot();
        assert_eq!(snap.request_count, 0);
        assert_eq!(snap.outbound_bytes, 0);
        assert_eq!(
            snap.resident_seconds, 0,
            "resident-seconds delta must be subtracted after heartbeat publish"
        );
    }

    /// Issue #484 mirror of `reset_meters_after_deployment_mismatch_skips`:
    /// when deployment_id changes between build_heartbeat and
    /// reset_meters_after, the resident-seconds counter must NOT be
    /// subtracted. fetch_sub would wrap to u64::MAX.
    #[tokio::test]
    async fn reset_meters_after_resident_deployment_mismatch_skips() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        meter.record_request();
        meter.record_outbound_bytes(100);
        meter.record_resident_seconds(60);

        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(), // initial deployment
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19006,
            status: AppInstanceStatus::Running,
            meter: meter.clone(),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);

        // Build heartbeat with current state (deployment_id="d1")
        let sup = make_supervisor(state.clone());
        let hb = sup.build_heartbeat().await;

        // Now simulate a new deployment replacing the app (deployment_id changes)
        // between build_heartbeat and reset_meters_after.
        {
            let mut guard = state.write().await;
            let existing = guard
                .apps
                .get_mut(&("t_test".into(), "my-app".into()))
                .unwrap();
            let mut inst = existing.lock().await;
            inst.deployment_id = "d2".into(); // deployment changed!
        }

        // Reset with the stale heartbeat (which has deployment_id="d1")
        sup.reset_meters_after(&hb).await;
        let snap = meter.snapshot();
        assert_eq!(
            snap.resident_seconds, 60,
            "resident-seconds counter must NOT be reset on deployment_id mismatch (would wrap to u64::MAX)"
        );
    }

    // ── issue #555 FaaS duration tests ───────────────────────────────────

    /// Issue #555, fourth metered dimension: `build_heartbeat` must
    /// stamp `duration_ms_total` from the FaaS duration counter on
    /// the heartbeat wire. Mirrors
    /// `reset_meters_after_subtracts_resident_seconds_delta` for the
    /// new axis — the dispatch path stamps
    /// `meter.record_duration(elapsed)` per request, and
    /// `build_heartbeat` reads the running total into
    /// `AppStatus.duration_ms_total`.
    #[tokio::test]
    async fn reset_meters_after_subtracts_duration_ms_delta() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        meter.record_request();
        meter.record_request();
        meter.record_outbound_bytes(100);
        // Pre-load the duration counter to simulate two FaaS requests
        // with elapsed wall-clock times summing to 200ms (typical
        // small-handler workload).
        meter.record_duration(Duration::from_millis(120));
        meter.record_duration(Duration::from_millis(80));

        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19007,
            status: AppInstanceStatus::Running,
            meter: meter.clone(),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = make_supervisor(state.clone());
        let hb = sup.build_heartbeat().await;
        // Sanity check: build_heartbeat stamped 200ms onto the wire.
        let stamped = hb.apps.get("my-app").expect("app present");
        assert_eq!(
            stamped.duration_ms_total, 200,
            "build_heartbeat must stamp duration_ms_total for Handler app"
        );
        sup.reset_meters_after(&hb).await;
        let snap = meter.snapshot();
        assert_eq!(snap.request_count, 0);
        assert_eq!(snap.outbound_bytes, 0);
        assert_eq!(
            snap.duration_ms, 0,
            "duration_ms delta must be subtracted after heartbeat publish"
        );
    }

    /// Issue #555 mirror of
    /// `reset_meters_after_resident_deployment_mismatch_skips`: when
    /// deployment_id changes between build_heartbeat and
    /// reset_meters_after, the duration_ms counter must NOT be
    /// subtracted. fetch_sub would wrap to u64::MAX.
    #[tokio::test]
    async fn reset_meters_after_duration_deployment_mismatch_skips() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        meter.record_request();
        meter.record_outbound_bytes(100);
        meter.record_duration(Duration::from_millis(150));

        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(), // initial deployment
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19008,
            status: AppInstanceStatus::Running,
            meter: meter.clone(),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::Handler,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);

        // Build heartbeat with current state (deployment_id="d1")
        let sup = make_supervisor(state.clone());
        let hb = sup.build_heartbeat().await;

        // Now simulate a new deployment replacing the app (deployment_id changes)
        // between build_heartbeat and reset_meters_after.
        {
            let mut guard = state.write().await;
            let existing = guard
                .apps
                .get_mut(&("t_test".into(), "my-app".into()))
                .unwrap();
            let mut inst = existing.lock().await;
            inst.deployment_id = "d2".into(); // deployment changed!
        }

        // Reset with the stale heartbeat (which has deployment_id="d1")
        sup.reset_meters_after(&hb).await;
        let snap = meter.snapshot();
        assert_eq!(
            snap.duration_ms, 150,
            "duration_ms counter must NOT be reset on deployment_id mismatch (would wrap to u64::MAX)"
        );
    }

    /// Issue #555: LongRunning apps leave `duration_ms_total = 0` on
    /// every heartbeat (the dispatch path never stamps for LR — the
    /// per-app resident ticker handles LR's metered dimension via
    /// `resident_seconds`). The reset path must accept the zero as
    /// a no-op subtract (not wrap to u64::MAX), so the LR app's
    /// meter stays healthy across heartbeat intervals even though
    /// the dispatch path never touched `duration_ms`.
    #[tokio::test]
    async fn reset_meters_after_long_running_app_skips_duration_subtract() {
        let engine = edge_runtime::create_engine().expect("engine");
        let pre = Some(fixture_pre(&engine));
        let state = Arc::new(RwLock::new(WorkerState::new(engine)));
        let meter = Arc::new(RequestMeter::new("t_test".into(), "d1".into()));
        // LR app: resident_ticker fired 2× but the dispatch path never
        // stamped duration. duration_ms stays at 0 — the heartbeat's
        // duration_ms_total is therefore 0, the subtract is a no-op.
        meter.record_resident_seconds(60);

        let app = Arc::new(Mutex::new(AppInstance {
            deployment_id: "d1".into(),
            app_name: "my-app".into(),
            tenant_id: "t_test".into(),
            port: 19009,
            status: AppInstanceStatus::Running,
            meter: meter.clone(),
            shutdown_tx: None,
            shutdown_tx_broadcast: None,
            instance_pre: pre,
            handle: None,
            ticker: None,
            resident_ticker: None,
            execution_model: ExecutionModel::LongRunning,
            dispatch: None,
            metrics_acc: None,
            ws_port: None,
            protocol: "http".to_string(),
            last_error: None,
        }));
        state
            .write()
            .await
            .apps
            .insert(("t_test".into(), "my-app".into()), app);
        let sup = make_supervisor(state.clone());
        let hb = sup.build_heartbeat().await;
        let stamped = hb.apps.get("my-app").expect("app present");
        assert_eq!(
            stamped.duration_ms_total, 0,
            "LongRunning app must stamp duration_ms_total = 0"
        );
        assert_eq!(
            stamped.resident_seconds,
            Some(60),
            "LongRunning app must stamp resident_seconds = Some(60)"
        );
        sup.reset_meters_after(&hb).await;
        let snap = meter.snapshot();
        // Duration stays at 0 — the heartbeat reported 0, the reset
        // subtracts 0, the meter doesn't change. This is the
        // contract being tested.
        assert_eq!(
            snap.duration_ms, 0,
            "LongRunning app's duration_ms stays at 0 — subtract was a no-op"
        );
        // Resident-seconds IS subtracted — the heartbeat reported 60,
        // the reset subtracts 60, the meter drops back to 0 ready
        // for the next interval. Same posture as
        // `reset_meters_after_subtracts_resident_seconds_delta`.
        assert_eq!(
            snap.resident_seconds, 0,
            "LongRunning app's resident-seconds delta is subtracted after heartbeat publish"
        );
    }
}
