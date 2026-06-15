//! Integration tests for the edge-worker supervisor.
//!
//! These tests use testcontainers to spin up a real NATS server and exercise
//! the full Supervisor lifecycle: start_app → run_app_loop → stop_app.
//!
//! Run with: cargo test --manifest-path edge-worker/Cargo.toml
//!
//! Prerequisites: Docker must be running for testcontainers.
//!
//! Skip in CI with: SKIP_INTEGRATION_TESTS=1 cargo test ...

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerRequest;
use testcontainers::ImageExt;
use testcontainers_modules::nats::Nats;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::timeout;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use edge_worker::config::Config;
use edge_worker::downloader::Downloader;
use edge_worker::messages::{AppSpec, HeartbeatMessage, TaskMessage};
use edge_worker::nats::{NatsClient as NatsClientTrait, NatsClientImpl};
use edge_worker::port_pool::PortPool;
use edge_worker::state::{AppInstanceStatus, WorkerState};
use edge_worker::supervisor::Supervisor;

/// Returns true if integration tests should be skipped (e.g., in CI environments
/// where Docker is unavailable or unreliable for container tests).
fn should_skip_integration_tests() -> bool {
    std::env::var("SKIP_INTEGRATION_TESTS").is_ok()
        || std::env::var("CI").is_ok()
        || !std::path::Path::new("/var/run/docker.sock").exists()
}

/// Test WASM component bytes — a minimal component that exports `handle` and `_start`.
fn test_component_bytes() -> &'static [u8] {
    include_bytes!("fixtures/test-handle.wasm")
}

/// Timeout for subscribing to heartbeats.
const HEARTBEAT_SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum time to wait for the full test harness to start (container + NATS connection).
const HARNESS_STARTUP_TIMEOUT: Duration = Duration::from_secs(45);

// ---------------------------------------------------------------------------
// Test Harness
// ---------------------------------------------------------------------------

/// Collects all test infrastructure: NATS container, mock HTTP server, and a
/// Supervisor wired up with real NATS and a mock Downloader.
///
/// The struct owns the NATS container so it is dropped (and cleaned up) when
/// the test ends.
pub struct TestHarness {
    pub nats_url: String,
    pub mock_server: MockServer,
    pub supervisor: Arc<Supervisor>,
    _nats_container: testcontainers::ContainerAsync<Nats>,
}

impl TestHarness {
    /// Start NATS, spin up a mock HTTP server, create a Supervisor.
    ///
    /// Fails fast: if NATS container doesn't start within 45s, returns an error
    /// instead of hanging indefinitely.
    pub async fn new() -> anyhow::Result<Self> {
        timeout(HARNESS_STARTUP_TIMEOUT, Self::new_inner())
            .await
            .context("harness startup timed out")?
    }

    /// Inner constructor — actual setup logic. Wrapped by a timeout in `new()`.
    async fn new_inner() -> anyhow::Result<Self> {
        let (_nats_container, nats_url) = nats_container().await;
        let mock_server = MockServer::start().await;

        let config = Config {
            worker_id: "test-worker".to_string(),
            region: "test-region".to_string(),
            nats_url: nats_url.clone(),
            control_plane_url: mock_server.uri(),
            cache_dir: PathBuf::from("/tmp/edge-worker-test-cache"),
            heartbeat_interval_secs: 30,
            port_cooldown_secs: 60,
            starting_port: 18_000,
        };

        let engine = edge_runtime::create_engine()?;
        let state = Arc::new(tokio::sync::RwLock::new(WorkerState::new(engine)));
        let downloader = Arc::new(Downloader::new(
            config.control_plane_url.clone(),
            config.cache_dir.clone(),
        ));
        let port_pool = Arc::new(TokioMutex::new(PortPool::new(
            config.starting_port,
            config.port_cooldown_secs,
        )));

        let nats = Arc::new(NatsClientImpl::connect(&nats_url).await?)
            as Arc<dyn NatsClientTrait>;

        let supervisor = Arc::new(Supervisor {
            config,
            state,
            downloader,
            port_pool,
            nats,
        });

        Ok(Self {
            nats_url,
            mock_server,
            supervisor,
            _nats_container,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Start a NATS container and return (container, url).
///
/// Uses a simple duration-based wait instead of the built-in stderr matching,
/// which can be unreliable in CI where NATS log messages may appear out of order.
/// A hard startup_timeout bounds the total wait so the test fails fast on error.
async fn nats_container() -> (testcontainers::ContainerAsync<Nats>, String) {
    let container: testcontainers::ContainerAsync<Nats> = ContainerRequest::from(Nats::default())
        .with_startup_timeout(std::time::Duration::from_secs(30))
        .with_ready_conditions(vec![WaitFor::Duration { length: std::time::Duration::from_secs(5) }])
        .start()
        .await
        .expect("start NATS container");
    let host = container.get_host().await.expect("get host");
    let port = container.get_host_port_ipv4(4222).await.expect("get NATS port");
    (container, format!("{}:{}", host, port))
}

/// Helper: subscribe to heartbeats and collect the first one, with its own timeout.
async fn subscribe_heartbeats(nats_url: &str, region: &str) -> anyhow::Result<HeartbeatMessage> {
    let client = async_nats::connect(nats_url).await?;
    let subject = format!("edgecloud.heartbeats.{}", region);
    let mut sub = client.subscribe(subject).await?;
    let msg = timeout(HEARTBEAT_SUBSCRIBE_TIMEOUT, sub.next())
        .await
        .context("heartbeat subscription timed out")?
        .context("no heartbeat message received")?;
    let heartbeat =
        serde_json::from_slice::<HeartbeatMessage>(&msg.payload).context("parse heartbeat")?;
    Ok(heartbeat)
}

/// Helper: wait for an app to appear in state with Running status.
async fn wait_for_app_running(
    supervisor: &Supervisor,
    app_name: &str,
    timeout_secs: u64,
) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        let state = supervisor.state.read().await;
        if let Some(inst) = state.apps.get(app_name) {
            let inst = inst.lock().await;
            if matches!(inst.status, AppInstanceStatus::Running) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Helper: wait for an app to disappear from state.
async fn wait_for_app_gone(
    supervisor: &Supervisor,
    app_name: &str,
    timeout_secs: u64,
) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        let state = supervisor.state.read().await;
        if !state.apps.contains_key(app_name) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_app_lifecycle() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    // Wire up the mock HTTP server to serve the test component.
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_deploy_001"))
        .respond_with(
            ResponseTemplate::new(200).set_body_bytes(test_component_bytes()),
        )
        .mount(&harness.mock_server)
        .await;

    // Step 1: send TaskMessage to start an app
    let spec = AppSpec {
        deployment_id: "d_deploy_001".to_string(),
        deployment_hash: "abc123".to_string(),
        env: HashMap::new(),
        allowlist: vec![],
    };

    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-15T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("test-app".to_string(), spec)]),
    };

    harness
        .supervisor
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    // Step 2: app should be Running
    let running = wait_for_app_running(&harness.supervisor, "test-app", 10).await;
    assert!(
        running,
        "app should be Running within 10s (check NATS connectivity and component compilation)"
    );

    // Step 3: heartbeat should include the app
    let heartbeat = harness.supervisor.build_heartbeat().await;
    assert!(
        heartbeat.apps.contains_key("test-app"),
        "heartbeat should contain test-app"
    );
    let app_status = heartbeat.apps.get("test-app").unwrap();
    assert_eq!(app_status.status, "running", "app status should be 'running'");
    assert_eq!(
        app_status.deployment_id, "d_deploy_001",
        "deployment_id should match"
    );

    // Step 4: send empty TaskMessage to stop the app
    let stop_msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-15T00:00:01Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::new(),
    };
    harness
        .supervisor
        .handle_task_message(stop_msg)
        .await
        .expect("handle_task_message");

    // Step 5: app should be removed from state
    let gone = wait_for_app_gone(&harness.supervisor, "test-app", 10).await;
    assert!(gone, "app should be removed from state after stop");
}

#[tokio::test]
async fn test_heartbeat_published() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    timeout(HARNESS_STARTUP_TIMEOUT, test_heartbeat_published_inner())
        .await
        .expect("test_heartbeat_published timed out")
        .expect("test_heartbeat_published failed");
}

async fn test_heartbeat_published_inner() -> anyhow::Result<()> {
    let (container, nats_url) = nats_container().await;
    std::mem::forget(container); // keep alive for test; dropped when test fn returns

    let config = Config {
        worker_id: "test-worker".to_string(),
        region: "test-region".to_string(),
        nats_url: nats_url.clone(),
        control_plane_url: "http://localhost:9999".to_string(),
        cache_dir: PathBuf::from("/tmp/edge-worker-test-cache"),
        heartbeat_interval_secs: 30,
        port_cooldown_secs: 60,
        starting_port: 18_000,
    };

    let engine = edge_runtime::create_engine().context("create engine")?;
    let state = Arc::new(tokio::sync::RwLock::new(WorkerState::new(engine)));
    let downloader = Arc::new(Downloader::new(
        config.control_plane_url.clone(),
        config.cache_dir.clone(),
    ));
    let port_pool = Arc::new(TokioMutex::new(PortPool::new(
        config.starting_port,
        config.port_cooldown_secs,
    )));

    let nats = Arc::new(NatsClientImpl::connect(&nats_url).await.context("connect nats")?)
        as Arc<dyn NatsClientTrait>;

    let supervisor = Arc::new(Supervisor {
        config,
        state,
        downloader,
        port_pool,
        nats,
    });

    // Build and publish a heartbeat manually
    let heartbeat = supervisor.build_heartbeat().await;
    supervisor
        .nats
        .publish_heartbeat(&supervisor.config.region, &heartbeat)
        .await
        .context("publish heartbeat")?;

    // Subscribe and receive it
    let received = timeout(
        Duration::from_secs(5),
        subscribe_heartbeats(&nats_url, "test-region"),
    )
    .await
    .context("heartbeat subscription timed out")?
    .context("subscribe_heartbeats")?;

    assert_eq!(received.worker_id, "test-worker");
    assert_eq!(received.region, "test-region");
    Ok(())
}

#[tokio::test]
async fn test_stop_all_apps() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    // Wire up the mock HTTP server to serve the test component.
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_deploy_001"))
        .respond_with(
            ResponseTemplate::new(200).set_body_bytes(test_component_bytes()),
        )
        .mount(&harness.mock_server)
        .await;

    // Start two apps
    for i in 0..2 {
        let spec = AppSpec {
            deployment_id: format!("d_deploy_{:03}", i),
            deployment_hash: "abc123".to_string(),
            env: HashMap::new(),
            allowlist: vec![],
        };
        let msg = TaskMessage::TaskUpdate {
            timestamp: "2026-06-15T00:00:00Z".to_string(),
            tenant_id: "t_test".to_string(),
            apps: HashMap::from([(format!("app-{}", i), spec)]),
        };
        harness
            .supervisor
            .handle_task_message(msg)
            .await
            .expect("handle_task_message");
    }

    // Wait for both apps to be running (not a fixed sleep)
    for i in 0..2 {
        let running =
            wait_for_app_running(&harness.supervisor, &format!("app-{}", i), 10).await;
        assert!(
            running,
            "app-{} should be running within 10s",
            i
        );
    }

    let state = harness.supervisor.state.read().await;
    assert_eq!(state.apps.len(), 2, "two apps should be running");

    // stop_all_apps
    harness.supervisor.stop_all_apps().await;

    let state = harness.supervisor.state.read().await;
    assert!(state.apps.is_empty(), "all apps should be stopped");
}
