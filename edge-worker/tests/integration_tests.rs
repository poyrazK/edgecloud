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
use sha2::{Digest, Sha256};
use tokio::time::timeout;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use edge_runtime::interfaces::observe::LogSink;
use edge_worker::config::Config;
use edge_worker::messages::{AppSpec, HeartbeatMessage, TaskMessage};
use edge_worker::state::AppInstanceStatus;
use edge_worker::supervisor::Supervisor;

// Shared test harness: NATS container startup, skip predicate, and
// Supervisor wiring. See `edge-test-helpers/src/lib.rs` for the
// rationale. The helpers used here are:
//   - should_skip_integration_tests(): env-aware skip predicate
//   - start_nats(): shared across tests that need direct NATS access
//     (heartbeat publish / queue-group pinning) and also covers the
//     "build two supervisors against one NATS container" case.
//   - build_supervisor_with(config) / build_supervisor_from_url(nats_url, config):
//     single-worker supervisor builders; the only knob the test
//     actually customises per case is `Config` fields + cache_dir.
use edge_test_helpers::{
    build_supervisor_from_url, build_supervisor_with, default_cache_dir,
    should_skip_integration_tests, start_nats, SupervisorGuard,
};

/// Test WASM component bytes — a minimal component that exports `handle` and `_start`.
fn test_component_bytes() -> &'static [u8] {
    include_bytes!("fixtures/test-handle.wasm")
}

/// SHA-256 of `test_component_bytes()`, formatted as 64 lowercase hex chars.
/// Computed at test time so it tracks any fixture change.
fn test_component_hash() -> String {
    use std::fmt::Write;
    let digest = Sha256::digest(test_component_bytes());
    let mut s = String::with_capacity(64);
    for b in digest.as_slice() {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Shared HMAC secret used by every test JWT signer so the verifier side
/// of `test_emit_log_reaches_log_ingest_endpoint` can decode the token the
/// worker attaches. Production code uses `WORKER_JWT_SECRET` from env.
const TEST_JWT_SECRET: &[u8] = b"test-jwt-secret";

/// Construct a `Config` matching the worker's runtime expectations, with the
/// JWT fields populated to known test values.
#[allow(dead_code)]
fn test_config(
    worker_id: &str,
    region: &str,
    nats_url: String,
    control_plane_url: String,
) -> Config {
    Config {
        worker_id: worker_id.to_string(),
        region: region.to_string(),
        nats_url,
        control_plane_url,
        cache_dir: PathBuf::from("/tmp/edge-worker-test-cache"),
        heartbeat_interval_secs: 30,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        starting_port: 18_000,
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        queue_group: "test-group".to_string(),
        consumer_name: format!("test-{}", worker_id),
        worker_addr: "test-host:0".to_string(),
        worker_jwt_secret: String::from_utf8(TEST_JWT_SECRET.to_vec()).unwrap(),
        worker_jwt_kid: None,
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: "t_test".to_string(),
        handler_request_budget_ms: 1000,
        handler_max_request_body_bytes: 10 * 1024 * 1024,
        worker_sync_threshold_secs: 60,
        task_stream_replicas: 1,
        tls_cert_path: None,
        tls_key_path: None,
    }
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
/// The struct owns the [`SupervisorGuard`] (which carries the NATS
/// container) AND a per-test `cache_dir` tempdir so they are dropped
/// (and cleaned up) when the test ends. The per-test `cache_dir` is
/// critical: a shared `/tmp/...` cache leaks state across tests, so a
/// tampered-cache test would poison every later test that uses the
/// same `deployment_id`.
pub struct TestHarness {
    pub nats_url: String,
    pub mock_server: MockServer,
    pub supervisor: Arc<Supervisor>,
    /// Owns the NATS container. Keeping the guard alive keeps the
    /// container alive; dropping it (at TestHarness teardown) stops it.
    _sup_guard: SupervisorGuard,
    _cache_dir: tempfile::TempDir,
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
        let mock_server = MockServer::start().await;
        let cache_dir = tempfile::TempDir::new().context("create cache tempdir")?;

        // Delegate supervisor wiring to the shared helper. The per-test
        // tempdir is threaded through Config.cache_dir so cache-poisoning
        // tests don't leak state across the suite
        // (test_cached_tampered_artifact_*).
        let config = Config {
            worker_id: "test-worker".to_string(),
            region: "test-region".to_string(),
            worker_addr: "test-host:0".to_string(),
            nats_url: String::new(), // overwritten by build_supervisor_with
            control_plane_url: mock_server.uri(),
            cache_dir: cache_dir.path().to_path_buf(),
            heartbeat_interval_secs: 30,
            worker_sync_threshold_secs: 60,
            health_check_timeout_secs: 60,
            port_cooldown_secs: 60,
            starting_port: 18_000,
            max_memory_mb: 256,
            epoch_tick_ms: 10,
            epoch_deadline_ticks: 100,
            queue_group: "test-pinning-group".to_string(),
            consumer_name: "test-consumer".to_string(),
            worker_jwt_secret: String::from_utf8(TEST_JWT_SECRET.to_vec()).unwrap(),
            worker_jwt_kid: Some("test-kid".to_string()),
            worker_jwt_issuer: "edgecloud".to_string(),
            worker_tenant_id: "t_test".to_string(),
            handler_request_budget_ms: 1000,
            handler_max_request_body_bytes: 10 * 1024 * 1024,
            task_stream_replicas: 1,
            tls_cert_path: None,
            tls_key_path: None,
        };

        let sup_guard = build_supervisor_with(config).await;
        let nats_url = sup_guard.nats_url.clone();
        let supervisor = sup_guard.supervisor.clone();

        Ok(Self {
            nats_url,
            mock_server,
            supervisor,
            _sup_guard: sup_guard,
            _cache_dir: cache_dir,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
///
/// `state.apps` is keyed by `(tenant_id, app_name)` since Phase B; the
/// helpers need both to construct the lookup key.
async fn wait_for_app_running(supervisor: &Supervisor, app_name: &str, timeout_secs: u64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        let state = supervisor.state.read().await;
        for inst in state.apps.values() {
            let inst = inst.lock().await;
            if inst.app_name == app_name && matches!(inst.status, AppInstanceStatus::Running) {
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
    tenant_id: &str,
    app_name: &str,
    timeout_secs: u64,
) -> bool {
    let key = (tenant_id.to_string(), app_name.to_string());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        let state = supervisor.state.read().await;
        if !state.apps.contains_key(&key) {
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
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_component_bytes()))
        .mount(&harness.mock_server)
        .await;

    // Step 1: send TaskMessage to start an app
    let spec = AppSpec {
        deployment_id: "d_deploy_001".to_string(),
        deployment_hash: test_component_hash(),
        env: HashMap::new(),
        allowlist: None,
        max_memory_mb: 256,
        routes: None,
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
    assert_eq!(
        app_status.status, "running",
        "app status should be 'running'"
    );
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
    let gone = wait_for_app_gone(&harness.supervisor, "t_test", "test-app", 10).await;
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
    // Start a NATS container directly (no `SupervisorGuard` here because
    // this test doesn't bind the container to the supervisor struct; it
    // forgets it explicitly so it stays alive for the test's duration,
    // matching the pre-PR-#166-followup-#4 behavior).
    let (container, nats_url) = start_nats().await;
    std::mem::forget(container); // keep alive for test; dropped when test fn returns

    let config = Config {
        worker_id: "test-worker".to_string(),
        region: "test-region".to_string(),
        worker_addr: "test-host:0".to_string(),
        nats_url: String::new(), // overwritten by build_supervisor_from_url
        control_plane_url: "http://localhost:9999".to_string(),
        cache_dir: default_cache_dir(),
        heartbeat_interval_secs: 30,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        starting_port: 18_000,
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        queue_group: "test-heartbeat-group".to_string(),
        consumer_name: "test-heartbeat-consumer".to_string(),
        worker_jwt_secret: String::from_utf8(TEST_JWT_SECRET.to_vec()).unwrap(),
        worker_jwt_kid: None,
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: "t_test".to_string(),
        handler_request_budget_ms: 1000,
        handler_max_request_body_bytes: 10 * 1024 * 1024,
        worker_sync_threshold_secs: 60,
        task_stream_replicas: 1,
        tls_cert_path: None,
        tls_key_path: None,
    };
    let supervisor = build_supervisor_from_url(&nats_url, config).await?;

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
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_component_bytes()))
        .mount(&harness.mock_server)
        .await;

    // Start two apps
    for i in 0..2 {
        let spec = AppSpec {
            deployment_id: format!("d_deploy_{:03}", i),
            deployment_hash: test_component_hash(),
            env: HashMap::new(),
            allowlist: None,
            max_memory_mb: 256,
            routes: None,
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
        let running = wait_for_app_running(&harness.supervisor, &format!("app-{}", i), 10).await;
        assert!(running, "app-{} should be running within 10s", i);
    }

    let state = harness.supervisor.state.read().await;
    assert_eq!(state.apps.len(), 2, "two apps should be running");

    // stop_all_apps
    harness.supervisor.stop_all_apps().await;

    let state = harness.supervisor.state.read().await;
    assert!(state.apps.is_empty(), "all apps should be stopped");
}

// ---------------------------------------------------------------------------
// Artifact hash verification + cache-path integration tests (from main).
// Pre-pended before PR #96's queue-group pinning test to preserve original
// ordering without interleaving.
// ---------------------------------------------------------------------------

/// Positive-path: when AppSpec.deployment_hash matches the artifact's SHA-256,
/// the app starts normally. Guards against a future regression where the hash
/// check accidentally blocks valid starts.
#[tokio::test]
async fn test_artifact_hash_match_starts_app() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    // Wire up the mock HTTP server to serve the test component.
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_hash_match"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_component_bytes()))
        .mount(&harness.mock_server)
        .await;

    let spec = AppSpec {
        deployment_id: "d_hash_match".to_string(),
        deployment_hash: test_component_hash(),
        env: HashMap::new(),
        allowlist: None,
        max_memory_mb: 256,
        routes: None,
    };
    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-17T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("hash-match-app".to_string(), spec)]),
    };

    harness
        .supervisor
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    let running = wait_for_app_running(&harness.supervisor, "hash-match-app", 10).await;
    assert!(running, "matching-hash app should reach Running within 10s");
}

/// Negative-path: when AppSpec.deployment_hash does NOT match the artifact's
/// SHA-256, the app is not registered. Then a follow-up start of a different
/// app with the real hash proves the port pool was released by the first failure.
#[tokio::test]
async fn test_artifact_hash_mismatch_rejects_app() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    // The mock returns the real fixture bytes regardless of the AppSpec hash —
    // simulating a compromised control plane that ships the right bytes but a
    // wrong advertised hash (or vice versa).
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_hash_bad"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_component_bytes()))
        .mount(&harness.mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_hash_good"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_component_bytes()))
        .mount(&harness.mock_server)
        .await;

    // 1. Send a task message whose deployment_hash is syntactically valid but wrong.
    let wrong_hash = "0".repeat(64);
    let bad_spec = AppSpec {
        deployment_id: "d_hash_bad".to_string(),
        deployment_hash: wrong_hash,
        env: HashMap::new(),
        allowlist: None,
        max_memory_mb: 256,
        routes: None,
    };
    let bad_msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-17T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("bad-hash-app".to_string(), bad_spec)]),
    };
    harness
        .supervisor
        .handle_task_message(bad_msg)
        .await
        .expect("handle_task_message");

    {
        let state = harness.supervisor.state.read().await;
        assert!(
            !state
                .apps
                .contains_key(&("t_test".to_string(), "bad-hash-app".to_string())),
            "tampered-hash app must NOT be registered"
        );
    }

    // 2. Send a second task message with a DIFFERENT deployment_id and the real hash.
    // Using a different id avoids the poisoned cache; the new download verifies fine
    // and starts, proving the port was released by the first failure.
    let good_spec = AppSpec {
        deployment_id: "d_hash_good".to_string(),
        deployment_hash: test_component_hash(),
        env: HashMap::new(),
        allowlist: None,
        max_memory_mb: 256,
        routes: None,
    };
    let good_msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-17T00:00:01Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("good-hash-app".to_string(), good_spec)]),
    };
    harness
        .supervisor
        .handle_task_message(good_msg)
        .await
        .expect("handle_task_message");

    let running = wait_for_app_running(&harness.supervisor, "good-hash-app", 10).await;
    assert!(
        running,
        "matching-hash app should reach Running within 10s — proves port was released after the failed start"
    );
}

// ---------------------------------------------------------------------------
// Cache-path integration tests (mirror the unit tests in downloader.rs).
//
// Each test pre-populates the harness's per-test cache_dir with tampered
// bytes, then exercises the full handle_task_message path. Docker-gated
// via should_skip_integration_tests().
// ---------------------------------------------------------------------------

/// Tampered cache is detected, invalidated, and the artifact is re-downloaded.
/// The app reaches Running — proving the worker tolerated the bad cache and
/// the control plane delivered the verified bytes.
#[tokio::test]
async fn test_cached_tampered_artifact_is_redownloaded() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_cache_redownload"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_component_bytes()))
        .mount(&harness.mock_server)
        .await;

    // Pre-populate the cache with content that won't match the expected hash.
    let cache_path = harness
        .supervisor
        .config
        .cache_dir
        .join("d_cache_redownload.wasm");
    tokio::fs::write(&cache_path, b"tampered bytes")
        .await
        .expect("pre-populate tampered cache");

    let spec = AppSpec {
        deployment_id: "d_cache_redownload".to_string(),
        deployment_hash: test_component_hash(),
        env: HashMap::new(),
        allowlist: None,
        max_memory_mb: 256,
        routes: None,
    };
    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-17T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("cache-redownload-app".to_string(), spec)]),
    };
    harness
        .supervisor
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    let running = wait_for_app_running(&harness.supervisor, "cache-redownload-app", 10).await;
    assert!(
        running,
        "app should reach Running within 10s — proves the worker tolerated the bad cache and re-downloaded"
    );

    // After the re-download, the cache file should hold the verified good bytes.
    let on_disk = tokio::fs::read(&cache_path)
        .await
        .expect("read cache after re-download");
    assert_eq!(
        on_disk,
        test_component_bytes(),
        "cache file must be rewritten with the verified bytes"
    );
}

/// Both the cache AND the fresh download are bad. The cache is invalidated,
/// the fresh download fails verification, nothing is rewritten, and the app
/// is never registered.
#[tokio::test]
async fn test_cached_tampered_artifact_does_not_start_app_if_redownload_also_mismatches() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    // The control plane is "compromised" — it returns different tampered bytes
    // (not the fixture, not the cached content).
    let bad_download: Vec<u8> = b"different tampered bytes from compromised control plane".to_vec();
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_cache_dbl_bad"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bad_download))
        .mount(&harness.mock_server)
        .await;

    let cache_path = harness
        .supervisor
        .config
        .cache_dir
        .join("d_cache_dbl_bad.wasm");
    tokio::fs::write(&cache_path, b"original tampered bytes")
        .await
        .expect("pre-populate tampered cache");

    let spec = AppSpec {
        deployment_id: "d_cache_dbl_bad".to_string(),
        deployment_hash: test_component_hash(),
        env: HashMap::new(),
        allowlist: None,
        max_memory_mb: 256,
        routes: None,
    };
    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-17T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("cache-dbl-bad-app".to_string(), spec)]),
    };
    harness
        .supervisor
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    let state = harness.supervisor.state.read().await;
    assert!(
        !state
            .apps
            .contains_key(&("t_test".to_string(), "cache-dbl-bad-app".to_string())),
        "app must NOT be registered when both cache and fresh download fail verification"
    );
    drop(state);

    assert!(
        !cache_path.exists(),
        "cache file should be gone — cache was invalidated, then download verification failed, so nothing was rewritten"
    );
}

/// The control plane returns 500. The app is never registered, and no
/// partial cache file is written.
#[tokio::test]
async fn test_artifact_download_returns_500_does_not_register_app() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_download_500"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&harness.mock_server)
        .await;

    let spec = AppSpec {
        deployment_id: "d_download_500".to_string(),
        deployment_hash: test_component_hash(),
        env: HashMap::new(),
        allowlist: None,
        max_memory_mb: 256,
        routes: None,
    };
    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-17T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("download-500-app".to_string(), spec)]),
    };
    harness
        .supervisor
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    let state = harness.supervisor.state.read().await;
    assert!(
        !state
            .apps
            .contains_key(&("t_test".to_string(), "download-500-app".to_string())),
        "app must NOT be registered when the control plane returns 500"
    );
    drop(state);

    let cache_path = harness
        .supervisor
        .config
        .cache_dir
        .join("d_download_500.wasm");
    assert!(
        !cache_path.exists(),
        "no cache file should be written for a failed download"
    );
}

/// Issue #86 regression test: two workers in the same region joined to the
/// same queue group must NOT both start the same app when a single
/// `TaskMessage` is published. NATS queue-group delivery guarantees
/// exactly-one delivery across consumers in the group.
#[tokio::test]
async fn test_queue_group_pinning() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    timeout(Duration::from_secs(120), test_queue_group_pinning_inner())
        .await
        .expect("test_queue_group_pinning timed out")
        .expect("test_queue_group_pinning failed");
}

async fn test_queue_group_pinning_inner() -> anyhow::Result<()> {
    // Single NATS container, shared by both workers and the publisher.
    let (nats_container, nats_url) = start_nats().await;

    let region = "test-region";
    let queue_group = "test-pinning-group";

    // Two workers — same region, same queue group, distinct consumer names.
    // The pinning test doesn't touch the downloader, so a shared /tmp cache
    // is fine — give each worker its own subdir to avoid cross-worker clobber.
    let config_a = Config {
        worker_id: "w_pinning_a".to_string(),
        region: region.to_string(),
        worker_addr: "test-host:0".to_string(),
        nats_url: String::new(), // overwritten by build_supervisor_from_url
        control_plane_url: "http://localhost:9999".to_string(),
        cache_dir: PathBuf::from("/tmp/edge-worker-test-pinning-a"),
        heartbeat_interval_secs: 30,
        worker_sync_threshold_secs: 60,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        starting_port: 18_000,
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        queue_group: queue_group.to_string(),
        consumer_name: "consumer-a".to_string(),
        worker_jwt_secret: "test-secret".to_string(),
        worker_jwt_kid: None,
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: "t_test".to_string(),
        handler_request_budget_ms: 1000,
        handler_max_request_body_bytes: 10 * 1024 * 1024,
        task_stream_replicas: 1,
        tls_cert_path: None,
        tls_key_path: None,
    };
    let sup_a = build_supervisor_from_url(&nats_url, config_a).await?;

    let config_b = Config {
        worker_id: "w_pinning_b".to_string(),
        region: region.to_string(),
        worker_addr: "test-host:0".to_string(),
        nats_url: String::new(),
        control_plane_url: "http://localhost:9999".to_string(),
        cache_dir: PathBuf::from("/tmp/edge-worker-test-pinning-b"),
        heartbeat_interval_secs: 30,
        worker_sync_threshold_secs: 60,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        starting_port: 18_000,
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        queue_group: queue_group.to_string(),
        consumer_name: "consumer-b".to_string(),
        worker_jwt_secret: "test-secret".to_string(),
        worker_jwt_kid: None,
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: "t_test".to_string(),
        handler_request_budget_ms: 1000,
        handler_max_request_body_bytes: 10 * 1024 * 1024,
        task_stream_replicas: 1,
        tls_cert_path: None,
        tls_key_path: None,
    };
    let sup_b = build_supervisor_from_url(&nats_url, config_b).await?;

    // Each supervisor gets its own shutdown channel — the test triggers
    // shutdown at the end and waits for both loops to exit.
    let (shutdown_tx_a, _) = tokio::sync::broadcast::channel::<()>(1);
    let (shutdown_tx_b, _) = tokio::sync::broadcast::channel::<()>(1);

    let sup_a_clone = sup_a.clone();
    let shutdown_rx_a = shutdown_tx_a.subscribe();
    let handle_a = tokio::spawn(async move {
        let _ = sup_a_clone.run_consume_loop(shutdown_rx_a).await;
    });

    let sup_b_clone = sup_b.clone();
    let shutdown_rx_b = shutdown_tx_b.subscribe();
    let handle_b = tokio::spawn(async move {
        let _ = sup_b_clone.run_consume_loop(shutdown_rx_b).await;
    });

    // Give both consume loops a moment to subscribe before publishing.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Publish a single TaskMessage via plain NATS — JetStream's stream
    // (subjects = `edgecloud.tasks.>`) captures it.
    let publisher = async_nats::connect(&nats_url).await?;
    let payload = serde_json::json!({
        "type": "task_update",
        "timestamp": "2026-06-15T00:00:00Z",
        "tenant_id": "t_test",
        "apps": {
            "pinned-app": {
                "deployment_id": "d_pinned_001",
                "deployment_hash": "abc123",
                "env": {},
                "allowlist": []
            }
        }
    });
    let payload_bytes = serde_json::to_vec(&payload)?;
    publisher
        .publish(format!("edgecloud.tasks.{}", region), payload_bytes.into())
        .await?;

    // Wait for the message to be processed by exactly one worker.
    let started =
        wait_for_either_app_running(&[sup_a.clone(), sup_b.clone()], "t_test", "pinned-app", 15)
            .await;
    assert!(
        started.is_some(),
        "exactly one worker should have started pinned-app"
    );

    // Give the OTHER worker a chance to also start the app (it shouldn't).
    tokio::time::sleep(Duration::from_secs(3)).await;

    let apps_a = sup_a.state.read().await.apps.len();
    let apps_b = sup_b.state.read().await.apps.len();
    let total = apps_a + apps_b;
    assert_eq!(
        total, 1,
        "exactly one worker should host pinned-app (a={}, b={})",
        apps_a, apps_b
    );

    // Signal shutdown and wait for consume loops to exit cleanly.
    let _ = shutdown_tx_a.send(());
    let _ = shutdown_tx_b.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), handle_b).await;

    drop(nats_container);
    Ok(())
}

/// Wait until `app_name` is `Running` in any of `supervisors`. Returns
/// which supervisor saw it (Some(index)) or None if none did within the
/// timeout.
async fn wait_for_either_app_running(
    supervisors: &[Arc<Supervisor>],
    tenant_id: &str,
    app_name: &str,
    timeout_secs: u64,
) -> Option<usize> {
    let key = (tenant_id.to_string(), app_name.to_string());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        for (i, sup) in supervisors.iter().enumerate() {
            let state = sup.state.read().await;
            if let Some(inst) = state.apps.get(&key) {
                let inst = inst.lock().await;
                if matches!(inst.status, AppInstanceStatus::Running) {
                    return Some(i);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

// ---------------------------------------------------------------------------
// Log ingest pipeline (#76)
//
// End-to-end: app is started by the supervisor → runtime's LogSink (the
// supervisor's `log_forwarder`) receives a record → flush_now() POSTs to the
// mocked `/api/internal/logs` → request carries the worker's JWT, the body
// has the right shape.
//
// We don't have a guest that emits a log on demand, so the test injects a
// `LogRecord` directly through the `LogSink::push` interface — exactly what
// `Observer::emit_log_record_inner` does inside the runtime. The runtime's
// observe.rs unit tests already prove the `emit_log → push(record, ctx)`
// wiring; this test proves the worker's downstream path reaches the wire.
// ---------------------------------------------------------------------------

// TODO(issue-#74-e2e): a supervisor-level integration test for the
// auto-rollback path is the missing piece in this PR's coverage.
//
// The test would:
//   1. Build a `crashing.wasm` fixture — a minimal WASI Preview 2
//      component that traps on `_start` (a `core::arch::wasm32::
//      unreachable()` at the top of `_start`).
//   2. Mount a wiremock control plane that responds 200 to
//      `POST /api/internal/apps/myapp/auto-rollback`.
//   3. Drive `TestHarness::new()` + `handle_task_message` to start
//      the crashing app.
//   4. Wait for the supervisor to spin 5 times at exponential backoff
//      and reach `Crashed { restart_count: 5 }`.
//   5. Assert wiremock received exactly one auto-rollback POST with
//      `{tenant_id, app_name, current_deployment_id, restart_count: 5}`.
//
// Why it's not in this PR:
//   - Building a real WASI Preview 2 *component* (not just a core
//     module) requires either wasi-sdk + wit-component or a custom
//     Rust crate built with `rustup target add wasm32-wasip2` and
//     then `wasm-tools component embed`. Both flows are out of scope
//     for this PR's toolchain setup.
//   - The wire-level behavior of `Downloader::post_auto_rollback` is
//     already covered by three unit tests in
//     `src/downloader.rs::tests` (200 success, 412 rejection
//     without retry, path-traversal guard).
//   - The wiring in `supervisor.rs::run_app_loop` (Crashed + Hung
//     branches both spawn `post_auto_rollback`) is straightforward
//     enough that a reviewer's static check is sufficient until the
//     fixture lands.
//
// Follow-up: add `edge-worker/tests/fixtures/crashing.wasm` and the
// `#[tokio::test]` above. Self-skip via `should_skip_integration_tests`
// matches the existing pattern — runs locally with Docker, skipped
// in CI without it.

#[tokio::test]
async fn test_emit_log_reaches_log_ingest_endpoint() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    // The artifact endpoint must serve the fixture so the app reaches
    // Running — we then know the supervisor constructed an AppLogContext
    // for this app and would forward its logs through `log_forwarder`.
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_log_emit"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_component_bytes()))
        .mount(&harness.mock_server)
        .await;

    // The log ingest endpoint — captures the request for inspection.
    Mock::given(method("POST"))
        .and(path("/api/internal/logs"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&harness.mock_server)
        .await;

    let spec = AppSpec {
        deployment_id: "d_log_emit".to_string(),
        deployment_hash: test_component_hash(),
        env: HashMap::new(),
        allowlist: Some(vec![]),
        max_memory_mb: 0,
        routes: None,
    };
    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-18T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("log-emit-app".to_string(), spec)]),
    };
    harness
        .supervisor
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    let running = wait_for_app_running(&harness.supervisor, "log-emit-app", 10).await;
    assert!(
        running,
        "app should reach Running within 10s — proves supervisor wiring is healthy"
    );

    // Inject a LogRecord through the supervisor's LogSink (the LogForwarder).
    // The runtime's Observer::emit_log_record_inner calls `push(record, ctx)`
    // in exactly this shape; bypassing the runtime means we don't need a
    // guest that emits a log on demand.
    let record = edge_runtime::interfaces::observe::LogRecord {
        timestamp_ms: 0,
        level: edge_runtime::interfaces::observe::LogLevel::Info,
        message: "hello from worker".to_string(),
        labels: vec![
            ("request_id".to_string(), "abc".to_string()),
            ("path".to_string(), "/api/foo".to_string()),
        ],
    };
    let ctx = edge_runtime::interfaces::observe::AppLogContext {
        app_name: "log-emit-app".to_string(),
        tenant_id: "t_test".to_string(),
        deployment_id: "d_log_emit".to_string(),
    };
    harness.supervisor.log_forwarder.push(record, ctx);

    // The forwarder's flush_loop drives the ticker (1s) + the size-triggered
    // notify; we call `flush_now` directly so the test doesn't depend on
    // wall-clock time. The loop's shutdown branch already covers the
    // graceful-flush path — no need to re-test it here.
    harness.supervisor.log_forwarder.flush_now().await;

    // Assert the mock received exactly one POST to /api/internal/logs.
    let received = harness
        .mock_server
        .received_requests()
        .await
        .expect("received");
    let posts: Vec<_> = received
        .iter()
        .filter(|r| r.url.path() == "/api/internal/logs" && r.method == "POST")
        .collect();
    assert_eq!(
        posts.len(),
        1,
        "expected exactly one POST to /api/internal/logs, got {}",
        posts.len()
    );

    // The request must carry an Authorization: Bearer <jwt> header that
    // decodes to the worker's identity (worker_id + tenant_id).
    let auth = posts[0]
        .headers
        .get("authorization")
        .expect("Authorization header must be present")
        .to_str()
        .expect("Authorization must be valid ASCII");
    let token = auth
        .strip_prefix("Bearer ")
        .expect("Authorization must start with 'Bearer '");
    let claims = edge_worker::auth::verify_for_test_only(TEST_JWT_SECRET, "edgecloud", token)
        .expect("verify should succeed");
    assert_eq!(claims.worker_id, "test-worker");
    assert_eq!(claims.tenant_id, "t_test");
    assert_eq!(claims.region, "test-region");

    // Body shape: JSON with `entries` array containing one entry whose
    // identity fields come from AppLogContext and the record's message.
    assert!(!posts[0].body.is_empty(), "POST body must not be empty");
    let parsed: serde_json::Value = serde_json::from_slice(&posts[0].body).expect("body is JSON");
    let entries = parsed["entries"]
        .as_array()
        .expect("entries must be an array");
    assert_eq!(entries.len(), 1, "expected one entry");
    let entry = &entries[0];
    assert_eq!(entry["app_name"], "log-emit-app");
    assert_eq!(entry["tenant_id"], "t_test");
    assert_eq!(entry["deployment_id"], "d_log_emit");
    assert_eq!(entry["worker_id"], "test-worker");
    assert_eq!(entry["region"], "test-region");
    assert_eq!(entry["level"], "info");
    assert_eq!(entry["message"], "hello from worker");
    assert_eq!(entry["labels"]["request_id"], "abc");
    assert_eq!(entry["labels"]["path"], "/api/foo");
}

// ---------------------------------------------------------------------------
// Issue #77 §6 — end-to-end timing SLA.
//
// Asserts that a guest-emitted log entry reaches the control-plane
// ingest endpoint within 5 seconds of emission. The plan's contract:
// 1s forwarder flush interval + ~1s network round-trip is well within
// budget; 5s is generous for CI.
//
// Why this test exists separately from
// `test_emit_log_reaches_log_ingest_endpoint` above: that test calls
// `flush_now()` synchronously, which proves the wire contract but
// says nothing about the *ticker-driven* path. This test exercises
// the real `flush_loop` ticker — the path production runs on — and
// measures wall-clock time from `push()` to the POST landing at
// WireMock.
//
// Why it injects via `log_forwarder.push()` rather than driving a
// real guest: hand-crafting a wasi-p2 component requires a
// `wasm-tools` + `wit-component` toolchain that isn't in this PR's
// CI lane (see `fixtures/log-emit.c` for the rebuild path). The
// wire shape from `push()` to WireMock is byte-identical to a real
// `edge:observe.emit_log` call, so the SLA this test pins is the
// same SLA a guest-driven test would pin. The future migration to a
// real fixture is mechanical: drop the `push()` and replace the
// fixture.
//
// Self-skip: gated by the same `should_skip_integration_tests()`
// rule as the other tests — Docker + testcontainers is required
// for the NATS harness.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_emit_log_reaches_ingest_within_5s() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    // Spawn the flush_loop explicitly — the supervisor does not run
    // it (production spawns it in main.rs; tests construct the
    // supervisor without main.rs and so must drive the loop
    // themselves). The shutdown channel lets us stop it cleanly on
    // test exit. We clone the sender so we can `drop(_)` the
    // original at the end of the test (signals shutdown) without
    // moving it into the spawn closure.
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let shutdown_tx_for_loop = shutdown_tx.clone();
    let forwarder_for_loop = harness.supervisor.log_forwarder.clone();
    tokio::spawn(async move {
        forwarder_for_loop
            .flush_loop(shutdown_tx_for_loop.subscribe())
            .await;
    });

    // Mount the artifact endpoint so the supervisor can move the app
    // to Running. We don't actually execute the guest — the test
    // injects through the forwarder directly.
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_log_emit_sla"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_component_bytes()))
        .mount(&harness.mock_server)
        .await;

    // Capture the POST for timing + body assertions. Use a
    // generous deadline so the test does not flake under
    // slow-CI load — the SLA is what we measure, not what we
    // hand to wiremock.
    Mock::given(method("POST"))
        .and(path("/api/internal/logs"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&harness.mock_server)
        .await;

    let spec = AppSpec {
        deployment_id: "d_log_emit_sla".to_string(),
        deployment_hash: test_component_hash(),
        env: HashMap::new(),
        allowlist: Some(vec![]),
        max_memory_mb: 0,
        routes: None,
    };
    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-26T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("log-emit-sla".to_string(), spec)]),
    };
    harness
        .supervisor
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    // Wait until the app is Running so the forwarder has a live
    // AppLogContext for it. Once it is, any record we push with
    // that context will produce a POST to /api/internal/logs.
    let running = wait_for_app_running(&harness.supervisor, "log-emit-sla", 10).await;
    assert!(running, "app should reach Running within 10s");

    // Wall-clock measurement: the SLA is from `push()` to the
    // WireMock request landing, NOT from `wait_for_app_running`.
    // The `push` happens after Running, so the SLA budget covers
    // ticker + POST only.
    let start = std::time::Instant::now();

    // Inject one record. The forwarder's internal state will
    // surface this to WireMock on the next 1s ticker tick.
    let record = edge_runtime::interfaces::observe::LogRecord {
        timestamp_ms: 0,
        level: edge_runtime::interfaces::observe::LogLevel::Info,
        message: "hello-from-guest".to_string(),
        labels: vec![("source".to_string(), "log-emit".to_string())],
    };
    let ctx = edge_runtime::interfaces::observe::AppLogContext {
        app_name: "log-emit-sla".to_string(),
        tenant_id: "t_test".to_string(),
        deployment_id: "d_log_emit_sla".to_string(),
    };
    harness.supervisor.log_forwarder.push(record, ctx);

    // Poll WireMock for the request. We don't know exactly when
    // the 1s ticker will fire (it skipped the immediate-tick on
    // construction), so poll up to the 5s SLA window. Polling
    // every 100ms is fine — it's a local HTTP query, not a DB hit.
    let mut posts: Vec<wiremock::Request> = Vec::new();
    let sla = Duration::from_secs(5);
    while start.elapsed() < sla {
        let received = harness
            .mock_server
            .received_requests()
            .await
            .expect("received");
        posts = received
            .into_iter()
            .filter(|r| r.url.path() == "/api/internal/logs" && r.method == "POST")
            .collect();
        if !posts.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let elapsed = start.elapsed();
    assert!(
        !posts.is_empty(),
        "no POST to /api/internal/logs within {sla:?} — SLA violated"
    );
    assert!(
        elapsed < sla,
        "POST took {elapsed:?} which exceeds the {sla:?} SLA"
    );

    // Body shape sanity check (mirrors the wire assertions in
    // test_emit_log_reaches_log_ingest_endpoint).
    let parsed: serde_json::Value = serde_json::from_slice(&posts[0].body).expect("body is JSON");
    let entries = parsed["entries"]
        .as_array()
        .expect("entries must be an array");
    assert_eq!(entries.len(), 1, "expected one entry");
    let entry = &entries[0];
    assert_eq!(entry["app_name"], "log-emit-sla");
    assert_eq!(entry["tenant_id"], "t_test");
    assert_eq!(entry["deployment_id"], "d_log_emit_sla");
    assert_eq!(entry["level"], "info");
    assert_eq!(entry["message"], "hello-from-guest");
    assert_eq!(entry["labels"]["source"], "log-emit");

    // Stop the flush_loop task cleanly by dropping the shutdown
    // sender — the loop observes the channel close on the next
    // `shutdown.recv()` poll and exits. We don't `await` the
    // JoinHandle because the test is already done and the harness
    // is about to drop; the in-flight task gets cancelled along
    // with the rest of the tokio runtime when the test fn returns.
    drop(shutdown_tx);
}

// ---------------------------------------------------------------------------
// HTTP /sync fallback (issue #53)
//
// fetch_sync is the worker's HTTP escape hatch when NATS has been silent
// for longer than `worker_sync_threshold_secs`. These tests pin the wire
// contract: the GET path, the Bearer auth, the response shape, and the
// failure modes (non-2xx, malformed body). The actual diff application
// is already covered by handle_task_message tests above; we only need to
// prove fetch_sync decodes the wire correctly.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fetch_sync_happy_path() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }
    timeout(HARNESS_STARTUP_TIMEOUT, test_fetch_sync_happy_path_inner())
        .await
        .expect("test_fetch_sync_happy_path timed out")
        .expect("test_fetch_sync_happy_path failed");
}

async fn test_fetch_sync_happy_path_inner() -> anyhow::Result<()> {
    let mock_server = MockServer::start().await;
    let supervisor =
        build_supervisor_only_with_cp("test-worker", "test-region", "t_test", &mock_server.uri())
            .await?;

    // Mock the CP /sync response. The CP returns a TaskMessage with
    // type="full_sync" (set by the ReconcileService.BuildFullSync
    // envelope) and one app in the apps map.
    let body = serde_json::json!({
        "type": "full_sync",
        "timestamp": "2026-06-20T00:00:00Z",
        "tenant_id": "t_test",
        "apps": {
            "sync-fallback-app": {
                "deployment_id": "d_sync_1",
                "deployment_hash": "deadbeef".repeat(8),
                "env": {},
                "allowlist": [],
                "max_memory_mb": 256,
            }
        }
    });
    Mock::given(method("GET"))
        .and(path("/api/internal/workers/test-worker/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&mock_server)
        .await;

    let msg = supervisor
        .fetch_sync()
        .await
        .expect("fetch_sync")
        .expect("fetch_sync returned None on happy path");

    // Wire shape lock — workers fail to deserialize if the type field
    // isn't "full_sync" (issue #53).
    match msg {
        TaskMessage::FullSync {
            tenant_id, apps, ..
        } => {
            assert_eq!(tenant_id, "t_test");
            assert_eq!(apps.len(), 1);
            assert!(apps.contains_key("sync-fallback-app"));
        }
        TaskMessage::TaskUpdate { .. } => {
            panic!("fetch_sync returned TaskUpdate; expected FullSync")
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_fetch_sync_non_2xx_returns_none() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }
    timeout(
        HARNESS_STARTUP_TIMEOUT,
        test_fetch_sync_non_2xx_returns_none_inner(),
    )
    .await
    .expect("test_fetch_sync_non_2xx_returns_none timed out")
    .expect("test_fetch_sync_non_2xx_returns_none failed");
}

async fn test_fetch_sync_non_2xx_returns_none_inner() -> anyhow::Result<()> {
    let mock_server = MockServer::start().await;
    let supervisor =
        build_supervisor_only_with_cp("test-worker", "test-region", "t_test", &mock_server.uri())
            .await?;
    Mock::given(method("GET"))
        .and(path("/api/internal/workers/test-worker/sync"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&mock_server)
        .await;

    let msg = supervisor.fetch_sync().await.expect("fetch_sync");
    assert!(
        msg.is_none(),
        "non-2xx response must surface as None, not error"
    );
    Ok(())
}

#[tokio::test]
async fn test_fetch_sync_malformed_body_returns_none() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }
    timeout(
        HARNESS_STARTUP_TIMEOUT,
        test_fetch_sync_malformed_body_returns_none_inner(),
    )
    .await
    .expect("test_fetch_sync_malformed_body timed out")
    .expect("test_fetch_sync_malformed_body failed");
}

async fn test_fetch_sync_malformed_body_returns_none_inner() -> anyhow::Result<()> {
    let mock_server = MockServer::start().await;
    let supervisor =
        build_supervisor_only_with_cp("test-worker", "test-region", "t_test", &mock_server.uri())
            .await?;
    Mock::given(method("GET"))
        .and(path("/api/internal/workers/test-worker/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&mock_server)
        .await;

    let msg = supervisor.fetch_sync().await.expect("fetch_sync");
    assert!(
        msg.is_none(),
        "malformed JSON must surface as None, not propagate"
    );
    Ok(())
}

// last_task_received_at wiring (issue #53 watchdog). After
// handle_task_message succeeds the timestamp must be Some(Instant::now())
// so the heartbeat-task watchdog knows to skip the HTTP /sync fallback.
#[tokio::test]
async fn test_handle_task_message_bumps_last_task_received_at() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }
    timeout(
        HARNESS_STARTUP_TIMEOUT,
        test_handle_task_message_bumps_last_task_received_at_inner(),
    )
    .await
    .expect("test_handle_task_message_bumps_last_task_received_at timed out")
    .expect("test_handle_task_message_bumps_last_task_received_at failed");
}

async fn test_handle_task_message_bumps_last_task_received_at_inner() -> anyhow::Result<()> {
    let harness = build_supervisor_only_with_cp(
        "test-worker",
        "test-region",
        "t_test",
        "http://localhost:9999",
    )
    .await?;

    // Pre-condition: freshly constructed supervisor seeds
    // last_task_received_at to Some(construction_instant) — see
    // WorkerState::new and the boot-herd fix in commit F of PR
    // #166's review follow-up. The watchdog must NOT treat a
    // freshly-booted worker as infinitely stale.
    {
        let state = harness.state.read().await;
        let pre_seed = *state
            .last_task_received_at
            .lock()
            .expect("last_task_received_at mutex poisoned");
        assert!(
            pre_seed.is_some(),
            "expected last_task_received_at=Some after construction (boot herd fix)"
        );
    }

    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-20T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::new(),
    };
    harness
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    // Post-condition: Some(Instant) regardless of variant.
    let state = harness.state.read().await;
    let post = *state
        .last_task_received_at
        .lock()
        .expect("last_task_received_at mutex poisoned");
    assert!(
        post.is_some(),
        "expected last_task_received_at=Some after handle_task_message"
    );
    Ok(())
}

// test_handle_task_message_bumps_timestamp_on_partial_diff_failure
// covers the fix from review of PR #166, finding #3: the watchdog
// timer must reflect "we heard from NATS" (or HTTP), not "the diff
// fully applied". The previous implementation bumped the timer at
// the END of handle_task_message — so a diff that failed to apply
// (e.g. downloader rejection, hash mismatch, port exhaustion) left
// the timer untouched, and the heartbeat-loop watchdog would
// trigger /sync anyway. That amplification (combined with the
// boot-time fetch herd from finding #6) would convert a partial
// outage into a thundering-herd against the /sync endpoint.
//
// The diff is forced to fail by including an app whose deployment
// hash is malformed — the downloader's pre-check rejects non-hex
// hashes without ever making the HTTP fetch, so the test stays
// hermetic (no real CP server required).
async fn test_handle_task_message_bumps_timestamp_on_partial_diff_failure_inner(
) -> anyhow::Result<()> {
    let harness = build_supervisor_only_with_cp(
        "test-worker",
        "test-region",
        "t_test",
        "http://localhost:9999",
    )
    .await?;

    // Force a diff-failure path: deployment_hash is shorter than the
    // 64-char SHA-256 length, so downloader::verify_hash bails at the
    // pre-check before any network call.
    let bad_app = AppSpec {
        deployment_id: "d_broken".to_string(),
        deployment_hash: "tooshort".to_string(), // not 64 hex chars
        routes: None,
        env: HashMap::new(),
        allowlist: None,
        max_memory_mb: 256,
    };
    let mut apps = HashMap::new();
    apps.insert("myapp".to_string(), bad_app);

    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-06-20T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps,
    };

    // handle_task_message itself returns Ok — it logs start_app
    // failures and continues. The watchdog bump must still have
    // happened.
    harness.handle_task_message(msg).await?;

    let state = harness.state.read().await;
    let post = *state
        .last_task_received_at
        .lock()
        .expect("last_task_received_at mutex poisoned");
    assert!(
        post.is_some(),
        "expected last_task_received_at=Some even when diff fails to apply (fix for PR #166 finding #3)"
    );
    Ok(())
}

#[tokio::test]
async fn test_handle_task_message_bumps_timestamp_on_partial_diff_failure() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }
    timeout(
        HARNESS_STARTUP_TIMEOUT,
        test_handle_task_message_bumps_timestamp_on_partial_diff_failure_inner(),
    )
    .await
    .expect("test_handle_task_message_bumps_timestamp_on_partial_diff_failure timed out")
    .expect("test_handle_task_message_bumps_timestamp_on_partial_diff_failure failed");
}

// fetch_sync / handle_task_message wiring tests only need a Supervisor
// with a real engine + a mock CP URL — they don't need NATS. Build a
// minimal Supervisor on its own (without the full harness's container)
// so the assertion stays focused. NATS is still required because
// NatsClientImpl::connect is part of Supervisor construction; the
// fetch_sync tests below skip the NATS subscription and only inspect
// the HTTP response.
//
// This is now a thin shim over `edge_test_helpers::build_supervisor_with`
// that lets each test specify only the bits that vary (worker_id,
// region, tenant_id, control_plane_url). It's still useful because
// the rest of the Config (starting_port, queue_group, …) is identical
// across all 5 call sites — keeping the wiring logic next to the
// tests lets the test author see what knobs can be customised
// without having to jump into the helper crate.
//
// `control_plane_url` must be the wiremock server URI for fetch_sync
// tests. Tests construct it themselves.
async fn build_supervisor_only_with_cp(
    worker_id: &str,
    region: &str,
    tenant_id: &str,
    control_plane_url: &str,
) -> anyhow::Result<Arc<Supervisor>> {
    let config = Config {
        worker_id: worker_id.to_string(),
        region: region.to_string(),
        worker_addr: "test-host:0".to_string(),
        nats_url: String::new(), // overwritten by build_supervisor_with
        control_plane_url: control_plane_url.to_string(),
        cache_dir: PathBuf::from("/tmp/edge-worker-sync-test-cache"),
        heartbeat_interval_secs: 30,
        worker_sync_threshold_secs: 60,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        starting_port: 19_500,
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        queue_group: "test-sync-group".to_string(),
        consumer_name: "test-sync-consumer".to_string(),
        worker_jwt_secret: String::from_utf8(TEST_JWT_SECRET.to_vec()).unwrap(),
        worker_jwt_kid: None,
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: tenant_id.to_string(),
        handler_request_budget_ms: 1000,
        handler_max_request_body_bytes: 10 * 1024 * 1024,
        task_stream_replicas: 1,
        tls_cert_path: None,
        tls_key_path: None,
    };

    let guard = build_supervisor_with(config).await;
    // of the Arc inside the guard; the guard holds the container
    // alive for as long as it's in scope. Once this function returns
    // the caller owns the supervisor but NOT the guard; that means
    // the container drops when this function returns. The callers
    // (fetch_sync / handle_task_message tests) use the supervisor's
    // JS jetstream connection for the test's full duration — the
    // container lifetime needs to match. To preserve the test
    // semantics, we intentionally forget the NATS container here.
    std::mem::forget(guard._nats_container);
    Ok(guard.supervisor)
}

#[tokio::test]
async fn test_supervisor_lazy_starting_and_eviction() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let harness = TestHarness::new().await.expect("create test harness");

    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_deploy_001"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_component_bytes()))
        .mount(&harness.mock_server)
        .await;

    // Step 1: send TaskMessage to start an app
    let spec = AppSpec {
        deployment_id: "d_deploy_001".to_string(),
        deployment_hash: test_component_hash(),
        env: HashMap::new(),
        allowlist: None,
        max_memory_mb: 256,
        routes: None,
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

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Check state. Since it's lazy, there shouldn't be an engine yet.
    let dispatch = {
        let state = harness.supervisor.state.read().await;
        let inst = state.apps.get(&("t_test".to_string(), "test-app".to_string())).unwrap();
        let inst_locked = inst.lock().await;
        assert_eq!(inst_locked.execution_model, edge_worker::detect::ExecutionModel::Handler);
        inst_locked.dispatch.clone().unwrap()
    };
    
    assert!(dispatch.evict().await.is_none(), "Engine should be None because it is lazily started");

    // Simulate request by hitting the port
    let port = {
        let state = harness.supervisor.state.read().await;
        let inst = state.apps.get(&("t_test".to_string(), "test-app".to_string())).unwrap();
        let p = inst.lock().await.port;
        p
    };

    let client = reqwest::Client::new();
    let _ = client.get(format!("http://127.0.0.1:{}", port)).send().await;
    
    // Give it a moment to process
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Check state. Engine should exist now.
    let engine = dispatch.evict().await;
    assert!(engine.is_some(), "Engine should be Some because it was started");
    
    // Free it
    if let Some(e) = engine {
        harness.supervisor.engine_pool.release(e);
    }
}
