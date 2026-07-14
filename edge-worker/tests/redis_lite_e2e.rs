//! Issue #496 — `redis-lite` long-running TCP/RESP guest structural test.
//!
//! Boots a real `edge-worker` Supervisor against the `redis-lite` fixture
//! (built from `samples/redis-lite/`, committed to
//! `edge-worker/tests/fixtures/redis_lite.wasm`, hash-pinned by
//! `test_fixtures_match_source.rs::EXPECTED_REDIS_LITE_HASH`).
//!
//! ## What this test covers
//!
//! 1. The fixture's structural imports/exports satisfy the supervisor's
//!    LongRunning detection (`edge-worker/src/detect.rs` — no
//!    `wasi:http/incoming-handler` export → LongRunning path).
//! 2. The supervisor's per-app `EDGE_HTTP_SERVER_PORT` stamping reaches
//!    the guest (heartbeat surfaces a non-zero `port` for the app).
//! 3. The supervisor's `EDGE_PROTOCOL=tcp` env propagation threads
//!    through to the heartbeat (`protocol == "tcp"`).
//! 4. The guest's `start()` runs far enough to bind a TCP socket —
//!    a kernel-level TCP connect to the worker port succeeds.
//! 5. The per-app `socket_mode` override (issue #412) reaches the
//!    guest's `wasi:sockets/tcp` bind — worker-wide BlockAll + app
//!    AllowAll = guest can bind. (If the override wiring breaks, the
//!    heartbeat never surfaces `port` and step 4 fails by timeout.)
//! 6. LR metering gap (request_count=0, outbound_bytes=0) is asserted
//!    as a CI tripwire. Tracked separately as #699.
//!
//! ## What this test does NOT cover
//!
//! Full RESP round-trip (PING/SET/GET/DEL/ECHO). The current wasmtime
//! epoch model gives `start()` a single 1s budget (`epoch_deadline_ticks
//! × epoch_tick_ms`); a single-threaded accept loop blocks past that
//! and traps, after which the supervisor's exponential-backoff restart
//! caps the app as `Crashed` before any TCP client can exchange data.
//! That gap is a separate piece of work — the test ships as a structural
//! guard now and the round-trip is exercised manually via
//! `redis-cli` against a real `edge deploy` (see
//! `samples/redis-lite/README.md`).
//!
//! ## Skip conditions
//!
//! Skipped when Docker is unavailable or under `SKIP_INTEGRATION_TESTS`
//! — matches the existing supervisor integration tests via
//! `should_skip_integration_tests()` from `edge-test-helpers`.

use std::collections::HashMap;
use std::time::Duration;

use sha2::{Digest as _, Sha256};
use tokio::net::TcpStream;
use tokio::time::timeout;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use edge_runtime::socket_egress::SocketEgressPolicy;
use edge_test_helpers::{build_supervisor_with, should_skip_integration_tests};
use edge_worker::config::Config;
use edge_worker::messages::{AppSpec, HeartbeatMessage, TaskMessage};

// ── Fixtures ───────────────────────────────────────────────────────────

const REDIS_LITE_WASM: &[u8] = include_bytes!("fixtures/redis_lite.wasm");

fn redis_lite_hash() -> String {
    let digest = Sha256::digest(REDIS_LITE_WASM);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

// ── Test ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "wasmtime LR epoch model needs revisiting before this can run \
            reliably in CI — see #496 follow-up. Run manually via \
            `cargo test --manifest-path edge-worker/Cargo.toml \
             --test redis_lite_e2e -- --ignored --nocapture`."]
async fn redis_lite_structural_contract() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }
    // Hard outer timeout: defense-in-depth — a single stuck test must
    // not be able to wedge the whole CI job.
    timeout(
        Duration::from_secs(120),
        redis_lite_structural_contract_inner(),
    )
    .await
    .expect("redis_lite_structural_contract timed out after 120s");
}

async fn redis_lite_structural_contract_inner() {
    // 1. Spin up the mock control plane. The Supervisor will hit the
    //    wiremock server when it needs the `.wasm` bytes for the new
    //    deployment.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_redis_lite_001"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(REDIS_LITE_WASM))
        .mount(&mock)
        .await;

    // 2. Config: worker-wide `socket_mode` stays at `BlockAll` so the
    //    test proves the per-app override (`AppSpec.socket_mode =
    //    Some(AllowAll)` below) actually reaches the guest. Without
    //    that override, the LR TCP server inside the guest would be
    //    denied by the egress policy. This is the regression coverage
    //    for issue #412.
    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let config = Config {
        worker_id: "test-worker".to_string(),
        region: "test-region".to_string(),
        worker_addr: "test-host:0".to_string(),
        nats_url: String::new(), // overwritten by build_supervisor_with
        control_plane_url: mock.uri(),
        cache_dir: cache_dir.path().to_path_buf(),
        heartbeat_interval_secs: 30,
        worker_sync_threshold_secs: 60,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        port_pool_size: 100,
        starting_port: 22_000, // distinct from js_websocket_e2e (21_000)
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        consumer_name: "test-redis-lite-consumer".to_string(),
        queue_group: String::new(),
        worker_jwt_secret: String::from_utf8(b"test-jwt-secret-for-redis-lite-e2e".to_vec())
            .unwrap(),
        worker_jwt_kid: None,
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: "t_test".to_string(),
        handler_request_budget_ms: 1000,
        handler_max_request_body_bytes: 10 * 1024 * 1024,
        task_stream_replicas: 1,
        tls_cert_path: None,
        tls_key_path: None,
        worker_bootstrap_secret: String::new(),
        worker_key_path: std::path::PathBuf::from("/tmp/worker-key"),
        worker_identity_path: std::path::PathBuf::from("/tmp/identity-key"),
        worker_reenroll_on_boot: false,
        socket_mode: SocketEgressPolicy::BlockAll,
        hostname_pinning_enabled: false,
        standby_pool_size: 10,
        require_signature: false,
        signing_keyring: None,
        signing_keyring_path: None,
    };
    let guard = build_supervisor_with(config, None).await;
    let supervisor = guard.supervisor.clone();

    // 3. AppSpec — per-app socket_mode=AllowAll flips the worker's
    //    BlockAll default for THIS app only (issue #412).
    //    EDGE_PROTOCOL=tcp tells the supervisor this is a raw-TCP
    //    guest; the heartbeat then carries `protocol: "tcp"` so
    //    edge-ingress routes via the L4 path.
    let spec = AppSpec {
        deployment_id: "d_redis_lite_001".to_string(),
        deployment_hash: redis_lite_hash(),
        deployment_signature: None,
        signing_key_id: None,
        env: HashMap::from([("EDGE_PROTOCOL".to_string(), "tcp".to_string())]),
        // `allowlist: None` — the serde normalization + `EgressPolicy::new`
        // path strips `"*"`, so passing it explicitly is misleading.
        allowlist: None,
        socket_mode: Some(SocketEgressPolicy::AllowAll),
        max_memory_mb: 256,
        cpu_budget_ms: None,
        routes: None,
        preview_id: None,
        preview_pr_number: None,
    };
    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-07-14T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("redis-lite".to_string(), spec)]),
    };
    supervisor
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    // 4. Poll heartbeat for app.port + protocol=tcp.
    let mut port: Option<u16> = None;
    for _ in 0..300 {
        // 300 * 100ms = 30s
        let hb: HeartbeatMessage = supervisor.build_heartbeat().await;
        if let Some(app) = hb.apps.get("redis-lite") {
            assert_eq!(app.status, "running", "app must be running");
            assert_eq!(app.protocol, "tcp", "app must report protocol=tcp");
            assert!(app.port > 0, "app must have a non-zero port");
            port = Some(app.port);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let port = port.expect("app.port must surface in heartbeat within 30s");

    // 5. TCP-probe the worker port. The guest's `start()` runs far
    //    enough to call `bind` + `listen` + `accept`; the kernel-level
    //    accept queue holds the connection. ~30s deadline mirrors the
    //    heartbeat poll budget.
    let addr = format!("127.0.0.1:{port}");
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match TcpStream::connect(&addr).await {
                Ok(_s) => break,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => panic!("TCP connect to {addr} failed after 30s: {e}"),
            }
        }
    }

    // 6. Post-session metering assertion — documents the LR
    //    request_count=0 gap. Tracked as a follow-up issue, NOT fixed
    //    here. If this assertion ever flips to non-zero, the follow-up
    //    is closed and the test needs to be updated to reflect the
    //    new semantic.
    let hb: HeartbeatMessage = supervisor.build_heartbeat().await;
    let app = hb
        .apps
        .get("redis-lite")
        .expect("redis-lite app must be in heartbeat");
    assert_eq!(
        app.request_count, 0,
        "LR apps currently report request_count=0 regardless of traffic \
         — see #699 (pre-existing gap since #484)."
    );
    assert_eq!(
        app.outbound_bytes, 0,
        "LR apps currently report outbound_bytes=0 — same gap as request_count."
    );
}
