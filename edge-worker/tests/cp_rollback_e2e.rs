//! Cross-language rollback end-to-end test for issue #613.
//!
//! This is the Rust half of a two-process e2e proving that a
//! `DeploymentService.RollbackDeployment` call on the CP reaches a
//! real worker over real NATS and swaps the running app. The Go half
//! lives at `edge-control-plane/internal/integration/rollback_e2e_test.go`,
//! and `scripts/rollback-e2e.sh` orchestrates both halves via sentinel
//! files in `/tmp/edge-e2e/`.
//!
//! Mirrors the cross-language precedent set by PR #652 (issue #611
//! wire-contract test). The CP-side test publishes TaskMessages over a
//! real JetStream publisher; this test consumes them via the worker's
//! real `run_consume_loop` and asserts the heartbeat `deployment_id`
//! flips A → B → A.
//!
//! Flow (CP-side perspective; see its test file for details):
//!
//!  1. CP boots Postgres + NATS, runs migrations, signs two
//!     deployment rows over the same `handler.wasm` artifact.
//!  2. CP writes `NATS_URL` to `/tmp/edge-e2e/nats-url`.
//!  3. This test reads `EDGE_TEST_NATS_URL`, builds a Supervisor
//!     via `edge_test_helpers::build_supervisor_from_url`, wires
//!     wiremock to serve the same `handler.wasm` bytes for both
//!     deployment_ids, subscribes to heartbeats (with `.flush()`),
//!     spawns `run_consume_loop`.
//!  4. This test writes `/tmp/edge-e2e/rust-ready`.
//!  5. CP activates A → B → rollback to A; each phase publishes a
//!     `TaskMessage::TaskUpdate` with the matching `deployment_id`.
//!  6. This test observes heartbeats and asserts the deployment_id
//!     transitions match the wire.
//!  7. This test writes `/tmp/edge-e2e/rust-done` and exits.
//!
//! Why `port_cooldown_secs = 0`: with the default 60s cooldown, the
//! rolled-back-to deployment's port would still be in `cooling_down`
//! when the worker tries to acquire a new one, and the test would
//! conflate "port released" with "port reused". Setting cooldown=0
//! makes release/reacquire observable within the test's time budget.
//!
//! Why `run_consume_loop` (not `handle_task_message`): the e2e
//! purpose is to prove the wire contract — JetStream subscribe +
//! serde round-trip — end-to-end. `handle_task_message` bypasses NATS
//! entirely and is exercised by the unit-level `compute_app_diff`
//! tests in `supervisor.rs`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context};
use async_nats::Subscriber;
use edge_test_helpers::build_supervisor_from_url;
use edge_worker::config::Config;
use edge_worker::messages::HeartbeatMessage;
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::time::timeout;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---- constants shared with the Go half (must match) ----
const SENTINEL_DIR: &str = "/tmp/edge-e2e";
const TEST_REGION: &str = "test-region";
const TEST_TENANT_ID: &str = "t_rollback_e2e";
const TEST_APP_NAME: &str = "myapp";
const DEPLOYMENT_ID_A: &str = "d_e2e_a";
const DEPLOYMENT_ID_B: &str = "d_e2e_b";

/// Hard upper bound for the whole e2e. Generous — the actual runtime
/// is ~15s once NATS + containers are up.
const OVERALL_BUDGET: Duration = Duration::from_secs(90);

#[tokio::test]
async fn cp_rollback_e2e() {
    if edge_test_helpers::should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    // `run_e2e` returns `Err` on assertion failure (which `expect`s
    // up into a panic) and `Ok(())` on success. The `timeout` arm
    // turns a stuck test into a clean panic with the budget, instead
    // of letting `cargo test` hang indefinitely.
    timeout(OVERALL_BUDGET, run_e2e())
        .await
        .context("overall e2e timed out")
        .expect("e2e timed out")
        .expect("e2e failed");
}

async fn run_e2e() -> anyhow::Result<()> {
    // 1. Read the NATS URL the Go half wrote.
    let nats_url = std::fs::read_to_string(format!("{}/nats-url", SENTINEL_DIR)).context(
        "read /tmp/edge-e2e/nats-url — did scripts/rollback-e2e.sh start the Go half first?",
    )?;
    let nats_url = nats_url.trim().to_string();
    eprintln!("cp_rollback_e2e: connecting to NATS at {}", nats_url);

    // 2. Mount a wiremock that returns the real handler.wasm bytes
    //    for both deployment IDs. The worker downloads via the
    //    standard /api/internal/download/{id} path.
    let mock_server = MockServer::start().await;
    let wasm_bytes =
        std::fs::read(fixture_path("handler.wasm")).context("read tests/fixtures/handler.wasm")?;
    for id in [DEPLOYMENT_ID_A, DEPLOYMENT_ID_B] {
        Mock::given(method("GET"))
            .and(path(format!("/api/internal/download/{}", id)))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(wasm_bytes.clone()))
            .mount(&mock_server)
            .await;
    }

    // 3. Build a Supervisor pointed at the SHARED NATS container.
    //    port_cooldown_secs=0 so port release/reacquire is observable
    //    within the test (see module doc).
    let cache_dir = tempfile::TempDir::new().context("create cache tempdir")?;
    let config = Config {
        worker_id: "test-worker".to_string(),
        region: TEST_REGION.to_string(),
        worker_addr: "test-host:0".to_string(),
        nats_url: String::new(), // overwritten by build_supervisor_from_url
        control_plane_url: mock_server.uri(),
        cache_dir: cache_dir.path().to_path_buf(),
        heartbeat_interval_secs: 2,
        worker_sync_threshold_secs: 60,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 0, // <— the load-bearing knob for this test
        starting_port: 19_000,
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        consumer_name: "test-consumer-rollback".to_string(),
        queue_group: String::new(),
        worker_jwt_secret: String::from_utf8(b"test-secret".to_vec()).unwrap(),
        worker_jwt_kid: Some("test-kid".to_string()),
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: TEST_TENANT_ID.to_string(),
        handler_request_budget_ms: 1000,
        handler_max_request_body_bytes: 10 * 1024 * 1024,
        task_stream_replicas: 1,
        tls_cert_path: None,
        tls_key_path: None,
        worker_bootstrap_secret: String::new(),
        socket_mode: edge_runtime::socket_egress::SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        standby_pool_size: 5,
        require_signature: false,
        signing_keyring: None,
        signing_keyring_path: None,
    };
    let supervisor = build_supervisor_from_url(&nats_url, config)
        .await
        .context("build_supervisor_from_url")?;

    // 4. Subscribe to heartbeats BEFORE the supervisor publishes any.
    //    The .flush() after subscribe is load-bearing — without it the
    //    SUB frame can race the publisher and messages are silently
    //    dropped (see heartbeat_subscriber in integration_tests.rs).
    let mut hb_sub = heartbeat_subscriber(&nats_url, TEST_REGION).await?;
    eprintln!("cp_rollback_e2e: subscribed to heartbeats; flushing");
    // flush is inside heartbeat_subscriber already

    // 5. Spawn the worker's JetStream consume loop. It will block on
    //    subscribe_tasks; the first message arrives once the CP half
    //    starts publishing (after rust-ready is written). We wire a
    //    shutdown receiver so the loop can be cleanly aborted at the
    //    end of the test without leaving the JetStream consumer
    //    subscribed to the shared NATS container.
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let shutdown_rx = shutdown_tx.subscribe();
    let supervisor_for_loop = supervisor.clone();
    let consume_handle = tokio::spawn(async move {
        let _ = supervisor_for_loop.run_consume_loop(shutdown_rx).await;
    });

    // 6. Tell the Go half we're live. After this, it will run
    //    activate(A) → activate(B) → rollback(A).
    std::fs::write(format!("{}/rust-ready", SENTINEL_DIR), "ok")
        .context("write rust-ready sentinel")?;
    eprintln!("cp_rollback_e2e: wrote rust-ready; awaiting 3 heartbeat transitions");

    // 7. Collect heartbeats until we observe three transitions for
    //    TEST_APP_NAME: deployment_id A → B → A. Each transition
    //    emits a heartbeat within ~2s of the wire TaskMessage.
    let observed = collect_transitions(&mut hb_sub).await?;
    let ports: Vec<u16> = observed.iter().map(|t| t.port).collect();
    let statuses: Vec<String> = observed.iter().map(|t| t.status.clone()).collect();
    let ids: Vec<String> = observed.iter().map(|t| t.deployment_id.clone()).collect();

    // 8. Assert the load-bearing shape.
    anyhow::ensure!(
        ids == vec![
            DEPLOYMENT_ID_A.to_string(),
            DEPLOYMENT_ID_B.to_string(),
            DEPLOYMENT_ID_A.to_string()
        ],
        "deployment_id transitions were {:?}, want [A, B, A]",
        ids
    );
    anyhow::ensure!(
        statuses.iter().all(|s| s == "running"),
        "final statuses were {:?}, want all 'running'",
        statuses
    );
    // The rolled-back-to deployment must have acquired a fresh port,
    // not reused the original (which is in cooling_down with
    // cooldown=0 — actually released, so a fresh acquire). The
    // transition between A and B must have changed the port too.
    anyhow::ensure!(
        ports[0] != ports[1] && ports[1] != ports[2] && ports[0] != ports[2],
        "expected three distinct ports across transitions, got {:?}",
        ports
    );

    eprintln!(
        "cp_rollback_e2e: PASS — deployment_id transitions {:?} across ports {:?}",
        ids, ports
    );

    // 9. Tell the Go half to exit so it can tear down NATS.
    std::fs::write(format!("{}/rust-done", SENTINEL_DIR), "ok")
        .context("write rust-done sentinel")?;

    // 10. Clean up the consume loop so it doesn't keep the test alive.
    consume_handle.abort();
    let _ = consume_handle.await;

    Ok(())
}

// ---- helpers ----

#[derive(Debug, Clone)]
struct Transition {
    deployment_id: String,
    port: u16,
    status: String,
}

/// Wait for three heartbeats carrying TEST_APP_NAME with distinct
/// deployment_ids. Returns the three transitions in observed order.
///
/// Heartbeat messages arrive at the configured tick (we set
/// heartbeat_interval_secs=2 in the test Config), and the supervisor
/// also emits an "immediate" heartbeat after every state change (see
/// `handle_task_message`'s `if has_changes` arm). So we expect
/// transitions to land within ~3-4s of each TaskMessage being
/// processed.
async fn collect_transitions(sub: &mut Subscriber) -> anyhow::Result<Vec<Transition>> {
    let mut transitions: Vec<Transition> = Vec::new();
    let mut last_id: Option<String> = None;

    let overall_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while transitions.len() < 3 {
        if tokio::time::Instant::now() > overall_deadline {
            return Err(anyhow!(
                "only observed {} transitions within budget: {:?}",
                transitions.len(),
                transitions
            ));
        }
        // Wait up to 10s for the next heartbeat. The supervisor
        // emits an immediate one after each TaskMessage, so 10s is
        // very generous.
        let msg = timeout(Duration::from_secs(10), sub.next())
            .await
            .map_err(|_| anyhow!("heartbeat timed out after 10s"))?
            .ok_or_else(|| anyhow!("heartbeat subscription closed"))?;
        let hb: HeartbeatMessage =
            serde_json::from_slice(&msg.payload).context("parse heartbeat")?;
        let Some(app) = hb.apps.get(TEST_APP_NAME) else {
            continue; // heartbeat before our app appeared; skip
        };
        let new_id = app.deployment_id.clone();
        // Only record a transition when deployment_id changes — this
        // collapses the multiple heartbeat emissions per swap into one
        // "the app moved" event.
        if last_id.as_deref() != Some(&new_id) {
            transitions.push(Transition {
                deployment_id: new_id.clone(),
                port: app.port,
                status: app.status.clone(),
            });
            last_id = Some(new_id);
        }
    }
    Ok(transitions)
}

/// Mirrors `heartbeat_subscriber` in `integration_tests.rs` — see that
/// doc comment for why `.flush()` is load-bearing.
async fn heartbeat_subscriber(nats_url: &str, region: &str) -> anyhow::Result<Subscriber> {
    let client = async_nats::connect(nats_url).await?;
    let subject = format!("edgecloud.heartbeats.{}", region);
    let sub = client.subscribe(subject).await?;
    client.flush().await?;
    Ok(sub)
}

/// Resolve a fixture path relative to this test file's directory.
/// Cargo runs `tests/*.rs` with cwd = the crate root, but
/// `CARGO_MANIFEST_DIR` is more reliable than CWD.
fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}
