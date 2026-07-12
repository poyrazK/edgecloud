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
//! real `subscribe_tasks` JetStream consumer (the same one
//! `run_consume_loop` uses) and asserts the heartbeat `deployment_id`
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
//!     deployment_ids, subscribes to heartbeats (with `.flush()`)
//!     and to the task stream.
//!  4. This test writes `/tmp/edge-e2e/rust-ready`.
//!  5. CP activates A → B → rollback to A; each phase publishes a
//!     `TaskMessage::TaskUpdate` with the matching `deployment_id`.
//!  6. This test consumes the wire, calls `handle_task_message` on
//!     each, and after each swap manually emits a heartbeat so the
//!     subscriber observes the deployment_id transition deterministically.
//!  7. This test writes `/tmp/edge-e2e/rust-done` and exits.
//!
//! Why `port_cooldown_secs = 0`: with the default 60s cooldown, the
//! rolled-back-to deployment's port would still be in `cooling_down`
//! when the worker tries to acquire a new one, and the test would
//! conflate "port released" with "port reused". Setting cooldown=0
//! makes release/reacquire observable within the test's time budget.
//!
//! Why manual consume (not `run_consume_loop`): `run_consume_loop` is
//! the production wrapper that pairs `subscribe_tasks` with
//! `process_task_message`. `process_task_message` only emits the
//! "immediate heartbeat on has_changes" path when the diff actually
//! fires; this test needs a heartbeat after EVERY wire message so the
//! subscriber sees the transition deterministically. We re-implement
//! the loop inline and call `build_heartbeat` + `publish_heartbeat`
//! after each swap — the wire round-trip is still real (we subscribe
//! via the same `subscribe_tasks` call), only the heartbeat emission
//! is more eager than production.
//!
//! Why `require_signature = false`: this test deliberately does NOT
//! exercise Ed25519 signature verification on the wire. The Go half
//! signs a SHA-256 hash the worker must agree with; both halves
//! compute that hash over the same `handler.wasm` fixture bytes
//! (the Go helper reads the fixture from disk; this Rust helper
//! returns the same bytes from wiremock). The worker still verifies
//! the SHA-256 (the contract under test here is "hash matches what
//! the deployment row says"), but the Ed25519 signature check is
//! relaxed via `require_signature = false` — the cross-language
//! wire-contract test from PR #652 (issue #611) owns the Ed25519
//! path end-to-end.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context};
use async_nats::Subscriber;
use edge_test_helpers::build_supervisor_from_url;
use edge_worker::config::Config;
use edge_worker::messages::{HeartbeatMessage, TaskMessage};
use edge_worker::nats::TaskMessageStream;
use edge_worker::supervisor::Supervisor;
use futures::StreamExt;
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
    // This test is the Rust half of a two-process cross-language e2e
    // orchestrated by `scripts/rollback-e2e.sh`. The orchestrator
    // publishes the shared NATS URL to `/tmp/edge-e2e/nats-url` before
    // launching us, then blocks on `/tmp/edge-e2e/rust-done`. If
    // `EDGE_TEST_NATS_URL` is unset we were launched by the bare
    // `rust-test-integration` job (no Go half, no orchestrator) — skip
    // rather than panic on the missing sentinel. The dedicated
    // `rollback-e2e` CI job sets `EDGE_TEST_NATS_URL` explicitly.
    if std::env::var("EDGE_TEST_NATS_URL").is_err() && std::env::var("SENTINEL_DIR").is_err() {
        eprintln!(
            "SKIPPED: cp_rollback_e2e requires scripts/rollback-e2e.sh \
             orchestrator (EDGE_TEST_NATS_URL or SENTINEL_DIR unset)"
        );
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
        // Per-worker identity (issue #430): paths are inside the
        // tempdir so the test never touches a real on-disk key. The
        // supervisor never reads these in this test (no bootstrap
        // enrollment happens — the test disables signature
        // verification).
        worker_key_path: cache_dir.path().join("identity.key"),
        worker_identity_path: cache_dir.path().join("identity.key"),
        worker_reenroll_on_boot: false,
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

    // 5. Subscribe to the JetStream task stream ourselves. We can't
    //    call `run_consume_loop` because that path owns the heartbeat
    //    ticker — we want manual control so we can `build_heartbeat`
    //    + `publish_heartbeat` between each TaskMessage. This is the
    //    same shape the production worker uses; `run_consume_loop`
    //    is just the production wrapper around `subscribe_tasks` +
    //    `process_task_message`.
    let mut task_stream = supervisor
        .nats
        .subscribe_tasks(TEST_REGION, "test-consumer-rollback")
        .await
        .context("subscribe_tasks")?;
    eprintln!("cp_rollback_e2e: subscribed to task stream");

    // 5b. JetStream warm-up: `consumer.messages()` returns a stream
    //     but the underlying push subscription only delivers after
    //     the server has registered the consumer's interest. Without
    //     this sleep, messages published immediately after
    //     rust-ready can land in the stream BEFORE the consumer's
    //     deliver-subject registration completes, and they're
    //     silently dropped (the consumer's last-delivered pointer
    //     advances past them). The fix mirrors
    //     integration_tests.rs:1093 — a 2s grace period before any
    //     publisher on the wire.
    tokio::time::sleep(Duration::from_secs(2)).await;
    eprintln!("cp_rollback_e2e: 2s JetStream warm-up complete");

    // 6. Tell the Go half we're live. After this, it will run
    //    activate(A) → activate(B) → rollback(A), each publishing
    //    a TaskMessage to the task stream we're subscribed to.
    std::fs::write(format!("{}/rust-ready", SENTINEL_DIR), "ok")
        .context("write rust-ready sentinel")?;
    eprintln!("cp_rollback_e2e: wrote rust-ready; awaiting 3 heartbeat transitions");

    // 7. Drive the consume loop manually: pull a TaskMessage,
    //    dispatch to `handle_task_message`, then build + publish a
    //    fresh heartbeat so the subscriber sees the new deployment_id.
    //    Three TaskMessages → three heartbeat transitions (A, B, A).
    let observed =
        drive_consume_and_collect_heartbeats(&supervisor, &mut task_stream, &mut hb_sub).await?;
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

    // 10. Drop the task-stream subscription so the JetStream consumer
    //     is reclaimed before the test exits.
    drop(task_stream);

    Ok(())
}

/// Pull three TaskMessages from the JetStream task stream, dispatch
/// each to `handle_task_message`, then build + publish a heartbeat so
/// the subscriber observes the deployment_id transition. Returns the
/// three observed transitions in order.
///
/// This is the cross-language wire test's heart: the TaskMessage
/// arrives via real JetStream (proving the CP→NATS→worker contract),
/// `handle_task_message` does the diff (proving the worker's stop/
/// start logic), and the manual `publish_heartbeat` after each
/// message guarantees the subscriber sees the transition regardless
/// of whether `has_changes` happened to fire inside `handle_task_message`.
async fn drive_consume_and_collect_heartbeats(
    supervisor: &Supervisor,
    task_stream: &mut TaskMessageStream,
    hb_sub: &mut Subscriber,
) -> anyhow::Result<Vec<Transition>> {
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

        // 1. Wait up to 15s for the next TaskMessage on the wire.
        //    The CP-side drainer ticks every 100ms (set in the Go
        //    half), so a message arrives within ~1s of the previous
        //    phase's commit; 15s is generous slack for the cold-cache
        //    cargo build to finish mid-test.
        let raw = timeout(Duration::from_secs(15), task_stream.next())
            .await
            .map_err(|_| anyhow!("task message timed out after 15s"))?
            .ok_or_else(|| anyhow!("task stream ended"))?;
        let payload = raw.payload.clone();
        let task_msg: TaskMessage =
            serde_json::from_slice(&payload).context("parse task message")?;

        // 2. Dispatch. Mirrors what `process_task_message` does inside
        //    `run_consume_loop`, but inline so we can publish a
        //    heartbeat *after* it finishes — `process_task_message`
        //    only publishes the immediate heartbeat when `has_changes`
        //    fires inside `handle_task_message`. We always want a
        //    heartbeat after a swap, so we publish unconditionally
        //    here.
        supervisor
            .handle_task_message(task_msg)
            .await
            .context("handle_task_message")?;

        // 3. Ack so JetStream doesn't redeliver on reconnect. ack
        //    failure is best-effort logged; the redelivery is harmless
        //    for this test (we'd just see the same swap twice).
        if let Err(e) = supervisor.nats.ack(&raw).await {
            eprintln!("cp_rollback_e2e: ack failed (non-fatal): {e}");
        }

        // 4. Build + publish a heartbeat. Mirrors the "immediate
        //    heartbeat on has_changes" arm inside `handle_task_message`
        //    (supervisor.rs:1706-1717) but is unconditional, so the
        //    subscriber sees the new deployment_id within ~1s rather
        //    than waiting up to heartbeat_interval_secs.
        let hb = supervisor.build_heartbeat().await;
        supervisor
            .nats
            .publish_heartbeat(&supervisor.config.region, &hb)
            .await
            .context("publish_heartbeat")?;

        // 5. Drain heartbeats on the subscription. The supervisor's
        //    immediate heartbeat (if any) plus our explicit publish
        //    will land within a few hundred ms. We read until the
        //    deployment_id matches one we haven't seen, then stop
        //    draining and wait for the NEXT TaskMessage.
        let transition_seen =
            collect_next_transition(hb_sub, &mut last_id, &mut transitions).await?;
        if !transition_seen {
            // No transition found in this drain — handle_task_message
            // didn't change our app. Continue to the next TaskMessage.
            eprintln!("cp_rollback_e2e: handle_task_message completed but no transition observed");
        }
    }

    Ok(transitions)
}

/// Read heartbeats until a new deployment_id for TEST_APP_NAME is
/// observed (compared to `last_id`), recording one Transition. Returns
/// `true` if a transition was recorded, `false` if no transition was
/// observed before the per-message deadline.
///
/// `last_id` is updated on transition so the next call starts fresh.
/// Multiple heartbeats with the same deployment_id (e.g. tick beats)
/// are dropped.
async fn collect_next_transition(
    sub: &mut Subscriber,
    last_id: &mut Option<String>,
    transitions: &mut Vec<Transition>,
) -> anyhow::Result<bool> {
    // Per-message deadline: we expect an immediate heartbeat within
    // ~2s; 5s is generous.
    let per_msg_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if transitions.len() >= 3 {
            return Ok(true);
        }
        let remaining = per_msg_deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            return Ok(false);
        }
        let msg = match timeout(remaining, sub.next()).await {
            Ok(Some(m)) => m,
            Ok(None) => return Err(anyhow!("heartbeat subscription closed")),
            Err(_) => return Ok(false), // per-msg timeout; caller decides
        };
        let hb: HeartbeatMessage =
            serde_json::from_slice(&msg.payload).context("parse heartbeat")?;
        let Some(app) = hb.apps.get(TEST_APP_NAME) else {
            continue; // heartbeat before our app appeared; skip
        };
        let new_id = app.deployment_id.clone();
        if last_id.as_deref() != Some(&new_id) {
            transitions.push(Transition {
                deployment_id: new_id.clone(),
                port: app.port,
                status: app.status.clone(),
            });
            *last_id = Some(new_id);
            return Ok(true);
        }
        // Same deployment_id as last observed — keep draining until
        // either the per-msg deadline fires (return false) or the
        // next TaskMessage's heartbeat flips it.
    }
}

// ---- helpers ----

#[derive(Debug, Clone)]
struct Transition {
    deployment_id: String,
    port: u16,
    status: String,
}

/// Subscribe to the heartbeat subject for `region`. The `.flush()`
/// after subscribe is load-bearing — see `heartbeat_subscriber` in
/// `integration_tests.rs`.
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
