//! Cross-wire integration test for issue #70 (public ingress).
//!
//! Proves the end-to-end wire contract between `edge-worker` and
//! `edge-ingress`:
//!
//! 1. The worker serializes a `HeartbeatMessage` and publishes it on
//!    `edgecloud.heartbeats.<region>`.
//! 2. A second NATS subscriber receives the raw bytes and parses them
//!    back into a `HeartbeatMessage` — proves the published JSON contains
//!    `worker_addr` as a top-level string key (not nested, not renamed).
//! 3. `edge_ingress::heartbeats::apply_heartbeat` (the same function the
//!    NATS subscriber loop in `edge-ingress` calls) consumes the parsed
//!    message and inserts a row into the ingress's `RoutingTable`.
//!
//! This is the "no more 'looks correct on paper'" test: if any future
//! refactor breaks the worker_addr wire contract, the routing-table
//! assertion fails loudly at integration-test time, not silently in
//! production (where the failure mode is "traffic doesn't flow and we
//! don't know why").
//!
//! Run with: cargo test --manifest-path edge-worker/Cargo.toml
//! Skip in CI: SKIP_INTEGRATION_TESTS=1 cargo test ...

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use tokio::time::timeout;

use edge_test_helpers::{
    build_supervisor_from_url, default_cache_dir, should_skip_integration_tests, start_nats,
};
use edge_worker::messages::HeartbeatMessage;
use edge_worker::supervisor::Supervisor;

use edge_ingress::heartbeats::apply_heartbeat;
use edge_ingress::routing::RoutingTable;
use edge_runtime::socket_egress::SocketEgressPolicy;

/// Construct a `Config` matching the worker's runtime expectations, for
/// the heartbeat wire test (which never starts apps; the only fields it
/// cares about are worker_id / region / worker_addr).
fn wire_test_config(
    worker_id: &str,
    region: &str,
    worker_addr: &str,
) -> edge_worker::config::Config {
    edge_worker::config::Config {
        worker_jwt_kid: None,
        worker_id: worker_id.to_string(),
        region: region.to_string(),
        worker_addr: worker_addr.to_string(),
        nats_url: String::new(), // overwritten by build_supervisor_from_url
        control_plane_url: "http://localhost:9999".to_string(),
        cache_dir: default_cache_dir(),
        heartbeat_interval_secs: 30,
        worker_sync_threshold_secs: 60,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        port_pool_size: 100,
        starting_port: 19_000,
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        consumer_name: format!("ingress-wire-{worker_id}"),
        queue_group: String::new(),
        worker_jwt_secret: "test-secret".to_string(),
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
        socket_mode: SocketEgressPolicy::default(),
        hostname_pinning_enabled: false,
        standby_pool_size: 10,
        // Issue #307 PR2: signature config off — ingress wire
        // test focuses on the heartbeat → routing table path.
        require_signature: false,
        signing_keyring: None,
        signing_keyring_path: None,
    }
}

/// The full #70 contract: worker emits a heartbeat with `worker_addr`,
/// ingress parses it and populates its routing table. The table's
/// `worker_addr` field must match what the worker put on the wire —
/// proving the worker→ingress JSON shape didn't drift (e.g. a future
/// serde rename from `worker_addr` → `workerUrl` would deserialise to
/// `None` here and fail the assertion below).
#[tokio::test]
async fn heartbeat_worker_addr_round_trips_into_ingress_routing_table() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    timeout(Duration::from_secs(60), async {
        run_test().await.expect("ingress wire test failed");
    })
    .await
    .expect("ingress wire test timed out");
}

async fn run_test() -> anyhow::Result<()> {
    let (nats_container, nats_url) = start_nats().await;
    // Forgetting the container keeps it alive until the test runtime
    // exits. We can't use a `SupervisorGuard` here because we need the
    // raw NATS URL to subscribe to it directly from this test.
    std::mem::forget(nats_container);

    let region = "fra";
    let worker_id = "w_ingress_wire";
    // Routable address the ingress will dial. Not an actual IP — this
    // test never opens a TCP connection. The point is the string flows
    // through the wire unmodified.
    let worker_addr = "203.0.113.42:8080";

    let config = wire_test_config(worker_id, region, worker_addr);
    let supervisor: Arc<Supervisor> = build_supervisor_from_url(&nats_url, config).await?;

    // Subscribe from a separate async-nats client (simulating the ingress
    // process) BEFORE publishing — core NATS pub/sub has no durability,
    // so a message published before the subscriber's interest reaches
    // the server is lost with no redelivery. The flush() is load-bearing:
    // subscribe() resolves once the SUB command is buffered client-side,
    // NOT once the server has registered interest; the publisher runs on
    // a different connection, so without the round-trip the publish can
    // still race ahead of the SUB frame.
    let client = async_nats::connect(&nats_url).await?;
    let subject = format!("edgecloud.heartbeats.{}", region);
    let mut sub = client.subscribe(subject).await?;
    client.flush().await?;

    // Build the heartbeat exactly as the worker would on its 30s tick
    // (see `edge-worker/src/main.rs:110`).
    let heartbeat = supervisor.build_heartbeat().await;
    let worker_worker_addr = heartbeat
        .worker_addr
        .clone()
        .expect("worker_addr must be set on worker-built heartbeat");

    // Publish via the worker's own NatsClient — same code path the
    // heartbeat loop in `main.rs` uses — so we're testing the production
    // publisher, not a parallel hand-rolled one.
    supervisor
        .nats
        .publish_heartbeat(&supervisor.config.region, &heartbeat)
        .await
        .context("publish heartbeat via NatsClient")?;

    let msg = timeout(Duration::from_secs(5), sub.next())
        .await
        .context("subscription timed out — heartbeat was not published")?
        .context("subscription ended")?;

    // Assert 1: the raw JSON contains the worker_addr as a top-level
    // string key. This catches serde renames or accidental nesting.
    let raw = std::str::from_utf8(&msg.payload).context("payload not utf-8")?;
    let expected_substring = format!(r#""worker_addr":"{}""#, worker_addr);
    assert!(
        raw.contains(&expected_substring),
        "heartbeat wire must include worker_addr={} as a top-level string; got: {raw}",
        worker_addr
    );

    // Assert 2: the payload round-trips through the ingress's parser.
    let received: HeartbeatMessage =
        serde_json::from_slice(&msg.payload).context("ingress-side parse of heartbeat failed")?;
    assert_eq!(
        received.worker_addr.as_deref(),
        Some(worker_addr),
        "deserialised worker_addr must match what the worker emitted"
    );
    assert_eq!(received.worker_id, worker_id, "worker_id must round-trip");
    assert_eq!(received.region, region, "region must round-trip");

    // Assert 3a: an app-less heartbeat is the "worker shutting down"
    // signal — apply_heartbeat removes any routes for the worker and
    // returns false when there was nothing to remove. (The pre-#583
    // behavior of inserting a placeholder port-0 row is gone.)
    let table = Arc::new(RoutingTable::new());
    assert_eq!(table.len().await, 0, "table starts empty");

    let changed = apply_heartbeat(&table, &received).await;
    assert!(
        !changed,
        "empty-apps heartbeat against an empty table must be a no-op"
    );
    assert_eq!(
        table.len().await,
        0,
        "no placeholder row for app-less heartbeats"
    );

    // Assert 3b: the same wire-parsed message WITH an app populates the
    // routing table, and the row carries the worker's address verbatim —
    // the actual #70 contract (worker_addr flows into routing).
    let mut with_app = received.clone();
    with_app.apps.insert(
        "wire-app".to_string(),
        edge_worker::messages::AppStatus {
            deployment_id: "d_wire_001".to_string(),
            status: "running".to_string(),
            exit_code: None,
            request_count: 0,
            outbound_bytes: 0,
            tenant_id: "t_wire".to_string(),
            port: 8081,
            observer_metrics: Vec::new(),
            ws_port: None,
            dedupe_id: None,
            last_error: None,
            resident_seconds: None,
            duration_ms_total: 0,
        },
    );
    let changed = apply_heartbeat(&table, &with_app).await;
    assert!(
        changed,
        "apply_heartbeat must return true for a heartbeat with a running app"
    );

    let snap = table.snapshot().await;
    assert_eq!(snap.len(), 1, "exactly one route expected");
    assert_eq!(snap[0].worker_addr, worker_worker_addr);
    assert_eq!(snap[0].worker_addr, "203.0.113.42:8080");
    assert_eq!(snap[0].port, 8081, "route must carry the app's port");
    assert_eq!(snap[0].tenant_id, "t_wire", "route must carry the tenant");

    Ok(())
}
