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
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerRequest;
use testcontainers::ImageExt;
use testcontainers_modules::nats::Nats;
use tokio::sync::Mutex as TokioMutex;
use tokio::time::timeout;

use edge_worker::auth::WorkerJwtSigner;
use edge_worker::config::Config;
use edge_worker::downloader::Downloader;
use edge_worker::log_forwarder::LogForwarder;
use edge_worker::messages::HeartbeatMessage;
use edge_worker::nats::{NatsClient as NatsClientTrait, NatsClientImpl};
use edge_worker::port_pool::PortPool;
use edge_worker::state::WorkerState;
use edge_worker::supervisor::Supervisor;

use edge_ingress::heartbeats::apply_heartbeat;
use edge_ingress::routing::RoutingTable;

// TODO(shared-test-harness): byte-for-byte duplicate of the same helpers
// in `edge-worker/tests/integration_tests.rs` and `edge-ingress/tests/integration.rs`.
// Extract `should_skip_integration_tests` and the NATS container startup
// into a shared `edge-test-helpers` crate so changes to the skip policy
// or NATS startup contract land in one place. Tracked separately to
// avoid expanding the scope of this PR.

fn should_skip_integration_tests() -> bool {
    std::env::var("SKIP_INTEGRATION_TESTS").is_ok()
        || std::env::var("CI").is_ok()
        || !std::path::Path::new("/var/run/docker.sock").exists()
}

async fn nats_container() -> (testcontainers::ContainerAsync<Nats>, String) {
    let container: testcontainers::ContainerAsync<Nats> = ContainerRequest::from(Nats::default())
        .with_startup_timeout(std::time::Duration::from_secs(30))
        .with_ready_conditions(vec![WaitFor::Duration {
            length: std::time::Duration::from_secs(5),
        }])
        .start()
        .await
        .expect("start NATS container");
    let host = container.get_host().await.expect("get host");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("get NATS port");
    (container, format!("{}:{}", host, port))
}

/// Build a minimal Supervisor pointed at the given NATS URL. No mock HTTP
/// server is needed — this test never starts apps, it only verifies the
/// heartbeat wire contract.
async fn build_supervisor(
    nats_url: &str,
    worker_id: &str,
    region: &str,
    worker_addr: &str,
) -> anyhow::Result<Arc<Supervisor>> {
    let config = Config {
        worker_id: worker_id.to_string(),
        region: region.to_string(),
        worker_addr: worker_addr.to_string(),
        nats_url: nats_url.to_string(),
        control_plane_url: "http://localhost:9999".to_string(),
        cache_dir: std::path::PathBuf::from("/tmp/edge-worker-ingress-wire-test"),
        heartbeat_interval_secs: 30,
        worker_sync_threshold_secs: 60,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        starting_port: 19_000,
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        queue_group: "ingress-wire-group".to_string(),
        consumer_name: format!("ingress-wire-{}", worker_id),
        // JWT fields: required by Config, but the wire tests construct
        // Config directly and never hit the auth path against a real
        // control plane (the mock server accepts anything). Any non-empty
        // placeholder is fine.
        worker_jwt_secret: "test-secret".to_string(),
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: "t_test".to_string(),
        handler_request_budget_ms: 1000,
        handler_max_request_body_bytes: 10 * 1024 * 1024,
    };

    let engine = edge_runtime::create_engine().context("create engine")?;
    let state = Arc::new(tokio::sync::RwLock::new(WorkerState::new(engine)));
    let jwt_signer = WorkerJwtSigner::new(
        config.worker_jwt_secret.clone(),
        config.worker_jwt_issuer.clone(),
        config.worker_id.clone(),
        config.region.clone(),
        config.worker_tenant_id.clone(),
    );
    let downloader = Arc::new(Downloader::new(
        config.control_plane_url.clone(),
        config.cache_dir.clone(),
        jwt_signer.clone(),
    ));
    let port_pool = Arc::new(TokioMutex::new(PortPool::new(
        config.starting_port,
        config.port_cooldown_secs,
    )));

    let nats = Arc::new(NatsClientImpl::connect(nats_url).await?) as Arc<dyn NatsClientTrait>;
    let log_forwarder = LogForwarder::new(
        config.control_plane_url.clone(),
        config.worker_id.clone(),
        config.region.clone(),
        jwt_signer,
    );
    Ok(Arc::new(Supervisor {
        config,
        state,
        downloader,
        port_pool,
        nats,
        log_forwarder,
    }))
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
    let (nats_container, nats_url) = nats_container().await;
    std::mem::forget(nats_container); // keep alive for the duration of the test; dropped at fn return

    let region = "fra";
    let worker_id = "w_ingress_wire";
    // Routable address the ingress will dial. Not an actual IP — this
    // test never opens a TCP connection. The point is the string flows
    // through the wire unmodified.
    let worker_addr = "203.0.113.42:8080";

    let supervisor = build_supervisor(&nats_url, worker_id, region, worker_addr).await?;

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

    // Subscribe from a separate async-nats client (simulating the ingress
    // process) and pull the raw bytes.
    let client = async_nats::connect(&nats_url).await?;
    let subject = format!("edgecloud.heartbeats.{}", region);
    let mut sub = client.subscribe(subject).await?;
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

    // Assert 3: the ingress's apply_heartbeat populates the routing
    // table from the parsed message. The table is empty before the
    // call, has exactly one row after, and the row carries the
    // worker's address verbatim.
    let table = Arc::new(RoutingTable::new());
    assert_eq!(table.len().await, 0, "table starts empty");

    let changed = apply_heartbeat(&table, &received).await;
    assert!(
        changed,
        "apply_heartbeat must return true for a valid heartbeat"
    );

    let snap = table.snapshot().await;
    assert_eq!(snap.len(), 1, "exactly one route expected");
    assert_eq!(snap[0].worker_addr, worker_worker_addr);
    assert_eq!(snap[0].worker_addr, "203.0.113.42:8080");
    assert_eq!(
        snap[0].port, 0,
        "no apps in heartbeat ⇒ port is the default 0"
    );
    assert_eq!(snap[0].tenant_id, "", "no apps ⇒ no tenant_id");

    Ok(())
}
