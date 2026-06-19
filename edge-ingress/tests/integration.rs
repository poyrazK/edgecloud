//! Integration test for the edge-ingress heartbeat → Caddy pipeline.
//!
//! Spins up a real NATS container via testcontainers, stands up a wiremock
//! to impersonate the Caddy admin API, publishes a synthetic heartbeat
//! directly to NATS, and asserts that the wiremock received a `POST /load`
//! (proving the full NATS → routing-table → renderer → Caddy pipeline
//! fired end-to-end). The exact JSON body shape is covered by unit tests
//! in `caddy.rs`.
//!
//! Run with: cargo test --manifest-path edge-ingress/Cargo.toml --test integration
//!
//! Skips automatically when Docker is unavailable (no `/var/run/docker.sock`)
//! or in CI environments (`CI` env var or `SKIP_INTEGRATION_TESTS=1`).

use std::time::Duration;

use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerRequest;
use testcontainers::ImageExt;
use testcontainers_modules::nats::Nats;
use tokio::time::timeout;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use edge_ingress::caddy::CaddyClient;
use edge_ingress::config::Config;
use edge_ingress::heartbeats;
use edge_ingress::routing::RoutingTable;

// TODO(shared-test-harness): this helper and the `start_nats`
// testcontainers setup below are byte-for-byte copies of the same code
// in `edge-worker/tests/integration_tests.rs`. Extract both into a
// shared `edge-test-helpers` crate (workspace-relative) so a future
// change to the test-skip policy or the NATS startup contract lands in
// one place.
fn should_skip_integration_tests() -> bool {
    std::env::var("SKIP_INTEGRATION_TESTS").is_ok()
        || std::env::var("CI").is_ok()
        || !std::path::Path::new("/var/run/docker.sock").exists()
}

async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
    let container: testcontainers::ContainerAsync<Nats> = ContainerRequest::from(Nats::default())
        .with_startup_timeout(Duration::from_secs(30))
        .with_ready_conditions(vec![WaitFor::Duration {
            length: Duration::from_secs(5),
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

fn test_config(nats_url: String, caddy_admin_url: String) -> Config {
    Config {
        nats_url,
        caddy_admin_url,
        region: "test-region".into(),
        cert_file: "/tmp/test-cert.pem".into(),
        key_file: "/tmp/test-key.pem".into(),
        listen_http: ":80".into(),
        listen_https: ":443".into(),
        refresh_debounce_ms: 50,
        http_to_https: false,
        admin_token: None,
    }
}

/// Heartbeat published to NATS must reach the wiremock (Caddy admin stub)
/// within a few seconds — proves the full pipeline is wired.
#[tokio::test]
async fn heartbeat_pipeline_drives_a_caddy_reload() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let (_nats, nats_url) = start_nats().await;

    // Stand up a wiremock that responds 200 to every POST /load. The
    // body shape is asserted by unit tests; here we just need to see
    // *some* request land, which proves the pipeline ran.
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/load"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1..)
        .mount(&mock_server)
        .await;

    let cfg = test_config(nats_url.clone(), mock_server.uri());
    let table = std::sync::Arc::new(RoutingTable::new());
    let caddy = std::sync::Arc::new(
        CaddyClient::new(&cfg.caddy_admin_url, cfg.admin_token.clone()).expect("caddy client"),
    );

    // Run the heartbeat pipeline in the background. It returns when the
    // NATS subscription ends — we want it to keep running while we publish.
    let run_cfg = cfg.clone();
    let run_table = table.clone();
    let run_caddy = caddy.clone();
    let pipeline =
        tokio::spawn(async move { heartbeats::run(run_cfg, run_table, run_caddy).await });

    // Give the pipeline a beat to subscribe to NATS.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish a synthetic heartbeat directly to the NATS subject the
    // pipeline subscribes to.
    let client = async_nats::connect(&nats_url)
        .await
        .expect("publish-side NATS connect");
    let subject = format!("edgecloud.heartbeats.{}", cfg.region);
    let payload = serde_json::json!({
        "type": "heartbeat",
        "timestamp": "2026-06-17T12:00:00Z",
        "worker_id": "w_test",
        "region": cfg.region,
        "worker_addr": "203.0.113.10",
        "apps": {
            "myapp": {
                "deployment_id": "d_test",
                "status": "running",
                "exit_code": null,
                "request_count": 0,
                "tenant_id": "t_acme",
                "port": 8081u16,
            }
        }
    })
    .to_string();
    client
        .publish(subject, payload.into())
        .await
        .expect("publish heartbeat");
    client.flush().await.expect("flush");

    // Wait up to 5s for the wiremock to record a request. The pipeline
    // subscribes → upserts → notify → debounce 50ms → POST /load. 5s is
    // more than enough on a developer machine.
    let deadline = Duration::from_secs(5);
    let received = timeout(deadline, async {
        loop {
            let reqs = mock_server.received_requests().await.unwrap_or_default();
            if !reqs.is_empty() {
                return reqs.len();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("wiremock saw a POST /load within 5s");

    assert!(
        received >= 1,
        "expected at least one Caddy reload, got {received}"
    );

    // Stop the pipeline (it'll error out on the next drop — that's fine).
    pipeline.abort();
}

/// Heartbeats with no `worker_addr` must NOT drive a Caddy reload.
#[tokio::test]
async fn heartbeat_without_worker_addr_is_ignored() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let (_nats, nats_url) = start_nats().await;

    let mock_server = MockServer::start().await;
    // The renderer also pushes an initial empty config at boot. So we set
    // expect = 1 to allow the boot push, but a heartbeat with no addr
    // should not produce a *second* push. We give a 2s observation window.
    Mock::given(method("POST"))
        .and(path("/load"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1) // exactly one: the boot push
        .mount(&mock_server)
        .await;

    let cfg = test_config(nats_url.clone(), mock_server.uri());
    let table = std::sync::Arc::new(RoutingTable::new());
    let caddy = std::sync::Arc::new(
        CaddyClient::new(&cfg.caddy_admin_url, cfg.admin_token.clone()).expect("caddy client"),
    );

    let run_cfg = cfg.clone();
    let run_table = table.clone();
    let run_caddy = caddy.clone();
    let pipeline =
        tokio::spawn(async move { heartbeats::run(run_cfg, run_table, run_caddy).await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = async_nats::connect(&nats_url)
        .await
        .expect("publish-side NATS connect");
    let subject = format!("edgecloud.heartbeats.{}", cfg.region);
    // No worker_addr, no per-app port.
    let payload = serde_json::json!({
        "type": "heartbeat",
        "timestamp": "2026-06-17T12:00:00Z",
        "worker_id": "w_test",
        "region": cfg.region,
        "apps": {
            "myapp": {
                "deployment_id": "d_test",
                "status": "running",
                "tenant_id": "t_acme"
            }
        }
    })
    .to_string();
    client
        .publish(subject, payload.into())
        .await
        .expect("publish heartbeat");
    client.flush().await.expect("flush");

    // Wait 2s — the renderer should NOT have pushed (it was already
    // pushed once at boot, mock_server.expect(1) caps further requests,
    // and 2s is plenty of time for the renderer to attempt another).
    tokio::time::sleep(Duration::from_secs(2)).await;

    let received = mock_server
        .received_requests()
        .await
        .unwrap_or_default()
        .len();
    assert_eq!(
        received, 1,
        "expected exactly 1 push (the boot push); got {received}"
    );

    pipeline.abort();
}
