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

use std::sync::OnceLock;
use std::time::Duration;

use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use edge_ingress::caddy::CaddyClient;
use edge_ingress::config::Config;
use edge_ingress::heartbeats;
use edge_ingress::routing::RoutingTable;

// Shared test harness: NATS container startup + skip predicate, imported
// from `edge_test_helpers`. These were byte-for-byte duplicates of the
// same helpers in `edge-worker/tests/integration_tests.rs` and
// `edge-worker/tests/ingress_wire_integration.rs` — see PR #166
// follow-up #4 for the rationale.
use edge_test_helpers::{should_skip_integration_tests, start_nats};

fn test_config(nats_url: String, caddy_admin_url: String) -> Config {
    Config {
        nats_url,
        caddy_admin_url,
        region: "test-region".into(),
        cert_file: "/tmp/test-cert.pem".into(),
        key_file: "/tmp/test-key.pem".into(),
        cert_file_2: None,
        key_file_2: None,
        listen_http: ":80".into(),
        listen_https: ":443".into(),
        refresh_debounce_ms: 50,
        http_to_https: false,
        admin_token: None,
        control_plane_api_url: "http://localhost:8080".into(),
        internal_token: None,
        control_plane_url: String::new(),
        service_token: String::new(),
        domain_poll_interval: Duration::from_secs(30),
        caddy_admin_listen: "localhost:2019".into(),
        metrics_listen: ":9091".into(),
        max_conns: 0,
        max_conns_per_ip: 0,
        per_ip_rps: 0,
        per_ip_burst: 0,
        rate_limit_rps_default: 0,
        rate_limit_burst_default: 0,
        rate_limit_fetch_interval: Duration::from_secs(60),
        quota_fetch_interval: Duration::from_secs(30),
        stale_timeout: Duration::from_secs(60),
        prune_interval: Duration::from_secs(30),
        health_check_interval: Duration::from_secs(10),
        health_check_timeout: Duration::from_secs(3),
        health_check_uri: "/healthz".into(),
        health_check_max_fails: 2,
    }
}

/// Install the Prometheus recorder globally exactly once across all
/// integration tests. Returns the handle so tests can call
/// `handle.render()` to assert metric names and values.
fn install_metrics_recorder() -> &'static metrics_exporter_prometheus::PrometheusHandle {
    static METRICS_HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
        OnceLock::new();
    METRICS_HANDLE.get_or_init(|| {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        // Install globally so the `metrics::counter!()` and similar macros
        // in the production code actually record data. Since the OnceLock
        // guards this, set_global_recorder is only called once.
        let _ = metrics::set_global_recorder(recorder);
        handle
    })
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
    // Pass a fresh Notify — the integration test doesn't have a
    // second task to coordinate with, so this Notify is just to
    // satisfy the post-#133 review signature. The renderer inside
    // heartbeats::run awaits it; it never fires (no domain poller
    // exists here) but the boot push fires from `push_now` directly.
    let pipeline_notify = std::sync::Arc::new(tokio::sync::Notify::new());
    let pipeline = tokio::spawn({
        let pipe_n = pipeline_notify.clone();
        async move {
            heartbeats::run(
                run_cfg,
                run_table,
                run_caddy,
                pipe_n,
                CancellationToken::new(),
            )
            .await
        }
    });

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

/// After a full heartbeat pipeline run, the Prometheus handle must contain
/// the expected metric names and values. This proves the instrumentation
/// macros fire correctly through the real code paths.
#[tokio::test]
async fn metrics_are_recorded_through_heartbeat_pipeline() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    let (_nats, nats_url) = start_nats().await;

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

    let pipeline_notify = std::sync::Arc::new(tokio::sync::Notify::new());
    let pipeline = tokio::spawn({
        let run_cfg = cfg.clone();
        let run_table = table.clone();
        let run_caddy = caddy.clone();
        let n = pipeline_notify.clone();
        async move { heartbeats::run(run_cfg, run_table, run_caddy, n, CancellationToken::new()).await }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish a heartbeat the pipeline can process.
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

    // Wait for the pipeline to process the heartbeat and trigger a reload.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Read recorded metrics from the handle.
    let handle = install_metrics_recorder();
    let output = handle.render();

    // The boot push and heartbeat push should have produced reload attempts.
    assert!(
        output.contains("ingress_caddy_reload_total"),
        "expected reload_total metric in render output:\n{output}"
    );
    assert!(
        output.contains("ingress_caddy_reload_total{status=\"success\"}"),
        "expected a success counter in render output:\n{output}"
    );
    // routes.active should be 1 (the one app).
    assert!(
        output.contains("ingress_routes_active 1"),
        "expected routes_active = 1:\n{output}"
    );
    // fqdns.active should be 0 (no domain poller running).
    assert!(
        output.contains("ingress_fqdns_active 0"),
        "expected fqdns_active = 0:\n{output}"
    );
    // heartbeats counter should have incremented (region tag).
    assert!(
        output.contains("ingress_heartbeats_received{region=\"test-region\"} 1"),
        "expected one heartbeat received:\n{output}"
    );

    // Histogram buckets should exist for render and reload durations.
    assert!(
        output.contains("ingress_caddy_render_duration_seconds"),
        "expected render duration histogram presence:\n{output}"
    );
    assert!(
        output.contains("ingress_caddy_reload_duration_seconds"),
        "expected reload duration histogram presence:\n{output}"
    );

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
    let pipeline_notify = std::sync::Arc::new(tokio::sync::Notify::new());
    let pipeline = tokio::spawn(async move {
        heartbeats::run(
            run_cfg,
            run_table,
            run_caddy,
            pipeline_notify,
            CancellationToken::new(),
        )
        .await
    });

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
