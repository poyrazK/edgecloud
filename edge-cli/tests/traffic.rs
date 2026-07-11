//! Integration tests for the `edge traffic` subcommand group.
//!
//! `edge traffic set` is operator-tunable: main.rs wires
//! `--max-retries` / `--retry-base-ms` / `--retry-cap-ms` flags
//! directly on the `Set` variant (mirrors the deploy-side shape).
//! `edge traffic show` uses hardcoded sensible defaults — the read
//! path doesn't expose flags but routes through the same
//! `commands::retry::call_with_retry` loop.
//!
//! Issue #571 propagation: the retry contract is pinned end-to-end by
//! `traffic_set_retries_503_then_succeeds` — a 503-then-200 pair on
//! PUT /api/v1/apps/{app}/traffic. The CLI must drive both requests
//! with the same JSON body (the retry loop's same-args contract).

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

fn write_minimal_edge_toml(project: &TempDir) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "traffic-test"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
    )
    .unwrap();
}

/// `State::load` reads `.edge/state.json` — the `traffic set` helper
/// needs `app_name` from the persisted state to address the
/// `/api/v1/apps/{app_name}/traffic` route.
fn write_state_with_app(project: &TempDir, app_name: &str) {
    std::fs::create_dir_all(project.path().join(".edge")).unwrap();
    std::fs::write(
        project.path().join(".edge").join("state.json"),
        format!(
            r#"{{"app_name":"{}","deployment_id":"d_abc","live_url":"https://t_test-{}.edgecloud.dev","regions":[],"desired_replicas":0}}"#,
            app_name, app_name
        ),
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Issue #571 propagation: retry transient 5xx on traffic set.
//
// `edge traffic set` is operator-tunable — main.rs forwards
// `--max-retries` / `--retry-base-ms` / `--retry-cap-ms` to it. The
// test passes `--retry-base-ms=10` so each retry sleeps ~10ms instead
// of the default 500ms; the test runs in well under a second.
//
// The PUT body uses `body_string` to pin the exact JSON wire shape —
// the test fails (with a wiremock matcher mismatch, not a panic) if
// the retry loop permutes the args or rebuilds the splits list.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn traffic_set_retries_503_then_succeeds() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    Mock::given(method("PUT"))
        .and(path("/api/v1/apps/myapp/traffic"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(body_string(
            r#"{"splits":[{"deployment_id":"d_v1","weight":95},{"deployment_id":"d_v2","weight":5}]}"#,
        ))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .named("traffic-set-503")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/apps/myapp/traffic"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(body_string(
            r#"{"splits":[{"deployment_id":"d_v1","weight":95},{"deployment_id":"d_v2","weight":5}]}"#,
        ))
        .respond_with(ResponseTemplate::new(200))
        .named("traffic-set-200")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri());
    cmd.env("EDGE_CLI_RETRY_BASE_MS", "10");
    cmd.timeout(std::time::Duration::from_secs(15));
    cmd.arg("traffic")
        .arg("set")
        .arg("d_v1=95")
        .arg("d_v2=5")
        .arg("--retry-base-ms=10");

    cmd.assert().success();
    let received = server.received_requests().await.expect("received requests");
    let put_count = received
        .iter()
        .filter(|r| r.method.as_str() == "PUT" && r.url.path() == "/api/v1/apps/myapp/traffic")
        .count();
    assert_eq!(
        put_count, 2,
        "expected 503 + 200 = 2 PUT attempts on /api/v1/apps/myapp/traffic"
    );
}

// ---------------------------------------------------------------------------
// Baseline happy-path: a single PUT to `edge traffic set` succeeds
// against a 200 mock. Locks the JSON wire shape of the splits body
// (mirrors the 503-then-200 test's body_string matcher).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn traffic_set_happy_path() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("PUT"))
        .and(path("/api/v1/apps/myapp/traffic"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(body_string(
            r#"{"splits":[{"deployment_id":"d_v1","weight":100}]}"#,
        ))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri());
    cmd.arg("traffic")
        .arg("set")
        .arg("d_v1=100")
        .arg("--max-retries=0");

    cmd.assert().success();
}

/// Baseline happy-path: `edge traffic show` GETs the splits array
/// and prints it. The retry test for the read path lives next to
/// the SET retry test (the retry loop is generic — proving set
/// works is sufficient to trust the show-side wiring).
#[tokio::test]
async fn traffic_show_happy_path() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/traffic"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "splits": [
                {"deployment_id": "d_v1", "weight": 80},
                {"deployment_id": "d_v2", "weight": 20},
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri());
    cmd.arg("traffic").arg("show");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("80%"))
        .stdout(predicate::str::contains("d_v1"))
        .stdout(predicate::str::contains("20%"))
        .stdout(predicate::str::contains("d_v2"));
}
