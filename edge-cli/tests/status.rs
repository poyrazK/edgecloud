//! Integration tests for `edge status`.
//!
//! Covers both nested subcommands:
//!
//! * `edge status runtime <app>` — worker-reported status via
//!   `GET /api/v1/apps/{appName}/status`. The new command; this
//!   file's primary coverage.
//! * `edge status deployment` (and the no-arg `edge status`) — the
//!   legacy DB-row view. One regression test pins that we didn't
//!   break it.
//!
//! Pattern mirrors `tests/logs.rs`: wiremock for the control plane,
//! `assert_cmd` for the binary, `HOME` override for config isolation,
//! shared helpers in `tests/common/mod.rs`.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

/// Write a minimal `edge.toml` plus a `.edge/state.json` with the
/// given app name. Same shape as `tests/logs.rs::seed_project` so
/// the two test files share conventions — copy-paste rather than
/// extract because the field set is tiny and the file boundary
/// keeps test setup self-contained.
fn seed_project(project: &TempDir, app_name: &str) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "statustest"
version = "0.1.0"
target = "wasm32-wasip2"

[deployment]
"#,
    )
    .unwrap();
    std::fs::create_dir_all(project.path().join(".edge")).unwrap();
    std::fs::write(
        project.path().join(".edge").join("state.json"),
        format!(
            r#"{{"deployment_id":"d_irrelevant","app_name":"{}","live_url":""}}"#,
            app_name
        ),
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// `edge status runtime <app>` — happy paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_runtime_running_prints_all_fields() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/status"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "app_name": "myapp",
            "status": "running",
            "last_heartbeat": "2026-06-27T10:00:00Z",
            "region": "us-east-1",
            "worker_id": "w_us-east-1_h01",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("status");
    cmd.arg("runtime");
    cmd.arg("myapp");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Runtime Status — myapp"))
        .stdout(predicate::str::contains("running"))
        .stdout(predicate::str::contains("us-east-1"))
        .stdout(predicate::str::contains("w_us-east-1_h01"))
        .stdout(predicate::str::contains("2026-06-27T10:00:00Z"));
}

#[tokio::test]
async fn status_runtime_crashed_prints_exit_code() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "app_name": "myapp",
            "status": "crashed",
            "last_heartbeat": "2026-06-27T10:00:30Z",
            "region": "us-east-1",
            "worker_id": "w_us-east-1_h01",
            "exit_code": 137,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("status");
    cmd.arg("runtime");
    cmd.arg("myapp");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("crashed"))
        .stdout(predicate::str::contains("137"));
}

#[tokio::test]
async fn status_runtime_unknown_prints_unknown_status() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    // No worker has reported on this app. Region/worker_id are
    // empty strings, last_heartbeat is null. The endpoint returns
    // 200 (not 404) per the no-information-leak contract — see
    // handler/worker_status.go for the design rationale.
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "app_name": "myapp",
            "status": "unknown",
            "last_heartbeat": null,
            "region": "",
            "worker_id": "",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("status");
    cmd.arg("runtime");
    cmd.arg("myapp");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("unknown"))
        .stdout(predicate::str::contains("(none — no worker has reported)"));
}

#[tokio::test]
async fn status_runtime_hung_prints_hung_status() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "app_name": "myapp",
            "status": "hung",
            "last_heartbeat": "2026-06-27T10:01:00Z",
            "region": "eu-west-1",
            "worker_id": "w_eu-west-1_h03",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("status");
    cmd.arg("runtime");
    cmd.arg("myapp");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("hung"))
        .stdout(predicate::str::contains("eu-west-1"))
        .stdout(predicate::str::contains("w_eu-west-1_h03"));
}

// ---------------------------------------------------------------------------
// App-name resolution — falls back to `.edge/state.json` when no
// positional arg is given. Mirrors the same path in `edge logs`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_runtime_falls_back_to_state_json_when_arg_empty() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "fromstate");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/fromstate/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "app_name": "fromstate",
            "status": "running",
            "last_heartbeat": "2026-06-27T10:00:00Z",
            "region": "us-east-1",
            "worker_id": "w_us-east-1_h01",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("status");
    cmd.arg("runtime");
    // No positional app — should fall back to .edge/state.json.

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Runtime Status — fromstate"))
        .stdout(predicate::str::contains("running"));
}

// ---------------------------------------------------------------------------
// Error path — 500 from `/status` surfaces as a non-zero exit and a
// useful stderr message. Differs from `edge logs`' silent skip
// because the user explicitly asked for the data.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_runtime_propagates_500_with_exit_code_1() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/status"))
        .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
            "error": {"code": "INTERNAL", "message": "boom"},
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("status");
    cmd.arg("runtime");
    cmd.arg("myapp");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("runtime status"));
}

// ---------------------------------------------------------------------------
// Regression: the existing `edge status deployment` path (and the
// no-arg `edge status`) must still work. Confirms we didn't break
// the legacy DB-row view when nesting under `Status`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_deployment_still_works() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    // The legacy endpoint is /api/v1/status/{deployment_id}. The
    // state.json we wrote has "d_irrelevant" as the deployment_id.
    Mock::given(method("GET"))
        .and(path("/api/v1/status/d_irrelevant"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "d_irrelevant",
            "status": "active",
            "created_at": "2026-06-26T00:00:00Z",
            "url": "https://t_test-myapp.edgecloud.dev",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("status");
    cmd.arg("deployment");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Deployment Status"))
        .stdout(predicate::str::contains("d_irrelevant"))
        .stdout(predicate::str::contains("active"))
        .stdout(predicate::str::contains(
            "https://t_test-myapp.edgecloud.dev",
        ));
}

#[tokio::test]
async fn status_no_arg_routes_to_deployment_subcommand() {
    // The bare `edge status` form must keep working — it routes to
    // `StatusAction::Deployment` under the new nested-subcommand
    // structure. This is the backward-compat pin.
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/status/d_irrelevant"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "d_irrelevant",
            "status": "active",
            "created_at": "2026-06-26T00:00:00Z",
            "url": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("status");
    // No subcommand — must default to `deployment`.

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Deployment Status"))
        .stdout(predicate::str::contains("d_irrelevant"));
}
