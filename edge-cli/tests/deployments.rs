//! Integration tests for `edge deployments`.
//!
//! The server returns a `{items, total, limit, offset}` envelope
//! (see `deploymentListResponse` in
//! `edge-control-plane/internal/handler/deployment.go`). Prior to
//! the wire-mismatch fix, the CLI tried to deserialize the envelope
//! as a bare array and failed at runtime — these tests pin the new
//! shape so it can't regress.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

/// Write a minimal `edge.toml` so `ApiClient::new` can resolve the
/// base URL via `EDGE_API_URL` (the env-supplied fallback).
fn write_minimal_edge_toml(project: &TempDir) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "deployments-test"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
    )
    .unwrap();
}

/// Write a `.edge/state.json` carrying the app name the
/// `edge deployments` command reads (it pulls `app_name` from
/// local state rather than taking an arg).
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

#[tokio::test]
async fn list_returns_empty_message() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/list/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [],
            "total": 0,
            "limit": 20,
            "offset": 0
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("deployments");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("No deployments found."));
}

#[tokio::test]
async fn list_renders_envelope_items_table() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/list/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [
                {
                    "id": "d_first",
                    "status": "deployed",
                    "created_at": "2026-07-01T00:00:00Z",
                    "url": "https://t_test-myapp.edgecloud.dev"
                },
                {
                    "id": "d_second",
                    "status": "active",
                    "created_at": "2026-07-02T00:00:00Z",
                    "url": "https://t_test-myapp.edgecloud.dev"
                }
            ],
            "total": 2,
            "limit": 20,
            "offset": 0
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("deployments");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("d_first"))
        .stdout(predicate::str::contains("d_second"))
        .stdout(predicate::str::contains("deployed"))
        .stdout(predicate::str::contains("active"))
        .stdout(predicate::str::contains(
            "https://t_test-myapp.edgecloud.dev",
        ));
}

/// Pinned regression test for the wire-mismatch fix: the CLI must
/// NOT silently succeed against the pre-fix envelope shape (bare
/// array of objects, no top-level items/total/limit/offset). The
/// server shape changed to a typed envelope in commit 1 of the
/// wire-mismatch fix; this test would fail if the CLI started
/// parsing the envelope as an array again.
#[tokio::test]
async fn list_rejects_pre_fix_bare_array_shape() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/list/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "d_legacy",
                "status": "deployed",
                "created_at": "2026-01-01T00:00:00Z",
                "url": "https://t_test-myapp.edgecloud.dev"
            }
        ])))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("deployments");

    // Expect a non-zero exit: serde_json fails to deserialize the
    // bare array into DeploymentListResponse { items, total, limit,
    // offset } and the CLI surfaces "invalid response body".
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("invalid response body"));
}
