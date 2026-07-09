//! Integration tests for `edge env list`.
//!
//! The server now returns a sorted `[{key, value}]` array
//! (see `envVarResponse` in
//! `edge-control-plane/internal/handler/env.go`). Prior to the
//! wire-mismatch fix, the server returned a `map[string]string`
//! object and the CLI failed at deserialize time. These tests pin
//! the new array shape so it can't regress.

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
name = "env-test"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
    )
    .unwrap();
}

/// Write a `.edge/state.json` carrying the app name the
/// `edge env list` command reads (it pulls `app_name` from local
/// state rather than taking an arg).
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
        .and(path("/api/v1/apps/myapp/env"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).args(["env-list"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("No environment variables set."));
}

#[tokio::test]
async fn list_renders_array_shape() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/env"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"key": "DATABASE_URL", "value": "postgres://localhost"},
            {"key": "LOG_LEVEL", "value": "debug"},
        ])))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).args(["env-list"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains(
            "DATABASE_URL = postgres://localhost",
        ))
        .stdout(predicate::str::contains("LOG_LEVEL = debug"));
}

/// Pinned regression test for the wire-mismatch fix: the server's
/// env list shape changed from `map[string]string` to a typed
/// array in commit 2 of the fix. The CLI must NOT silently succeed
/// against the pre-fix object shape — that would mask a server
/// regression that dropped the array wrapper.
#[tokio::test]
async fn list_rejects_pre_fix_map_shape() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/env"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "DATABASE_URL": "postgres://localhost",
            "LOG_LEVEL": "debug",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).args(["env-list"]);

    // Expect a non-zero exit: serde_json fails to deserialize the
    // JSON object into Vec<EnvVar> and the CLI surfaces
    // "invalid response body".
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("invalid response body"));
}
