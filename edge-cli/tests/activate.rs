//! Integration tests for `edge activate`.
//!
//! Uses `wiremock` for the control plane, `assert_cmd` to drive the
//! `edge` binary, and `HOME` override (via `dirs::config_dir()`) to
//! isolate the config file per-test. Cross-cutting CLI helpers
//! (isolated_home / config_file_for / set_platform_env / seed_api_key)
//! live in `tests/common/mod.rs`; the activate-specific project seeder
//! stays below.
//!
//! The `edge activate <id>` subcommand takes ONE positional argument
//! (the deployment id). The app name is resolved from
//! `.edge/state.json` — that's why every happy-path test seeds state
//! with both fields. On success, state.json must be updated so that
//! subsequent `edge open` / `edge rollback` / `edge status` see the
//! new active id.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

/// Seed a minimal `edge.toml` (no `[deployment].api` — URL falls
/// through to `EDGE_API_URL`) plus a `.edge/state.json` that records
/// the prior active deployment for `app_name`.
fn seed_project(project: &TempDir, app_name: &str, current_deployment_id: &str) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "activatetest"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
    )
    .unwrap();
    std::fs::create_dir_all(project.path().join(".edge")).unwrap();
    std::fs::write(
        project.path().join(".edge").join("state.json"),
        format!(
            r#"{{"deployment_id":"{}","app_name":"{}","live_url":""}}"#,
            current_deployment_id, app_name
        ),
    )
    .unwrap();
}

fn read_state_deployment_id(project: &TempDir) -> Option<String> {
    let path = project.path().join(".edge").join("state.json");
    let content = std::fs::read_to_string(&path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&content).ok()?;
    v.get("deployment_id")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

// ---------------------------------------------------------------------------
// Happy path: server returns 200; CLI persists the activated id.
// ---------------------------------------------------------------------------

// activate_persists_returned_deployment_id_to_state_json covers the
// contract that motivated the dedicated Activate subcommand: a tenant
// wants to point their app at a previously-deployed artifact (e.g.
// from `edge migrate`) and have subsequent commands (`open`,
// `rollback`, `status`) see the new id. After `edge activate d_new`,
// state.json.deployment_id MUST be "d_new" — otherwise the next
// `edge open` would open the OLD deployment's URL.
#[tokio::test]
async fn activate_persists_returned_deployment_id_to_state_json() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp", "d_old");

    Mock::given(method("POST"))
        .and(path("/api/v1/apps/myapp/activate/d_new"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "activated",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("activate");
    cmd.arg("d_new");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("d_new"));

    // state.json must now reflect the activated id.
    let stored = read_state_deployment_id(&project).expect("state.json exists");
    assert_eq!(
        stored, "d_new",
        "state.json.deployment_id should be updated to the activated id"
    );
}

// ---------------------------------------------------------------------------
// Server error: CLI exits non-zero and does NOT mutate state.json.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn activate_server_error_does_not_overwrite_state() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp", "d_old");

    Mock::given(method("POST"))
        .and(path("/api/v1/apps/myapp/activate/d_new"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("activate");
    cmd.arg("d_new");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("500"))
        .stderr(predicate::str::contains("boom"));

    // state.json must NOT be mutated on a failed activate.
    let stored = read_state_deployment_id(&project).expect("state.json exists");
    assert_eq!(
        stored, "d_old",
        "state.json must not be updated on a failed activate"
    );
}
