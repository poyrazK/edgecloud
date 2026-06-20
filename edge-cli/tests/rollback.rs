//! Integration tests for `edge rollback`.
//!
//! Uses `wiremock` for the control plane, `assert_cmd` to drive the
//! `edge` binary, and `HOME` override (via `dirs::config_dir()`) to
//! isolate the config file per-test. Mirrors the helpers in `tests/auth.rs`
//! — we duplicate the small helpers rather than introduce a shared
//! module to keep the test files independently runnable.

use std::io::Write;
use std::path::PathBuf;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers (mirrored from tests/auth.rs — kept local to avoid coupling).
// ---------------------------------------------------------------------------

fn isolated_home() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn config_file_for(home: &TempDir) -> PathBuf {
    if cfg!(target_os = "macos") {
        home.path()
            .join("Library")
            .join("Application Support")
            .join("edgecloud")
            .join("config.toml")
    } else if cfg!(target_os = "windows") {
        home.path()
            .join("AppData")
            .join("Roaming")
            .join("edgecloud")
            .join("config.toml")
    } else {
        home.path()
            .join(".config")
            .join("edgecloud")
            .join("config.toml")
    }
}

fn set_platform_env(cmd: &mut Command, home: &TempDir) {
    if cfg!(target_os = "windows") {
        cmd.env("APPDATA", home.path().join("AppData").join("Roaming"));
        cmd.env("USERPROFILE", home.path());
    } else {
        cmd.env("HOME", home.path());
    }
    cmd.env_remove("XDG_CONFIG_HOME");
    cmd.env_remove("EDGE_API_KEY");
}

fn seed_api_key(home: &TempDir, key: &str) {
    let cfg_path = config_file_for(home);
    if let Some(parent) = cfg_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(f, "[default]\napi_key = \"{key}\"\n").unwrap();
}

/// Write a minimal `edge.toml` (no `[deployment].api` — URL falls through
/// to EDGE_API_URL) plus a `.edge/state.json` so `edge rollback` can
/// resolve the app name and persist the new deployment id.
fn seed_project(project: &TempDir, app_name: &str, current_deployment_id: &str) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "rollbacktest"
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
// Happy path: server returns 200 + {deployment_id}; CLI persists it.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rollback_flips_state_to_returned_deployment_id() {
    let home = isolated_home();
    let project = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp", "d_broken");

    Mock::given(method("POST"))
        .and(path("/api/apps/myapp/rollback"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deployment_id": "d_prev",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("rollback");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Rolled back to deployment d_prev"))
        .stdout(predicate::str::contains("d_prev"));

    // state.json must now reflect the rolled-back id.
    let stored = read_state_deployment_id(&project).expect("state.json exists");
    assert_eq!(
        stored, "d_prev",
        "state.json.deployment_id should be updated to the rolled-back id"
    );
}

// ---------------------------------------------------------------------------
// 409: no last-good pointer. CLI must exit non-zero and surface a useful
// message (not a panic).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rollback_no_last_good_exits_non_zero() {
    let home = isolated_home();
    let project = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp", "d_only");

    Mock::given(method("POST"))
        .and(path("/api/apps/myapp/rollback"))
        .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
            "error": "no previous deployment to roll back to",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("rollback");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("rollback failed"))
        .stderr(predicate::str::contains("no previous deployment"));

    // state.json must NOT be mutated on a failed rollback.
    let stored = read_state_deployment_id(&project).expect("state.json exists");
    assert_eq!(
        stored, "d_only",
        "state.json must not be updated on a failed rollback"
    );
}

// ---------------------------------------------------------------------------
// App name resolution: positional `<app>` wins over `state.json.app_name`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rollback_resolves_app_from_positional_when_state_differs() {
    let home = isolated_home();
    let project = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    // state.json says app = "oldapp" but the user passed "newapp".
    seed_project(&project, "oldapp", "d_broken");

    Mock::given(method("POST"))
        .and(path("/api/apps/newapp/rollback"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deployment_id": "d_prev",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("rollback");
    cmd.arg("newapp");

    cmd.assert().success();
}

// ---------------------------------------------------------------------------
// App name resolution: positional empty → fall back to state.json.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rollback_resolves_app_from_state_when_arg_empty() {
    let home = isolated_home();
    let project = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    seed_project(&project, "fromstate", "d_broken");

    Mock::given(method("POST"))
        .and(path("/api/apps/fromstate/rollback"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deployment_id": "d_prev",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("rollback"); // no positional

    cmd.assert().success();
}

// ---------------------------------------------------------------------------
// state.json for a DIFFERENT app must not be overwritten even though the
// rollback succeeded for the resolved app.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rollback_does_not_overwrite_state_for_different_app() {
    let home = isolated_home();
    let project = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    // state.json says app = "oldapp" but the user passed "newapp".
    seed_project(&project, "oldapp", "d_oldapp_state");

    Mock::given(method("POST"))
        .and(path("/api/apps/newapp/rollback"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deployment_id": "d_newapp_prev",
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("rollback");
    cmd.arg("newapp");

    cmd.assert().success();

    // state.json still belongs to oldapp — must NOT have been touched.
    let stored = read_state_deployment_id(&project).expect("state.json exists");
    assert_eq!(
        stored, "d_oldapp_state",
        "state.json for a different app must not be overwritten"
    );
}

// ---------------------------------------------------------------------------
// No positional and no state.json → "requires an app name" error.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rollback_without_app_or_state_exits_non_zero() {
    let home = isolated_home();
    let project = isolated_home(); // empty dir, no state.json
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "rollbacktest"
version = "0.1.0"
target = "wasm32-wasip2"

[deployment]
"#,
    )
    .unwrap();

    // No mock mounted — if the CLI erroneously tries to hit the API,
    // it will get a connection refused and surface a different error.

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("rollback");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("requires an app name"));
}
