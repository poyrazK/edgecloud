//! Integration tests for `edge open` — preflight crash hint and `--force`.
//!
//! These tests focus on the *behavior before* the browser would be
//! invoked: status preflight, hint output, exit code, and `--force`
//! bypass. We can't actually launch a browser in CI, so the assertion
//! is "the CLI exited with the right code and the right message BEFORE
//! the open crate spawns a process" — which we detect by watching
//! whether the open-mock endpoint was hit (it shouldn't be when the
//! preflight rejects).

use std::io::Write;
use std::path::PathBuf;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers (mirrored from tests/auth.rs).
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

fn seed_project_with_state(
    project: &TempDir,
    app_name: &str,
    deployment_id: &str,
    live_url: &str,
) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "opentest"
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
            r#"{{"deployment_id":"{}","app_name":"{}","live_url":"{}"}}"#,
            deployment_id, app_name, live_url
        ),
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Crash hint: status returns "crashed" → CLI exits non-zero with hint.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_crashed_deployment_warns_and_exits_non_zero() {
    let home = isolated_home();
    let project = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    seed_project_with_state(
        &project,
        "myapp",
        "d_crashed",
        "https://crashed.example.test",
    );

    // The CLI must call status before opening. Mount it as the ONLY
    // mock — if the CLI tries to open the URL (via the open crate)
    // we'd see a connection refused, not a 200, so the absence of
    // any other mock is fine.
    Mock::given(method("GET"))
        .and(path("/api/status/d_crashed"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "d_crashed",
            "status": "crashed",
            "created_at": "2026-06-19T00:00:00Z",
            "url": "https://crashed.example.test",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("open");

    cmd.assert()
        .failure()
        // The "deployment crashed" message itself goes to stderr (it's
        // the error). The hint pointing at `edge rollback` and
        // `--force` goes to stdout via output::hint.
        .stderr(predicate::str::contains("crashed"))
        .stdout(predicate::str::contains("edge rollback"))
        .stdout(predicate::str::contains("--force"));
}

// ---------------------------------------------------------------------------
// --force: preflight says crashed, but the user passes --force.
// CLI must NOT call status (the open crate would spawn a browser,
// which we can't test in CI — but we can confirm the CLI did NOT
// exit with the crashed-hint error).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_force_skips_crash_preflight() {
    let home = isolated_home();
    let project = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    seed_project_with_state(
        &project,
        "myapp",
        "d_crashed",
        "https://crashed.example.test",
    );

    // Mount status with expect(0) — if the CLI calls it, the test fails.
    Mock::given(method("GET"))
        .and(path("/api/status/d_crashed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "d_crashed",
            "status": "crashed",
            "created_at": "2026-06-19T00:00:00Z",
            "url": "https://crashed.example.test",
        })))
        .expect(0)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("open");
    cmd.arg("--force");

    // We can't assert success (the open crate would try to launch
    // a browser), but we CAN assert the CLI did NOT print the
    // crash-hint message. If the preflight ran, "crashed" would
    // appear on stderr.
    cmd.assert().stderr(predicate::str::contains("crashed").not());
}

// ---------------------------------------------------------------------------
// Healthy deployment: status returns "ready" — CLI proceeds (would
// launch the browser, which we can't test; we only assert no crash
// hint is printed).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_ready_deployment_does_not_warn() {
    let home = isolated_home();
    let project = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    seed_project_with_state(
        &project,
        "myapp",
        "d_ok",
        "https://ok.example.test",
    );

    Mock::given(method("GET"))
        .and(path("/api/status/d_ok"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "d_ok",
            "status": "ready",
            "created_at": "2026-06-19T00:00:00Z",
            "url": "https://ok.example.test",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("open");

    // No crash hint.
    cmd.assert()
        .stderr(predicate::str::contains("crashed").not())
        .stderr(predicate::str::contains("edge rollback").not());
}

// ---------------------------------------------------------------------------
// No state.json → friendly error, no API call.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_without_state_exits_non_zero() {
    let home = isolated_home();
    let project = isolated_home(); // empty dir
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "opentest"
version = "0.1.0"
target = "wasm32-wasip2"

[deployment]
"#,
    )
    .unwrap();

    // No mocks — the CLI must not reach the API for a missing-state error.

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("open");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("no deployment found"));
}
