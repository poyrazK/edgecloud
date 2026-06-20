//! Integration tests for `edge open` — preflight crash hint and `--force`.
//!
//! These tests focus on the *behavior before* the browser would be
//! invoked: status preflight, hint output, exit code, and `--force`
//! bypass. We can't actually launch a browser in CI, so the assertion
//! is "the CLI exited with the right code and the right message BEFORE
//! the open crate spawns a process" — which we detect by watching
//! whether the open-mock endpoint was hit (it shouldn't be when the
//! preflight rejects).

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

fn seed_project_with_state(project: &TempDir, app_name: &str, deployment_id: &str, live_url: &str) {
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
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
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
    common::set_platform_env(&mut cmd, &home);
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
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    // Use a URL that doesn't contain the substring "crashed" — on Linux
    // CI the open crate may try to launch xdg-open and surface the
    // URL in its own error message, which would false-positive a bare
    // substring assertion below.
    seed_project_with_state(&project, "myapp", "d_crashed", "https://app.example.test");

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
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("open");
    cmd.arg("--force");

    // We can't assert success (the open crate would try to launch
    // a browser — on Linux CI that may itself emit a noisy error
    // containing the URL, which could include substrings like
    // "crashed" if we used one in the URL).
    //
    // What we CAN assert: if the preflight had run, the CLI would
    // have bailed with the specific preflight error
    // "deployment <id> has crashed" BEFORE the open crate was called.
    // The absence of that exact phrase on stderr means the preflight
    // was skipped — i.e., --force worked.
    cmd.assert()
        .stderr(predicate::str::contains("has crashed").not());
}

// ---------------------------------------------------------------------------
// Healthy deployment: status returns "ready" — CLI proceeds (would
// launch the browser, which we can't test; we only assert no crash
// hint is printed).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_ready_deployment_does_not_warn() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project_with_state(&project, "myapp", "d_ok", "https://ok.example.test");

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
    common::set_platform_env(&mut cmd, &home);
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
    let home = common::isolated_home();
    let project = common::isolated_home(); // empty dir
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
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
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("open");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("no deployment found"));
}
