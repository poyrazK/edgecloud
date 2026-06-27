//! Integration tests for `edge logs`.
//!
//! Uses `wiremock` for the control plane, `assert_cmd` to drive the
//! `edge` binary, and `HOME` override (via `dirs::config_dir()`) to
//! isolate the config file per-test. The cross-cutting helpers
//! (isolated_home / seed_api_key / set_platform_env) live in
//! `tests/common/mod.rs`; the logs-specific project seeder stays
//! below.

use assert_cmd::Command;
use predicates::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

/// Write a minimal `edge.toml` (no `[deployment].api` — URL falls
/// through to EDGE_API_URL) plus a `.edge/state.json` so `edge logs`
/// can resolve the app name from the state when not passed
/// positionally. The deployment_id field is required by State
/// deserialization but is irrelevant for the read path; pick any
/// non-empty value.
fn seed_project(project: &TempDir, app_name: &str) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "logstest"
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
// Happy path: server returns 2 entries; CLI prints both.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn logs_prints_entries_returned_by_server() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/logs"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [
                {
                    "id": 1,
                    "tenant_id": "t_test",
                    "deployment_id": "d_a",
                    "app_name": "myapp",
                    "worker_id": "w_us-east-1_h01",
                    "region": "us-east-1",
                    "level": "info",
                    "message": "hello",
                    "labels": {},
                    "ts": "2026-06-24T12:00:00Z",
                },
                {
                    "id": 2,
                    "tenant_id": "t_test",
                    "deployment_id": "d_a",
                    "app_name": "myapp",
                    "worker_id": "w_us-east-1_h01",
                    "region": "us-east-1",
                    "level": "warn",
                    "message": "rate limit approaching",
                    "labels": {"request_id": "req_42"},
                    "ts": "2026-06-24T12:00:01Z",
                },
            ],
            "limit": 100,
            "since": "2026-06-24T11:55:00Z",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("logs");
    cmd.arg("myapp");

    // In non-TTY mode (assert_cmd captures stdout) the CLI prints
    // one JSON object per line, so both messages must appear.
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("hello"))
        .stdout(predicate::str::contains("rate limit approaching"));
}

// ---------------------------------------------------------------------------
// JSON pipe mode: each line of stdout is a valid JSON object with
// all wire fields.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn logs_emits_one_json_object_per_line_in_pipe_mode() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/logs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [
                {
                    "id": 7,
                    "tenant_id": "t_test",
                    "deployment_id": "d_x",
                    "app_name": "myapp",
                    "worker_id": "w_h01",
                    "region": "us-east-1",
                    "level": "error",
                    "message": "boom",
                    "labels": {"trace_id": "abc"},
                    "ts": "2026-06-24T12:00:00Z",
                },
            ],
            "limit": 100,
            "since": "2026-06-24T11:55:00Z",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("logs");
    cmd.arg("myapp");

    let output = cmd.output().expect("run edge logs");
    assert!(output.status.success(), "exit code: {:?}", output.status);

    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    // assert_cmd captures stdout via a pipe, so is_terminal()
    // returns false and the CLI goes into JSON mode.
    let mut parsed = 0;
    for line in stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line {line:?} is not valid JSON: {e}"));
        // Pin the wire-shape contract: id, level, message, ts.
        for key in ["id", "level", "message", "ts", "deployment_id"] {
            assert!(v.get(key).is_some(), "json line missing {key}: {v}");
        }
        parsed += 1;
    }
    assert!(
        parsed >= 1,
        "expected at least one JSON entry, got stdout: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// 400 from server: invalid level → CLI must exit non-zero with a
// useful message (not a panic).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn logs_returns_error_on_400_from_server() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/logs"))
        .and(query_param("level", "critical"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": {"code": "BAD_REQUEST", "message": "invalid level: critical"},
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("logs");
    cmd.arg("myapp");
    cmd.arg("--level");
    cmd.arg("critical");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("400"));
}

// ---------------------------------------------------------------------------
// Auth header: the request must include the seeded API key as
// `Authorization: Bearer <key>`. A regression that dropped the
// header would be caught here.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn logs_sends_bearer_auth_header() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/logs"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [],
            "limit": 100,
            "since": "",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("logs");
    cmd.arg("myapp");

    cmd.assert().success();
}

// ---------------------------------------------------------------------------
// Crashed hint tests — issue #77 §5.
//
// The CLI fetches /api/v1/apps/{appName}/status before the first
// logs() call. If the worker reports `crashed` with a fresh
// heartbeat, the CLI prints a stderr hint pointing at
// `edge rollback <app>`. The hint must never fail the command:
// any error from the status endpoint is silently ignored.
// ---------------------------------------------------------------------------

/// Build an RFC3339 timestamp `now - offset_secs` (UTC, second
/// precision, trailing `Z`). Mirrors the CLI's own formatter so
/// the parse in `commands::logs::is_fresh` accepts the test value.
fn rfc3339_now_minus(offset_secs: u64) -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .saturating_sub(offset_secs);
    format_rfc3339_utc(secs)
}

fn format_rfc3339_utc(secs: u64) -> String {
    // Howard Hinnant's civil_from_days; copy of the helper in
    // commands::logs.rs so the integration tests can produce a
    // timestamp the CLI will parse. If the production helper
    // drifts, this copy must drift too — but the format is
    // simple enough (20 bytes, UTC, second precision) that a
    // divergence would surface as a parse error in `is_fresh`
    // and fail the test.
    let days = (secs / 86_400) as i64;
    let secs_of_day = (secs % 86_400) as u32;
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day % 3600) / 60;
    let ss = secs_of_day % 60;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let year = if m <= 2 { y + 1 } else { y };

    format!(
        "{year:04}-{month:02}-{day:02}T{h:02}:{mm:02}:{ss:02}Z",
        year = year,
        month = m,
        day = d,
        h = hh,
    )
}

#[tokio::test]
async fn logs_prints_crashed_hint_when_worker_status_is_crashed() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    // Status endpoint reports `crashed` with a heartbeat 30s ago —
    // well within the 5-minute freshness window.
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "app_name": "myapp",
            "status": "crashed",
            "last_heartbeat": rfc3339_now_minus(30),
            "region": "us-east-1",
            "worker_id": "w_us-east-1_h01",
            "exit_code": 137,
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/logs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [],
            "limit": 100,
            "since": "",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("logs");
    cmd.arg("myapp");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("crashed"))
        .stdout(predicate::str::contains("edge rollback myapp"));
}

#[tokio::test]
async fn logs_skips_hint_when_heartbeat_stale() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    // Heartbeat is 10 minutes old — beyond the 5-minute freshness
    // threshold. The CLI must NOT show the hint, because the
    // worker is dead and "crashed 10 minutes ago" is stale state,
    // not an active crash.
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "app_name": "myapp",
            "status": "crashed",
            "last_heartbeat": rfc3339_now_minus(10 * 60),
            "region": "us-east-1",
            "worker_id": "w_us-east-1_h01",
            "exit_code": 137,
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/logs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [],
            "limit": 100,
            "since": "",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("logs");
    cmd.arg("myapp");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("crashed").not());
}

#[tokio::test]
async fn logs_skips_hint_when_status_is_running() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "app_name": "myapp",
            "status": "running",
            "last_heartbeat": rfc3339_now_minus(5),
            "region": "us-east-1",
            "worker_id": "w_us-east-1_h01",
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/logs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [],
            "limit": 100,
            "since": "",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("logs");
    cmd.arg("myapp");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("crashed").not())
        .stdout(predicate::str::contains("rollback").not());
}

#[tokio::test]
async fn logs_silently_continues_when_status_endpoint_fails() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    // Status endpoint blows up. The CLI must NOT surface this to
    // the user — the hint is a courtesy, not a hard dependency.
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/status"))
        .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
            "error": {"code": "INTERNAL", "message": "boom"},
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp/logs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [],
            "limit": 100,
            "since": "",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("logs");
    cmd.arg("myapp");

    // The log query is the primary purpose; the command must
    // succeed even when the hint source is broken. No hint, no
    // panic, exit 0.
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("crashed").not())
        .stdout(predicate::str::contains("rollback").not());
}
