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
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Issue #571 propagation: retry transient 5xx on env delete.
//
// `edge env-delete` is operator-tunable — main.rs wires
// `--max-retries` / `--retry-base-ms` / `--retry-cap-ms` to it. A
// single 503-then-200 sequence exercises the wire contract: the
// retried DELETE must hit the same path with the same Authorization
// header (so the retry loop's same-args contract is enforced by
// wiremock's exact-match matcher, not by code-level inspection).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn delete_retries_503_then_succeeds() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");

    Mock::given(method("DELETE"))
        .and(path("/api/v1/apps/myapp/env/LOG_LEVEL"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .named("env-delete-503")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/apps/myapp/env/LOG_LEVEL"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200))
        .named("env-delete-200")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.env("EDGE_CLI_RETRY_BASE_MS", "10");
    cmd.current_dir(project.path());
    cmd.args([
        "env-delete",
        "--app",
        "myapp",
        "LOG_LEVEL",
        "--retry-base-ms=10",
    ]);

    cmd.assert().success();
    let received = server.received_requests().await.expect("received requests");
    assert_eq!(
        received.len(),
        2,
        "expected 503 + 200 = 2 received requests"
    );
}

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

    let mut cmd = Command::cargo_bin("edge").unwrap();
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

    let mut cmd = Command::cargo_bin("edge").unwrap();
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

    let mut cmd = Command::cargo_bin("edge").unwrap();
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

// ---------------------------------------------------------------------------
// Positional <app> resolution (issue: deferred CLI gap from PR #457).
//
// Before the positional was added, `edge env {list,set,delete}` hard-wired
// `&state.app_name` and surfaced 'no deployment found — run `edge deploy`
// first' on a missing state.json. The new behavior:
//   - positional wins when present (works without state.json at all)
//   - state.json wins when positional is absent (legacy callers unaffected)
//   - error message names the failing command and references state.json
// ---------------------------------------------------------------------------

/// Pin the new behavior: `edge env-list other-app` works when
/// `.edge/state.json` is absent. Without the positional the
/// runner would have errored on `load_state_optional`.
#[tokio::test]
async fn list_resolves_app_from_positional_when_state_missing() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    // NOTE: no write_state_with_app — state.json is intentionally absent.
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/other-app/env"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"key": "OTHER_VAR", "value": "from-positional"},
        ])))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .args(["env-list", "--app", "other-app"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("OTHER_VAR = from-positional"));
}

/// Pin the regression case: state.json present + no positional =
/// state.json's app_name is used (identical to the old behavior).
/// If the refactor accidentally swapped precedence this test fails
/// because the mock matches `state-app`, not `myapp`.
#[tokio::test]
async fn list_resolves_app_from_state_when_positional_empty() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "state-app");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/state-app/env"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"key": "STATE_VAR", "value": "from-state"},
        ])))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).args(["env-list"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("STATE_VAR = from-state"));
}

/// Pin `edge env-set <app> KEY VAL` with no state.json. The wire
/// request lands on PUT /api/v1/apps/<app>/env with the key=value
/// payload. Mirrors the list test above; verifies that the same
/// resolution rule applies to the SET verb.
#[tokio::test]
async fn set_resolves_app_from_positional_when_state_missing() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    // no state.json — positional must carry the run.
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    // Use body_string for an exact-body match so a missing or
    // malformed payload surfaces as a wiremock mismatch.
    Mock::given(method("POST"))
        .and(path("/api/v1/apps/other-app/env"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(body_string(r#"{"key":"LOG_LEVEL","value":"debug"}"#))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).args([
        "env-set",
        "--app",
        "other-app",
        "LOG_LEVEL",
        "debug",
    ]);

    cmd.assert().success();
}

/// Pin `edge env-delete <app> KEY` with no state.json. Symmetric
/// to the SET test above; locks DELETE /api/v1/apps/<app>/env/KEY.
#[tokio::test]
async fn delete_resolves_app_from_positional_when_state_missing() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("DELETE"))
        .and(path("/api/v1/apps/other-app/env/LOG_LEVEL"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .args(["env-delete", "--app", "other-app", "LOG_LEVEL"]);

    cmd.assert().success();
}

/// Pre-flight path-component validation on `edge env-set --app <app>
/// <key> <value>` and `edge env-delete --app <app> <key>`. Both
/// interpolate `app_name` into the URL path; env-delete also
/// interpolates `key` into the path (`/apps/{app}/env/{key}`).
/// Issue #671.
#[tokio::test]
async fn env_set_and_delete_reject_invalid_app_or_key() {
    for (verb_args, expected_substr, verb) in [
        // env-set with bad app
        (
            vec!["env-set", "--app", "my../api", "FOO", "bar"],
            "'..'",
            "POST",
        ),
        (
            vec!["env-set", "--app", "my%2Fapi", "FOO", "bar"],
            "invalid character",
            "POST",
        ),
        // env-delete with bad app
        (
            vec!["env-delete", "--app", "my../api", "FOO"],
            "'..'",
            "DELETE",
        ),
        // env-delete with bad key
        (
            vec!["env-delete", "--app", "api", "FOO/../BAR"],
            "'..'",
            "DELETE",
        ),
        (
            vec!["env-delete", "--app", "api", "FOO%2FBAR"],
            "invalid character",
            "DELETE",
        ),
    ] {
        let home = common::isolated_home();
        let project = common::isolated_home();
        let server = MockServer::start().await;

        common::seed_api_key(&home, "k_seed");
        // edge.toml + state.json are required by `edge env-set` /
        // `edge env-delete` to resolve the [deployment] api URL.
        // We seed them so the path-component validator (which runs
        // AFTER the toml parse) actually gets to fire.
        write_minimal_edge_toml(&project);
        write_state_with_app(&project, "myapp");

        // Fence: NO POST/DELETE on env paths may ever land.
        Mock::given(method(verb))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&server)
            .await;

        let mut cmd = Command::cargo_bin("edge").unwrap();
        common::set_platform_env(&mut cmd, &home);
        cmd.env("EDGE_API_URL", server.uri());
        cmd.current_dir(project.path());
        cmd.args(&verb_args);

        common::assert_invalid_path_component(cmd, expected_substr);
    }
}
