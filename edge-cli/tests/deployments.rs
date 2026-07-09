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
use wiremock::matchers::{header, method, path, query_param};
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
    // bare array into DeploymentListEnvelope { items, total, limit,
    // offset } and the CLI surfaces "invalid response body".
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("invalid response body"));
}

// ---------------------------------------------------------------------------
// Pagination tests (issue: deferred CLI gap from PR #457).
// ---------------------------------------------------------------------------

/// Pin that `--page N --limit N` both forward onto the wire as
/// `?limit=` and `?offset=` query params. offset = (page-1) * limit,
/// so `--page 2 --limit 10` -> `?limit=10&offset=10`. If either
/// query param goes missing the route work is broken; future
/// renames will surface as a test failure here.
#[tokio::test]
async fn forwards_limit_and_offset_query_params() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/list/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(query_param("limit", "10"))
        .and(query_param("offset", "10"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [{
                "id": "d_eighth",
                "status": "active",
                "created_at": "2026-07-09T00:00:00Z",
                "url": "https://t_test-myapp.edgecloud.dev"
            }],
            "total": 30,
            "limit": 10,
            "offset": 10
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .args(["deployments", "--page", "2", "--limit", "10"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("d_eighth"));
}

/// Pin the default passthrough: with neither `--page` nor `--limit`
/// the CLI does NOT add `?limit=` or `?offset=` to the wire
/// request. wiremock's `path` matcher alone accepts (and this
/// test fails) any extra query params, so the absence of a
/// `query_param` matcher here is the assertion.
#[tokio::test]
async fn omits_query_params_when_no_flags() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/list/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        // No `query_param` matcher: wiremock will fail on any
        // ?k=v the CLI emits when no flag is set. If a future
        // commit adds a default `?limit=`, this mock stops
        // matching and the test fails.
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [{
                "id": "d_only",
                "status": "deployed",
                "created_at": "2026-07-01T00:00:00Z",
                "url": "https://t_test-myapp.edgecloud.dev"
            }],
            "total": 1,
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
        .stdout(predicate::str::contains("d_only"));
}

/// Pin that the page-of-N footer renders when `total > limit`.
/// total=30 + limit=10 -> page 1 of 3, with a `next:` hint.
#[tokio::test]
async fn renders_footer_when_total_exceeds_limit() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/list/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(query_param("limit", "10"))
        // No `offset=` matcher: this CLI call uses
        // `--limit=10` with no `--page` (page defaults to 1,
        // so offset=(1-1)*10=0 and the wire request omits
        // `?offset=`). An `offset=0` matcher would be passed
        // here but is intentionally absent because the
        // pagination contract is "omit zero-valued params".
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [{
                "id": "d_first",
                "status": "deployed",
                "created_at": "2026-07-01T00:00:00Z",
                "url": "https://t_test-myapp.edgecloud.dev"
            }],
            "total": 30,
            "limit": 10,
            "offset": 0
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .args(["deployments", "--limit", "10"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("d_first"))
        .stdout(predicate::str::contains("page 1 of 3"))
        .stdout(predicate::str::contains("30 deployments"))
        .stdout(predicate::str::contains("next: --page 2"))
        // No `prev:` on the first page.
        .stdout(predicate::str::contains("prev:").not());
}

/// Pin the silent-first-page UX: when `total <= limit` the
/// footer does not render at all. Searching for the literal
/// "page " in the output catches anything that mentions
/// "page-of-N" but does not match the table itself (the
/// table prints deployment IDs, not the word "page").
#[tokio::test]
async fn silent_when_total_within_one_page() {
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
            "items": [{
                "id": "d_only",
                "status": "deployed",
                "created_at": "2026-07-01T00:00:00Z",
                "url": "https://t_test-myapp.edgecloud.dev"
            }],
            "total": 1,
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
        .stdout(predicate::str::contains("d_only"))
        // The footer text starts with "page " (with a space).
        // Searching the bare word "page" would false-positive
        // against the docs/help text in some shells; the
        // trailing space pins the footer specifically.
        .stdout(predicate::str::contains("page ").not())
        .stdout(predicate::str::contains("next:").not())
        .stdout(predicate::str::contains("prev:").not());
}

/// Pin the explicit `--page 0` rejection. clap accepts
/// `--page 0` (the field type is u32), and a silent
/// `--page 0 -> page 1` fallback would be a footgun;
/// instead the CLI exits non-zero with a clear error
/// before any wire request is made.
#[tokio::test]
async fn rejects_zero_page_with_exit_one() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    // No MockServer mount: any wire request would error with
    // a connection-refused failure, distinct from the
    // --page=0 bail we want to pin.

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    // Point at the mock so the test stays portable even if a
    // future commit moves the validation to AFTER the wire
    // request — the mock returns 404 for any unmounted path,
    // which would still be a connection-style error rather
    // than our "--page must be >= 1" validation error.
    cmd.env("EDGE_API_URL", server.uri())
        .args(["deployments", "--page", "0"]);

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("--page must be >= 1"));
}
