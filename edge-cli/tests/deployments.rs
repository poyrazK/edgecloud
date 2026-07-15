//! Integration tests for `edge deployments`.
//!
//! The server returns a `{items, total, limit, next_cursor}` envelope
//! (see `deploymentListResponse` in
//! `edge-control-plane/internal/handler/deployment.go`). Post-#709
//! the wire shape is cursor-only — `?offset=` returns 400 and
//! `next_offset` is no longer emitted. `edge deployments` walks the
//! cursor chain to exhaustion (mirrors `edge apps` / `edge logs`).

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
            "limit": 20,
            "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
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
            "limit": 20,
            "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
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
/// array of objects, no top-level items/total/limit/next_cursor).
/// The server shape changed to a typed envelope in commit 1 of
/// the wire-mismatch fix; this test would fail if the CLI started
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

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("deployments");

    // Expect a non-zero exit: serde_json fails to deserialize the
    // bare array into DeploymentListEnvelope { items, total, limit,
    // next_cursor } and the CLI surfaces "invalid response body".
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("invalid response body"));
}

// ---------------------------------------------------------------------------
// Pagination tests (issue: deferred CLI gap from PR #457; hard-cut in #709).
// ---------------------------------------------------------------------------

/// Pin the default passthrough: with no `--limit` the CLI sends
/// `?limit=` to mean "server default" — the wire request omits the
/// query param entirely. wiremock's `path` matcher alone accepts
/// (and this test fails) any extra query params, so the absence of
/// a `query_param` matcher here is the assertion.
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
            "limit": 20,
            "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("deployments");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("d_only"));
}

/// Pin `--limit N` forwarding as `?limit=N` on the wire. Post-#709
/// the cursor walker is the only pager; `--limit` is still honored
/// as the per-page size.
#[tokio::test]
async fn forwards_limit_query_param() {
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
        // No `?offset=` matcher: post-#709 the wire shape is
        // cursor-only, and the CLI never emits `?offset=`. If a
        // future commit regresses and re-adds the offset param,
        // this mock stops matching and the test fails.
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [{
                "id": "d_first",
                "status": "deployed",
                "created_at": "2026-07-01T00:00:00Z",
                "url": "https://t_test-myapp.edgecloud.dev"
            }],
            "limit": 10,
            "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .args(["deployments", "--limit", "10"]);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("d_first"));
}

/// Pin the silent-single-page UX: when the walker terminates on
/// the first page (no `next_cursor`), the CLI renders the table
/// silently with no footer.
#[tokio::test]
async fn silent_when_single_page() {
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
            "limit": 20,
            "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("deployments");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("d_only"))
        // Post-#709 there's no "page X of N" footer at all; the
        // total-count header ("N deployments") replaces it.
        .stdout(predicate::str::contains("page ").not())
        .stdout(predicate::str::contains("next:").not())
        .stdout(predicate::str::contains("prev:").not());
}

/// Pin that a single-deployment result renders the singular
/// "1 deployment" header (no trailing 's').
#[tokio::test]
async fn renders_total_header_singular() {
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
                "id": "d_first",
                "status": "deployed",
                "created_at": "2026-07-01T00:00:00Z",
                "url": "https://t_test-myapp.edgecloud.dev"
            }],
            "limit": 20,
            "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("deployments");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("1 deployment\n"))
        // Pluralization: must NOT render "1 deployments".
        .stdout(predicate::str::contains("1 deployments").not());
}

/// Pin that the cursor walker follows `next_cursor` through
/// multiple pages. The mock returns page 1 with a cursor, then
/// page 2 with no cursor — the walker should make exactly 2
/// requests and concatenate the items.
#[tokio::test]
async fn list_walks_cursor_chain() {
    use wiremock::matchers::query_param_is_missing;

    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    write_state_with_app(&project, "myapp");
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    // First page: returns 1 item + cursor for page 2. The
    // `query_param_is_missing("cursor")` matcher pins that the
    // initial request carries no `?cursor=` (the walker only
    // sends a cursor on the SECOND-and-later pages).
    Mock::given(method("GET"))
        .and(path("/api/v1/list/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(query_param_is_missing("cursor"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [{
                "id": "d_first",
                "status": "deployed",
                "created_at": "2026-07-01T00:00:00Z",
                "url": "https://t_test-myapp.edgecloud.dev"
            }],
            "limit": 20,
            "next_cursor": "page2cursor"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Second page: returns the second item, no further cursor.
    Mock::given(method("GET"))
        .and(path("/api/v1/list/myapp"))
        .and(query_param("cursor", "page2cursor"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "items": [{
                "id": "d_second",
                "status": "active",
                "created_at": "2026-07-02T00:00:00Z",
                "url": "https://t_test-myapp.edgecloud.dev"
            }],
            "limit": 20,
            "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("deployments");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("d_first"))
        .stdout(predicate::str::contains("d_second"))
        // Cursor value must not leak into stdout — the CLI treats
        // it as opaque and the user gets only the items.
        .stdout(predicate::str::contains("page2cursor").not());
}
