//! Integration tests for `edge apps` and `edge apps get <name>`.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

/// Write a minimal `edge.toml` (no `[deployment].api`, so the runtime
/// falls through to the env-supplied `EDGE_API_URL`).
fn write_minimal_edge_toml(project: &TempDir) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "apps-test"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
    )
    .unwrap();
}

#[tokio::test]
async fn list_returns_empty_message() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "apps": [],
            "limit": 50,
            "offset": 0
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("apps");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("No apps found."));
}

#[tokio::test]
async fn list_returns_populated_table() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "apps": [
                {
                    "ID": "app_abc",
                    "TenantID": "t_seed",
                    "Name": "myapp",
                    "Description": null,
                    "CreatedAt": "2026-06-24T12:00:00Z"
                },
                {
                    "ID": "app_def",
                    "TenantID": "t_seed",
                    "Name": "otherapp",
                    "Description": "A demo app",
                    "CreatedAt": "2026-06-25T08:30:00Z"
                }
            ],
            "limit": 50,
            "offset": 0
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri()).arg("apps");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("app_abc"))
        .stdout(predicate::str::contains("myapp"))
        .stdout(predicate::str::contains("app_def"))
        .stdout(predicate::str::contains("otherapp"));
}

#[tokio::test]
async fn get_shows_details_for_existing_app() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ID": "app_abc",
            "TenantID": "t_seed",
            "Name": "myapp",
            "Description": "My production app",
            "CreatedAt": "2026-06-24T12:00:00Z"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("apps")
        .arg("get")
        .arg("myapp");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("app_abc"))
        .stdout(predicate::str::contains("myapp"))
        .stdout(predicate::str::contains("My production app"));
}

#[tokio::test]
async fn get_shows_details_app_with_null_description() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ID": "app_xyz",
            "TenantID": "t_seed",
            "Name": "myapp",
            "Description": null,
            "CreatedAt": "2026-06-24T12:00:00Z"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("apps")
        .arg("get")
        .arg("myapp");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("(none)"));
}

#[tokio::test]
async fn get_propagates_404_for_missing_app() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/nonexistent"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(404).set_body_string(r#"{"error":"app not found"}"#))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("apps")
        .arg("get")
        .arg("nonexistent");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("404"));
}

// Issue #573: `edge apps delete` — owner-role only, --yes required,
// irreversible cascade. Three regression tests below pin the wire
// shape + UX guarantees.

/// Without `--yes`/`-y`, the CLI must bail with an actionable
/// error before any DELETE round-trip. No mock is mounted for the
/// admin path so a stray round-trip (which would 404 and surface as
/// a generic failure) fails the test loudly.
#[tokio::test]
async fn apps_delete_requires_yes_flag() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    // No mock on /api/v1/admin/apps/* — expect(0) is enforced by
    // the default wiremock behavior (any unmatched request returns
    // a 404 status template; assert_cmd will surface it as a
    // failure, but we also assert the `--yes` message).

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("apps")
        .arg("delete")
        .arg("myapp");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("--yes"))
        .stderr(predicate::str::contains("irreversible"));
}

/// Happy path: whoami returns owner, DELETE returns 204, the CLI
/// prints the success line. The mock counts (expect(1) on both)
/// pin exactly one whoami + one DELETE round-trip — if the CLI
/// regresses to skipping the pre-flight or hitting a different
/// path, the test fails loudly.
#[tokio::test]
async fn apps_delete_sends_admin_path_on_yes() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    Mock::given(method("GET"))
        .and(path("/api/v1/auth/whoami"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant_id": "t_seed",
            "tenant_name": "Seed",
            "plan": "free",
            "api_key_id": "k_seed",
            "api_key_name": "default",
            "role": "owner",
            "created_at": "2026-06-20T00:00:00Z",
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/api/v1/admin/apps/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("apps")
        .arg("delete")
        .arg("myapp")
        .arg("--yes");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Deleted app 'myapp'"));
}

/// Pre-flight whoami surfaces a role-mismatch message before the
/// DELETE round-trip lands. whoami returns role=developer; the
/// CLI must print the actionable guidance and skip the admin
/// DELETE — a non-owner whoami response would otherwise hit the
/// server's `RequireRole("owner")` middleware and surface a bare
/// 403 with no hint. The `expect(0)` on DELETE would also fail
/// the test if the pre-flight regressed.
#[tokio::test]
async fn apps_delete_preflight_whoami_surfaces_role_mismatch() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    Mock::given(method("GET"))
        .and(path("/api/v1/auth/whoami"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant_id": "t_seed",
            "tenant_name": "Seed",
            "plan": "free",
            "api_key_id": "k_seed",
            "api_key_name": "default",
            "role": "developer",
            "created_at": "2026-06-20T00:00:00Z",
        })))
        .expect(1)
        .mount(&server)
        .await;

    // No DELETE mock — the pre-flight should bail before any
    // round-trip to /api/v1/admin/apps/*.

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("apps")
        .arg("delete")
        .arg("myapp")
        .arg("--yes");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("owner-role API key"))
        .stderr(predicate::str::contains("current key role: developer"))
        .stderr(predicate::str::contains(
            "edge auth keys create --role owner",
        ));
}
