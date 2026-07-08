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

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
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

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
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

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
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

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
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

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
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
