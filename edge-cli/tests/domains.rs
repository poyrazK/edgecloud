//! Integration tests for the `edge domains` subcommand group.
//!
//! Each test drives the `edge-cli` binary against a wiremock control
//! plane, an isolated tempdir `HOME`, and a separate tempdir as the
//! CLI's `current_dir` (where it reads `edge.toml`).
//!
//! `list_decodes_wrapped_response_*` is the regression for the
//! pre-merge finding that `DomainClient::list()` was deserializing
//! the response body as `Vec<Domain>` while the handler emits
//! `{"domains": [...]}` — without this fix every `edge domains list`
//! fails with "missing field `domains`".

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
name = "domains-test"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
    )
    .unwrap();
}

/// Drive `edge domains list <app>` against a wiremock that returns a
/// wrapped `{"domains": [...]}` body. The pinned contract:
///
/// 1. Empty array → exit 0, prints "No custom domains for {app}."
/// 2. Populated array → exit 0, prints the header + every row's
///    id/status/fqdn/created_at columns.
#[tokio::test]
async fn list_decodes_wrapped_response_empty() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/api/domains"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "domains": []
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("domains")
        .arg("list")
        .arg("api");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("No custom domains for api"));
}

#[tokio::test]
async fn list_decodes_wrapped_response_populated() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/api/domains"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "domains": [
                {
                    "id": "dom_abc",
                    "tenant_id": "t_seed",
                    "app_name": "api",
                    "fqdn": "api.example.com",
                    "status": "pending",
                    "last_error": null,
                    "created_at": "2026-06-24T12:00:00Z",
                    "verified_at": null,
                },
                {
                    "id": "dom_def",
                    "tenant_id": "t_seed",
                    "app_name": "api",
                    "fqdn": "v2.example.com",
                    "status": "active",
                    "last_error": null,
                    "created_at": "2026-06-24T13:00:00Z",
                    "verified_at": "2026-06-24T13:05:00Z",
                },
            ]
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("domains")
        .arg("list")
        .arg("api");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("dom_abc"))
        .stdout(predicate::str::contains("api.example.com"))
        .stdout(predicate::str::contains("dom_def"))
        .stdout(predicate::str::contains("v2.example.com"));
}

/// A 404 from the control plane should bubble up as a non-zero exit
/// (the CLI surfaces the server's error via anyhow's chain). Pin the
/// wire-format of `check_response` for this route too — a regression
/// here means a "missing field" decoding error masks the real 404.
#[tokio::test]
async fn list_propagates_404_for_missing_app() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/apps/nope/domains"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(404).set_body_string(r#"{"error":"app not found"}"#))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("domains")
        .arg("list")
        .arg("nope");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("404"));
}
