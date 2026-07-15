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

// ---------------------------------------------------------------------------
// Issue #571 propagation: retry transient 5xx on domains remove.
//
// `edge domains remove` is naturally idempotent (DELETE-by-(app,
// fqdn); second call returns 404 with no side effect). The retry
// loop uses hardcoded sensible defaults — there's no `--max-retries`
// flag on the surface. The retry path is exercised end-to-end by
// mounting a 503-then-200 pair on the DELETE route and asserting
// both requests landed.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn remove_retries_503_then_succeeds() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    Mock::given(method("DELETE"))
        .and(path("/api/v1/apps/api/domains/api.example.com"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .named("domains-remove-503")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/apps/api/domains/api.example.com"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200))
        .named("domains-remove-200")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri());
    cmd.env("EDGE_CLI_RETRY_BASE_MS", "10");
    cmd.timeout(std::time::Duration::from_secs(15));
    cmd.arg("domains")
        .arg("remove")
        .arg("api")
        .arg("api.example.com");

    cmd.assert().success();
    let received = server.received_requests().await.expect("received requests");
    let delete_count = received
        .iter()
        .filter(|r| {
            r.method.as_str() == "DELETE"
                && r.url.path() == "/api/v1/apps/api/domains/api.example.com"
        })
        .count();
    assert_eq!(
        delete_count, 2,
        "expected 503 + 200 = 2 DELETE attempts on /api/v1/apps/api/domains/api.example.com"
    );
}

/// Pre-flight path-component validation: malformed `app_name` or
/// `fqdn` values must bail with an actionable error before any DELETE
/// round-trip lands. Sub-cases cover both identifiers, since
/// `DomainClient::remove` validates them separately. Issue #671.
#[tokio::test]
async fn domains_remove_rejects_invalid_app_or_fqdn() {
    for (app, fqdn, expected_substr) in [
        ("my../api", "api.example.com", "'..'"),
        ("api", "evil%2Ffqdn", "invalid character"),
        ("api", "", "cannot be empty"),
    ] {
        let home = common::isolated_home();
        let project = common::isolated_home();
        write_minimal_edge_toml(&project);
        let server = MockServer::start().await;

        common::seed_api_key(&home, "k_seed");

        // Fence: NO DELETE may ever land. The validator must fire
        // before any round-trip to /api/v1/apps/{}/domains/{fqdn}.
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&server)
            .await;

        let mut cmd = Command::cargo_bin("edge").unwrap();
        common::set_platform_env(&mut cmd, &home);
        cmd.current_dir(project.path());
        cmd.env("EDGE_API_URL", server.uri());
        cmd.arg("domains").arg("remove").arg(app).arg(fqdn);

        cmd.assert()
            .failure()
            .stderr(predicate::str::contains(expected_substr));
    }
}

/// Pre-flight path-component validation on `edge domains check <app>
/// <fqdn>`. Both `app_name` and `fqdn` interpolate into
/// `GET /api/v1/apps/{app}/domains/{fqdn}`. Issue #671.
#[tokio::test]
async fn domains_check_rejects_invalid_app_or_fqdn() {
    for (app, fqdn, expected_substr) in [
        ("my../api", "api.example.com", "'..'"),
        ("api", "evil%2Ffqdn", "invalid character"),
        ("api", "", "cannot be empty"),
    ] {
        let home = common::isolated_home();
        let project = common::isolated_home();
        write_minimal_edge_toml(&project);
        let server = MockServer::start().await;

        common::seed_api_key(&home, "k_seed");

        // Fence: NO GET on the domains path may ever land.
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let mut cmd = Command::cargo_bin("edge").unwrap();
        common::set_platform_env(&mut cmd, &home);
        cmd.current_dir(project.path());
        cmd.env("EDGE_API_URL", server.uri());
        cmd.arg("domains").arg("check").arg(app).arg(fqdn);

        common::assert_invalid_path_component(cmd, expected_substr);
    }
}

/// Pre-flight path-component validation on `edge domains add <app>
/// <fqdn>`. The `app_name` interpolates into the URL path; `fqdn`
/// currently goes in the body but we still validate it (defense in
/// depth). Issue #671.
#[tokio::test]
async fn domains_add_rejects_invalid_app_name() {
    for (bad_app, expected_substr) in [
        ("my../api", "'..'"),
        ("my%2Fapi", "invalid character"),
        ("", "cannot be empty"),
    ] {
        let home = common::isolated_home();
        let project = common::isolated_home();
        write_minimal_edge_toml(&project);
        let server = MockServer::start().await;

        common::seed_api_key(&home, "k_seed");

        // Fence: NO POST on the domains path may ever land.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(201))
            .expect(0)
            .mount(&server)
            .await;

        let mut cmd = Command::cargo_bin("edge").unwrap();
        common::set_platform_env(&mut cmd, &home);
        cmd.current_dir(project.path());
        cmd.env("EDGE_API_URL", server.uri());
        cmd.arg("domains")
            .arg("add")
            .arg(bad_app)
            .arg("api.example.com");

        common::assert_invalid_path_component(cmd, expected_substr);
    }
}

/// Pre-flight path-component validation on `edge domains list <app>`.
/// The `app_name` interpolates into
/// `GET /api/v1/apps/{app}/domains`. Issue #671.
#[tokio::test]
async fn domains_list_rejects_invalid_app_name() {
    for (bad_app, expected_substr) in [
        ("my../api", "'..'"),
        ("my%2Fapi", "invalid character"),
        ("", "cannot be empty"),
    ] {
        let home = common::isolated_home();
        let project = common::isolated_home();
        write_minimal_edge_toml(&project);
        let server = MockServer::start().await;

        common::seed_api_key(&home, "k_seed");

        // Fence: NO GET on the domains path may ever land.
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"domains": []})),
            )
            .expect(0)
            .mount(&server)
            .await;

        let mut cmd = Command::cargo_bin("edge").unwrap();
        common::set_platform_env(&mut cmd, &home);
        cmd.current_dir(project.path());
        cmd.env("EDGE_API_URL", server.uri());
        cmd.arg("domains").arg("list").arg(bad_app);

        common::assert_invalid_path_component(cmd, expected_substr);
    }
}
