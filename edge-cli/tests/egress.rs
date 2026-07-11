//! Integration tests for the `edge egress` subcommand group.
//!
//! `edge egress set` is naturally idempotent (PUT-replaces; the
//! same final state replays). It uses hardcoded sensible defaults
//! — there's no `--max-retries` flag on the surface. Tests
//! override the backoff via the `EDGE_CLI_RETRY_BASE_MS` env var.
//!
//! `set_egress` PUTs to `/api/v1/egress` and then internally calls
//! `get_egress` to surface the stored allowlist back to the CLI.
//! The retry loop only retries the PUT — the trailing GET runs
//! once on success (and is NOT under the retry umbrella). The
//! happy-path test asserts both requests land; the retry test
//! asserts the PUT chain (503 + 200) plus exactly one GET.
//!
//! Issue #571 propagation.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

fn write_minimal_edge_toml(project: &TempDir) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "egress-test"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Issue #571 propagation: retry transient 5xx on egress set.
//
// `set_egress` is naturally idempotent (PUT-replaces). The retry
// loop uses hardcoded sensible defaults — there's no flag triple
// on the surface. The test shrinks the backoff via
// `EDGE_CLI_RETRY_BASE_MS=10`.
//
// Wiremock assertion: the PUT mock chain expects exactly 2 hits
// (503 + 200); the trailing GET mock expects exactly 1 hit (the
// re-fetch that runs once after a successful PUT).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn egress_set_retries_503_then_succeeds() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    Mock::given(method("PUT"))
        .and(path("/api/v1/egress"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .named("egress-set-503")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/egress"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200))
        .named("egress-set-200")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    // Trailing re-fetch — runs once after the PUT succeeds.
    Mock::given(method("GET"))
        .and(path("/api/v1/egress"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "allowlist": ["api.stripe.com"],
        })))
        .named("egress-get-refetch")
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
    cmd.arg("egress").arg("set").arg("api.stripe.com");

    cmd.assert().success();
    let received = server.received_requests().await.expect("received requests");
    let put_count = received
        .iter()
        .filter(|r| r.method.as_str() == "PUT" && r.url.path() == "/api/v1/egress")
        .count();
    let get_count = received
        .iter()
        .filter(|r| r.method.as_str() == "GET" && r.url.path() == "/api/v1/egress")
        .count();
    assert_eq!(
        put_count, 2,
        "expected 503 + 200 = 2 PUT attempts on /api/v1/egress"
    );
    assert_eq!(
        get_count, 1,
        "expected exactly one trailing GET re-fetch on /api/v1/egress"
    );
}

// ---------------------------------------------------------------------------
// Baseline happy-path: PUT 200 + GET 200 on `edge egress set`. Locks
// the wire shape end-to-end (PUT replaces + GET re-fetches). The
// retry test above proves the same shape under transient failure.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn egress_set_happy_path() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("PUT"))
        .and(path("/api/v1/egress"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/egress"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "allowlist": ["api.stripe.com", "*.sendgrid.net"],
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri());
    cmd.arg("egress")
        .arg("set")
        .arg("api.stripe.com")
        .arg("*.sendgrid.net");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("api.stripe.com"))
        .stdout(predicate::str::contains("*.sendgrid.net"));
}

/// Baseline happy-path for `edge egress show`. Mirrors the set
/// happy-path but for the read verb. The show path doesn't need a
/// trailing re-fetch — it's a single GET.
#[tokio::test]
async fn egress_show_happy_path() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/egress"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "allowlist": ["api.stripe.com"],
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri());
    cmd.arg("egress").arg("show");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("api.stripe.com"));
}
