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
            "next_cursor": null
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
            "next_cursor": null
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
// irreversible cascade. Six regression tests below pin the wire
// shape + UX guarantees (issue ACs: --yes path + confirmation +
// 404 custom message + 401 auth guidance).

/// Without `--yes`/`-y` and without a TTY, the CLI must bail with
/// an actionable error before any DELETE round-trip. The
/// `expect(0)` mock on the admin DELETE acts as a fence: a
/// regression that bypasses the gate and fires anyway would
/// otherwise be absorbed by wiremock's default 404 fallback and
/// surface as a generic failure. Note: `assert_cmd` pipes stderr,
/// so `is_terminal()` is always false here — this test exercises
/// the non-TTY bail arm, not the TTY prompt arm (the TTY path is
/// covered by `keys_revoke`'s equivalent test, since both share
/// the same `output::confirm` helper).
#[tokio::test]
async fn apps_delete_requires_yes_flag() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    // Assert no DELETE round-trip lands: a regression that bypasses
    // the --yes gate and fires anyway would otherwise be absorbed by
    // wiremock's default 404 fallback. The expect(0) mock fails the
    // test if any DELETE reaches the server. Mirrors the
    // `expect(0)`-as-fence pattern used at tests/auth.rs:714-718 and
    // tests/webhooks.rs:327-330.
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("apps")
        .arg("delete")
        .arg("myapp");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("pass --yes"))
        .stderr(predicate::str::contains("non-interactive shells"));
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

/// Retry path: a 503 on the first DELETE is retried, the second
/// attempt returns 204, and the CLI prints the success line. Pins
/// that `call_with_retry_no_interrupt` is wired on `apps delete` —
/// if a future refactor accidentally drops the wrapper, this test
/// fails loudly. Mirrors `webhooks.rs::remove_retries_503_then_succeeds`.
#[tokio::test]
async fn apps_delete_retries_503_then_succeeds() {
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

    // First DELETE → 503 (forces one retry).
    Mock::given(method("DELETE"))
        .and(path("/api/v1/admin/apps/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .named("apps-delete-503")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    // Second DELETE → 204.
    Mock::given(method("DELETE"))
        .and(path("/api/v1/admin/apps/myapp"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(204))
        .named("apps-delete-204")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri());
    // EDGE_CLI_RETRY_BASE_MS is a test-only hook (see `commands/retry.rs`)
    // that shrinks the default 500ms backoff below the 15s test timeout.
    cmd.env("EDGE_CLI_RETRY_BASE_MS", "10");
    cmd.timeout(std::time::Duration::from_secs(15));
    cmd.arg("apps").arg("delete").arg("myapp").arg("--yes");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Deleted app 'myapp'"));

    let received = server.received_requests().await.expect("received");
    let delete_count = received
        .iter()
        .filter(|r| r.method.as_str() == "DELETE" && r.url.path() == "/api/v1/admin/apps/myapp")
        .count();
    assert_eq!(
        delete_count, 2,
        "expected 503 + 204 = 2 DELETE attempts on /api/v1/admin/apps/myapp"
    );
}

/// Server-side 404 (app already deleted) must surface as a
/// non-zero exit with the actionable "app not found" message,
/// not the generic `rejected by server: 404 ...` from
/// `ApiError::Display`. Issue #573 AC: "404 → 'app not found'".
#[tokio::test]
async fn apps_delete_propagates_404_for_missing_app() {
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
        .and(path("/api/v1/admin/apps/nonexistent"))
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
        .arg("delete")
        .arg("nonexistent")
        .arg("--yes");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("'nonexistent' not found"))
        .stderr(predicate::str::contains("404"))
        .stderr(predicate::str::contains("edge apps"));
}

/// Server-side 401 (expired/invalid bearer) on the DELETE call
/// surfaces dedicated auth-guidance rather than the bare
/// `rejected by server: 401 ...`. Issue #573 AC: "401 → auth
/// guidance". whoami succeeds with role=owner (the bearer is
/// fine for that scope but the DELETE rejects it — could happen
/// if a key was downgraded server-side between calls).
#[tokio::test]
async fn apps_delete_401_surfaces_auth_guidance() {
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
        .respond_with(ResponseTemplate::new(401).set_body_string(r#"{"error":"unauthorized"}"#))
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
        .failure()
        .stderr(predicate::str::contains("authentication failed"))
        .stderr(predicate::str::contains("401"))
        .stderr(predicate::str::contains("edge auth login"));
}

// list_walks_cursor_chain pins the page-walk loop added for
// issue #58. The fixture issues three sequential GETs against
// `/api/v1/apps`:
//
//   1. limit=0 (server default) returns 2 apps + next_cursor="c1"
//   2. limit=0 cursor=c1 returns 2 apps + next_cursor="c2"
//   3. limit=0 cursor=c2 returns 1 app + next_cursor=null
//
// The CLI's `list_all_apps` walker must chain all three pages
// and print all five rows. The mocks assert the expected
// `expect(N)` per page so an accidental single-page shortcut
// (e.g. dropping the loop or short-circuiting on first response)
// trips the wiremock `Verify` failure.
#[tokio::test]
async fn list_walks_cursor_chain() {
    use wiremock::matchers::query_param;

    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    // Page 1 — first request, no cursor.
    Mock::given(method("GET"))
        .and(path("/api/v1/apps"))
        .and(query_param("limit", "0"))
        .and(wiremock::matchers::query_param_is_missing("cursor"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "apps": [
                {"ID": "app_1", "TenantID": "t_seed", "Name": "alpha", "Description": null, "CreatedAt": "2026-06-24T12:00:00Z"},
                {"ID": "app_2", "TenantID": "t_seed", "Name": "bravo", "Description": null, "CreatedAt": "2026-06-24T12:01:00Z"}
            ],
            "limit": 0,
            "next_cursor": "c1"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Page 2 — cursor=c1.
    Mock::given(method("GET"))
        .and(path("/api/v1/apps"))
        .and(query_param("limit", "0"))
        .and(query_param("cursor", "c1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "apps": [
                {"ID": "app_3", "TenantID": "t_seed", "Name": "charlie", "Description": null, "CreatedAt": "2026-06-24T12:02:00Z"},
                {"ID": "app_4", "TenantID": "t_seed", "Name": "delta", "Description": null, "CreatedAt": "2026-06-24T12:03:00Z"}
            ],
            "limit": 0,
            "next_cursor": "c2"
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Page 3 — cursor=c2; next_cursor=null terminates the walk.
    Mock::given(method("GET"))
        .and(path("/api/v1/apps"))
        .and(query_param("limit", "0"))
        .and(query_param("cursor", "c2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "apps": [
                {"ID": "app_5", "TenantID": "t_seed", "Name": "echo", "Description": null, "CreatedAt": "2026-06-24T12:04:00Z"}
            ],
            "limit": 0,
            "next_cursor": null
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
        .stdout(predicate::str::contains("alpha"))
        .stdout(predicate::str::contains("bravo"))
        .stdout(predicate::str::contains("charlie"))
        .stdout(predicate::str::contains("delta"))
        .stdout(predicate::str::contains("echo"));
}
