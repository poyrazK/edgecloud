//! Integration tests for the `edge webhooks` subcommand group
//! (issue #565).
//!
//! Each test drives the `edge-cli` binary against a wiremock control
//! plane, an isolated tempdir `HOME`, and a separate tempdir as the
//! CLI's `current_dir` (where it reads `edge.toml`).
//!
//! `list_decodes_wrapped_response_*` is the regression for the same
//! envelope-decoding footgun that bit `domains`: the handler returns
//! `{"webhooks": [...]}` and `WebhookClient::list()` deserializes
//! through a `WebhookListResponse` wrapper. Without the wrapper, every
//! `edge webhooks list` would fail with "missing field `webhooks`".
//!
//! `add_sends_secret_in_body` pins the wire shape of the POST body so
//! a future refactor that drops the `secret` field (or moves it to a
//! header) fails CI. `clap_rejects_unknown_event_at_parse_time` pins
//! the clap-level rejection — exit 2 with no wiremock round-trip —
//! so the unit-tested `validate_events` helper actually reaches the
//! binary boundary.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

/// Write a minimal `edge.toml` (no `[deployment].api`, so the runtime
/// falls through to the env-supplied `EDGE_API_URL`).
fn write_minimal_edge_toml(project: &TempDir) {
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "webhooks-test"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_decodes_wrapped_response_empty() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "webhooks": []
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("list");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("No webhook subscriptions"));
}

#[tokio::test]
async fn list_decodes_wrapped_response_populated() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "webhooks": [
                {
                    "id": "wh_alpha",
                    "tenant_id": "t_seed",
                    "url": "https://hooks.example.com/deploys",
                    "events": ["deploy", "activate"],
                    "description": "primary",
                    "enabled": true,
                    "created_at": "2026-07-12T10:00:00Z"
                },
                {
                    "id": "wh_beta",
                    "tenant_id": "t_seed",
                    "url": "https://other.example.com/in",
                    "events": ["rollback"],
                    "description": "",
                    "enabled": false,
                    "created_at": "2026-07-12T11:00:00Z"
                }
            ]
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("list");

    cmd.assert()
        .success()
        // Header + both rows
        .stdout(predicate::str::contains("ID"))
        .stdout(predicate::str::contains("wh_alpha"))
        .stdout(predicate::str::contains("wh_beta"))
        // URL column
        .stdout(predicate::str::contains(
            "https://hooks.example.com/deploys",
        ))
        .stdout(predicate::str::contains("https://other.example.com/in"))
        // Status column derived from `enabled`
        .stdout(predicate::str::contains("ENABLED"))
        .stdout(predicate::str::contains("DISABLED"));
}

#[tokio::test]
async fn list_propagates_401_with_actionable_message() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_stale");
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks"))
        .and(header("Authorization", "Bearer k_stale"))
        .respond_with(
            ResponseTemplate::new(401).set_body_string(r#"{"error":"api key not recognized"}"#),
        )
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("list");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("401"));
}

// ---------------------------------------------------------------------------
// add — POST body shape + secret UX
// ---------------------------------------------------------------------------

/// Pin the wire shape of the POST body: the secret MUST be in the
/// JSON body (not a header), and the events list MUST be a JSON
/// array (not a string). A regression that drops the `secret` field
/// breaks the wire contract with the control plane; a regression
/// that switches events to a string breaks the JSON deserializer on
/// the server side.
#[tokio::test]
async fn add_sends_secret_and_events_in_body() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("POST"))
        .and(path("/api/v1/webhooks"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(header("content-type", "application/json"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "wh_new",
            "tenant_id": "t_seed",
            "url": "https://hooks.example.com/deploys",
            "events": ["deploy"],
            "description": "",
            "enabled": true,
            "created_at": "2026-07-12T10:00:00Z"
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("add")
        .arg("https://hooks.example.com/deploys")
        .arg("--events")
        .arg("deploy")
        .arg("--secret")
        .arg("sixteen-chars-min-1234");

    cmd.assert().success();
    let received = server.received_requests().await.expect("received");
    let post = received
        .iter()
        .find(|r| r.method.as_str() == "POST" && r.url.path() == "/api/v1/webhooks")
        .expect("POST /api/v1/webhooks not received");

    // Body is JSON; re-parse + assert shape.
    let body: serde_json::Value =
        serde_json::from_slice(&post.body).expect("POST body must be JSON");
    assert_eq!(
        body["url"].as_str(),
        Some("https://hooks.example.com/deploys"),
        "url field missing or wrong"
    );
    assert_eq!(
        body["secret"].as_str(),
        Some("sixteen-chars-min-1234"),
        "secret field missing or wrong"
    );
    assert_eq!(
        body["events"].as_array().map(|a| a.len()),
        Some(1),
        "events must be a JSON array"
    );
    assert_eq!(
        body["events"][0].as_str(),
        Some("deploy"),
        "events[0] must be the validated token"
    );
    assert!(
        body.get("description").is_some(),
        "description must be sent (even if empty)"
    );
}

/// Server echoes 201 but the response body is `{"webhooks": [...]}`-
/// style — wait, Create returns the bare row per
/// `edge-control-plane/internal/handler/webhook.go:71`. The CLI's
/// `WebhookClient::add` deserializes the response as `Webhook`
/// (not `WebhookListResponse`). This test exercises the
/// "response has no `webhooks` envelope" path so a future change
/// that wraps the Create response in `{"webhook": {...}}` (or
/// `{"webhooks": [{...}]}`) is caught here before reaching main.
#[tokio::test]
async fn add_deserializes_create_response_as_bare_row() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("POST"))
        .and(path("/api/v1/webhooks"))
        .and(header("content-type", "application/json"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "wh_new",
            "tenant_id": "t_seed",
            "url": "https://hooks.example.com/deploys",
            "events": ["deploy"],
            "description": "new",
            "enabled": true,
            "created_at": "2026-07-12T10:00:00Z"
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("add")
        .arg("https://hooks.example.com/deploys")
        .arg("--events")
        .arg("deploy")
        .arg("--description")
        .arg("new")
        .arg("--secret")
        .arg("sixteen-chars-min-1234");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Created webhook wh_new"))
        .stdout(predicate::str::contains("URL:"))
        // Secret-never-echoed: the success line must NOT contain the
        // raw secret value. A regression that prints the secret leaks
        // HMAC signing material to whatever captured stdout (CI logs,
        // terminal scrollback).
        .stdout(predicate::str::contains("sixteen-chars-min-1234").not())
        // The "store it now" reminder is a WARN-level message that
        // `output::warn` writes to stderr (so the success line stays
        // clean for `| jq`-style piping). The reminder lives on
        // stderr so a future `--json` mode could replace stdout
        // without overlapping the warning.
        .stderr(predicate::str::contains("store it now"));
}

/// Pin the clap-level rejection: `--events delete` should exit 2
/// WITHOUT touching the wiremock. This guards the unit-tested
/// `validate_events` helper against accidental removal from the
/// `From<WebhooksCommand>` impl — without it, an invalid event
/// would round-trip to the server and surface as a 400, which is
/// strictly worse UX than the current 1-message clap error.
#[tokio::test]
async fn clap_rejects_unknown_event_at_parse_time() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    // The mock below would fail the test if it were ever called —
    // wired up to confirm zero wire requests land.
    Mock::given(method("POST"))
        .and(path("/api/v1/webhooks"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0) // must NOT be called
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("add")
        .arg("https://hooks.example.com/x")
        .arg("--events")
        .arg("delete")
        .arg("--secret")
        .arg("sixteen-chars-min-1234");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("invalid event: delete"))
        .stderr(predicate::str::contains("valid: deploy, activate"));
}

/// Server-side validation failures (non-HTTPS url, short secret,
/// invalid event, etc.) should surface as a non-zero exit with
/// the server's body in stderr so the user can self-correct. The
/// CLI does not pre-validate URL scheme or secret length in
/// `WebhooksCommand` (those checks are server-side per
/// `internal/handler/webhook.go:147-170`), so the wire round-trip
/// is the only path for those error messages — pin the propagation
/// here.
#[tokio::test]
async fn add_propagates_400_from_server_validation() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("POST"))
        .and(path("/api/v1/webhooks"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(header("content-type", "application/json"))
        .respond_with(
            ResponseTemplate::new(400).set_body_string(r#"{"error":"url must use https scheme"}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    // http:// slips past clap (no scheme validator), through
    // acquire_secret (secret is valid length), and lands at the
    // server's `validateWebhookRequest` which 400s. Pin that the
    // body reaches the user's stderr.
    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("add")
        .arg("http://hooks.example.com/insecure")
        .arg("--events")
        .arg("deploy")
        .arg("--secret")
        .arg("sixteen-chars-min-1234");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("400"))
        .stderr(predicate::str::contains("https"));
}

// ---------------------------------------------------------------------------
// update — PUT shape + enabled flag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_changes_url_and_disables() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("PUT"))
        .and(path("/api/v1/webhooks/wh_alpha"))
        .and(header("Authorization", "Bearer k_seed"))
        .and(header("content-type", "application/json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "wh_alpha",
            "tenant_id": "t_seed",
            "url": "https://new.example.com/in",
            "events": ["deploy"],
            "description": "rotated",
            "enabled": false,
            "created_at": "2026-07-12T10:00:00Z"
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("update")
        .arg("wh_alpha")
        .arg("--url")
        .arg("https://new.example.com/in")
        .arg("--description")
        .arg("rotated")
        .arg("--disable");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Updated webhook wh_alpha"))
        .stdout(predicate::str::contains("https://new.example.com/in"))
        .stdout(predicate::str::contains("DISABLED"));

    // Pin the wire shape of the PUT body. The server's Update
    // handler (`internal/handler/webhook.go:101-115`) treats
    // absent fields as no-ops and present fields as overwrite —
    // a refactor that accidentally sends `"events": null` or
    // `"secret": null` would silently zero out the tenant's
    // existing values. This test fails CI the moment such a
    // regression lands.
    let received = server.received_requests().await.expect("received");
    let put = received
        .iter()
        .find(|r| r.method.as_str() == "PUT" && r.url.path() == "/api/v1/webhooks/wh_alpha")
        .expect("PUT /api/v1/webhooks/wh_alpha not received");
    let body: serde_json::Value = serde_json::from_slice(&put.body).expect("PUT body must be JSON");
    assert_eq!(
        body["url"].as_str(),
        Some("https://new.example.com/in"),
        "url must be the new value"
    );
    assert_eq!(
        body["description"].as_str(),
        Some("rotated"),
        "description must be the new value"
    );
    assert_eq!(
        body["enabled"].as_bool(),
        Some(false),
        "enabled must be sent as false (--disable), not omitted"
    );
    assert!(
        body.get("events").is_none(),
        "events must be ABSENT (server treats absent as no-op; \
         sending null/[] would overwrite the existing list)"
    );
    assert!(
        body.get("secret").is_none(),
        "secret must be ABSENT (sending null/'' would overwrite \
         the existing HMAC signing key)"
    );
}

// ---------------------------------------------------------------------------
// remove — retry path (idempotent DELETE, 503 then 204)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn remove_retries_503_then_succeeds() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    Mock::given(method("DELETE"))
        .and(path("/api/v1/webhooks/wh_alpha"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .named("webhooks-remove-503")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/api/v1/webhooks/wh_alpha"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(204))
        .named("webhooks-remove-204")
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
    cmd.arg("webhooks").arg("remove").arg("wh_alpha");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Removed webhook wh_alpha"));
    let received = server.received_requests().await.expect("received");
    let delete_count = received
        .iter()
        .filter(|r| r.method.as_str() == "DELETE" && r.url.path() == "/api/v1/webhooks/wh_alpha")
        .count();
    assert_eq!(
        delete_count, 2,
        "expected 503 + 204 = 2 DELETE attempts on /api/v1/webhooks/wh_alpha"
    );
}

// ---------------------------------------------------------------------------
// deliveries — GET shape + cursor pagination (issue #565 follow-up)
// ---------------------------------------------------------------------------
//
// The server-side route (`GET /api/v1/webhooks/{id}/deliveries`)
// is proposed in issue #659. These tests pin the wire shape the
// CLI expects; if #659's actual server implementation drifts from
// the proposed envelope (`{"deliveries": [...], "next_cursor":
// ...}`), the deserialization or table renderer will fail here
// before the divergence reaches a real tenant.

/// Happy path: a single page with two delivery rows + a null
/// `next_cursor`. Pins the wire envelope AND the table
/// rendering (column headers, status string passthrough,
/// `--limit` query parameter).
#[tokio::test]
async fn deliveries_decodes_envelope_and_renders_table() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v1/webhooks/wh_alpha/deliveries"))
        .and(query_param("limit", "50"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deliveries": [
                {
                    "id": 1,
                    "webhook_id": "wh_alpha",
                    "event_type": "deploy",
                    "status": "delivered",
                    "status_code": 200,
                    "error_msg": "",
                    "attempt": 1,
                    "max_attempts": 3,
                    "created_at": "2026-07-12T10:00:00Z",
                    "completed_at": "2026-07-12T10:00:01Z"
                },
                {
                    "id": 2,
                    "webhook_id": "wh_alpha",
                    "event_type": "activate",
                    "status": "failed",
                    "status_code": 502,
                    "error_msg": "upstream returned 502",
                    "attempt": 3,
                    "max_attempts": 3,
                    "created_at": "2026-07-12T11:00:00Z",
                    "completed_at": "2026-07-12T11:00:05Z"
                }
            ],
            "next_cursor": null
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("deliveries")
        .arg("wh_alpha");

    cmd.assert()
        .success()
        // Header line + divider
        .stdout(predicate::str::contains("ATTEMPT"))
        .stdout(predicate::str::contains("EVENT"))
        .stdout(predicate::str::contains("STATUS"))
        // Two rows
        .stdout(predicate::str::contains("deploy"))
        .stdout(predicate::str::contains("activate"))
        // Status passthrough — both server-defined values must
        // appear unmodified so a server rename (pending →
        // in_flight, etc.) fails CI here.
        .stdout(predicate::str::contains("delivered"))
        .stdout(predicate::str::contains("failed"))
        // HTTP status code column — 200 for the success row, 502
        // for the failure row.
        .stdout(predicate::str::contains("200"))
        .stdout(predicate::str::contains("502"))
        // No "Next page" hint when next_cursor is null.
        .stdout(predicate::str::contains("Next page").not());
}

/// Pagination: server returns a `next_cursor` and the CLI must
/// print the re-run hint with the cursor verbatim. Pin the
/// opaque-passthrough contract — the CLI must never inspect or
/// reformat the cursor token.
#[tokio::test]
async fn deliveries_emits_next_cursor_hint_when_more_pages() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v1/webhooks/wh_alpha/deliveries"))
        .and(query_param("limit", "2"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deliveries": [
                {
                    "id": 1,
                    "webhook_id": "wh_alpha",
                    "event_type": "deploy",
                    "status": "delivered",
                    "status_code": 200,
                    "error_msg": "",
                    "attempt": 1,
                    "max_attempts": 3,
                    "created_at": "2026-07-12T10:00:00Z",
                    "completed_at": "2026-07-12T10:00:01Z"
                }
            ],
            "next_cursor": "opaque-cursor-token-xyz"
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("deliveries")
        .arg("wh_alpha")
        .arg("--limit")
        .arg("2");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Next page:"))
        .stdout(predicate::str::contains("opaque-cursor-token-xyz"));
}

/// Empty set: server returns `{"deliveries": [], "next_cursor":
/// null}`. Pin the empty-state line so a refactor that drops
/// the period (or rephrases) fails the visual-consistency pin
/// (matches `webhooks list`'s "No webhook subscriptions." line).
#[tokio::test]
async fn deliveries_empty_set_prints_period_terminated_line() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v1/webhooks/wh_alpha/deliveries"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deliveries": [],
            "next_cursor": null
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("deliveries")
        .arg("wh_alpha");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("No delivery attempts recorded for webhook wh_alpha."));
}

/// 404: the webhook id doesn't exist OR belongs to a different
/// tenant. Server returns 404; CLI surfaces the error code so
/// the user can self-correct (typo'd id vs cross-tenant access
/// — both are 404 from the tenant's POV, server doesn't leak).
#[tokio::test]
async fn deliveries_propagates_404_from_server() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v1/webhooks/wh_does_not_exist/deliveries"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(
            ResponseTemplate::new(404).set_body_string(r#"{"error":"webhook not found"}"#),
        )
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("deliveries")
        .arg("wh_does_not_exist");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("404"))
        .stderr(predicate::str::contains("webhook not found"));
}

/// Pre-flight validation: `--limit 0` and `--limit 99999` should
/// fail at the command layer (before the wire round-trip) with
/// the server-clamp message. Pin so a future refactor that
/// forwards out-of-range limits to the server (relying on the
/// server's clamp) breaks CI here — the user-facing error
/// message is part of the CLI contract.
#[tokio::test]
async fn deliveries_rejects_limit_outside_server_clamp() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    // Mock below would fail the test if called — confirming the
    // bail fires before the round-trip.
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v1/webhooks/wh_alpha/deliveries"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    for bad_limit in ["0", "99999"] {
        let mut cmd = Command::cargo_bin("edge").unwrap();
        common::set_platform_env(&mut cmd, &home);
        cmd.current_dir(project.path());
        cmd.env("EDGE_API_URL", server.uri())
            .arg("webhooks")
            .arg("deliveries")
            .arg("wh_alpha")
            .arg("--limit")
            .arg(bad_limit);

        cmd.assert()
            .failure()
            .stderr(predicate::str::contains("between 1 and 200"));
    }
}

/// Cursor passthrough: when `--cursor X` is supplied, the CLI
/// must forward `X` verbatim as `?cursor=X`. Pin the URL shape
/// — a refactor that strips/encodes the cursor (or sends it as
/// a header by mistake) breaks here.
#[tokio::test]
async fn deliveries_forwards_cursor_query_param_verbatim() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v1/webhooks/wh_alpha/deliveries"))
        .and(query_param("limit", "50"))
        .and(query_param("cursor", "abc==/xyz+"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deliveries": [],
            "next_cursor": null
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("deliveries")
        .arg("wh_alpha")
        .arg("--cursor")
        .arg("abc==/xyz+");

    cmd.assert().success();
}
