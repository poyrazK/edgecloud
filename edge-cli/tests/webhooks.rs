//! Integration tests for the `edge webhooks` subcommand group
//! (issues #565, #659).
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
//!
//! `deliveries_*` tests cover issue #659's `edge webhooks deliveries`
//! subcommand. The envelope-decoding test pins the
//! `{deliveries, limit, next_cursor}` wire shape; the cursor/limit
//! forwarding tests pin the query-param contract; the pipe-mode test
//! pins the JSON-lines output for `jq`-style consumers.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path, query_param};
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
// deliveries (issue #659)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deliveries_decodes_envelope_with_next_cursor() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks/wh_alpha/deliveries"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deliveries": [
                {
                    "id": 12,
                    "webhook_id": "wh_alpha",
                    "event_type": "deploy",
                    "status": "success",
                    "status_code": 200,
                    "error_msg": "",
                    "attempt": 1,
                    "max_attempts": 3,
                    "created_at": "2026-07-14T12:00:00Z",
                    "completed_at": "2026-07-14T12:00:01Z"
                },
                {
                    "id": 11,
                    "webhook_id": "wh_alpha",
                    "event_type": "deploy",
                    "status": "failed",
                    "status_code": 503,
                    "error_msg": "upstream timeout",
                    "attempt": 3,
                    "max_attempts": 3,
                    "created_at": "2026-07-14T11:59:00Z",
                    "completed_at": "2026-07-14T11:59:01Z"
                }
            ],
            "limit": 50,
            "next_cursor": "eyJ2IjoxLCJ0cyI6IjIwMjYtMDctMTRUMTE6NTk6MDBaIiwiaWQiOjExfQ"
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

    // assert_cmd captures stdout, which the CLI treats as a non-TTY
    // (the same as piped output) — so the JSON-lines path fires.
    // We assert: (a) the two deliveries decode as JSON objects, and
    // (b) the next-page hint is emitted on stderr (output::hint
    // prints to stderr to keep stdout's JSON contract clean).
    let output = cmd.output().expect("output");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines().filter(|l| !l.is_empty());
    let first = lines.next().expect("at least one JSON line");
    let parsed: serde_json::Value =
        serde_json::from_str(first).unwrap_or_else(|e| panic!("invalid JSON line {first:?}: {e}"));
    assert_eq!(parsed["id"], 12);
    assert_eq!(parsed["webhook_id"], "wh_alpha");
    assert_eq!(parsed["status"], "success");

    let second = lines.next().expect("second JSON line");
    let parsed: serde_json::Value = serde_json::from_str(second)
        .unwrap_or_else(|e| panic!("invalid JSON line {second:?}: {e}"));
    assert_eq!(parsed["id"], 11);
    assert_eq!(parsed["status"], "failed");

    // next-page hint is appended to stdout after the JSON rows
    // (output::hint uses println). Pipe-mode consumers can grep for
    // the "More deliveries available" prefix to filter it out.
    assert!(
        stdout.contains("More deliveries available"),
        "expected next-page hint on stdout, got:\n{stdout}"
    );
    assert!(
        stdout.contains("eyJ2IjoxLCJ0cyI6IjIwMjYtMDctMTRUMTE6NTk6MDBaIiwiaWQiOjExfQ"),
        "hint must include the next_cursor verbatim:\n{stdout}"
    );
}

#[tokio::test]
async fn deliveries_emits_one_json_object_per_line_in_pipe_mode() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks/wh_alpha/deliveries"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deliveries": [
                {
                    "id": 1,
                    "webhook_id": "wh_alpha",
                    "event_type": "deploy",
                    "status": "success",
                    "status_code": 200,
                    "error_msg": "",
                    "attempt": 1,
                    "max_attempts": 3,
                    "created_at": "2026-07-14T12:00:00Z",
                    "completed_at": "2026-07-14T12:00:01Z"
                }
            ],
            "limit": 50,
            "next_cursor": null
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        // pipe redirect → stdout is not a TTY → JSON-lines mode.
        .arg("webhooks")
        .arg("deliveries")
        .arg("wh_alpha");

    let output = cmd.output().expect("output");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines().filter(|l| !l.is_empty());
    let first = lines.next().expect("at least one JSON line");
    let parsed: serde_json::Value =
        serde_json::from_str(first).unwrap_or_else(|e| panic!("invalid JSON line {first:?}: {e}"));
    assert_eq!(parsed["id"], 1);
    assert_eq!(parsed["webhook_id"], "wh_alpha");
    // next_cursor is null → no hint line.
    assert!(
        !stdout.contains("More deliveries available"),
        "no next-page hint when next_cursor=null:\n{stdout}"
    );
}

#[tokio::test]
async fn deliveries_passes_limit_and_cursor_query_params() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks/wh_alpha/deliveries"))
        .and(query_param("limit", "10"))
        .and(query_param("cursor", "eyJ2IjoxfQ"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deliveries": [],
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
        .arg("webhooks")
        .arg("deliveries")
        .arg("wh_alpha")
        .arg("--limit")
        .arg("10")
        .arg("--cursor")
        .arg("eyJ2IjoxfQ");

    cmd.assert().success();
}

#[tokio::test]
async fn deliveries_omits_query_when_only_id() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    // No query_param matcher — wiremock would 404 on a query string.
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks/wh_alpha/deliveries"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deliveries": [],
            "limit": 50,
            "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("deliveries")
        .arg("wh_alpha");

    cmd.assert().success();
}

#[tokio::test]
async fn deliveries_401_propagates() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks/wh_alpha/deliveries"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "unauthorized"
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
        .failure()
        .stderr(predicate::str::contains("401").or(predicate::str::contains("unauthorized")));
}

#[tokio::test]
async fn deliveries_404_propagates_for_missing_webhook() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks/wh_missing/deliveries"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": "webhook not found"
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.current_dir(project.path());
    cmd.env("EDGE_API_URL", server.uri())
        .arg("webhooks")
        .arg("deliveries")
        .arg("wh_missing");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("404").or(predicate::str::contains("not found")));
}

#[tokio::test]
async fn deliveries_400_on_invalid_cursor_propagates() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    write_minimal_edge_toml(&project);
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    Mock::given(method("GET"))
        .and(path("/api/v1/webhooks/wh_alpha/deliveries"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": "invalid cursor"
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
        .arg("garbage");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("400").or(predicate::str::contains("invalid cursor")));
}
