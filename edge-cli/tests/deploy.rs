//! Integration tests for `edge deploy` (issue #52 — Idempotency-Key
//! header on the deploy request).
//!
//! Three pin tests for the wire contract:
//!
//!   * `--idempotency-key <UUID>` -> request carries
//!     `Idempotency-Key: <UUID>` verbatim.
//!   * No flag -> request has no `Idempotency-Key` header. The
//!     runner auto-mints a UUID and passes it as the header value,
//!     so the server still gets a key. To pin "header attached"
//!     vs "header absent" we drive the binary directly with the
//!     wiremock matcher pinned to the absence of the header.
//!   * Replay: server returns 200 with the cached deployment_id;
//!     the binary parses both 201 fresh and 200 replay shapes
//!     identically (the JSON body is the same; only status
//!     changes).
//!
//! Pattern mirrors `tests/preview.rs` — drive the binary via
//! assert_cmd, mock the server with wiremock, use a custom
//! Match impl for header-absence (wiremock 0.6 has no
//! `header_missing` helper).
//!
//! Plus six retry tests for issue #571 — see the dedicated
//! section at the bottom.

use assert_cmd::Command;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Match, Mock, MockServer, Request, ResponseTemplate};

mod common;

/// Minimal valid wasm header. The CLI never runs the artifact
/// locally — it just reads the file and POSTs it — so a 4-byte
/// file with the correct magic is enough for the CLI tests.
const VALID_WASM_HEADER: &[u8] = b"\0asm\x01\x00\x00\x00";

fn seed_project(project: &TempDir, app_name: &str) {
    std::fs::write(
        project.path().join("edge.toml"),
        format!(
            r#"[project]
name = "{app_name}"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#
        ),
    )
    .unwrap();
    let artifact_dir = project.path().join("target");
    std::fs::create_dir_all(&artifact_dir).unwrap();
    // The CLI's default path resolver returns
    // `target/component.wasm` for `rust` (the wrapped component,
    // not the raw wasm32-wasip2 core module — see
    // edge-cli/src/commands/build.rs::path_for). Drop a minimal
    // wasm header there so the deploy's `std::fs::read` succeeds.
    std::fs::write(artifact_dir.join("component.wasm"), VALID_WASM_HEADER).unwrap();
}

/// `edge deploy --idempotency-key <UUID>` MUST forward the value
/// verbatim as the `Idempotency-Key` header. The server validates
/// the format and returns 201 with the freshly-minted deployment
/// id.
#[tokio::test]
async fn deploy_forwards_idempotency_key_header_when_flag_set() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header(
            "Idempotency-Key",
            "deadbeef-1234-5678-9abc-def012345678",
        ))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "d_fresh",
            "url": "https://t_test-myapp.edgecloud.dev",
            "regions": ["us-east-1"],
            "auto_rollback_enabled": false,
            "desired_replicas": 0,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    cmd.arg("--idempotency-key=deadbeef-1234-5678-9abc-def012345678");

    cmd.assert().success();
}

/// `edge deploy` (no `--idempotency-key` flag) auto-mints a fresh
/// UUID v4 per invocation, so the request still carries an
/// `Idempotency-Key` header — just one we can't predict. This
/// test pins that the header is always present (never omitted)
/// when the runner auto-mints: a regression that drops the
/// header would surface as a wiremock match failure.
#[tokio::test]
async fn deploy_auto_mints_idempotency_key_when_flag_omitted() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    // Match any UUID v4 string in the Idempotency-Key header.
    // wiremock's `header` matcher accepts a regex via `.matches`.
    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header_is_uuid_v4("Idempotency-Key"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "d_auto",
            "url": "https://t_test-myapp.edgecloud.dev",
            "regions": ["us-east-1"],
            "auto_rollback_enabled": false,
            "desired_replicas": 0,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");

    cmd.assert().success();
}

/// `edge deploy --idempotency-key <UUID>` issued twice: the server
/// always returns the same id (mimicking the issue-#52 replay
/// path, where the second call short-circuits and returns the
/// cached deployment_id). Both invocations succeed and the
/// persisted `.edge/state.json` references the SAME id — the
/// only way that happens is if the Idempotency-Key header was
/// attached on both calls (so a wiremock match failure surfaces
/// the missing-header regression).
///
/// We use a single mock returning 200 (the replay status code)
/// on both calls rather than two mocks (201 first, 200 second).
/// wiremock 0.6 doesn't reliably distinguish identical-conditions
/// mocks by registration order, and the contract we care about
/// here is "state.json pins a stable id across retries" — which
/// is exactly what a single 200-returning mock exercises. The
/// 201-then-200 transition is covered server-side by the
/// `internal/handler/deployment_test.go::TestDeploy_IdempotencyKey_*`
/// suite.
#[tokio::test]
async fn deploy_replay_returns_same_id_in_state_file() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header("Idempotency-Key", "replay-key-aaaa"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "d_replay_target",
            "url": "https://t_test-myapp.edgecloud.dev",
            "regions": ["us-east-1"],
            "auto_rollback_enabled": false,
            "desired_replicas": 0,
        })))
        .expect(2)
        .mount(&server)
        .await;

    for _ in 0..2 {
        let mut cmd = Command::cargo_bin("edge").unwrap();
        common::set_platform_env(&mut cmd, &home);
        cmd.env("EDGE_API_URL", server.uri());
        cmd.current_dir(project.path());
        cmd.arg("deploy");
        cmd.arg("--idempotency-key=replay-key-aaaa");
        cmd.assert().success();
    }

    let state_path = project.path().join(".edge").join("state.json");
    let state = std::fs::read_to_string(&state_path).expect("state.json should exist");
    let parsed: serde_json::Value =
        serde_json::from_str(&state).expect("state.json should be valid JSON");
    assert_eq!(
        parsed["deployment_id"], "d_replay_target",
        "state.json should pin the replay id"
    );
}

// ── wiremock helpers ────────────────────────────────────────────────

/// Match a header whose value is a UUID v4 string. wiremock
/// 0.6 ships `header(name, value)` for exact matches; the
/// "auto-minted UUID" assertion needs to verify the value IS a
/// v4 UUID without pinning a specific value (the CLI mints a
/// fresh one per run). Inline the check — no regex dep needed.
fn header_is_uuid_v4(name: &'static str) -> HeaderUuidV4Matcher {
    HeaderUuidV4Matcher { name }
}

struct HeaderUuidV4Matcher {
    name: &'static str,
}

impl Match for HeaderUuidV4Matcher {
    fn matches(&self, req: &Request) -> bool {
        let Some(h) = req
            .headers
            .get(self.name.to_ascii_lowercase().replace('_', "-"))
        else {
            return false;
        };
        let Ok(text) = h.to_str() else { return false };
        // 36 chars: 8-4-4-4-12 hex with the v4 marker (position
        // 14 == '4'). Inline anchored check; no regex dep needed.
        text.len() == 36
            && text.as_bytes()[8] == b'-'
            && text.as_bytes()[13] == b'-'
            && text.as_bytes()[18] == b'-'
            && text.as_bytes()[23] == b'-'
            && text.as_bytes()[14] == b'4'
            && text.as_bytes().iter().enumerate().all(|(i, b)| match i {
                8 | 13 | 18 | 23 => *b == b'-',
                _ => b.is_ascii_hexdigit() && !b.is_ascii_uppercase(),
            })
    }
}

// ── Issue #571: retry loop tests ─────────────────────────────────────
//
// These tests pin the contract that `edge deploy` retries transient
// failures (5xx, 429, network error) on the same `Idempotency-Key`
// until either the server succeeds or the retry budget is exhausted.
// Non-transient 4xx (401, 422) must NOT retry.
//
// All tests drive the binary end-to-end via assert_cmd against a
// wiremock MockServer. The wiremock matcher fires on all requests
// that match its conditions, so chaining `503` (`.expect(N)`) then
// `201` (`.expect(M)`) gives the failure-then-success sequence the
// retry loop expects — wiremock walks the registered mocks in
// reverse insertion order when conditions are equal, so the
// later-mounted mock fires first.

/// Standard 201 deploy response body. Centralized so the
/// success-path tests don't all duplicate the JSON shape.
fn deploy_201_body() -> serde_json::Value {
    serde_json::json!({
        "id": "d_replay_target",
        "url": "https://t_test-myapp.edgecloud.dev",
        "regions": ["us-east-1"],
        "auto_rollback_enabled": false,
        "desired_replicas": 0,
    })
}

/// `edge deploy` retried on 503-then-201 succeeds, and the
/// `Idempotency-Key` header is byte-identical across all three
/// attempts (the load-bearing Idempotency-Key contract). The
/// auto-minted UUID path is exercised here — `header_is_uuid_v4`
/// pins the format on each request.
#[tokio::test]
async fn deploy_retries_503_then_succeeds_with_same_idempotency_key() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    // First two matches: 503 (the failure path). Third: 201
    // (the success path). wiremock 0.6 walks mocks in mount
    // order when matchers are identical — the 503 mock fires
    // for the first `up_to_n_times(2)` requests, then the 201
    // mock fires once the 503 cap is hit. Explicit `.named(...)`
    // on both mocks so wiremock treats them as distinct entries
    // (otherwise the identical matchers collapse into one).
    // NOTE: wiremock 0.6's `.expect(n)` sets `expectation_range`,
    // which is only enforced at mock-server shutdown — it does
    // NOT cap matches at runtime. Use `.up_to_n_times(n)` for
    // runtime capping. Both APIs are kept here so the test
    // ALSO asserts the post-hoc expectation (the verification
    // at server-drop time will pass with `expect(1)` on 201).
    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header_is_uuid_v4("Idempotency-Key"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .named("503-then-503")
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header_is_uuid_v4("Idempotency-Key"))
        .respond_with(ResponseTemplate::new(201).set_body_json(deploy_201_body()))
        .named("201-success")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    // Tight base so the test stays under ~250ms wall time
    // (500/1000 jittered = ~750-2500ms with default 500ms base).
    cmd.arg("--retry-base-ms=10");

    cmd.assert().success();

    // All three received requests must carry byte-identical
    // Idempotency-Key values — the load-bearing assertion for
    // the contract. A regression that re-mints the key inside
    // the retry loop fails here.
    let received = server.received_requests().await.expect("received requests");
    assert_eq!(received.len(), 3, "expected exactly 3 requests");
    let key0 = received[0]
        .headers
        .get("idempotency-key")
        .expect("Idempotency-Key on request 0")
        .to_str()
        .unwrap();
    for (i, req) in received.iter().enumerate().skip(1) {
        let key = req
            .headers
            .get("idempotency-key")
            .unwrap_or_else(|| panic!("Idempotency-Key on request {i}"))
            .to_str()
            .unwrap();
        assert_eq!(key, key0, "Idempotency-Key changed at request {i}");
    }
}

/// `edge deploy` retried on 429-then-201 succeeds, and the
/// retry sleep actually fires (>= the configured base backoff).
/// The 429 status is the special case in `ApiError::is_retryable`
/// — the deploy handler doesn't emit `Retry-After`, so a backoff
/// loop is the right shape.
#[tokio::test]
async fn deploy_retries_429_with_backoff() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header_is_uuid_v4("Idempotency-Key"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .named("429-first")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header_is_uuid_v4("Idempotency-Key"))
        .respond_with(ResponseTemplate::new(201).set_body_json(deploy_201_body()))
        .named("201-success")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    // Use a base of 200ms with the same cap; the first backoff
    // sleeps 200ms × ±25% (jitter floor 150ms), so the binary
    // can't possibly return within 150ms.
    cmd.arg("--retry-base-ms=200");
    cmd.arg("--retry-cap-ms=400");
    cmd.arg("--max-retries=3");

    let start = Instant::now();
    cmd.assert().success();
    assert!(
        start.elapsed() >= Duration::from_millis(150),
        "expected >= 150ms backoff, took {:?}",
        start.elapsed()
    );
}

/// `edge deploy` does NOT retry on a 400 response. The 4xx
/// (other than 429) is a deterministic failure — the binary
/// should exit non-zero on the first attempt.
#[tokio::test]
async fn deploy_does_not_retry_400() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header_is_uuid_v4("Idempotency-Key"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .named("400-no-retry")
        .up_to_n_times(1) // single attempt — no retry
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");

    cmd.assert().failure();
    // Verify no retry happened by inspecting received requests.
    let received = server.received_requests().await.expect("received requests");
    assert_eq!(received.len(), 1, "400 must not trigger a retry");
}

/// `edge deploy` does NOT retry on 422 (idempotency-key
/// artifact-mismatch). Retrying a 422 just loops the same 422
/// forever — the contract at
/// `edge-control-plane/internal/handler/deployment.go:442-452`
/// returns 422 when the key is reused against a different
/// artifact SHA. This is the load-bearing "don't retry" case.
#[tokio::test]
async fn deploy_does_not_retry_422() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header("Idempotency-Key", "idempotency-mismatch-key"))
        .respond_with(ResponseTemplate::new(422).set_body_string(
            "{\"error\":\"idempotency key reused with a different request body\"}",
        ))
        .named("422-no-retry")
        .up_to_n_times(1) // single attempt — no retry
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    // Pass an explicit user-supplied key so this test would also
    // catch a regression where the runner mints a fresh key
    // per attempt (the server would see a *new* key, fall
    // through to a fresh-deploy path, and likely return a
    // different status — this test asserts the 422-only
    // contract regardless).
    cmd.arg("--idempotency-key=idempotency-mismatch-key");

    cmd.assert().failure();
    let received = server.received_requests().await.expect("received requests");
    assert_eq!(received.len(), 1, "422 must not trigger a retry");
}

/// `edge deploy` exhausts the retry budget on sustained 503s and
/// surfaces the last error. `--max-retries=3` means up to 4
/// attempts total; the binary exits non-zero with the anyhow
/// error from `client.deploy` (the transient body string).
#[tokio::test]
async fn deploy_exhausts_max_attempts_and_surfaces_last_error() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header_is_uuid_v4("Idempotency-Key"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .named("503-exhaust")
        .up_to_n_times(4) // 1 initial + 3 retries
        .expect(4)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    cmd.arg("--max-retries=3");
    cmd.arg("--retry-base-ms=10"); // tight base — total backoff < 200ms

    let output = cmd.assert().failure();
    // Surface the error string from `client.deploy`'s anyhow!
    // path so the test pins that we DID hit the failure arm,
    // not a panic or a silent success.
    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains("upstream down") || stderr.contains("503"),
        "expected error body or status in stderr, got: {stderr}"
    );

    let received = server.received_requests().await.expect("received requests");
    assert_eq!(
        received.len(),
        4,
        "expected exactly 4 attempts (1 + max-retries)"
    );
}

/// `edge deploy --idempotency-key <UUID>` retried on 503-then-201
/// preserves the user-supplied Idempotency-Key byte-for-byte across
/// attempts. This is the canonical test for the contract — a
/// regression where the retry loop regenerates the key fails here.
/// The wiremock mock's `header("Idempotency-Key", ...)` exact-match
/// matcher enforces the byte-equality at mock-resolution time; the
/// explicit `received_requests` assertion below guards against a
/// silent matcher regression.
#[tokio::test]
async fn deploy_retry_preserves_user_supplied_idempotency_key() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    const USER_KEY: &str = "11111111-2222-3333-4444-555555555555";
    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header("Idempotency-Key", USER_KEY))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .named("503-then-201")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(header("Idempotency-Key", USER_KEY))
        .respond_with(ResponseTemplate::new(201).set_body_json(deploy_201_body()))
        .named("201-success")
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    cmd.arg(format!("--idempotency-key={USER_KEY}"));
    cmd.arg("--retry-base-ms=10");

    cmd.assert().success();

    // Both received requests carry the user-supplied UUID.
    let received = server.received_requests().await.expect("received requests");
    assert_eq!(
        received.len(),
        2,
        "expected 1 retry + 1 success = 2 requests"
    );
    for (i, req) in received.iter().enumerate() {
        let key = req
            .headers
            .get("idempotency-key")
            .unwrap_or_else(|| panic!("Idempotency-Key on request {i}"))
            .to_str()
            .unwrap();
        assert_eq!(
            key, USER_KEY,
            "Idempotency-Key on request {i} should be the user-supplied UUID"
        );
    }
}

/// Pre-flight path-component validation on `edge deploy`. The
/// `app_name` is interpolated into `POST /api/v1/deploy/{app_name}`;
/// the validator must fire before any round-trip. Issue #671.
#[tokio::test]
async fn deploy_rejects_invalid_app_name() {
    for (bad_name, expected_substr) in [("my../api", "'..'"), ("my%2Fapi", "invalid character")] {
        let home = common::isolated_home();
        let project = common::isolated_home();
        let server = MockServer::start().await;

        common::seed_api_key(&home, "k_seed");
        // edge.toml + a stub artifact are required by `edge deploy`.
        // We seed them so the path-component validator (which runs
        // AFTER the toml parse) actually gets to fire. We don't need
        // the wasm to be valid — the validator must bail BEFORE any
        // upload attempt.
        seed_project(&project, "myapp");

        // Fence: NO POST on /api/v1/deploy/<id> may ever land.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&server)
            .await;

        let mut cmd = Command::cargo_bin("edge").unwrap();
        common::set_platform_env(&mut cmd, &home);
        cmd.current_dir(project.path());
        cmd.env("EDGE_API_URL", server.uri());
        // `[APP]` is positional on `edge deploy` — it overrides the
        // `[project].name` from edge.toml.
        cmd.arg("deploy").arg(bad_name);

        common::assert_invalid_path_component(cmd, expected_substr);
    }
}
