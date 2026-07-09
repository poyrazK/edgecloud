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

use assert_cmd::Command;
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

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
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

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
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
        let mut cmd = Command::cargo_bin("edge-cli").unwrap();
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
