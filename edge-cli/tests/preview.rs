//! Integration tests for `edge deploy --preview --pr-number=<N>`
//! (issue #308).
//!
//! Mirrors the wiremock pattern from `tests/auto_rollback.rs`. The
//! CLI's job here is narrow: forward the preview-id, pr-number, and
//! optional ttl as query params to the control plane, then echo
//! back the server's preview metadata into `.edge/state.json`.
//! Server-side parsing/validation is exercised in
//! `edge-control-plane/internal/handler/deployment_test.go`.

use assert_cmd::Command;
use serde_json::Value;
use tempfile::TempDir;
use wiremock::matchers::{method, query_param};
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
    std::fs::write(artifact_dir.join("component.wasm"), VALID_WASM_HEADER).unwrap();
}

/// `edge deploy --preview --pr-number=<N>` MUST suffix the app
/// (`myapp` → `myapp--preview-<hash>`) and forward three query
/// params: `preview-id=<hex>`, `preview-pr-number=<N>`, and
/// `preview-ttl=<duration>` when supplied. The server's parsePreviewOpts
/// rejects malformed inputs — this test exercises the happy path
/// where the CLI mints a valid hex suffix.
#[tokio::test]
async fn deploy_preview_forwards_pr_number_to_query_param() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    // The CLI mints the preview-id as a hex hash, so we can't pin
    // a specific value — but we CAN pin the format with the regex
    // matcher. wiremock's `query_param` accepts a regex via
    // `matches` (separate from the value-exact matcher).
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/v1/deploy/myapp--preview-[a-f0-9]+$"))
        .and(query_param("preview-pr-number", "123"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "d_preview",
            "url": "https://preview.example.test",
            "regions": ["global"],
            "auto_rollback_enabled": false,
            "preview_id": "abcd1234",
            "preview_pr_number": 123,
            "preview_expires_at": "2026-07-16T00:00:00Z",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    cmd.arg("--preview");
    cmd.arg("--pr-number=123");

    cmd.assert().success();

    // After deploy, `.edge/state.json` should reflect the preview
    // metadata so `edge status` can show "PR #123 — expires
    // 2026-07-16" without re-querying the server.
    let state_path = project.path().join(".edge").join("state.json");
    let raw = std::fs::read_to_string(&state_path).expect("state.json was not written");
    let json: Value = serde_json::from_str(&raw).expect("state.json is not valid JSON");
    assert_eq!(
        json["preview_pr_number"], 123,
        "preview_pr_number must round-trip into state.json"
    );
    assert_eq!(
        json["preview_id"], "abcd1234",
        "preview_id must round-trip into state.json"
    );
}

/// `edge deploy --preview` without `--pr-number` must still emit the
/// `preview-id` query param (the marker that makes this a preview
/// deploy) but MUST NOT emit `preview-pr-number`. A laptop user
/// running `--preview` outside CI is the canonical case here.
#[tokio::test]
async fn deploy_preview_without_pr_number_omits_pr_param() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    // Match on `preview-id` present (any hex value). wiremock
    // doesn't have a built-in "param absent" matcher — instead we
    // rely on the fact that wiremock REJECTS requests with
    // unexpected query params IF you mount with `Mock::given(...)`
    // AND `expect(1)`. To verify "preview-pr-number NOT present"
    // cleanly, we accept the request and assert via a closure
    // matcher on the request body / URL.
    Mock::given(method("POST"))
        .and(path_regex(r"^/api/v1/deploy/myapp--preview-[a-f0-9]+$"))
        .and(query_param_exists("preview-id"))
        .and(query_param_absent("preview-pr-number"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "d_preview",
            "url": "https://preview.example.test",
            "regions": ["global"],
            "auto_rollback_enabled": false,
            "preview_id": "abcd1234",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    cmd.arg("--preview");

    cmd.assert().success();
}

/// `edge deploy --preview --preview-ttl 24h` MUST forward the
/// override TTL as `?preview-ttl=24h`. The server applies it
/// instead of the default 168h.
#[tokio::test]
async fn deploy_preview_forwards_ttl_override() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("POST"))
        .and(path_regex(r"^/api/v1/deploy/myapp--preview-[a-f0-9]+$"))
        .and(query_param("preview-ttl", "24h"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "d_preview",
            "url": "https://preview.example.test",
            "regions": ["global"],
            "auto_rollback_enabled": false,
            "preview_id": "abcd1234",
            "preview_expires_at": "2026-07-10T00:00:00Z",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    cmd.arg("--preview");
    cmd.arg("--preview-ttl=24h");

    cmd.assert().success();
}

// ── wiremock helpers ────────────────────────────────────────────────

/// Path regex matcher — wiremock ships with `path_regex` already,
/// but `path_regex` is not in the public prelude when only
/// `wiremock::matchers::{method, path, query_param}` are imported
/// above. Re-export here for clarity and to avoid the `path_regex`
/// feature-flag dance.
fn path_regex(re: &str) -> wiremock::matchers::PathRegexMatcher {
    wiremock::matchers::path_regex(re)
}

/// Match a query param that exists (any value). Used to assert
/// `preview-id` is present without pinning a specific value (the
/// CLI mints a fresh hash per invocation).
fn query_param_exists(name: &'static str) -> QueryParamExistsMatcher {
    QueryParamExistsMatcher(name)
}

/// Match a query param that is absent. Used to assert
/// `preview-pr-number` is omitted on a `--preview` (no `--pr-number`)
/// run.
fn query_param_absent(name: &'static str) -> QueryParamAbsentMatcher {
    QueryParamAbsentMatcher(name)
}

struct QueryParamExistsMatcher(&'static str);
impl Match for QueryParamExistsMatcher {
    fn matches(&self, req: &Request) -> bool {
        req.url.query_pairs().any(|(k, _)| k == self.0)
    }
}

struct QueryParamAbsentMatcher(&'static str);
impl Match for QueryParamAbsentMatcher {
    fn matches(&self, req: &Request) -> bool {
        !req.url.query_pairs().any(|(k, _)| k == self.0)
    }
}
