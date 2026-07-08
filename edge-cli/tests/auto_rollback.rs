//! Integration tests for `edge deploy --auto-rollback` and `edge deploy --file`.
//!
//! Mirrors the wiremock pattern from `tests/rollback.rs` and
//! `tests/deploy.rs`. The CLI test exists to pin the contract that
//! the flag is forwarded to the control plane as the query
//! parameter `?auto-rollback=true`; the server's behavior on that
//! param (copying it onto the deployments row, etc.) is exercised
//! in `edge-control-plane/internal/service/deployment_test.go`.

use assert_cmd::Command;
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

/// A minimal valid wasm header (magic bytes \0asm). The CLI never
/// runs the artifact locally — it just reads the file and POSTs it
/// — so a 4-byte file with the correct magic is enough for the CLI
/// tests. (The server's stricter validation is exercised separately
/// in `edge-control-plane/internal/service/deployment_test.go`.)
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
    // Drop the wasm artifact at the expected location so the CLI's
    // upload path finds it. Issue #410: the rust layout moved from
    // `target/wasm32-wasip2/release/<name>.wasm` (cargo output) to
    // `target/component.wasm` (the wasm-tools-wrapped component).
    let artifact_dir = project.path().join("target");
    std::fs::create_dir_all(&artifact_dir).unwrap();
    std::fs::write(artifact_dir.join("component.wasm"), VALID_WASM_HEADER).unwrap();
}

/// `edge deploy --auto-rollback` MUST append `?auto-rollback=true`
/// to the upload URL — the server reads that param and copies the
/// flag onto the deployments row.
#[tokio::test]
async fn deploy_auto_rollback_flag_forwards_to_query_param() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .and(query_param("auto-rollback", "true"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "d_new",
            "url": "https://myapp.example.test",
            "regions": ["global"],
            "auto_rollback_enabled": true,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    cmd.arg("--auto-rollback");

    cmd.assert().success();
}

/// `edge deploy --file <path>` MUST read the artifact from the
/// given path instead of the default target directory.
#[tokio::test]
async fn deploy_with_file_flag_uploads_custom_artifact() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    // Write edge.toml but do NOT seed the default target dir.
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "myapp"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
    )
    .unwrap();
    // Write the artifact to a custom path.
    let custom_wasm = project.path().join("custom.wasm");
    std::fs::write(&custom_wasm, VALID_WASM_HEADER).unwrap();

    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "d_custom",
            "url": "https://myapp.example.test",
            "regions": ["global"],
            "auto_rollback_enabled": false,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("deploy");
    cmd.arg("--file");
    cmd.arg(custom_wasm.to_string_lossy().as_ref());

    cmd.assert().success();
}

/// Without the flag, the upload MUST NOT carry the `auto-rollback`
/// query param. Mounting a mock that REQUIRES the param absent is
/// awkward in wiremock (no built-in "not equals" matcher), so the
/// assertion here is structural: the upload must succeed against
/// a mock that matches any POST to the deploy path. If the CLI
/// spuriously appended `?auto-rollback=false` (a redundant but
/// tolerable alternative), this test would still pass — the goal
/// is to confirm the happy path doesn't regress, not to enforce
/// the URL shape on the false branch (which has its own
/// `TestDeploy_DefaultFalse_OmitsQueryParam` server-side check).
#[tokio::test]
async fn deploy_without_auto_rollback_flag_still_uploads() {
    let home = common::isolated_home();
    let project = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");
    seed_project(&project, "myapp");

    Mock::given(method("POST"))
        .and(path("/api/v1/deploy/myapp"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "d_new",
            "url": "https://myapp.example.test",
            "regions": ["global"],
            "auto_rollback_enabled": false,
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
