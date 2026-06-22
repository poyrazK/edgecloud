//! Integration tests for the `edge auth` subcommand group.
//!
//! Uses `wiremock` for the control plane, `assert_cmd` to drive the
//! `edge` binary, and a `HOME` override (via `dirs::config_dir()`) to
//! isolate the config file per-test.

use std::io::Write;
use std::path::PathBuf;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Returns a fresh tempdir. The caller passes `home.path()` to the
/// child as `HOME` (and on Windows, as `APPDATA`/`USERPROFILE`).
/// This function does not mutate the parent process env, so concurrent
/// tests do not race.
fn isolated_home() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

/// Path to the config file the CLI will actually read/write, given the
/// tempdir we passed as `HOME` (or `APPDATA` on Windows) to the child.
///
/// IMPORTANT: do not call `dirs::config_dir()` here — that resolves
/// against the *test process* env, which is the developer's real home,
/// not the child's overridden home. Tests would then read/write the
/// developer's actual config file and stomp on each other. Instead,
/// compute the path the same way the child will: on macOS, join
/// `Library/Application Support`; on Linux, join `.config` (the XDG
/// default — we deliberately do NOT set `XDG_CONFIG_HOME` for the
/// child, so `dirs::config_dir()` falls back to `$HOME/.config` and
/// the path here matches the path the child sees).
fn config_file_for(home: &TempDir) -> PathBuf {
    if cfg!(target_os = "macos") {
        home.path()
            .join("Library")
            .join("Application Support")
            .join("edgecloud")
            .join("config.toml")
    } else if cfg!(target_os = "windows") {
        home.path()
            .join("AppData")
            .join("Roaming")
            .join("edgecloud")
            .join("config.toml")
    } else {
        home.path()
            .join(".config")
            .join("edgecloud")
            .join("config.toml")
    }
}

/// Inject the platform-appropriate env vars so the child CLI resolves
/// its config dir to the test tempdir, not the developer's real home.
///
/// We set `HOME` (and on Windows, `APPDATA`/`USERPROFILE`) and
/// explicitly strip `XDG_CONFIG_HOME`. On Linux, `dirs::config_dir()`
/// prefers `XDG_CONFIG_HOME` over `HOME`; if the test process (or the
/// CI runner) has `XDG_CONFIG_HOME` pointing at the developer's real
/// config, the child would inherit it and read/write the real file
/// instead of the test tempdir. macOS and Windows ignore `XDG_CONFIG_HOME`,
/// but stripping it is harmless there.
fn set_platform_env(cmd: &mut Command, home: &TempDir) {
    if cfg!(target_os = "windows") {
        cmd.env("APPDATA", home.path().join("AppData").join("Roaming"));
        cmd.env("USERPROFILE", home.path());
    } else {
        cmd.env("HOME", home.path());
    }
    // Always strip any host-process env vars that could shadow the test.
    cmd.env_remove("XDG_CONFIG_HOME");
    cmd.env_remove("EDGE_API_KEY");
}

/// Read the config file and parse out `default.api_key` (if any).
fn read_api_key(home: &TempDir) -> Option<String> {
    let path = config_file_for(home);
    let content = std::fs::read_to_string(&path).ok()?;
    #[derive(serde::Deserialize)]
    struct Cfg {
        default: DefaultSection,
    }
    #[derive(serde::Deserialize)]
    struct DefaultSection {
        api_key: Option<String>,
    }
    let cfg: Cfg = toml::from_str(&content).ok()?;
    cfg.default.api_key
}

#[tokio::test]
async fn signup_writes_returned_key_to_config_file() {
    let home = isolated_home();
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v1/tenants"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "tenant_id": "t_abc123",
            "api_key": "k_returned_by_server",
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("signup")
        .arg("--name")
        .arg("test-user");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("t_abc123"));

    let stored = read_api_key(&home).expect("config file should exist with api_key");
    assert_eq!(stored, "k_returned_by_server");
}

#[test]
fn login_with_key_flag_writes_to_config() {
    let home = isolated_home();

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.arg("auth").arg("login").arg("--key").arg("k_from_flag");

    // Login also tries to call whoami at the end. We don't mount a
    // server here, so it should fail gracefully (warning, not error)
    // and the local save should still succeed.
    cmd.assert().success();

    let stored = read_api_key(&home).expect("config file should exist");
    assert_eq!(stored, "k_from_flag");
}

#[test]
fn login_from_stdin_writes_to_config() {
    let home = isolated_home();

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.arg("auth").arg("login").write_stdin("k_from_stdin\n");

    cmd.assert().success();

    let stored = read_api_key(&home).expect("config file should exist");
    assert_eq!(stored, "k_from_stdin");
}

#[tokio::test]
async fn whoami_prints_tenant_info() {
    let home = isolated_home();
    let server = MockServer::start().await;

    // Pre-seed the config so the client has a key.
    let cfg_path = config_file_for(&home);
    if let Some(parent) = cfg_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(f, "[default]\napi_key = \"k_seed\"\n").unwrap();

    Mock::given(method("GET"))
        .and(path("/api/v1/auth/whoami"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant_id": "t_xyz",
            "tenant_name": "Acme",
            "plan": "free",
            "api_key_id": "k_def",
            "api_key_name": "default",
            "role": "owner",
            "created_at": "2026-06-17T12:00:00Z",
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("whoami");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Acme"))
        .stdout(predicate::str::contains("t_xyz"))
        .stdout(predicate::str::contains("owner"));
}

#[test]
fn logout_removes_key_from_config() {
    let home = isolated_home();
    let cfg_path = config_file_for(&home);
    if let Some(parent) = cfg_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(f, "[default]\napi_key = \"k_to_remove\"\n").unwrap();

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.arg("auth").arg("logout");

    cmd.assert().success();
    assert!(
        read_api_key(&home).is_none(),
        "api_key should be removed from config after logout"
    );
}

#[test]
fn logout_is_idempotent_when_no_key() {
    let home = isolated_home();
    // No config file exists.

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.arg("auth").arg("logout");

    cmd.assert().success();
}

/// F1: a key the server rejects (401) must exit non-zero, but the
/// rejected key is still on disk so the user can re-run `login` to
/// overwrite it. Verifies the post-save verification now treats
/// credential rejection as a hard error rather than a soft warning.
#[tokio::test]
async fn login_rejects_bad_key_exits_one_keeps_saved_key() {
    let home = isolated_home();
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/auth/whoami"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid key"))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("login")
        .arg("--key")
        .arg("k_typo");

    cmd.assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("rejected"));

    // The bad key is still on disk; the user can fix it with another
    // `login` call rather than starting from scratch.
    let stored = read_api_key(&home).expect("rejected key should remain on disk");
    assert_eq!(stored, "k_typo");
}

/// F2: an exported `EDGE_API_KEY` must NOT shadow the just-saved key
/// during the post-save verification. The server should see the
/// pasted `k_real` as the Bearer token, not the env-var `k_env`.
#[tokio::test]
async fn login_verifies_just_saved_key_not_env_var() {
    use wiremock::matchers::header;

    let home = isolated_home();
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/auth/whoami"))
        .and(header("Authorization", "Bearer k_real"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant_id": "t_xyz",
            "tenant_name": "Acme",
            "plan": "free",
            "api_key_id": "k_def",
            "api_key_name": "default",
            "role": "owner",
            "created_at": "2026-06-18T00:00:00Z",
        })))
        // If the env-var shadowed the just-saved key, the request would
        // arrive with `Bearer k_env` and be rejected by this mock.
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .env("EDGE_API_KEY", "k_env_stale")
        .arg("auth")
        .arg("login")
        .write_stdin("k_real\n");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Logged in as Acme"));
}

/// F11: when the server rejects the signup request (e.g. invalid plan),
/// the CLI must exit non-zero AND must not write a key to the config
/// file — otherwise the user would end up with a saved credential for
/// a tenant the server never created.
#[tokio::test]
async fn signup_server_rejects_invalid_plan_does_not_write_key() {
    let home = isolated_home();
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v1/tenants"))
        .respond_with(ResponseTemplate::new(400).set_body_string("invalid plan"))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("signup")
        .arg("--name")
        .arg("test-user")
        .arg("--plan")
        .arg("bogus");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("signup failed"));

    // The rejected signup must NOT leave a key behind.
    assert!(
        read_api_key(&home).is_none(),
        "rejected signup should not write api_key to config"
    );
}

/// Pre-seed the tempdir config with a known API key. Used by the
/// `keys create` tests below so the CLI is already authenticated when
/// it tries to mint an additional key.
fn seed_api_key(home: &TempDir, key: &str) {
    let cfg_path = config_file_for(home);
    if let Some(parent) = cfg_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(f, "[default]\napi_key = \"{key}\"\n").unwrap();
}

#[tokio::test]
async fn keys_create_prints_token_and_does_not_overwrite_saved_key() {
    let home = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_existing");

    // Assert the bearer token the CLI sends matches the on-disk key
    // (i.e. the new key never overwrites it; we are using the saved
    // key to authenticate the create call). Also assert the request
    // body contains the default role "developer" so a future refactor
    // that drops the `default_value` attribute would be caught.
    Mock::given(method("POST"))
        .and(path("/api/v1/keys"))
        .and(header("Authorization", "Bearer k_existing"))
        .and(body_string(r#"{"name":"ci-key","role":"developer"}"#))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "id": "k_new",
            "name": "ci-key",
            "role": "developer",
            "token": "raw-token-shown-once",
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("keys")
        .arg("create")
        .arg("--name")
        .arg("ci-key");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("k_new"))
        .stdout(predicate::str::contains("raw-token-shown-once"))
        .stderr(predicate::str::contains("NOT saved"));

    // The on-disk key must NOT have been overwritten.
    let stored = read_api_key(&home).expect("config should still have a key");
    assert_eq!(
        stored, "k_existing",
        "keys create must not overwrite the saved api_key"
    );
}

#[tokio::test]
async fn keys_create_without_saved_key_exits_non_zero() {
    let home = isolated_home();
    // No seed_api_key call → no key on disk, no EDGE_API_KEY env.
    // The CLI must refuse to try to authenticate against /api/keys.

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.arg("auth")
        .arg("keys")
        .arg("create")
        .arg("--name")
        .arg("n");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("API key not found"));
}

#[tokio::test]
async fn keys_create_server_rejects_does_not_overwrite_key() {
    let home = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_existing");

    Mock::given(method("POST"))
        .and(path("/api/v1/keys"))
        .respond_with(ResponseTemplate::new(400).set_body_string("invalid role"))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("keys")
        .arg("create")
        .arg("--name")
        .arg("ci-key")
        .arg("--role")
        .arg("bogus");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("keys create failed"))
        .stdout(predicate::str::contains("raw-token-shown-once").not());

    let stored = read_api_key(&home).expect("config should still have a key");
    assert_eq!(
        stored, "k_existing",
        "rejected keys create must not touch the saved api_key"
    );
}

/// D: when `edge.toml` `[deployment]` has no `api` key, the runtime
/// must fall through to `EDGE_API_URL`. This pins the end-to-end
/// behavior of the 7 call-site updates that switched from
/// `edge_toml.deployment.api.clone()` to `edge_toml.api_url(...)`.
#[tokio::test]
async fn status_falls_through_to_env_api_url_when_toml_has_no_api() {
    let home = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_seed");

    // The CLI is invoked with `current_dir = <project>` and reads
    // `edge.toml` from there. Write a minimal `edge.toml` with NO
    // `[deployment].api` so the only source of the URL is the env var.
    let project = isolated_home();
    std::fs::write(
        project.path().join("edge.toml"),
        r#"[project]
name = "fallthrough"
version = "0.1.0"
target = "wasm32-wasip2"

[deployment]
"#,
    )
    .unwrap();
    std::fs::create_dir_all(project.path().join(".edge")).unwrap();
    std::fs::write(
        project.path().join(".edge").join("state.json"),
        r#"{"deployment_id":"d_xyz","app_name":"fallthrough","live_url":""}"#,
    )
    .unwrap();

    // Mount a mock at the env-var URL; the CLI must call this and not
    // the production `https://api.edgecloud.dev`. If the fall-through
    // regressed to the production URL, this mock would receive 0
    // requests and the test would time out on the `expect(1)` assertion
    // when checking received_requests.
    Mock::given(method("GET"))
        .and(path("/api/v1/status/d_xyz"))
        .and(header("Authorization", "Bearer k_seed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "d_xyz",
            "status": "ready",
            "created_at": "2026-06-19T00:00:00Z",
            "url": "https://fallthrough.example.com",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri());
    cmd.current_dir(project.path());
    cmd.arg("status");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("d_xyz"))
        .stdout(predicate::str::contains("ready"));
}

/// E: `--force` bypasses the F2 "saved key + EDGE_API_KEY" guard
/// AND the F2 "saved key" warning. A future refactor that drops
/// the `if !force` guard would fail this test.
#[tokio::test]
async fn signup_force_overwrites_saved_key_without_warning() {
    let home = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_old");

    Mock::given(method("POST"))
        .and(path("/api/v1/tenants"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "tenant_id": "t_xyz",
            "api_key": "k_new",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("signup")
        .arg("--name")
        .arg("test-user")
        .arg("--force");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("t_xyz"))
        // --force should NOT print the "already saved locally" warning.
        .stderr(predicate::str::contains("already saved locally").not());

    let stored = read_api_key(&home).expect("config should have a key");
    assert_eq!(stored, "k_new", "--force must overwrite the saved key");
}

/// E (bonus): without `--force`, signup with a saved key must warn
/// but still proceed and overwrite. This pins the F2 warn-then-proceed
/// branch — a future refactor that hard-fails on the warn would break
/// this test.
#[tokio::test]
async fn signup_warns_then_overwrites_when_saved_key_present() {
    let home = isolated_home();
    let server = MockServer::start().await;

    seed_api_key(&home, "k_old");

    Mock::given(method("POST"))
        .and(path("/api/v1/tenants"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "tenant_id": "t_xyz",
            "api_key": "k_new",
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("signup")
        .arg("--name")
        .arg("test-user");

    cmd.assert()
        .success()
        .stderr(predicate::str::contains("already saved locally"))
        .stdout(predicate::str::contains("t_xyz"));

    let stored = read_api_key(&home).expect("config should have a key");
    assert_eq!(
        stored, "k_new",
        "warn-then-proceed branch must still overwrite the saved key"
    );
}
