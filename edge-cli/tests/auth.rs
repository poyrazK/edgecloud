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
use wiremock::matchers::{method, path};
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
/// On Linux we set `HOME` only — NOT `XDG_CONFIG_HOME`. If we set
/// `XDG_CONFIG_HOME=tempdir`, `dirs::config_dir()` returns the tempdir
/// directly (without the `.config` suffix), which then diverges from
/// the layout `config_file_for` expects and tests start reading/writing
/// at different paths. Leaving `XDG_CONFIG_HOME` unset lets
/// `dirs::config_dir()` fall back to the XDG default `$HOME/.config`,
/// matching `config_file_for`.
fn set_platform_env(cmd: &mut Command, home: &TempDir) {
    if cfg!(target_os = "windows") {
        cmd.env("APPDATA", home.path().join("AppData").join("Roaming"));
        cmd.env("USERPROFILE", home.path());
    } else {
        cmd.env("HOME", home.path());
    }
    // Always strip any host-process env vars that could shadow the test.
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
        .and(path("/api/tenants"))
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
        .and(path("/api/auth/whoami"))
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
        .and(path("/api/auth/whoami"))
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
        .and(path("/api/auth/whoami"))
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
