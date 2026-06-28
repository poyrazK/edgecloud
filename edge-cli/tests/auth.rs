//! Integration tests for the `edge auth` subcommand group.
//!
//! Uses `wiremock` for the control plane, `assert_cmd` to drive the
//! `edge` binary, and a `HOME` override (via `dirs::config_dir()`) to
//! isolate the config file per-test.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

/// Read the config file and parse out `default.api_key` (if any).
fn read_api_key(home: &TempDir) -> Option<String> {
    let path = common::config_file_for(home);
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
    let home = common::isolated_home();
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
    common::set_platform_env(&mut cmd, &home);
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
    let home = common::isolated_home();

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.arg("auth").arg("login").arg("--key").arg("k_from_flag");

    // Login also tries to call whoami at the end. We don't mount a
    // server here, so it should fail gracefully (warning, not error)
    // and the local save should still succeed.
    cmd.assert().success();

    let stored = read_api_key(&home).expect("config file should exist");
    assert_eq!(stored, "k_from_flag");
}

/// Default path: stdin read with echo on (issue #108). The --no-echo
/// path is covered by `login_no_echo_*` tests below.
#[test]
fn login_from_stdin_writes_to_config() {
    let home = common::isolated_home();

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.arg("auth").arg("login").write_stdin("k_from_stdin\n");

    cmd.assert().success();

    let stored = read_api_key(&home).expect("config file should exist");
    assert_eq!(stored, "k_from_stdin");
}

/// `--no-echo` requires a controlling TTY. Without one (e.g. under
/// `assert_cmd`, in a CI job with no TTY allocation), the secure read
/// path must fail with a clear error AND must NOT write a (possibly
/// empty) key to disk. The no-write assertion is the load-bearing
/// platform-independent check; the exit-code assertion is secondary.
///
/// We deliberately do not assert on a specific stderr substring:
/// `rpassword`'s `io::Error` text varies by OS (`os error 6`,
/// `os error 25`, etc.). The "no config file written" assertion is
/// the only platform-independent proof that the secure read path
/// was actually taken — if a buggy implementation silently fell
/// back to stdin, it would either succeed (no TTY mock) or write
/// something to disk.
#[test]
fn login_no_echo_without_tty_fails_clearly_and_does_not_write_key() {
    let home = common::isolated_home();

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    // No `--key`, so the no_echo branch is taken; rpassword reads
    // from /dev/tty which is unavailable under assert_cmd.
    cmd.arg("auth")
        .arg("login")
        .arg("--no-echo")
        .write_stdin("k_x\n");

    cmd.assert().failure();

    // The load-bearing assertion: nothing was written to disk.
    // Platform-independent — works on linux/macOS/Windows.
    assert!(
        read_api_key(&home).is_none(),
        "--no-echo path must not write a key when /dev/tty is unavailable"
    );
}

/// `--no-echo --key <KEY>` must save the explicit key, identical to
/// `--key` alone. `--no_echo` is a no-op when `--key` is provided
/// (no read happens), so this pins the contract that the flag does
/// not affect the explicit-key path. Mirrors `login_with_key_flag_writes_to_config`
/// above to make the no-op relationship explicit.
#[test]
fn login_no_echo_with_explicit_key_still_writes_to_config() {
    let home = common::isolated_home();

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.arg("auth")
        .arg("login")
        .arg("--no-echo")
        .arg("--key")
        .arg("k_explicit");

    cmd.assert().success();

    let stored = read_api_key(&home).expect("config file should exist");
    assert_eq!(stored, "k_explicit");
}

#[tokio::test]
async fn whoami_prints_tenant_info() {
    let home = common::isolated_home();
    let server = MockServer::start().await;

    // Pre-seed the config so the client has a key.
    common::seed_api_key(&home, "k_seed");

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
    common::set_platform_env(&mut cmd, &home);
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
    let home = common::isolated_home();
    common::seed_api_key(&home, "k_to_remove");

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.arg("auth").arg("logout");

    cmd.assert().success();
    assert!(
        read_api_key(&home).is_none(),
        "api_key should be removed from config after logout"
    );
}

#[test]
fn logout_is_idempotent_when_no_key() {
    let home = common::isolated_home();
    // No config file exists.

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.arg("auth").arg("logout");

    cmd.assert().success();
}

/// F1: a key the server rejects (401) must exit non-zero, but the
/// rejected key is still on disk so the user can re-run `login` to
/// overwrite it. Verifies the post-save verification now treats
/// credential rejection as a hard error rather than a soft warning.
#[tokio::test]
async fn login_rejects_bad_key_exits_one_keeps_saved_key() {
    let home = common::isolated_home();
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/auth/whoami"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid key"))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
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

    let home = common::isolated_home();
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
    common::set_platform_env(&mut cmd, &home);
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
    let home = common::isolated_home();
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v1/tenants"))
        .respond_with(ResponseTemplate::new(400).set_body_string("invalid plan"))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
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

#[tokio::test]
async fn keys_create_prints_token_and_does_not_overwrite_saved_key() {
    let home = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_existing");

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
    common::set_platform_env(&mut cmd, &home);
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
    let home = common::isolated_home();
    // No seed_api_key call → no key on disk, no EDGE_API_KEY env.
    // The CLI must refuse to try to authenticate against /api/keys.

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
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
    let home = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_existing");

    Mock::given(method("POST"))
        .and(path("/api/v1/keys"))
        .respond_with(ResponseTemplate::new(400).set_body_string("invalid role"))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
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

#[tokio::test]
async fn keys_list_prints_table_with_current_marker() {
    let home = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_existing");

    // whoami identifies the current key (matches the seeded one) so
    // the matching row in the GET /api/v1/keys response should be
    // annotated with "(current)".
    Mock::given(method("GET"))
        .and(path("/api/v1/auth/whoami"))
        .and(header("Authorization", "Bearer k_existing"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant_id": "t_seed",
            "tenant_name": "Seed",
            "plan": "free",
            "api_key_id": "k_existing",
            "api_key_name": "default",
            "role": "developer",
            "created_at": "2026-06-20T00:00:00Z",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v1/keys"))
        .and(header("Authorization", "Bearer k_existing"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "k_existing",
                "name": "default",
                "role": "developer",
                "created_at": "2026-06-20T00:00:00Z",
            },
            {
                "id": "k_other",
                "name": "ci-deploy",
                "role": "viewer",
                "created_at": "2026-06-22T00:00:00Z",
            },
        ])))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("keys")
        .arg("list");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("k_existing"))
        .stdout(predicate::str::contains("k_other"))
        .stdout(predicate::str::contains("default"))
        .stdout(predicate::str::contains("ci-deploy"))
        .stdout(predicate::str::contains("developer"))
        .stdout(predicate::str::contains("viewer"))
        .stdout(predicate::str::contains("(current)"));
}

#[tokio::test]
async fn keys_list_json_emits_raw_array() {
    let home = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_existing");

    // whoami may or may not be called depending on the path the code
    // takes; mock both so either is fine.
    Mock::given(method("GET"))
        .and(path("/api/v1/auth/whoami"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant_id": "t_seed",
            "tenant_name": "Seed",
            "plan": "free",
            "api_key_id": "k_existing",
            "api_key_name": "default",
            "role": "developer",
            "created_at": "2026-06-20T00:00:00Z",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v1/keys"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "id": "k_a",
                "name": "alpha",
                "role": "developer",
                "created_at": "2026-06-20T00:00:00Z",
            },
            {
                "id": "k_b",
                "name": "beta",
                "role": "viewer",
                "created_at": "2026-06-22T00:00:00Z",
            },
        ])))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("keys")
        .arg("list")
        .arg("--json");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains(r#""id": "k_a""#))
        .stdout(predicate::str::contains(r#""id": "k_b""#))
        .stdout(predicate::str::contains(r#""role": "viewer""#))
        // Table header should NOT be present in --json mode.
        .stdout(predicate::str::contains("ID").not());
}

#[tokio::test]
async fn keys_list_without_saved_key_exits_non_zero() {
    let home = common::isolated_home();
    // No seed_api_key → no key on disk, no EDGE_API_KEY env.
    // The CLI must refuse to try to authenticate against /api/v1/keys.

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.arg("auth").arg("keys").arg("list");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("API key not found"));
}

/// Shared wiremock setup for `keys_revoke` tests: seeds a saved key,
/// mounts `GET /api/v1/auth/whoami` returning `whoami_id`, and mounts
/// `GET /api/v1/keys` returning `keys`. Returns the mock server.
async fn setup_revoke_mocks(
    home: &TempDir,
    whoami_id: &str,
    keys: serde_json::Value,
) -> MockServer {
    let server = MockServer::start().await;
    common::seed_api_key(home, "k_existing");

    Mock::given(method("GET"))
        .and(path("/api/v1/auth/whoami"))
        .and(header("Authorization", "Bearer k_existing"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant_id": "t_seed",
            "tenant_name": "Seed",
            "plan": "free",
            "api_key_id": whoami_id,
            "api_key_name": "default",
            "role": "developer",
            "created_at": "2026-06-20T00:00:00Z",
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/v1/keys"))
        .respond_with(ResponseTemplate::new(200).set_body_json(keys))
        .mount(&server)
        .await;

    server
}

#[tokio::test]
async fn keys_revoke_by_id_sends_delete_with_bearer() {
    let home = common::isolated_home();
    let keys = serde_json::json!([
        {
            "id": "k_other",
            "name": "ci-deploy",
            "role": "viewer",
            "created_at": "2026-06-22T00:00:00Z",
        },
    ]);
    let server = setup_revoke_mocks(&home, "k_existing", keys).await;

    Mock::given(method("DELETE"))
        .and(path("/api/v1/keys/k_other"))
        .and(header("Authorization", "Bearer k_existing"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("keys")
        .arg("revoke")
        .arg("--id")
        .arg("k_other")
        .arg("--yes");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Revoked key k_other"));
}

#[tokio::test]
async fn keys_revoke_self_refuses_without_force() {
    let home = common::isolated_home();
    // whoami reports k_existing — the same key the CLI is using —
    // so the self-revoke guard must fire before any DELETE goes out.
    let keys = serde_json::json!([
        {
            "id": "k_existing",
            "name": "default",
            "role": "developer",
            "created_at": "2026-06-20T00:00:00Z",
        },
    ]);
    let server = setup_revoke_mocks(&home, "k_existing", keys).await;

    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("keys")
        .arg("revoke")
        .arg("--id")
        .arg("k_existing")
        .arg("--yes");

    cmd.assert()
        .failure()
        .code(2)
        .stderr(predicate::str::contains("refusing to revoke"));
}

#[tokio::test]
async fn keys_revoke_self_proceeds_with_force() {
    let home = common::isolated_home();
    let keys = serde_json::json!([
        {
            "id": "k_existing",
            "name": "default",
            "role": "developer",
            "created_at": "2026-06-20T00:00:00Z",
        },
    ]);
    let server = setup_revoke_mocks(&home, "k_existing", keys).await;

    Mock::given(method("DELETE"))
        .and(path("/api/v1/keys/k_existing"))
        .and(header("Authorization", "Bearer k_existing"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("keys")
        .arg("revoke")
        .arg("--id")
        .arg("k_existing")
        .arg("--force")
        .arg("--yes");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Revoked key k_existing"));
}

#[tokio::test]
async fn keys_revoke_404_surfaces_in_stderr() {
    let home = common::isolated_home();
    let keys = serde_json::json!([
        {
            "id": "k_other",
            "name": "ci-deploy",
            "role": "viewer",
            "created_at": "2026-06-22T00:00:00Z",
        },
    ]);
    let server = setup_revoke_mocks(&home, "k_existing", keys).await;

    Mock::given(method("DELETE"))
        .and(path("/api/v1/keys/k_missing"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .expect(1)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("keys")
        .arg("revoke")
        .arg("--id")
        .arg("k_missing")
        .arg("--yes");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("keys revoke failed"))
        .stderr(predicate::str::contains("404"));
}

#[tokio::test]
async fn keys_revoke_without_yes_in_non_tty_refuses_with_clear_error() {
    // assert_cmd runs the child without a controlling TTY, so
    // stderr.is_terminal() inside the CLI returns false. The TTY
    // gate must fire BEFORE any prompt is read and BEFORE the DELETE
    // is sent — refusing is friendlier than silently bypassing the
    // confirmation in a non-interactive shell (CI, pipes, heredocs).
    let home = common::isolated_home();
    let keys = serde_json::json!([
        {
            "id": "k_other",
            "name": "ci-deploy",
            "role": "viewer",
            "created_at": "2026-06-22T00:00:00Z",
        },
    ]);
    let server = setup_revoke_mocks(&home, "k_existing", keys).await;

    // No DELETE mock. If the gate fires correctly, the child exits
    // before sending any DELETE; if it does not, the unmounted
    // route would panic the test (or a leftover 404 mock would
    // give a misleading error message — neither is desired).
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
    cmd.env("EDGE_API_URL", server.uri())
        .arg("auth")
        .arg("keys")
        .arg("revoke")
        .arg("--id")
        .arg("k_other");
    // Note: no --yes, no --force.

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("pass --yes"));
}

/// D: when `edge.toml` `[deployment]` has no `api` key, the runtime
/// must fall through to `EDGE_API_URL`. This pins the end-to-end
/// behavior of the 7 call-site updates that switched from
/// `edge_toml.deployment.api.clone()` to `edge_toml.api_url(...)`.
#[tokio::test]
async fn status_falls_through_to_env_api_url_when_toml_has_no_api() {
    let home = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_seed");

    // The CLI is invoked with `current_dir = <project>` and reads
    // `edge.toml` from there. Write a minimal `edge.toml` with NO
    // `[deployment].api` so the only source of the URL is the env var.
    let project = common::isolated_home();
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
    common::set_platform_env(&mut cmd, &home);
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
    let home = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_old");

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
    common::set_platform_env(&mut cmd, &home);
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
    let home = common::isolated_home();
    let server = MockServer::start().await;

    common::seed_api_key(&home, "k_old");

    Mock::given(method("POST"))
        .and(path("/api/v1/tenants"))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "tenant_id": "t_xyz",
            "api_key": "k_new",
        })))
        .mount(&server)
        .await;

    let mut cmd = Command::cargo_bin("edge-cli").unwrap();
    common::set_platform_env(&mut cmd, &home);
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
