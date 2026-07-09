//! M3.C6 — CLI tests for the `--language rust` flag.
//!
//! Verifies that:
//! - `--transform --language rust` emits wasi::socket calls
//! - `--analyze-json --language rust` produces a valid JSON report
//!   with the expected `TcpBind` detection
//! - `--tree --language rust` walks only `.rs` files
//! - the default language is `c` when `--language` is omitted
//! - an unknown `--language` value is rejected up front
//!
//! These tests do **not** talk to the network — they exercise local
//! analysis and transformation only. Network-based tests (e.g.
//! `edge-migrate --tree --language rust` actually POSTing to
//! `/api/migrate-tree`) belong in the Go control-plane suite.

use std::fs;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

/// Minimal tempdir helper for tests that need an actual directory
/// tree. Mirrors the pattern in `cli_tree_test.rs` so a follow-up
/// can lift it into a shared module if more tests need it.
struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        let path =
            std::env::temp_dir().join(format!("edge_migrate_rusttest_{}_{}_{}", label, pid, id));
        fs::create_dir_all(&path).expect("create tempdir");
        Self { path }
    }
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// `--transform --language rust <fixture>` should emit a Rust source
/// that begins with the WASI `use` block and contains the canonical
/// `TcpSocket::new(...).start_bind(...)...` rewrite for
/// `std::net::TcpListener::bind`. This is the load-bearing assertion
/// of M3 — it proves the bin dispatches the `--language` flag to
/// `RustTransformer` and the transformer actually rewrites.
#[test]
fn test_transform_rust_emits_wasi_socket_calls() {
    let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/http_server.rs");

    let output = Command::new(env!("CARGO_BIN_EXE_edge-migrate"))
        .arg("--language")
        .arg("rust")
        .arg("--transform")
        .arg(fixture)
        .output()
        .expect("failed to run --language rust --transform");

    assert!(
        output.status.success(),
        "--language rust --transform failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // The Rust transformer prepends a use block with the
    // `crate::wasi::sockets::tcp_create_socket::create_tcp_socket`
    // import (matching the wit-bindgen 0.45 binding tree; see issue
    // #417). The prelude always emits this exact import line — the
    // older `use wasi::socket::tcp::TcpSocket;` form is gone.
    assert!(
        stdout.contains("crate::wasi::sockets::tcp_create_socket::create_tcp_socket"),
        "expected crate::wasi::sockets::tcp_create_socket::create_tcp_socket import, got:\n{}",
        stdout
    );

    // The TcpBind rewrite now uses the bindgen-0.45 free-function
    // factory `create_tcp_socket(IpAddressFamily::Ipv4)` instead of
    // the older adapter-API `TcpSocket::new(AddressFamily::Ipv4)`.
    assert!(
        stdout.contains("create_tcp_socket("),
        "expected create_tcp_socket( emit, got:\n{}",
        stdout
    );
    assert!(
        stdout.contains("IpAddressFamily::Ipv4"),
        "expected IpAddressFamily::Ipv4 (bindgen-0.45 case) in emit, got:\n{}",
        stdout
    );
    assert!(
        stdout.contains(".start_bind("),
        "expected .start_bind( rewrite, got:\n{}",
        stdout
    );

    // The prelude now exposes parse_addr_v4 as an inline helper so
    // the address literal can be routed through it.
    assert!(
        stdout.contains("fn parse_addr_v4"),
        "expected parse_addr_v4 helper in prelude, got:\n{}",
        stdout
    );

    // TcpConnect (also auto-transformable) must appear too —
    // confirms the multi-match case is handled in a single pass.
    assert!(
        stdout.contains(".start_connect("),
        "expected .start_connect( rewrite, got:\n{}",
        stdout
    );
}

/// `--analyze-json --language rust` should emit valid JSON that
/// parses as a `MigrationReport` and carries the expected `TcpBind`
/// detection for `std::net::TcpListener::bind`. This is the
/// structured-data path the Go control plane consumes; the JSON
/// must be machine-parseable (single object, no leading log lines).
#[test]
fn test_analyze_json_rust_detects_listen() {
    let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/http_server.rs");

    let output = Command::new(env!("CARGO_BIN_EXE_edge-migrate"))
        .arg("--language")
        .arg("rust")
        .arg("--analyze-json")
        .arg(fixture)
        .output()
        .expect("failed to run --language rust --analyze-json");

    assert!(
        output.status.success(),
        "--language rust --analyze-json failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    assert!(
        trimmed.starts_with('{'),
        "expected JSON object, got: {}",
        trimmed
    );
    assert!(
        trimmed.ends_with('}'),
        "expected JSON object end, got: {}",
        trimmed
    );

    let v: serde_json::Value = serde_json::from_str(trimmed).expect("parse JSON");
    let patterns_detected = v
        .get("patterns_detected")
        .and_then(|p| p.as_array())
        .expect("patterns_detected must be an array");
    // TcpBind is the first match in http_server.rs.
    let has_tcp_bind = patterns_detected.iter().any(|p| {
        p.get("pattern")
            .and_then(|s| s.as_str())
            .map(|s| s.contains("TcpBind"))
            .unwrap_or(false)
    });
    assert!(
        has_tcp_bind,
        "expected a TcpBind detection in patterns_detected, got: {}",
        serde_json::to_string_pretty(&patterns_detected).unwrap()
    );
}

/// `--tree --language rust` should walk only `.rs` files. A mixed
/// project (`main.rs`, `helper.c`, `Makefile`) returns just
/// `main.rs`. We assert this by inspecting the local tree report
/// (printed before the upload attempt) — the upload step is short-
/// circuited because we set `EDGE_API_URL` to a port nothing
/// listens on and the test only checks stdout.
#[test]
fn test_tree_rust_walks_rs_only() {
    let dir = TempDir::new("tree_rust");
    fs::write(
        dir.path().join("main.rs"),
        "fn main() {\n    let _ = std::net::TcpListener::bind(\"127.0.0.1:80\");\n}\n",
    )
    .unwrap();
    fs::write(dir.path().join("ignored.c"), "// not a Rust file\n").unwrap();
    fs::write(dir.path().join("Makefile"), "# not Rust\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_edge-migrate"))
        .arg("--language")
        .arg("rust")
        .arg("--tree")
        .arg(dir.path())
        .arg("--app-name")
        .arg("rust-tree")
        .env("EDGE_API_URL", "http://127.0.0.1:1") // never called; the
        //                                  // tree report is printed
        //                                  // before the upload attempt.
        .env_remove("EDGE_API_KEY")
        .output()
        .expect("run");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // The walker picked up exactly one file (main.rs).
    assert!(
        stdout.contains("main.rs"),
        "expected main.rs in tree report, got stdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
    );
    assert!(
        !stdout.contains("ignored.c"),
        "walker must not include .c files in Rust mode, got stdout:\n{}",
        stdout
    );
    // The report's section header switches to "Rust std patterns"
    // (or "(Rust std patterns)" in the tree header).
    assert!(
        stdout.contains("Rust std patterns"),
        "expected 'Rust std patterns' header in Rust report, got:\n{}",
        stdout
    );
}

/// The default language is `c` when `--language` is omitted. We
/// prove this by running `--analyze-json` (no `--language`) on a C
/// fixture and asserting the JSON report mentions the C-specific
/// `SocketTcp` pattern (or any POSIX pattern).
#[test]
fn test_default_language_is_c() {
    let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/http_client.c");

    let output = Command::new(env!("CARGO_BIN_EXE_edge-migrate"))
        .arg("--analyze-json")
        .arg(fixture)
        .output()
        .expect("run --analyze-json (default language)");

    assert!(
        output.status.success(),
        "default-language --analyze-json failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).expect("parse JSON");
    let patterns_detected = v
        .get("patterns_detected")
        .and_then(|p| p.as_array())
        .expect("patterns_detected must be an array");
    assert!(
        !patterns_detected.is_empty(),
        "expected at least one POSIX pattern from the C fixture"
    );
    // The C fixture's first pattern is SocketTcp (the
    // `socket(AF_INET, SOCK_STREAM, 0)` call). The Debug-format
    // rendering of `PatternKind::Posix(SocketTcp)` is `Posix(SocketTcp)`,
    // so we use `contains` for the same reason the lib tests do.
    let has_socket_tcp = patterns_detected.iter().any(|p| {
        p.get("pattern")
            .and_then(|s| s.as_str())
            .map(|s| s.contains("SocketTcp") || s.contains("socket"))
            .unwrap_or(false)
    });
    assert!(
        has_socket_tcp,
        "expected a SocketTcp pattern in default-language report, got: {}",
        serde_json::to_string_pretty(&patterns_detected).unwrap()
    );
}

/// `--language python` (or any value other than `c` / `rust`)
/// should be rejected up front with a clear error message and a
/// non-zero exit code. The CLI must not silently fall back to the
/// C default — the developer has made an explicit (wrong) choice.
#[test]
fn test_unknown_language_rejected() {
    let output = Command::new(env!("CARGO_BIN_EXE_edge-migrate"))
        .arg("--language")
        .arg("python")
        .arg("--analyze-json")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../testdata/http_client.c"
        ))
        .output()
        .expect("run --language python");

    assert!(
        !output.status.success(),
        "expected non-zero exit for unknown language"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("invalid --language") || combined.contains("must be 'c' or 'rust'"),
        "expected clear error message; stdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
    );
}
