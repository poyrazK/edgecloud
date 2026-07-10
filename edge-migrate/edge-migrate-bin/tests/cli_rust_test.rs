//! M3.C6 — CLI tests for the `--language rust` flag.
//!
//! Verifies that:
//! - `--transform --language rust` emits wasi::socket calls
//! - `--analyze-json --language rust` produces a valid JSON report
//!   with the expected `TcpBind` detection
//! - `--tree --language rust` walks only `.rs` files
//! - the default language is `c` when `--language` is omitted
//! - an unknown `--language` value is rejected up front
//! - the transformer's emit compiles end-to-end to a wasi:http@0.2.1
//!   component under `wit-bindgen = "0.45"` (issue #417 load-bearing
//!   regression — pins that the transformer stays in sync with the
//!   bindgen-generated binding tree)
//!
//! Most tests do **not** talk to the network — they exercise local
//! analysis and transformation only. Network-based tests (e.g.
//! `edge-migrate --tree --language rust` actually POSTing to
//! `/api/migrate-tree`) belong in the Go control-plane suite. The
//! end-to-end build test does shell out to `cargo` + `wasm-tools`
//! locally; it skips on CI without the full toolchain.

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

/// Locate `wasm32-unknown-unknown` and `wasm-tools` on the test host.
/// Returns `None` if either is missing — the test then skips rather
/// than fails, mirroring the Go control-plane's `skipIfNoWasmTools`
/// helper so CI hosts without the full toolchain don't turn red.
fn e2e_toolchain() -> Option<(String, String)> {
    let rustup_out = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()
        .ok()?;
    if !rustup_out.status.success() {
        return None;
    }
    let installed = String::from_utf8_lossy(&rustup_out.stdout);
    if !installed
        .lines()
        .any(|l| l.trim() == "wasm32-unknown-unknown")
    {
        return None;
    }
    // Confirm cargo + wasm-tools are on PATH. cargo is required
    // implicitly by env!("CARGO") in the build step below; we
    // re-check here so the skip message is precise.
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    if Command::new(&cargo).arg("--version").output().is_err() {
        return None;
    }
    let wasm_tools = Command::new("wasm-tools")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|_| "wasm-tools".to_string())?;
    Some((cargo, wasm_tools))
}

/// End-to-end transformer compile check (issue #417).
///
/// **What this pins:** the transformer's emit stays in lock-step with
/// the canonical `wit-bindgen = "0.45"` binding tree generated from
/// the repo's `wit/` tree. Every plausible regression — a future
/// transform edit that drifts back to `TcpSocket::new(...)` form, or
/// a wit-bindgen upgrade that renames a submodule — fails this test
/// loudly.
///
/// **How it works:**
/// 1. Run `edge-migrate --transform --language rust` on the canonical
///    `udp_bind.rs` fixture (covers UdpBind — the smallest pattern
///    that exercises the full bind factory + start_bind + finish_bind
///    chain). We use `udp_bind.rs` rather than `http_server.rs`
///    because the latter calls `listener.accept()` after the bind,
///    and the bindgen-generated `accept` signature is
///    `(TcpSocket, InputStream, OutputStream)` — a 3-tuple mismatch
///    with the fixture's `(stream, _)` 2-tuple. The transform
///    replaces only the bind site; the accept site is left verbatim.
///    Switching to UdpBind sidesteps the bindgen tuple-shape
///    mismatch while still exercising the load-bearing emit shapes.
/// 2. Splice the transformer's prelude + body into a `cdylib`
///    cargo project that also calls `wit_bindgen::generate!({ world:
///    "edge-runtime-handler" })` — that's what produces the
///    `crate::wasi::*` module tree the transformer references.
/// 3. Run `cargo build --target wasm32-unknown-unknown --release`.
/// 4. Run `wasm-tools component new` and inspect the WIT — assert
///    `wasi:http@0.2.1` shows up in the export list.
///
/// **Skips** on hosts without the full toolchain (`wasm32-unknown-
/// unknown` not installed, or no `wasm-tools`). The Go control-plane
/// has a parallel end-to-end test that does the same wrap — this one
/// pins the Rust side independently so a transformer regression
/// shows up here, not just on the Go side.
#[test]
fn test_transform_rust_emits_compilable_wit_bindgen_0_45_code() {
    let (cargo, wasm_tools) = match e2e_toolchain() {
        Some(t) => t,
        None => {
            eprintln!("skipping: wasm32-unknown-unknown / cargo / wasm-tools not all available");
            return;
        }
    };

    // Step 1: transform the canonical fixture via the bin (so we
    // exercise the bin→library dispatch path, not just the lib).
    let fixture = concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/udp_bind.rs");
    let transform_out = Command::new(env!("CARGO_BIN_EXE_edge-migrate"))
        .args(["--language", "rust", "--transform", fixture])
        .output()
        .expect("failed to invoke edge-migrate --transform");
    assert!(
        transform_out.status.success(),
        "--transform failed: {}",
        String::from_utf8_lossy(&transform_out.stderr)
    );
    let transformed = String::from_utf8(transform_out.stdout).expect("stdout is utf-8");

    // Step 2: build a synthetic cargo project that links the
    // transformer's emit into a real `wit_bindgen::generate!`-produced
    // crate. The macro is what produces `crate::wasi::*`, which is
    // the import shape the transformer's prelude references. The
    // `cdylib` crate-type + `edge-runtime-handler` world match the
    // shape the Go control plane writes at
    // `edge-control-plane/internal/service/migration.go::compileRustAsComponent`.
    let dir = TempDir::new("rust_e2e");
    fs::create_dir_all(dir.path().join("src")).expect("mkdir src");

    // WIT path — the canonical tree at the repo root. Hardcoded
    // relative to the worktree so the test pins the same WIT the
    // runtime uses. (CARGO_MANIFEST_DIR is
    // edge-migrate/edge-migrate-bin, so ../../wit is the root.)
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let wit_path = std::path::Path::new(manifest_dir)
        .join("../../wit")
        .canonicalize()
        .expect("canonicalize wit dir");
    let wit_path_str = wit_path.to_str().expect("wit path is utf-8");

    let cargo_toml = "[package]\n\
         name = \"rust_migrate_e2e\"\n\
         version = \"0.1.0\"\n\
         edition = \"2021\"\n\
         \n\
         [lib]\n\
         crate-type = [\"cdylib\"]\n\
         \n\
         [dependencies]\n\
         wit-bindgen = \"0.45\"\n\
         \n\
         [profile.release]\n\
         opt-level = \"s\"\n\
         lto = true\n\
         codegen-units = 1\n";
    fs::write(dir.path().join("Cargo.toml"), cargo_toml).expect("write Cargo.toml");

    // The harness stubs out wasi:cli/run (no-op) and the HTTP handler
    // (no-op) so wit_bindgen::generate! has something to attach to.
    // The transformer's emit goes into a `#[allow(dead_code)] mod`
    // so it's clearly marked as test-only — the emit is typecheck-only
    // (it would panic at runtime without preopens); issue #417 scope
    // is typecheck + load-only.
    let mut lib = String::new();
    lib.push_str("#![no_main]\n\n");
    lib.push_str(&format!(
        "wit_bindgen::generate!({{\n\
         \x20\x20\x20\x20world: \"edge-runtime-handler\",\n\
         \x20\x20\x20\x20path: {wit_path_str:?},\n\
         \x20\x20\x20\x20generate_all,\n\
         }});\n\n\
         use crate::exports::wasi::http::incoming_handler::Guest;\n\
         use crate::wasi::http::types::{{\n\
         \x20\x20\x20\x20Fields, IncomingRequest, OutgoingResponse, ResponseOutparam,\n\
         }};\n\n\
         struct Comp;\n\
         export!(Comp);\n\n\
         impl crate::exports::wasi::cli::run::Guest for Comp {{\n\
         \x20\x20\x20\x20fn run() -> Result<(), ()> {{ Err(()) }}\n\
         }}\n\n\
         impl Guest for Comp {{\n\
         \x20\x20\x20\x20fn handle(_req: IncomingRequest, _out: ResponseOutparam) {{}}\n\
         }}\n\n\
         // The transformer's emit goes into its own module so its\n\
         // `fn main()` doesn't collide with the lib's #[no_main]\n\
         // entry point. We allow dead_code because the transform\n\
         // emits the full prelude + body, which includes symbols\n\
         // (create_udp_socket, Descriptor, etc.) that this particular\n\
         // fixture doesn't reference. The runtime semantics are out\n\
         // of scope for issue #417; only the typecheck is load-bearing.\n\
         #[allow(dead_code)]\n\
         mod migrated {{\n"
    ));
    lib.push_str(&transformed);
    lib.push_str("\n}\n");

    fs::write(dir.path().join("src/lib.rs"), lib).expect("write lib.rs");

    // Step 3: cargo build. The `cargo` invocation inherits the test
    // process's PATH and env, so `rustup target add wasm32-unknown-
    // unknown` and `wit-bindgen` 0.45 registry access must already
    // work on the host. We pass `--manifest-path` so we don't have
    // to cd into the tempdir.
    let build_out = Command::new(&cargo)
        .args(["build", "--manifest-path"])
        .arg(dir.path().join("Cargo.toml"))
        .args(["--target", "wasm32-unknown-unknown", "--release"])
        // Force a per-test target dir. Without this, the
        // workspace's `target-dir = "../target-cache/edgecloud"`
        // (set in `.cargo/config.toml` for the cross-worktree
        // shared cache) interprets the path relative to the
        // synthetic manifest's directory, putting the output
        // outside the test's tempdir — so the post-build glob
        // misses. Setting CARGO_TARGET_DIR scopes the build to
        // the tempdir and cleans up via TempDir's Drop impl.
        .env("CARGO_TARGET_DIR", dir.path().join("target"))
        .output()
        .expect("cargo build");
    if !build_out.status.success() {
        // Dump the generated lib.rs so a CI failure has a useful
        // breadcrumb. Without this the only signal is rustc's
        // cryptic "unexpected closing delimiter".
        eprintln!(
            "--- generated lib.rs ({} bytes) ---\n{}\n--- end lib.rs ---",
            fs::metadata(dir.path().join("src/lib.rs"))
                .map(|m| m.len())
                .unwrap_or(0),
            fs::read_to_string(dir.path().join("src/lib.rs")).unwrap_or_default()
        );
    }
    assert!(
        build_out.status.success(),
        "cargo build failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&build_out.stdout),
        String::from_utf8_lossy(&build_out.stderr),
    );

    let core_wasm = dir
        .path()
        .join("target")
        .join("wasm32-unknown-unknown")
        .join("release")
        .join("librust_migrate_e2e.rlib");
    // cdylib output is `.so`-named on Linux, `.rlib`-shaped on
    // macOS. We use a glob via `ls` to find the actual output name
    // — easier than uname-gating.
    let out_dir = dir
        .path()
        .join("target")
        .join("wasm32-unknown-unknown")
        .join("release");
    let entries = fs::read_dir(&out_dir).expect("read target dir");
    let core_wasm = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().and_then(|s| s.to_str()) == Some("wasm"))
        .unwrap_or_else(|| {
            panic!(
                "no .wasm output in {}; entries: {:?}",
                out_dir.display(),
                core_wasm
            )
        });

    // Step 4: wrap as a component. Newer wasm-tools (1.x) doesn't
    // take a --world flag — the wit-bindgen-embedded metadata is
    // sufficient. Older wasm-tools (pre-1.x) wanted --world; the
    // test silently passes either way because the embedded metadata
    // already names the world.
    let component_path = dir.path().join("migrated.wasm");
    let wrap_out = Command::new(&wasm_tools)
        .args(["component", "new"])
        .arg(&core_wasm)
        .args(["-o"])
        .arg(&component_path)
        .output()
        .expect("wasm-tools component new");
    assert!(
        wrap_out.status.success(),
        "wasm-tools component new failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&wrap_out.stdout),
        String::from_utf8_lossy(&wrap_out.stderr),
    );

    // Inspect the component's WIT spec. We require `wasi:http/types@0.2.1`
    // (the version wasmtime 45.0.3 expects) and forbid 0.2.4 (the
    // version rustc 1.93.0's bundled adapter pins — the bug PR #414
    // fixed). This catches both regressions: the transformer going
    // back to the broken adapter API, and a future rustc upgrade
    // re-bundling 0.2.4.
    let wit_out = Command::new(&wasm_tools)
        .args(["component", "wit"])
        .arg(&component_path)
        .output()
        .expect("wasm-tools component wit");
    assert!(
        wit_out.status.success(),
        "wasm-tools component wit failed: {}",
        String::from_utf8_lossy(&wit_out.stderr)
    );
    let spec = String::from_utf8_lossy(&wit_out.stdout);

    assert!(
        spec.contains("wasi:http/types@0.2.1"),
        "expected wasi:http/types@0.2.1 in component spec; got:\n{}",
        spec
    );
    assert!(
        !spec.contains("wasi:http/types@0.2.4"),
        "component still references wasi:http/types@0.2.4 — the transformer emit regressed:\n{}",
        spec
    );

    // Confirm we got back a component (not a core module) by
    // checking that the wasm-tools `component wit` decoder accepted
    // it. A core module would fail the decode with "unknown
    // version" or similar before printing the WIT, which is what
    // the `wit_out.status.success()` check above already enforces
    // implicitly. (We don't add a magic-byte check — wasm 1.0 core
    // modules and components share the same `\x00asm\x01\x00\x00\x00`
    // header; the distinguishing bytes live deeper in the file.)
}
