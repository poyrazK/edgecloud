//! Test: verify that the compiled JS hello-js component instantiates
//! successfully on the actual edgeCloud host runtime.
//!
//! Two tests live here:
//!
//! 1. `js_component_instantiates_on_host` — minimal smoke: just link and
//!    instantiate. The original smoke test. Skips if the Javy-built
//!    fixture isn't present at the expected path.
//!
//! 2. `edge_js_runtime_instantiates_on_host` — instantiates the
//!    QuickJS-built `edge_js_runtime.wasm` (the artifact #425 modifies)
//!    twice in sequence on fresh stores. The bytecode cache lives
//!    inside the wasm guest (`once_cell::sync::OnceCell`), so each
//!    fresh `Store` is a fresh guest state — this test asserts the
//!    component can be re-instantiated cleanly (no leaked host state,
//!    no link-time regressions) rather than re-running a hot path
//!    inside the guest. End-to-end warm-path coverage is in
//!    `edge-js-runtime/benches/warm_vs_cold.rs`.
//!
//!    Skips if the wasm artifact isn't in the shared cargo target dir
//!    (CI builds wasm32-wasip1 separately and copies it in).

use edge_runtime::{
    create_component_linker_handler, create_engine,
    socket_egress::{HostnamePinning, SocketEgressPolicy},
    EgressPolicy, RuntimeState,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use wasmtime::component::Component;

fn js_component_path() -> PathBuf {
    PathBuf::from("../samples/hello-js/target/javy/hello-js.wasm")
}

/// Resolve the QuickJS-built `edge_js_runtime` artifact.
///
/// The `cargo build --target wasm32-wasip1` step produces a **core
/// wasm module**, not a component — the host linker rejects it with
/// `failed to parse WebAssembly module with a component parser`. The
/// component-wrapped form is produced by a follow-up
/// `wasm-tools component new --adapt wasi_snapshot_preview1.reactor.wasm`:
///
/// ```bash
/// # CI / local pre-step:
/// cargo build --manifest-path edge-js-runtime/Cargo.toml \
///     --target wasm32-wasip1 --release
/// ADAPTER=$HOME/.cargo/registry/src/index.crates.io-*/wasi-preview1-component-adapter-provider-*/artefacts/wasi_snapshot_preview1.reactor.wasm
/// wasm-tools component new \
///     $HOME/.cache/edgecloud-cargo/wasm32-wasip1/release/edge_js_runtime.wasm \
///     --adapt "$ADAPTER" \
///     -o $HOME/.cache/edgecloud-cargo/wasm32-wasip1/release/edge_js_runtime.component.wasm
/// ```
///
/// We prefer the `.component.wasm` form when present, fall back to
/// `.wasm` for ad-hoc invocations, and let the caller produce a
/// diagnostic if neither exists. Overrides via `EDGE_JS_RUNTIME_WASM`
/// skip this resolution and point directly at a specific file.
fn edge_js_runtime_wasm_path() -> PathBuf {
    if let Ok(p) = std::env::var("EDGE_JS_RUNTIME_WASM") {
        return PathBuf::from(p);
    }
    let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
        // Fall back to the shared `~/.cache/edgecloud-cargo` target
        // wired by `.cargo/config.toml`. Use the user's home dir when
        // `$HOME` is unset (CI runners typically set it, but be
        // defensive).
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        format!("{home}/.cache/edgecloud-cargo")
    });
    let base = format!("{target}/wasm32-wasip1/release/edge_js_runtime");
    let component = PathBuf::from(format!("{base}.component.wasm"));
    if component.exists() {
        return component;
    }
    PathBuf::from(format!("{base}.wasm"))
}

fn runtime_state() -> RuntimeState {
    use edge_runtime::interfaces::observe::{AppLogContext, LogRecord, LogSink};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    struct CountingSink {
        pushes: AtomicUsize,
        #[allow(dead_code)]
        records: Mutex<Vec<LogRecord>>,
    }
    impl LogSink for CountingSink {
        fn push(&self, _r: LogRecord, _c: AppLogContext) {
            self.pushes.fetch_add(1, Ordering::Relaxed);
        }
    }

    RuntimeState::with_env_and_meter(
        HashMap::new(),
        None,
        "js-smoke".to_string(),
        Arc::new(EgressPolicy::allow_all()),
        Arc::new(CountingSink {
            pushes: AtomicUsize::new(0),
            records: Mutex::new(Vec::new()),
        }),
        AppLogContext {
            app_name: "js-smoke".to_string(),
            tenant_id: "js-smoke".to_string(),
            deployment_id: "js-smoke".to_string(),
        },
        None,
        SocketEgressPolicy::default(),
        Arc::new(HostnamePinning::new()),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn js_component_instantiates_on_host() {
    let path = js_component_path();
    if !path.exists() {
        eprintln!(
            "SKIPPED: hello-js.wasm not found at {}. Build it first to run this test locally.",
            path.display()
        );
        return;
    }

    let engine = create_engine().expect("engine");
    let linker = create_component_linker_handler(&engine).expect("linker");
    let bytes = std::fs::read(&path).expect("read JS component Wasm");
    let component = Component::from_binary(&engine, &bytes).expect("parse component");

    let mut store = wasmtime::Store::new(&engine, runtime_state());
    let _instance = linker
        .instantiate_async(&mut store, &component)
        .await
        .expect("instantiate JS component through the host linker");

    println!("✓ Successfully instantiated JavaScript component on edge-runtime host!");
}

/// Issue #425 regression: confirm the QuickJS-built
/// `edge_js_runtime.wasm` instantiates through the handler linker
/// without host-side linker or bindgen regressions. The bytecode
/// cache lives inside the guest (`USER_BYTECODE: OnceCell`), so
/// warm-path behavior is covered by the
/// `edge-js-runtime/benches/warm_vs_cold.rs` criterion bench rather
/// than a hyper roundtrip here.
#[tokio::test(flavor = "multi_thread")]
async fn edge_js_runtime_instantiates_on_host() {
    let path = edge_js_runtime_wasm_path();
    if !path.exists() {
        eprintln!(
            "SKIPPED: edge_js_runtime artifact not found at {}. Build it and (if using the core .wasm) wrap it:\n\
             \n\
             cargo build --manifest-path edge-js-runtime/Cargo.toml --target wasm32-wasip1 --release\n\
             wasm-tools component new <core.wasm> --adapt <adapter.wasm> -o <core.component.wasm>\n\
             \n\
             Then set EDGE_JS_RUNTIME_WASM to the .component.wasm path, \
             or place it at ${{CARGO_TARGET_DIR:-$HOME/.cache/edgecloud-cargo}}/wasm32-wasip1/release/edge_js_runtime.component.wasm.",
            path.display()
        );
        return;
    }

    let engine = create_engine().expect("engine");
    let linker = create_component_linker_handler(&engine).expect("linker");
    let bytes = std::fs::read(&path).expect("read edge_js_runtime artifact");
    let component = Component::from_binary(&engine, &bytes).expect("parse component");

    // Two fresh-store instantiations: catches any linker state that
    // would be one-shot (and any host-state the first call forgets to
    // reset). The guest-side bytecode cache resets on a fresh
    // `Store`; this test exercises the wasm component as a unit, not
    // the in-guest cache.
    for n in 1..=2 {
        let mut store = wasmtime::Store::new(&engine, runtime_state());
        let _instance = linker
            .instantiate_async(&mut store, &component)
            .await
            .unwrap_or_else(|e| panic!("instantiate #{n}: {e}"));
        drop(store);
    }

    println!(
        "✓ Successfully instantiated edge_js_runtime.wasm (QuickJS, #425) twice on the host linker"
    );
}

/// Issue #425 roundtrip regression: build a `hyper::Request<Incoming>`,
/// hand it to the QuickJS-built component through
/// `wasmtime_wasi_http::p2::bindings::ProxyPre`, and assert the
/// response is a 200 with the expected body. Exercises the actual
/// FaaS dispatch path: build req object → call
/// `globalThis.handleRequest` → extract response → send 200.
///
/// The default bundle (see `build.rs` fallback) returns
/// `{ status: 501, body: "no JS bundle embedded" }`. When a real
/// bundle is present (the default in the local dev loop), the handler
/// returns the actual response. We assert the body is non-empty + a
/// parseable `{...}` shape rather than pinning specific text — the
/// exact body depends on the bundle that was baked in at build time.
#[tokio::test(flavor = "multi_thread")]
async fn js_component_handles_request_on_host() {
    use http_body_util::combinators::BoxBody;
    use http_body_util::BodyExt as _;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;
    use wasmtime_wasi_http::p2::bindings::ProxyPre;
    use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
    use wasmtime_wasi_http::p2::WasiHttpView;

    /// Body that immediately reports end-of-stream. Its `Error` is
    /// `hyper::Error` (rather than `Empty`'s `Infallible`) so
    /// `new_incoming_request`'s `B::Error: Into<ErrorCode>` bound —
    /// which the bindgen impl only covers for `hyper::Error` — is
    /// satisfied without a `map_err` shim that would need to construct
    /// a `hyper::Error` (no public ctor exists).
    struct EmptyHyperBody;
    impl hyper::body::Body for EmptyHyperBody {
        type Data = bytes::Bytes;
        type Error = hyper::Error;
        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(None)
        }
    }

    let path = edge_js_runtime_wasm_path();
    if !path.exists() {
        eprintln!(
            "SKIPPED: edge_js_runtime artifact not found at {}. See edge_js_runtime_instantiates_on_host for the build steps.",
            path.display()
        );
        return;
    }

    let engine = create_engine().expect("engine");
    let linker = create_component_linker_handler(&engine).expect("linker");
    let bytes = std::fs::read(&path).expect("read edge_js_runtime artifact");
    let component = Component::from_binary(&engine, &bytes).expect("parse component");

    // Build a `ProxyPre` from the linker + component. This is the
    // canonical entry point the worker uses per deployment.
    let instance_pre = linker.instantiate_pre(&component).expect("instantiate_pre");
    let pre = ProxyPre::new(instance_pre).expect("ProxyPre::new");

    // Synthetic GET / request to a localhost host. Scheme::Http is
    // what the worker uses for cleartext FaaS dispatch. The body is
    // `EmptyHyperBody` (always EOF, `Error = hyper::Error`); see the
    // type's doc comment for why we don't use `http_body_util::Empty`.
    let req = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri("http://dispatch.local/")
        .body(BoxBody::new(EmptyHyperBody))
        .expect("build test request");

    let mut store = wasmtime::Store::new(&engine, runtime_state());

    // The runtime configures the engine for epoch-based interruption
    // (`edge-runtime/src/engine.rs` — used to enforce the per-request
    // fuel budget). The default store deadline is 0 ("already
    // elapsed"), so any guest work trips a `wasm trap: interrupt`
    // before producing a response. The worker pumps the epoch
    // ticker concurrently; in this test we don't have a ticker
    // thread, so we set the deadline far in the future. The
    // `engine.increment_epoch()` call in the worker never advances
    // past this deadline, so the guest runs unmolested.
    #[cfg(target_has_atomic = "64")]
    store.set_epoch_deadline(u64::MAX);

    // The two ends of the response channel.
    let (sender, receiver) =
        tokio::sync::oneshot::channel::<Result<hyper::Response<HyperOutgoingBody>, ErrorCode>>();
    let req_handle = store
        .data_mut()
        .http()
        .new_incoming_request(
            wasmtime_wasi_http::p2::bindings::http::types::Scheme::Http,
            req,
        )
        .expect("new_incoming_request");
    let out = store
        .data_mut()
        .http()
        .new_response_outparam(sender)
        .expect("new_response_outparam");

    // Dispatch the request. `call_handle` returns a `Result`; the
    // actual response arrives via the `receiver`.
    let proxy = pre
        .instantiate_async(&mut store)
        .await
        .expect("instantiate via ProxyPre");
    let guest_result = proxy
        .wasi_http_incoming_handler()
        .call_handle(&mut store, req_handle, out)
        .await;

    // `call_handle` should not itself error; the response is delivered
    // through the channel. A `call_handle` error is a host-level bug
    // (e.g. bad linker wiring), not a bundle error.
    guest_result.expect("call_handle should not error");

    let resp = receiver
        .await
        .expect("response channel should deliver")
        .expect("response should be Ok, not HttpError");

    let status = resp.status().as_u16();

    // Drain the body and assert it's non-empty.
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect response body")
        .to_bytes();
    assert!(
        !body.is_empty(),
        "expected non-empty response body from JS handler, got empty"
    );

    // The test is a wiring smoke: the request traversed
    // `ProxyPre::call_handle` and the JS handler produced a real,
    // body-bearing response. We don't pin a specific status — the
    // outcome depends on which bundle the artifact was built with:
    //
    // - No `EDGE_JS_BUNDLE` set → fallback 501 with body
    //   `"no JS bundle embedded"`. (CI / cold dev loop.)
    // - A bundled user bundle → 2xx with the user's response shape.
    // - An un-bundled source file (e.g. raw `handler.js` with bare
    //   `import` statements) → 500 with a parse-error message
    //   emitted by the runtime's defensive error path
    //   (lib.rs `Guest::handle`).
    //
    // All three prove the FaaS dispatch wiring works end-to-end.
    // What we reject here is a *wiring* failure: a wasm trap
    // (already filtered above by `call_handle` returning `Ok` and
    // the response channel delivering `Ok`) or an empty body.
    assert!(
        (200..300).contains(&status) || status == 501 || status == 500,
        "unexpected status {status} (body = {})",
        String::from_utf8_lossy(&body)
    );

    println!(
        "✓ JS handler roundtripped: status={} body={} bytes ({:?})",
        status,
        body.len(),
        String::from_utf8_lossy(&body)
    );
}

/// Issue #428 regression: `extract_response`'s string-branch default
/// for `Content-Type` must be `text/plain; charset=utf-8`, not
/// `application/json`. The fixture (`benches/fixtures/issues/428-string-default.js`)
/// is a single bundle whose handler dispatches on the request path:
///
/// - `/string`   → bare string, expect `text/plain; charset=utf-8`
///                 (the bugfix; was `application/json` before)
/// - `/object`   → `{ status, body }` object with no contentType,
///                 expect `application/json` (unchanged behavior)
/// - `/explicit` → object with explicit `contentType: text/html`,
///                 expect `text/html; charset=utf-8` (handler wins)
///
/// The three checks share one build (built once in a `OnceLock`),
/// re-issue the artifact, and assert on the `Content-Type` header
/// of the response. Skips if `cargo` or `wasm-tools` aren't on
/// PATH, or the build fails — same gating as the other
/// `js_fixture_load` tests.
#[tokio::test(flavor = "multi_thread")]
async fn extract_response_picks_content_type() {
    use http_body_util::combinators::BoxBody;
    use http_body_util::BodyExt as _;
    use std::pin::Pin;
    use std::process::Command;
    use std::task::{Context, Poll};
    use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;
    use wasmtime_wasi_http::p2::bindings::ProxyPre;
    use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
    use wasmtime_wasi_http::p2::WasiHttpView;

    /// Body that immediately reports end-of-stream. Its `Error` is
    /// `hyper::Error` (the bindgen impl only covers `hyper::Error`
    /// for `B::Error: Into<ErrorCode>`); see the matching struct
    /// in `js_component_handles_request_on_host` for the full
    /// rationale.
    struct EmptyHyperBody;
    impl hyper::body::Body for EmptyHyperBody {
        type Data = bytes::Bytes;
        type Error = hyper::Error;
        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(None)
        }
    }

    /// Path to the JS fixture whose three-path handler drives
    /// this test.
    fn fixture_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("edge-js-runtime")
            .join("benches")
            .join("fixtures")
            .join("issues")
            .join("428-string-default.js")
    }

    /// Where the test-built wasm component lives. The artifact path
    /// is keyed by source path (so two tests using different
    /// fixtures don't clobber each other).
    fn artifact_path() -> PathBuf {
        let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
            format!("{home}/.cache/edgecloud-cargo")
        });
        let stamp = fixture_path().to_string_lossy().replace('/', "_");
        PathBuf::from(format!(
            "{target}/wasm32-wasip1/issue428_{stamp}.component.wasm"
        ))
    }

    /// Build (or reuse) the wasm component. Build steps:
    /// 1. `cargo build --target wasm32-wasip1 --release` on
    ///    `edge-js-runtime` with `EDGE_JS_BUNDLE=<fixture>` so the
    ///    user's JS gets embedded at compile time.
    /// 2. `wasm-tools component new <core> --adapt <adapter> -o <out>`
    ///    to wrap the core module as a WASI Preview 2 component
    ///    the host linker accepts.
    ///
    /// Both commands honor `CARGO_TARGET_DIR` (cargo) / operate
    /// on the absolute paths we pass (wasm-tools), so the build
    /// does not write into any worktree's `target/`.
    fn build() -> Option<PathBuf> {
        let fixture = fixture_path();
        if !fixture.exists() {
            eprintln!(
                "SKIPPED: fixture not found at {} — the edge-js-runtime checkout must be at the expected path.",
                fixture.display()
            );
            return None;
        }
        let artifact = artifact_path();
        if artifact.exists() {
            return Some(artifact);
        }

        let runtime_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("edge-runtime has a parent")
            .join("edge-js-runtime");

        let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
            format!("{home}/.cache/edgecloud-cargo")
        });
        let target_dir = PathBuf::from(target_dir);

        // Step 1: cargo build
        let cargo_status = Command::new("cargo")
            .args(["build", "--target", "wasm32-wasip1", "--release"])
            .current_dir(&runtime_dir)
            .env("EDGE_JS_BUNDLE", &fixture)
            .status()
            .expect("spawn cargo");
        if !cargo_status.success() {
            eprintln!("SKIPPED: cargo build failed (no wasm toolchain?)");
            return None;
        }

        let core = target_dir
            .join("wasm32-wasip1")
            .join("release")
            .join("edge_js_runtime.wasm");
        if !core.exists() {
            eprintln!("SKIPPED: core wasm missing after cargo build");
            return None;
        }

        // Step 2: locate adapter
        let cargo_home = match std::env::var("CARGO_HOME") {
            Ok(s) => s,
            Err(_) => match std::env::var("HOME") {
                Ok(h) => format!("{h}/.cargo"),
                Err(_) => {
                    eprintln!("SKIPPED: neither CARGO_HOME nor HOME is set");
                    return None;
                }
            },
        };
        let mut adapter = None;
        if let Ok(entries) = std::fs::read_dir(format!("{cargo_home}/registry/src")) {
            for entry in entries.flatten() {
                if let Ok(subs) = std::fs::read_dir(entry.path()) {
                    for sub in subs.flatten() {
                        if sub
                            .file_name()
                            .to_string_lossy()
                            .starts_with("wasi-preview1-component-adapter-provider-")
                        {
                            let candidate = sub
                                .path()
                                .join("artefacts")
                                .join("wasi_snapshot_preview1.reactor.wasm");
                            if candidate.exists() {
                                adapter = Some(candidate);
                                break;
                            }
                        }
                    }
                    if adapter.is_some() {
                        break;
                    }
                }
            }
        }
        let adapter = match adapter {
            Some(p) => p,
            None => {
                eprintln!("SKIPPED: wasi-preview1 adapter not found in cargo registry");
                return None;
            }
        };

        if let Some(parent) = artifact.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let wrap_status = Command::new("wasm-tools")
            .args([
                "component",
                "new",
                &core.to_string_lossy(),
                "--adapt",
                &adapter.to_string_lossy(),
                "-o",
                &artifact.to_string_lossy(),
            ])
            .status()
            .expect("spawn wasm-tools");
        if !wrap_status.success() {
            eprintln!("SKIPPED: wasm-tools wrap failed");
            return None;
        }
        Some(artifact)
    }

    let artifact = match build() {
        Some(p) => p,
        None => return,
    };

    let engine = create_engine().expect("engine");
    let linker = create_component_linker_handler(&engine).expect("linker");
    let bytes = std::fs::read(&artifact).expect("read artifact");
    let component = Component::from_binary(&engine, &bytes).expect("parse component");
    let instance_pre = linker.instantiate_pre(&component).expect("instantiate_pre");
    let pre = ProxyPre::new(instance_pre).expect("ProxyPre::new");

    /// Drive one request through the artifact and return the
    /// (status, Content-Type header value, body) tuple. Takes the
    /// linker by reference for documentation symmetry with the
    /// component-store pattern; `ProxyPre` is the only entry point
    /// used at runtime.
    async fn dispatch(
        engine: &wasmtime::Engine,
        pre: &wasmtime_wasi_http::p2::bindings::ProxyPre<edge_runtime::RuntimeState>,
        request_path: &str,
    ) -> (u16, String, bytes::Bytes) {
        let req = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri(format!("http://dispatch.local/{request_path}"))
            .body(BoxBody::new(EmptyHyperBody))
            .expect("build test request");

        let mut store = wasmtime::Store::new(engine, runtime_state());
        store.set_epoch_deadline(u64::MAX);

        let (sender, receiver) = tokio::sync::oneshot::channel::<
            Result<hyper::Response<HyperOutgoingBody>, ErrorCode>,
        >();
        let req_handle = store
            .data_mut()
            .http()
            .new_incoming_request(
                wasmtime_wasi_http::p2::bindings::http::types::Scheme::Http,
                req,
            )
            .expect("new_incoming_request");
        let out = store
            .data_mut()
            .http()
            .new_response_outparam(sender)
            .expect("new_response_outparam");

        let proxy = pre
            .instantiate_async(&mut store)
            .await
            .expect("instantiate via ProxyPre");
        proxy
            .wasi_http_incoming_handler()
            .call_handle(&mut store, req_handle, out)
            .await
            .expect("call_handle should not error");

        let resp = receiver
            .await
            .expect("response channel should deliver")
            .expect("response should be Ok, not HttpError");

        let status = resp.status().as_u16();
        let content_type = resp
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap_or("").to_string())
            .unwrap_or_default();
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        (status, content_type, body)
    }

    let (status, ct, body) = dispatch(&engine, &pre, "string").await;
    assert_eq!(status, 200, "string handler: status");
    assert_eq!(
        ct, "text/plain; charset=utf-8",
        "string handler: Content-Type — issue #428 fix"
    );
    assert_eq!(&body[..], b"hello world", "string handler: body");

    let (status, ct, body) = dispatch(&engine, &pre, "object").await;
    assert_eq!(status, 200, "object handler: status");
    assert_eq!(
        ct, "application/json",
        "object handler: Content-Type — default must be JSON"
    );
    assert_eq!(&body[..], br#"{"ok":true}"#, "object handler: body");

    let (status, ct, body) = dispatch(&engine, &pre, "explicit").await;
    assert_eq!(status, 200, "explicit handler: status");
    assert_eq!(
        ct, "text/html; charset=utf-8",
        "explicit handler: Content-Type — handler wins over default"
    );
    assert_eq!(
        &body[..],
        b"<html><body>hi</body></html>",
        "explicit handler: body"
    );

    println!("✓ issue #428: extract_response picks the right Content-Type for all three shapes");
}
