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

/// Path to the QuickJS-built `edge_js_runtime.wasm` artifact. By
/// default the shared cargo target dir is `$HOME/.cache/edgecloud-cargo`
/// (set in `.cargo/config.toml`); allow override via
/// `EDGE_JS_RUNTIME_WASM` for CI layouts.
fn edge_js_runtime_wasm_path() -> PathBuf {
    if let Ok(p) = std::env::var("EDGE_JS_RUNTIME_WASM") {
        return PathBuf::from(p);
    }
    let home = std::env::var("HOME").expect("HOME");
    let target = std::env::var("CARGO_TARGET_DIR")
        .unwrap_or_else(|_| format!("{home}/.cache/edgecloud-cargo"));
    PathBuf::from(format!(
        "{target}/wasm32-wasip1/release/edge_js_runtime.wasm"
    ))
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
            "SKIPPED: edge_js_runtime.wasm not found at {}. \
             Build with `cargo build --manifest-path edge-js-runtime/Cargo.toml --target wasm32-wasip1 --release` \
             and re-run, or set EDGE_JS_RUNTIME_WASM.",
            path.display()
        );
        return;
    }

    let engine = create_engine().expect("engine");
    let linker = create_component_linker_handler(&engine).expect("linker");
    let bytes = std::fs::read(&path).expect("read edge_js_runtime.wasm");
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
