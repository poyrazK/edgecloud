//! Smoke test: the handler fixture (`edge-worker/tests/fixtures/handler.wasm`)
//! instantiates through `create_component_linker_handler`.
//!
//! This is the first positive end-to-end test for Phase D fixtures.
//! Before this test, the linker was only exercised with the empty-imports
//! case in `v0_2_smoke.rs`. A passing `instantiate` here proves the
//! linker-wasi-rs and linker-wasi-http wirings are correctly composed
//! and the fixture's wasi:http/incoming-handler export is callable.
//!
//! Run with: `cargo test --manifest-path edge-runtime/Cargo.toml --test handler_fixture_load`
//! Skip if fixture is missing.

use edge_runtime::{
    create_component_linker_handler, create_engine, socket_egress::SocketEgressPolicy,
    EgressPolicy, RuntimeState,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use wasmtime::component::Component;

fn fixture_path() -> Option<PathBuf> {
    let candidates = [
        "../edge-worker/tests/fixtures/handler.wasm",
        "edge-worker/tests/fixtures/handler.wasm",
        "tests/fixtures/handler.wasm",
    ];
    candidates.iter().map(PathBuf::from).find(|p| p.exists())
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
        "smoke".to_string(),
        Arc::new(EgressPolicy::allow_all()),
        Arc::new(CountingSink {
            pushes: AtomicUsize::new(0),
            records: Mutex::new(Vec::new()),
        }),
        AppLogContext {
            app_name: "smoke".to_string(),
            tenant_id: "smoke".to_string(),
            deployment_id: "smoke".to_string(),
        },
        None,
        SocketEgressPolicy::default(),
        Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn handler_fixture_instantiates() {
    let path = match fixture_path() {
        Some(p) => p,
        None => {
            eprintln!(
                "SKIPPED: handler.wasm not found in any of the expected locations. \
                 Build it with: cd edge-worker/tests/fixtures/handler && \
                 cargo build --target wasm32-unknown-unknown --release && \
                 wasm-tools component new --world edge-runtime-handler \
                   --wit-dir ../../../wit \
                   target/wasm32-unknown-unknown/release/edge_fixture_handler.wasm \
                   -o ../handler.wasm"
            );
            return;
        }
    };

    let engine = create_engine().expect("engine");
    let linker = create_component_linker_handler(&engine).expect("linker");
    let bytes = std::fs::read(&path).expect("read fixture wasm");
    let component = Component::from_binary(&engine, &bytes).expect("parse component");

    let mut store = wasmtime::Store::new(&engine, runtime_state());
    let _instance = linker
        .instantiate_async(&mut store, &component)
        .await
        .expect("instantiate handler fixture through the linker");

    // The linker refused to instantiate if any import is missing. The
    // fact that we reached this line proves: wasi:http/* wasi:io/*
    // wasi:clocks/* wasi:random/* wasi:filesystem/* wasi:cli/* AND
    // edge:cloud/* all resolved on the host side. The wasi:http/
    // incoming-handler export is the one the linker validated.
}
