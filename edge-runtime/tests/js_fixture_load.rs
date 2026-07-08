//! Test: verify that the compiled JS hello-js component instantiates
//! successfully on the actual edgeCloud host runtime.

use edge_runtime::{
    create_component_linker_handler, create_engine, socket_egress::SocketEgressPolicy,
    EgressPolicy, RuntimeState,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use wasmtime::component::Component;

fn js_component_path() -> PathBuf {
    PathBuf::from("../samples/hello-js/target/wasm32-wasip2/release/hello-js.wasm")
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
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn js_component_instantiates_on_host() {
    let path = js_component_path();
    assert!(path.exists(), "hello-js.wasm component not found at {}. Build it first!", path.display());

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
