//! wasmtime Engine creation with security configuration.

use anyhow::Result;
use wasmtime::{Config, Engine};

/// Create a wasmtime Engine with security-hardened configuration.
pub fn create_engine() -> Result<wasmtime::Engine> {
    let mut config = Config::new();

    // Security: disable features that expand attack surface
    config.wasm_threads(false);

    // Reference types MUST be enabled for compatibility with
    // `wasm32-unknown-unknown` components produced via the
    // `wasm-tools component new` workflow (Phase D fix). The compiled
    // core wasm uses multi-byte LEB128 zero encoding for memory
    // indices in bulk-memory instructions (`memory.copy`, `memory.fill`,
    // etc.). With reference types disabled, the wasmtime parser runs in
    // "single-memory" mode and rejects multi-byte zeros at those
    // positions with `Invalid input WebAssembly code at offset N:
    // zero byte expected`. Reference types was historically disabled
    // for defense-in-depth, but the bulk-memory instructions it gates
    // are required for any modern toolchain.
    config.wasm_reference_types(true);

    // Performance: enable SIMD
    config.wasm_simd(true);

    // Required for WASI Preview 2 / component model
    config.wasm_component_model(true);

    // Enable epoch interruption for CPU time limits
    config.epoch_interruption(true);

    // Async support is now unconditional in wasmtime 36+ (the
    // `async_support(true)` call was deprecated and removed in 45).
    // The `async` + `component-model` features on the `wasmtime`
    // dependency enable everything `wasmtime_wasi::p2::add_to_linker_async`
    // and `wasmtime_wasi_http::p2::add_only_http_to_linker_async` need.

    let engine = Engine::new(&config)?;
    Ok(engine)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::RuntimeState;
    use crate::store::create_store;

    #[test]
    fn create_engine_succeeds() {
        let engine = create_engine().expect("engine");
        drop(engine);
    }

    #[test]
    fn engine_is_cloneable() {
        let engine = create_engine().expect("engine");
        let _clone = engine.clone();
    }

    #[test]
    fn engine_shares_across_stores() {
        let engine = create_engine().expect("engine");
        let state1 = RuntimeState::new();
        let state2 = RuntimeState::new();
        let store1 = create_store(&engine, 0, state1);
        let store2 = create_store(&engine, 0, state2);
        drop(store1);
        drop(store2);
    }

    #[tokio::test(flavor = "current_thread")]
    #[cfg_attr(
        windows,
        ignore = "wasmtime epoch signals trigger STATUS_STACK_BUFFER_OVERRUN on Windows"
    )]
    async fn epoch_interruption_wired() {
        let engine = create_engine().expect("engine");
        let state = RuntimeState::new();
        let mut store = create_store(&engine, 0, state);

        // store.set_epoch_deadline requires epoch_interruption(true) on the
        // engine config — if the call succeeds, the config took effect.
        store.set_epoch_deadline(1);
    }

    #[test]
    fn component_model_enabled() {
        let engine = create_engine().expect("engine");

        let component_wat = r#"
            (component
              (core module
                (func (export "run") (result i32)
                  i32.const 42
                )
              )
              (core instance (instantiate 0))
            )
        "#;

        let bytes = wat::parse_str(component_wat).expect("valid component wat");
        let _component = wasmtime::component::Component::new(&engine, bytes)
            .expect("component model must be enabled on engine");
    }
}
