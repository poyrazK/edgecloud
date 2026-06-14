//! wasmtime Engine creation with security configuration.

use anyhow::Result;
use wasmtime::{Config, Engine};

/// Create a wasmtime Engine with security-hardened configuration.
pub fn create_engine() -> Result<wasmtime::Engine> {
    let mut config = Config::new();

    // Security: disable features that expand attack surface
    config.wasm_threads(false);
    config.wasm_reference_types(false);

    // Performance: enable SIMD
    config.wasm_simd(true);

    // Required for WASI Preview 2 / component model
    config.wasm_component_model(true);

    // Enable epoch interruption for CPU time limits
    config.epoch_interruption(true);

    let engine = Engine::new(&config)?;
    Ok(engine)
}
