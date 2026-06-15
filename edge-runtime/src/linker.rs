//! Linker setup for both core wasm and component model.

use crate::RuntimeState;
use anyhow::Result;
use wasmtime::component::Linker as ComponentLinker;
use wasmtime::{Engine, Linker};

/// Create a linker for core wasm modules (WASI Preview 1).
pub fn create_linker(engine: &Engine) -> Result<Linker<()>> {
    let linker: Linker<()> = Linker::new(engine);
    Ok(linker)
}

/// Create a linker for WASI Preview 2 components.
/// WASI P2 and edge:* interfaces are wired via the bindgen-generated add_to_linker.
pub fn create_component_linker(engine: &Engine) -> Result<ComponentLinker<RuntimeState>> {
    let linker: ComponentLinker<RuntimeState> = ComponentLinker::new(engine);
    Ok(linker)
}
