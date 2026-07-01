//! Linker setup for both core wasm and component model.

use crate::EdgeRuntime;
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
    let mut linker: ComponentLinker<RuntimeState> = ComponentLinker::new(engine);
    linker.allow_shadowing(true);

    // Wire wasi:filesystem, wasi:io, wasi:clocks, wasi:random into the linker
    // before the custom edge:* interfaces so that allow_shadowing(true) lets
    // edge:* re-export any overlapping names cleanly.
    #[cfg(feature = "filesystem")]
    wasmtime_wasi::add_to_linker_sync(&mut linker)?;

    EdgeRuntime::add_to_linker(&mut linker, |state: &mut RuntimeState| state)?;

    Ok(linker)
}
