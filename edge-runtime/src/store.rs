//! wasmtime Store creation.

use crate::RuntimeState;
use wasmtime::{Engine, Store};

/// Create a wasmtime Store with memory limits enforced via StoreLimits.
///
/// Memory limits are configured via `RuntimeState::limits` (defaults to 1GB).
/// The store data type is currently narrowed to `RuntimeState` only.
pub fn create_store(engine: &Engine, data: RuntimeState) -> Store<RuntimeState> {
    let mut store = Store::new(engine, data);
    store.limiter(|state| &mut state.limits);
    store
}
