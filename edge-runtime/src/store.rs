//! wasmtime Store creation.

use wasmtime::{Engine, Store};

/// Create a wasmtime Store.
///
/// Memory limits are enforced at the OS level (cgroups) in the MVP.
/// A future version will add a proper ResourceLimiter implementation.
pub fn create_store<T>(engine: &Engine, _max_memory_mb: u64, data: T) -> Store<T> {
    Store::new(engine, data)
}
