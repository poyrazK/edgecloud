//! Memory enforcement via wasmtime's StoreLimitsBuilder.

use wasmtime::StoreLimitsBuilder;

/// Create a MemoryLimits configured for a given max memory in MB.
pub fn new_memory_limits(max_memory_mb: u64) -> wasmtime::StoreLimits {
    StoreLimitsBuilder::new()
        .memory_size((max_memory_mb * 1024 * 1024) as usize)
        .table_elements(100_000)
        .instances(1)
        .memories(1)
        .build()
}
