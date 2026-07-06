//! Memory access helpers for crossing the wasm boundary.

use anyhow::Result;
use wasmtime::{Caller, Memory, Val};

/// Read a UTF-8 string from wasm linear memory.
pub fn read_string<T>(memory: &Memory, caller: &Caller<'_, T>, ptr: i32, len: i32) -> String {
    let data = memory.data(caller);
    let ptr = ptr as usize;
    let len = len as usize;

    if ptr > data.len() || ptr + len > data.len() {
        return String::new();
    }

    String::from_utf8_lossy(&data[ptr..ptr + len]).into_owned()
}

/// Write a string to wasm linear memory, returning (ptr, len).
pub fn write_string<T>(memory: &Memory, caller: &mut Caller<'_, T>, s: &str) -> Result<(i32, i32)> {
    let ptr = allocate(memory, caller, s.len() as i32)?;
    memory.write(caller, ptr as usize, s.as_bytes())?;
    Ok((ptr as i32, s.len() as i32))
}

/// Allocate `size` bytes in wasm linear memory, returning the pointer.
pub fn allocate<T>(memory: &Memory, caller: &mut Caller<'_, T>, size: i32) -> Result<i32> {
    if let Some(export) = caller.get_export("allocate") {
        if let Some(func) = export.into_func() {
            let mut results = [Val::I32(0)];
            func.call(caller, &[Val::I32(size)], &mut results)?;
            return Ok(results[0].i32().unwrap_or(0));
        }
    }

    let current_len = memory.data(&*caller).len();
    let new_len = current_len.saturating_add(size as usize);
    let pages = (new_len as u64).div_ceil(65536);
    memory.grow(caller, pages)?;
    Ok(current_len as i32)
}

/// Re-fetch the memory export from a caller (requires mutable Caller).
/// Call AFTER any wasm execution — do NOT cache the Memory handle
/// across wasm calls because memory.grow() invalidates the handle.
#[inline]
pub fn get_memory<T>(caller: &mut Caller<'_, T>, name: &str) -> Option<Memory> {
    caller.get_export(name)?.into_memory()
}

/// Read raw bytes from wasm linear memory.
pub fn read_bytes<T>(memory: &Memory, caller: &Caller<'_, T>, ptr: i32, len: i32) -> Vec<u8> {
    let data = memory.data(caller);
    let ptr = ptr as usize;
    let len = len as usize;

    if ptr > data.len() || ptr + len > data.len() {
        return Vec::new();
    }

    data[ptr..ptr + len].to_vec()
}

/// Write raw bytes to wasm linear memory, returning (ptr, len).
pub fn write_bytes<T>(
    memory: &Memory,
    caller: &mut Caller<'_, T>,
    bytes: &[u8],
) -> Result<(i32, i32)> {
    let ptr = allocate(memory, caller, bytes.len() as i32)?;
    memory.write(caller, ptr as usize, bytes)?;
    Ok((ptr as i32, bytes.len() as i32))
}

#[cfg(test)]
mod tests {
    // Memory access functions (read_string, read_bytes, allocate, etc.)
    // take &Caller which can only be constructed inside a wasm host
    // function context. Unit tests for these require a full wasm
    // component — see edge-runtime/tests/ for integration coverage.
}
