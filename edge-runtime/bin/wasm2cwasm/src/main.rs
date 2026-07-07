//! wasm2cwasm — compile a .wasm component to a pre-compiled .cwasm file.
//!
//! Usage: wasm2cwasm <input.wasm> <output.cwasm>
//!
//! Reads a WASM component binary from the given path, compiles it with
//! the same wasmtime Engine configuration used in production (via
//! `edge_runtime::create_engine`), and writes the serialized component
//! to the output path. The output can be loaded later via
//! `unsafe { Component::deserialize(&engine, &bytes) }` in the worker
//! supervisor or dispatch, avoiding the JIT compilation cost.
//!
//! Exit codes:
//!   0 — success
//!   1 — argument error (wrong number of args, missing file)
//!   2 — compilation or serialization failure

use std::fs;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: wasm2cwasm <input.wasm> <output.cwasm>");
        return ExitCode::from(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    let wasm_bytes = match fs::read(input_path) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("error: failed to read input {input_path}: {e}");
            return ExitCode::from(1);
        }
    };

    let engine = match edge_runtime::create_engine() {
        Ok(engine) => engine,
        Err(e) => {
            eprintln!("error: failed to create wasmtime engine: {e}");
            return ExitCode::from(2);
        }
    };

    let component = match wasmtime::component::Component::from_binary(&engine, &wasm_bytes) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: compilation failed: {e}");
            return ExitCode::from(2);
        }
    };

    let cwasm_bytes = match component.serialize() {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("error: serialization failed: {e}");
            return ExitCode::from(2);
        }
    };

    if let Err(e) = fs::write(output_path, &cwasm_bytes) {
        eprintln!("error: failed to write output {output_path}: {e}");
        return ExitCode::from(2);
    }

    eprintln!(
        "compiled {} ({} bytes → {} bytes)",
        input_path,
        wasm_bytes.len(),
        cwasm_bytes.len()
    );
    ExitCode::SUCCESS
}
