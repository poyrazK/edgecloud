//! Linker setup for both core wasm and component model.

use crate::EdgeRuntime;
use crate::RuntimeState;
use anyhow::Result;
use wasmtime::component::Linker as ComponentLinker;
use wasmtime::Engine;
#[cfg(feature = "wasi-preview1")]
use wasmtime::{Linker, Store};

/// Number of epoch ticks before a P1 guest is interrupted.
/// Matches the P2 worker default (`EPOCH_DEADLINE_TICKS=100`, `EPOCH_TICK_MS=10`),
/// giving each guest call a ~1 s CPU budget at the default tick rate.
#[cfg(feature = "wasi-preview1")]
const P1_EPOCH_DEADLINE_TICKS: u64 = 100;

/// Create a linker for core wasm modules (WASI Preview 1).
///
/// Registers all `wasi_snapshot_preview1` and `wasi_unstable` host functions so
/// that P1 modules can call them without trapping.  The store data type must be
/// [`WasiP1Ctx`]; build one with [`build_wasi_p1_ctx`].
#[cfg(feature = "wasi-preview1")]
pub fn create_linker(
    engine: &Engine,
) -> Result<Linker<wasmtime_wasi::preview1::WasiP1Ctx>> {
    let mut linker: Linker<wasmtime_wasi::preview1::WasiP1Ctx> = Linker::new(engine);
    wasmtime_wasi::preview1::add_to_linker_sync(&mut linker, |ctx| ctx)?;
    Ok(linker)
}

/// Build a [`WasiP1Ctx`] suitable for use as the store data with [`create_linker`].
///
/// stdout and stderr are inherited so guest output reaches the worker log.
/// stdin is left empty (EOF) — edge workers have no interactive input.
/// Env vars are filtered through the same blocklist applied to P2 guests
/// (`AWS_*`, `SECRET`, `API_KEY`, `DATABASE_URL`, etc.) before being
/// forwarded to the guest.
#[cfg(feature = "wasi-preview1")]
pub fn build_wasi_p1_ctx(
    env: &[(impl AsRef<str>, impl AsRef<str>)],
    args: &[impl AsRef<str>],
) -> wasmtime_wasi::preview1::WasiP1Ctx {
    use wasmtime_wasi::WasiCtxBuilder;

    // Filter on the key reference before cloning the value so blocked entries
    // (AWS_*, SECRET, etc.) never reach a heap allocation for their value.
    let filtered: Vec<(String, String)> = env
        .iter()
        .filter(|(k, _)| !crate::interfaces::process::is_blocked_env_key(k.as_ref()))
        .map(|(k, v)| (k.as_ref().to_owned(), v.as_ref().to_owned()))
        .collect();

    WasiCtxBuilder::new()
        .inherit_stdout()
        .inherit_stderr()
        .envs(&filtered)
        .args(args)
        .build_p1()
}

/// Create a [`Store`] for a WASI P1 module with memory limits and an epoch
/// deadline pre-configured.
///
/// Sets [`P1_EPOCH_DEADLINE_TICKS`] on the store so that a background thread
/// calling `engine.increment_epoch()` (e.g. the worker's epoch ticker) will
/// interrupt runaway guests. The deadline alone is not sufficient — a ticker
/// thread must also be running.
///
/// # Panics (debug)
/// Panics in debug builds when `max_memory_mb == 0`. Passing 0 would skip
/// the memory limiter entirely (see [`create_store`]); always supply an
/// explicit limit for P1 guests.
///
/// [`create_store`]: crate::store::create_store
#[cfg(feature = "wasi-preview1")]
pub fn create_p1_store(
    engine: &Engine,
    max_memory_mb: u64,
    ctx: wasmtime_wasi::preview1::WasiP1Ctx,
) -> Store<wasmtime_wasi::preview1::WasiP1Ctx> {
    debug_assert!(
        max_memory_mb > 0,
        "max_memory_mb=0 skips the memory limiter; specify an explicit limit for P1 guests"
    );
    let mut store = crate::store::create_store(engine, max_memory_mb, ctx);
    store.set_epoch_deadline(P1_EPOCH_DEADLINE_TICKS);
    store
}

/// Create a linker for WASI Preview 2 components.
/// WASI P2 and edge:* interfaces are wired via the bindgen-generated add_to_linker.
pub fn create_component_linker(engine: &Engine) -> Result<ComponentLinker<RuntimeState>> {
    let mut linker: ComponentLinker<RuntimeState> = ComponentLinker::new(engine);
    linker.allow_shadowing(true);

    EdgeRuntime::add_to_linker(&mut linker, |state: &mut RuntimeState| state)?;

    Ok(linker)
}

#[cfg(test)]
#[cfg(feature = "wasi-preview1")]
mod tests {
    use super::*;
    use crate::engine::create_engine;
    use wasmtime::Module;

    /// A P1 module whose sole import is `wasi_snapshot_preview1::proc_exit`.
    /// Before the fix, instantiation succeeded but the first call trapped with
    /// "unknown import".  After the fix it must NOT trap on "unknown import" —
    /// wasmtime surfaces proc_exit(0) as a clean `I32Exit(0)` trap instead.
    ///
    /// Skipped on Windows: wasmtime trap delivery triggers
    /// STATUS_STACK_BUFFER_OVERRUN in the Windows test runner.
    #[test]
    #[cfg_attr(
        windows,
        ignore = "wasmtime trap delivery triggers STATUS_STACK_BUFFER_OVERRUN on Windows"
    )]
    fn p1_module_does_not_trap_on_unknown_import() {
        let wat = r#"
            (module
              (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
              (func (export "_start") (call $exit (i32.const 0)))
            )
        "#;
        let engine = create_engine().expect("engine");
        let module = Module::new(&engine, wat::parse_str(wat).expect("wat")).expect("module");
        let linker = create_linker(&engine).expect("linker");
        let ctx = build_wasi_p1_ctx(&[] as &[(&str, &str)], &["test-program"]);
        let mut store = create_p1_store(&engine, 64, ctx);

        let instance = linker
            .instantiate(&mut store, &module)
            .expect("instantiate must succeed — all wasi imports are registered");

        let start = instance
            .get_typed_func::<(), ()>(&mut store, "_start")
            .expect("_start export");

        // proc_exit(0) causes wasmtime to surface an I32Exit trap, not
        // "unknown import". Verify the error is NOT about a missing import.
        let err = start.call(&mut store, ()).expect_err("proc_exit always traps");
        let msg = format!("{:?}", err).to_lowercase();
        assert!(
            !msg.contains("unknown import"),
            "got 'unknown import' trap — wasi host functions not wired: {msg}"
        );
    }

    /// Smoke test: env vars and args passed to build_wasi_p1_ctx reach the
    /// guest via fd_environ_get / args_get.  We verify indirectly by running a
    /// WAT module that calls args_sizes_get and checks the count is non-zero
    /// (we passed one arg above).
    ///
    /// Skipped on Windows: wasmtime's WASI signal handler setup triggers
    /// STATUS_STACK_BUFFER_OVERRUN in the Windows test runner.
    #[test]
    #[cfg_attr(
        windows,
        ignore = "wasmtime WASI signal handler triggers STATUS_STACK_BUFFER_OVERRUN on Windows"
    )]
    fn build_wasi_p1_ctx_passes_args() {
        let wat = r#"
            (module
              (import "wasi_snapshot_preview1" "args_sizes_get"
                (func $args_sizes_get (param i32 i32) (result i32)))
              (memory (export "memory") 1)
              (func (export "check_args") (result i32)
                ;; write argc into offset 0, argv_buf_size into offset 4
                (call $args_sizes_get (i32.const 0) (i32.const 4))
                drop
                ;; return argc (should be 1 — "my-app")
                (i32.load (i32.const 0))
              )
            )
        "#;
        let engine = create_engine().expect("engine");
        let module = Module::new(&engine, wat::parse_str(wat).expect("wat")).expect("module");
        let linker = create_linker(&engine).expect("linker");
        let ctx = build_wasi_p1_ctx(&[] as &[(&str, &str)], &["my-app"]);
        let mut store = create_p1_store(&engine, 64, ctx);

        let instance = linker.instantiate(&mut store, &module).expect("instantiate");
        let check = instance
            .get_typed_func::<(), i32>(&mut store, "check_args")
            .expect("check_args export");

        let argc = check.call(&mut store, ()).expect("call");
        assert_eq!(argc, 1, "expected 1 arg (my-app), got {argc}");
    }
}
