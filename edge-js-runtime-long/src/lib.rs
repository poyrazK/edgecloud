//! edge-js-runtime-long: a QuickJS-based WebAssembly component that
//! exports the canonical `edge-runtime` (long-running) world for the
//! edgeCloud edge-worker.
//!
//! Issue #426: prior to this change, `edge-js-runtime` only exported
//! the `edge-runtime-handler` (FaaS) world. JS apps that needed
//! `wasi:sockets/tcp`, WebSocket listeners, scheduled jobs, or any
//! other long-lived behavior had to compile a separate Rust shim
//! (as `samples/hello-js-ws/` did). This crate adds a first-class
//! long-running cdylib to the workspace.
//!
//! **Build:**
//! ```text
//! cargo build --manifest-path edge-js-runtime-long/Cargo.toml \
//!   --target wasm32-wasip1 --release
//! ```
//! (with `EDGE_JS_BUNDLE=/path/to/esbuild-bundle.js` set in the env
//! if you want a real bundle embedded instead of the placeholder).
//!
//! The resulting `.wasm` exports `start: func()`. The worker detects
//! it as LongRunning (`edge-worker/src/detect.rs`) and dispatches via
//! `execute_app`, which already calls `start: func()` per PR #465.
//!
//! **Why a separate crate, not a `long-running` Cargo feature on
//! `edge-js-runtime`?** Each `wit_bindgen::generate!` invocation
//! produces a `wasi:cli/run` export (the `wasi:cli/command@0.2.1`
//! include requires it). Two such invocations in the same cdylib
//! collide on the `wasi:cli/run` symbol at link time. The sibling
//! crate is the cleanest fix — two cdylibs, two worlds, no collision.
//! `edge-js-runtime` (FaaS) and `edge-js-runtime-long` (LR) share
//! `compile_user_bundle` + `USER_BYTECODE` semantics and the
//! `register` body shape, but the bodies are duplicated (the
//! bindgen output is world-bound). See `register.rs` header for
//! the full rationale.
//!
//! **Why mirror `edge-js-runtime`'s bytecode cache?** `compile_user_bundle`
//! is a pure function of `Runtime` + the embedded `USER_JS`; the LR
//! cdylib has its own `USER_JS` and its own in-wasm QuickJS engine,
//! so it needs its own `USER_BYTECODE::OnceCell`. The implementation
//! is duplicated here (it cannot be imported from `edge-js-runtime`
//! because that crate's `wasm_only` is private).

#![cfg_attr(target_arch = "wasm32", no_main)]

// ─── Host-safe items (used by the wasm target only) ──────────────

/// The user's bundled JS, embedded at compile time by build.rs.
pub const USER_JS: &str = include_str!(concat!(env!("OUT_DIR"), "/bundle.js"));

/// Wrap USER_JS into a synthetic ES module so QuickJS's bytecode writer
/// has a module-form input. Mirrors `edge-js-runtime::wrap_as_module`.
///
/// The IIFE executes the user's bundle inside its scope (so any
/// closures/var declarations it makes stay reachable AND
/// `globalThis.start = ...` lands as a side effect), then re-exports
/// the assigned function so module consumers can find it.
///
/// The wrapper is harmless on the LR path — we don't actually need
/// the `export const handleRequest` re-export, but reusing the same
/// `USER_JS` form means both worlds can ship the same esbuild output
/// without a second `wrap_as_module` variant.
pub fn wrap_as_module(user_js: &str) -> String {
    let mut out = String::with_capacity(user_js.len() + 128);
    out.push_str("let __user = (function(){\n");
    out.push_str(user_js);
    out.push_str("\n})();\n");
    out.push_str("export const handleRequest = globalThis.handleRequest;\n");
    out
}

/// Defensive cap on the cached bytecode blob. The control plane already
/// enforces `MaxArtifactSize` (100 MiB) on the input; this is the
/// inner-side guardrail. ~10 MiB bytecode corresponds to ~7-8 MiB of
/// source — well above any reasonable esbuild bundle.
pub const MAX_BYTECODE_BYTES: usize = 10 * 1024 * 1024;

/// Lazily-compiled bytecode for the wrapped user bundle. The first
/// call to `start()` pays the lex+parse cost via `compile_user_bundle`;
/// subsequent calls in the same process (e.g. if the JS bundle is
/// re-evaluated after a `process.exit` + restart) replay the bytes
/// through `Module::load(&[u8])`.
///
/// **Lifetime invariant:** the bytes are version-tied to the QuickJS
/// engine compiled into this .wasm. They must NEVER be persisted to
/// disk or shared across wasm rebuilds — every new deployment ships a
/// fresh .wasm with a fresh `static`, so this is automatic in practice.
pub static USER_BYTECODE: once_cell::sync::OnceCell<Result<Vec<u8>, String>> =
    once_cell::sync::OnceCell::new();

/// Compile the wrapped user bundle to module-form bytecode.
///
/// The throwaway `Context` is dropped on return — only the `Vec<u8>`
/// we produce escapes. Errors are reported as strings so the caller
/// can surface them via `observe::emit_log` before `process::exit`.
pub fn compile_user_bundle(rt: &rquickjs::Runtime) -> Result<Vec<u8>, String> {
    let wrapped = wrap_as_module(USER_JS);
    let ctx = rquickjs::Context::full(rt).map_err(|e| format!("context: {e}"))?;
    ctx.with(|ctx| {
        let module = rquickjs::module::Module::declare(ctx.clone(), "user.js", wrapped.as_bytes())
            .map_err(|e| format!("declare: {e}"))?;
        module.write_le().map_err(|e| format!("write_le: {e}"))
    })
}

// ─── WASI bindings (wasm target only) ────────────────────────────
//
// Everything below references `wit_bindgen`-generated symbols and
// the `wasi:cli/command@0.2.1` include's exports. Gated behind
// `#[cfg(target_arch = "wasm32")]` so any host-target build (e.g.,
// `cargo check --manifest-path edge-js-runtime-long/Cargo.toml` for
// quick typechecking) skips the WASI-bound symbols and avoids the
// link-time `_wasi:cli/run@0.2.1` resolution.
#[cfg(target_arch = "wasm32")]
pub mod wasm_only {
    use rquickjs::{Object, Value};

    wit_bindgen::generate!({
        world: "edge-runtime",
        path: "../wit",
        generate_all,
    });

    // The world's `Guest` trait (for `start: func()`) is brought into
    // scope by `wit_bindgen::generate!` — see the `impl Guest for
    // LongJsHandler` below. The `wasi:cli/run` Guest is reached via
    // its fully-qualified `exports::wasi::cli::run::Guest` path
    // (same pattern as `samples/hello-js-ws/src/lib.rs:201`).

    // Re-export the bindgen-generated `edge:cloud/*` modules so the
    // sibling `register` module can `use
    // super::wasm_only::{kv_store, cache, ...};` without naming the
    // bindgen-internal `edge::cloud::` path. **Only** the `pub use`
    // — a non-`pub` `use self::edge::cloud::*` would shadow this
    // re-export with a private alias and break the sibling import
    // (rustc E0603 "module import is private"). The FaaS-side mirror
    // at `edge-js-runtime/src/lib.rs::wasm_only` follows the same
    // single-line pattern.
    pub use self::edge::cloud::{cache, kv_store, observe, process, scheduling, time, websocket};

    /// The long-running entry point. The supervisor calls
    /// `instance.get_typed_func::<(), ()>("start")` (PR #465 fix;
    /// the previous reactor-style `_start` is explicitly noted as
    /// legacy). Zero args, `()` return. Reads `EDGE_WS_PORT` from
    /// the process env (the worker allocates the port from its
    /// `PortPool` and threads it in via the per-app env — see
    /// `edge-worker/src/supervisor.rs`).
    pub fn run() {
        // Collect every failure into a single `Result<(), String>` so
        // the type system doesn't have to know about `process::exit`
        // returning `()` (which would otherwise break every `match`
        // arm that returns a value of type T). At the end we either
        // loop forever (the success path — `ctx.with` never returns
        // because the JS `start` is a synchronous infinite loop) or
        // call `process::exit(N)` with an `unreachable!()` to satisfy
        // the `-> ()` signature.
        let result: Result<(), String> = (|| -> Result<(), String> {
            // Read EDGE_WS_PORT from process env. Missing = supervisor
            // didn't set the sentinel; invalid = misconfigured deploy.
            let ws_port_str = process::get_env("EDGE_WS_PORT")
                .ok_or_else(|| "EDGE_WS_PORT not set".to_string())?;
            let ws_port: u16 = ws_port_str
                .parse()
                .map_err(|e| format!("EDGE_WS_PORT={ws_port_str:?} is not a valid u16: {e}"))?;

            // Build QuickJS runtime + full context once for the
            // lifetime of the process. The bytecode cache
            // (`USER_BYTECODE::get_or_init`) populates on first use
            // and persists across the LR loop.
            let rt = rquickjs::Runtime::new().map_err(|e| format!("runtime: {e}"))?;
            let ctx = rquickjs::Context::full(&rt).map_err(|e| format!("context: {e}"))?;

            ctx.with(|ctx| {
                // 1. Register edge:cloud modules on globalThis.EdgeCloud.
                super::register::register_all(&ctx).map_err(|e| format!("register: {e}"))?;

                // 2. Get cached bytecode (one-shot AOT compile on
                //    first use). Same SAFETY invariant as the FaaS
                //    path: the bytes are version-tied to the QuickJS
                //    engine compiled into this `.wasm`; any drift
                //    between producer + consumer engines surfaces as
                //    an `Err` on `Module::load`, not as UB.
                let bytecode: Vec<u8> = super::USER_BYTECODE
                    .get_or_init(|| super::compile_user_bundle(&rt))
                    .clone()
                    .map_err(|e| e.clone())?;
                if bytecode.len() > super::MAX_BYTECODE_BYTES {
                    return Err(format!("bytecode too large: {}", bytecode.len()));
                }

                // 3. Load cached bytecode → declared module.
                //
                // SAFETY: see #2.
                let module = unsafe { rquickjs::module::Module::load(ctx.clone(), &bytecode) }
                    .map_err(|e| format!("bytecode load: {e}"))?;

                // 4. Evaluate module. The bundle is expected to be
                //    IIFE-shaped (esbuild's default), wrapped as
                //    `let __user = (function(){...})();` by
                //    `wrap_as_module`. Bundles that use top-level-await
                //    are out of scope for the LR path (same limitation
                //    as the FaaS path; the wrap_as_module-then-eval
                //    pattern can't drive a pending-job queue).
                let promise = module.eval().map_err(|e| format!("module eval: {e}"))?;
                // Drop the promise — for the LR path we don't await
                // it (the IIFE form has already executed the user's
                // `globalThis.start = ...` assignment synchronously).
                drop(promise);

                // 5. Look up `globalThis.start` and invoke it with
                //    `{wsPort}`. The user's JS exports a synchronous
                //    `start({wsPort})` that loops forever (e.g. `for
                //    (;;) { ws.accept(); ... }`). Because the function
                //    never returns, control never reaches the end of
                //    this `ctx.with` closure — that's the design. If
                //    the user's `start` does return, we propagate the
                //    return value (a warning + clean exit code 0) up
                //    to the outer match.
                let start_fn: rquickjs::Function = ctx
                    .globals()
                    .get("start")
                    .map_err(|e| format!("globalThis.start not found: {e}"))?;

                // Build `{ wsPort }` as a JS object.
                let arg = Object::new(ctx.clone()).map_err(|e| format!("start arg obj: {e}"))?;
                arg.set("wsPort", ws_port)
                    .map_err(|e| format!("start.wsPort: {e}"))?;

                // Call start({wsPort}). The user's start is expected
                // to be synchronous and never return. If it does
                // return, propagate up — the outer match logs a
                // warning and exits cleanly with code 0.
                let _: Value = start_fn
                    .call::<_, Value>((arg,))
                    .map_err(|e| format!("globalThis.start threw: {e}"))?;

                // If the JS `start` returned (it shouldn't), surface
                // a sentinel error so the outer match logs it.
                Err("globalThis.start returned; long-running process exiting cleanly".to_string())
            })?;

            // Unreachable: the synchronous JS `start()` blocks the
            // QuickJS runtime forever inside `ctx.with`. The function
            // is `-> ()` so we still need a tail expression.
            //
            // TODO(#448-followup): once the JS bundling path can
            // produce a bundle whose `start()` resolves (e.g. via a
            // `Promise`-returning entrypoint driven by QuickJS's
            // pending-job queue), replace this `loop` with
            // `ctx.execute_pending_job()`-driven event-loop. Today
            // the synchronous-bundle contract is the only supported
            // shape, so a busy-wait is the correct placeholder.
            #[allow(unreachable_code, unused_variables)]
            loop {
                core::hint::spin_loop();
            }
            // Unreachable but needed for the type system.
            #[allow(unreachable_code)]
            Ok(())
        })();

        // If we got here (we shouldn't — the JS `start` is supposed
        // to loop forever), `result` carries either an Err or the
        // "start returned" sentinel. Log + exit cleanly.
        if let Err(e) = result {
            let (level, code) = if e.starts_with("globalThis.start returned") {
                ("warn", 0u32)
            } else {
                ("error", 1u32)
            };
            observe::emit_log(level, &format!("edge-js-runtime-long: {e}"), &[]);
            process::exit(code);
            unreachable!("wit-bindgen's process::exit returns ()")
        }
    }

    // The `start: func()` export required by the `edge-runtime` world
    // lives in the shim (`samples/hello-js-ws/src/lib.rs` and any
    // future `edge init --world=edge-runtime` scaffold), NOT here.
    // The shim owns its `wit_bindgen::generate!` (so it can pick the
    // world it targets and emit its own `start` symbol) and pulls
    // `register_all` from this crate's rlib. If this crate exported
    // `start` itself, two `start` symbols would land in the final
    // cdylib and rustc's wasm linker would error with
    // "Linking globals named 'start': symbol multiply defined".
    //
    // `run()` is still pub so a shim can call it as
    // `edge_js_runtime_long::wasm_only::run` and wire it into
    // its own `impl Guest for Shim { fn start() { run() } }`.
}

// LR-world `register_all` + per-namespace registrars. Mirrors
// `edge-js-runtime::register` but bound to this crate's bindgen
// output (the LR world's `edge::cloud::*`). Exposed as a public
// module so the long-running shim samples (`samples/hello-js-ws`,
// or any future edge-js-runtime-long host) can reuse the bindings
// without redefining the seven per-namespace registrars. The
// sibling-crate structure (issue #426) was driven by the bindgen
// `wasi:cli/run` symbol collision, so the bindgen output is unique
// to this crate — the registrars must live with the bindgen output
// they wrap. See `register.rs` header.
#[cfg(target_arch = "wasm32")]
pub mod register;

#[cfg(test)]
mod tests {
    use super::MAX_BYTECODE_BYTES;

    /// `MAX_BYTECODE_BYTES` is the inner-side guardrail on the cached
    /// user-bundle bytecode blob (see the const's doc comment). The
    /// control plane already caps the input artifact at
    /// `MaxArtifactSize` (100 MiB), so this is a defense-in-depth
    /// check inside the guest: it rejects a bundle whose compiled
    /// form blows past a sane memory budget. The cap lives inside
    /// the wasm guest's `Guest::start`, so we can't drive it from a
    /// host-side test without rebuilding the wasm with a lowered
    /// cap; this test pins the constant to a reasonable bound and
    /// catches a regression where the cap is set to 0 (would block
    /// every bundle) or `usize::MAX` (defeats the guardrail).
    #[test]
    fn max_bytecode_bytes_is_bounded() {
        assert!(
            MAX_BYTECODE_BYTES >= 1 * 1024 * 1024,
            "MAX_BYTECODE_BYTES = {MAX_BYTECODE_BYTES} is too low \
             (would block reasonable esbuild bundles)"
        );
        assert!(
            MAX_BYTECODE_BYTES <= 100 * 1024 * 1024,
            "MAX_BYTECODE_BYTES = {MAX_BYTECODE_BYTES} is too high \
             (defeats the inner-side guardrail)"
        );
    }
}
