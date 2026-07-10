//! `hello-js-ws` — long-running JS shim for the issue #448 e2e test.
//!
//! Pairs a small Rust cdylib (this file) with an esbuild-bundled JS
//! (`src/handler.js`) so the JS WebSocket binding can be exercised
//! end-to-end against a real `edge-worker`. The shim's `start` entry
//! reads `EDGE_WS_PORT` from the process env, builds a QuickJS
//! runtime in-process, reuses `edge_js_runtime_long::register::register_all`
//! to wire the seven `edge:cloud/*` namespaces onto
//! `globalThis.EdgeCloud`, evaluates the embedded JS once, and calls
//! `globalThis.start({ wsPort })`.
//!
//! ## Why a thin shim (issue #426)
//!
//! The shim used to duplicate ~570 lines of `register_*` helpers
//! that the FaaS `edge-js-runtime` already owned. Issue #426 lifts
//! the long-running path into a first-class cdylib
//! (`edge-js-runtime-long/`) — a separate crate because two
//! `wit_bindgen::generate!` invocations in the same cdylib collide
//! on `wasi:cli/run` (issue #426 notes). The per-namespace
//! registrars moved into `edge-js-runtime-long/src/register.rs` so
//! the LR shim and any future LR host can both reuse them. The
//! shim here is now ~30 lines of world-binding wiring: read env,
//! build QuickJS, `register_all(&ctx)`, evaluate the bundle, call
//! `globalThis.start({wsPort})`.
//!
//! ## Why this stays alive
//!
//! The block that runs the user JS is `ctx.with(|ctx| ...)` (see the
//! `start` impl below). `Context::with` blocks the current thread
//! for the duration of the closure, and the user bundle's `start()`
//! is a synchronous `for(;;)` that never returns — so the closure
//! never returns, which is exactly the long-running shape we want.
//! After `ctx.with` the trailing `loop { core::hint::spin_loop() }`
//! is unreachable in practice; it exists only because the `start`
//! export's signature is `fn start() -> ()` and Rust requires a tail
//! expression. (rquickjs 0.9 has no `Runtime::idle()`; the legacy JS
//! binding (#422, edge-js-runtime) does not use one either — the
//! synchronous bundle is the LR-model norm. A future async-aware
//! bundle would need an event loop driven by
//! `ctx.execute_pending_job()`.)
//!
//! ## Why use the canonical `edge-runtime` world
//!
//! The canonical `wit/edge-cloud.wit::edge-runtime` world
//! (`wit/edge-cloud.wit:124-148`) declares a top-level `export start:
//! func();` (issue #448) — the long-running entry point the
//! supervisor calls via `instance.get_typed_func("start")` at
//! `edge-worker/src/supervisor.rs:1920`. We generate bindings against
//! that canonical world (no local WIT copy) so:
//!   - `samples/hello-js-ws/wit/` doesn't need to exist (one source
//!     of truth for the WIT surface).
//!   - The bindgen produces the `edge::cloud::*` module path with the
//!     right `Host`/`Guest` shape, which a custom local world does
//!     not.
//!   - The component's top-level `start` export matches the canonical
//!     world's declaration.
//!
//! ## Build
//!
//! ```sh
//! cd samples/hello-js-ws
//! npm install
//! npx esbuild src/handler.js --bundle --format=iife --platform=neutral \
//!   --outfile=.edge/bundle.js
//! EDGE_JS_BUNDLE=$PWD/.edge/bundle.js cargo build --target wasm32-wasip1 --release
//! ```
//!
//! Then `wasm-tools component new <core> --adapt
//! ../../edge-cli/adapters/wasi_snapshot_preview1.reactor.wasm -o
//! <out>` wraps the core module into a component the edge-worker can
//! instantiate. The adapter is vendored in-repo (issue #423; SHA-256
//! pinned in `edge-cli/adapters/SHA256SUMS` and verified by the
//! `rust-js-build` CI job) so a fresh clone builds without any extra
//! `cargo fetch` step. Override with `$EDGE_JS_WASI_ADAPTER` if your
//! local toolchain needs a different one.

#![cfg_attr(target_arch = "wasm32", no_main)]

#[cfg(target_arch = "wasm32")]
mod wasm_only {
    use edge::cloud::{observe, process};
    use rquickjs::{Context, Function, Runtime, Value};

    wit_bindgen::generate!({
        world: "edge-runtime",
        path: "../../wit",
        generate_all,
    });

    /// Bundle embedded at compile time by `build.rs` (mirrors
    /// `edge-js-runtime/src/lib.rs:785`).
    const USER_JS: &str = include_str!(concat!(env!("OUT_DIR"), "/bundle.js"));

    struct Shim;

    impl Guest for Shim {
        fn start() {
            // 1. Resolve the WS port from env. The worker allocates it
            //    from the PortPool and threads it in; missing means
            //    the supervisor didn't set the sentinel correctly.
            let ws_port: u16 = match process::get_env("EDGE_WS_PORT") {
                Some(s) => match s.parse::<u16>() {
                    Ok(p) => p,
                    Err(_) => {
                        observe::emit_log(
                            "error",
                            &format!("hello-js-ws: invalid EDGE_WS_PORT={s:?}"),
                            &[],
                        );
                        process::exit(2);
                        unreachable!("wit-bindgen's process::exit returns ()")
                    }
                },
                None => {
                    observe::emit_log("error", "hello-js-ws: EDGE_WS_PORT not set in env", &[]);
                    process::exit(2);
                    unreachable!("wit-bindgen's process::exit returns ()")
                }
            };

            // 2. Build QuickJS runtime + context.
            let rt = match Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    observe::emit_log("error", &format!("hello-js-ws: runtime: {e}"), &[]);
                    process::exit(3);
                    unreachable!("process::exit returns ()")
                }
            };
            let ctx = match Context::full(&rt) {
                Ok(ctx) => ctx,
                Err(e) => {
                    observe::emit_log("error", &format!("hello-js-ws: context: {e}"), &[]);
                    process::exit(3);
                    unreachable!("process::exit returns ()")
                }
            };

            // 3-5. Register namespaces, evaluate bundle, call start().
            //
            // `ctx.with(|ctx| ...)` blocks the current thread for the
            // duration of the closure. The user JS's `start()` is a
            // synchronous `for(;;)` that never returns, so this
            // closure never returns — which is exactly the long-running
            // shape we want. If a future bundle uses async (Promises,
            // setTimeout), we'll need to drive the event loop with
            // `ctx.execute_pending_job()`; for now the synchronous
            // bundle is sufficient.
            //
            // The per-namespace `register_*` bodies live in
            // `edge-js-runtime-long::register` (issue #426 refactor)
            // and are shared with the long-running cdylib. The shim
            // does not redefine them.
            let eval_result: Result<(), String> = ctx.with(|ctx| {
                edge_js_runtime_long::register::register_all(&ctx)
                    .map_err(|e| format!("register_all: {e}"))?;
                // Wrap the bundle as an IIFE so `globalThis.start = ...`
                // assignments land as side effects. We do NOT use the
                // bytecode cache (that's a FaaS optimization; the LR
                // path evaluates the bundle once at boot).
                let wrapped = format!("let __user = (function(){{\n{}\n}})();\n", USER_JS);
                ctx.eval::<(), _>(wrapped.as_bytes())
                    .map_err(|e| format!("bundle eval: {e}"))?;
                let start_val: Value = ctx
                    .eval("typeof globalThis.start")
                    .map_err(|e| format!("start lookup: {e}"))?;
                let start_kind = start_val
                    .as_string()
                    .and_then(|s| s.to_string().ok())
                    .unwrap_or_default();
                if start_kind != "function" {
                    return Err(format!(
                        "globalThis.start is not a function (got {start_kind:?})"
                    ));
                }
                let port_str = ws_port.to_string();
                let port_val: Value = ctx
                    .eval(format!("{{ wsPort: {port_str} }}").as_bytes())
                    .map_err(|e| format!("port obj: {e}"))?;
                let start_fn: Function = ctx
                    .globals()
                    .get("start")
                    .map_err(|e| format!("start get: {e}"))?;
                start_fn
                    .call::<_, ()>((port_val,))
                    .map_err(|e| format!("start call: {e}"))?;
                Ok(())
            });
            if let Err(e) = eval_result {
                observe::emit_log("error", &format!("hello-js-ws: {e}"), &[]);
                process::exit(4);
                unreachable!("process::exit returns ()")
            }

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
        }
    }

    // The canonical `edge-runtime` world includes `wasi:cli/command`,
    // which exports a `wasi:cli/run` interface. The supervisor
    // dispatches via the `start` top-level export (see supervisor.rs
    // `run_app_loop`); the host never calls `run`. Stub it so
    // wit-bindgen generates the `run` export. Same shape as
    // `samples/hello/src/lib.rs:64-72`.
    impl exports::wasi::cli::run::Guest for Shim {
        fn run() -> Result<(), ()> {
            Err(())
        }
    }

    export!(Shim);
}
