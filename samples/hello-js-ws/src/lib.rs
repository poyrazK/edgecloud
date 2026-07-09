//! `hello-js-ws` — long-running JS shim for the issue #448 e2e test.
//!
//! Pairs a small Rust cdylib (this file) with an esbuild-bundled JS
//! (`src/handler.js`) so the JS WebSocket binding can be exercised
//! end-to-end against a real `edge-worker`. The shim's `start` entry
//! reads `EDGE_WS_PORT` from the process env, builds a QuickJS
//! runtime in-process, registers the seven `edge:cloud/*` namespaces
//! on `globalThis.EdgeCloud`, evaluates the embedded JS once, calls
//! `globalThis.start({ wsPort })`, and loops `runtime.idle()` to keep
//! the long-running world alive.
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
//! ## Why not modify `edge-js-runtime`?
//!
//! `edge-js-runtime` is FaaS-only and uses a single
//! `wit_bindgen::generate!` invocation (one world per crate). Adding
//! a long-running entry would require a second `generate!` in the
//! same crate, which isn't supported — a separate crate is the
//! structural minimum.
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
//! wasi_snapshot_preview1.reactor.wasm -o <out>` wraps the core
//! module into a component the edge-worker can instantiate.

#![cfg_attr(target_arch = "wasm32", no_main)]

#[cfg(target_arch = "wasm32")]
mod wasm_only {
    use rquickjs::{Context, Ctx, Function, Object, Runtime, TypedArray, Value};
    use edge::cloud::{cache, kv_store, observe, process, scheduling, time, websocket};

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
                    observe::emit_log(
                        "error",
                        "hello-js-ws: EDGE_WS_PORT not set in env",
                        &[],
                    );
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
            // `ctx.execute_pending_job()` (rquickjs 0.9 has no `idle()`);
            // for now the synchronous bundle is sufficient.
            let eval_result: Result<(), String> = ctx.with(|ctx| {
                register_all(&ctx).map_err(|e| format!("register_all: {e}"))?;
                // Wrap the bundle as an IIFE so `globalThis.start = ...`
                // assignments land as side effects. We do NOT use the
                // bytecode cache (that's a FaaS optimization; the LR
                // path evaluates the bundle once at boot).
                let wrapped = format!(
                    "let __user = (function(){{\n{}\n}})();\n",
                    USER_JS
                );
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

    // ─── edge:cloud/* registrations (long-running world) ─────────────
    //
    // These bodies are duplicated from `edge-js-runtime/src/lib.rs`
    // (lines 274-779) because each `wit_bindgen::generate!`
    // invocation produces a distinct set of bindgen symbols. Sharing
    // them would require a macro or generic abstraction that exceeds
    // the duplicate's size, and would couple the FaaS and LR paths
    // unnecessarily.

    fn register_all<'js>(ctx: &Ctx<'js>) -> rquickjs::Result<()> {
        let edge_cloud = Object::new(ctx.clone())?;
        register_kv_store(ctx, &edge_cloud)?;
        register_cache(ctx, &edge_cloud)?;
        register_observe(ctx, &edge_cloud)?;
        register_time(ctx, &edge_cloud)?;
        register_scheduling(ctx, &edge_cloud)?;
        register_process(ctx, &edge_cloud)?;
        register_websocket(ctx, &edge_cloud)?;
        ctx.globals().set("EdgeCloud", edge_cloud)?;
        Ok(())
    }

    // ─── Helpers (also duplicated from edge-js-runtime) ─────────────

    fn js_to_tuple_vec<'js>(val: Value<'js>) -> rquickjs::Result<Vec<(String, String)>> {
        let array = match val.into_array() {
            Some(arr) => arr,
            None => return Ok(Vec::new()),
        };
        let mut vec = Vec::with_capacity(array.len());
        for item in array.iter() {
            let item: Value<'js> = item?;
            if let Some(pair) = item.as_array() {
                if pair.len() >= 2 {
                    let k: String = pair.get(0)?;
                    let v: String = pair.get(1)?;
                    vec.push((k, v));
                }
            }
        }
        Ok(vec)
    }

    fn tuple_vec_to_js<'js>(
        ctx: &Ctx<'js>,
        vec: Vec<(String, String)>,
    ) -> rquickjs::Result<rquickjs::Array<'js>> {
        let arr = rquickjs::Array::new(ctx.clone())?;
        for (i, (k, v)) in vec.into_iter().enumerate() {
            let pair = rquickjs::Array::new(ctx.clone())?;
            pair.set(0, k)?;
            pair.set(1, v)?;
            arr.set(i, pair)?;
        }
        Ok(arr)
    }

    fn js_to_set_many_items<'js>(
        val: Value<'js>,
    ) -> rquickjs::Result<Vec<(String, Vec<u8>, Option<u32>)>> {
        let array = match val.into_array() {
            Some(arr) => arr,
            None => return Ok(Vec::new()),
        };
        let mut vec = Vec::with_capacity(array.len());
        for item in array.iter() {
            let item: Value<'js> = item?;
            if let Some(tuple) = item.as_array() {
                if tuple.len() >= 2 {
                    let k: String = tuple.get(0)?;
                    let v_val: Value<'js> = tuple.get(1)?;
                    let v: Vec<u8> = if let Ok(ta) = TypedArray::<'js, u8>::from_value(v_val) {
                        let bytes: &[u8] = ta.as_ref();
                        bytes.to_vec()
                    } else {
                        Vec::new()
                    };
                    let ttl: Option<u32> = if tuple.len() >= 3 {
                        tuple.get(2)?
                    } else {
                        None
                    };
                    vec.push((k, v, ttl));
                }
            }
        }
        Ok(vec)
    }

    // ─── kv-store ──────────────────────────────────────────────────

    fn register_kv_store<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        let kv = Object::new(ctx.clone())?;
        kv.set("get", Function::new(ctx.clone(), |ctx: Ctx<'js>, key: String| -> rquickjs::Result<Value<'js>> {
            match kv_store::get(&key) {
                Some(bytes) => {
                    let ta = TypedArray::new(ctx, bytes)?;
                    Ok(ta.into_value())
                }
                None => Ok(Value::new_null(ctx)),
            }
        }))?;
        kv.set("set", Function::new(ctx.clone(), |value_val: Value<'js>, key: String, ttl: Option<u32>| -> rquickjs::Result<()> {
            let value = TypedArray::<'js, u8>::from_value(value_val)?;
            let bytes: &[u8] = value.as_ref();
            kv_store::set(&key, bytes, ttl);
            Ok(())
        }))?;
        kv.set("delete", Function::new(ctx.clone(), |key: String| { kv_store::delete(&key); }))?;
        kv.set("listKeys", Function::new(ctx.clone(), |prefix: String| -> Vec<String> { kv_store::list_keys(&prefix) }))?;
        kv.set("getMany", Function::new(ctx.clone(), |ctx: Ctx<'js>, keys: Vec<String>| -> rquickjs::Result<Vec<Value<'js>>> {
            let results = kv_store::get_many(&keys);
            let mut js_results = Vec::with_capacity(results.len());
            for opt in results {
                match opt {
                    Some(bytes) => {
                        let ta = TypedArray::new(ctx.clone(), bytes)?;
                        js_results.push(ta.into_value());
                    }
                    None => js_results.push(Value::new_null(ctx.clone())),
                }
            }
            Ok(js_results)
        }))?;
        kv.set("setMany", Function::new(ctx.clone(), |items_val: Value<'js>| -> rquickjs::Result<()> {
            let items = js_to_set_many_items(items_val)?;
            kv_store::set_many(&items);
            Ok(())
        }))?;
        kv.set("deleteMany", Function::new(ctx.clone(), |keys: Vec<String>| { kv_store::delete_many(&keys); }))?;
        kv.set("exists", Function::new(ctx.clone(), |key: String| -> bool { kv_store::exists(&key) }))?;
        kv.set("clear", Function::new(ctx.clone(), || { kv_store::clear(); }))?;
        parent.set("kv", kv)?;
        Ok(())
    }

    // ─── cache ─────────────────────────────────────────────────────

    fn register_cache<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        let c = Object::new(ctx.clone())?;
        c.set("get", Function::new(ctx.clone(), |ctx: Ctx<'js>, key: String| -> rquickjs::Result<Value<'js>> {
            match cache::get(&key) {
                Some(bytes) => {
                    let ta = TypedArray::new(ctx, bytes)?;
                    Ok(ta.into_value())
                }
                None => Ok(Value::new_null(ctx)),
            }
        }))?;
        c.set("set", Function::new(ctx.clone(), |value_val: Value<'js>, key: String, ttl: Option<u32>| -> rquickjs::Result<()> {
            let value = TypedArray::<'js, u8>::from_value(value_val)?;
            let bytes: &[u8] = value.as_ref();
            cache::set(&key, bytes, ttl);
            Ok(())
        }))?;
        c.set("delete", Function::new(ctx.clone(), |key: String| { cache::delete(&key); }))?;
        c.set("clear", Function::new(ctx.clone(), || { cache::clear(); }))?;
        c.set("size", Function::new(ctx.clone(), || -> u32 { cache::size() }))?;
        c.set("exists", Function::new(ctx.clone(), |key: String| -> bool { cache::exists(&key) }))?;
        c.set("listKeys", Function::new(ctx.clone(), |prefix: String| -> Vec<String> { cache::list_keys(&prefix) }))?;
        c.set("getMany", Function::new(ctx.clone(), |ctx: Ctx<'js>, keys: Vec<String>| -> rquickjs::Result<Vec<Value<'js>>> {
            let results = cache::get_many(&keys);
            let mut js_results = Vec::with_capacity(results.len());
            for opt in results {
                match opt {
                    Some(bytes) => {
                        let ta = TypedArray::new(ctx.clone(), bytes)?;
                        js_results.push(ta.into_value());
                    }
                    None => js_results.push(Value::new_null(ctx.clone())),
                }
            }
            Ok(js_results)
        }))?;
        c.set("setMany", Function::new(ctx.clone(), |items_val: Value<'js>| -> rquickjs::Result<()> {
            let items = js_to_set_many_items(items_val)?;
            cache::set_many(&items);
            Ok(())
        }))?;
        c.set("deleteMany", Function::new(ctx.clone(), |keys: Vec<String>| { cache::delete_many(&keys); }))?;
        parent.set("cache", c)?;
        Ok(())
    }

    // ─── observe ──────────────────────────────────────────────────

    fn register_observe<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        let obs = Object::new(ctx.clone())?;
        obs.set("incrementCounter", Function::new(ctx.clone(), |name: String, labels_val: Value<'js>| -> rquickjs::Result<()> {
            let labels = js_to_tuple_vec(labels_val)?;
            observe::increment_counter(&name, &labels);
            Ok(())
        }))?;
        obs.set("recordGauge", Function::new(ctx.clone(), |name: String, value: f64, labels_val: Value<'js>| -> rquickjs::Result<()> {
            let labels = js_to_tuple_vec(labels_val)?;
            observe::record_gauge(&name, value, &labels);
            Ok(())
        }))?;
        obs.set("recordHistogram", Function::new(ctx.clone(), |name: String, value: f64, labels_val: Value<'js>| -> rquickjs::Result<()> {
            let labels = js_to_tuple_vec(labels_val)?;
            observe::record_histogram(&name, value, &labels);
            Ok(())
        }))?;
        obs.set("emitLog", Function::new(ctx.clone(), |level: String, message: String, labels_val: Value<'js>| -> rquickjs::Result<()> {
            let labels = js_to_tuple_vec(labels_val)?;
            observe::emit_log(&level, &message, &labels);
            Ok(())
        }))?;
        obs.set("emitLogRecord", Function::new(ctx.clone(), |timestamp_ms: u64, level: String, message: String, labels_val: Value<'js>| -> rquickjs::Result<()> {
            let labels = js_to_tuple_vec(labels_val)?;
            let lvl = match level.as_str() {
                "error" => observe::LogLevel::Error,
                "warn" => observe::LogLevel::Warn,
                "info" => observe::LogLevel::Info,
                "debug" => observe::LogLevel::Debug,
                _ => observe::LogLevel::Trace,
            };
            observe::emit_log_record(&observe::LogRecord {
                timestamp_ms,
                level: lvl,
                message,
                labels,
            });
            Ok(())
        }))?;
        parent.set("observe", obs)?;
        Ok(())
    }

    // ─── time ─────────────────────────────────────────────────────

    fn register_time<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        let t = Object::new(ctx.clone())?;
        t.set("now", Function::new(ctx.clone(), || -> u64 { time::now() }))?;
        t.set("sleep", Function::new(ctx.clone(), |duration_ms: u64| { time::sleep(duration_ms); }))?;
        t.set("resolution", Function::new(ctx.clone(), || -> u64 { time::resolution() }))?;
        parent.set("time", t)?;
        Ok(())
    }

    // ─── scheduling ────────────────────────────────────────────────

    fn register_scheduling<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        let s = Object::new(ctx.clone())?;
        s.set("scheduleOnce", Function::new(ctx.clone(), |delay_ms: u64, payload_val: Value<'js>| -> rquickjs::Result<String> {
            let payload = TypedArray::<'js, u8>::from_value(payload_val)?;
            let bytes: &[u8] = payload.as_ref();
            Ok(scheduling::schedule_once(delay_ms, bytes))
        }))?;
        s.set("scheduleRepeating", Function::new(ctx.clone(), |interval_ms: u64, payload_val: Value<'js>| -> rquickjs::Result<String> {
            let payload = TypedArray::<'js, u8>::from_value(payload_val)?;
            let bytes: &[u8] = payload.as_ref();
            Ok(scheduling::schedule_repeating(interval_ms, bytes))
        }))?;
        s.set("cancelScheduled", Function::new(ctx.clone(), |id: String| { scheduling::cancel_scheduled(&id); }))?;
        parent.set("scheduling", s)?;
        Ok(())
    }

    // ─── process ───────────────────────────────────────────────────

    fn register_process<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        let p = Object::new(ctx.clone())?;
        p.set("getEnv", Function::new(ctx.clone(), |key: String| -> Option<String> { process::get_env(&key) }))?;
        p.set("getAllEnv", Function::new(ctx.clone(), |ctx: Ctx<'js>| -> rquickjs::Result<rquickjs::Array<'js>> {
            let envs = process::get_all_env();
            tuple_vec_to_js(&ctx, envs)
        }))?;
        p.set("getArgs", Function::new(ctx.clone(), || -> Vec<String> { process::get_args() }))?;
        p.set("getCwd", Function::new(ctx.clone(), |ctx: Ctx<'js>| -> rquickjs::Result<Value<'js>> {
            match process::get_cwd() {
                Ok(cwd) => {
                    let obj = Object::new(ctx.clone())?;
                    obj.set("ok", cwd)?;
                    Ok(obj.into_value())
                }
                Err(err) => {
                    let obj = Object::new(ctx.clone())?;
                    obj.set("err", err)?;
                    Ok(obj.into_value())
                }
            }
        }))?;
        p.set("exit", Function::new(ctx.clone(), |code: u32| { process::exit(code); }))?;
        parent.set("process", p)?;
        Ok(())
    }

    // ─── websocket ─────────────────────────────────────────────────

    fn js_to_message_type(s: &str) -> Option<websocket::MessageType> {
        match s {
            "text" => Some(websocket::MessageType::Text),
            "binary" => Some(websocket::MessageType::Binary),
            "ping" => Some(websocket::MessageType::Ping),
            "pong" => Some(websocket::MessageType::Pong),
            _ => None,
        }
    }

    fn message_type_to_js(kind: websocket::MessageType) -> &'static str {
        match kind {
            websocket::MessageType::Text => "text",
            websocket::MessageType::Binary => "binary",
            websocket::MessageType::Ping => "ping",
            websocket::MessageType::Pong => "pong",
            websocket::MessageType::Close => "close",
        }
    }

    fn register_websocket<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        use rquickjs::Exception;

        let ws = Object::new(ctx.clone())?;

        ws.set(
            "listen",
            Function::new(ctx.clone(), move |ctx: Ctx<'js>, port: u16| -> rquickjs::Result<u32> {
                websocket::listen(port).map_err(|e| {
                    let msg = format!("websocket listen failed: {e}");
                    Exception::throw_message(&ctx, &msg)
                })
            }),
        )?;

        ws.set(
            "accept",
            Function::new(ctx.clone(), move |ctx: Ctx<'js>, listener: u32| -> rquickjs::Result<u32> {
                websocket::accept(listener).map_err(|e| {
                    let msg = format!("websocket accept failed: {e}");
                    Exception::throw_message(&ctx, &msg)
                })
            }),
        )?;

        ws.set(
            "send",
            Function::new(
                ctx.clone(),
                move |ctx: Ctx<'js>,
                      conn: u32,
                      data_val: Value<'js>,
                      kind: String|
                      -> rquickjs::Result<()> {
                    let data = TypedArray::<'js, u8>::from_value(data_val)?;
                    let bytes: &[u8] = data.as_ref();
                    let k = js_to_message_type(&kind).ok_or_else(|| {
                        let msg = format!("invalid message-type {kind:?}; expected text | binary | ping | pong");
                        Exception::throw_message(&ctx, &msg)
                    })?;
                    websocket::send(conn, bytes, k)
                        .map_err(|_| Exception::throw_message(&ctx, "websocket send failed"))
                },
            ),
        )?;

        ws.set(
            "receive",
            Function::new(ctx.clone(), move |ctx: Ctx<'js>, conn: u32| -> rquickjs::Result<Value<'js>> {
                match websocket::receive(conn) {
                    Ok((bytes, kind)) => {
                        let obj = Object::new(ctx.clone())?;
                        let ta = TypedArray::new(ctx.clone(), bytes)?;
                        obj.set("data", ta.into_value())?;
                        obj.set("kind", message_type_to_js(kind))?;
                        Ok(obj.into_value())
                    }
                    Err(ci) => {
                        let close = Object::new(ctx.clone())?;
                        close.set("code", ci.code)?;
                        close.set("reason", ci.reason)?;
                        let obj = Object::new(ctx.clone())?;
                        obj.set("close", close)?;
                        Ok(obj.into_value())
                    }
                }
            }),
        )?;

        ws.set(
            "close",
            Function::new(
                ctx.clone(),
                move |ctx: Ctx<'js>, conn: u32, info: Value<'js>| -> rquickjs::Result<()> {
                    let info_obj = info.as_object().ok_or_else(|| {
                        Exception::throw_message(&ctx, "close info must be an object {code, reason}")
                    })?;
                    let code: u16 = info_obj.get("code")?;
                    let reason: String = info_obj.get("reason")?;
                    let ci = websocket::CloseInfo { code, reason };
                    websocket::close(conn, &ci)
                        .map_err(|_| Exception::throw_message(&ctx, "websocket close failed"))
                },
            ),
        )?;

        parent.set("websocket", ws)?;
        Ok(())
    }
}
