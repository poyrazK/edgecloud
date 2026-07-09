//! edge-js-runtime: a QuickJS-based WebAssembly component that exports
//! `wasi:http/incoming-handler@0.2.1` for the edgeCloud FaaS path.
//!
//! Issue #425 fix: the user bundle is AOT-compiled to module-form bytecode
//! on the first request and cached in a guest-side `OnceCell`; subsequent
//! requests load the bytecode via `rquickjs::module::Module::load`,
//! skipping QuickJS lex+parse entirely. See `USER_BYTECODE` below.
//!
//! **Build targets:**
//! - `wasm32-wasip1` — the deployed `.wasm` artifact. WASI bindings
//!   (`wit_bindgen::generate!`, `export!(JsHandler)`, `Guest::handle`)
//!   are compiled into the component.
//! - Host (rlib) — only the host-safe items at the bottom of the file
//!   are compiled. Used by the `warm_vs_cold` bench.
//!
//! The `path = "../wit"` argument to `wit_bindgen::generate!` points at
//! the repo-root canonical `wit/` (promoted to top-level by PR #414
//! and consumed by samples/hello, edge-js-runtime, and edge-worker/test
//! fixtures; mirrored into `edge-control-plane/internal/service/wit/`
//! for the Go control plane's `embed.FS`). The wit-drift-check CI job
//! fails the build if the two copies diverge. The `wasi:cli/command@0.2.1`
//! include + the seven `edge:cloud/*` imports (kv-store, cache, observe,
//! time, scheduling, process, websocket) are declared in that file.

#![cfg_attr(target_arch = "wasm32", no_main)]

// `pub` (not just `mod`) so the host-target bench can reach
// `register_all_stub` via `edge_js_runtime::edge_modules::register_all_stub`.
// The wasm cdylib build tree-shakes; the bench pulls it in by name.
//
// `edge_modules` only contains the host-safe `register_all_stub`. The
// WASI-bound `register_all` + per-namespace registrars live in
// `mod wasm_only` below (where `wit_bindgen`-generated symbols are
// available).
pub mod edge_modules;

// ─── WASI bindings (wasm target only) ─────────────────────────────────
//
// Everything below references `wit_bindgen`-generated symbols and
// WASI http types. Gated behind `#[cfg(target_arch = "wasm32")]` so
// the host-target bench build skips this block and never tries to
// resolve `_wasi:cli/run@0.2.1` etc. at link time.
#[cfg(target_arch = "wasm32")]
mod wasm_only {
    use super::{compile_user_bundle, USER_BYTECODE};
    use exports::wasi::http::incoming_handler::Guest;
    use rquickjs::{Ctx, Function, Object, TypedArray, Value};
    use self::edge::cloud::{cache, kv_store, observe, process, scheduling, time, websocket};
    use wasi::http::types::{
        Fields, IncomingRequest, OutgoingResponse, ResponseOutparam,
    };

    wit_bindgen::generate!({
        world: "edge-runtime-handler",
        path: "../wit",
        generate_all,
    });

    pub struct JsHandler;

    export!(JsHandler);

    // Stub for wasi:cli/run (required by the world, never called by the host).
    impl exports::wasi::cli::run::Guest for JsHandler {
        fn run() -> Result<(), ()> {
            Err(())
        }
    }

    impl Guest for JsHandler {
        fn handle(req: IncomingRequest, out: ResponseOutparam) {
            // 1. Fresh runtime + context per request (cheap — µs scale).
            let rt = match rquickjs::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => return send_error(out, &format!("runtime: {e}")),
            };
            let ctx = match rquickjs::Context::full(&rt) {
                Ok(ctx) => ctx,
                Err(e) => return send_error(out, &format!("context: {e}")),
            };

            // 2. Get cached bytecode (first request compiles via Module::declare).
            let bytecode: Vec<u8> = match USER_BYTECODE.get_or_init(|| compile_user_bundle(&rt)) {
                Ok(b) => b.clone(),
                Err(e) => return send_error(out, e),
            };
            if bytecode.len() > super::MAX_BYTECODE_BYTES {
                return send_error(out, &format!("bytecode too large: {}", bytecode.len()));
            }

            ctx.with(|ctx| {
                // 3. Register edge:cloud modules on globalThis.EdgeCloud.
                if let Err(e) = register_all(&ctx) {
                    return send_error(out, &format!("register: {e}"));
                }

                // 4. Load cached bytecode → declared module (skips lex+parse).
                //
                // SAFETY: the `bytecode` slice was produced by
                // `rquickjs::module::Module::write_le` inside
                // `compile_user_bundle` on the *same* QuickJS engine
                // compiled into this `.wasm`. The bytes are
                // well-formed for `Module::load`'s
                // `JS_ReadObject(JS_READ_OBJ_BYTECODE)` path, and
                // the resulting `Module` is bound to the same
                // `Runtime` that produced them. Any drift between
                // producer + consumer engines would surface as an
                // `Err` on the next line, not as UB.
                let module = match unsafe {
                    rquickjs::module::Module::load(ctx.clone(), &bytecode)
                } {
                    Ok(m) => m,
                    Err(e) => return send_error(out, &format!("bytecode load: {e}")),
                };

                // 5. Evaluate module. Returns (Module, Promise) — the promise
                //    resolves on the next event-loop tick; we drop it here
                //    (resolved-to-undefined is a no-op). Bundles that use
                //    top-level-await don't fit this single-shot dispatch
                //    model — they need to await the promise before looking
                //    up `globalThis.handleRequest`. For #425 we accept this
                //    as a known limitation; document in the PR.
                let promise = match module.eval() {
                    Ok((_m, p)) => p,
                    Err(e) => return send_error(out, &format!("module eval: {e}")),
                };
                drop(promise);

                // 6. Build the request object for JS.
                let method = format!("{:?}", req.method());
                let path = req.path_with_query().unwrap_or_else(|| "/".into());
                let headers_handle = req.headers();
                let header_entries = headers_handle.entries();

                let js_req = match rquickjs::Object::new(ctx.clone()) {
                    Ok(o) => o,
                    Err(e) => return send_error(out, &format!("req obj: {e}")),
                };
                if let Err(e) = js_req.set("method", method) {
                    return send_error(out, &format!("req.method: {e}"));
                }
                if let Err(e) = js_req.set("path", path) {
                    return send_error(out, &format!("req.path: {e}"));
                }

                let js_headers = match rquickjs::Object::new(ctx.clone()) {
                    Ok(o) => o,
                    Err(e) => return send_error(out, &format!("headers obj: {e}")),
                };
                for (name, value) in &header_entries {
                    let val_str = String::from_utf8_lossy(value).to_string();
                    if let Err(e) = js_headers.set(name.as_str(), val_str) {
                        return send_error(out, &format!("headers[{}]: {}", name, e));
                    }
                }
                if let Err(e) = js_req.set("headers", js_headers) {
                    return send_error(out, &format!("req.headers: {e}"));
                }

                let body_str = read_incoming_body(&req);
                if let Err(e) = js_req.set("body", body_str) {
                    return send_error(out, &format!("req.body: {e}"));
                }

                // 7. Expose __req on globalThis so the user's handler can read it.
                if let Err(e) = ctx.globals().set("__req", js_req) {
                    return send_error(out, &format!("globalThis.__req: {e}"));
                }

                // 8. Call the user's handleRequest.
                let result: rquickjs::Value = match ctx.eval("globalThis.handleRequest(__req)") {
                    Ok(v) => v,
                    Err(e) => return send_error(out, &format!("handleRequest: {e}")),
                };

                // 9. Extract and send response.
                let (status, resp_body, content_type) = extract_response(&ctx, result);
                send_response(out, status, &resp_body, &content_type);
            });
        }
    }

    /// Read the full body from an IncomingRequest as a String.
    fn read_incoming_body(req: &IncomingRequest) -> String {
        match req.consume() {
            Ok(body) => {
                let stream = body.stream().expect("body stream");
                let mut buf = Vec::new();
                loop {
                    match stream.blocking_read(4096) {
                        Ok(chunk) if chunk.is_empty() => break,
                        Ok(chunk) => buf.extend_from_slice(&chunk),
                        Err(_) => break,
                    }
                }
                String::from_utf8_lossy(&buf).to_string()
            }
            Err(_) => String::new(),
        }
    }

    /// Extract { status, body, contentType } from a JS response object.
    fn extract_response(
        _ctx: &rquickjs::Ctx<'_>,
        val: rquickjs::Value<'_>,
    ) -> (u16, Vec<u8>, String) {
        if let Some(obj) = val.as_object() {
            let status: u16 = obj.get("status").unwrap_or(200);
            let body: String = obj.get("body").unwrap_or_default();
            let content_type: String = obj
                .get("contentType")
                .unwrap_or_else(|_| "application/json".to_string());
            (status, body.into_bytes(), content_type)
        } else {
            // If the handler returns a string, treat it as the body
            let body = val
                .as_string()
                .map(|s| s.to_string().unwrap_or_default())
                .unwrap_or_else(|| "null".to_string());
            (200, body.into_bytes(), "application/json".to_string())
        }
    }

    /// Send an HTTP response via the component model.
    fn send_response(out: ResponseOutparam, status: u16, body: &[u8], content_type: &str) {
        let headers = Fields::new();
        let _ = headers.set("content-type", &[content_type.as_bytes().to_vec()]);
        let resp = OutgoingResponse::new(headers);
        resp.set_status_code(status).unwrap();
        let body_handle = resp.body().expect("response body");
        let stream = body_handle.write().expect("output stream");
        stream.blocking_write_and_flush(body).unwrap();
        drop(stream);
        let _ = wasi::http::types::OutgoingBody::finish(body_handle, None);
        wasi::http::types::ResponseOutparam::set(out, Ok(resp));
    }

    /// Emit a 500 with the error message as a text/plain body. Replaces the
    /// pre-#425 `.expect(...)` panic sites — a misbehaving bundle must not
    /// trap the wasm guest (the host would just see `Err` from
    /// `ProxyPre::call_handle` with no useful detail).
    fn send_error(out: ResponseOutparam, msg: &str) {
        send_response(out, 500, msg.as_bytes(), "text/plain");
    }

    // ─── edge:cloud/* registrations ─────────────────────────────────
    //
    // Each `register_*` function attaches a sub-namespace of
    // `globalThis.EdgeCloud` (e.g. `EdgeCloud.kv.get`). The
    // `register_all` entry point is called once per FaaS request from
    // `Guest::handle`. All of these reference the
    // `wit_bindgen`-generated modules under `self::edge::cloud::*`,
    // which is why they live inside `mod wasm_only` (gated on the
    // wasm target). See `edge_modules::register_all_stub` for the
    // host-target equivalent used by the `warm_vs_cold` bench.

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

    // ─── JS ↔ Rust helpers ─────────────────────────────────────────

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

        kv.set("delete", Function::new(ctx.clone(), |key: String| {
            kv_store::delete(&key);
        }))?;

        kv.set("listKeys", Function::new(ctx.clone(), |prefix: String| -> Vec<String> {
            kv_store::list_keys(&prefix)
        }))?;

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

        kv.set("deleteMany", Function::new(ctx.clone(), |keys: Vec<String>| {
            kv_store::delete_many(&keys);
        }))?;

        kv.set("exists", Function::new(ctx.clone(), |key: String| -> bool {
            kv_store::exists(&key)
        }))?;

        kv.set("clear", Function::new(ctx.clone(), || {
            kv_store::clear();
        }))?;

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

        c.set("delete", Function::new(ctx.clone(), |key: String| {
            cache::delete(&key);
        }))?;

        c.set("clear", Function::new(ctx.clone(), || {
            cache::clear();
        }))?;

        c.set("size", Function::new(ctx.clone(), || -> u32 {
            cache::size()
        }))?;

        c.set("exists", Function::new(ctx.clone(), |key: String| -> bool {
            cache::exists(&key)
        }))?;

        c.set("listKeys", Function::new(ctx.clone(), |prefix: String| -> Vec<String> {
            cache::list_keys(&prefix)
        }))?;

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

        c.set("deleteMany", Function::new(ctx.clone(), |keys: Vec<String>| {
            cache::delete_many(&keys);
        }))?;

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

    // ─── time ──────────────────────────────────────────────────────

    fn register_time<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        let t = Object::new(ctx.clone())?;

        t.set("now", Function::new(ctx.clone(), || -> u64 {
            time::now()
        }))?;

        t.set("sleep", Function::new(ctx.clone(), |duration_ms: u64| {
            time::sleep(duration_ms);
        }))?;

        t.set("resolution", Function::new(ctx.clone(), || -> u64 {
            time::resolution()
        }))?;

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

        s.set("cancelScheduled", Function::new(ctx.clone(), |id: String| {
            scheduling::cancel_scheduled(&id);
        }))?;

        parent.set("scheduling", s)?;
        Ok(())
    }

    // ─── process ───────────────────────────────────────────────────

    fn register_process<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        let p = Object::new(ctx.clone())?;

        p.set("getEnv", Function::new(ctx.clone(), |key: String| -> Option<String> {
            process::get_env(&key)
        }))?;

        p.set("getAllEnv", Function::new(ctx.clone(), |ctx: Ctx<'js>| -> rquickjs::Result<rquickjs::Array<'js>> {
            let envs = process::get_all_env();
            tuple_vec_to_js(&ctx, envs)
        }))?;

        p.set("getArgs", Function::new(ctx.clone(), || -> Vec<String> {
            process::get_args()
        }))?;

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

        p.set("exit", Function::new(ctx.clone(), |code: u32| {
            process::exit(code);
        }))?;

        parent.set("process", p)?;
        Ok(())
    }

    // ─── websocket ─────────────────────────────────────────────────

    /// Translate a JS string into the bindgen-generated `MessageType` enum.
    ///
    /// `kind` strings mirror the WIT `enum message-type { text, binary, ping,
    /// pong, close }` at `edge-runtime/src/wit/edge-cloud.wit`. We do not
    /// surface the fifth variant (`close`) here because JS handlers send
    /// `close` frames via the dedicated `EdgeCloud.websocket.close()` method,
    /// not via `send({kind: "close"})`. A typo or unknown variant is an error,
    /// not a silent fallback.
    fn js_to_message_type(s: &str) -> Option<websocket::MessageType> {
        match s {
            "text" => Some(websocket::MessageType::Text),
            "binary" => Some(websocket::MessageType::Binary),
            "ping" => Some(websocket::MessageType::Ping),
            "pong" => Some(websocket::MessageType::Pong),
            _ => None,
        }
    }

    /// Map a `websocket::MessageType` back to the JS-facing string form.
    fn message_type_to_js(kind: websocket::MessageType) -> &'static str {
        match kind {
            websocket::MessageType::Text => "text",
            websocket::MessageType::Binary => "binary",
            websocket::MessageType::Ping => "ping",
            websocket::MessageType::Pong => "pong",
            websocket::MessageType::Close => "close",
        }
    }

    /// Register the `websocket` interface on `parent.websocket`.
    ///
    /// Note on errors: the WIT declares `listen`/`accept` as
    /// `result<u32, string>`, so we surface the host error reason. `send` and
    /// `close` use bare `result` (WIT lines 95, 101); the bindgen-shadowed
    /// Host impls in `edge-runtime/src/runtime.rs:1122, 1147` `map_err(|_| ())`
    /// the actual reason away before the JS binding ever sees it. So JS
    /// callers see generic "websocket send/close failed" messages until the
    /// v0.3 WIT-level rework tracked alongside issue #422. Accept and
    /// receive work fine.
    fn register_websocket<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
        use rquickjs::Exception;

        let ws = Object::new(ctx.clone())?;

        // listen(port) -> listenerId (u32). Throws on bind failure.
        ws.set(
            "listen",
            Function::new(ctx.clone(), move |ctx: Ctx<'js>, port: u16| -> rquickjs::Result<u32> {
                websocket::listen(port).map_err(|e| {
                    let msg = format!("websocket listen failed: {e}");
                    Exception::throw_message(&ctx, &msg)
                })
            }),
        )?;

        // accept(listenerId) -> connId (u32). Throws on accept failure.
        ws.set(
            "accept",
            Function::new(ctx.clone(), move |ctx: Ctx<'js>, listener: u32| -> rquickjs::Result<u32> {
                websocket::accept(listener).map_err(|e| {
                    let msg = format!("websocket accept failed: {e}");
                    Exception::throw_message(&ctx, &msg)
                })
            }),
        )?;

        // send(conn, data, kind) — data is a Uint8Array; kind is one of
        // "text" | "binary" | "ping" | "pong". Throws on bad kind or send
        // failure (no reason; see note above).
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

        // receive(conn) -> { data, kind } | { close: { code, reason } }.
        //
        // The WIT declares `receive` as `result<tuple<list<u8>, message-type>,
        // close-info>` — an asymmetric Result where the success branch carries
        // the message payload and the error branch carries a peer-initiated
        // close frame. Both forms are surfaced as JS objects with
        // discriminating fields, mirroring the `{ok} | {err}` shape of
        // `process.cwd` (see this file's `register_process` for the precedent).
        // JS callers should check `if (res.close)` first.
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

        // close(conn, {code, reason}) — `info` is a JS object with numeric
        // `code` and string `reason` fields. The bindgen-generated
        // `websocket::CloseInfo` is a public-field struct and the
        // `close(conn, info)` signature takes `&CloseInfo`. The host impl in
        // `edge-runtime/src/runtime.rs:1146-1147` shows the equivalent shape.
        // Throws on close failure (no reason; see note above).
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

// ─── Host-safe items (used by the wasm target AND the bench) ─────────

/// The user's bundled JS, embedded at compile time by build.rs.
pub const USER_JS: &str = include_str!(concat!(env!("OUT_DIR"), "/bundle.js"));

/// Wrap USER_JS into a synthetic ES module so QuickJS's bytecode writer
/// has a module-form input. The IIFE executes the user's bundle inside
/// its scope (so any closures/var declarations it makes stay reachable
/// AND `globalThis.handleRequest = ...` lands as a side effect), then
/// re-exports the assigned function so module consumers can find it.
///
/// This preserves the existing `globalThis.handleRequest` contract that
/// `samples/hello-js/src/handler.js` and all shipped bundles use — no
/// esbuild changes required.
///
/// We can't express this as `concat!("...", USER_JS, "...")` because
/// `concat!` only accepts literals. The wrapper is built in
/// `wrap_as_module` below.
pub fn wrap_as_module(user_js: &str) -> String {
    // Pre-size the String to avoid one allocation + several grows.
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
/// request pays the lex+parse cost via `compile_user_bundle`;
/// subsequent requests replay the bytes through `Module::load(&[u8])`,
/// which calls `JS_ReadObject(JS_READ_OBJ_BYTECODE)` and skips
/// lex+parse entirely.
///
/// **Lifetime invariant:** the bytes are version-tied to the QuickJS
/// engine compiled into this .wasm. They must NEVER be persisted to
/// disk or shared across wasm rebuilds — every new deployment ships a
/// fresh .wasm with a fresh `static`, so this is automatic in practice.
pub static USER_BYTECODE: once_cell::sync::OnceCell<Result<Vec<u8>, String>> =
    once_cell::sync::OnceCell::new();

/// Compile the wrapped user bundle to module-form bytecode.
///
/// The throwaway `Context` is dropped on return — only the `Vec<u8>` we
/// produce escapes. Errors are reported as strings so the caller can
/// surface them to the client as a 500.
pub fn compile_user_bundle(rt: &rquickjs::Runtime) -> Result<Vec<u8>, String> {
    let wrapped = wrap_as_module(USER_JS);
    let ctx = rquickjs::Context::full(rt).map_err(|e| format!("context: {e}"))?;
    ctx.with(|ctx| {
        let module = rquickjs::module::Module::declare(
            ctx.clone(),
            "user.js",
            wrapped.as_bytes(),
        )
        .map_err(|e| format!("declare: {e}"))?;
        module
            .write_le()
            .map_err(|e| format!("write_le: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::MAX_BYTECODE_BYTES;

    /// `MAX_BYTECODE_BYTES` is the inner-side guardrail on the cached
    /// user-bundle bytecode blob (see the const's doc comment). The
    /// control plane already caps the input artifact at
    /// `MaxArtifactSize` (100 MiB), so this is a defense-in-depth
    /// check inside the guest: it rejects a bundle whose compiled
    /// form blows past a sane memory budget. The cap lives inside
    /// the wasm guest's `Guest::handle`, so we can't drive it from a
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
        // Loose upper bound: the control plane caps the input at
        // 100 MiB (`MaxArtifactSize`), so the bytecode form is at
        // most that large. A cap of 100 MiB or more is a no-op.
        assert!(
            MAX_BYTECODE_BYTES <= 100 * 1024 * 1024,
            "MAX_BYTECODE_BYTES = {MAX_BYTECODE_BYTES} is too high \
             (defeats the inner-side guardrail)"
        );
    }
}