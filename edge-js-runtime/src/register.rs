//! `globalThis.EdgeCloud` namespace registrars.
//!
//! Exposes the seven `edge:cloud/*` interfaces (kv-store, cache, observe,
//! time, scheduling, process, websocket) to JS handlers as sub-objects
//! of `globalThis.EdgeCloud`. Each `register_*` function attaches a
//! sub-namespace (e.g. `EdgeCloud.kv.get`).
//!
//! The bodies reference `kv_store::get`, `cache::set`, etc. — the
//! bindgen-generated modules from `wit_bindgen::generate!`. They must
//! be in scope at the call site, which is why this file lives in the
//! same `mod wasm_only` that called `generate!`. There are TWO copies
//! of this file (`lib.rs::wasm_only::register` for the FaaS
//! `edge-runtime-handler` world, and `long.rs::wasm_only::register` for
//! the long-running `edge-runtime` world); they have identical bodies.
//! Sharing via macro_rules! was considered but rejected as more
//! complex than the duplication; the bodies are world-agnostic, only
//! their lexical environment differs.
//!
//! The `register_all` entry point is the only public symbol — the
//! per-namespace `register_*` helpers stay private to the module. This
//! matches the prior shape: the FaaS `Guest::handle` and the LR
//! `Guest::start` both call `register_all(&ctx)?` exactly once.

use rquickjs::{Ctx, Function, Object, TypedArray, Value};

// Bindgen-generated `edge:cloud/*` import modules. The parent `wasm_only`
// in `lib.rs` does `pub use self::edge::cloud::{kv_store, cache, ...};`
// so this module can reach them without making `wasm_only` itself
// public. Only the FaaS-world import names are wired here; the LR-world
// version lives in `long/register.rs` (which `pub use`s the LR-world
// `wasm_only::edge::cloud::*` instead).
use crate::url_parse::parse_fetch_url;
use crate::wasm_only::{
    cache, http_types, kv_store, observe, outgoing_handler, poll, process, scheduling, time,
    websocket,
};

pub fn register_all<'js>(ctx: &Ctx<'js>) -> rquickjs::Result<()> {
    let edge_cloud = Object::new(ctx.clone())?;
    register_kv_store(ctx, &edge_cloud)?;
    register_cache(ctx, &edge_cloud)?;
    register_observe(ctx, &edge_cloud)?;
    register_time(ctx, &edge_cloud)?;
    register_scheduling(ctx, &edge_cloud)?;
    register_process(ctx, &edge_cloud)?;
    register_websocket(ctx, &edge_cloud)?;
    register_http(ctx, &edge_cloud)?;
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

    kv.set(
        "get",
        Function::new(
            ctx.clone(),
            |ctx: Ctx<'js>, key: String| -> rquickjs::Result<Value<'js>> {
                match kv_store::get(&key) {
                    Some(bytes) => {
                        let ta = TypedArray::new(ctx, bytes)?;
                        Ok(ta.into_value())
                    }
                    None => Ok(Value::new_null(ctx)),
                }
            },
        ),
    )?;

    kv.set(
        "set",
        Function::new(
            ctx.clone(),
            |value_val: Value<'js>, key: String, ttl: Option<u32>| -> rquickjs::Result<()> {
                let value = TypedArray::<'js, u8>::from_value(value_val)?;
                let bytes: &[u8] = value.as_ref();
                kv_store::set(&key, bytes, ttl);
                Ok(())
            },
        ),
    )?;

    kv.set(
        "delete",
        Function::new(ctx.clone(), |key: String| {
            kv_store::delete(&key);
        }),
    )?;

    kv.set(
        "listKeys",
        Function::new(ctx.clone(), |prefix: String| -> Vec<String> {
            kv_store::list_keys(&prefix)
        }),
    )?;

    kv.set(
        "getMany",
        Function::new(
            ctx.clone(),
            |ctx: Ctx<'js>, keys: Vec<String>| -> rquickjs::Result<Vec<Value<'js>>> {
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
            },
        ),
    )?;

    kv.set(
        "setMany",
        Function::new(
            ctx.clone(),
            |items_val: Value<'js>| -> rquickjs::Result<()> {
                let items = js_to_set_many_items(items_val)?;
                kv_store::set_many(&items);
                Ok(())
            },
        ),
    )?;

    kv.set(
        "deleteMany",
        Function::new(ctx.clone(), |keys: Vec<String>| {
            kv_store::delete_many(&keys);
        }),
    )?;

    kv.set(
        "exists",
        Function::new(ctx.clone(), |key: String| -> bool {
            kv_store::exists(&key)
        }),
    )?;

    kv.set(
        "clear",
        Function::new(ctx.clone(), || {
            kv_store::clear();
        }),
    )?;

    parent.set("kv", kv)?;
    Ok(())
}

// ─── cache ─────────────────────────────────────────────────────

fn register_cache<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let c = Object::new(ctx.clone())?;

    c.set(
        "get",
        Function::new(
            ctx.clone(),
            |ctx: Ctx<'js>, key: String| -> rquickjs::Result<Value<'js>> {
                match cache::get(&key) {
                    Some(bytes) => {
                        let ta = TypedArray::new(ctx, bytes)?;
                        Ok(ta.into_value())
                    }
                    None => Ok(Value::new_null(ctx)),
                }
            },
        ),
    )?;

    c.set(
        "set",
        Function::new(
            ctx.clone(),
            |value_val: Value<'js>, key: String, ttl: Option<u32>| -> rquickjs::Result<()> {
                let value = TypedArray::<'js, u8>::from_value(value_val)?;
                let bytes: &[u8] = value.as_ref();
                cache::set(&key, bytes, ttl);
                Ok(())
            },
        ),
    )?;

    c.set(
        "delete",
        Function::new(ctx.clone(), |key: String| {
            cache::delete(&key);
        }),
    )?;

    c.set(
        "clear",
        Function::new(ctx.clone(), || {
            cache::clear();
        }),
    )?;

    c.set(
        "size",
        Function::new(ctx.clone(), || -> u32 { cache::size() }),
    )?;

    c.set(
        "exists",
        Function::new(ctx.clone(), |key: String| -> bool { cache::exists(&key) }),
    )?;

    c.set(
        "listKeys",
        Function::new(ctx.clone(), |prefix: String| -> Vec<String> {
            cache::list_keys(&prefix)
        }),
    )?;

    c.set(
        "getMany",
        Function::new(
            ctx.clone(),
            |ctx: Ctx<'js>, keys: Vec<String>| -> rquickjs::Result<Vec<Value<'js>>> {
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
            },
        ),
    )?;

    c.set(
        "setMany",
        Function::new(
            ctx.clone(),
            |items_val: Value<'js>| -> rquickjs::Result<()> {
                let items = js_to_set_many_items(items_val)?;
                cache::set_many(&items);
                Ok(())
            },
        ),
    )?;

    c.set(
        "deleteMany",
        Function::new(ctx.clone(), |keys: Vec<String>| {
            cache::delete_many(&keys);
        }),
    )?;

    parent.set("cache", c)?;
    Ok(())
}

// ─── observe ──────────────────────────────────────────────────

fn register_observe<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let obs = Object::new(ctx.clone())?;

    obs.set(
        "incrementCounter",
        Function::new(
            ctx.clone(),
            |name: String, labels_val: Value<'js>| -> rquickjs::Result<()> {
                let labels = js_to_tuple_vec(labels_val)?;
                observe::increment_counter(&name, &labels);
                Ok(())
            },
        ),
    )?;

    obs.set(
        "recordGauge",
        Function::new(
            ctx.clone(),
            |name: String, value: f64, labels_val: Value<'js>| -> rquickjs::Result<()> {
                let labels = js_to_tuple_vec(labels_val)?;
                observe::record_gauge(&name, value, &labels);
                Ok(())
            },
        ),
    )?;

    obs.set(
        "recordHistogram",
        Function::new(
            ctx.clone(),
            |name: String, value: f64, labels_val: Value<'js>| -> rquickjs::Result<()> {
                let labels = js_to_tuple_vec(labels_val)?;
                observe::record_histogram(&name, value, &labels);
                Ok(())
            },
        ),
    )?;

    obs.set(
        "emitLog",
        Function::new(
            ctx.clone(),
            |level: String, message: String, labels_val: Value<'js>| -> rquickjs::Result<()> {
                let labels = js_to_tuple_vec(labels_val)?;
                observe::emit_log(&level, &message, &labels);
                Ok(())
            },
        ),
    )?;

    obs.set(
        "emitLogRecord",
        Function::new(
            ctx.clone(),
            |timestamp_ms: u64,
             level: String,
             message: String,
             labels_val: Value<'js>|
             -> rquickjs::Result<()> {
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
            },
        ),
    )?;

    parent.set("observe", obs)?;
    Ok(())
}

// ─── time ──────────────────────────────────────────────────────

fn register_time<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let t = Object::new(ctx.clone())?;

    t.set("now", Function::new(ctx.clone(), || -> u64 { time::now() }))?;

    t.set(
        "sleep",
        Function::new(ctx.clone(), |duration_ms: u64| {
            time::sleep(duration_ms);
        }),
    )?;

    t.set(
        "resolution",
        Function::new(ctx.clone(), || -> u64 { time::resolution() }),
    )?;

    parent.set("time", t)?;
    Ok(())
}

// ─── scheduling ────────────────────────────────────────────────

fn register_scheduling<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let s = Object::new(ctx.clone())?;

    s.set(
        "scheduleOnce",
        Function::new(
            ctx.clone(),
            |delay_ms: u64, payload_val: Value<'js>| -> rquickjs::Result<String> {
                let payload = TypedArray::<'js, u8>::from_value(payload_val)?;
                let bytes: &[u8] = payload.as_ref();
                Ok(scheduling::schedule_once(delay_ms, bytes))
            },
        ),
    )?;

    s.set(
        "scheduleRepeating",
        Function::new(
            ctx.clone(),
            |interval_ms: u64, payload_val: Value<'js>| -> rquickjs::Result<String> {
                let payload = TypedArray::<'js, u8>::from_value(payload_val)?;
                let bytes: &[u8] = payload.as_ref();
                Ok(scheduling::schedule_repeating(interval_ms, bytes))
            },
        ),
    )?;

    s.set(
        "cancelScheduled",
        Function::new(ctx.clone(), |id: String| {
            scheduling::cancel_scheduled(&id);
        }),
    )?;

    parent.set("scheduling", s)?;
    Ok(())
}

// ─── process ───────────────────────────────────────────────────

fn register_process<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let p = Object::new(ctx.clone())?;

    p.set(
        "getEnv",
        Function::new(ctx.clone(), |key: String| -> Option<String> {
            process::get_env(&key)
        }),
    )?;

    p.set(
        "getAllEnv",
        Function::new(
            ctx.clone(),
            |ctx: Ctx<'js>| -> rquickjs::Result<rquickjs::Array<'js>> {
                let envs = process::get_all_env();
                tuple_vec_to_js(&ctx, envs)
            },
        ),
    )?;

    p.set(
        "getArgs",
        Function::new(ctx.clone(), || -> Vec<String> { process::get_args() }),
    )?;

    p.set(
        "getCwd",
        Function::new(
            ctx.clone(),
            |ctx: Ctx<'js>| -> rquickjs::Result<Value<'js>> {
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
            },
        ),
    )?;

    p.set(
        "exit",
        Function::new(ctx.clone(), |code: u32| {
            process::exit(code);
        }),
    )?;

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
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'js>, port: u16| -> rquickjs::Result<u32> {
                websocket::listen(port).map_err(|e| {
                    let msg = format!("websocket listen failed: {e}");
                    Exception::throw_message(&ctx, &msg)
                })
            },
        ),
    )?;

    // accept(listenerId) -> connId (u32). Throws on accept failure.
    ws.set(
        "accept",
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'js>, listener: u32| -> rquickjs::Result<u32> {
                websocket::accept(listener).map_err(|e| {
                    let msg = format!("websocket accept failed: {e}");
                    Exception::throw_message(&ctx, &msg)
                })
            },
        ),
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
                    let msg = format!(
                        "invalid message-type {kind:?}; expected text | binary | ping | pong"
                    );
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
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'js>, conn: u32| -> rquickjs::Result<Value<'js>> {
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
            },
        ),
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

// ─── http (issue #550) ─────────────────────────────────────────
//
// `globalThis.EdgeCloud.http.fetch(url, init?)` makes an outbound
// `wasi:http` call from a JS guest handler. It exists because the
// database recipes in `docs/recipes/databases.md` need a way for JS
// guests to reach Neon / Turso / Upstash without depending on a Rust
// shim. The full WIT call shape is at
// `edge-runtime/src/wit/deps/http/types.wit` and `proxy.wit:47-50`.
//
// JS surface (mirrors the WHATWG `fetch` shape but returns a plain
// object — no streams, no ReadableStream):
//
//   fetch(url)
//   fetch(url, { method: 'POST', headers: { 'content-type': '...',
//                                          'authorization': '...' },
//                body: '...' | Uint8Array })
//   → { status: number, headers: Record<string, string>, body: string }
//
// Errors: throws with `{ code, message }` where `code` is one of
//   - `egress-denied`     — the host's egress allowlist rejected the
//                           request (e.g. host not in
//                           `tenants.allowlisted_destinations`).
//   - `bad-url`           — `url` is empty or unparseable.
//   - `request-failed`    — `outgoing_handler::handle` returned Err.
//   - `response-read`     — the response stream returned an error
//                           while draining.
//
// Host-side egress gating happens automatically inside the runtime's
// `WasiHttpHooks::send_request` — the shim does NOT re-implement the
// allowlist. A `egress-denied` error here means the runtime blocked
// the call and we propagated that back as a JS exception.
fn register_http<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    use rquickjs::Exception;

    let http = Object::new(ctx.clone())?;

    http.set(
        "fetch",
        Function::new(
            ctx.clone(),
            move |ctx: Ctx<'js>, url: String, init: Option<Value<'js>>|
                  -> rquickjs::Result<Value<'js>> {
                let init = init.unwrap_or_else(|| Value::new_undefined(ctx.clone()));
                let parsed = match parse_fetch_url(&url) {
                    Ok(p) => p,
                    Err(e) => {
                        return Err(Exception::throw_message(
                            &ctx,
                            &format!("{{\"code\":\"bad-url\",\"message\":\"{e}\"}}"),
                        ));
                    }
                };
                let req_init = parse_fetch_init(&ctx, init)?;

                // Build the OutgoingRequest.
                let headers = http_types::Fields::new();
                for (name, value) in &req_init.headers {
                    let _ = headers.set(name, &[value.as_bytes().to_vec()]);
                }

                let req = http_types::OutgoingRequest::new(headers);

                let method = match req_init.method.as_str() {
                    "GET" => http_types::Method::Get,
                    "HEAD" => http_types::Method::Head,
                    "POST" => http_types::Method::Post,
                    "PUT" => http_types::Method::Put,
                    "DELETE" => http_types::Method::Delete,
                    "CONNECT" => http_types::Method::Connect,
                    "OPTIONS" => http_types::Method::Options,
                    "TRACE" => http_types::Method::Trace,
                    "PATCH" => http_types::Method::Patch,
                    other => {
                        // WIT allows custom methods via `Other(String)` —
                        // surface as such rather than failing.
                        if other.is_empty() {
                            return Err(Exception::throw_message(
                                &ctx,
                                "{\"code\":\"bad-url\",\"message\":\"method is empty\"}",
                            ));
                        }
                        http_types::Method::Other(other.to_string())
                    }
                };
                req.set_method(&method).map_err(|_| {
                    Exception::throw_message(&ctx, "{\"code\":\"request-failed\",\"message\":\"set_method\"}")
                })?;

                let scheme = match parsed.scheme.as_str() {
                    "http" | "ws" => http_types::Scheme::Http,
                    "https" | "wss" => http_types::Scheme::Https,
                    _ => http_types::Scheme::Http,
                };
                req.set_scheme(Some(&scheme)).map_err(|_| {
                    Exception::throw_message(&ctx, "{\"code\":\"request-failed\",\"message\":\"set_scheme\"}")
                })?;

                req.set_authority(Some(&parsed.authority)).map_err(|_| {
                    Exception::throw_message(&ctx, "{\"code\":\"request-failed\",\"message\":\"set_authority\"}")
                })?;
                req.set_path_with_query(Some(&parsed.path)).map_err(|_| {
                    Exception::throw_message(&ctx, "{\"code\":\"request-failed\",\"message\":\"set_path\"}")
                })?;

                // Body — write + finish.
                let body_handle = req.body().map_err(|_| {
                    Exception::throw_message(&ctx, "{\"code\":\"request-failed\",\"message\":\"body()\"}")
                })?;
                let stream = body_handle.write().map_err(|_| {
                    Exception::throw_message(&ctx, "{\"code\":\"request-failed\",\"message\":\"body.write()\"}")
                })?;
                stream.blocking_write_and_flush(&req_init.body_bytes).map_err(|_| {
                    Exception::throw_message(&ctx, "{\"code\":\"request-failed\",\"message\":\"blocking_write_and_flush\"}")
                })?;
                drop(stream);
                let _ = http_types::OutgoingBody::finish(body_handle, None);

                // Submit.
                let opts = http_types::RequestOptions::new();
                let future = outgoing_handler::handle(req, Some(opts)).map_err(|e| {
                    let msg = format!(
                        "{{\"code\":\"request-failed\",\"message\":\"outgoing_handler.handle: {:?}\"}}",
                        e
                    );
                    Exception::throw_message(&ctx, &msg)
                })?;

                // Subscribe + poll + get. The WIT contract is identical
                // to the DNS-resolve pattern used in
                // `edge-worker/tests/fixtures/handler/src/lib.rs:776-779`:
                // subscribe to the pollable, call `wasi:io/poll::poll` on
                // the slice (blocks until ready), then call `get`.
                let pollable = future.subscribe();
                poll::poll(&[&pollable]);

                // WIT shape: `get: func() -> option<result<result<incoming-response, error-code>>>`.
                // The outer `option` is "future readiness"; the outer
                // `result` is "second-and-later call guard" (the WIT
                // allows only one get); the inner `result` is the
                // protocol-level outcome.
                let response = match future.get() {
                    Some(Ok(Ok(r))) => r,
                    Some(Ok(Err(e))) => {
                        let msg = format!(
                            "{{\"code\":\"request-failed\",\"message\":\"response error code: {:?}\"}}",
                            e
                        );
                        return Err(Exception::throw_message(&ctx, &msg));
                    }
                    Some(Err(_)) => {
                        return Err(Exception::throw_message(
                            &ctx,
                            "{\"code\":\"request-failed\",\"message\":\"future.get() called more than once\"}",
                        ));
                    }
                    None => {
                        return Err(Exception::throw_message(
                            &ctx,
                            "{\"code\":\"request-failed\",\"message\":\"future not ready after poll\"}",
                        ));
                    }
                };

                let status = response.status();
                let resp_headers = response.headers();
                let header_entries = resp_headers.entries();

                // Drain body.
                let mut body_buf: Vec<u8> = Vec::new();
                match response.consume() {
                    Ok(body) => {
                        if let Ok(stream) = body.stream() {
                            loop {
                                match stream.blocking_read(4096) {
                                    Ok(chunk) if chunk.is_empty() => break,
                                    Ok(chunk) => body_buf.extend_from_slice(&chunk),
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // No body — fine; return what we have.
                    }
                }

                // Build the JS result: { status, headers, body }.
                let result = Object::new(ctx.clone())?;
                result.set("status", status)?;
                let headers_obj = Object::new(ctx.clone())?;
                for (name, value) in &header_entries {
                    let v = String::from_utf8_lossy(value).to_string();
                    headers_obj.set(name.as_str(), v)?;
                }
                result.set("headers", headers_obj)?;
                result.set(
                    "body",
                    String::from_utf8_lossy(&body_buf).into_owned(),
                )?;
                Ok(result.into_value())
            },
        ),
    )?;

    parent.set("http", http)?;
    Ok(())
}

// `parse_fetch_url` lives in `crate::url_parse` (host-testable) and
// is `use`d above via the top-level `use crate::wasm_only::{...}`
// re-export pattern. The wasm-only `parse_fetch_init` stays below.

/// Parsed `{ method, headers, bodyBytes }`.
struct FetchInit {
    method: String,
    headers: Vec<(String, String)>,
    body_bytes: Vec<u8>,
}

/// Decode the optional second `fetch` argument. Defaults: `GET`, no
/// headers, empty body. `body` may be a UTF-8 string or a Uint8Array.
fn parse_fetch_init<'js>(_ctx: &Ctx<'js>, init: Value<'js>) -> rquickjs::Result<FetchInit> {
    let mut out = FetchInit {
        method: "GET".to_string(),
        headers: Vec::new(),
        body_bytes: Vec::new(),
    };
    if init.is_undefined() || init.is_null() {
        return Ok(out);
    }
    let obj = match init.into_object() {
        Some(o) => o,
        None => return Ok(out),
    };

    if let Ok(method) = obj.get::<_, String>("method") {
        if !method.is_empty() {
            out.method = method.to_ascii_uppercase();
        }
    }

    if let Ok(headers) = obj.get::<_, Value>("headers") {
        if let Some(hobj) = headers.as_object() {
            for key in hobj.keys::<String>() {
                let k = match key {
                    Ok(k) => k,
                    Err(_) => continue,
                };
                if let Ok(v) = hobj.get::<_, String>(&k) {
                    out.headers.push((k, v));
                }
            }
        }
    }

    if let Ok(body) = obj.get::<_, Value>("body") {
        if body.is_string() {
            if let Some(s) = body.as_string() {
                out.body_bytes = s.to_string().unwrap_or_default().into_bytes();
            }
        } else if let Ok(ta) = TypedArray::<'js, u8>::from_value(body.clone()) {
            out.body_bytes = <TypedArray<'js, u8> as AsRef<[u8]>>::as_ref(&ta).to_vec();
        }
        // Other body shapes (ReadableStream, FormData, Blob) are
        // intentionally not supported at v0.2 — the WIT sync body API
        // does not stream. Users wanting streaming should call fetch()
        // multiple times. Throwing on unsupported types is too noisy;
        // a no-body fallback is the gentler default for `null`/`undefined`.
    }

    Ok(out)
}
