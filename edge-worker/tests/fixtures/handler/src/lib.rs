//! `handler` — multi-path WASI Preview 2 wasi:http/incoming-handler.
//!
//! Used by Phase E L1–L10 integration tests. Implemented in Rust with
//! `wit-bindgen 0.45` (the version wasmtime 25.0.3 transitively pulls in).
//!
//! ## Paths
//!
//! | Path                          | Behavior                                    |
//! |-------------------------------|---------------------------------------------|
//! | `GET /`                       | 200, body `{"hello":"handler","path":"/"}`  |
//! | `GET /busy`                   | Busy-loops for ~5s, then 200                |
//! | `GET /env/{key}`              | process.get-env(key) → 200 with value       |
//! | `GET /time/now`               | time.now() → 200 with timestamp             |
//! | `GET /kv/set?key=x&val=y`     | kv-store.set(x, y) → 200 "ok"               |
//! | `GET /kv/get?key=x`           | kv-store.get(x) → 200 with value            |
//! | `GET /kv/del?key=x`           | kv-store.delete(x) → 200 "ok"               |
//! | `GET /cache/set?key=x&val=y`  | cache.set(x, y) → 200 "ok"                  |
//! | `GET /cache/get?key=x`        | cache.get(x) → 200 with value               |
//! | `GET /cache/del?key=x`        | cache.delete(x) → 200 "ok"                  |
//! | `GET /log?msg=hello`          | observe.emit-log("info", msg, []) → 200     |
//! | `GET /sched/once?ms=60000`    | scheduling.schedule-once(ms, []) → 200      |
//!
//! All other paths return 404.

#![no_main]

wit_bindgen::generate!({
    world: "edge-runtime-handler",
    path: "../wit",
    generate_all,
});

use crate::exports::wasi::http::incoming_handler::Guest;
use crate::wasi::http::types::{
    Fields, IncomingRequest, Method, OutgoingResponse, ResponseOutparam,
};

// Edge cloud interface imports — available via generate_all.
use crate::edge::cloud::kv_store;
use crate::edge::cloud::cache;
use crate::edge::cloud::process;
use crate::edge::cloud::time;
use crate::edge::cloud::observe;
use crate::edge::cloud::scheduling;

struct Component;
export!(Component);

impl crate::exports::wasi::cli::run::Guest for Component {
    fn run() -> Result<(), ()> {
        unreachable!("wasi:cli/run should not be called for a FaaS component")
    }
}

impl Guest for Component {
    fn handle(req: IncomingRequest, out: ResponseOutparam) {
        let full_path = req.path_with_query().unwrap_or_else(|| "/".to_string());
        let method = req.method();

        // Split path and query string.
        let (path, query) = match full_path.split_once('?') {
            Some((p, q)) => (p, q),
            None => (&full_path[..], ""),
        };

        if !matches!(method, Method::Get) {
            return_json(out, 405, br#"{"error":"method not allowed"}"#);
            return;
        }

        match path {
            "/" => {
                return_json(out, 200, br#"{"hello":"handler","path":"/"}"#);
            }
            "/busy" => {
                return_busy_then_ok(out);
            }
            // ── process ─────────────────────────────────────────────────
            p if p.starts_with("/env/") => {
                let key = &p[5..];
                let val = process::get_env(key);
                match val {
                    Some(v) => return_text(out, 200, v.as_bytes()),
                    None => return_text(out, 404, b"env var not found"),
                }
            }
            "/env" => {
                let all = process::get_all_env();
                let json = serde_json::to_string(&all).unwrap_or_else(|_| "[]".to_string());
                return_text(out, 200, json.as_bytes());
            }
            "/args" => {
                let args = process::get_args();
                let json = serde_json::to_string(&args).unwrap_or_else(|_| "[]".to_string());
                return_text(out, 200, json.as_bytes());
            }
            "/cwd" => {
                match process::get_cwd() {
                    Ok(p) => return_text(out, 200, p.as_bytes()),
                    Err(e) => return_text(out, 500, e.as_bytes()),
                }
            }
            // ── time ──────────────────────────────────────────────────
            "/time/now" => {
                let now = time::now();
                let body = format!("{now}");
                return_text(out, 200, body.as_bytes());
            }
            "/time/sleep" => {
                let ms: u64 = get_query_param(query, "ms")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(5);
                time::sleep(ms);
                return_text(out, 200, b"ok");
            }
            "/time/resolution" => {
                let r = time::resolution();
                let body = format!("{r}");
                return_text(out, 200, body.as_bytes());
            }
            // ── kv-store ──────────────────────────────────────────────
            "/kv/set" => {
                let key = get_query_param(query, "key").unwrap_or("");
                let val = get_query_param(query, "val").unwrap_or("");
                let ttl: Option<u32> = get_query_param(query, "ttl")
                    .and_then(|v| v.parse().ok());
                kv_store::set(key, val.as_bytes(), ttl);
                return_text(out, 200, b"ok");
            }
            "/kv/get" => {
                let key = get_query_param(query, "key").unwrap_or("");
                match kv_store::get(key) {
                    Some(data) => return_bytes(out, 200, &data),
                    None => return_text(out, 404, b"key not found"),
                }
            }
            "/kv/del" => {
                let key = get_query_param(query, "key").unwrap_or("");
                kv_store::delete(key);
                return_text(out, 200, b"ok");
            }
            "/kv/exists" => {
                let key = get_query_param(query, "key").unwrap_or("");
                let ok = kv_store::exists(key);
                return_text(out, 200, if ok { b"true" } else { b"false" });
            }
            "/kv/list" => {
                let prefix = get_query_param(query, "prefix").unwrap_or("");
                let keys = kv_store::list_keys(prefix);
                let json = serde_json::to_string(&keys).unwrap_or_else(|_| "[]".to_string());
                return_text(out, 200, json.as_bytes());
            }
            "/kv/clear" => {
                kv_store::clear();
                return_text(out, 200, b"ok");
            }
            "/kv/get-many" => {
                let keys = split_csv(get_query_param(query, "keys").unwrap_or(""));
                let vals = kv_store::get_many(&keys);
                let strings: Vec<Option<String>> = vals
                    .into_iter()
                    .map(|opt| opt.map(|v| String::from_utf8_lossy(&v).into_owned()))
                    .collect();
                let json = serde_json::to_string(&strings).unwrap_or_else(|_| "[]".to_string());
                return_text(out, 200, json.as_bytes());
            }
            "/kv/set-many" => {
                let keys = split_csv(get_query_param(query, "keys").unwrap_or(""));
                let vals = split_csv(get_query_param(query, "vals").unwrap_or(""));
                let items: Vec<(String, Vec<u8>, Option<u32>)> = keys
                    .into_iter()
                    .zip(vals.into_iter())
                    .map(|(k, v)| (k, v.into_bytes(), None))
                    .collect();
                kv_store::set_many(&items);
                return_text(out, 200, b"ok");
            }
            "/kv/del-many" => {
                let keys = split_csv(get_query_param(query, "keys").unwrap_or(""));
                kv_store::delete_many(&keys);
                return_text(out, 200, b"ok");
            }
            // ── cache ─────────────────────────────────────────────────
            "/cache/set" => {
                let key = get_query_param(query, "key").unwrap_or("");
                let val = get_query_param(query, "val").unwrap_or("");
                let ttl: Option<u32> = get_query_param(query, "ttl")
                    .and_then(|v| v.parse().ok());
                cache::set(key, val.as_bytes(), ttl);
                return_text(out, 200, b"ok");
            }
            "/cache/get" => {
                let key = get_query_param(query, "key").unwrap_or("");
                match cache::get(key) {
                    Some(data) => return_bytes(out, 200, &data),
                    None => return_text(out, 404, b"key not found"),
                }
            }
            "/cache/del" => {
                let key = get_query_param(query, "key").unwrap_or("");
                cache::delete(key);
                return_text(out, 200, b"ok");
            }
            "/cache/exists" => {
                let key = get_query_param(query, "key").unwrap_or("");
                let ok = cache::exists(key);
                return_text(out, 200, if ok { b"true" } else { b"false" });
            }
            "/cache/list" => {
                let prefix = get_query_param(query, "prefix").unwrap_or("");
                let keys = cache::list_keys(prefix);
                let json = serde_json::to_string(&keys).unwrap_or_else(|_| "[]".to_string());
                return_text(out, 200, json.as_bytes());
            }
            "/cache/size" => {
                let n = cache::size();
                let body = format!("{n}");
                return_text(out, 200, body.as_bytes());
            }
            "/cache/clear" => {
                cache::clear();
                return_text(out, 200, b"ok");
            }
            "/cache/get-many" => {
                let keys = split_csv(get_query_param(query, "keys").unwrap_or(""));
                let vals = cache::get_many(&keys);
                let strings: Vec<Option<String>> = vals
                    .into_iter()
                    .map(|opt| opt.map(|v| String::from_utf8_lossy(&v).into_owned()))
                    .collect();
                let json = serde_json::to_string(&strings).unwrap_or_else(|_| "[]".to_string());
                return_text(out, 200, json.as_bytes());
            }
            "/cache/set-many" => {
                let keys = split_csv(get_query_param(query, "keys").unwrap_or(""));
                let vals = split_csv(get_query_param(query, "vals").unwrap_or(""));
                let items: Vec<(String, Vec<u8>, Option<u32>)> = keys
                    .into_iter()
                    .zip(vals.into_iter())
                    .map(|(k, v)| (k, v.into_bytes(), None))
                    .collect();
                cache::set_many(&items);
                return_text(out, 200, b"ok");
            }
            "/cache/del-many" => {
                let keys = split_csv(get_query_param(query, "keys").unwrap_or(""));
                cache::delete_many(&keys);
                return_text(out, 200, b"ok");
            }
            // ── observe ───────────────────────────────────────────────
            "/log" => {
                let msg = get_query_param(query, "msg").unwrap_or("no message");
                observe::emit_log("info", msg, &[]);
                return_text(out, 200, b"ok");
            }
            "/observe/counter" => {
                let name = get_query_param(query, "name").unwrap_or("test");
                let val: u64 = get_query_param(query, "val")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(1);
                for _ in 0..val {
                    observe::increment_counter(name, &[]);
                }
                return_text(out, 200, b"ok");
            }
            "/observe/gauge" => {
                let name = get_query_param(query, "name").unwrap_or("test");
                let val: f64 = get_query_param(query, "val")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0.0);
                observe::record_gauge(name, val, &[]);
                return_text(out, 200, b"ok");
            }
            "/observe/histogram" => {
                let name = get_query_param(query, "name").unwrap_or("test");
                let val: f64 = get_query_param(query, "val")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0.0);
                observe::record_histogram(name, val, &[]);
                return_text(out, 200, b"ok");
            }
            // ── scheduling ────────────────────────────────────────────
            "/sched/once" => {
                let ms: u64 = get_query_param(query, "ms")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60_000);
                let id = scheduling::schedule_once(ms, b"");
                return_text(out, 200, id.as_bytes());
            }
            "/sched/repeat" => {
                let ms: u64 = get_query_param(query, "ms")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60_000);
                let id = scheduling::schedule_repeating(ms, b"");
                return_text(out, 200, id.as_bytes());
            }
            "/sched/cancel" => {
                let id = get_query_param(query, "id").unwrap_or("");
                scheduling::cancel_scheduled(id);
                return_text(out, 200, b"ok");
            }
            // ── SSE / streaming ────────────────────────────────────
            "/sse" => {
                let count: usize = get_query_param(query, "count")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(5);

                let headers = Fields::new();
                let _ = headers.set("content-type", &[b"text/event-stream".to_vec()]);
                let _ = headers.set("cache-control", &[b"no-cache".to_vec()]);
                let resp = OutgoingResponse::new(headers);
                resp.set_status_code(200).unwrap();
                let body_handle = resp.body().expect("response body");
                let stream = body_handle.write().expect("output stream");

                // Deliver headers immediately — the host starts serving
                // the response while we continue writing body chunks.
                ResponseOutparam::set(out, Ok(resp));

                for i in 0..count {
                    let msg = format!("id: {}\ndata: {{\"event\":{}}}\n\n", i, i);
                    stream.blocking_write_and_flush(msg.as_bytes()).unwrap();
                }
                let _ =
                    crate::wasi::http::types::OutgoingBody::finish(body_handle, None);
            }
            _ => {
                return_json(out, 404, br#"{"error":"not found"}"#);
            }
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn return_json(out: ResponseOutparam, status: u16, body: &[u8]) {
    let headers = Fields::new();
    let _ = headers.set("content-type", &[b"application/json".to_vec()]);
    let resp = OutgoingResponse::new(headers);
    resp.set_status_code(status).unwrap();
    let body_handle = resp.body().expect("response body");
    let stream = body_handle.write().expect("output stream");
    stream.blocking_write_and_flush(body).unwrap();
    drop(stream);
    let _ = crate::wasi::http::types::OutgoingBody::finish(body_handle, None);
    crate::wasi::http::types::ResponseOutparam::set(out, Ok(resp));
}

fn return_text(out: ResponseOutparam, status: u16, body: &[u8]) {
    let headers = Fields::new();
    let _ = headers.set("content-type", &[b"text/plain".to_vec()]);
    let resp = OutgoingResponse::new(headers);
    resp.set_status_code(status).unwrap();
    let body_handle = resp.body().expect("response body");
    let stream = body_handle.write().expect("output stream");
    stream.blocking_write_and_flush(body).unwrap();
    drop(stream);
    let _ = crate::wasi::http::types::OutgoingBody::finish(body_handle, None);
    crate::wasi::http::types::ResponseOutparam::set(out, Ok(resp));
}

fn return_bytes(out: ResponseOutparam, status: u16, body: &[u8]) {
    let headers = Fields::new();
    let _ = headers.set("content-type", &[b"application/octet-stream".to_vec()]);
    let resp = OutgoingResponse::new(headers);
    resp.set_status_code(status).unwrap();
    let body_handle = resp.body().expect("response body");
    let stream = body_handle.write().expect("output stream");
    stream.blocking_write_and_flush(body).unwrap();
    drop(stream);
    let _ = crate::wasi::http::types::OutgoingBody::finish(body_handle, None);
    crate::wasi::http::types::ResponseOutparam::set(out, Ok(resp));
}

/// Parse a query parameter from a query string like "key=a&val=b".
fn get_query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == name {
                return Some(v);
            }
        }
    }
    None
}

/// Split a comma-separated value into individual strings.
/// Handles URL-encoded commas (%2C → not supported — use raw commas).
fn split_csv(s: &str) -> Vec<String> {
    if s.is_empty() {
        Vec::new()
    } else {
        s.split(',').map(|p| p.to_string()).collect()
    }
}

/// Busy loop — calibrated to exceed ~5s of Wasm execution at default
/// 1ms epoch ticks. A 100ms budget will trap this at the 10th tick.
fn return_busy_then_ok(out: ResponseOutparam) {
    let mut counter: u64 = 0;
    for i in 0u64..5_000_000_000 {
        counter = counter.wrapping_mul(31).wrapping_add(i);
    }
    core::hint::black_box(counter);
    return_json(out, 200, br#"{"slept":"~5s in Wasm"}"#);
}
