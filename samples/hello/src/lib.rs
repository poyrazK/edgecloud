//! `hello` — minimal deployable edgeCloud FaaS handler.
//!
//! For any inbound HTTP request, returns a small JSON document with the
//! host that received the request, the path the request hit, and the
//! runtime's idea of wall-clock time. The point of the sample is to be
//! the smallest possible end-to-end-deployable guest component — it
//! exists so the preview CI in `.github/workflows/preview.yml` has a
//! real artifact to upload on every PR.
//!
//! ## Query parameters
//!
//! - `size=N` (issue #664 smoke fixture) — return a body of exactly
//!   `N` bytes instead of the small JSON document. Capped at 1 MiB
//!   to bound the worker's memory + the ingress's pacing-window
//!   arithmetic. The body is a JSON envelope wrapping a `data`
//!   string of the requested size; default content-type is
//!   `application/json` so the smoke script's response-shape checks
//!   stay simple. Used by `scripts/dev-bandwidth-smoke.sh` to
//!   exercise the per-tenant bandwidth cap: with a `bandwidth_bps`
//!   cap set on the tenant, a `?size=10000` request takes a
//!   predictable, sized response and the smoke script asserts
//!   `time_total ≥ expected` (no pacing → instant; paced → slow).
//!
//! ## Build
//!
//! The CLI does the two-step build (cargo + wasm-tools wrap) for you:
//!
//! ```sh
//! cd samples/hello
//! ../../target/release/edge build
//! ```
//!
//! See `README.md` for why the `wasm32-wasip2` target alone is
//! insufficient (wasi:http@0.2.4 vs 0.2.1 mismatch with wasmtime 45.0.3).

#![no_main]

wit_bindgen::generate!({
    world: "edge-runtime-handler",
    // Canonical wit-bindgen-compatible WIT lives at the repo root
    // (`wit/edge-cloud.wit` + `wit/deps/*`). The runtime's own
    // `edge-runtime/src/wit/` is the source of truth for wasmtime's
    // resolver but is NOT directly usable by wit-bindgen: its `include
    // wasi:cli/command@0.2.1;` syntax is wasmtime-only, and its dep
    // .wit files don't carry top-level `package` declarations. The
    // top-level `wit/` tree is explicitly adapted for wit-bindgen
    // (with package decls and a `wasi:http/outgoing-handler` import
    // on the handler world). `edge-worker/tests/fixtures/wit` is a
    // symlink to the same tree, kept around for fixture-build scripts
    // that look for it there.
    path: "../../wit",
    generate_all,
});

use crate::exports::wasi::http::incoming_handler::Guest;
use crate::wasi::http::types::{
    Fields, IncomingRequest, OutgoingResponse, ResponseOutparam,
};
use crate::edge::cloud::time;

/// Maximum body size for the `?size=N` query parameter (issue #664
/// smoke fixture). 1 MiB is large enough for any plausible pacing
/// window (a 1 KB/s cap stretches 1 MiB over ~17 minutes) without
/// exhausting the worker's per-request memory budget. Requests over
/// this limit get a 400 response so the smoke script can't OOM the
/// worker by accident.
const MAX_SIZE: usize = 1024 * 1024;

struct Hello;
export!(Hello);

// The `wasi:cli/run` export is part of the `edge-runtime-handler` world
// (it pulls in `wasi:cli/command`), but the host never calls it — the
// handler dispatch path goes through `wasi:http/incoming-handler`. We
// still have to provide a stub so wit-bindgen generates the export.
impl crate::exports::wasi::cli::run::Guest for Hello {
    fn run() -> Result<(), ()> {
        // Unreachable in normal operation; the host dispatches per
        // request through the http handler below. If a misconfigured
        // host does call run(), returning Err makes the failure visible
        // in the host logs rather than silently exiting 0.
        Err(())
    }
}

impl Guest for Hello {
    fn handle(req: IncomingRequest, out: ResponseOutparam) {
        let path_with_query =
            req.path_with_query().unwrap_or_else(|| "/".to_string());

        // ?size=N smoke fixture (issue #664). When present, emit a
        // sized JSON envelope instead of the small default body so
        // the bandwidth-cap smoke can assert pacing behavior on a
        // body whose size the script controls. Reject N > 1 MiB with
        // 400 so a typo or hostile probe can't OOM the worker.
        if let Some(size) = parse_size_query(&path_with_query) {
            if size > MAX_SIZE {
                return_json(
                    out,
                    400,
                    format!("{{\"error\":\"size exceeds {} byte cap\"}}", MAX_SIZE)
                        .as_bytes(),
                );
                return;
            }
            let payload = "x".repeat(size);
            let body = format!(
                "{{\"size\":{},\"data\":\"{}\"}}",
                size,
                payload
            );
            return_json(out, 200, body.as_bytes());
            return;
        }

        let now = time::now();
        let body = format!(
            "{{\"hello\":\"world\",\"path\":\"{}\",\"now\":{}}}",
            escape_json(&path_with_query),
            now
        );
        return_json(out, 200, body.as_bytes());
    }
}

/// Parse `?size=N` from a path-with-query string. Returns `None`
/// when the parameter is absent or unparseable. Caps the parsed
/// value at 1 MiB so an OOM probe can't crash the worker — the
/// caller still needs to enforce MAX_SIZE for the canonical 400
/// response, but the cap here is the belt-and-braces second line.
fn parse_size_query(path_with_query: &str) -> Option<usize> {
    let qpos = path_with_query.find('?')?;
    let query = &path_with_query[qpos + 1..];
    for pair in query.split('&') {
        if let Some(eq) = pair.find('=') {
            let key = &pair[..eq];
            let value = &pair[eq + 1..];
            if key == "size" {
                return value.parse::<usize>().ok().map(|n| n.min(MAX_SIZE));
            }
        }
    }
    None
}

// ── helpers ──────────────────────────────────────────────────────────

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

/// Escape the four characters that JSON requires (`"`, `\`, control
/// chars < 0x20). The path component of an HTTP request can contain
/// any of these; emitting unescaped bytes into a JSON document is a
/// malformed-response bug that makes the sample worse than its
/// "hello world" name suggests.
fn escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}