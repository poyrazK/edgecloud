//! `hello` — minimal deployable edgeCloud FaaS handler.
//!
//! For any inbound HTTP request, returns a small JSON document with the
//! host that received the request, the path the request hit, and the
//! runtime's idea of wall-clock time. The point of the sample is to be
//! the smallest possible end-to-end-deployable guest component — it
//! exists so the preview CI in `.github/workflows/preview.yml` has a
//! real artifact to upload on every PR.
//!
//! ## Build
//!
//! ```sh
//! # 1. Compile the Rust source to a core wasm module.
//! cd samples/hello
//! cargo build --target wasm32-unknown-unknown --release
//!
//! # 2. Wrap the core module into a wasi:http component matching the
//! #    runtime's expected WIT version (wasi:http@0.2.1, wasi:io@0.2.1).
//! #    The `wasm32-wasip2` target embeds wit-component 0.241.x, which
//! #    emits @0.2.4 / @0.2.6 and is rejected by wasmtime 45.0.3's
//! #    wasi:http wiring. Building core + wrapping is the only path
//! #    that matches the runtime today.
//! wasm-tools component new \
//!   target/wasm32-unknown-unknown/release/hello.wasm \
//!   --world edge-runtime-handler \
//!   -o target/component.wasm
//! ```
//!
//! The `preview.yml` CI does the same two-step build, copies the
//! wrapped component to `target/wasm32-wasip2/release/hello.wasm` (the
//! path `edge deploy` looks for by default), and uploads it.

#![no_main]

wit_bindgen::generate!({
    world: "edge-runtime-handler",
    // Point at the host repo's existing wit-bindgen-compatible WIT tree
    // (edge-worker/tests/fixtures/wit/) instead of vendoring. The runtime's
    // own edge-runtime/src/wit/ is the source of truth for wasmtime's
    // resolver but is NOT directly usable by wit-bindgen: its `include
    // wasi:cli/command@0.2.1;` syntax is wasmtime-only, and its dep .wit
    // files don't carry top-level `package` declarations. The fixture tree
    // was explicitly adapted for wit-bindgen (with package decls and a
    // `wasi:http/outgoing-handler` import on the handler world).
    path: "../../edge-worker/tests/fixtures/wit",
    generate_all,
});

use crate::exports::wasi::http::incoming_handler::Guest;
use crate::wasi::http::types::{
    Fields, IncomingRequest, OutgoingResponse, ResponseOutparam,
};
use crate::edge::cloud::time;

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
        let path = req.path_with_query().unwrap_or_else(|| "/".to_string());
        let now = time::now();
        let body = format!(
            "{{\"hello\":\"world\",\"path\":\"{}\",\"now\":{}}}",
            escape_json(&path),
            now
        );
        return_json(out, 200, body.as_bytes());
    }
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