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
// `edge_modules` itself is split: only the WASI-bound pieces
// (`register_all` + per-namespace registrars) compile on the wasm
// target. The bench accesses the always-compiled `register_all_stub`.
pub mod edge_modules;

// ─── WASI bindings (wasm target only) ─────────────────────────────────
//
// Everything below references `wit_bindgen`-generated symbols and
// WASI http types. Gated behind `#[cfg(target_arch = "wasm32")]` so
// the host-target bench build skips this block and never tries to
// resolve `_wasi:cli/run@0.2.1` etc. at link time.
#[cfg(target_arch = "wasm32")]
mod wasm_only {
    use super::{compile_user_bundle, edge_modules, USER_BYTECODE};
    use exports::wasi::http::incoming_handler::Guest;
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
                if let Err(e) = edge_modules::register_all(&ctx) {
                    return send_error(out, &format!("register: {e}"));
                }

                // 4. Load cached bytecode → declared module (skips lex+parse).
                let module = match unsafe { rquickjs::module::Module::load(ctx.clone(), &bytecode) }
                {
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