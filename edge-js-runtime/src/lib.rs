#![no_main]

mod edge_modules;

// Generate bindings matching the handler world.
//
// The `path` argument points at the canonical WIT under the `edge-runtime`
// crate, not at the (now-removed) repo-root `wit/` copy. There is only one
// canonical `edge-cloud.wit` in this repo, sourced from
// `edge-runtime/src/wit/edge-cloud.wit` — keeping both crates bound to the
// same canonical file prevents the two copies from drifting the way they
// did before issue #422. The `wasi:cli/command@0.2.1` include + seven
// `edge:cloud/*` imports are declared in that file.
wit_bindgen::generate!({
    world: "edge-runtime-handler",
    path: "../edge-runtime/src/wit",
    generate_all,
});

use exports::wasi::http::incoming_handler::Guest;
use wasi::http::types::{
    Fields, IncomingRequest, OutgoingResponse, ResponseOutparam,
};

/// The user's bundled JS, embedded at compile time by build.rs.
const USER_JS: &str = include_str!(concat!(env!("OUT_DIR"), "/bundle.js"));

struct JsHandler;
export!(JsHandler);

// Stub for wasi:cli/run (required by the world, never called by the host).
impl exports::wasi::cli::run::Guest for JsHandler {
    fn run() -> Result<(), ()> {
        Err(())
    }
}

impl Guest for JsHandler {
    fn handle(req: IncomingRequest, out: ResponseOutparam) {
        // 1. Create QuickJS runtime and context
        let rt = rquickjs::Runtime::new().expect("quickjs runtime");
        let ctx = rquickjs::Context::full(&rt).expect("quickjs context");

        ctx.with(|ctx| {
            // 2. Register edge:cloud modules on globalThis.EdgeCloud
            edge_modules::register_all(&ctx).expect("register edge modules");

            // 3. Build the request object for JS
            let method = format!("{:?}", req.method());
            let path = req.path_with_query().unwrap_or_else(|| "/".into());
            let headers_handle = req.headers();
            let header_entries = headers_handle.entries();

            let js_req = rquickjs::Object::new(ctx.clone()).unwrap();
            js_req.set("method", method).unwrap();
            js_req.set("path", path).unwrap();

            // Convert headers to JS object
            let js_headers = rquickjs::Object::new(ctx.clone()).unwrap();
            for (name, value) in &header_entries {
                let val_str = String::from_utf8_lossy(value).to_string();
                js_headers.set(name.as_str(), val_str).unwrap();
            }
            js_req.set("headers", js_headers).unwrap();

            // Read request body if present
            let body_str = read_incoming_body(&req);
            js_req.set("body", body_str).unwrap();

            // 4. Set the request on globalThis so the user's handler can access it
            ctx.globals().set("__req", js_req).unwrap();

            // 5. Execute user code
            ctx.eval::<(), _>(USER_JS).expect("user JS eval");

            // 6. Call the handleRequest function
            let result: rquickjs::Value = ctx
                .eval("globalThis.handleRequest(__req)")
                .expect("handleRequest call");

            // 7. Extract response fields from JS result
            let (status, resp_body, content_type) = extract_response(&ctx, result);

            // 8. Send HTTP response
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
