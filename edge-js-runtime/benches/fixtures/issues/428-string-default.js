// Issue #428: regression fixture.
//
// Three handler shapes, dispatched on the request path:
//   - `/string` returns a bare string. Content-Type must be
//     `text/plain; charset=utf-8`.
//   - `/object` returns an object without setting contentType.
//     Must default to `application/json`.
//   - `/explicit` returns an object with an explicit contentType.
//     The handler-supplied value must win.
//
// This file is fed through `edge build` (→ esbuild → embedded in
// the QuickJS artifact) and exercised by
// `edge-runtime/tests/js_fixture_load.rs::extract_response_picks_content_type`.

globalThis.handleRequest = function (req) {
  if (req.path === "/string") {
    return "hello world";
  }
  if (req.path === "/object") {
    return { status: 200, body: JSON.stringify({ ok: true }) };
  }
  if (req.path === "/explicit") {
    return {
      status: 200,
      body: "<html><body>hi</body></html>",
      contentType: "text/html; charset=utf-8",
    };
  }
  return { status: 404, body: "not found" };
};
