// hello-js — built with edgeCloud (JavaScript via the QuickJS
// `edge-js-runtime`).
//
// The runtime's `globalThis.handleRequest(req)` contract matches the
// canonical contract used by `samples/hello-js/src/handler.js` and
// the `edge-js-runtime` build:
//   - `req` is a plain JS object: { method, path, headers, body }.
//   - return value is a plain object: { status, body, contentType }.
//
// This file used to declare an `export async function handle(request)`
// (Fetch-style); that contract was tied to a Javy-built preview
// (deleted). The current runtime (issue #425) calls
// `globalThis.handleRequest`; reconciling this file to match keeps
// the sample compilable through `edge build` and runnable in the
// quickstart.

globalThis.handleRequest = function (req) {
  return {
    status: 200,
    body: JSON.stringify({
      hello: "world",
      path: req.path,
      method: req.method,
    }),
    contentType: "application/json",
  };
};
