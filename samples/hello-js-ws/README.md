# hello-js-ws

Long-running JS sample for the `edge:cloud/websocket` e2e test (issue #448).

## What this sample is

A minimal WebSocket echo server. The Rust shim at `src/lib.rs` is built for
the long-running `edge-runtime` world; it embeds the esbuild-bundled JS from
`src/handler.js` via `include_str!` and calls `globalThis.start({ wsPort })`
once per process boot. The synchronous JS `start()` runs inside
`ctx.with(...)` and never returns, which is what keeps the world alive — no
`runtime.idle()`, no re-invocation from the supervisor.

## How the port is wired

The worker allocates a port from its `PortPool` and threads it in via
`EDGE_WS_PORT` (see `edge-worker/src/supervisor.rs:972-978, 1217-1218`).
The shim reads it with `process.getEnv("EDGE_WS_PORT")` and passes it
to the JS `start()` function as `wsPort`. The JS calls
`websocket.listen(wsPort)` — the host's `WebSocket::listen` actually
binds the `TcpListener` (`edge-runtime/src/interfaces/websocket.rs:158-168`).

## How to build

```bash
# from samples/hello-js-ws/
npm install
npx esbuild src/handler.js --bundle --format=iife --platform=neutral \
  --outfile=.edge/bundle.js
EDGE_JS_BUNDLE=$PWD/.edge/bundle.js \
  cargo build --target wasm32-wasip1 --release
```

Then `edge build` (from the repo root or with
`edge build --manifest-path samples/hello-js-ws/edge.toml`) wraps the
core module with `wasm-tools component new --adapt <wasi adapter> -o
target/javy/hello-js-ws.wasm`.

## How to test end-to-end

`edge-worker/tests/js_websocket_e2e.rs` (added in the same PR) is the
e2e test. It loads the committed `edge-worker/tests/fixtures/
js_websocket_handler.wasm`, spins up a real supervisor against a
wiremock'd control plane, TCP-probes the bound port, completes the
RFC 6455 Upgrade handshake, and asserts a text frame round-trips
byte-for-byte. See the test's docstring for the exact flow.

If a future change to the shim or its dependencies produces a
different `.wasm`, `edge-worker/tests/test_js_websocket_fixture_match.rs`
trips on the SHA-256 pin; rebuild the fixture and update the pin.

## Why a separate long-running JS sample, not a change to `samples/hello-js`

`samples/hello-js` is FaaS-only — `edge.toml` line: `world =
"edge-runtime-handler"`. The FaaS world cannot accept WebSocket
upgrades (issue #326 #3): the host owns the TCP listener, and the
per-request JS runtime is destroyed between requests. Adding a
long-running entry to `edge-js-runtime` is impossible within a single
`wit_bindgen::generate!` invocation, so this sample pairs a small
Rust long-running shim with an esbuild-bundled JS file. See the
implementation plan that drove this work for the full design (search
the git history for "Issue #448: JS WebSocket e2e" plan documents,
or `git log --all --oneline | grep -i "#448"` for the commits that
landed this PR).
