# samples/hello-js

Minimal deployable [edgeCloud](https://github.com/poyrazK/edgecloud) FaaS
handler in JavaScript. The handler at `src/handler.js` reads the request
and returns a small JSON document:

```json
{"hello":"world","path":"/the/request/path","method":"GET"}
```

The QuickJS host in `edge-js-runtime` (issue #317) is built directly
for `wasm32-wasip2` — the cargo target emits a complete WASI
Preview 2 component natively, so no `wasm-tools component new
--adapt` wrap step (or wasi-preview1 reactor adapter) is needed.
The component is what `edge build` emits to
`target/javy/hello-js.wasm`.

## Requirements

- Rust toolchain with `wasm32-wasip2` target:
  `rustup target add wasm32-wasip2`
- `wasm-tools` 1.252.x on `PATH`:
  `cargo install wasm-tools --locked --version "^1.252"`
  (`wasm-tools` is required by the Rust guest pipeline — `edge build
  --lang=rust`, `edge-migrate`, the worker fixture build — NOT by
  the JS pipeline anymore.)
- Node 20+ (for `npm install` and the esbuild bundling step).
- `edge` CLI on `PATH` (`cargo install --path edge-cli`).

## Build

```sh
cd samples/hello-js
npm install                    # resolves @edgecloud/sdk from npm (^0.2.0)
edge build --lang=js           # bundles src/handler.js, builds edge-js-runtime for wasm32-wasip2
```

`edge build` writes the component to `target/javy/hello-js.wasm`.

## Deploy

```sh
edge deploy
```

## Layout

```
samples/hello-js/
├── edge.toml         # [project] name = "hello-js", language = "js"
├── package.json      # @edgecloud/sdk from npm (^0.2.0)
├── src/
│   └── handler.js    # globalThis.handleRequest(req) → {status, body, contentType}
└── README.md         # this file
```
