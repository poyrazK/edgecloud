# samples/hello-js

Minimal deployable [edgeCloud](https://github.com/poyrazK/edgecloud) FaaS
handler in JavaScript. The handler at `src/handler.js` reads the request
and returns a small JSON document:

```json
{"hello":"world","path":"/the/request/path","method":"GET"}
```

The QuickJS host in `edge-js-runtime` (issue #317) compiles the user's
JS to a `wasm32-wasip1` core module and wraps it into a Preview 2
component via `wasm-tools component new --adapt` (the wasi-preview1
reactor adapter). The wrapped component is what `edge build` emits to
`target/javy/hello-js.wasm`.

## Requirements

- Rust toolchain with `wasm32-wasip1` target:
  `rustup target add wasm32-wasip1`
- `wasm-tools` 1.252.x on `PATH`:
  `cargo install wasm-tools --locked --version "^1.252"`
  The CLI's `edge build` globs `$CARGO_HOME/registry/.../wasi-preview1-component-adapter-provider-*/artefacts/wasi_snapshot_preview1.reactor.wasm`
  to find the wasi-preview1 reactor adapter; this glob is populated
  when `wasm-tools` is installed (the adapter is a transitive
  dep of `wasi-preview1-component-adapter-provider`).
- Node 20+ (for `npm install` and the esbuild bundling step).
- `edge` CLI on `PATH` (`cargo install --path edge-cli`).

## Build

```sh
cd samples/hello-js
npm install                    # resolves @edgecloud/sdk from local edge-js-sdk
edge build --lang=js           # bundles src/handler.js, builds edge-js-runtime, wraps with adapter
```

`edge build` writes the wrapped component to `target/javy/hello-js.wasm`.

## Deploy

```sh
edge deploy
```

## Layout

```
samples/hello-js/
├── edge.toml         # [project] name = "hello-js", language = "js"
├── package.json      # @edgecloud/sdk from local edge-js-sdk (file:../../edge-js-sdk)
├── src/
│   └── handler.js    # globalThis.handleRequest(req) → {status, body, contentType}
└── README.md         # this file
```
