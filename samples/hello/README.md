# samples/hello

Minimal deployable [edgeCloud](https://github.com/poyrazK/edgecloud) FaaS
handler. For any inbound HTTP request it returns a small JSON document:

```json
{"hello":"world","path":"/the/request/path","now":1717689600000}
```

The point of the sample is to be the smallest possible end-to-end-deployable
guest component. It exists so the preview CI in
[`.github/workflows/preview.yml`](../../.github/workflows/preview.yml) has
a real artifact to upload on every PR, and so a new edgeCloud tenant has
something to fork and modify when they're learning the `wasi:http` guest
interface.

## Build

The two-step build is required because of a WIT-version mismatch between
the `wasm32-wasip2` target (which embeds `wit-component 0.241.x` and emits
`wasi:io@0.2.6` / `wasi:http/types@0.2.4`) and `wasmtime 45.0.3` (which
expects `wasi:io@0.2.1` / `wasi:http/types@0.2.1`). The matching
toolchain is `wasm32-unknown-unknown` core module + `wasm-tools component
new` wrapping with `--world edge-runtime-handler`.

```sh
cd samples/hello
cargo build --target wasm32-unknown-unknown --release
wasm-tools component new \
  target/wasm32-unknown-unknown/release/hello.wasm \
  --world edge-runtime-handler \
  -o target/component.wasm
```

The wrapped `target/component.wasm` is what `edge deploy` uploads. The
preview CI copies it to `target/wasm32-wasip2/release/hello.wasm` (the
path the CLI looks at by default) before invoking `edge deploy --preview`.

## Deploy

```sh
EDGE_API_KEY=... EDGE_API_URL=https://api.edgecloud.dev \
  edge deploy --preview
```

The CLI prints the deployed URL on its own line (`  URL: <url>`), which
the preview composite action captures and posts to the originating PR.

## Layout

```
samples/hello/
├── Cargo.toml         # crate-type = ["cdylib"], isolated [workspace]
├── edge.toml          # [project] name = "hello", [deployment] api = ...
├── README.md          # this file
└── src/
    └── lib.rs         # wasi:http/incoming-handler implementation
```

The WIT tree used by `wit-bindgen` lives in
[`edge-worker/tests/fixtures/wit/`](../../edge-worker/tests/fixtures/wit/)
and is referenced via the `path: "../../edge-worker/tests/fixtures/wit"`
field in `src/lib.rs`. The runtime's own WIT at
[`edge-runtime/src/wit/`](../../edge-runtime/src/wit/) is the source of
truth for wasmtime's resolver but isn't directly usable by
`wit-bindgen` — its `include wasi:cli/command@0.2.1;` syntax is
wasmtime-only and its dep `.wit` files don't carry top-level
`package` declarations. The fixture tree was explicitly adapted for
`wit-bindgen` (with package decls and a `wasi:http/outgoing-handler`
import on the handler world), so the sample points at it instead of
duplicating 33 files.