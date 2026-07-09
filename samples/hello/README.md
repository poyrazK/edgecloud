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

The CLI does the two-step build (cargo + `wasm-tools component new`
wrap) for you:

```sh
cd samples/hello
../../target/release/edge build
```

That command runs `cargo build --target wasm32-unknown-unknown --release`
and then `wasm-tools component new <core> --world <detected>
--wit-dir ../../wit -o target/component.wasm`. The wrapped
`target/component.wasm` is what `edge deploy` uploads.

### Why the two-step build exists

rustc 1.93.0's `wasm32-wasip2` target embeds `wit-component 0.241.x` in
the produced core module, which emits `wasi:io@0.2.6` and
`wasi:http/types@0.2.4`. Wasmtime 45.0.3 (the version this repo ships
in `edge-runtime` and `edge-worker`) is built against the WASI WIT
files at `edge-runtime/src/wit/deps/`, which declare
`wasi:http@0.2.1` / `wasi:io@0.2.1`. The component model's resolver
rejects any component that imports a higher minor version than the
linker was built with — the load fails with a `wasi:http` import
mismatch before any guest code runs.

Building the core module with `wasm32-unknown-unknown` (which doesn't
embed the buggy `wit-component`) and then wrapping it with
`wasm-tools component new` produces a component that uses the
matching `wasi:http@0.2.1` interface. The `wasm-tools 1.252.x` default
adapter is what makes the wrap step "just work" — no `--adapt` flag
required.

## Deploy

```sh
EDGE_API_KEY=... EDGE_API_URL=https://api.edgecloud.dev \
  edge deploy --preview
```

The CLI prints the deployed URL on its own line (`  URL: <url>`), which
the preview composite action captures and posts to the originating PR.

### Preview environment flags (issue #308)

`edge deploy --preview` accepts two optional flags the composite action
forwards automatically:

| Flag | Default | What it does |
|---|---|---|
| `--pr-number=<N>` | unset | Stamps `EDGE_PREVIEW_PR_NUMBER=<N>` into the guest env. The composite action sets this to `${{ github.event.pull_request.number }}`. |
| `--preview-ttl=<duration>` | `168h` (7d) | Override the per-deploy TTL. The PreviewGCService deletes the row + artifact blob after the deadline passes. |

The sample doesn't read `EDGE_PREVIEW_PR_NUMBER` today — adding it is
straightforward:

```rust
// src/lib.rs (excerpt)
fn handler(req: Request) -> Result<Response, ErrorCode> {
    let env = process::get_environment();
    let pr_number = env.into_iter()
        .find(|(k, _)| k == "EDGE_PREVIEW_PR_NUMBER")
        .map(|(_, v)| v);
    // render the response with `pr_number` when set
    // ...
}
```

The guest's `process.get_environment` returns the merged env map
(declarative app env + runtime-injected vars); `EDGE_PREVIEW_PR_NUMBER`
is one of the runtime-injected ones, set when the deployment was
uploaded with `?preview-pr-number=<N>`. Non-preview deploys simply
don't see the key — branch on presence, not on empty string.

## Layout

```
samples/hello/
├── Cargo.toml         # crate-type = ["cdylib"], isolated [workspace]
├── edge.toml          # [project] name + target, [deployment] api
├── README.md          # this file
└── src/
    └── lib.rs         # wasi:http/incoming-handler implementation
```

The WIT tree used by `wit-bindgen` lives at
[`wit/`](../../wit/) (with `wit/deps/*` for the WASI 0.2.1 deps) and
is referenced via the `path: "../../wit"` field in `src/lib.rs`. The
runtime's own WIT at
[`edge-runtime/src/wit/`](../../edge-runtime/src/wit/) is the source of
truth for wasmtime's resolver but isn't directly usable by
`wit-bindgen` — its `include wasi:cli/command@0.2.1;` syntax is
wasmtime-only and its dep `.wit` files don't carry top-level
`package` declarations. The top-level `wit/` tree is explicitly adapted
for `wit-bindgen` (with package decls and a `wasi:http/outgoing-handler`
import on the handler world), so the sample points at it instead of
duplicating 33 files. The historical
`edge-worker/tests/fixtures/wit/` path is now a symlink to `wit/`.