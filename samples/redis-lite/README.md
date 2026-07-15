# samples/redis-lite

A minimal RESP ([Redis wire format][resp]) server for
[edgeCloud](https://github.com/poyrazK/edgecloud)'s L4/TCP long-running
guest path. Speaks RESP2 over `wasi:sockets/tcp` and persists state via
`edge:cloud/kv-store` so values survive a worker restart (per issue
#495's restart-persistence requirement).

## Commands

| Command                  | Reply                                            |
| ------------------------ | ------------------------------------------------ |
| `PING`                   | `+PONG\r\n`                                      |
| `ECHO <msg>`             | bulk-string carrying `<msg>`                     |
| `SET <key> <value>`      | `+OK\r\n` (persistent, no TTL)                   |
| `GET <key>`              | bulk-string reply, or `$-1\r\n` if missing       |
| `DEL <key>`              | `:1\r\n` if it existed, `:0\r\n` if not          |
| anything else            | `-ERR unknown command\r\n`                       |
| wrong-arity command      | `-ERR wrong number of arguments\r\n`             |

## Session example

```text
$ public_port=$(curl -sH "Authorization: Bearer $EDGE_API_KEY" \
  http://localhost:8080/api/v1/apps/redis-lite/l4-port | jq -r .public_port)

$ printf '*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n*2\r\n$4\r\nECHO\r\n$5\r\nworld\r\n' \
    | nc -w 2 localhost "$public_port"
+PONG
+OK
$5
hello
:1
$-1
$5
world
```

The Redis CLI also works natively:

```text
$ redis-cli -p $public_port PING
PONG
$ redis-cli -p $public_port SET foo bar
OK
$ redis-cli -p $public_port GET foo
"bar"
```

## Why long-running (`edge-runtime`) and not FaaS (`edge-runtime-handler`)?

The FaaS world is HTTP-only by construction — the worker owns the
TCP listener and the guest is invoked once per HTTP request. A
raw-TCP server has to own its socket for the duration of the process
(Redis clients, MQTT brokers, custom protocols don't fit the FaaS
shape). The `edge-runtime` world exposes the full
`wasi:cli/command@0.2.1` family (the workspace `wit/edge-cloud.wit`
`include`s it), so the guest can `bind` + `accept` itself. `wit-bindgen`
maps the WASI sockets types into the guest's `crate::wasi::sockets::tcp`
namespace; see [`src/lib.rs`](src/lib.rs).

## Build

The CLI does the two-step build (cargo + wasm-tools wrap) for you:

```sh
cd samples/redis-lite
../../target/release/edge build
```

That command runs `cargo build --target wasm32-unknown-unknown --release`
and then `wasm-tools component new <core> -o target/component.wasm`. The
wrapped `target/component.wasm` is what `edge deploy` uploads. See the
"Why the two-step build exists" section in [`samples/hello-tcp/README.md`](../hello-tcp/README.md#build)
for the `wasi:http@0.2.1` ↔ `wit-component` version-drift story.

## Deploy

```sh
EDGE_API_KEY=... EDGE_API_URL=https://api.edgecloud.dev \
  edge deploy
```

The CLI calls `POST /api/v1/apps/redis-lite/l4-port` (issue #548, Commit 9)
on the control plane, which atomically allocates a public port in the
`L4_PORT_RANGE_START..=L4_PORT_RANGE_END` window (default `31000..=31999`)
and returns it. `edge tcp-info redis-lite` prints the same port back.

## Persistence

`edge:cloud/kv-store` is **scoped per-tenant** — values written by this
guest live in `<EDGE_KV_STORE_PATH>/<tenant_id>/store.json` on the
worker (see [`edge-runtime/src/interfaces/kv_store.rs`](../../edge-runtime/src/interfaces/kv_store.rs)).
Two apps of the same tenant share the same key namespace (issue #558).
The host's `KvStore` writes the JSON store atomically via
`rename-to-replace`, so a worker restart reopens the same dir and
SETs from before the restart are still visible.

Restart persistence is exercised **manually** via `redis-cli` against
a real `edge deploy`. The wasmtime LR epoch model — 1 s budget for
`start()` — prevents a full RESP round-trip in CI. The persistence
test at
`edge-worker/tests/redis_lite_e2e.rs::redis_lite_persists_kv_across_supervisor_restart`
covers the same path in-process but is `#[ignore]`'d for the same
reason, and additionally carries a doc-only caveat: the static
`KV_STORES` cache at `edge-runtime/src/runtime.rs:80` lives for the
process lifetime, so an in-process supervisor restart reuses the
in-memory store unless a follow-up runtime-side escape hatch lands
(filed separately). Run the manual test via
`cargo test --manifest-path edge-worker/Cargo.toml --test redis_lite_e2e \
   -- --ignored --nocapture`.

## Security

This sample has **no authentication**. Anyone with TCP reachability to
the worker port can `SET`/`GET`/`DEL` keys. Do not expose a
`redis-lite` deployment to the public internet without putting it
behind a TLS-terminating proxy (e.g. `stunnel`, `caddy tls`) or
forking the sample to add an `AUTH` command. The single-threaded
accept loop also has no backpressure — a slow peer can starve other
connections. The read-side and write-side bulk-string caps in
`src/lib.rs::MAX_BULK_BYTES` (64 MiB) bound the per-frame memory
footprint of a single misbehaving client.

## Why `EDGE_HTTP_SERVER_PORT`?

The worker stamps the guest's private upstream port into
`EDGE_HTTP_SERVER_PORT` at start time. The name is mildly wrong for
TCP but the semantics — "the worker port your server should listen
on" — are identical, and the env-var rename would have broken every
existing HTTP guest. Same dual-use as `samples/hello-tcp`; see the
"protocol" explanation there for the full rationale.

## E2E test

The `redis-lite` fixture (`edge-worker/tests/fixtures/redis_lite.wasm`,
hash-pinned by `EXPECTED_REDIS_LITE_HASH`) is exercised by
`edge-worker/tests/redis_lite_e2e.rs`. That test boots a real
`edge-worker` Supervisor with the worker-wide `socket_mode` left at
`BlockAll` (the default) and asserts that the per-app override
(`AppSpec.socket_mode = Some(AllowAll)`) actually reaches the guest —
proving issue #412's per-app socket-mode plumbing end-to-end.

The test also asserts `request_count == 0` on the post-session heartbeat
to document the pre-existing LR metering gap (since #484) — surfaced as
a follow-up issue, **not** a fix in this PR. See the comment block at
the bottom of `redis_lite_e2e.rs::redis_lite_round_trip_inner`.

## Layout

```
samples/redis-lite/
├── Cargo.toml         # crate-type = ["cdylib"], isolated [workspace], wit-bindgen 0.45
├── edge.toml          # [project] name + target + world + protocol
├── .cargo/config.toml # opt out of the shared target-cache + sccache
├── .gitignore         # target/, .edge/
├── README.md          # this file
└── src/
    ├── lib.rs         # wasi:sockets/tcp listener + kv-store dispatcher
    └── resp.rs        # RESP2 array/bulk parser (unit-tested)
```

The RESP parser lives in its own module with 8 unit tests so future
protocol extensions can grow the parser without re-reading the TCP
plumbing. The unit tests run locally with
`cargo test --target wasm32-unknown-unknown --release src/resp.rs`.

The WIT tree used by `wit-bindgen` lives at [`wit/`](../../wit/) (with
`wit/deps/*` for the WASI 0.2.1 deps) and is referenced via the
`path: "../../wit"` field in `src/lib.rs`. See the matching rationale
at the bottom of [`samples/hello-tcp/README.md`](../hello-tcp/README.md#layout).

[resp]: https://redis.io/docs/latest/develop/reference/protocol-spec/