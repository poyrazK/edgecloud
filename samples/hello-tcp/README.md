# samples/hello-tcp

Minimal deployable [edgeCloud](https://github.com/poyrazK/edgecloud) raw-TCP
server. For any inbound TCP connection it reads RESP commands and replies
to `PING\r\n` with `+PONG\r\n`:

```text
$ nc localhost <public_port> <<< "PING"
+PONG
```

The point of the sample is to be the smallest possible
end-to-end-deployable raw-TCP guest component — issue #548's
`wasi:sockets/tcp` wire path through `edge-ingress`'s L4 routing table
and Caddy's [`apps.layer4`][caddy-l4] gets exercised byte-for-byte with
every `nc localhost <port> <<< "PING"` a developer runs against it.

[caddy-l4]: https://github.com/mholt/caddy-l4

## Why long-running (`edge-runtime`) and not FaaS (`edge-runtime-handler`)?

The FaaS world (`edge-runtime-handler`) is HTTP-only by construction:
the worker owns the TCP listener, hands the bytes to the guest via
`wasi:http/incoming-handler`, and discards the guest when the response
is sent. A raw-TCP listener has to own its socket for the duration of
the process — Redis clients, MQTT brokers, custom protocols don't fit
that shape. The `edge-runtime` world exposes the full
`wasi:cli/command@0.2.1` family (the workspace `wit/edge-cloud.wit`
`include`s it), so the guest can `bind` + `accept` itself the same way
a stdlib `TcpListener` would on a Linux box. `wit-bindgen` maps the
WASI sockets types into the guest's `crate::wasi::sockets::tcp`
namespace; see [`src/lib.rs`](src/lib.rs).

## Build

The CLI does the two-step build (cargo + wasm-tools wrap) for you:

```sh
cd samples/hello-tcp
../../target/release/edge build
```

That command runs `cargo build --target wasm32-unknown-unknown --release`
and then `wasm-tools component new <core> --world edge-runtime
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
  edge deploy
```

The CLI calls `POST /api/v1/apps/hello-tcp/l4-port` (issue #548, Commit 9)
on the control plane, which atomically allocates a public port in the
`L4_PORT_RANGE_START..=L4_PORT_RANGE_END` window (default `31000..=31999`)
and returns it. Subsequent `edge tcp-info hello-tcp` lookups print the
same port back:

```text
$ edge tcp-info hello-tcp
hello-tcp: tcp://localhost:31042 on worker w_us-east-1_abc12.example
```

(The hostname in production resolves to your Caddy-l4 ingress
host — `localhost` here is the dev `docker-compose` shortcut.)

## Reach it

The smoke recipe in [`../../scripts/dev-l4-smoke.sh`](../../scripts/dev-l4-smoke.sh)
runs through the whole flow end-to-end (build → deploy → discover port
→ `nc` → assert `PONG`). Manually:

```sh
public_port=$(curl -sH "Authorization: Bearer $EDGE_API_KEY" \
  http://localhost:8080/api/v1/apps/hello-tcp/l4-port | jq -r .public_port)
echo -e 'PING\r\n' | nc -w 2 localhost "$public_port"
```

The expected response is exactly `+PONG\r\n` (the `\r\n` is part of
the RESP wire protocol — `nc` strips the trailing newline from
its stdout, so the human-readable form is just `+PONG`).

## Why `EDGE_HTTP_SERVER_PORT`?

The worker stamps the guest's private upstream port into
`EDGE_HTTP_SERVER_PORT` at start time (see
`edge-worker/src/supervisor.rs::start_app` line ~2144). The name is
mildly wrong for TCP but the semantics — "the worker port your
server should listen on" — are identical, and the env-var rename
would have broken every existing HTTP guest. So hello-tcp reads the
same env var the HTTP samples do, and the dual-use is documented
here rather than spreading as tribal knowledge:

| Sample     | Reads `EDGE_HTTP_SERVER_PORT`? | World              | Protocol |
|---|---|---|---|
| `samples/hello`       | yes | `edge-runtime-handler` | `http` (default) |
| `samples/hello-js`    | no  | `edge-runtime-handler` | `http` (default) |
| `samples/hello-js-ws` | no  | `edge-runtime-handler` | `http` (default) |
| `samples/hello-tcp`   | yes | `edge-runtime`         | `tcp` |

The CLI rejects `world = "edge-runtime-handler" + protocol = "tcp"`
at build time (see `edge-cli/src/commands/build.rs::validate_protocol_combo`)
because the FaaS handler export path doesn't expose `wasi:sockets` to
the guest, so the `bind` call inside the guest would trap on load.

## Protocol: RESP, intentionally minimal

RESP ([Redis wire format][resp]) is a great fit for the v1 demo because
the only command you need to "make it go" is `PING`. The sample's
output is a single `+PONG\r\n` — no globbing, no auth, no SELECT
against a database; that's the minimum surface that proves the whole
L4 stack (ingress route → apps.layer4 → upstream socket → guest
accept → wasi:sockets read/write → response byte-for-byte) round-trips
a payload correctly.

If you fork this for a real protocol (MQTT, plain TCP echo for
benchmarking, your own custom request/response format), the key
places to extend are:

- **Line framing** — change `handle_connection` to match your frame
  delimiter (binary length prefixes for RESP `BulkString`s, fixed-size
  headers for protocols like DNS, etc.).
- **Concurrent connections** — wrap `handle_connection` in a
  `wasi::io::poll::Pollable` driven by `wasi::io::poll::poll`, with
  one pollable per in-flight connection. A backpressure-aware proxy
  might also expose `Control` over edge:cloud/process env vars (e.g.
  `EDGE_MAX_CONNS_PER_IP`).
- **DDoS caps** — the worker stamps `EDGE_L4_MAX_CONNS_PER_IP` (set
  via `INGRESS_L4_MAX_CONNS_PER_IP`) for ingress-side enforcement;
  the guest doesn't need to duplicate that limiter but a backpressure
  signal here (close the accepted fd once the cap is reached) makes
  the cap visible to the client side too.

[resp]: https://redis.io/docs/latest/develop/reference/protocol-spec/

## Layout

```
samples/hello-tcp/
├── Cargo.toml         # crate-type = ["cdylib"], isolated [workspace]
├── edge.toml          # [project] name + target + world + protocol
├── README.md          # this file
└── src/
    └── lib.rs         # wasi:sockets/tcp listener + RESP loop
```

The WIT tree used by `wit-bindgen` lives at
[`wit/`](../../wit/) (with `wit/deps/*` for the WASI 0.2.1 deps) and
is referenced via the `path: "../../wit"` field in `src/lib.rs`. The
runtime's own WIT at
[`edge-runtime/src/wit/`](../../edge-runtime/src/wit/) is the source of
truth for wasmtime's resolver but isn't directly usable by
`wit-bindgen` — its `include wasi:cli/command@0.2.1;` syntax is
wasmtime-only and its dep `.wit` files don't carry top-level `package`
declarations. The top-level `wit/` tree is explicitly adapted for
`wit-bindgen` (with package decls and a `wasi:http/outgoing-handler`
import on the handler world), so the sample points at it instead of
duplicating 33 files. The historical
`edge-worker/tests/fixtures/wit/` path is now a symlink to `wit/`.
