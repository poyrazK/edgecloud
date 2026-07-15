# tenant_bandwidth

First-party Caddy HTTP middleware that enforces a per-key byte-rate
cap on the response payload. It is the **enforcement half** of
edgeCloud's per-tenant bandwidth throttling (issue #664, sub-feature
#3 of #305). The companion data path (schema, repo, handler, admin
endpoint, ingress cache) shipped in PR #661 — this module is what
actually paces responses when the cap is reached.

## Why a custom module (not stock `rate_limit.bandwidth`)

`caddy:2` ships `rate_limit`, but it is a token-bucket / RPS-only
primitive — there is no response-payload byte-rate throttle in the
Caddy core. The widely-used
[caddyserver/caddy#4476](https://github.com/caddyserver/caddy/issues/4476)
"Feature Request: Bandwidth Limiting" was closed as **not planned**,
with the comment that it would have to be a plugin.

Rather than pull in a third-party limiter, edgeCloud vendors this
first-party module into the custom image
`edgecloud/caddy-concurrent:latest` (see
`edge-ingress/Dockerfile.caddy-concurrent`). The image also contains
the sibling `tenant_concurrent` middleware (issue #663) and the L4
plugin.

## Module shape

```go
type TenantBandwidth struct {
    Key         string // static, set by the renderer at config-load time
    BytesPerSec int64  // > 0; validated in Provision
    // ... per-key rate.Limiter map, RWMutex-guarded
}
```

Module ID: `http.handlers.tenant_bandwidth`. JSON shape:

```json
{
  "handler": "tenant_bandwidth",
  "key": "tenant-t_acme",
  "bytes_per_sec": 1000000
}
```

The renderer (`edge-ingress/src/caddy.rs`) emits one route per
bandwidth-cap tenant, matched on
`host_regexp: ^t_acme-[^.]+\.edgecloud\.dev$`, ordered BEFORE the
per-app reverse_proxy so the pacing wrapper is installed before the
response stream begins.

## How the cap actually fires

On request entry, `ServeHTTP` installs a `pacingWriter` that wraps
the downstream `http.ResponseWriter`. Each `Write([]byte)` call:

1. Splits the input into chunks of `max(1, BytesPerSec / 16)` bytes.
2. Calls `limiter.WaitN(ctx, chunkSize)` — blocks until enough
   tokens are available.
3. Forwards the chunk to the wrapped writer.
4. Repeats until the input is drained.

The chunking is load-bearing. `rate.Limiter.WaitN(ctx, n)` returns
`rate: Wait(n=X) exceeds limiter's burst Y` immediately when
`n > burst`, so a 5 KB body would fail every request under
production burst=BytesPerSec. Splitting into 16 chunks-per-burst-
window keeps each `WaitN` call well within the burst and pacing
smooth (one pacing event every ~62 ms at the 1-second-burst rate).

When the request context is cancelled (client disconnect), the
active `WaitN` returns `ctx.Err()` and the wrapper surfaces it as
the write error rather than hanging forever. **The downstream
handler must propagate `Write` errors** — discarding them would
mask the disconnect signal.

## Multi-replica caveat

Each Caddy process enforces its own copy of the cap. With N ingress
replicas, the effective cap is `N × bandwidth_bps`. Cross-replica
aggregation is the same shape of follow-up as the per-replica RPS
cap (issue #665).

## Build and test

```bash
# from this directory
go vet ./...
go test -race ./... -count=1            # 9 tests, ~15s wall
go test -short ./... -count=1           # 6 tests (skip pacing assertions)
```

The pacing-assertion tests (`TestServeHTTPThrottlesAtCap`,
`TestServeHTTPReleasesLimiterOnError`, `TestPacingWriterContextCancellation`)
are skipped under `-short` so quick CI gates don't pay the
~10-second tax. The full run in the `caddy-image` CI job does NOT
pass `-short`, so they fire there.

## Build into the edgeCloud Caddy image

```bash
# from the repo root
docker build -t edgecloud/caddy-concurrent:test \
  -f edge-ingress/Dockerfile.caddy-concurrent .
docker run --rm edgecloud/caddy-concurrent:test \
  caddy list-modules --packages http.handlers | grep tenant_bandwidth
```

The xcaddy invocation is in `Dockerfile.caddy-concurrent`; both
`tenant_concurrent` (PR #698) and `tenant_bandwidth` are
side-loaded via the local-path form. `xcaddy` consumes
`caddy-modules/` from `/usr/bin/caddy-modules/` (the builder COPY
in the Dockerfile).
