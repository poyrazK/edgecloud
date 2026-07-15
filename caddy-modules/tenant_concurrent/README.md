# tenant_concurrent

First-party Caddy HTTP middleware that enforces a per-key
concurrent-request cap (issue #663, sub-feature #2 of #305). Built
into the `edgecloud/caddy-concurrent:latest` image — see
`edge-ingress/Dockerfile.caddy-concurrent`.

## Why

Stock `caddy:2` ships `rate_limit`, but it is token-bucket /
RPS-only. There is no in-flight concurrency counter primitive in
the Caddy core. This module fills that gap for the
edgeCloud data plane (per-tenant caps).

## Module ID

```
http.handlers.tenant_concurrent
```

The renderer (edge-ingress) writes this string verbatim into the
Caddyfile-JSON `handler` field.

## JSON shape

```json
{
  "handler": "tenant_concurrent",
  "key": "tenant-t_acme",
  "limit": 50
}
```

- **`key`** — static string identifying which cap to apply. The
  renderer always sets this to `tenant-<tenant_id>`. Required.
- **`limit`** — max in-flight requests per key. Must be > 0; the
  renderer only emits a route when the underlying
  `concurrent_limit > 0`, so 0-limit routes never reach this struct.

A request whose `key` already has `limit` requests in flight
receives `429 Too Many Requests` with `Retry-After: 1`.

## Dev build

xcaddy compiles this module into a Caddy binary. From the repo
root:

```bash
xcaddy build \
    --with github.com/mholt/caddy-l4 \
    --with github.com/poyrazK/edgecloud-caddy-modules/tenant_concurrent=./caddy-modules/tenant_concurrent
```

The local-path `=./caddy-modules/tenant_concurrent` form is used
during the bootstrap period. Once the repo publishes tagged
releases, switch to the GitHub ref:

```bash
xcaddy build \
    --with github.com/mholt/caddy-l4 \
    --with github.com/poyrazK/edgecloud-caddy-modules/tenant_concurrent@v0.1.0
```

## Verify the build

```bash
docker run --rm edgecloud/caddy-concurrent:latest \
    caddy list-modules --packages http.handlers | grep tenant_concurrent
```

Should print `http.handlers.tenant_concurrent`.

## Multi-replica caveat

Each Caddy process enforces its own copy of the cap. With N
replicas, a tenant can sustain N × `limit` in-flight requests
across the fleet. Cross-replica aggregation is the same shape of
follow-up as the per-replica RPS cap (issue #665).

## Lifecycle

- `Provision` runs once per route-load. Lazy-allocates the
  per-key bucket map.
- `Cleanup` runs on config reload. Drops the bucket map so the
  channels release.
- `ServeHTTP` runs per request. Acquires a slot via non-blocking
  channel send; releases via deferred receive.

## Testing

Unit tests live alongside the module. Run with:

```bash
cd caddy-modules/tenant_concurrent
go test ./...
```

The CI job `caddy-image` (`.github/workflows/ci.yml`) builds the
Caddy image and asserts the module is registered.

## Go version

Pinned to `go 1.25.0` to match `edge-control-plane/go.mod`.
xcaddy fetches the matching toolchain automatically.