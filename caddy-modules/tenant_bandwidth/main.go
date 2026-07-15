// Package tenantbandwidth implements a Caddy HTTP middleware that
// limits the per-key bytes/sec on the response payload. It is the
// enforcement half of edgeCloud's per-tenant bandwidth cap
// (issue #664, sub-feature #3 of #305). The companion data path
// (schema, repo, handler, admin endpoint, ingress cache) shipped in
// PR #661. This module is what actually paces responses when the cap
// is reached.
//
// # Why a custom module
//
// Stock `caddy:2` ships `rate_limit`, but it is a token-bucket /
// RPS-only primitive — there is no response-payload byte-rate
// throttle in the Caddy core. caddyserver/caddy#4476 "Feature Request:
// Bandwidth Limiting" was closed as not-planned, with the comment
// that it would have to be a plugin. Rather than pull in a third-
// party limiter, edgeCloud vendors this first-party module into a
// custom image (`edgecloud/caddy-concurrent:latest`, see
// `edge-ingress/Dockerfile.caddy-concurrent`). The image also
// contains the sibling `tenant_concurrent` middleware (issue #663).
//
// # Design
//
// One module instance per route — edge-ingress renders one handler
// invocation per bandwidth-cap tenant (see `edge-ingress/src/caddy.rs`,
// commit 4 of PR #664). Each instance keeps a `sync.RWMutex`-guarded
// `map[key]*rate.Limiter` where `key` is a static string the
// renderer passes in (e.g. `tenant-t_acme`). On request entry, the
// middleware installs a `pacingWriter` that wraps the downstream
// `http.ResponseWriter`; each `Write([]byte)` blocks (via
// `rate.Limiter.WaitN(ctx, n)`) until enough tokens are available,
// pacing the response stream at `BytesPerSec`. When the request
// context is cancelled (client disconnect), `WaitN` returns
// `ctx.Err()` and the write surfaces that as the error.
//
// Note: the cap fires on response payload size, not request count.
// This is the key difference from `tenant_concurrent` — a tenant
// with `BytesPerSec=1000` can sustain an arbitrary number of
// concurrent in-flight requests; each is just paced slower.
//
// # Multi-replica caveat
//
// Each Caddy process enforces its own copy of the cap. With N
// replicas a tenant can sustain N × `bytes_per_sec` throughput
// across the fleet. Cross-replica aggregation is the same shape
// of follow-up as the per-replica RPS cap (issue #665).
//
// # Lifecycle
//
// Caddy decodes the JSON into the struct fields (`Key`,
// `BytesPerSec`), then calls `Provision` once per route-load. We
// lazy-create the per-key limiter map in `Provision` (zero-value
// safe — limiters themselves are lazy on first request) and drop
// the map in `Cleanup` to release memory on config reload.
package tenantbandwidth

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"golang.org/x/time/rate"
)

// TenantBandwidth is a per-key response-payload bandwidth limiter.
// Each unique `Key` gets a `*rate.Limiter` sized at `BytesPerSec`;
// a `Write([]byte)` on the wrapped ResponseWriter blocks until
// enough tokens are available. The pattern mirrors stock Caddy's
// `rate_limit` but paces on response bytes (not RPS tokens).
type TenantBandwidth struct {
	// Key is the static string identifying which cap to apply. The
	// renderer (edge-ingress) sets this to `tenant-<tenant_id>` so
	// each tenant gets its own limiter. Required.
	Key string `json:"key,omitempty"`

	// BytesPerSec is the per-key response-payload cap in bytes/sec.
	// Must be > 0; the renderer only emits a route when
	// `bandwidth_bps > 0`, so 0-byte/sec routes never reach this
	// struct. The field is named to match stock Caddy's
	// `bytes_per_sec` JSON key convention.
	BytesPerSec int64 `json:"bytes_per_sec"`

	mu       sync.RWMutex
	limiters map[string]*rate.Limiter
}

// init registers this module with the Caddy module registry at
// process start. Without `init()`, the xcaddy-generated main.go
// imports the package for side effects but the module ID never
// lands in `caddy.Modules()`, so `caddy list-modules` and the
// runtime both reject it. Registering `&TenantBandwidth{}`
// (pointer) is the pattern caddyserver-caddy-l4's
// `modules/l4tls/handler.go` uses for handlers that carry a
// mutex; `Provision` and the rest of the methods all run on
// `*TenantBandwidth` so the pointer is what actually gets
// configured. (See issue #663's same lesson at
// `caddy-modules/tenant_concurrent/main.go`.)
func init() {
	caddy.RegisterModule(&TenantBandwidth{})
}

// CaddyModule registers this handler under
// `http.handlers.tenant_bandwidth`. The ID is stable across
// module versions — the renderer writes it directly into
// Caddyfile-JSON. Pointer receiver — the struct carries a
// `sync.RWMutex`, and the other methods all run on
// `*TenantBandwidth` so `Provision` can lazy-init `limiters`.
func (tb *TenantBandwidth) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "http.handlers.tenant_bandwidth",
		New: func() caddy.Module {
			return new(TenantBandwidth)
		},
	}
}

// Provision lazily allocates the per-key limiter map. Caddy has
// already unmarshaled `Key` and `BytesPerSec` from the route JSON
// by the time this runs. We validate `BytesPerSec` here as
// defense-in-depth: the renderer (edge-ingress) only emits a route
// when `bandwidth_bps > 0`, but a hand-edited Caddyfile-JSON or a
// future renderer regression could ship a 0/negative value. Without
// this check, `rate.NewLimiter(0, 0)` would build a limiter that
// rejects every byte — turning the pacer into a hard-deny-everything
// block that would be very confusing to debug in production.
func (tb *TenantBandwidth) Provision(_ caddy.Context) error {
	if tb.BytesPerSec <= 0 {
		return fmt.Errorf("tenant_bandwidth: bytes_per_sec must be > 0, got %d", tb.BytesPerSec)
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.limiters = make(map[string]*rate.Limiter)
	return nil
}

// Cleanup drops the limiter map so a config reload that removes
// the handler releases the limiters immediately. Caddy calls this
// when the route is replaced.
func (tb *TenantBandwidth) Cleanup() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.limiters = nil
	return nil
}

// resolveKey returns the limiter key for the current request. The
// module does not evaluate Caddy placeholders itself — `Key` is a
// static string set by the renderer at config-load time, so every
// request flowing through the route shares the same bucket.
func (tb *TenantBandwidth) resolveKey(_ *http.Request) (string, error) {
	if tb.Key == "" {
		return "", fmt.Errorf("tenant_bandwidth: key is required")
	}
	return tb.Key, nil
}

// limiterFor returns (creating if needed) the `*rate.Limiter` for
// the given key. Double-checked locking — readers take the RLock
// for the common case; only first-time creation takes the write
// lock. Burst = BytesPerSec (one second of traffic at the cap).
// Mirrors stock Caddy's `rate_limit` burst-default shape.
func (tb *TenantBandwidth) limiterFor(key string) *rate.Limiter {
	tb.mu.RLock()
	l, ok := tb.limiters[key]
	tb.mu.RUnlock()
	if ok {
		return l
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if l, ok = tb.limiters[key]; ok {
		return l // lost the upgrade race — another goroutine created it
	}
	l = rate.NewLimiter(rate.Limit(tb.BytesPerSec), int(tb.BytesPerSec))
	tb.limiters[key] = l
	return l
}

// pacingWriter wraps an http.ResponseWriter so each Write blocks
// until the limiter has enough tokens. The request context is
// threaded through so a client disconnect mid-pacing surfaces as
// `ctx.Err()` rather than hanging forever.
//
// Implementation note: rate.Limiter.WaitN is the canonical
// pacing primitive, but it has a sharp edge — if the request
// size exceeds the limiter's burst, WaitN returns
// `rate: Wait(n=X) exceeds limiter's burst Y` IMMEDIATELY without
// waiting. With our burst=BytesPerSec, a 5 KB response body
// (larger than the 1-second burst at 1 KB/s) would fail every
// request. The fix is to chunk writes so each WaitN call asks
// for at most `burst` tokens. We choose `chunkSize = max(1,
// burst/16)` — 16 chunks per burst window keeps the pacing
// smooth (one pacing event every ~62 ms at the 1-second-burst
// rate) and avoids pathologically small chunks at low rates.
type pacingWriter struct {
	w            http.ResponseWriter
	limiter      *rate.Limiter
	chunkSize    int
	requesterCtx context.Context
}

// Write blocks until all `len(p)` tokens are available (chunked
// so each WaitN fits within the burst), then forwards to the
// wrapped writer. rate.Limiter.WaitN returns immediately when
// enough tokens are present and blocks otherwise. If the request
// context is cancelled (client disconnect), WaitN returns
// `ctx.Err()` which we surface as the write error so the caller
// sees the disconnect rather than a hang.
//
// Returns the count of bytes successfully forwarded; partial
// writes are possible if the context cancels mid-chunk.
func (pw *pacingWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := len(p)
		if n > pw.chunkSize {
			n = pw.chunkSize
		}
		if err := pw.limiter.WaitN(pw.requesterCtx, n); err != nil {
			return written, err
		}
		w, err := pw.w.Write(p[:n])
		written += w
		if err != nil {
			return written, err
		}
		p = p[n:]
	}
	return written, nil
}

// Header delegates so the caller can still set response headers
// (and the underlying writer will see them at WriteHeader time).
func (pw *pacingWriter) Header() http.Header { return pw.w.Header() }

// WriteHeader delegates so a 200/4xx/5xx sent before the first
// Write passes through unchanged. Pacing only applies to body
// bytes — header writes are not metered.
func (pw *pacingWriter) WriteHeader(code int) { pw.w.WriteHeader(code) }

// ServeHTTP implements caddyhttp.MiddlewareHandler. Installs the
// pacing writer and forwards the request to `next`. The pacing
// fires on every downstream Write call until `next` returns.
func (tb *TenantBandwidth) ServeHTTP(
	w http.ResponseWriter, r *http.Request, next caddyhttp.Handler,
) error {
	key, err := tb.resolveKey(r)
	if err != nil {
		// Misconfiguration — `Key` is empty. Surface as 500 since
		// the route cannot function.
		return caddyhttp.Error(http.StatusInternalServerError, err)
	}
	lim := tb.limiterFor(key)
	pw := &pacingWriter{
		w:            w,
		limiter:      lim,
		chunkSize:    chunkSizeFor(int(tb.BytesPerSec)),
		requesterCtx: r.Context(),
	}
	return next.ServeHTTP(pw, r)
}

// chunkSizeFor returns the per-Write chunk size for a given
// bytes-per-second rate. 16 chunks per burst window keeps pacing
// smooth and avoids the rate.WaitN "exceeds burst" error for any
// response body size. Floors at 1 byte.
func chunkSizeFor(bps int) int {
	n := bps / 16
	if n < 1 {
		return 1
	}
	return n
}

// Interface guards. Compile-time enforcement that this type
// satisfies the Caddy module + middleware contracts.
var (
	_ caddy.Module                = (*TenantBandwidth)(nil)
	_ caddyhttp.MiddlewareHandler = (*TenantBandwidth)(nil)
)
