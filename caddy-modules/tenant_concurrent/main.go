// Package tenantconcurrent implements a Caddy HTTP middleware that
// limits the number of in-flight requests per static key. It is the
// enforcement half of edgeCloud's per-tenant concurrent-request cap
// (issue #663, sub-feature #2 of #305). The companion data path
// (schema, repo, handler, admin endpoint, ingress cache) shipped in
// PR #661. This module is what actually rejects requests when the
// cap is reached.
//
// # Why a custom module
//
// Stock `caddy:2` ships `rate_limit`, but it is a token-bucket /
// RPS-only primitive. There is no in-flight concurrency counter
// primitive in the Caddy core. Rather than fork Caddy or pull in a
// third-party limiter, edgeCloud vendors this first-party module
// into a custom image (`edgecloud/caddy-concurrent:latest`, see
// `edge-ingress/Dockerfile.caddy-concurrent`).
//
// # Design
//
// One module instance per route — edge-ingress renders one handler
// invocation per concurrent-cap tenant (see `edge-ingress/src/caddy.rs`,
// commit 4 of PR #693). Each instance keeps a `sync.RWMutex`-guarded
// `map[key]chan struct{}` where `key` is a static string the
// renderer passes in (e.g. `tenant-t_acme`). On request entry, the
// middleware attempts a non-blocking send into the channel; a full
// channel means the in-flight cap is reached and the request is
// rejected with 429 + `Retry-After: 1`. On handler-chain return
// (regardless of error), the slot is released via a deferred
// receive.
//
// # Multi-replica caveat
//
// Each Caddy process enforces its own copy of the cap. With N Caddy
// replicas a tenant can sustain N × `limit` in-flight requests
// across the fleet. Cross-replica aggregation is the same shape of
// follow-up as the per-replica RPS cap (issue #665).
//
// # Lifecycle
//
// Caddy decodes the JSON into the struct fields (`Key`, `Limit`),
// then calls `Provision` once per route-load. We lazy-create the
// per-key bucket map in `Provision` (zero-value safe — buckets
// themselves are lazy on first request) and drop the map in
// `Cleanup` to release memory on config reload.
package tenantconcurrent

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// TenantConcurrent is a per-key concurrent-request limiter. Each
// unique `Key` gets a buffered channel sized at `Limit`; a
// non-blocking send on entry either proceeds or returns 429. The
// pattern mirrors stock Caddy's `rate_limit` but uses an in-flight
// count instead of RPS tokens.
type TenantConcurrent struct {
	// Key is the static string identifying which cap to apply. The
	// renderer (edge-ingress) sets this to `tenant-<tenant_id>` so
	// each tenant gets its own bucket. Required.
	Key string `json:"key,omitempty"`

	// Limit is the per-key in-flight cap. Must be > 0; the renderer
	// only emits a route when `concurrent_limit > 0`, so 0-limit
	// routes never reach this struct. The field is named to match
	// stock Caddy's `limit` JSON key on `rate_limit`.
	Limit int `json:"limit"`

	mu      sync.RWMutex
	buckets map[string]chan struct{}
}

// init registers this module with the Caddy module registry at
// process start. Without `init()`, the xcaddy-generated main.go
// imports the package for side effects but the module ID never
// lands in `caddy.Modules()`, so `caddy list-modules` and the
// runtime both reject it. Registering `&TenantConcurrent{}`
// (pointer) is the pattern caddy-l4's `modules/l4tls/handler.go`
// uses for handlers that carry a mutex; `Provision` and the rest
// of the methods all run on `*TenantConcurrent` so the pointer is
// what actually gets configured.
func init() {
	caddy.RegisterModule(&TenantConcurrent{})
}

// CaddyModule registers this handler under
// `http.handlers.tenant_concurrent`. The ID is stable across module
// versions — the renderer writes it directly into Caddyfile-JSON.
// Pointer receiver — the struct carries a `sync.RWMutex`, and the
// other methods all run on `*TenantConcurrent` so `Provision` can
// lazy-init `buckets`.
func (tc *TenantConcurrent) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "http.handlers.tenant_concurrent",
		New: func() caddy.Module {
			return new(TenantConcurrent)
		},
	}
}

// Provision lazily allocates the per-key bucket map. Caddy has
// already unmarshaled `Key` and `Limit` from the route JSON by the
// time this runs.
func (tc *TenantConcurrent) Provision(_ caddy.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.buckets = make(map[string]chan struct{})
	return nil
}

// Cleanup drops the bucket map so a config reload that removes the
// handler releases the channels immediately. Caddy calls this when
// the route is replaced.
func (tc *TenantConcurrent) Cleanup() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.buckets = nil
	return nil
}

// resolveKey returns the bucket key for the current request. The
// module does not evaluate Caddy placeholders itself — `Key` is a
// static string set by the renderer at config-load time, so every
// request flowing through the route shares the same bucket.
func (tc *TenantConcurrent) resolveKey(_ *http.Request) (string, error) {
	if tc.Key == "" {
		return "", fmt.Errorf("tenant_concurrent: key is required")
	}
	return tc.Key, nil
}

// bucketFor returns (creating if needed) the channel semaphore for
// the given key. Double-checked locking — readers take the RLock for
// the common case; only first-time creation takes the write lock.
func (tc *TenantConcurrent) bucketFor(key string) chan struct{} {
	tc.mu.RLock()
	b, ok := tc.buckets[key]
	tc.mu.RUnlock()
	if ok {
		return b
	}
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if b, ok = tc.buckets[key]; ok {
		return b // lost the upgrade race — another goroutine created it
	}
	b = make(chan struct{}, tc.Limit)
	tc.buckets[key] = b
	return b
}

// ServeHTTP implements caddyhttp.MiddlewareHandler. Acquires a slot
// before forwarding to `next`; releases on return. The slot is held
// for the full handler-chain duration (deferred receive runs even
// when `next` returns an error), so the cap correctly counts
// in-flight requests.
func (tc *TenantConcurrent) ServeHTTP(
	w http.ResponseWriter, r *http.Request, next caddyhttp.Handler,
) error {
	key, err := tc.resolveKey(r)
	if err != nil {
		// Misconfiguration — `Key` is empty. Surface as 500 since
		// the route cannot function.
		return caddyhttp.Error(http.StatusInternalServerError, err)
	}
	bucket := tc.bucketFor(key)

	// Non-blocking acquire. A full bucket means the in-flight cap
	// is reached — return 429 with Retry-After: 1 so clients with
	// sane backoff retry shortly.
	select {
	case bucket <- struct{}{}:
		defer func() { <-bucket }()
		return next.ServeHTTP(w, r)
	default:
		w.Header().Set("Retry-After", "1")
		return caddyhttp.Error(
			http.StatusTooManyRequests,
			fmt.Errorf("tenant concurrent cap reached for %s", key),
		)
	}
}

// Interface guards. Compile-time enforcement that this type
// satisfies the Caddy module + middleware contracts.
var (
	_ caddy.Module                = (*TenantConcurrent)(nil)
	_ caddyhttp.MiddlewareHandler = (*TenantConcurrent)(nil)
)