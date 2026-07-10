// Package cache holds in-process caches used by the control plane's
// HTTP layer. Currently the only entry is UsageCache, the
// stale-while-revalidate cache for GET /api/v1/usage (issue #421).
//
// Caches here are deliberately simple: a sync.Map of small structs,
// no eviction policy beyond an absolute TTL, no background refresh
// goroutines. The dashboard polls every few seconds, and tenant count
// is small; LRU is unnecessary until we see memory pressure.
package cache

import (
	"sync"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// UsageCache is a per-tenant stale-while-revalidate cache for
// domain.TenantUsage. The intended caller pattern is:
//
//  1. TryGet(tenantID, now) → if fresh, return immediately.
//  2. On miss or stale hit, the service does a synchronous refresh.
//  3. After refresh, Set(tenantID, payload, ttl).
//
// To avoid thundering-herd on cache expiry, callers must guard the
// refresh path with TryStartRefresh(tenantID): only one in-flight
// refresh per tenant at a time. The other concurrent callers either
// serve the stale value (preferred) or block briefly waiting for the
// in-flight refresh to publish.
//
// Memory: O(active tenants). Each entry is roughly the size of a
// TenantUsage (~1 KiB for a moderately-populated events slice). At
// 10k tenants that's 10 MiB — well below any pressure threshold.
//
// Concurrency: safe for concurrent use. All entry access goes through
// sync.Map; the refreshing flag uses sync/atomic via a per-entry
// sync.Mutex so the state transition (false → true → false) is
// serialized without a global lock.
type UsageCache struct {
	ttl      time.Duration
	maxStale time.Duration // how long past expiry stale values remain servable
	entries  sync.Map      // map[tenantID]*usageCacheEntry
}

// usageCacheEntry is the value stored under each tenant key. Mutex
// guards the refreshing flag only — payload + expiresAt are immutable
// from the moment Set publishes them (sync.Map's Load+Store pattern
// guarantees the new pointer is visible atomically).
type usageCacheEntry struct {
	payload    *domain.TenantUsage
	expiresAt  time.Time  // wall-clock time, set by the caller via Set
	refreshing bool       // guarded by mu
	mu         sync.Mutex // guards refreshing only
}

// NewUsageCache builds a cache with the given fresh TTL and the
// maximum window past expiry during which stale values remain
// servable. Typical values: TTL=10s, MaxStale=60s. MaxStale must be
// >= 0; values <= 0 disable stale-while-revalidate (every entry is
// either fresh or missing).
func NewUsageCache(ttl, maxStale time.Duration) *UsageCache {
	return &UsageCache{ttl: ttl, maxStale: maxStale}
}

// TTL returns the configured fresh-window duration. Callers use this
// when computing Set's TTL argument so a future tweak to NewUsageCache
// (e.g. making TTL a config-driven knob) propagates without touching
// every call site.
func (c *UsageCache) TTL() time.Duration { return c.ttl }

// TryGet returns the cached payload and whether it is fresh, stale, or
// missing. The caller must check the freshness before deciding
// whether to refresh.
//
//	(false,  nil)  → no entry; caller does a synchronous refetch.
//	(true,   nil)  → fresh entry; serve immediately, no refresh needed.
//	(true,   time) → stale entry; serve it AND consider kicking a
//	                 background refresh (guarded by TryStartRefresh).
//
// The second return is non-nil only when the entry is stale but still
// within the maxStale window — the value is the time at which the
// entry will become too stale to serve (so the caller can log it).
func (c *UsageCache) TryGet(tenantID string, now time.Time) (*domain.TenantUsage, bool, time.Time) {
	raw, ok := c.entries.Load(tenantID)
	if !ok {
		return nil, false, time.Time{}
	}
	e := raw.(*usageCacheEntry)
	if now.Before(e.expiresAt) {
		return e.payload, true, time.Time{}
	}
	// Stale. Servable only if now is within maxStale past expiresAt.
	staleCutoff := e.expiresAt.Add(c.maxStale)
	if c.maxStale <= 0 || now.After(staleCutoff) {
		// Too stale to serve. Drop the entry and treat as a miss so the
		// caller refetches synchronously.
		c.entries.Delete(tenantID)
		return nil, false, time.Time{}
	}
	return e.payload, true, staleCutoff
}

// Set publishes a fresh entry for tenantID, replacing any previous
// value. The caller is responsible for ensuring the new payload is
// correct (typically via a synchronous refetch on a TryGet miss).
//
// Note: Set does NOT clear the refreshing flag — that flag is owned
// by TryStartRefresh / FinishRefresh so a slow refresh can race with
// a later Set without leaving the flag stuck.
func (c *UsageCache) Set(tenantID string, payload *domain.TenantUsage, ttl time.Duration) {
	if ttl <= 0 {
		ttl = c.ttl
	}
	c.entries.Store(tenantID, &usageCacheEntry{
		payload:   payload,
		expiresAt: time.Now().Add(ttl),
	})
}

// TryStartRefresh atomically claims the right to refresh a tenant's
// entry. Returns true if this caller should run the refresh, false if
// another goroutine already started one (or the entry has been
// removed). Callers that get false should serve the stale value (or
// return 503 / wait) — they MUST NOT block waiting for the refresh
// to finish, because the refresh function isn't passed in.
func (c *UsageCache) TryStartRefresh(tenantID string) bool {
	raw, ok := c.entries.Load(tenantID)
	if !ok {
		// No entry — caller is the refetcher; let it through.
		return true
	}
	e := raw.(*usageCacheEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.refreshing {
		return false
	}
	e.refreshing = true
	return true
}

// FinishRefresh clears the refreshing flag so the next expiry can
// kick another background refresh. Idempotent — safe to call when no
// refresh was in progress.
func (c *UsageCache) FinishRefresh(tenantID string) {
	raw, ok := c.entries.Load(tenantID)
	if !ok {
		return
	}
	e := raw.(*usageCacheEntry)
	e.mu.Lock()
	e.refreshing = false
	e.mu.Unlock()
}

// Invalidate removes the entry for tenantID. Called when the caller
// learns of a state change (e.g. a 402 surfaced, a billing event
// arrived) that should be reflected on the next read.
func (c *UsageCache) Invalidate(tenantID string) {
	c.entries.Delete(tenantID)
}

// Len returns the current entry count. Test-only — production code
// has no need for this. Marked as exported only because the test file
// is in the same package.
func (c *UsageCache) Len() int {
	n := 0
	c.entries.Range(func(_, _ any) bool { n++; return true })
	return n
}
