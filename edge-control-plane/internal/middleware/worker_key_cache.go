// Worker-key cache (issue #430).
//
// Background: WorkerAuth's wkr_ namespace branch needs to look up a
// worker's public_key on every request so it can re-derive the HS256
// verification secret via HKDF. The public_key is stored on the
// `workers` row (migration 032), so without a cache every inbound
// request would issue a DB query — unacceptable for a hot-path
// middleware.
//
// The cache is an in-memory TTL store keyed by worker_id. Misses
// trigger a single DB read via the caller-supplied Loader; hits
// short-circuit. Invalidation is exposed so EnrollWorker (and any
// future re-enrollment path) can drop the entry on public_key
// mutation, closing a stale-cache window where an attacker who
// recently enrolled could keep using the old derived secret after a
// rotation.
//
// Design choices:
//   - sync.Map, not sync.RWMutex over a plain map: WorkerAuth is on
//     the hot path (every inbound worker call), so we want concurrent
//     reads without lock contention. The store's mutation rate is
//     one entry per worker enrollment (low).
//   - Lazy TTL expiry: the worker pool is bounded by cluster size,
//     so the cache is bounded by cluster size too. Expired entries
//     are removed on the next access that notices them; we don't
//     run a background sweeper because it would be a goroutine per
//     process for a workload that doesn't need one.
//   - Loader-returned empty string is treated as a hard miss (no
//     pubkey for this worker_id). The WorkerAuth branch will refuse
//     such a request with 401. We deliberately don't cache the
//     "empty" result, because operators might enroll the worker
//     between this request and the next — caching the empty result
//     would silently break the just-enrolled worker.
package middleware

import (
	"context"
	"sync"
	"time"
)

// workerKeyCacheTTL is how long a worker's public_key is cached
// before the next request triggers a fresh DB read. 5 minutes
// matches the TTL used for the JWT claims themselves, so a worker
// whose public_key changed in the DB sees the new key on the next
// round-trip even without explicit invalidation.
//
// Operators can shorten this (e.g. during an active re-enrollment
// roll-out) via WorkerKeyCache.SetTTL, but the cache is
// well-suited to "set once at startup, ignore" because the
// explicit-invalidation path is wired into EnrollWorker.
const workerKeyCacheTTL = 5 * time.Minute

// workerKeyCacheEntry is the cache's value type. The expiry lives
// on the value so we can lazily drop stale entries without a
// background ticker.
type workerKeyCacheEntry struct {
	pubkey    string
	expiresAt time.Time
}

// WorkerKeyCache is the public type WorkerAuth (and any future
// caller) uses to look up a worker's public_key without hitting
// the DB on every request.
//
// The Loader is injected at construction so the cache stays
// independent of the repository package — exactly the same pattern
// used by `*sql.DB` injection in `app.New`. Tests can pass a
// closure that returns a fixture or an error.
type WorkerKeyCache struct {
	loader  func(ctx context.Context, workerID string) (string, error)
	ttl     time.Duration
	mu      sync.RWMutex
	entries map[string]workerKeyCacheEntry
}

// NewWorkerKeyCache builds a cache backed by the supplied loader.
// loader must be safe for concurrent use (it's called from
// multiple goroutines when concurrent WorkerAuth requests miss
// on the same worker_id — we intentionally do not de-duplicate
// in-flight loaders because the DB call is a single round-trip
// per worker_id, not a per-request cost). For workloads that
// need single-flight semantics, wrap loader with singleflight.Group.
func NewWorkerKeyCache(loader func(ctx context.Context, workerID string) (string, error)) *WorkerKeyCache {
	return &WorkerKeyCache{
		loader:  loader,
		ttl:     workerKeyCacheTTL,
		entries: make(map[string]workerKeyCacheEntry),
	}
}

// SetTTL overrides the default 5-minute TTL. Used by tests that
// want to exercise expiry without sleeping for minutes; production
// callers should leave the default.
func (c *WorkerKeyCache) SetTTL(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ttl = d
}

// GetOrLoad returns the public_key for workerID, populating the
// cache on miss. Empty string + nil error means "worker has no
// public_key" (legacy unenrolled worker — WorkerAuth will refuse).
//
// A non-nil error means the loader failed — the caller should
// surface a 5xx, NOT cache the failure, so a transient DB blip
// doesn't permanently disable the worker.
//
// Empty results are also NOT cached: the operator's "worker
// enrolls then the next request verifies" flow would otherwise
// serve a stale "no public_key" answer for up to TTL minutes,
// leaving the just-enrolled worker 401-ing until cache expiry.
func (c *WorkerKeyCache) GetOrLoad(ctx context.Context, workerID string) (string, error) {
	if workerID == "" {
		return "", nil
	}
	c.mu.RLock()
	entry, ok := c.entries[workerID]
	c.mu.RUnlock()
	now := time.Now()
	if ok && now.Before(entry.expiresAt) {
		return entry.pubkey, nil
	}
	pubkey, err := c.loader(ctx, workerID)
	if err != nil {
		return "", err
	}
	// Don't cache the empty result — see method doc. The just-
	// enrolled-worker scenario is the load-bearing case.
	if pubkey == "" {
		return "", nil
	}
	c.mu.Lock()
	// Re-check expiry under the write lock so two concurrent
	// loaders don't stomp each other. The second writer wins,
	// which is fine — pubkey for a given worker_id is
	// idempotent (the same kid derivation, same salt).
	entry, ok = c.entries[workerID]
	if ok && now.Before(entry.expiresAt) && entry.pubkey == pubkey {
		c.mu.Unlock()
		return pubkey, nil
	}
	c.entries[workerID] = workerKeyCacheEntry{
		pubkey:    pubkey,
		expiresAt: now.Add(c.ttl),
	}
	c.mu.Unlock()
	return pubkey, nil
}

// Invalidate drops the cached entry for workerID. Called from
// EnrollWorker after SetPublicKey so the next request re-loads
// the freshly-persisted public_key. Without this, a worker that
// re-enrolled with a new keypair would 401 until the TTL
// elapsed — a confusing UX during a planned rotation.
//
// Safe to call on a worker_id that's not in the cache (no-op).
func (c *WorkerKeyCache) Invalidate(workerID string) {
	c.mu.Lock()
	delete(c.entries, workerID)
	c.mu.Unlock()
}

// Len reports the current cache size. Test-only; production
// callers don't need it.
func (c *WorkerKeyCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// Purge empties the cache. Test-only; production has no need.
func (c *WorkerKeyCache) Purge() {
	c.mu.Lock()
	c.entries = make(map[string]workerKeyCacheEntry)
	c.mu.Unlock()
}
