package cache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// newUsage builds a minimal TenantUsage for cache tests. The struct's
// fields are not asserted on; only the pointer identity is, so the
// caller can compare "served payload == stored payload" without
// scanning the whole shape.
func newUsage(tenantID string) *domain.TenantUsage {
	return &domain.TenantUsage{TenantID: tenantID}
}

// TestUsageCache_FreshHit confirms that a payload stored at t0 is
// returned immediately when queried at t0+5s (TTL is 10s).
func TestUsageCache_FreshHit(t *testing.T) {
	c := NewUsageCache(10*time.Second, 60*time.Second)
	now := time.Now()
	c.Set("t_1", newUsage("t_1"), 10*time.Second)

	got, ok, stale := c.TryGet("t_1", now.Add(5*time.Second))
	if !ok {
		t.Fatal("TryGet = miss, want hit")
	}
	if got.TenantID != "t_1" {
		t.Errorf("got tenant %q, want t_1", got.TenantID)
	}
	if !stale.IsZero() {
		t.Errorf("fresh hit returned stale cutoff %v, want zero", stale)
	}
}

// TestUsageCache_MissThenSetThenHit exercises the full miss → fetch
// → Set → hit cycle that the usage service uses on the request path.
func TestUsageCache_MissThenSetThenHit(t *testing.T) {
	c := NewUsageCache(10*time.Second, 60*time.Second)
	now := time.Now()

	if _, ok, _ := c.TryGet("t_1", now); ok {
		t.Fatal("initial TryGet = hit, want miss")
	}
	c.Set("t_1", newUsage("t_1"), 10*time.Second)
	got, ok, _ := c.TryGet("t_1", now.Add(time.Second))
	if !ok {
		t.Fatal("after Set, TryGet = miss, want hit")
	}
	if got.TenantID != "t_1" {
		t.Errorf("got tenant %q, want t_1", got.TenantID)
	}
}

// TestUsageCache_StaleHitWithinWindow confirms that an entry past
// expiry but within the max-stale window is still served, with a
// non-zero stale cutoff returned so the caller knows it's stale.
func TestUsageCache_StaleHitWithinWindow(t *testing.T) {
	c := NewUsageCache(10*time.Second, 60*time.Second)
	t0 := time.Now()
	c.Set("t_1", newUsage("t_1"), 10*time.Second)

	// 15s later: past expiry (10s), inside max-stale window (60s).
	got, ok, staleCutoff := c.TryGet("t_1", t0.Add(15*time.Second))
	if !ok {
		t.Fatal("TryGet = miss, want stale-hit")
	}
	if got.TenantID != "t_1" {
		t.Errorf("got tenant %q, want t_1", got.TenantID)
	}
	if !staleCutoff.After(t0) {
		t.Errorf("stale cutoff %v should be after t0=%v", staleCutoff, t0)
	}
	// Singleflight guard: TryStartRefresh should succeed because no
	// other caller has claimed the refresh yet.
	if !c.TryStartRefresh("t_1") {
		t.Error("first TryStartRefresh = false, want true")
	}
}

// TestUsageCache_StaleHitPastWindow confirms that an entry past the
// max-stale window is dropped (treated as a miss) so the next caller
// does a synchronous refetch. Without this, a long-stale value would
// be served indefinitely after the refresh path stops being hit.
func TestUsageCache_StaleHitPastWindow(t *testing.T) {
	c := NewUsageCache(10*time.Second, 60*time.Second)
	t0 := time.Now()
	c.Set("t_1", newUsage("t_1"), 10*time.Second)

	// 200s later: past both expiry (10s) and max-stale (60s).
	if _, ok, _ := c.TryGet("t_1", t0.Add(200*time.Second)); ok {
		t.Fatal("TryGet past max-stale = hit, want miss (drop)")
	}
	if c.Len() != 0 {
		t.Errorf("entry should be dropped after max-stale; Len = %d", c.Len())
	}
}

// TestUsageCache_SingleflightConcurrent confirms that TryStartRefresh
// returns true for exactly one caller among many concurrent callers
// hitting a stale entry. The other callers serve the stale value or
// return a 503 — they MUST NOT also kick a refetch goroutine, or we'd
// run 100 redundant DB queries on every dashboard refresh tick.
func TestUsageCache_SingleflightConcurrent(t *testing.T) {
	c := NewUsageCache(10*time.Second, 60*time.Second)
	c.Set("t_1", newUsage("t_1"), 10*time.Second)

	const goroutines = 100
	var winners int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if c.TryStartRefresh("t_1") {
				atomic.AddInt32(&winners, 1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if winners != 1 {
		t.Errorf("winners = %d, want 1 (singleflight)", winners)
	}

	// After FinishRefresh, the next caller should be able to claim.
	c.FinishRefresh("t_1")
	if !c.TryStartRefresh("t_1") {
		t.Error("TryStartRefresh after FinishRefresh = false, want true")
	}
}

// TestUsageCache_TryStartRefresh_NoEntry confirms that TryStartRefresh
// returns true when there's no entry — the caller is the refetcher,
// not a concurrent duplicate.
func TestUsageCache_TryStartRefresh_NoEntry(t *testing.T) {
	c := NewUsageCache(10*time.Second, 60*time.Second)
	if !c.TryStartRefresh("t_missing") {
		t.Error("TryStartRefresh on missing entry = false, want true")
	}
}

// TestUsageCache_Invalidate confirms Invalidate removes the entry so
// the next TryGet returns a miss. Used by the service when it learns
// of a state change (e.g. quota reset) that should be reflected
// immediately.
func TestUsageCache_Invalidate(t *testing.T) {
	c := NewUsageCache(10*time.Second, 60*time.Second)
	c.Set("t_1", newUsage("t_1"), 10*time.Second)
	c.Invalidate("t_1")
	if _, ok, _ := c.TryGet("t_1", time.Now()); ok {
		t.Error("TryGet after Invalidate = hit, want miss")
	}
}

// TestUsageCache_MaxStaleZero confirms that maxStale=0 disables
// stale-while-revalidate entirely: past expiry, every read is a miss.
// This is the safe default for callers that don't want stale data
// ever (e.g. when refreshing on a state-change event).
func TestUsageCache_MaxStaleZero(t *testing.T) {
	c := NewUsageCache(10*time.Second, 0)
	t0 := time.Now()
	c.Set("t_1", newUsage("t_1"), 10*time.Second)

	if _, ok, _ := c.TryGet("t_1", t0.Add(11*time.Second)); ok {
		t.Error("TryGet past expiry with maxStale=0 = hit, want miss")
	}
}

// TestUsageCache_RaceSafe runs all the read/write paths under -race
// to catch any concurrency holes. Run with `go test -race ./...`.
func TestUsageCache_RaceSafe(t *testing.T) {
	c := NewUsageCache(10*time.Second, 60*time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(4)
		go func() { defer wg.Done(); c.Set("t_1", newUsage("t_1"), 10*time.Second) }()
		go func() { defer wg.Done(); c.TryGet("t_1", time.Now()) }()
		go func() { defer wg.Done(); c.TryStartRefresh("t_1") }()
		go func() { defer wg.Done(); c.FinishRefresh("t_1") }()
	}
	wg.Wait()
}
