package middleware

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestWorkerKeyCache_HitMissExpiry pins the three cache states: a
// fresh miss triggers the loader and caches the result; a hit
// short-circuits the loader; an expired entry triggers a fresh
// load on the next GetOrLoad.
func TestWorkerKeyCache_HitMissExpiry(t *testing.T) {
	var calls int32
	loader := func(ctx context.Context, workerID string) (string, error) {
		atomic.AddInt32(&calls, 1)
		return "pubkey-" + workerID, nil
	}
	c := NewWorkerKeyCache(loader)
	c.SetTTL(50 * time.Millisecond) // tight TTL so expiry is testable
	defer c.Purge()

	// 1. Miss.
	got, err := c.GetOrLoad(context.Background(), "w_1")
	if err != nil {
		t.Fatalf("miss: %v", err)
	}
	if got != "pubkey-w_1" {
		t.Errorf("miss: got %q, want pubkey-w_1", got)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("after miss: loader calls = %d, want 1", n)
	}
	// 2. Hit (within TTL).
	got, err = c.GetOrLoad(context.Background(), "w_1")
	if err != nil {
		t.Fatalf("hit: %v", err)
	}
	if got != "pubkey-w_1" {
		t.Errorf("hit: got %q, want pubkey-w_1", got)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("after hit: loader calls = %d, want 1 (still cached)", n)
	}
	// 3. Wait for TTL to elapse, then re-read.
	time.Sleep(60 * time.Millisecond)
	got, err = c.GetOrLoad(context.Background(), "w_1")
	if err != nil {
		t.Fatalf("post-expiry: %v", err)
	}
	if got != "pubkey-w_1" {
		t.Errorf("post-expiry: got %q, want pubkey-w_1", got)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("after expiry: loader calls = %d, want 2", n)
	}
}

// TestWorkerKeyCache_NegativeLookup pins the empty-result path: the
// loader returns "" (the worker has no enrolled public_key). The
// cache does NOT memoize the empty result — the next call retries
// the loader, which is the right behavior for the operator's
// just-enrolled-worker scenario.
func TestWorkerKeyCache_NegativeLookup(t *testing.T) {
	var calls int32
	loader := func(ctx context.Context, workerID string) (string, error) {
		atomic.AddInt32(&calls, 1)
		return "", nil // no enrolled pubkey
	}
	c := NewWorkerKeyCache(loader)

	got, err := c.GetOrLoad(context.Background(), "w_x")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("first call: loader calls = %d, want 1", n)
	}
	// Second call must NOT hit the cache (empty is not memoized).
	got, err = c.GetOrLoad(context.Background(), "w_x")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("second call: loader calls = %d, want 2 (empty result must not be cached)", n)
	}
}

// TestWorkerKeyCache_InvalidationOnPubkeyChange pins the
// post-enrollment path: a worker re-enrolls with a new keypair,
// EnrollWorker calls Invalidate, and the next GetOrLoad fetches
// the new public_key. Without Invalidate the worker would 401
// until the TTL elapsed.
func TestWorkerKeyCache_InvalidationOnPubkeyChange(t *testing.T) {
	var calls int32
	pubkeyA := "aaaa" + repeat("a", 60)
	pubkeyB := "bbbb" + repeat("b", 60)
	loader := func(ctx context.Context, workerID string) (string, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return pubkeyA, nil
		}
		return pubkeyB, nil
	}
	c := NewWorkerKeyCache(loader)

	got, err := c.GetOrLoad(context.Background(), "w_r")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if got != pubkeyA {
		t.Errorf("first: got %q, want pubkeyA", got)
	}

	// Simulate EnrollWorker calling Invalidate after SetPublicKey.
	c.Invalidate("w_r")

	got, err = c.GetOrLoad(context.Background(), "w_r")
	if err != nil {
		t.Fatalf("after invalidate: %v", err)
	}
	if got != pubkeyB {
		t.Errorf("after invalidate: got %q, want pubkeyB", got)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("loader calls = %d, want 2 (invalidate must force re-fetch)", n)
	}
}

// TestWorkerKeyCache_LoaderErrorNotCached pins the loader-error
// path: a transient DB blip must not permanently disable the
// worker. The cache surfaces the error but does NOT memoize it,
// so the next request retries.
func TestWorkerKeyCache_LoaderErrorNotCached(t *testing.T) {
	var calls int32
	loader := func(ctx context.Context, workerID string) (string, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return "", errors.New("db blip")
		}
		return "recovered-pubkey", nil
	}
	c := NewWorkerKeyCache(loader)

	if _, err := c.GetOrLoad(context.Background(), "w_e"); err == nil {
		t.Fatal("first call: expected error, got nil")
	}
	// Second call must retry — error not cached.
	got, err := c.GetOrLoad(context.Background(), "w_e")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got != "recovered-pubkey" {
		t.Errorf("got %q, want recovered-pubkey", got)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("loader calls = %d, want 2 (error must not be cached)", n)
	}
}

// TestWorkerKeyCache_EmptyWorkerID pins the no-op path: an empty
// workerID returns ("", nil) without calling the loader. This
// guards WorkerAuth from spending a DB query when claims.WorkerID
// is empty (defense-in-depth — WorkerAuth already checks this
// before resolving, but the cache layer's contract should not
// silently make a DB call either).
func TestWorkerKeyCache_EmptyWorkerID(t *testing.T) {
	var called bool
	loader := func(ctx context.Context, workerID string) (string, error) {
		called = true
		return "", nil
	}
	c := NewWorkerKeyCache(loader)
	got, err := c.GetOrLoad(context.Background(), "")
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
	if called {
		t.Error("loader called for empty workerID; should short-circuit")
	}
}

// repeat is a tiny helper to avoid pulling in `strings.Repeat` for
// a single use in the InvalidatedOnPubkeyChange test. The two
// calls only need 60+ chars of "a" / "b" to make the comparison
// readable.
func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}
