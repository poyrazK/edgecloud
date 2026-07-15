package tenantconcurrent

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// newKeyed returns a fresh TenantConcurrent with `Key` set and the
// bucket map pre-allocated. Tests bypass `Provision` since they
// construct the struct directly.
func newKeyed(key string, limit int) *TenantConcurrent {
	return &TenantConcurrent{
		Key:     key,
		Limit:   limit,
		buckets: make(map[string]chan struct{}),
	}
}

// rec is a minimal caddyhttp.Handler that records that it ran and
// returns the supplied status.
type rec struct {
	called  atomic.Int32
	status  int
	delay   time.Duration
}

func (r *rec) ServeHTTP(w http.ResponseWriter, _ *http.Request) error {
	r.called.Add(1)
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	w.WriteHeader(r.status)
	return nil
}

func TestProvisionLazyAllocatesBuckets(t *testing.T) {
	tc := &TenantConcurrent{Key: "tenant-t_acme", Limit: 5}
	var ctx caddy.Context
	if err := tc.Provision(ctx); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if tc.buckets == nil {
		t.Fatal("Provision must initialize the buckets map")
	}
	// No bucket created yet — buckets are lazy on first request.
	if len(tc.buckets) != 0 {
		t.Fatalf("expected 0 buckets after Provision, got %d", len(tc.buckets))
	}
}

// TestProvisionRejectsNonPositiveLimit pins defense-in-depth: the
// renderer only emits a route when concurrent_limit > 0, but a
// hand-edited Caddyfile-JSON or a future renderer regression could
// ship 0/negative. Without this check, `make(chan struct{}, 0)`
// would build an unbuffered channel that always rejects, silently
// turning the limiter into a hard-deny-everything block.
func TestProvisionRejectsNonPositiveLimit(t *testing.T) {
	for _, limit := range []int{0, -1, -50} {
		tc := &TenantConcurrent{Key: "tenant-t_acme", Limit: limit}
		var ctx caddy.Context
		if err := tc.Provision(ctx); err == nil {
			t.Fatalf("Provision with limit=%d must return an error, got nil", limit)
		}
		// Provision must not have allocated the buckets map on failure —
		// a partial init would be worse than a clean reject.
		if tc.buckets != nil {
			t.Fatalf("Provision with limit=%d must not initialize buckets, got %v", limit, tc.buckets)
		}
	}
}

func TestCleanupDropsBuckets(t *testing.T) {
	tc := newKeyed("tenant-t_acme", 5)
	// Trigger one request to materialize a bucket.
	if err := tc.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil),
		&rec{status: 200},
	); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if len(tc.buckets) == 0 {
		t.Fatal("expected at least one bucket after a request")
	}
	if err := tc.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if tc.buckets != nil {
		t.Fatal("Cleanup must drop the buckets map")
	}
}

func TestServeHTTPProceedsUnderCap(t *testing.T) {
	tc := newKeyed("tenant-t_acme", 3)
	rec := &rec{status: 200}
	w := httptest.NewRecorder()
	if err := tc.ServeHTTP(w, httptest.NewRequest("GET", "/", nil), rec); err != nil {
		t.Fatalf("ServeHTTP under cap: %v", err)
	}
	if rec.called.Load() != 1 {
		t.Fatalf("next must be called once, got %d", rec.called.Load())
	}
	if w.Code != 200 {
		t.Fatalf("status: got %d want 200", w.Code)
	}
}

func TestServeHTTPRejectsAtCap(t *testing.T) {
	tc := newKeyed("tenant-t_acme", 2)

	// Hold two slots concurrently.
	hold := make(chan struct{})
	released := make(chan struct{})
	go func() {
		_ = tc.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest("GET", "/", nil),
			&rec{status: 200, delay: 200 * time.Millisecond},
		)
		hold <- struct{}{}
	}()
	go func() {
		_ = tc.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest("GET", "/", nil),
			&rec{status: 200, delay: 200 * time.Millisecond},
		)
		hold <- struct{}{}
	}()
	// Give the two goroutines time to acquire.
	time.Sleep(20 * time.Millisecond)

	// Third request — should be rejected with 429 + Retry-After: 1.
	w := httptest.NewRecorder()
	err := tc.ServeHTTP(w, httptest.NewRequest("GET", "/", nil), &rec{status: 200})
	if err == nil {
		t.Fatal("expected error from ServeHTTP at cap")
	}
	httpErr, ok := err.(caddyhttp.HandlerError)
	if !ok {
		t.Fatalf("expected caddyhttp.HandlerError, got %T: %v", err, err)
	}
	if httpErr.StatusCode != 429 {
		t.Fatalf("status: got %d want 429", httpErr.StatusCode)
	}
	if got := w.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After: got %q want \"1\"", got)
	}

	// Release the two held slots.
	go func() {
		<-hold
		<-hold
		close(released)
	}()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("held goroutines did not release in time")
	}
}

func TestServeHTTPReleasesSlotOnError(t *testing.T) {
	// Even when `next` returns an error, the deferred receive must
	// release the slot. We assert this by exhausting the cap and
	// confirming a follow-up acquire succeeds (i.e. no slot leak).
	tc := newKeyed("tenant-t_acme", 1)
	boom := &errRec{err: errors.New("boom")}
	if err := tc.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil),
		boom,
	); err == nil {
		t.Fatal("expected error from boom handler")
	}
	// Slot must be free now.
	rec := &rec{status: 200}
	if err := tc.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil),
		rec,
	); err != nil {
		t.Fatalf("ServeHTTP after error: %v", err)
	}
	if rec.called.Load() != 1 {
		t.Fatalf("next must be called once, got %d", rec.called.Load())
	}
}

func TestServeHTTPEmptyKeyReturns500(t *testing.T) {
	tc := &TenantConcurrent{
		Key:     "",
		Limit:   5,
		buckets: make(map[string]chan struct{}),
	}
	err := tc.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil),
		&rec{status: 200},
	)
	if err == nil {
		t.Fatal("expected error when Key is empty")
	}
	httpErr, ok := err.(caddyhttp.HandlerError)
	if !ok {
		t.Fatalf("expected caddyhttp.HandlerError, got %T", err)
	}
	if httpErr.StatusCode != 500 {
		t.Fatalf("status: got %d want 500", httpErr.StatusCode)
	}
}

func TestServeHTTPSharedKeyAcrossRequests(t *testing.T) {
	// Two TenantConcurrent instances with the same Key share the
	// same bucket (keying is by string, not by struct). Useful
	// regression guard if we ever decide to dedupe at the renderer
	// layer.
	tc1 := newKeyed("tenant-shared", 1)
	tc2 := newKeyed("tenant-shared", 1)

	// First request on tc1 holds the only slot.
	hold := make(chan struct{})
	released := make(chan struct{})
	go func() {
		_ = tc1.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest("GET", "/", nil),
			&rec{status: 200, delay: 100 * time.Millisecond},
		)
		close(hold)
	}()
	time.Sleep(10 * time.Millisecond)

	// tc2 with the same key — but tc2 has its own buckets map, so
	// its bucket is independent. This documents the current
	// behavior: each route gets its own struct + map.
	w := httptest.NewRecorder()
	if err := tc2.ServeHTTP(
		w,
		httptest.NewRequest("GET", "/", nil),
		&rec{status: 200},
	); err != nil {
		t.Fatalf("tc2 under cap should succeed, got %v", err)
	}
	if w.Code != 200 {
		t.Fatalf("tc2 status: got %d want 200", w.Code)
	}

	<-hold
	_ = released
}

// errRec is a caddyhttp.Handler that returns the configured error.
type errRec struct{ err error }

func (r *errRec) ServeHTTP(http.ResponseWriter, *http.Request) error {
	return r.err
}