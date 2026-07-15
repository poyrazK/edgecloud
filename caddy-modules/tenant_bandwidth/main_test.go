package tenantbandwidth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"golang.org/x/time/rate"
)

// newKeyed returns a fresh TenantBandwidth with `Key` set and the
// limiter map pre-allocated. Tests bypass `Provision` since they
// construct the struct directly.
func newKeyed(key string, bps int64) *TenantBandwidth {
	return &TenantBandwidth{
		Key:         key,
		BytesPerSec: bps,
		limiters:    make(map[string]*rate.Limiter),
	}
}

// newKeyedWithBurst is the test-only escape hatch used by the
// pacing-assertion tests (TestServeHTTPThrottlesAtCap and
// TestPacingWriterContextCancellation). Production code uses
// burst=BytesPerSec (one second of traffic). Tests that want
// to assert pacing behavior pin the burst explicitly. The
// minimum useful burst is the chunkSize the production code
// computes (bps/16); below that, WaitN errors immediately with
// "exceeds burst". We expose burst as a parameter so each test
// picks the smallest value that exercises the path it cares
// about.
//
// We construct the limiter directly (instead of going through
// limiterFor + BytesPerSec) so the test can pin the burst
// independently of the per-second rate.
func newKeyedWithBurst(key string, bps int64, burst int) *TenantBandwidth {
	tb := newKeyed(key, bps)
	tb.limiters[key] = rate.NewLimiter(rate.Limit(bps), burst)
	return tb
}

// rec is a minimal caddyhttp.Handler that writes a body of the
// configured size and returns the supplied status. The body is
// written as a single Write call so the pacing wrapper sees one
// WaitN(len(body)) invocation.
type rec struct {
	called  atomic.Int32
	status  int
	bodyLen int
	delay   time.Duration
}

func (r *rec) ServeHTTP(w http.ResponseWriter, _ *http.Request) error {
	r.called.Add(1)
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	w.WriteHeader(r.status)
	if r.bodyLen > 0 {
		body := make([]byte, r.bodyLen)
		// Propagate the Write error verbatim. The pacing writer
		// returns context.Canceled when the request context is
		// cancelled (e.g. client disconnect); swallowing that
		// would mask the disconnect signal — production handlers
		// must surface it. The discarded-by-design `_` form
		// would mask TestPacingWriterContextCancellation.
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	return nil
}

func TestProvisionLazyAllocatesLimiters(t *testing.T) {
	tb := &TenantBandwidth{Key: "tenant-t_acme", BytesPerSec: 1000}
	var ctx caddy.Context
	if err := tb.Provision(ctx); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if tb.limiters == nil {
		t.Fatal("Provision must initialize the limiters map")
	}
	// No limiter created yet — limiters are lazy on first request.
	if len(tb.limiters) != 0 {
		t.Fatalf("expected 0 limiters after Provision, got %d", len(tb.limiters))
	}
}

// TestProvisionRejectsNonPositiveBytesPerSec pins defense-in-depth:
// the renderer only emits a route when bandwidth_bps > 0, but a
// hand-edited Caddyfile-JSON or a future renderer regression could
// ship 0/negative. Without this check, rate.NewLimiter(0, 0) would
// build a limiter that rejects every byte — silently turning the
// pacer into a hard-deny-everything block.
func TestProvisionRejectsNonPositiveBytesPerSec(t *testing.T) {
	for _, bps := range []int64{0, -1, -50} {
		tb := &TenantBandwidth{Key: "tenant-t_acme", BytesPerSec: bps}
		var ctx caddy.Context
		if err := tb.Provision(ctx); err == nil {
			t.Fatalf("Provision with bytes_per_sec=%d must return an error, got nil", bps)
		}
		// Provision must not have allocated the limiters map on
		// failure — a partial init would be worse than a clean reject.
		if tb.limiters != nil {
			t.Fatalf("Provision with bytes_per_sec=%d must not initialize limiters, got %v", bps, tb.limiters)
		}
	}
}

func TestCleanupDropsLimiters(t *testing.T) {
	tb := newKeyed("tenant-t_acme", 1000)
	// Trigger one request to materialize a limiter.
	if err := tb.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil),
		&rec{status: 200, bodyLen: 16},
	); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if len(tb.limiters) == 0 {
		t.Fatal("expected at least one limiter after a request")
	}
	if err := tb.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if tb.limiters != nil {
		t.Fatal("Cleanup must drop the limiters map")
	}
}

func TestServeHTTPProceedsUnderCap(t *testing.T) {
	// 1 MB/s cap, 1 KB body — the bucket starts full at burst size
	// (1 MB worth of tokens) so the first request finishes in well
	// under a millisecond. We allow a generous 500ms ceiling to
	// dodge CI scheduler jitter on slow runners.
	tb := newKeyed("tenant-t_acme", 1_000_000)
	rec := &rec{status: 200, bodyLen: 1024}
	w := httptest.NewRecorder()
	start := time.Now()
	if err := tb.ServeHTTP(w, httptest.NewRequest("GET", "/", nil), rec); err != nil {
		t.Fatalf("ServeHTTP under cap: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("under-cap request took %v, expected < 500ms", d)
	}
	if rec.called.Load() != 1 {
		t.Fatalf("next must be called once, got %d", rec.called.Load())
	}
	if w.Code != 200 {
		t.Fatalf("status: got %d want 200", w.Code)
	}
}

// TestServeHTTPThrottlesAtCap exercises the pacing path end-to-end:
// a 100 B/s limiter is asked to deliver a 1 KB body (10× the burst
// of 100 bytes). With the bucket drained to 0 between bursts, the
// cap paces the response so the full body takes roughly
// (1024-100)/100 = ~9.24s. Asserting ≥ 8s gives CI scheduler
// jitter a comfortable margin (a slow runner might still finish at
// 9.5s; a fast one at 9.0s; either passes).
//
// Skipped under `go test -short` so CI's quick-gate build (which
// runs on every commit) doesn't pay the ~10s tax. The full
// `go test -race ./...` run in the caddy-image CI job does NOT pass
// `-short`, so this assertion fires there.
func TestServeHTTPThrottlesAtCap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pacing assertion under -short (full run exercises this)")
	}
	// Burst=100 (1 second of traffic at the cap). The 1024-byte
	// body drains in two phases: first 100 bytes from the burst
	// (instant), then the remaining 924 bytes paced at 100 B/s
	// (≈ 9.24s). Asserting ≥ 8s gives CI scheduler jitter a
	// comfortable margin (slow runner finishes at ~9.5s; fast
	// one at ~9.0s; either passes).
	//
	// Note: burst must be ≥ the production code's chunkSize
	// (bps/16 = 6 for bps=100); a smaller burst causes
	// rate.Limiter.WaitN to error immediately with "exceeds
	// burst" — that's why production code pairs burst with
	// chunked writes, and why the test uses burst=100, not 0.
	tb := newKeyedWithBurst("tenant-t_acme", 100, 100)
	rec := &rec{status: 200, bodyLen: 1024}
	w := httptest.NewRecorder()
	start := time.Now()
	if err := tb.ServeHTTP(w, httptest.NewRequest("GET", "/", nil), rec); err != nil {
		t.Fatalf("ServeHTTP at cap: %v", err)
	}
	if d := time.Since(start); d < 8*time.Second {
		t.Fatalf("paced request took %v, expected ≥ 8s (1024B body at 100B/s cap with burst=100)", d)
	}
	if rec.called.Load() != 1 {
		t.Fatalf("next must be called once, got %d", rec.called.Load())
	}
}

// TestServeHTTPReleasesLimiterOnError is the bandwidth-side mirror
// of tenant_concurrent's TestServeHTTPReleasesSlotOnError: even
// when the downstream handler returns an error, the limiter state
// must NOT be left in a corrupted state. Limiter state is
// per-key (not per-request) — the bucket drain happens inside
// rate.Limiter.WaitN, which is fully driven by the token clock
// regardless of how next.ServeHTTP returns. We assert this by
// exhausting the burst (so the bucket is at zero), then triggering
// an error path, then re-issuing a request and confirming the
// limiter is still functional (the follow-up eventually completes).
func TestServeHTTPReleasesLimiterOnError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pacing assertion under -short (full run exercises this)")
	}
	tb := newKeyed("tenant-t_acme", 100) // 100 byte burst
	// First request drains the bucket. Use a 500-byte body so
	// WaitN blocks for ~4s waiting for tokens.
	if err := tb.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil),
		&errRec{err: errors.New("boom"), bodyLen: 500},
	); err == nil {
		t.Fatal("expected error from boom handler")
	}
	// Wait for the bucket to refill enough that the follow-up
	// finishes quickly (200 ms at 100 B/s = 20 tokens).
	time.Sleep(250 * time.Millisecond)
	// Follow-up request — limiter is still functional.
	rec := &rec{status: 200, bodyLen: 16}
	if err := tb.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil),
		rec,
	); err != nil {
		t.Fatalf("ServeHTTP after error: %v", err)
	}
	if rec.called.Load() != 1 {
		t.Fatalf("next must be called once after error recovery, got %d", rec.called.Load())
	}
}

func TestServeHTTPEmptyKeyReturns500(t *testing.T) {
	tb := &TenantBandwidth{
		Key:         "",
		BytesPerSec: 1000,
		limiters:    make(map[string]*rate.Limiter),
	}
	err := tb.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil),
		&rec{status: 200, bodyLen: 16},
	)
	if err == nil {
		t.Fatal("expected error when Key is empty")
	}
	httpErr, ok := err.(caddyhttp.HandlerError)
	if !ok {
		t.Fatalf("expected caddyhttp.HandlerError, got %T: %v", err, err)
	}
	if httpErr.StatusCode != 500 {
		t.Fatalf("status: got %d want 500", httpErr.StatusCode)
	}
}

// TestServeHTTPSharedKeyAcrossRequests documents the per-instance
// isolation: two TenantBandwidth instances with the same Key keep
// independent limiters. The renderer could choose to dedupe at
// config-load time, but each route is a distinct struct instance,
// so the per-route bucket stays scoped to that route. This is the
// bandwidth-side mirror of tenant_concurrent's
// TestServeHTTPSharedKeyAcrossRequests.
func TestServeHTTPSharedKeyAcrossRequests(t *testing.T) {
	tb1 := newKeyed("tenant-shared", 100)
	tb2 := newKeyed("tenant-shared", 100)

	// First request on tb1 drains its limiter's burst.
	if err := tb1.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil),
		&rec{status: 200, bodyLen: 16},
	); err != nil {
		t.Fatalf("tb1.ServeHTTP: %v", err)
	}
	if len(tb1.limiters) != 1 || len(tb2.limiters) != 0 {
		t.Fatalf("expected tb1 to have 1 limiter and tb2 to have 0, got %d / %d",
			len(tb1.limiters), len(tb2.limiters))
	}

	// tb2 with the same key — but tb2 has its own limiters map, so
	// its limiter is independent of tb1's.
	w := httptest.NewRecorder()
	if err := tb2.ServeHTTP(
		w,
		httptest.NewRequest("GET", "/", nil),
		&rec{status: 200, bodyLen: 16},
	); err != nil {
		t.Fatalf("tb2.ServeHTTP under cap: %v", err)
	}
	if w.Code != 200 {
		t.Fatalf("tb2 status: got %d want 200", w.Code)
	}
	if len(tb2.limiters) != 1 {
		t.Fatalf("tb2 should now have 1 limiter after first request, got %d", len(tb2.limiters))
	}
}

// TestPacingWriterContextCancellation pins that rate.Limiter.WaitN
// honors the request context. The pacing primitive must surface
// ctx.Err() when the client disconnects (rather than blocking
// forever). We pre-cancel the context so the FIRST chunked WaitN
// sees an already-cancelled context — deterministic timing, no
// race with the cancel goroutine. This is the load-bearing
// property: a cancelled context must return immediately, not hang.
func TestPacingWriterContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pacing assertion under -short (full run exercises this)")
	}
	tb := newKeyed("tenant-t_acme", 100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so the first WaitN observes ctx.Err()
	rec := &rec{status: 200, bodyLen: 200}
	err := tb.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil).WithContext(ctx),
		rec,
	)
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %T: %v", err, err)
	}
}

// errRec is a caddyhttp.Handler that returns the configured error
// after writing the configured body length. Used by
// TestServeHTTPReleasesLimiterOnError.
type errRec struct {
	err     error
	bodyLen int
}

func (r *errRec) ServeHTTP(w http.ResponseWriter, _ *http.Request) error {
	if r.bodyLen > 0 {
		body := make([]byte, r.bodyLen)
		_, _ = w.Write(body)
	}
	return r.err
}
