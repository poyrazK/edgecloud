package middleware

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMaxBodyBytesMiddleware_CapsOversizeBody confirms that a body
// larger than the middleware cap is rejected by the downstream
// reader as a *http.MaxBytesError. We don't assert the HTTP
// status here — that's the per-handler httperror.MaxBodyBytes
// helper's job. The middleware only caps; it does not translate
// the error into a response.
func TestMaxBodyBytesMiddleware_CapsOversizeBody(t *testing.T) {
	cap := int64(1024)
	var downstreamErr error
	h := MaxBodyBytes(cap)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, downstreamErr = io.ReadAll(r.Body)
	}))

	body := strings.Repeat("x", 2048) // 2x the cap
	req := httptest.NewRequest(http.MethodPost, "/anything", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if downstreamErr == nil {
		t.Fatal("expected io.ReadAll to fail on oversized body, got nil error")
	}
	var maxErr *http.MaxBytesError
	if !errors.As(downstreamErr, &maxErr) {
		t.Fatalf("expected *http.MaxBytesError, got %T: %v", downstreamErr, downstreamErr)
	}
	if maxErr.Limit != cap {
		t.Errorf("expected Limit=%d, got %d", cap, maxErr.Limit)
	}
}

// TestMaxBodyBytesMiddleware_AllowsUnderCap confirms the middleware
// is transparent for bodies under the cap — the downstream reader
// gets every byte.
func TestMaxBodyBytesMiddleware_AllowsUnderCap(t *testing.T) {
	cap := int64(1024)
	var read int
	h := MaxBodyBytes(cap)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		read = len(buf)
	}))

	body := strings.Repeat("y", 512) // half the cap
	req := httptest.NewRequest(http.MethodPost, "/anything", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if read != 512 {
		t.Errorf("expected 512 bytes read, got %d", read)
	}
}

// TestMaxBodyBytesMiddleware_GETBodyEmpty confirms GET requests
// pass through (empty body, no cap interaction). A handler that
// ignores the body on GET should see EOF on read.
func TestMaxBodyBytesMiddleware_GETBodyEmpty(t *testing.T) {
	cap := int64(1024)
	var empty bool
	h := MaxBodyBytes(cap)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("unexpected error on GET: %v", err)
		}
		empty = len(buf) == 0
	}))

	req := httptest.NewRequest(http.MethodGet, "/anything", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if !empty {
		t.Error("expected GET request to have empty body")
	}
}

// TestMaxBodyBytesMiddleware_ComposesWithTighterHandler confirms
// that a downstream handler's tighter MaxBytesReader wrap takes
// precedence over a more permissive middleware cap. The smaller
// cap wins via the wrapper chain. Migrate (50 MiB) and IngestLogs
// (1 MiB) rely on this composition.
func TestMaxBodyBytesMiddleware_ComposesWithTighterHandler(t *testing.T) {
	// Middleware cap = 1 MiB; handler cap = 100 B (tighter).
	// A 200-byte body should be capped at 100 B even though the
	// middleware allowed 1 MiB.
	middlewareCap := int64(1 << 20) // 1 MiB
	handlerCap := int64(100)
	var downstreamErr error
	h := MaxBodyBytes(middlewareCap)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Mirror the per-handler pattern: re-wrap the body.
		r.Body = http.MaxBytesReader(w, r.Body, handlerCap)
		_, downstreamErr = io.ReadAll(r.Body)
	}))

	body := strings.Repeat("z", 200)
	req := httptest.NewRequest(http.MethodPost, "/anything", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if downstreamErr == nil {
		t.Fatal("expected tighter handler cap to trigger, got nil error")
	}
	var maxErr *http.MaxBytesError
	if !errors.As(downstreamErr, &maxErr) {
		t.Fatalf("expected *http.MaxBytesError, got %T: %v", downstreamErr, downstreamErr)
	}
	if maxErr.Limit != handlerCap {
		t.Errorf("expected handler cap (Limit=%d) to win over middleware cap, got Limit=%d", handlerCap, maxErr.Limit)
	}
}
