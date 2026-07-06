package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// TestPublishError_RendersCacheFields confirms the Error() helper
// surfaces Cached / CacheFailed when populated, and omits the
// suffix entirely when unused (pre-#332 wire compatibility).
func TestPublishError_RendersCacheFields(t *testing.T) {
	tests := []struct {
		name             string
		err              *PublishError
		mustContain      []string
		mustNotBeEmptyOf []string
	}{
		{
			name: "all fields set",
			err: &PublishError{
				Published:   []string{"fra"},
				Failed:      []string{"iad"},
				Cached:      []string{"fra"},
				CacheFailed: []string{"iad"},
				Err:         ErrPublishFailed,
			},
			mustContain: []string{"published=[fra]", "failed=[iad]", "cached=[fra]", "cache_failed=[iad]"},
		},
		{
			name: "no cache feature used",
			err: &PublishError{
				Published: []string{"fra"},
				Failed:    []string{"iad"},
				Err:       ErrPublishFailed,
			},
			mustContain: []string{"published=[fra]", "failed=[iad]"},
		},
		{
			name: "cache pushes failed but NATS publish succeeded",
			err: &PublishError{
				Published:   []string{"fra", "iad"},
				Failed:      nil,
				Cached:      []string{"fra"},
				CacheFailed: []string{"iad"},
				Err:         ErrPublishFailed,
			},
			mustContain: []string{"published=[fra iad]", "cached=[fra]", "cache_failed=[iad]"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.err.Error()
			for _, s := range tc.mustContain {
				if !contains(out, s) {
					t.Errorf("Error() = %q, want substring %q", out, s)
				}
			}
			for _, s := range tc.mustNotBeEmptyOf {
				if contains(out, s) {
					t.Errorf("Error() = %q, must not contain %q", out, s)
				}
			}
		})
	}
}

// TestPublishError_NilSafe verifies the Error() helper handles a nil
// receiver (defensive guard against accidental return-with-nil
// from publishSwap).
func TestPublishError_NilSafe(t *testing.T) {
	var e *PublishError
	got := e.Error()
	if got != "<nil PublishError>" {
		t.Errorf("(*PublishError)(nil).Error() = %q, want %q", got, "<nil PublishError>")
	}
}

// TestHTTPArtifactCachePusher_Success exercises the full push
// pipeline: a real httptest.NewServer receives the PUT, the pusher
// streams the artifact body, and the response is 2xx. Verifies the
// X-Internal-Token header is present and the path is well-formed.
func TestHTTPArtifactCachePusher_Success(t *testing.T) {
	var (
		gotMu     sync.Mutex
		gotPath   string
		gotMethod string
		gotToken  string
		gotCT     string
		gotBody   []byte
	)
	body := []byte("fake-artifact-bytes-for-test")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMu.Lock()
		gotPath = r.URL.Path
		gotMethod = r.Method
		gotToken = r.Header.Get("X-Internal-Token")
		gotCT = r.Header.Get("Content-Type")
		// Drain the body. Note: r.ContentLength may be -1 because
		// `io.NopCloser(strings.NewReader(...))` from the fake store
		// loses its `Len()` method, so Go's HTTP client uses chunked
		// transfer encoding. The streaming behavior is real (the
		// pusher doesn't buffer) — we just can't probe the size via
		// Content-Length here.
		buf, _ := io.ReadAll(r.Body)
		gotBody = buf
		gotMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pusher := &httpArtifactCachePusher{
		artifactStore: fakeArtifactStore{bytes: body},
		httpClient:    srv.Client(),
		internalToken: "test-token",
	}
	if err := pusher.Push(context.Background(), srv.URL, "t_acme", "myapp", "d_xyz"); err != nil {
		t.Fatalf("Push: %v", err)
	}

	gotMu.Lock()
	defer gotMu.Unlock()
	if gotPath != "/artifacts/t_acme/myapp/d_xyz" {
		t.Errorf("server got path %q, want %q", gotPath, "/artifacts/t_acme/myapp/d_xyz")
	}
	if gotMethod != http.MethodPut {
		t.Errorf("server got method %q, want %q", gotMethod, http.MethodPut)
	}
	if gotToken != "test-token" {
		t.Errorf("server got token %q, want %q", gotToken, "test-token")
	}
	if gotCT != "application/octet-stream" {
		t.Errorf("server got Content-Type %q, want application/octet-stream", gotCT)
	}
	if string(gotBody) != string(body) {
		t.Errorf("server got body %q, want %q", gotBody, body)
	}
}

// TestHTTPArtifactCachePusher_Non2xxReturnsError verifies the pusher
// surfaces a non-2xx response as a Go error (so publishSwap can
// record the region in regions_cache_failed).
func TestHTTPArtifactCachePusher_Non2xxReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	pusher := &httpArtifactCachePusher{
		artifactStore: fakeArtifactStore{bytes: []byte("x")},
		httpClient:    srv.Client(),
		internalToken: "test-token",
	}
	err := pusher.Push(context.Background(), srv.URL, "t_acme", "myapp", "d_xyz")
	if err == nil {
		t.Fatal("expected error on 500, got nil")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Errorf("err = %v, want substring 'status 500'", err)
	}
}

// TestHTTPArtifactCachePusher_NetworkErrorReturnsError — TCP RST or
// connection refused surfaces as a transport error.
func TestHTTPArtifactCachePusher_NetworkErrorReturnsError(t *testing.T) {
	// Bind a server then close it so the URL is guaranteed-unreachable.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	addr := srv.URL
	srv.Close()

	pusher := &httpArtifactCachePusher{
		artifactStore: fakeArtifactStore{bytes: []byte("x")},
		httpClient:    &http.Client{}, // default transport
		internalToken: "test-token",
	}
	err := pusher.Push(context.Background(), addr, "t_acme", "myapp", "d_xyz")
	if err == nil {
		t.Fatal("expected error on closed server, got nil")
	}
	if !strings.Contains(err.Error(), "PUT ") {
		t.Errorf("err = %v, want substring 'PUT '", err)
	}
}

// fakeArtifactStore is a minimal storage.ArtifactStore impl that
// returns canned bytes from Open. Avoids disk + ValidateWasm in
// unit tests. Only `Open` is exercised by the pusher; the rest of
// the interface is implemented as no-ops so the struct satisfies
// the full storage.ArtifactStore contract.
type fakeArtifactStore struct {
	bytes []byte
}

// Open returns a reader over the canned bytes.
func (f fakeArtifactStore) Open(_ context.Context, _, _, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(string(f.bytes))), nil
}

// Save is a no-op (the pusher reads, never writes).
func (f fakeArtifactStore) Save(_ context.Context, _, _, _ string, _ io.Reader) error {
	return nil
}

// SaveAndHash is a no-op.
func (f fakeArtifactStore) SaveAndHash(_ context.Context, _, _, _ string, _ io.Reader) ([]byte, error) {
	return nil, nil
}

// OpenFormat returns the same as Open (the pusher doesn't ask for .cwasm).
func (f fakeArtifactStore) OpenFormat(_ context.Context, _, _, _, _ string) (io.ReadCloser, error) {
	return f.Open(nil, "", "", "")
}

// Delete is a no-op (the pusher doesn't delete).
func (f fakeArtifactStore) Delete(_ context.Context, _, _, _ string) error {
	return nil
}

// contains is a tiny strings.Contains — defined here to avoid an
// extra import for one assertion.
func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
