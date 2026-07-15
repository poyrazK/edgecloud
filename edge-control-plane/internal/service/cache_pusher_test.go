package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mockArtifactStoreForCache implements storage.ArtifactStore for testing.
type mockArtifactStoreForCache struct {
	openFn func(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error)
}

func (m *mockArtifactStoreForCache) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	return nil
}
func (m *mockArtifactStoreForCache) Open(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error) {
	if m.openFn != nil {
		return m.openFn(ctx, tenantID, appName, deploymentID)
	}
	return io.NopCloser(strings.NewReader("wasm")), nil
}
func (m *mockArtifactStoreForCache) SaveAndHash(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) ([]byte, error) {
	return nil, nil
}
func (m *mockArtifactStoreForCache) SaveFormat(ctx context.Context, tenantID, appName, deploymentID, format string, r io.Reader) error {
	return nil
}
func (m *mockArtifactStoreForCache) OpenFormat(ctx context.Context, tenantID, appName, deploymentID, format string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockArtifactStoreForCache) Delete(ctx context.Context, tenantID, appName, deploymentID string) error {
	return nil
}
func (m *mockArtifactStoreForCache) DeleteFormat(ctx context.Context, tenantID, appName, deploymentID, format string) error {
	return nil
}

func TestNewHTTPArtifactCachePusher_HasTimeout(t *testing.T) {
	p := NewHTTPArtifactCachePusher(&mockArtifactStoreForCache{}, "token")
	httpP, ok := p.(*httpArtifactCachePusher)
	if !ok {
		t.Fatalf("expected *httpArtifactCachePusher, got %T", p)
	}
	if httpP.httpClient.Timeout == 0 {
		t.Error("httpClient timeout is not set")
	}
}

func TestCachePusher_Push_2xxReturnsNil(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Token") != "s3cr3t" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	p := &httpArtifactCachePusher{
		artifactStore: &mockArtifactStoreForCache{},
		httpClient:    ts.Client(),
		internalToken: "s3cr3t",
	}
	err := p.Push(context.Background(), ts.URL, "t_test", "myapp", "d_1")
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
}

func TestCachePusher_Push_Non2xxReturnsError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	p := &httpArtifactCachePusher{
		artifactStore: &mockArtifactStoreForCache{},
		httpClient:    ts.Client(),
		internalToken: "token",
	}
	err := p.Push(context.Background(), ts.URL, "t_test", "myapp", "d_1")
	if err == nil {
		t.Fatal("expected error for 404")
	}
}

func TestCachePusher_Push_StoreError(t *testing.T) {
	p := &httpArtifactCachePusher{
		artifactStore: &mockArtifactStoreForCache{
			openFn: func(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error) {
				return nil, io.ErrUnexpectedEOF
			},
		},
		httpClient:    http.DefaultClient,
		internalToken: "token",
	}
	err := p.Push(context.Background(), "http://cache.local", "t_test", "myapp", "d_1")
	if err == nil {
		t.Fatal("expected error from store")
	}
}

func TestCachePusher_Push_InvalidURL(t *testing.T) {
	p := &httpArtifactCachePusher{
		artifactStore: &mockArtifactStoreForCache{},
		httpClient:    http.DefaultClient,
		internalToken: "token",
	}
	err := p.Push(context.Background(), "://invalid", "t_test", "myapp", "d_1")
	if err == nil {
		t.Fatal("expected error for invalid URL")
	}
}
