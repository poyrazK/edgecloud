package storage

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestRemoteArtifactStore_ColdCachePullsFromPeer pins the basic
// pull-through behavior: cache miss → GET to peer → cache populated
// → second Open hits cache without a second GET.
func TestRemoteArtifactStore_ColdCachePullsFromPeer(t *testing.T) {
	payload := []byte("\x00asmremote-cold")
	var peerHits int32
	peer := newTLSPeer(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&peerHits, 1)
		if r.Header.Get("X-Internal-Token") != "shared-secret" {
			t.Errorf("peer saw X-Internal-Token=%q, want shared-secret", r.Header.Get("X-Internal-Token"))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/api/internal/download/d_dep1" {
			t.Errorf("peer saw path=%q, want /api/internal/download/d_dep1", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/wasm")
		w.Write(payload)
	})

	cacheDir := t.TempDir()
	s := mustNewRemote(t, peer, "shared-secret", cacheDir)

	// Open #1: cold cache → peer GET → cache fill.
	rc, err := s.Open(t.Context(), "t_t", "myapp", "d_dep1")
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll #1: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("Open #1 bytes = %q, want %q", got, payload)
	}
	if atomic.LoadInt32(&peerHits) != 1 {
		t.Errorf("peerHits after Open #1 = %d, want 1", peerHits)
	}

	// Open #2: warm cache → no second peer GET.
	rc, err = s.Open(t.Context(), "t_t", "myapp", "d_dep1")
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	got, _ = io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("Open #2 bytes = %q, want %q", got, payload)
	}
	if atomic.LoadInt32(&peerHits) != 1 {
		t.Errorf("peerHits after Open #2 = %d, want 1 (warm cache should not re-fetch)", peerHits)
	}

	// The cache file should exist on disk.
	cacheFile := filepath.Join(cacheDir, "t_t", "myapp", "d_dep1.wasm")
	if _, err := os.Stat(cacheFile); err != nil {
		t.Errorf("cache file missing after pull-through: %v", err)
	}
}

// TestRemoteArtifactStore_Peer404_SurfacesAsErrNotExist pins the
// contract that a missing artifact on BOTH the local cache AND the
// peer surfaces as os.ErrNotExist (or an error that wraps it). The
// worker download handler's `os.IsNotExist` check depends on this to
// return 404 instead of 500.
func TestRemoteArtifactStore_Peer404_SurfacesAsErrNotExist(t *testing.T) {
	peer := newTLSPeer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	s := mustNewRemote(t, peer, "tok", t.TempDir())
	_, err := s.Open(t.Context(), "t_x", "a", "d_z")
	if err == nil {
		t.Fatal("Open returned nil error on peer 404")
	}
	if !os.IsNotExist(err) {
		t.Errorf("err = %v, want os.IsNotExist == true", err)
	}
}

// TestRemoteArtifactStore_Peer5xx_IsError pins the contract that
// non-2xx peer responses (5xx, 401, 403) surface as an error so a
// misconfigured peer doesn't silently produce empty artifacts.
func TestRemoteArtifactStore_Peer5xx_IsError(t *testing.T) {
	peer := newTLSPeer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	s := mustNewRemote(t, peer, "tok", t.TempDir())
	_, err := s.Open(t.Context(), "t_x", "a", "d_z")
	if err == nil {
		t.Fatal("Open returned nil error on peer 500")
	}
	if os.IsNotExist(err) {
		t.Errorf("err is os.ErrNotExist on 500; want a non-nil-isnotexist error")
	}
}

// TestRemoteArtifactStore_Peer401_PullsNothing verifies that the
// pull-through doesn't cache an error response (which would mask
// later transient 401s).
func TestRemoteArtifactStore_Peer401_PullsNothing(t *testing.T) {
	peer := newTLSPeer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	s := mustNewRemote(t, peer, "wrong-token", t.TempDir())
	_, err := s.Open(t.Context(), "t_x", "a", "d_z")
	if err == nil {
		t.Fatal("Open returned nil on 401")
	}
	// Cache should be empty.
	cacheFile := filepath.Join(s.cache.BasePath(), "t_x", "a", "d_z.wasm")
	if _, err := os.Stat(cacheFile); !os.IsNotExist(err) {
		t.Errorf("cache file should not exist after failed pull: stat err=%v", err)
	}
}

// TestRemoteArtifactStore_Save_LocalCacheOnly verifies that Save
// writes to the local cache and does NOT make any peer call. The peer
// pulls on first miss — pre-warming is a follow-up.
func TestRemoteArtifactStore_Save_LocalCacheOnly(t *testing.T) {
	var peerHits int32
	peer := newTLSPeer(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&peerHits, 1)
		w.WriteHeader(http.StatusOK)
	})
	s := mustNewRemote(t, peer, "tok", t.TempDir())
	payload := []byte("local-only-save")
	if err := s.Save(t.Context(), "t_x", "a", "d_z", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if atomic.LoadInt32(&peerHits) != 0 {
		t.Errorf("Save triggered %d peer calls; want 0", peerHits)
	}
	cacheFile := filepath.Join(s.cache.BasePath(), "t_x", "a", "d_z.wasm")
	got, err := os.ReadFile(cacheFile)
	if err != nil {
		t.Fatalf("cache file: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("cache file = %q, want %q", got, payload)
	}
}

// TestRemoteArtifactStore_Delete_LocalCacheOnly verifies Delete
// removes only the local cache entry and does not touch the peer.
func TestRemoteArtifactStore_Delete_LocalCacheOnly(t *testing.T) {
	var peerHits int32
	peer := newTLSPeer(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&peerHits, 1)
	})
	s := mustNewRemote(t, peer, "tok", t.TempDir())
	// Seed the cache via Save.
	if err := s.Save(t.Context(), "t_x", "a", "d_z", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Delete(t.Context(), "t_x", "a", "d_z"); err != nil {
		t.Errorf("Delete: %v", err)
	}
	if atomic.LoadInt32(&peerHits) != 0 {
		t.Errorf("Delete triggered %d peer calls; want 0 (cross-CP GC is out of scope)", peerHits)
	}
}

// TestNewRemoteArtifactStore_RejectsInsecureURL pins the contract that
// http:// peer URLs are rejected at startup. The X-Internal-Token would
// be sent in cleartext over an http:// connection.
func TestNewRemoteArtifactStore_RejectsInsecureURL(t *testing.T) {
	cases := []string{
		"http://peer.example.com",
		"HTTP://peer.example.com", // case-insensitive
		"ftp://peer.example.com",  // other schemes also rejected
	}
	for _, u := range cases {
		t.Run(u, func(t *testing.T) {
			_, err := NewRemoteArtifactStore(BackendConfig{
				ArtifactPath:                  t.TempDir(),
				PeerControlPlaneURL:           u,
				PeerControlPlaneInternalToken: "tok",
			})
			if err == nil {
				t.Errorf("NewRemoteArtifactStore(%q) returned nil error; want reject", u)
			}
		})
	}
}

// TestNewRemoteArtifactStore_HTTPS_ConstructsOK — sanity check that
// the https:// scheme is accepted (the httptest URL is https).
func TestNewRemoteArtifactStore_HTTPS_ConstructsOK(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer peer.Close()
	if _, err := NewRemoteArtifactStore(BackendConfig{
		ArtifactPath:                  t.TempDir(),
		PeerControlPlaneURL:           peer.URL, // https://
		PeerControlPlaneInternalToken: "tok",
	}); err != nil {
		t.Errorf("NewRemoteArtifactStore: %v", err)
	}
}

// TestNewRemoteArtifactStore_RequiresAllFields verifies the constructor
// rejects each missing required field independently.
func TestNewRemoteArtifactStore_RequiresAllFields(t *testing.T) {
	cases := []struct {
		name string
		cfg  BackendConfig
	}{
		{
			"missingURL",
			BackendConfig{ArtifactPath: "/tmp", PeerControlPlaneInternalToken: "tok"},
		},
		{
			"missingToken",
			BackendConfig{ArtifactPath: "/tmp", PeerControlPlaneURL: "https://peer"},
		},
		{
			"missingCacheDir",
			BackendConfig{PeerControlPlaneURL: "https://peer", PeerControlPlaneInternalToken: "tok"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewRemoteArtifactStore(c.cfg)
			if err == nil {
				t.Errorf("NewRemoteArtifactStore(%+v) = nil err; want validation error", c.cfg)
			}
		})
	}
}

// TestRemoteArtifactStore_KeyValidation verifies path-traversal inputs
// are rejected before any HTTP call (mirrors FSArtifactStore and
// S3ArtifactStore).
func TestRemoteArtifactStore_KeyValidation(t *testing.T) {
	peer := newTLSPeer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("HTTP call made despite invalid key: %s", r.URL.Path)
	})
	s := mustNewRemote(t, peer, "tok", t.TempDir())
	for _, tc := range []struct{ t, a, d string }{
		{"", "a", "d"},
		{"t", "a", ".."},
		{"t/1", "a", "d"},
	} {
		err := s.Save(t.Context(), tc.t, tc.a, tc.d, bytes.NewReader(nil))
		if err == nil {
			t.Errorf("Save(%q,%q,%q) returned nil; want validation error", tc.t, tc.a, tc.d)
		}
	}
}

// TestRemoteArtifactStore_Open_OnPeerErrorDoesNotCorruptCache verifies
// that a transient peer failure (e.g. connection refused) doesn't
// leave a partial cache file behind that would be served stale.
func TestRemoteArtifactStore_Open_OnPeerErrorDoesNotCorruptCache(t *testing.T) {
	// httptest server that closes the connection mid-stream
	// (simulating a flaky peer). The pull-through should NOT leave
	// a partial cache file.
	peer := newTLSPeer(t, func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("ResponseWriter does not implement Hijacker")
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Errorf("Hijack: %v", err)
			return
		}
		// Send a partial response then close.
		conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial"))
		conn.Close()
	})
	s := mustNewRemote(t, peer, "tok", t.TempDir())
	_, err := s.Open(t.Context(), "t_t", "a", "d_d")
	if err == nil {
		t.Fatal("Open returned nil on hijacked connection")
	}
	// Either no cache file or a .tmp staging file — neither should be
	// the canonical cache file with the wrong content.
	cacheFile := filepath.Join(s.cache.BasePath(), "t_t", "a", "d_d.wasm")
	if _, err := os.Stat(cacheFile); !os.IsNotExist(err) {
		t.Errorf("canonical cache file should not exist on partial pull: stat err=%v", err)
	}
}

// TestRemoteArtifactStore_TimeoutHonored verifies the constructor's
// 120s timeout is wired into the http.Client. We can't actually wait
// 120s in a test; we just confirm the timeout is non-zero.
//
// We construct the store via NewRemoteArtifactStore directly (without
// swapping in the peer's client) because the test peer's Client() has
// no timeout — that's not the production code path.
func TestRemoteArtifactStore_TimeoutHonored(t *testing.T) {
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer peer.Close()
	s, err := NewRemoteArtifactStore(BackendConfig{
		ArtifactPath:                  t.TempDir(),
		PeerControlPlaneURL:           peer.URL,
		PeerControlPlaneInternalToken: "tok",
	})
	if err != nil {
		t.Fatalf("NewRemoteArtifactStore: %v", err)
	}
	if s.httpClient.Timeout < time.Second {
		t.Errorf("httpClient.Timeout = %v, want >= 1s (artifact downloads may be large)", s.httpClient.Timeout)
	}
	// unused — keep imports stable if test is ever deleted in isolation
	_ = errors.Is
	_ = strings.Contains
}

// newTLSPeer constructs an httptest TLS server (so the peer URL is
// https:// and passes the RemoteArtifactStore constructor's TLS check)
// and registers cleanup. The returned *httptest.Server has its own
// http.Client pre-configured to trust its self-signed cert, which we
// reuse for the store.
func newTLSPeer(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	peer := httptest.NewTLSServer(http.HandlerFunc(h))
	t.Cleanup(peer.Close)
	return peer
}

// mustNewRemote constructs a RemoteArtifactStore pointed at the given
// TLS peer (must be https:// for the constructor's TLS check). The
// store's httpClient is replaced with the peer's pre-configured
// Client() so it trusts the test server's self-signed cert.
func mustNewRemote(t *testing.T, peer *httptest.Server, token, cacheDir string) *RemoteArtifactStore {
	t.Helper()
	cfg := BackendConfig{
		ArtifactPath:                  cacheDir,
		PeerControlPlaneURL:           peer.URL,
		PeerControlPlaneInternalToken: token,
	}
	s, err := NewRemoteArtifactStore(cfg)
	if err != nil {
		t.Fatalf("NewRemoteArtifactStore: %v", err)
	}
	s.httpClient = peer.Client()
	return s
}
