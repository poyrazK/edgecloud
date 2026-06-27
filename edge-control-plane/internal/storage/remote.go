package storage

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RemoteArtifactStore is a pull-through cache: a local FS cache in
// front of a peer control-plane that serves artifacts over HTTPS
// using the `X-Internal-Token` shared-secret auth. On Open, if the
// local cache has the blob, return it; if not, fetch from the peer
// CP, write to the cache (atomic rename), then return the cached file.
//
// The local CP acts as a CDN edge node — a worker in region B hits its
// own CP first, and only on a cold cache does the request go across
// regions to the originating CP.
//
// v1 keeps Save / Delete local-only:
//   - Save writes only to the local cache (the peer CP pulls on first
//     miss). Pre-warming peers is a follow-up issue; the first request
//     after activation pays the cross-region latency cost once, then
//     every subsequent request hits the local cache.
//   - Delete removes only the local cache entry. Cross-CP GC is a
//     separate concern — a peer with stale cached blobs is harmless
//     (the worker re-verifies the artifact hash on download; see
//     edge-worker/src/downloader.rs::verify_hash).
//
// Threat model:
//   - The peer URL MUST be HTTPS (TLS protects the shared secret in
//     transit). An http:// URL is rejected at startup so a misconfigured
//     operator can't silently expose the token on the wire.
//   - The shared secret is never logged. On 4xx/5xx peer responses the
//     peer body is drained but never returned to the caller, so a peer
//     that includes diagnostic details (e.g. a stack trace containing
//     the request's headers) doesn't leak them upward.
//
// Cold-cache race (issue #127 Risk 3 mitigation): two concurrent
// pull-throughs for the same key both write to the same deterministic
// staging path (`cacheDir/.staging/{deploymentID}.tmp`). The second
// writer's `os.Create` truncates the first stream; the second
// `os.Rename` overwrites the first. The end state is consistent
// regardless of which writer wins — the file is the same content
// either way. Cheaper than singleflight.Group and survives process
// restarts.
type RemoteArtifactStore struct {
	cache     *FSArtifactStore
	peerURL   string
	peerToken string

	httpClient *http.Client
}

// NewRemoteArtifactStore validates the required peer config fields
// (URL + token + cache dir) and constructs the store. Fail-closed:
// empty peerURL, empty peerToken, or http:// peerURL returns an error
// so a misconfigured peer can't silently fall back to an
// unauthenticated GET or leak the token in transit.
func NewRemoteArtifactStore(cfg BackendConfig) (*RemoteArtifactStore, error) {
	if cfg.PeerControlPlaneURL == "" {
		return nil, fmt.Errorf("RemoteArtifactStore: PeerControlPlaneURL is required")
	}
	if cfg.PeerControlPlaneInternalToken == "" {
		return nil, fmt.Errorf("RemoteArtifactStore: PeerControlPlaneInternalToken is required")
	}
	if cfg.ArtifactPath == "" {
		return nil, fmt.Errorf("RemoteArtifactStore: ArtifactPath is required (local cache dir)")
	}
	// TLS-only peer URL. http:// would put the X-Internal-Token on
	// the wire in cleartext. Operators running the peer on localhost
	// for dev should use https://localhost with a self-signed cert
	// configured via standard CA bundle paths — not relax this.
	if !strings.HasPrefix(strings.ToLower(cfg.PeerControlPlaneURL), "https://") {
		return nil, fmt.Errorf(
			"RemoteArtifactStore: PeerControlPlaneURL must use https:// (got %q) — TLS is required to protect the shared secret",
			cfg.PeerControlPlaneURL,
		)
	}
	return &RemoteArtifactStore{
		cache:     NewFSArtifactStore(cfg.ArtifactPath),
		peerURL:   strings.TrimRight(cfg.PeerControlPlaneURL, "/"),
		peerToken: cfg.PeerControlPlaneInternalToken,
		httpClient: &http.Client{
			Timeout: 120 * time.Second, // large enough for the biggest artifact (100 MiB cap)
		},
	}, nil
}

// Save writes only to the local cache (v1). Pre-warming peers is a
// follow-up issue; for now the peer CP pulls on first miss.
func (s *RemoteArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	return s.cache.Save(ctx, tenantID, appName, deploymentID, r)
}

// Open checks the local cache and falls back to a peer CP GET.
// On cache hit, the FSArtifactStore's ReadCloser is returned directly.
// On cache miss, the artifact is fetched from the peer, streamed to a
// staging file, atomically renamed into the cache, and re-opened for
// the caller.
//
// Returns os.ErrNotExist (via fs.PathError) if BOTH the local cache and
// the peer return 404 — the existing `httperror.NotFoundCtx` path in
// the worker download handler surfaces a clean 404.
func (s *RemoteArtifactStore) Open(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error) {
	// Lane 1: local cache.
	rc, err := s.cache.Open(ctx, tenantID, appName, deploymentID)
	if err == nil {
		return rc, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("RemoteArtifactStore.Open: cache lookup: %w", err)
	}
	// Lane 2: peer pull-through.
	if err := s.pullFromPeer(ctx, tenantID, appName, deploymentID); err != nil {
		return nil, err
	}
	return s.cache.Open(ctx, tenantID, appName, deploymentID)
}

// Delete removes the local cache entry only. Cross-CP GC is a
// separate concern.
func (s *RemoteArtifactStore) Delete(ctx context.Context, tenantID, appName, deploymentID string) error {
	return s.cache.Delete(ctx, tenantID, appName, deploymentID)
}

// pullFromPeer GETs the artifact from the peer CP and writes it into
// the local cache via atomic rename. The peer response body is streamed
// to a staging file under cacheDir/.staging/{deploymentID}.tmp, fsynced,
// then renamed to the canonical path.
//
// We don't verify the artifact's hash against the deployment row at
// this layer — the worker re-verifies on download (see
// edge-worker/src/downloader.rs::verify_hash). The peer CP is trusted
// to return the same bytes as the original upload; a compromised peer
// can only DoS or return garbage, which the worker rejects.
func (s *RemoteArtifactStore) pullFromPeer(ctx context.Context, tenantID, appName, deploymentID string) error {
	peerURL := s.peerURL + "/api/internal/download/" + deploymentID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, peerURL, nil)
	if err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: building request: %w", err)
	}
	req.Header.Set("X-Internal-Token", s.peerToken)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: GET %s: %w", peerURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// Drain (limited) so the connection is reusable.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return &fs.PathError{
			Op:   "open",
			Path: deploymentID,
			Err:  os.ErrNotExist,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: GET %s: status %d", peerURL, resp.StatusCode)
	}

	// Stream the body into a staging file, then atomic-rename into
	// the cache. The staging path is per-deploymentID (not per-tenant
	// + per-app) because the rename target uniquely identifies the
	// artifact and we only need uniqueness during the single download
	// in flight. Two concurrent downloads for the same deploymentID
	// race on the staging file by design (Risk 3 — last writer wins,
	// and the result is identical bytes either way).
	stagingDir := filepath.Join(s.cache.BasePath(), ".staging")
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: mkdir staging: %w", err)
	}
	stagingPath := filepath.Join(stagingDir, deploymentID+".tmp")
	finalPath, err := s.cache.Path(tenantID, appName, deploymentID)
	if err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: invalid cache path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: mkdir cache dir: %w", err)
	}

	f, err := os.Create(stagingPath)
	if err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: create staging: %w", err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(stagingPath)
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: write staging: %w", err)
	}
	// fsync so the bytes are durable before we publish the rename.
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(stagingPath)
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: fsync staging: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(stagingPath)
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: close staging: %w", err)
	}
	if err := os.Rename(stagingPath, finalPath); err != nil {
		os.Remove(stagingPath)
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: rename to cache: %w", err)
	}
	return nil
}

// Compile-time check that *RemoteArtifactStore implements ArtifactStore.
var _ ArtifactStore = (*RemoteArtifactStore)(nil)
