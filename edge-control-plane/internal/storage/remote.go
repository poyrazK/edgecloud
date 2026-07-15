package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
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
// pull-throughs for the same key both create their own staging file
// via `os.CreateTemp` (per-process + random suffix, no truncation).
// The on-disk footprint briefly doubles while both writers stream,
// but the rename target is the canonical path — `os.Rename` is atomic
// on POSIX, so the final file is byte-identical regardless of which
// process wins. Cheaper than singleflight.Group and survives process
// restarts.
//
// Orphan janitor: on construction we sweep `.staging/` and remove
// any leftover `.tmp` files older than `stagingJanitorThreshold`.
// A live pull completes in seconds; anything older than 24 h is
// unambiguously orphaned (the prior process died holding it, or it
// pre-dates a deploy that included the janitor).
type RemoteArtifactStore struct {
	cache     *FSArtifactStore
	peerURL   string
	peerToken string

	httpClient *http.Client
}

// stagingJanitorThreshold is the minimum age for a leftover staging
// file to be considered orphaned. A live pull completes in seconds;
// 24 h is a generous bound so a legitimate in-flight pull is never
// swept out from under its writer.
const stagingJanitorThreshold = 24 * time.Hour

// NewRemoteArtifactStore validates the required peer config fields
// (URL + token + cache dir) and constructs the store. Fail-closed:
// empty peerURL, empty peerToken, or http:// peerURL returns an error
// so a misconfigured peer can't silently fall back to an
// unauthenticated GET or leak the token in transit.
//
// On success the constructor sweeps any orphaned staging files
// (see sweepStagingDir) so a long-broken deployment doesn't
// accumulate `.tmp` debris indefinitely.
func NewRemoteArtifactStore(cfg config.StorageConfig) (*RemoteArtifactStore, error) {
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
	s := &RemoteArtifactStore{
		cache:     NewFSArtifactStore(cfg.ArtifactPath),
		peerURL:   strings.TrimRight(cfg.PeerControlPlaneURL, "/"),
		peerToken: cfg.PeerControlPlaneInternalToken,
		httpClient: &http.Client{
			Timeout: 120 * time.Second, // large enough for the biggest artifact (100 MiB cap)
		},
	}
	s.sweepStagingDir()
	return s, nil
}

// sweepStagingDir removes any leftover staging files older than
// stagingJanitorThreshold. Called once from NewRemoteArtifactStore.
// Best-effort: errors reading the dir or stat'ing a file are
// silently ignored (we don't want a transient FS hiccup to fail
// startup — the next sweep or the next janitor pass will catch it).
func (s *RemoteArtifactStore) sweepStagingDir() {
	stagingDir := filepath.Join(s.cache.BasePath(), ".staging")
	entries, err := os.ReadDir(stagingDir)
	if err != nil {
		return // first run, dir doesn't exist yet
	}
	cutoff := time.Now().Add(-stagingJanitorThreshold)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(stagingDir, e.Name()))
		}
	}
}

// Save writes only to the local cache (v1). Pre-warming peers is a
// follow-up issue; for now the peer CP pulls on first miss.
func (s *RemoteArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	return s.cache.Save(ctx, tenantID, appName, deploymentID, r)
}

// SaveFormat writes only to the local cache (same as Save — peers pull on first miss).
func (s *RemoteArtifactStore) SaveFormat(ctx context.Context, tenantID, appName, deploymentID, format string, r io.Reader) error {
	return s.cache.SaveFormat(ctx, tenantID, appName, deploymentID, format, r)
}

// Open checks the local cache and falls back to a peer CP GET.
// On cache hit, the FSArtifactStore's ReadCloser is returned directly.
// On cache miss, the artifact is fetched from the peer, streamed to a
// staging file, atomically renamed into the cache, and opened for
// the caller via os.Open on the canonical path (no second
// cache.Open round-trip).
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
	// pullFromPeer just renamed the canonical file into place; open
	// it directly rather than round-tripping through cache.Open
	// (which would re-Stat the same path we already validated).
	finalPath, err := s.cache.Path(tenantID, appName, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("RemoteArtifactStore.Open: cache path: %w", err)
	}
	// Wrap in newLimitReadCloser so the read-side cap matches what
	// FSArtifactStore.Open (artifact.go:144) and S3ArtifactStore.Open
	// (s3.go:150) apply. Without this, the cache-miss post-rename path
	// returns a raw *os.File and the Download handler's io.Copy
	// (handler/internal.go:57) streams unbounded bytes into the
	// worker's response. The limitReadCloser is the live guard: it
	// stops reading at MaxArtifactSize even if a concurrent writer
	// inflates the file after we open it.
	f, err := os.Open(finalPath)
	if err != nil {
		return nil, fmt.Errorf("RemoteArtifactStore.Open: open cache: %w", err)
	}
	return newLimitReadCloser(f, MaxArtifactSize), nil
}

func (s *RemoteArtifactStore) OpenFormat(ctx context.Context, tenantID, appName, deploymentID, format string) (io.ReadCloser, error) {
	if format == "" || format == "wasm" {
		return s.Open(ctx, tenantID, appName, deploymentID)
	}
	if format != "cwasm" {
		return nil, fmt.Errorf("unsupported format %q", format)
	}

	// Lane 1: local cache lookup.
	rc, err := s.cache.OpenFormat(ctx, tenantID, appName, deploymentID, "cwasm")
	if err == nil {
		return rc, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("RemoteArtifactStore.OpenFormat: cache lookup: %w", err)
	}

	// Lane 2: peer pull-through.
	peerURL := s.peerURL + "/api/internal/download/" + deploymentID + "?format=cwasm"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, peerURL, nil)
	if err != nil {
		return nil, fmt.Errorf("RemoteArtifactStore.OpenFormat: building request: %w", err)
	}
	req.Header.Set("X-Internal-Token", s.peerToken)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("RemoteArtifactStore.OpenFormat: GET %s: %w", peerURL, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("OpenFormat pull: failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, &fs.PathError{Op: "open", Path: deploymentID + ".cwasm", Err: os.ErrNotExist}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("RemoteArtifactStore.OpenFormat: status %d", resp.StatusCode)
	}

	cwasmPath, err := s.cache.Path(tenantID, appName, deploymentID)
	if err != nil {
		return nil, err
	}
	cwasmPath = strings.TrimSuffix(cwasmPath, ".wasm") + ".cwasm"

	tmp := fmt.Sprintf("%s.tmp.%d", cwasmPath, os.Getpid())
	f, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	cleanup := func() { _ = os.Remove(tmp) }
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		cleanup()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		cleanup()
		return nil, err
	}
	if err := f.Close(); err != nil {
		cleanup()
		return nil, err
	}
	if err := os.Rename(tmp, cwasmPath); err != nil {
		cleanup()
		return nil, err
	}

	finalFile, err := os.Open(cwasmPath)
	if err != nil {
		return nil, err
	}
	return newLimitReadCloser(finalFile, MaxArtifactSize), nil
}

// Delete removes the local cache entry only. Cross-CP GC is a
// separate concern.
func (s *RemoteArtifactStore) Delete(ctx context.Context, tenantID, appName, deploymentID string) error {
	return s.cache.Delete(ctx, tenantID, appName, deploymentID)
}

// DeleteFormat removes the local cache entry for the given format only.
// The "" / "wasm" / "cwasm" forms all delegate to the underlying
// FSArtifactStore.DeleteFormat — the peer CP is not contacted, mirroring
// Delete's local-only semantics. Cross-CP GC for companion artifacts is a
// separate concern (issue #60); a peer with stale cached blobs is
// harmless because the worker re-verifies the artifact hash on
// download.
//
// Other formats return an error before any I/O.
func (s *RemoteArtifactStore) DeleteFormat(ctx context.Context, tenantID, appName, deploymentID, format string) error {
	return s.cache.DeleteFormat(ctx, tenantID, appName, deploymentID, format)
}

// SaveAndHash writes the artifact to the local cache while computing
// its SHA-256 in the same pass. Streams r through io.TeeReader to
// both the cache.Save call and a sha256 hasher — no intermediate
// buffer. Remote pull-through is the worker's job, not this method's.
func (s *RemoteArtifactStore) SaveAndHash(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) ([]byte, error) {
	hasher := sha256.New()
	tee := io.TeeReader(r, hasher)
	if err := s.cache.Save(ctx, tenantID, appName, deploymentID, tee); err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
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
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("pullFromPeer: failed to close response body: %v", err)
		}
	}()

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
	// the cache. The staging file uses os.CreateTemp so two concurrent
	// downloads for the same deploymentID no longer truncate each
	// other (Risk 3 — both streams write to separate fds; the final
	// rename target is canonical and atomic).
	stagingDir := filepath.Join(s.cache.BasePath(), ".staging")
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: mkdir staging: %w", err)
	}
	finalPath, err := s.cache.Path(tenantID, appName, deploymentID)
	if err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: invalid cache path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: mkdir cache dir: %w", err)
	}

	// os.CreateTemp gives us a per-process random suffix so we don't
	// truncate an in-flight stream from another downloader. The
	// rename below still publishes the same canonical finalPath.
	stagingFile, err := os.CreateTemp(stagingDir, deploymentID+".*.tmp")
	if err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: create staging: %w", err)
	}
	stagingPath := stagingFile.Name()
	// Best-effort cleanup on any non-success path. The successful
	// rename below removes stagingPath from stagingDir; this defer
	// catches every other exit (including panics).
	defer func() {
		if _, statErr := os.Stat(stagingPath); statErr == nil {
			_ = os.Remove(stagingPath)
		}
	}()

	// Cap the peer stream at MaxArtifactSize+1 so we can detect an oversize
	// response during the copy itself, before committing it to staging.
	// Without this, a compromised peer can return multi-GiB and the only
	// remaining bound is the 120s httpClient.Timeout + free disk space.
	//
	// io.LimitReader does NOT return an error on overflow by itself — it
	// silently stops at the cap. We use a +1 sentinel and Stat the staging
	// file after the copy: anything beyond MaxArtifactSize triggers an
	// ErrArtifactTooLarge return BEFORE the Sync/Close/Rename block, so no
	// canonical cache file is ever created for an oversize response. The
	// existing defer (above) cleans the partial staging file.
	limited := io.LimitReader(resp.Body, MaxArtifactSize+1)
	if _, err := io.Copy(stagingFile, limited); err != nil {
		if closeErr := stagingFile.Close(); closeErr != nil {
			log.Printf("pullFromPeer: failed to close staging file: %v", closeErr)
		}
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: write staging: %w", err)
	}
	info, err := stagingFile.Stat()
	if err != nil {
		if closeErr := stagingFile.Close(); closeErr != nil {
			log.Printf("pullFromPeer: failed to close staging file: %v", closeErr)
		}
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: stat staging: %w", err)
	}
	if info.Size() > MaxArtifactSize {
		if closeErr := stagingFile.Close(); closeErr != nil {
			log.Printf("pullFromPeer: failed to close staging file: %v", closeErr)
		}
		return fmt.Errorf("%w: peer response is %d bytes", ErrArtifactTooLarge, info.Size())
	}
	// fsync so the bytes are durable before we publish the rename.
	if err := stagingFile.Sync(); err != nil {
		if closeErr := stagingFile.Close(); closeErr != nil {
			log.Printf("pullFromPeer: failed to close staging file: %v", closeErr)
		}
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: fsync staging: %w", err)
	}
	if err := stagingFile.Close(); err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: close staging: %w", err)
	}
	if err := os.Rename(stagingPath, finalPath); err != nil {
		return fmt.Errorf("RemoteArtifactStore.pullFromPeer: rename to cache: %w", err)
	}
	return nil
}

// Compile-time check that *RemoteArtifactStore implements ArtifactStore.
var _ ArtifactStore = (*RemoteArtifactStore)(nil)
