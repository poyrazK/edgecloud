package service

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
)

// artifactCachePusher pushes the activation artifact bytes to a
// per-region edge-artifact-cache binary. Issue #332 (Layer 3).
//
// Defined locally as an interface so tests can mock the cache layer
// without standing up an HTTP server. The production implementation
// is *httpArtifactCachePusher (below).
//
// Contract:
//   - Input: cache base URL (e.g. "http://cache.fra.svc:18080"), the
//     (tenant, app, deployment_id) tuple identifying the artifact.
//   - Output: nil on a 2xx response from the cache. A descriptive
//     error otherwise. The caller (publishSwap) treats non-nil as a
//     best-effort failure and continues to the NATS publish so the
//     worker can still pull from the CP's /api/internal/download/.
//   - Timeout: 3 seconds per request, controlled by the
//     httpArtifactCachePusher.httpClient.Timeout.
type artifactCachePusher interface {
	Push(ctx context.Context, cacheBaseURL, tenantID, appName, deploymentID string) error
}

// httpArtifactCachePusher is the production implementation. It PUTs
// the artifact bytes to {cacheBaseURL}/artifacts/{tenant}/{app}/{id}
// with an `X-Internal-Token` header. The internalToken is shared
// with the cache binary's INTERNAL_TOKEN env var.
type httpArtifactCachePusher struct {
	artifactStore storage.ArtifactStore
	httpClient    *http.Client
	internalToken string
}

// NewHTTPArtifactCachePusher returns a pusher backed by the
// given ArtifactStore. `internalToken` is the shared secret presented
// as `X-Internal-Token` on every PUT.
func NewHTTPArtifactCachePusher(store storage.ArtifactStore, internalToken string) artifactCachePusher {
	return &httpArtifactCachePusher{
		artifactStore: store,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		internalToken: internalToken,
	}
}

// Push reads the artifact from the store and PUTs it to the cache.
// The artifact store's Open() is streamed directly into the request
// body so we never buffer a 100 MiB artifact in RAM (matches the
// SaveAndHash streaming pattern at storage/artifact.go:171).
//
// Returns nil on a 2xx response. On non-2xx, network error, or
// context cancellation, returns a descriptive error.
func (p *httpArtifactCachePusher) Push(ctx context.Context, cacheBaseURL, tenantID, appName, deploymentID string) error {
	// Build the target URL: {baseURL}/artifacts/{tenant}/{app}/{id}.
	// We don't path.Join() on the tenant/app/id segments because the
	// cache binary does its own validation (validatePathComponent in
	// edge-control-plane/internal/storage/artifact.go:69-80, mirrored
	// in edge-artifact-cache). If the caller passes an invalid id,
	// the cache will 400; we surface that as an error here.
	base, err := url.Parse(cacheBaseURL)
	if err != nil {
		return fmt.Errorf("parsing cache base URL %q: %w", cacheBaseURL, err)
	}
	base.Path = path.Join(base.Path, "artifacts", tenantID, appName, deploymentID)

	body, err := p.artifactStore.Open(ctx, tenantID, appName, deploymentID)
	if err != nil {
		return fmt.Errorf("opening artifact from store: %w", err)
	}
	defer body.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, base.String(), body)
	if err != nil {
		return fmt.Errorf("building PUT request: %w", err)
	}
	req.Header.Set("X-Internal-Token", p.internalToken)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", base.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s returned status %d", base.String(), resp.StatusCode)
	}
	return nil
}
