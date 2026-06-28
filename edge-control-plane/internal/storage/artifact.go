package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// ArtifactStore persists tenant WASM artifacts. The interface is the
// contract between the service layer and any backend implementation;
// production code depends on this type, not on a concrete struct.
//
// Three backends implement this interface (see factory.go):
//   - FSArtifactStore (current filesystem implementation; default)
//   - S3ArtifactStore (PUT/GET/DELETE against an S3-compatible bucket)
//   - RemoteArtifactStore (pull-through cache backed by a peer CP over HTTPS)
//
// `ctx` is included on every method so S3 / HTTP backends can honor
// request timeouts. The FS backend ignores it — `os.*` does not take
// a context. This keeps every call site identical regardless of the
// backend selected at startup.
type ArtifactStore interface {
	Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error
	Open(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error)
	Delete(ctx context.Context, tenantID, appName, deploymentID string) error
}

// FSArtifactStore is the filesystem-backed implementation of
// ArtifactStore. The artifact is written to:
//
//	<basePath>/<tenantID>/<appName>/<deploymentID>.wasm
//
// All path components are validated for traversal safety before any
// filesystem operation; a malicious caller passing ".." or "/" in
// any component gets a 400-equivalent error from the storage layer.
type FSArtifactStore struct {
	basePath string
}

// BasePath returns the filesystem root the store writes to. Used by
// RemoteArtifactStore (which embeds an FSArtifactStore as its cache
// layer) to know where to put the pull-through staging directory.
// Not on the ArtifactStore interface — pure-FS detail.
func (s *FSArtifactStore) BasePath() string {
	return s.basePath
}

// NewFSArtifactStore constructs an FSArtifactStore rooted at basePath.
func NewFSArtifactStore(basePath string) *FSArtifactStore {
	return &FSArtifactStore{basePath: basePath}
}

// validatePathComponent checks that a path component doesn't contain traversal
// sequences or absolute paths. Returns an error if invalid.
func validatePathComponent(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s cannot be empty", name)
	}
	if strings.ContainsAny(value, "/\\") {
		return fmt.Errorf("%s contains invalid characters", name)
	}
	if value == ".." || strings.Contains(value, "..") {
		return fmt.Errorf("%s cannot contain '..'", name)
	}
	return nil
}

// Path returns the filesystem path for a deployment artifact.
// Returns an error if any component is invalid.
//
// Path is intentionally NOT on the ArtifactStore interface — it is a
// filesystem leak. Tests use it for assertions; production code uses
// Save/Open/Delete only.
func (s *FSArtifactStore) Path(tenantID, appName, deploymentID string) (string, error) {
	if err := validatePathComponent("tenantID", tenantID); err != nil {
		return "", err
	}
	if err := validatePathComponent("appName", appName); err != nil {
		return "", err
	}
	if err := validatePathComponent("deploymentID", deploymentID); err != nil {
		return "", err
	}

	path := filepath.Join(s.basePath, tenantID, appName, deploymentID+".wasm")
	// Verify the resolved path is still under basePath (defense-in-depth)
	clean := filepath.Clean(path)
	if !strings.HasPrefix(clean, filepath.Clean(s.basePath)) {
		return "", fmt.Errorf("path traversal detected")
	}
	return path, nil
}

// Save writes a Wasm artifact to disk. ctx is ignored — `os.Create`
// and `io.Copy` do not take a context.
func (s *FSArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	path, err := s.Path(tenantID, appName, deploymentID)
	if err != nil {
		return fmt.Errorf("invalid artifact path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("creating artifact dir: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating artifact file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("ArtifactStore.Save: failed to close file: %v", err)
		}
	}()

	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("writing artifact: %w", err)
	}
	return nil
}

// Open reads a Wasm artifact from disk. ctx is ignored — `os.Open`
// does not take a context.
//
// Defense-in-depth: a Stat pre-check rejects files larger than
// MaxArtifactSize before opening (cheaper than reading), and the
// returned ReadCloser is wrapped in a limitReadCloser so a concurrent
// writer cannot inflate the file past the cap between Stat and Open.
func (s *FSArtifactStore) Open(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error) {
	path, err := s.Path(tenantID, appName, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("invalid artifact path: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() > MaxArtifactSize {
		return nil, fmt.Errorf("%w: file is %d bytes", ErrArtifactTooLarge, info.Size())
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return newLimitReadCloser(f, MaxArtifactSize), nil
}

// Delete removes a Wasm artifact from disk. ctx is ignored —
// `os.Remove` does not take a context.
//
// Idempotent: removing a file that doesn't exist returns nil.
// AppService.Delete (internal/service/app.go) loops over the list
// of deployments and calls Delete on each — a concurrent delete
// racing the loop would otherwise surface a spurious error. The
// pre-interface code documented the same intent ("os.Remove is
// idempotent") but didn't actually swallow os.ErrNotExist; this
// fix makes the behavior match the documentation.
func (s *FSArtifactStore) Delete(ctx context.Context, tenantID, appName, deploymentID string) error {
	path, err := s.Path(tenantID, appName, deploymentID)
	if err != nil {
		return fmt.Errorf("invalid artifact path: %w", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
