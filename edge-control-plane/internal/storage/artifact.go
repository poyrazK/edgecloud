package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
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
	// SaveAndHash streams the artifact to disk and returns its SHA-256
	// in a single io.Copy pass (no intermediate buffer). The hash and
	// the file are written concurrently via io.MultiWriter; the final
	// path either contains the full artifact (with a verified hash) or
	// doesn't exist (atomic via temp-rename). ctx is ignored — `os.*`
	// and `sha256` don't take a context. Used by Migrate/MigrateTree
	// (commit 0e08a32) and Deploy (commit 26578b2) to avoid buffering
	// a 100 MiB artifact in RAM.
	SaveAndHash(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) ([]byte, error)
	OpenFormat(ctx context.Context, tenantID, appName, deploymentID, format string) (io.ReadCloser, error)
	// SaveFormat writes an artifact in the given format (e.g. "cwasm")
	// alongside the default .wasm file. Used by the pre-compilation step
	// to store AOT-compiled components on the control plane.
	SaveFormat(ctx context.Context, tenantID, appName, deploymentID, format string, r io.Reader) error
	// DeleteFormat removes a companion artifact (e.g. a .cwasm
	// precompiled blob) at the given format. Idempotent: a missing
	// companion file is not an error. Used by AppService.Delete
	// (issue #60) to clean up the .cwasm pair alongside the .wasm
	// when an app is removed — without this, the precompiled blob
	// outlives the app on every backend.
	//
	// format accepts "" / "wasm" (delegates to Delete) or "cwasm".
	// Other values return an error without making any I/O call.
	DeleteFormat(ctx context.Context, tenantID, appName, deploymentID, format string) error
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

// Save writes a Wasm artifact to disk atomically. The write goes
// to `<path>.tmp.<pid>` first; if the copy completes the file is
// fsynced and renamed onto the final path. A crash or io.Copy
// error mid-write leaves the temp file behind; a background
// cleanup (or operator rm) can remove it. The final path either
// contains the full artifact or does not exist — never a partial
// write. `os.Rename` is atomic on POSIX filesystems; on Windows
// it can fail if the destination is open, but the deployment runs
// on Linux per the edge-worker architecture (CLAUDE.md), so this
// is not a concern. ctx is ignored — `os.*` does not take a context.
func (s *FSArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	path, err := s.Path(tenantID, appName, deploymentID)
	if err != nil {
		return fmt.Errorf("invalid artifact path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("creating artifact dir: %w", err)
	}

	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("creating artifact temp file: %w", err)
	}
	cleanup := func() { _ = os.Remove(tmp) }

	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		cleanup()
		return fmt.Errorf("writing artifact: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		cleanup()
		return fmt.Errorf("syncing artifact: %w", err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return fmt.Errorf("closing artifact: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		cleanup()
		return fmt.Errorf("renaming artifact: %w", err)
	}
	return nil
}

// SaveAndHash streams the artifact to disk and returns its SHA-256
// hash in a single pass. Bytes fan out to both the file and the
// hasher via io.MultiWriter — the caller no longer needs to buffer
// the artifact in memory just to hash it before saving.
//
// Atomicity matches Save: the write goes to `<path>.tmp.<pid>`
// first, gets fsynced, then is renamed onto the final path. A
// crash or io.Copy error mid-write leaves the temp file behind;
// the final path either contains the full artifact or doesn't
// exist — never a partial write. After rename, the hash returned
// here is the hash of the bytes at the final path.
//
// For very large artifacts (100 MiB cap) the in-RAM cost drops
// from ~3× the artifact size (handler ReadAll → service ReadAll →
// io.Copy) to one streaming pass with a 32-byte hash state.
// ctx is ignored — `os.*` and `sha256` don't take a context.
func (s *FSArtifactStore) SaveAndHash(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) ([]byte, error) {
	path, err := s.Path(tenantID, appName, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("invalid artifact path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("creating artifact dir: %w", err)
	}

	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	f, err := os.Create(tmp)
	if err != nil {
		return nil, fmt.Errorf("creating artifact temp file: %w", err)
	}
	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, hasher), r); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("writing artifact: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("syncing artifact: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("closing artifact: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("renaming artifact: %w", err)
	}
	return hasher.Sum(nil), nil
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

// OpenFormat reads a Wasm or serialized native code artifact (.cwasm) from disk.
func (s *FSArtifactStore) OpenFormat(ctx context.Context, tenantID, appName, deploymentID, format string) (io.ReadCloser, error) {
	if format == "" || format == "wasm" {
		return s.Open(ctx, tenantID, appName, deploymentID)
	}
	if format != "cwasm" {
		return nil, fmt.Errorf("unsupported format %q", format)
	}

	if err := validatePathComponent("tenantID", tenantID); err != nil {
		return nil, err
	}
	if err := validatePathComponent("appName", appName); err != nil {
		return nil, err
	}
	if err := validatePathComponent("deploymentID", deploymentID); err != nil {
		return nil, err
	}

	path := filepath.Join(s.basePath, tenantID, appName, deploymentID+".cwasm")
	clean := filepath.Clean(path)
	if !strings.HasPrefix(clean, filepath.Clean(s.basePath)) {
		return nil, fmt.Errorf("path traversal detected")
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

// SaveFormat writes a pre-compiled artifact (e.g. .cwasm) to the store
// alongside the default .wasm file. Uses the same atomic temp-rename
// pattern as Save. ctx is ignored — `os.*` does not take a context.
func (s *FSArtifactStore) SaveFormat(ctx context.Context, tenantID, appName, deploymentID, format string, r io.Reader) error {
	ext := "." + format
	if err := validatePathComponent("tenantID", tenantID); err != nil {
		return err
	}
	if err := validatePathComponent("appName", appName); err != nil {
		return err
	}
	if err := validatePathComponent("deploymentID", deploymentID); err != nil {
		return err
	}

	path := filepath.Join(s.basePath, tenantID, appName, deploymentID+ext)
	clean := filepath.Clean(path)
	if !strings.HasPrefix(clean, filepath.Clean(s.basePath)) {
		return fmt.Errorf("path traversal detected")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("creating artifact dir: %w", err)
	}

	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	cleanup := func() { _ = os.Remove(tmp) }

	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		cleanup()
		return fmt.Errorf("writing artifact: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		cleanup()
		return fmt.Errorf("syncing artifact: %w", err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return fmt.Errorf("closing artifact: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		cleanup()
		return fmt.Errorf("renaming artifact: %w", err)
	}
	return nil
}

// DeleteFormat removes a companion artifact at the given format. The
// "" and "wasm" forms delegate to Delete so the default-extension
// path is the same single code path. "cwasm" removes
// `<basePath>/<tenantID>/<appName>/<deploymentID>.cwasm`. Idempotent:
// missing files return nil, mirroring Delete's contract so
// AppService.Delete can loop over deployments without surfacing
// spurious errors on concurrent deletes. Other formats return an
// error without touching the filesystem.
//
// ctx is ignored — `os.Remove` does not take a context. See
// `Delete` for the matching rationale.
func (s *FSArtifactStore) DeleteFormat(ctx context.Context, tenantID, appName, deploymentID, format string) error {
	if format == "" || format == "wasm" {
		return s.Delete(ctx, tenantID, appName, deploymentID)
	}
	if format != "cwasm" {
		return fmt.Errorf("unsupported format %q", format)
	}

	if err := validatePathComponent("tenantID", tenantID); err != nil {
		return err
	}
	if err := validatePathComponent("appName", appName); err != nil {
		return err
	}
	if err := validatePathComponent("deploymentID", deploymentID); err != nil {
		return err
	}

	path := filepath.Join(s.basePath, tenantID, appName, deploymentID+".cwasm")
	clean := filepath.Clean(path)
	if !strings.HasPrefix(clean, filepath.Clean(s.basePath)) {
		return fmt.Errorf("path traversal detected")
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
