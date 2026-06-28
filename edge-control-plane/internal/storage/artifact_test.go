package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFSArtifactStore_SaveOpenRoundTrip covers the happy path: Save
// writes bytes through to disk, Open returns them verbatim.
func TestFSArtifactStore_SaveOpenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)

	payload := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00} // \0asm header
	if err := s.Save(context.Background(), "t_1", "hello", "d_1", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Save: %v", err)
	}

	rc, err := s.Open(context.Background(), "t_1", "hello", "d_1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("failed to close read closer: %v", err)
		}
	}()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: got %x, want %x", got, payload)
	}
}

// TestFSArtifactStore_DeleteRemovesFile covers the simple case: Save
// then Delete, then Open must return an error.
func TestFSArtifactStore_DeleteRemovesFile(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)

	if err := s.Save(context.Background(), "t_1", "hello", "d_1", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Delete(context.Background(), "t_1", "hello", "d_1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Open(context.Background(), "t_1", "hello", "d_1"); err == nil {
		t.Fatal("Open after Delete should have failed, got nil")
	}
}

// TestFSArtifactStore_DeleteAbsentIsNotAnError documents that Delete
// is idempotent: removing an absent file returns nil. This matches
// the pre-interface behavior the AppService cleanup loop relied on
// (see internal/service/app.go:178 — "os.Remove is idempotent").
func TestFSArtifactStore_DeleteAbsentIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)
	if err := s.Delete(context.Background(), "t_1", "hello", "d_absent"); err != nil {
		t.Errorf("Delete absent should be nil, got %v", err)
	}
}

// TestFSArtifactStore_OpenAbsentReturnsError confirms an Open of a
// never-saved deployment surfaces as os.ErrNotExist — the same error
// the Download handler checks for in internal/handler/internal.go.
func TestFSArtifactStore_OpenAbsentReturnsError(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)
	_, err := s.Open(context.Background(), "t_1", "hello", "d_absent")
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open absent: want os.ErrNotExist, got %v", err)
	}
}

// TestFSArtifactStore_RejectsPathTraversal locks the security guarantee
// that the storage layer refuses to read or write outside basePath.
// Each input is a path component (tenant, app, or deployment id) that
// would, if accepted, escape the configured root.
func TestFSArtifactStore_RejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)

	cases := []struct {
		name string
		fn   func() error
	}{
		{"Save with ../tenant", func() error {
			return s.Save(context.Background(), "../etc", "hello", "d_1", bytes.NewReader([]byte("x")))
		}},
		{"Save with slash in app", func() error {
			return s.Save(context.Background(), "t_1", "sub/hello", "d_1", bytes.NewReader([]byte("x")))
		}},
		{"Save with .. in deployment", func() error {
			return s.Save(context.Background(), "t_1", "hello", "d..1", bytes.NewReader([]byte("x")))
		}},
		{"Open with .. in tenant", func() error {
			_, err := s.Open(context.Background(), "..", "hello", "d_1")
			return err
		}},
		{"Open with backslash in app", func() error {
			_, err := s.Open(context.Background(), "t_1", "hello\\..", "d_1")
			return err
		}},
		{"Delete with empty tenant", func() error {
			return s.Delete(context.Background(), "", "hello", "d_1")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			// The exact wording varies by input; we just confirm the
			// error message references either "invalid" or "traversal"
			// so a future maintainer can quickly grep for it.
			msg := err.Error()
			if !strings.Contains(msg, "invalid") && !strings.Contains(msg, "traversal") {
				t.Errorf("error %q does not look like a traversal rejection", msg)
			}
		})
	}
}

// TestFSArtifactStore_Path_BuildsUnderBasePath confirms the (test-only)
// Path helper produces a path inside basePath for valid components.
func TestFSArtifactStore_Path_BuildsUnderBasePath(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)

	got, err := s.Path("t_1", "hello", "d_1")
	if err != nil {
		t.Fatalf("Path: %v", err)
	}
	wantPrefix := filepath.Clean(dir) + string(os.PathSeparator)
	if !strings.HasPrefix(got, wantPrefix) {
		t.Errorf("Path = %q, want prefix %q", got, wantPrefix)
	}
	if filepath.Base(got) != "d_1.wasm" {
		t.Errorf("Path basename = %q, want d_1.wasm", filepath.Base(got))
	}
}

// TestFSArtifactStore_Open_RejectsOversizedFile covers the size cap
// introduced for issue #127 follow-ups: a file larger than
// MaxArtifactSize must surface ErrArtifactTooLarge at Open time so
// the download handler can map it to HTTP 413 instead of streaming
// the full file into memory.
//
// We use a sparse file (just an info.Size() > MaxArtifactSize layout)
// rather than writing a 100+ MiB buffer into the test — the cap is
// checked from Stat before any read.
func TestFSArtifactStore_Open_RejectsOversizedFile(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)

	// Build a sparse file whose reported size is one byte past the cap.
	path, err := s.Path("t_1", "hello", "d_1")
	if err != nil {
		t.Fatalf("Path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := f.Truncate(MaxArtifactSize + 1); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close file: %v", err)
	}

	_, err = s.Open(context.Background(), "t_1", "hello", "d_1")
	if !errors.Is(err, ErrArtifactTooLarge) {
		t.Errorf("Open oversized: want ErrArtifactTooLarge, got %v", err)
	}
}

// TestFSArtifactStore_Open_CapEnforcedDuringRead covers the second
// layer of the cap: a file exactly at the cap returns all bytes
// successfully, but a file whose size is reported as under cap yet
// returns more bytes than the cap on read (simulated by a hand-rolled
// reader) also surfaces ErrArtifactTooLarge.
//
// This test exercises the limitReadCloser wrapper directly because
// synthesizing a "Stat says under cap but Read overshoots" file is
// not portable across filesystems.
func TestFSArtifactStore_Open_CapEnforcedDuringRead(t *testing.T) {
	// Under cap.
	underRC := io.NopCloser(bytes.NewReader(make([]byte, 100)))
	wrapped := newLimitReadCloser(underRC, 100)
	got, err := io.ReadAll(wrapped)
	if err != nil {
		t.Fatalf("under-cap ReadAll: %v", err)
	}
	if len(got) != 100 {
		t.Errorf("under-cap: got %d bytes, want 100", len(got))
	}
	if err := wrapped.Close(); err != nil {
		t.Errorf("under-cap Close: %v", err)
	}

	// Reader claims under-cap size but returns more bytes than budget.
	overRC := io.NopCloser(bytes.NewReader(make([]byte, 500)))
	wrapped2 := newLimitReadCloser(overRC, 100)
	got2, err := io.ReadAll(wrapped2)
	if !errors.Is(err, ErrArtifactTooLarge) {
		t.Errorf("over-cap: want ErrArtifactTooLarge, got %v (got %d bytes)", err, len(got2))
	}
	if len(got2) > 100 {
		t.Errorf("over-cap: %d bytes leaked past cap", len(got2))
	}
}
