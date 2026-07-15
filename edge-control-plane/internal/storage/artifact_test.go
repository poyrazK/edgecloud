package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("RoundTrip: got %x, want %x", got, payload)
	}
}

// TestFSArtifactStore_Delete_RemovesFile covers the basic Delete
// path: a previously-Saved file goes away. Idempotent semantics
// (delete-on-missing) are exercised separately.
func TestFSArtifactStore_Delete_RemovesFile(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)

	payload := []byte("hello")
	if err := s.Save(context.Background(), "t_1", "hello", "d_1", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Delete(context.Background(), "t_1", "hello", "d_1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	path, _ := s.Path("t_1", "hello", "d_1")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected file gone, got Stat err = %v", err)
	}

	// Deleting again must be idempotent (return nil), not surface
	// os.ErrNotExist to the caller.
	if err := s.Delete(context.Background(), "t_1", "hello", "d_1"); err != nil {
		t.Errorf("Delete on missing file: want nil, got %v", err)
	}
}

// TestFSArtifactStore_PathValidation rejects any component that
// could escape the base path. The exact wording varies by input
// ("invalid characters" vs "contains '..'") but the error must
// reference traversal-safety keywords so a maintainer can grep it.
func TestFSArtifactStore_PathValidation(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)

	cases := []struct {
		name string
		fn   func() error
	}{
		{"Save with empty tenant", func() error {
			return s.Save(context.Background(), "", "hello", "d_1", bytes.NewReader([]byte("x")))
		}},
		{"Save with slash in tenant", func() error {
			return s.Save(context.Background(), "t/1", "hello", "d_1", bytes.NewReader([]byte("x")))
		}},
		{"Save with .. in app", func() error {
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

// TestFSArtifactStore_SaveAndHash_MatchesSha256 confirms the streaming
// hash equals what you'd get from sha256.Sum256 of the full input
// bytes. Single-pass contract: the streaming MultiWriter must produce
// the same digest as the non-streaming reference.
func TestFSArtifactStore_SaveAndHash_MatchesSha256(t *testing.T) {
	dir := t.TempDir()
	store := NewFSArtifactStore(dir)

	// 1 MiB of pseudo-random bytes. Enough to span multiple
	// io.Copy read buffers without bloating the test runtime.
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	expected := sha256.Sum256(payload)
	expectedHex := hex.EncodeToString(expected[:])

	hash, err := store.SaveAndHash(context.Background(), "t_test", "myapp", "d_stream1",
		io.NopCloser(strings.NewReader(string(payload))))
	if err != nil {
		t.Fatalf("SaveAndHash: %v", err)
	}
	if got := hex.EncodeToString(hash); got != expectedHex {
		t.Errorf("hash mismatch:\n  got:  %s\n  want: %s", got, expectedHex)
	}

	// Sanity check: the bytes at the final path are the full
	// payload. Catches an off-by-one in io.Copy.
	path, err := store.Path("t_test", "myapp", "d_stream1")
	if err != nil {
		t.Fatalf("Path: %v", err)
	}
	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(written) != len(payload) {
		t.Errorf("written len = %d, want %d", len(written), len(payload))
	}
	if !bytes.Equal(written, payload) {
		t.Error("written bytes != payload bytes")
	}
}

// TestFSArtifactStore_SaveAndHash_AtomicOnWriteError confirms a
// reader error mid-stream leaves no partial file at the final path.
// SaveAndHash shares Save's atomicity guarantee: the temp-rename
// pattern means the final path is either the full file or absent.
func TestFSArtifactStore_SaveAndHash_AtomicOnWriteError(t *testing.T) {
	dir := t.TempDir()
	store := NewFSArtifactStore(dir)

	// Reader that errors after delivering 100 bytes.
	bad := &errAfter{N: 100, Err: errors.New("simulated read failure")}

	_, err := store.SaveAndHash(context.Background(), "t_test", "myapp", "d_partial", bad)
	if err == nil {
		t.Fatal("expected error from SaveAndHash with broken reader")
	}

	path, err := store.Path("t_test", "myapp", "d_partial")
	if err != nil {
		t.Fatalf("Path: %v", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Errorf("expected final path to be absent, got os.Stat err = %v", statErr)
	}
}

// TestFSArtifactStore_SaveAndHash_EmptyInput is the edge case: a
// zero-byte artifact must succeed (the hasher returns the well-known
// SHA-256 of empty input) and produce an empty file at the final
// path.
func TestFSArtifactStore_SaveAndHash_EmptyInput(t *testing.T) {
	dir := t.TempDir()
	store := NewFSArtifactStore(dir)

	hash, err := store.SaveAndHash(context.Background(), "t_test", "myapp", "d_empty",
		strings.NewReader(""))
	if err != nil {
		t.Fatalf("SaveAndHash: %v", err)
	}
	// SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	const expectedEmptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got := hex.EncodeToString(hash); got != expectedEmptySHA256 {
		t.Errorf("empty-input hash = %s, want %s", got, expectedEmptySHA256)
	}
	path, _ := store.Path("t_test", "myapp", "d_empty")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("expected empty file, got %d bytes", info.Size())
	}
}

// TestFSArtifactStore_SaveAndHash_InvalidPathRejectsTraversal confirms
// the path validation runs before any disk I/O — a malicious
// deployment id gets a clean error, not a partial write into a
// sibling directory.
func TestFSArtifactStore_SaveAndHash_InvalidPathRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	store := NewFSArtifactStore(dir)

	_, err := store.SaveAndHash(context.Background(), "t_test", "myapp", "../escape",
		strings.NewReader("x"))
	if err == nil {
		t.Fatal("expected error for traversal in deploymentID")
	}
	// The traversal target shouldn't exist either.
	if _, statErr := os.Stat(filepath.Join(dir, "escape")); !os.IsNotExist(statErr) {
		t.Errorf("expected no escape file, got os.Stat err = %v", statErr)
	}
}

// errAfter is a small io.Reader that yields N bytes then returns
// the configured error. Used to simulate a mid-stream read failure.
type errAfter struct {
	N   int
	Err error
	off int
}

func (e *errAfter) Read(p []byte) (int, error) {
	if e.off >= e.N {
		return 0, e.Err
	}
	remaining := e.N - e.off
	n := len(p)
	if n > remaining {
		n = remaining
	}
	for i := 0; i < n; i++ {
		p[i] = byte(e.off + i)
	}
	e.off += n
	return n, nil
}

func TestFSArtifactStore_OpenFormat(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)

	// Save a .wasm artifact
	payload := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	if err := s.Save(context.Background(), "t_1", "hello", "d_1", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// 1. Open with default format (empty string)
	rc, err := s.OpenFormat(context.Background(), "t_1", "hello", "d_1", "")
	if err != nil {
		t.Fatalf("OpenFormat empty: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("expected %x, got %x", payload, got)
	}

	// 2. Open with "wasm" format
	rc, err = s.OpenFormat(context.Background(), "t_1", "hello", "d_1", "wasm")
	if err != nil {
		t.Fatalf("OpenFormat wasm: %v", err)
	}
	got, _ = io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, payload) {
		t.Errorf("expected %x, got %x", payload, got)
	}

	// 3. Open with "cwasm" format (should fail since it's missing)
	_, err = s.OpenFormat(context.Background(), "t_1", "hello", "d_1", "cwasm")
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}

	// Write a mock .cwasm file manually to test OpenFormat("cwasm")
	cwasmPath := filepath.Join(dir, "t_1", "hello", "d_1.cwasm")
	cwasmPayload := []byte("cwasm native code")
	if err := os.WriteFile(cwasmPath, cwasmPayload, 0644); err != nil {
		t.Fatalf("writing mock cwasm: %v", err)
	}

	rc, err = s.OpenFormat(context.Background(), "t_1", "hello", "d_1", "cwasm")
	if err != nil {
		t.Fatalf("OpenFormat cwasm: %v", err)
	}
	got, _ = io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, cwasmPayload) {
		t.Errorf("expected %s, got %s", cwasmPayload, got)
	}

	// 4. Open with unsupported format
	_, err = s.OpenFormat(context.Background(), "t_1", "hello", "d_1", "invalid")
	if err == nil || !strings.Contains(err.Error(), "unsupported format") {
		t.Fatalf("expected unsupported format error, got %v", err)
	}
}

// TestFSArtifactStore_DeleteFormat_RemovesCwasm covers the
// happy path of issue #60's artifact-cleanup half: a .cwasm saved
// via SaveFormat goes away on DeleteFormat(_, "cwasm"), and a
// second DeleteFormat call returns nil (idempotent) instead of
// bubbling os.ErrNotExist up to the caller.
func TestFSArtifactStore_DeleteFormat_RemovesCwasm(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)

	if err := s.SaveFormat(context.Background(), "t_1", "hello", "d_1", "cwasm", bytes.NewReader([]byte("native code"))); err != nil {
		t.Fatalf("SaveFormat: %v", err)
	}
	cwasmPath := filepath.Join(dir, "t_1", "hello", "d_1.cwasm")
	if _, err := os.Stat(cwasmPath); err != nil {
		t.Fatalf("cwasm not on disk after SaveFormat: %v", err)
	}

	if err := s.DeleteFormat(context.Background(), "t_1", "hello", "d_1", "cwasm"); err != nil {
		t.Fatalf("DeleteFormat cwasm: %v", err)
	}
	if _, err := os.Stat(cwasmPath); !os.IsNotExist(err) {
		t.Errorf("cwasm still on disk after DeleteFormat: %v", err)
	}

	// Idempotent — a concurrent or repeated delete must not surface
	// os.ErrNotExist to the caller. Mirrors FSArtifactStore.Delete's
	// contract so AppService.Delete's error aggregation doesn't drown
	// in missing-file noise.
	if err := s.DeleteFormat(context.Background(), "t_1", "hello", "d_1", "cwasm"); err != nil {
		t.Errorf("DeleteFormat on missing cwasm: want nil, got %v", err)
	}
}

// TestFSArtifactStore_DeleteFormat_EmptyAndWasmDelegateToDelete
// pins the contract that "" / "wasm" both go through the regular
// Delete path. An app with no AOT-compiled counterpart should pay the
// exact same I/O cost whether the caller passes "" or "wasm".
func TestFSArtifactStore_DeleteFormat_EmptyAndWasmDelegateToDelete(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)
	ctx := context.Background()

	// Empty format.
	if err := s.Save(ctx, "t_1", "hello", "d_1", bytes.NewReader([]byte("wasm"))); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.DeleteFormat(ctx, "t_1", "hello", "d_1", ""); err != nil {
		t.Fatalf("DeleteFormat empty: %v", err)
	}
	wasmPath, _ := s.Path("t_1", "hello", "d_1")
	if _, err := os.Stat(wasmPath); !os.IsNotExist(err) {
		t.Errorf("DeleteFormat(\"\") did not remove .wasm: %v", err)
	}

	// "wasm" format.
	if err := s.Save(ctx, "t_1", "hello", "d_1", bytes.NewReader([]byte("wasm"))); err != nil {
		t.Fatalf("Save again: %v", err)
	}
	if err := s.DeleteFormat(ctx, "t_1", "hello", "d_1", "wasm"); err != nil {
		t.Fatalf("DeleteFormat wasm: %v", err)
	}
	if _, err := os.Stat(wasmPath); !os.IsNotExist(err) {
		t.Errorf("DeleteFormat(\"wasm\") did not remove .wasm: %v", err)
	}
}

// TestFSArtifactStore_DeleteFormat_RejectsUnsupportedFormat guards
// against future format additions that forget to update the path
// validation. Any string outside { "", "wasm", "cwasm" } returns an
// error before any I/O.
func TestFSArtifactStore_DeleteFormat_RejectsUnsupportedFormat(t *testing.T) {
	dir := t.TempDir()
	s := NewFSArtifactStore(dir)
	ctx := context.Background()

	cases := []string{"js", "wasm-eval", "../etc/cwasm", "wat"}
	for _, fmt := range cases {
		t.Run(fmt, func(t *testing.T) {
			err := s.DeleteFormat(ctx, "t_1", "hello", "d_1", fmt)
			if err == nil {
				t.Fatalf("DeleteFormat(%q): expected error, got nil", fmt)
			}
			if !strings.Contains(err.Error(), "unsupported format") {
				t.Errorf("DeleteFormat(%q) err = %v, want unsupported-format message", fmt, err)
			}
		})
	}
}
