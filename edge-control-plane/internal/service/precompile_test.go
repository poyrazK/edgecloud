package service

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
)

// fakeCmdRunner implements cmdRunner for testing precompileCwasm.
type fakeCmdRunner struct {
	runFn func(ctx context.Context, name string, args ...string) (string, error)
}

func (f *fakeCmdRunner) Run(ctx context.Context, name string, args ...string) (string, error) {
	if f.runFn != nil {
		return f.runFn(ctx, name, args...)
	}
	return "", nil
}

func TestPrecompileCwasm_EmptyPathSkips(t *testing.T) {
	// Should return immediately without calling store or runner.
	precompileCwasm(context.Background(), nil, "", "t_test", "myapp", "d_1", &fakeCmdRunner{})
}

func TestPrecompileCwasm_BinaryNotFound(t *testing.T) {
	precompileCwasm(context.Background(), nil, "/nonexistent/wasm2cwasm", "t_test", "myapp", "d_1", &fakeCmdRunner{})
}

func TestPrecompileCwasm_StoreOpenError(t *testing.T) {
	store := &mockArtifactStoreForCache{
		openFn: func(ctx context.Context, _, _, _ string) (rc io.ReadCloser, err error) {
			return nil, errors.New("open failed")
		},
	}
	precompileCwasm(context.Background(), store, "/usr/bin/env", "t_test", "myapp", "d_1", &fakeCmdRunner{})
}

func TestPrecompileCwasm_CommandSucceeds(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewFSArtifactStore(dir)

	// Create a valid wasm file
	wasmPath, _ := store.Path("t_test", "myapp", "d_1")
	if err := os.MkdirAll(wasmPath[:len(wasmPath)-len("/d_1.wasm")], 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	wasmBytes := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	if err := os.WriteFile(wasmPath, wasmBytes, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Use a binary that exists (env) to pass the os.Stat check.
	var ran bool
	runner := &fakeCmdRunner{
		runFn: func(ctx context.Context, name string, args ...string) (string, error) {
			ran = true
			if len(args) >= 2 {
				_ = os.WriteFile(args[1], []byte("cwasm bytes"), 0644)
			}
			return "", nil
		},
	}

	precompileCwasm(context.Background(), store, "/usr/bin/env", "t_test", "myapp", "d_1", runner)

	if !ran {
		t.Error("command was not executed")
	}

	rc, err := store.OpenFormat(context.Background(), "t_test", "myapp", "d_1", "cwasm")
	if err != nil {
		t.Fatalf("OpenFormat(.cwasm): %v", err)
	}
	defer rc.Close()
}

func TestPrecompileCwasm_CommandFails(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewFSArtifactStore(dir)

	wasmPath, _ := store.Path("t_test", "myapp", "d_1")
	if err := os.MkdirAll(wasmPath[:len(wasmPath)-len("/d_1.wasm")], 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(wasmPath, []byte{0x00, 0x61, 0x73, 0x6d}, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	runner := &fakeCmdRunner{
		runFn: func(ctx context.Context, name string, args ...string) (string, error) {
			return "segfault", errors.New("exit status 139")
		},
	}

	precompileCwasm(context.Background(), store, "/usr/bin/env", "t_test", "myapp", "d_1", runner)

	_, err := store.OpenFormat(context.Background(), "t_test", "myapp", "d_1", "cwasm")
	if err == nil {
		t.Error("expected error opening non-existent .cwasm")
	}
}
