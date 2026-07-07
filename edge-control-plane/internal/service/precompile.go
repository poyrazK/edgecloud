package service

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"os/exec"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
)

// PrecompileCwasm reads a previously stored .wasm artifact, compiles it
// to .cwasm using the wasm2cwasm binary, and stores the result.
//
// Best-effort: errors are logged but not returned — the worker can
// still JIT-compile the .wasm at runtime if .cwasm is unavailable.
// This is a performance optimization, not a correctness requirement.
//
// wasm2cwasmPath is the path to the wasm2cwasm binary (set via
// EDGE_WASM2CWASM_PATH env var). When empty, the precompile step
// is skipped silently (the worker will lazily compile on first load).
func PrecompileCwasm(ctx context.Context, store storage.ArtifactStore, wasm2cwasmPath, tenantID, appName, deploymentID string) {
	if wasm2cwasmPath == "" {
		return // precompilation not configured
	}

	// Sanity check: verify the binary exists before reading the artifact.
	if _, err := os.Stat(wasm2cwasmPath); err != nil {
		log.Printf("PrecompileCwasm: wasm2cwasm binary not found at %s: %v", wasm2cwasmPath, err)
		return
	}

	// Read the .wasm from the artifact store.
	rc, err := store.Open(ctx, tenantID, appName, deploymentID)
	if err != nil {
		log.Printf("PrecompileCwasm: failed to open .wasm for %s/%s/%s: %v", tenantID, appName, deploymentID, err)
		return
	}
	defer rc.Close()

	wasmBytes, err := io.ReadAll(rc)
	if err != nil {
		log.Printf("PrecompileCwasm: failed to read .wasm for %s/%s/%s: %v", tenantID, appName, deploymentID, err)
		return
	}

	// Write .wasm to a temp file for the wasm2cwasm binary.
	tmpDir, err := os.MkdirTemp("", "wasm2cwasm-*")
	if err != nil {
		log.Printf("PrecompileCwasm: failed to create temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	inputPath := tmpDir + "/input.wasm"
	outputPath := tmpDir + "/output.cwasm"

	if err := os.WriteFile(inputPath, wasmBytes, 0644); err != nil {
		log.Printf("PrecompileCwasm: failed to write temp input: %v", err)
		return
	}

	// Run wasm2cwasm binary.
	cmd := exec.CommandContext(ctx, wasm2cwasmPath, inputPath, outputPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		log.Printf("PrecompileCwasm: wasm2cwasm failed for %s/%s/%s: %v (stderr: %s)",
			tenantID, appName, deploymentID, err, stderr.String())
		return
	}

	// Read the .cwasm output and save to the artifact store.
	cwasmBytes, err := os.ReadFile(outputPath)
	if err != nil {
		log.Printf("PrecompileCwasm: failed to read .cwasm output: %v", err)
		return
	}

	if err := store.SaveFormat(ctx, tenantID, appName, deploymentID, "cwasm", bytes.NewReader(cwasmBytes)); err != nil {
		log.Printf("PrecompileCwasm: failed to save .cwasm for %s/%s/%s: %v", tenantID, appName, deploymentID, err)
		return
	}

	log.Printf("PrecompileCwasm: compiled %s/%s/%s (%d bytes -> %d bytes)",
		tenantID, appName, deploymentID, len(wasmBytes), len(cwasmBytes))
}
