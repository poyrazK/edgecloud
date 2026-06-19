package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/google/uuid"
)

// ErrEdgeMigrateFailed is returned when the edge-migrate subprocess fails.
var ErrEdgeMigrateFailed = fmt.Errorf("edge-migrate transform failed")

// ErrClangFailed is returned when the wasi-sdk clang subprocess fails.
var ErrClangFailed = fmt.Errorf("wasi-sdk clang compilation failed")

// DeploymentRepoInterface abstracts deployment creation for testing.
type DeploymentRepoInterface interface {
	Create(ctx context.Context, d *domain.Deployment) error
}

// ArtifactStoreInterface abstracts wasm artifact storage for testing.
type ArtifactStoreInterface interface {
	Save(tenantID, appName, deploymentID string, r io.Reader) error
}

// transformEnvelope mirrors edge-migrate-lib's `TransformOutput`.
// Emitted by `edge-migrate --transform --format json`. The Go control
// plane is the only consumer in this repo. The `Version` field lets the
// server reject envelopes from a binary whose wire shape this server
// doesn't know how to parse.
type transformEnvelope struct {
	Version uint32                 `json:"version"`
	Report  domain.MigrationReport `json:"report"`
	WasiC   string                 `json:"wasi_c"`
}

// MigrationService transforms POSIX C source to WASI and compiles it to wasm.
type MigrationService struct {
	deploymentRepo  DeploymentRepoInterface
	artifactStore   ArtifactStoreInterface
	edgeMigratePath string
	wasiSdkPath     string
}

// NewMigrationService creates a MigrationService.
func NewMigrationService(
	deploymentRepo DeploymentRepoInterface,
	artifactStore ArtifactStoreInterface,
	edgeMigratePath, wasiSdkPath string,
) *MigrationService {
	return &MigrationService{
		deploymentRepo:  deploymentRepo,
		artifactStore:   artifactStore,
		edgeMigratePath: edgeMigratePath,
		wasiSdkPath:     wasiSdkPath,
	}
}

// sanitizeAppName derives the app name from an uploaded filename, rejecting
// values that would either be unsafe as a path component or would silently
// collide across tenants. Mirrors the checks in storage.validatePathComponent.
func sanitizeAppName(filename string) (string, error) {
	appName := strings.TrimSuffix(filename, ".c")
	if appName == "" {
		return "", fmt.Errorf("invalid filename %q: cannot derive app name", filename)
	}
	if strings.ContainsAny(appName, "/\\") {
		return "", fmt.Errorf("invalid filename %q: contains path separator", filename)
	}
	if strings.Contains(appName, "..") {
		return "", fmt.Errorf("invalid filename %q: contains '..'", filename)
	}
	return appName, nil
}

// Migrate transforms the given C source to WASI C, compiles it to wasm,
// stores the artifact, and creates a deployment record.
func (s *MigrationService) Migrate(ctx context.Context, tenantID, filename, _language, source string) (*domain.MigrationReport, error) {
	appName, err := sanitizeAppName(filename)
	if err != nil {
		return nil, err
	}

	// Write source to a temp file for edge-migrate (reads a path, not stdin)
	tmpSrc, err := os.CreateTemp("", "migrate-*.c")
	if err != nil {
		return nil, fmt.Errorf("creating temp source file: %w", err)
	}
	tmpSrcPath := tmpSrc.Name()
	defer os.Remove(tmpSrcPath)
	if _, err := tmpSrc.WriteString(source); err != nil {
		tmpSrc.Close()
		return nil, fmt.Errorf("writing temp source: %w", err)
	}
	tmpSrc.Close()

	// Run edge-migrate --transform <path> --format json. The binary
	// emits a `transformEnvelope` with the structured report and the
	// WASI C source.
	edgeMigCmd := exec.CommandContext(ctx, s.edgeMigratePath,
		"--transform", tmpSrcPath, "--format", "json")
	var edgeMigOut bytes.Buffer
	edgeMigCmd.Stdout = &edgeMigOut
	var edgeMigErr bytes.Buffer
	edgeMigCmd.Stderr = &edgeMigErr
	if err := edgeMigCmd.Run(); err != nil {
		return &domain.MigrationReport{
			Status:     domain.MigrationStatusFailed,
			WasmStored: false,
			AppName:    appName,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: fmt.Sprintf("edge-migrate failed: %s — %s", err, edgeMigErr.String()),
			}},
		}, ErrEdgeMigrateFailed
	}

	var envelope transformEnvelope
	if err := json.Unmarshal(edgeMigOut.Bytes(), &envelope); err != nil {
		return &domain.MigrationReport{
			Status:     domain.MigrationStatusFailed,
			WasmStored: false,
			AppName:    appName,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: fmt.Sprintf("edge-migrate JSON parse failed: %v — stderr: %s", err, edgeMigErr.String()),
			}},
		}, ErrEdgeMigrateFailed
	}

	// Reject envelopes whose wire shape this server doesn't understand.
	// Adding a new optional field is NOT a version bump; renaming or
	// retyping an existing field IS.
	if envelope.Version != domain.MigrateEnvelopeVersion {
		return &domain.MigrationReport{
			Status:     domain.MigrationStatusFailed,
			WasmStored: false,
			AppName:    appName,
			Errors: []domain.ErrorInfo{{
				Line: 0,
				Message: fmt.Sprintf(
					"edge-migrate envelope version %d unsupported (server expects %d) — please upgrade",
					envelope.Version, domain.MigrateEnvelopeVersion,
				),
			}},
		}, ErrEdgeMigrateFailed
	}
	wasiC := envelope.WasiC

	// Compile WASI C → wasm via clang
	tmpWasm, err := os.CreateTemp("", "migrate-*.wasm")
	if err != nil {
		return nil, fmt.Errorf("creating temp wasm file: %w", err)
	}
	tmpWasmPath := tmpWasm.Name()
	tmpWasm.Close()
	defer os.Remove(tmpWasmPath)

	clangBin := filepath.Join(s.wasiSdkPath, "clang")
	clangCmd := exec.CommandContext(ctx, clangBin,
		"--target=wasm32-wasip2", "-nostdlib",
		"-o", tmpWasmPath, "-")
	clangCmd.Stdin = strings.NewReader(wasiC)
	var clangErr bytes.Buffer
	clangCmd.Stderr = &clangErr

	if err := clangCmd.Run(); err != nil {
		report := envelope.Report
		report.Status = domain.MigrationStatusPartial
		report.WasmStored = false
		report.AppName = appName
		report.DeploymentID = nil
		report.Errors = []domain.ErrorInfo{{
			Line:    0,
			Message: fmt.Sprintf("clang failed: %s — %s", err, clangErr.String()),
		}}
		return &report, ErrClangFailed
	}

	// Read wasm bytes
	wasmBytes, err := os.ReadFile(tmpWasmPath)
	if err != nil {
		return nil, fmt.Errorf("reading compiled wasm: %w", err)
	}

	// Reject clang output that isn't actually wasm. A misconfigured
	// wasi-sdk or a non-wasm target will produce a file that passes the
	// compiler but fails on the worker — surface that here so the
	// migration report reflects a clear failure rather than silently
	// storing a broken artifact.
	if !validateWasm(wasmBytes) {
		report := envelope.Report
		report.Status = domain.MigrationStatusFailed
		report.WasmStored = false
		report.AppName = appName
		report.DeploymentID = nil
		report.Errors = []domain.ErrorInfo{{
			Line:    0,
			Message: "compiled output is not a valid wasm binary (missing magic bytes)",
		}}
		return &report, fmt.Errorf("compiled output is not a valid wasm binary")
	}

	// Generate deployment ID and hash
	depID := "d_" + uuid.New().String()
	hash := sha256.Sum256(wasmBytes)

	// Create deployment DB record
	deployment := &domain.Deployment{
		ID:        depID,
		TenantID:  tenantID,
		AppName:   appName,
		Status:    domain.StatusMigrated,
		Hash:      hex.EncodeToString(hash[:]),
		CreatedAt: time.Now(),
	}
	if err := s.deploymentRepo.Create(ctx, deployment); err != nil {
		return nil, fmt.Errorf("creating deployment record: %w", err)
	}

	// Store wasm artifact
	if err := s.artifactStore.Save(tenantID, appName, depID, bytes.NewReader(wasmBytes)); err != nil {
		return nil, fmt.Errorf("saving wasm artifact: %w", err)
	}

	report := envelope.Report
	report.Status = domain.MigrationStatusSuccess
	report.WasmStored = true
	report.AppName = appName
	report.DeploymentID = &depID
	return &report, nil
}

// validateWasm checks whether b is a valid wasm binary (magic number check).
func validateWasm(b []byte) bool {
	return bytes.HasPrefix(b, []byte{0x00, 0x61, 0x73, 0x6d})
}
