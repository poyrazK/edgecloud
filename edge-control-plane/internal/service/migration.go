package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

// MaxSourceSize is the maximum allowed source code size (10 MB).
const MaxSourceSize = 10 << 20

// Migrate transforms the given C source to WASI C, compiles it to wasm,
// stores the artifact, and creates a deployment record.
func (s *MigrationService) Migrate(ctx context.Context, tenantID, filename, _language, source string) (*domain.MigrationReport, error) {
	if source == "" {
		return nil, fmt.Errorf("source code is empty")
	}
	if len(source) > MaxSourceSize {
		return nil, fmt.Errorf("source exceeds maximum size of %d bytes", MaxSourceSize)
	}

	// Derive app name: strip .c suffix; "." alone falls through to "app"
	appName := strings.TrimSuffix(filename, ".c")
	if appName == "" {
		appName = "app"
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

	// Run edge-migrate --transform --report-json <path>
	// --report-json emits a JSON MigrationReport to stderr with full pattern analysis.
	edgeMigCmd := exec.CommandContext(ctx, s.edgeMigratePath, "--transform", "--report-json", tmpSrcPath)
	var edgeMigOut bytes.Buffer
	edgeMigCmd.Stdout = &edgeMigOut
	var edgeMigErr bytes.Buffer
	edgeMigCmd.Stderr = &edgeMigErr
	if err := edgeMigCmd.Run(); err != nil {
		return &domain.MigrationReport{
			Status:    domain.MigrationStatusFailed,
			WasmStored: false,
			AppName:   appName,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: fmt.Sprintf("edge-migrate failed: %s — %s", err, edgeMigErr.String()),
			}},
		}, ErrEdgeMigrateFailed
	}
	wasiC := edgeMigOut.String()

	// Parse the JSON MigrationReport from stderr to get authoritative pattern data.
	var edgeReport domain.MigrationReport
	if err := json.Unmarshal([]byte(edgeMigErr.String()), &edgeReport); err != nil {
		// Fallback: if JSON parsing fails, log the error and return an empty report.
		// The migration still proceeds using the WASI C from stdout.
		log.Printf("warning: failed to parse migration report from edge-migrate: %v", err)
		edgeReport = domain.MigrationReport{
			Status:    domain.MigrationStatusSuccess,
			WasmStored: false,
			AppName:  appName,
		}
	}

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
		// Compilation failed — return partial report with pattern data intact.
		return &domain.MigrationReport{
			Status:               domain.MigrationStatusPartial,
			WasmStored:           false,
			AppName:              appName,
			PatternsDetected:     edgeReport.PatternsDetected,
			PatternsTransformed:  edgeReport.PatternsTransformed,
			PatternsManualReview: edgeReport.PatternsManualReview,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: fmt.Sprintf("clang failed: %s — %s", err, clangErr.String()),
			}},
		}, ErrClangFailed
	}

	// Read wasm bytes
	wasmBytes, err := os.ReadFile(tmpWasmPath)
	if err != nil {
		return nil, fmt.Errorf("reading compiled wasm: %w", err)
	}

	// Verify the compiled output is actually a valid wasm binary.
	if !validateWasm(wasmBytes) {
		return &domain.MigrationReport{
			Status:               domain.MigrationStatusFailed,
			WasmStored:           false,
			AppName:              appName,
			PatternsDetected:     edgeReport.PatternsDetected,
			PatternsTransformed:  edgeReport.PatternsTransformed,
			PatternsManualReview: edgeReport.PatternsManualReview,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: "clang produced invalid wasm binary",
			}},
		}, ErrClangFailed
	}

	// Generate deployment ID and hash
	depID := "d_" + uuid.New().String()
	hash := sha256.Sum256(wasmBytes)

	// Create deployment DB record
	deployment := &domain.Deployment{
		ID:        depID,
		TenantID:  tenantID,
		AppName:   appName,
		Status:    "migrated",
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

	return &domain.MigrationReport{
		Status:               domain.MigrationStatusSuccess,
		WasmStored:           true,
		DeploymentID:        &depID,
		AppName:              appName,
		PatternsDetected:     edgeReport.PatternsDetected,
		PatternsTransformed:  edgeReport.PatternsTransformed,
		PatternsManualReview: edgeReport.PatternsManualReview,
	}, nil
}

// Analyze runs pattern analysis on C source using edge-migrate and returns
// the MigrationReport without compiling or storing anything.
func (s *MigrationService) Analyze(ctx context.Context, tenantID, filename, source string) (*domain.MigrationReport, error) {
	if source == "" {
		return nil, fmt.Errorf("source code is empty")
	}
	if len(source) > MaxSourceSize {
		return nil, fmt.Errorf("source exceeds maximum size of %d bytes", MaxSourceSize)
	}

	appName := strings.TrimSuffix(filename, ".c")
	if appName == "" {
		appName = "app"
	}

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

	// Run edge-migrate --transform --report-json (no clang step).
	edgeMigCmd := exec.CommandContext(ctx, s.edgeMigratePath, "--transform", "--report-json", tmpSrcPath)
	var edgeMigOut bytes.Buffer
	edgeMigCmd.Stdout = &edgeMigOut
	var edgeMigErr bytes.Buffer
	edgeMigCmd.Stderr = &edgeMigErr
	if err := edgeMigCmd.Run(); err != nil {
		return &domain.MigrationReport{
			Status:    domain.MigrationStatusFailed,
			WasmStored: false,
			AppName:   appName,
			Errors: []domain.ErrorInfo{{
				Line:    0,
				Message: fmt.Sprintf("edge-migrate failed: %s — %s", err, edgeMigErr.String()),
			}},
		}, ErrEdgeMigrateFailed
	}

	var report domain.MigrationReport
	if err := json.Unmarshal([]byte(edgeMigErr.String()), &report); err != nil {
		return nil, fmt.Errorf("parsing migration report: %w", err)
	}
	return &report, nil
}

// validateWasm checks whether b is a valid wasm binary (magic number check).
func validateWasm(b []byte) bool {
	return bytes.HasPrefix(b, []byte{0x00, 0x61, 0x73, 0x6d})
}
