package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// Migrate transforms the given C source to WASI C, compiles it to wasm,
// stores the artifact, and creates a deployment record.
func (s *MigrationService) Migrate(ctx context.Context, tenantID, filename, _language, source string) (*domain.MigrationReport, error) {
	// Derive app name: strip .c suffix
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

	// Run edge-migrate --transform <path>
	edgeMigCmd := exec.CommandContext(ctx, s.edgeMigratePath, "--transform", tmpSrcPath)
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
	wasiC := edgeMigOut.String()

	// Build pattern report from the WASI C output
	patternsTransformed := detectTransformedPatterns(wasiC)

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
		return &domain.MigrationReport{
			Status:              domain.MigrationStatusPartial,
			WasmStored:          false,
			AppName:             appName,
			PatternsTransformed: patternsTransformed,
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
		Status:              domain.MigrationStatusSuccess,
		WasmStored:          true,
		DeploymentID:        &depID,
		AppName:             appName,
		PatternsTransformed: patternsTransformed,
	}, nil
}

// detectTransformedPatterns scans WASI C output for known WASI function names
// and returns a list of PatternInfo describing what was transformed.
func detectTransformedPatterns(wasiC string) []domain.PatternInfo {
	transforms := []struct {
		contains string
		pattern  string
		wasi     string
	}{
		{"wasi_socket_tcp_create", "socket(AF_INET, SOCK_STREAM, 0)", "wasi_socket_tcp_create"},
		{"wasi_socket_tcp_start_bind", "bind(fd, addr, len)", "wasi_socket_tcp_start_bind"},
		{"wasi_socket_tcp_start_listen", "listen(fd, backlog)", "wasi_socket_tcp_start_listen"},
		{"wasi_socket_tcp_accept", "accept(fd, ...)", "wasi_socket_tcp_accept"},
		{"wasi_socket_tcp_start_connect", "connect(fd, addr, len)", "wasi_socket_tcp_start_connect"},
		{"wasi_output_stream_write", "send(fd, buf, len, flags)", "wasi_output_stream_write"},
		{"wasi_input_stream_read", "recv(fd, buf, len, flags)", "wasi_input_stream_read"},
		{"wasi_filesystem_open", "fopen(path, mode)", "wasi_filesystem_open"},
		{"wasi_ip_name_lookup_resolve", "gethostbyname(name)", "wasi_ip_name_lookup_resolve"},
	}

	var patterns []domain.PatternInfo
	seen := make(map[string]bool)
	for _, t := range transforms {
		if strings.Contains(wasiC, t.contains) && !seen[t.pattern] {
			seen[t.pattern] = true
			patterns = append(patterns, domain.PatternInfo{
				Line:             0,
				Pattern:          t.pattern,
				Snippet:          t.pattern,
				WasiEquivalent:   t.wasi,
				Transformability: "Auto-transformable",
			})
		}
	}
	return patterns
}

// validateWasm checks whether b is a valid wasm binary (magic number check).
func validateWasm(b []byte) bool {
	return bytes.HasPrefix(b, []byte{0x00, 0x61, 0x73, 0x6d})
}
