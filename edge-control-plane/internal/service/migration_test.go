package service

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// mockDeploymentRepo implements DeploymentRepoInterface for testing.
type mockDeploymentRepo struct {
	deployments []*domain.Deployment
	createErr   error
}

func (m *mockDeploymentRepo) Create(ctx context.Context, d *domain.Deployment) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.deployments = append(m.deployments, d)
	return nil
}

// mockArtifactStore implements ArtifactStoreInterface for testing.
type mockArtifactStore struct {
	artifacts map[string][]byte // key: "tenantID/appName/depID"
}

func newMockArtifactStore() *mockArtifactStore {
	return &mockArtifactStore{artifacts: make(map[string][]byte)}
}

func (m *mockArtifactStore) Save(tenantID, appName, deploymentID string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.artifacts[tenantID+"/"+appName+"/"+deploymentID] = data
	return nil
}

// migrationSvcForTest builds a MigrationService with mock dependencies.
func migrationSvcForTest(repo *mockDeploymentRepo, store *mockArtifactStore) *MigrationService {
	return NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
}

func skipIfNoEdgeMigrate(t *testing.T) {
	if _, err := exec.LookPath("edge-migrate"); err != nil {
		t.Skip("edge-migrate not in PATH")
	}
}

func skipIfNoClang(t *testing.T) {
	if _, err := exec.LookPath(filepath.Join("/usr/local/wasi-sdk/bin", "clang")); err != nil {
		t.Skip("wasi-sdk clang not available at /usr/local/wasi-sdk/bin/clang")
	}
}

// posixHTTPSource is a simple POSIX C program with socket + bind + listen + accept.
const posixHTTPSource = `#include <stdio.h>
int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    bind(fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(fd, 128);
    int client = accept(fd, NULL, NULL);
    return 0;
}`

// emptySource has no POSIX patterns.
const emptySource = `#include <stdio.h>
int main() {
    printf("Hello, world!\n");
    return 0;
}`

func TestMigrationService_Migrate_Success(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.c", "c", posixHTTPSource)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if report.Status != domain.MigrationStatusSuccess {
		t.Errorf("expected status success, got: %s", report.Status)
	}
	if !report.WasmStored {
		t.Error("expected WasmStored=true")
	}
	if report.DeploymentID == nil || *report.DeploymentID == "" {
		t.Error("expected non-empty deployment ID")
	}
	if report.AppName != "hello" {
		t.Errorf("expected appName=hello, got: %s", report.AppName)
	}
	if len(repo.deployments) != 1 {
		t.Errorf("expected 1 deployment created, got: %d", len(repo.deployments))
	}
	if repo.deployments[0].Status != domain.StatusMigrated {
		t.Errorf("expected deployment status=migrated, got: %s", repo.deployments[0].Status)
	}
	if len(store.artifacts) != 1 {
		t.Errorf("expected 1 artifact saved, got: %d", len(store.artifacts))
	}
}

func TestMigrationService_Migrate_AppNameStripsC(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	report, err := svc.Migrate(context.Background(), "tenant-1", "my_app.c", "c", emptySource)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if report.AppName != "my_app" {
		t.Errorf("expected appName=my_app, got: %s", report.AppName)
	}
}

func TestMigrationService_Migrate_EmptySource(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.c", "c", emptySource)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if report.Status != domain.MigrationStatusSuccess {
		t.Errorf("expected status success, got: %s", report.Status)
	}
	if !report.WasmStored {
		t.Error("expected WasmStored=true")
	}
}

func TestMigrationService_Migrate_EdgeMigrateFails(t *testing.T) {
	skipIfNoEdgeMigrate(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store, "edge-migrate-that-does-not-exist", "/usr/local/wasi-sdk/bin")

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.c", "c", posixHTTPSource)
	if !errors.Is(err, ErrEdgeMigrateFailed) {
		t.Fatalf("expected ErrEdgeMigrateFailed, got: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil report")
	}
	if report.Status != domain.MigrationStatusFailed {
		t.Errorf("expected status failed, got: %s", report.Status)
	}
	if report.WasmStored {
		t.Error("expected WasmStored=false")
	}
	if len(repo.deployments) != 0 {
		t.Errorf("expected 0 deployments, got: %d", len(repo.deployments))
	}
}

func TestMigrationService_Migrate_ClangFails(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	// Source that edge-migrate will accept but clang will reject (syntax error)
	badSource := `int main() { invalid syntax here }`

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.c", "c", badSource)
	if !errors.Is(err, ErrClangFailed) {
		t.Fatalf("expected ErrClangFailed, got: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil report")
	}
	if report.Status != domain.MigrationStatusPartial {
		t.Errorf("expected status partial, got: %s", report.Status)
	}
	if report.WasmStored {
		t.Error("expected WasmStored=false")
	}
	if len(report.Errors) == 0 {
		t.Error("expected at least one error in report")
	}
}

func TestMigrationService_Migrate_DBError(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{createErr: os.ErrPermission}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	_, err := svc.Migrate(context.Background(), "tenant-1", "hello.c", "c", emptySource)
	if err == nil {
		t.Fatal("expected error when DB create fails")
	}
}

func TestMigrationService_Migrate_AppNameNoExtension(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello", "c", emptySource)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// filename without .c suffix should be used as-is
	if report.AppName != "hello" {
		t.Errorf("expected appName=hello, got: %s", report.AppName)
	}
}

func TestMigrationService_Migrate_PathTraversalFilename(t *testing.T) {
	// Service-level rejection: this fires before any subprocess, so no
	// skipIfNoEdgeMigrate / skipIfNoClang needed. Guards against future
	// refactors that try to remove the defense-in-depth check on the
	// grounds that the handler already rejects.
	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	_, err := svc.Migrate(context.Background(), "tenant-1", "../etc.c", "c", emptySource)
	if err == nil {
		t.Fatal("expected error for path-traversal filename")
	}
	if len(repo.deployments) != 0 {
		t.Errorf("expected 0 deployments created, got: %d", len(repo.deployments))
	}
	if len(store.artifacts) != 0 {
		t.Errorf("expected 0 artifacts stored, got: %d", len(store.artifacts))
	}
}

func TestMigrationService_Migrate_EmptyFilename(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	_, err := svc.Migrate(context.Background(), "tenant-1", "", "c", emptySource)
	if err == nil {
		t.Fatal("expected error for empty filename")
	}
	if len(repo.deployments) != 0 {
		t.Errorf("expected 0 deployments created, got: %d", len(repo.deployments))
	}
}

func TestMigrationService_Migrate_PopulatesPatternsDetected(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	// posixHTTPSource has socket + bind + listen + accept — all transformable
	// (Accept is "best-effort"; the rest are "auto-transformable").
	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.c", "c", posixHTTPSource)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Bug #3: PatternsDetected was always empty in API responses. With
	// --format json, the report now carries the lib's classification.
	if len(report.PatternsDetected) == 0 {
		t.Error("expected PatternsDetected to be non-empty for a source with POSIX patterns")
	}
	if len(report.PatternsTransformed) == 0 {
		t.Error("expected PatternsTransformed to be non-empty for an auto-transformable source")
	}

	// Bug #3 regression guard: the wire form must be kebab-case, matching
	// edge-migrate-lib's `Transformability::as_str`. A future drift (e.g.,
	// someone switching back to Debug-serialized CamelCase) would otherwise
	// pass a non-empty check silently — the API would just stop returning
	// patterns the control plane can interpret.
	for _, p := range report.PatternsDetected {
		if p.Line == 0 {
			t.Errorf("expected non-zero line on detected pattern: %+v", p)
		}
		switch p.Transformability {
		case domain.TransformabilityAutoTransformable,
			domain.TransformabilityBestEffort,
			domain.TransformabilityNotTransformable:
			// ok — one of the three documented kebab-case values
		default:
			t.Errorf("transformability must be one of the documented kebab-case values, got: %q (pattern: %s)", p.Transformability, p.Pattern)
		}
	}

	// posixHTTPSource has socket + bind + listen + accept. The first three
	// are auto-transformable; accept is best-effort (poll loop). Count the
	// bins to catch a regression where, say, Accept is silently dropped.
	var gotAuto, gotBest int
	for _, p := range report.PatternsDetected {
		switch p.Transformability {
		case domain.TransformabilityAutoTransformable:
			gotAuto++
		case domain.TransformabilityBestEffort:
			gotBest++
		}
	}
	if gotAuto < 3 {
		t.Errorf("expected at least 3 auto-transformable patterns (socket/bind/listen), got: %d", gotAuto)
	}
	if gotBest < 1 {
		t.Errorf("expected at least 1 best-effort pattern (accept), got: %d", gotBest)
	}

	// The struct field name is PatternsManualReview; for this source there
	// are no untransformable patterns.
	if len(report.PatternsManualReview) != 0 {
		t.Errorf("expected no manual-review patterns for an all-transformable source, got: %d", len(report.PatternsManualReview))
	}
}

func TestValidateWasm(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		valid bool
	}{
		{"valid wasm magic", []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}, true},
		{"empty", []byte{}, false},
		{"wrong magic", []byte{0x00, 0x00, 0x00, 0x00}, false},
		{"partial magic", []byte{0x00, 0x61, 0x73}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validateWasm(tt.data); got != tt.valid {
				t.Errorf("ValidateWasm() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestSanitizeAppName_StripsDotC(t *testing.T) {
	got, err := sanitizeAppName("hello.c")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "hello" {
		t.Errorf("expected hello, got: %s", got)
	}
}

func TestSanitizeAppName_NoExtension(t *testing.T) {
	got, err := sanitizeAppName("hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "hello" {
		t.Errorf("expected hello, got: %s", got)
	}
}

func TestSanitizeAppName_Empty(t *testing.T) {
	_, err := sanitizeAppName(".c")
	if err == nil {
		t.Fatal("expected error for empty derived app name")
	}
}

func TestSanitizeAppName_PathTraversal(t *testing.T) {
	_, err := sanitizeAppName("../etc.c")
	if err == nil {
		t.Fatal("expected error for path-traversal filename")
	}
}

func TestSanitizeAppName_AbsolutePath(t *testing.T) {
	_, err := sanitizeAppName("/etc/passwd.c")
	if err == nil {
		t.Fatal("expected error for absolute-path filename")
	}
}

func TestSanitizeAppName_Backslash(t *testing.T) {
	_, err := sanitizeAppName(`foo\bar.c`)
	if err == nil {
		t.Fatal("expected error for backslash filename")
	}
}

func TestSanitizeAppName_EmptyString(t *testing.T) {
	_, err := sanitizeAppName("")
	if err == nil {
		t.Fatal("expected error for empty filename")
	}
}
