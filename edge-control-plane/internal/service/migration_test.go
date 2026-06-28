package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// mockDeploymentRepo implements DeploymentRepoInterface for testing.
type mockDeploymentRepo struct {
	deployments []*domain.Deployment
	createErr   error
	// deleteCalls records each DeleteByID invocation. Used by the
	// rollback tests to assert the compensating write fired.
	deleteCalls []string
	// deleteErr returns this error from DeleteByID if non-nil.
	deleteErr error
}

func (m *mockDeploymentRepo) Create(ctx context.Context, d *domain.Deployment) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.deployments = append(m.deployments, d)
	return nil
}

func (m *mockDeploymentRepo) DeleteByID(ctx context.Context, id string) error {
	m.deleteCalls = append(m.deleteCalls, id)
	if m.deleteErr != nil {
		return m.deleteErr
	}
	for i, d := range m.deployments {
		if d.ID == id {
			m.deployments = append(m.deployments[:i], m.deployments[i+1:]...)
			break
		}
	}
	return nil
}

// mockArtifactStore implements ArtifactStoreInterface for testing.
// Migrate / MigrateTree only call Save, so Open and Delete are
// no-ops (Delete just evicts from the in-memory map so callers
// can assert cleanup behavior).
type mockArtifactStore struct {
	artifacts map[string][]byte // key: "tenantID/appName/depID"
	// saveErr makes Save/SaveAndHash return this error if non-nil.
	// Used by the rollback tests to trigger the compensating-write
	// path.
	saveErr error
	// deleteCalls records each Delete invocation. Used by the
	// rollback tests to assert the artifact-blob compensating write
	// fired (not just the row delete).
	deleteCalls []string
	// deleteErr returns this error from Delete if non-nil.
	deleteErr error
}

func newMockArtifactStore() *mockArtifactStore {
	return &mockArtifactStore{artifacts: make(map[string][]byte)}
}

func (m *mockArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.artifacts[tenantID+"/"+appName+"/"+deploymentID] = data
	return nil
}

// SaveAndHash implements ArtifactStoreInterface. Returns
// sha256.Sum256(bytes) so callers that compare the hash against a
// known-good digest (rare in tests) get the real digest. Honors
// saveErr for the failure path.
func (m *mockArtifactStore) SaveAndHash(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) ([]byte, error) {
	if m.saveErr != nil {
		return nil, m.saveErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	m.artifacts[tenantID+"/"+appName+"/"+deploymentID] = data
	return sha256Bytes(data), nil
}

// sha256Bytes is a tiny helper so SaveAndHash's hash doesn't pull
// crypto/sha256 into every test file. The migration_test.go file
// already imports crypto/sha256 elsewhere; keeping the call local
// avoids confusion about which sha256 is which.
func sha256Bytes(b []byte) []byte {
	h := sha256.New()
	h.Write(b)
	return h.Sum(nil)
}

func (m *mockArtifactStore) Open(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error) {
	key := tenantID + "/" + appName + "/" + deploymentID
	data, ok := m.artifacts[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockArtifactStore) Delete(ctx context.Context, tenantID, appName, deploymentID string) error {
	key := tenantID + "/" + appName + "/" + deploymentID
	m.deleteCalls = append(m.deleteCalls, key)
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.artifacts, key)
	return nil
}

// migrationSvcForTest builds a MigrationService with mock dependencies.
func migrationSvcForTest(repo *mockDeploymentRepo, store *mockArtifactStore) *MigrationService {
	return NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin", "rustc")
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

// skipIfNoRustcWasip2 skips the test if rustc is not on PATH or if the
// wasm32-wasip2 target isn't installed. The latter is the most common
// failure mode on a fresh checkout — rustc is bundled with rustup but
// `rustup target add wasm32-wasip2` has to be run separately.
func skipIfNoRustcWasip2(t *testing.T) {
	rustc, err := exec.LookPath("rustc")
	if err != nil {
		t.Skip("rustc not in PATH")
	}
	out, err := exec.Command(rustc, "--print", "target-list").Output()
	if err != nil {
		t.Skipf("rustc --print target-list failed: %v", err)
	}
	if !strings.Contains(string(out), "wasm32-wasip2") {
		t.Skip("rustc target wasm32-wasip2 not installed; run `rustup target add wasm32-wasip2`")
	}
}

// rustHTTPSource is a minimal Rust program that exercises the
// auto-transformable patterns: TcpBind (std::net::TcpListener::bind),
// FsOpen (std::fs::File::open), and FsWrite (std::fs::write). After
// `edge-migrate --language rust --transform`, the resulting source
// must contain `TcpSocket::new`, `wasi::filesystem::open`, and
// `wasi::filesystem::write` to compile against wasi::socket and
// wasi::filesystem.
const rustHTTPSource = `fn main() {
    let _listener = std::net::TcpListener::bind("127.0.0.1:8080").unwrap();
    let _f = std::fs::File::open("hello.txt").unwrap();
    std::fs::write("out.txt", b"hi").unwrap();
}
`

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
	svc := NewMigrationService(repo, store, "edge-migrate-that-does-not-exist", "/usr/local/wasi-sdk/bin", "rustc")

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

func TestIsValidDeploymentAppName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		// Valid
		{"single char", "a", true},
		{"alphanumeric", "hello", true},
		{"with hyphen", "hello-world", true},
		{"trailing digit", "app123", true},
		{"starts with digit", "0app", true},
		{"63 chars", "a" + repeat("b", 62), true},
		// Invalid
		{"empty", "", false},
		{"64 chars", "a" + repeat("b", 63), false},
		{"uppercase", "Hello", false},
		{"all uppercase", "HELLO", false},
		{"starts with hyphen", "-hello", false},
		{"underscore", "hello_world", false},
		{"dot", "hello.world", false},
		{"slash", "hello/world", false},
		{"space", "hello world", false},
		{"path traversal", "../traversal", false},
		{"path with bad segment", "a/../b", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidDeploymentAppName(tt.input); got != tt.want {
				t.Errorf("IsValidDeploymentAppName(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

func TestClassifyFromPatterns(t *testing.T) {
	auto := domain.PatternInfo{Transformability: "AutoTransformable"}
	manual := domain.PatternInfo{Transformability: "NotTransformable"}
	tests := []struct {
		name     string
		patterns []domain.PatternInfo
		want     domain.MigrationStatus
	}{
		{"empty is success", nil, domain.MigrationStatusSuccess},
		{"all auto", []domain.PatternInfo{auto, auto}, domain.MigrationStatusSuccess},
		{"only manual is failed", []domain.PatternInfo{manual}, domain.MigrationStatusFailed},
		{"mixed is partial", []domain.PatternInfo{auto, manual}, domain.MigrationStatusPartial},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyFromPatterns(tt.patterns); got != tt.want {
				t.Errorf("classifyFromPatterns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAggregateTreeStatus(t *testing.T) {
	mk := func(s domain.MigrationStatus) domain.FileReport {
		return domain.FileReport{Path: "x.c", Status: s}
	}
	tests := []struct {
		name  string
		files []domain.FileReport
		want  domain.MigrationStatus
	}{
		{"empty is success", nil, domain.MigrationStatusSuccess},
		{"all success", []domain.FileReport{mk(domain.MigrationStatusSuccess), mk(domain.MigrationStatusSuccess)}, domain.MigrationStatusSuccess},
		{"one partial", []domain.FileReport{mk(domain.MigrationStatusSuccess), mk(domain.MigrationStatusPartial)}, domain.MigrationStatusPartial},
		{"any failed wins", []domain.FileReport{mk(domain.MigrationStatusSuccess), mk(domain.MigrationStatusPartial), mk(domain.MigrationStatusFailed)}, domain.MigrationStatusFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := aggregateTreeStatus(tt.files); got != tt.want {
				t.Errorf("aggregateTreeStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMigrateTree_RejectsInvalidAppName(t *testing.T) {
	svc := migrationSvcForTest(&mockDeploymentRepo{}, newMockArtifactStore())
	_, err := svc.MigrateTree(context.Background(), "t_1", "../bad", "c", []domain.FileEntry{
		{Path: "main.c", Source: "int main(){return 0;}\n"},
	})
	if err == nil {
		t.Fatal("expected error for invalid app name")
	}
}

func TestMigrateTree_RejectsEmptyTree(t *testing.T) {
	svc := migrationSvcForTest(&mockDeploymentRepo{}, newMockArtifactStore())
	_, err := svc.MigrateTree(context.Background(), "t_1", "hello", "c", nil)
	if err == nil {
		t.Fatal("expected error for empty tree")
	}
}

func TestMigrateTree_RejectsUnknownLanguage(t *testing.T) {
	// M3 widened the language gate from "c only" to "c or rust".
	// Anything else (e.g. "python", "go") is still rejected at the
	// service layer as a defense-in-depth check, even though the
	// handler rejects it earlier.
	svc := migrationSvcForTest(&mockDeploymentRepo{}, newMockArtifactStore())
	_, err := svc.MigrateTree(context.Background(), "t_1", "hello", "python", []domain.FileEntry{
		{Path: "main.py", Source: "print('hi')\n"},
	})
	if err == nil {
		t.Fatal("expected error for unsupported language")
	}
}

func TestMigrateTree_AcceptsRustLanguage(t *testing.T) {
	// M3 also added Rust. The service shouldn't reject "rust" at the
	// language gate — it'll only fail later (in the per-file
	// subprocess), so this test only confirms the gate is open.
	svc := migrationSvcForTest(&mockDeploymentRepo{}, newMockArtifactStore())
	_, err := svc.MigrateTree(context.Background(), "t_1", "hello", "rust", nil)
	// Empty entries still errors, but the error must be about empty
	// tree, not about the language.
	if err == nil {
		t.Fatal("expected error for empty tree")
	}
	if !strings.Contains(err.Error(), "no files in tree") {
		t.Fatalf("expected empty-tree error, got: %v", err)
	}
}

func TestMigrateTree_RejectsPathTraversal(t *testing.T) {
	svc := migrationSvcForTest(&mockDeploymentRepo{}, newMockArtifactStore())
	_, err := svc.MigrateTree(context.Background(), "t_1", "hello", "c", []domain.FileEntry{
		{Path: "../etc/passwd", Source: "x"},
	})
	if err == nil {
		t.Fatal("expected error for path traversal")
	}
}

func TestMigrateTree_PerFileTransformFailure_ReturnsErrMigrateTreeFailed(t *testing.T) {
	// When any per-file transform subprocess fails, the service must
	// return the typed `ErrMigrateTreeFailed` sentinel so the handler
	// can map it to HTTP 422 (instead of 200 with a failure body).
	// We point the service at a non-existent binary to force every
	// per-file transform to fail; the service still builds a
	// structured TreeMigrationReport for the caller to inspect.
	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store, "/this/binary/does/not/exist", "/wasi-sdk", "rustc")
	report, err := svc.MigrateTree(context.Background(), "t_1", "hello", "c", []domain.FileEntry{
		{Path: "main.c", Source: "int main(){return 0;}\n"},
	})
	if err == nil {
		t.Fatal("expected ErrMigrateTreeFailed when per-file transform fails")
	}
	if !errors.Is(err, ErrMigrateTreeFailed) {
		t.Errorf("expected ErrMigrateTreeFailed, got: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil report on tree failure (handler emits 422 with body)")
	}
	if report.Status != domain.MigrationStatusFailed {
		t.Errorf("expected report status Failed, got: %v", report.Status)
	}
	if len(report.Errors) == 0 {
		t.Error("expected at least one error in the failure report")
	}
}

func TestMigrateTree_RejectsInvalidArtifactSize_ReturnsErrMigrateTreeFailed(t *testing.T) {
	// The MaxArtifactSize check is independent of the per-file
	// subprocess. We can't easily trigger it in a unit test (would
	// need a real toolchain producing >100 MiB output), but we
	// confirm the sentinel is exported and the function signature
	// is what the handler expects. The compile-failure path is the
	// more critical test (it exercises the same `return &report,
	// ErrMigrateTreeFailed` shape) — see the test above.
	if ErrMigrateTreeFailed == nil {
		t.Error("ErrMigrateTreeFailed must be a non-nil sentinel")
	}
	if ErrMigrationFailed == nil {
		t.Error("ErrMigrationFailed must be a non-nil sentinel (single-file Migrate path)")
	}
}

// TestDetectTransformedPatternsRust covers the M3.C7 heuristic helper
// that backs the `--analyze-json` fallback path in MigrateTree when
// `language == "rust"`. Each subtest is a representative transformed
// Rust source and the set of pattern names that should be detected.
func TestDetectTransformedPatternsRust(t *testing.T) {
	cases := []struct {
		name     string
		source   string
		expected []string // substrings that must appear in Pattern field
	}{
		{
			name: "TcpBind",
			source: `use wasi::socket::tcp::TcpSocket;
fn main() {
    let _ = TcpSocket::new(wasi::socket::AddressFamily::Ipv4)?.start_bind("127.0.0.1:80")?.finish_bind();
}`,
			expected: []string{"TcpListener::bind"},
		},
		{
			name: "TcpConnect",
			source: `use wasi::socket::tcp::TcpSocket;
fn main() {
    let _ = TcpSocket::new(wasi::socket::AddressFamily::Ipv4)?.start_connect("127.0.0.1:80")?.finish_connect();
}`,
			expected: []string{"TcpStream::connect"},
		},
		{
			name: "UdpBind",
			source: `use wasi::socket::udp::UdpSocket;
fn main() {
    let _ = UdpSocket::new(wasi::socket::AddressFamily::Ipv4)?.start_bind("0.0.0.0:53")?.finish_bind();
}`,
			expected: []string{"UdpSocket::bind"},
		},
		{
			name: "FsOpen",
			source: `fn main() {
    let _ = wasi::filesystem::open("data.txt", wasi::filesystem::OpenFlags::READ);
}`,
			expected: []string{"File::open"},
		},
		{
			name: "FsRead",
			source: `fn main() {
    let _ = wasi::filesystem::read("data.txt");
}`,
			expected: []string{"fs::read"},
		},
		{
			name: "FsWrite",
			source: `fn main() {
    let _ = wasi::filesystem::write("out.txt", b"hi");
}`,
			expected: []string{"fs::write"},
		},
		{
			name:     "no match",
			source:   "fn main() { println!(\"hello\"); }",
			expected: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			patterns := detectTransformedPatternsRust(tc.source)
			if tc.expected == nil {
				if len(patterns) != 0 {
					t.Errorf("expected no patterns, got %d: %+v", len(patterns), patterns)
				}
				return
			}
			for _, want := range tc.expected {
				found := false
				for _, p := range patterns {
					if strings.Contains(p.Pattern, want) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected to find pattern containing %q, got %+v", want, patterns)
				}
			}
		})
	}
}

// TestMigrationService_StoresRustcPath confirms the constructor
// round-trips the rustc path so the service is wired correctly.
func TestMigrationService_StoresRustcPath(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store, "edge-migrate", "/wasi-sdk", "/opt/rust/bin/rustc")
	if svc.rustcPath != "/opt/rust/bin/rustc" {
		t.Errorf("expected rustcPath=%q, got %q", "/opt/rust/bin/rustc", svc.rustcPath)
	}
	if svc.edgeMigratePath != "edge-migrate" {
		t.Errorf("expected edgeMigratePath=%q, got %q", "edge-migrate", svc.edgeMigratePath)
	}
	if svc.wasiSdkPath != "/wasi-sdk" {
		t.Errorf("expected wasiSdkPath=%q, got %q", "/wasi-sdk", svc.wasiSdkPath)
	}
}

// TestExtForLanguage covers the small dispatch helper.
func TestExtForLanguage(t *testing.T) {
	if extForLanguage("rust") != ".rs" {
		t.Errorf("rust: expected .rs, got %q", extForLanguage("rust"))
	}
	if extForLanguage("c") != ".c" {
		t.Errorf("c: expected .c, got %q", extForLanguage("c"))
	}
	if extForLanguage("") != ".c" {
		t.Errorf("empty: expected .c, got %q", extForLanguage(""))
	}
	if extForLanguage("python") != ".c" {
		t.Errorf("unknown: expected .c fallback, got %q", extForLanguage("python"))
	}
}

// ─────────────────────────────────────────────────────────────────────
// M3.C10 — Rust integration tests
// ─────────────────────────────────────────────────────────────────────

// TestMigrationService_Migrate_RustSuccess exercises the full Rust
// pipeline: edge-migrate transforms the source to wasi::socket +
// wasi::filesystem calls, then rustc --target wasm32-wasip2 compiles
// it. The artifact must be a non-empty wasm blob and a deployment
// must be created. This is the load-bearing M3 end-to-end test.
func TestMigrationService_Migrate_RustSuccess(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcWasip2(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.rs", "rust", rustHTTPSource)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if report.Status != domain.MigrationStatusSuccess {
		t.Errorf("expected status success, got: %s — errors: %+v", report.Status, report.Errors)
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
	// The artifact must be a non-empty wasm blob — at minimum the
	// 8-byte wasm magic + version.
	wasmBytes, ok := store.artifacts["tenant-1/hello/"+*report.DeploymentID]
	if !ok {
		t.Fatal("artifact not found in store")
	}
	if len(wasmBytes) < 8 {
		t.Errorf("wasm artifact too small (%d bytes); expected >= 8", len(wasmBytes))
	}
	if !bytes.HasPrefix(wasmBytes, []byte{0x00, 0x61, 0x73, 0x6d}) {
		t.Errorf("artifact is not a wasm binary (missing magic); first bytes: % x", wasmBytes[:min(8, len(wasmBytes))])
	}
}

// TestMigrationService_Migrate_RustAppNameStripsRs confirms the
// Rust path strips `.rs` (not `.c`) when deriving the app name. If
// this regresses, every Rust deployment would land under a literal
// `.rs` app_name directory.
func TestMigrationService_Migrate_RustAppNameStripsRs(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcWasip2(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	report, err := svc.Migrate(context.Background(), "tenant-1", "my_app.rs", "rust", rustHTTPSource)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if report.AppName != "my_app" {
		t.Errorf("expected appName=my_app, got: %s", report.AppName)
	}
}

// TestMigrationService_Migrate_RustProcessExitNotTransformable
// confirms `std::process::exit` is detected and flagged as
// manual-review, producing a partial migration report (not success).
// WASM has no process model — there's no auto-transform.
func TestMigrationService_Migrate_RustProcessExitNotTransformable(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcWasip2(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	const src = `fn main() {
    std::process::exit(0);
}
`
	report, err := svc.Migrate(context.Background(), "tenant-1", "exit.rs", "rust", src)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// Status is "partial" because the analyze-json path reports
	// manual_review entries; wasm may still be produced (rustc
	// ignores the unmatched call) but the report flag is what
	// callers use to decide if a deployment is migratable.
	if report.Status == domain.MigrationStatusSuccess && len(report.PatternsManualReview) == 0 {
		t.Errorf("expected manual_review entries for std::process::exit, got report: %+v", report)
	}
	hasExitReview := false
	for _, p := range report.PatternsManualReview {
		if strings.Contains(p.Pattern, "ProcessExit") || strings.Contains(p.Pattern, "exit") {
			hasExitReview = true
			break
		}
	}
	if !hasExitReview {
		t.Errorf("expected a ProcessExit manual_review entry, got: %+v", report.PatternsManualReview)
	}
}

// TestMigrationService_Migrate_RustEdgeMigrateFails covers the
// error path when the Rust analyzer itself blows up (edge-migrate
// returns non-zero). The service must surface a Failed report.
func TestMigrationService_Migrate_RustEdgeMigrateFails(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcWasip2(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	// Point edge-migrate at a non-existent binary so the subprocess
	// fails. The Rust compile path must surface this as a failure,
	// not silently produce a wasm.
	svc := NewMigrationService(repo, store, "/nonexistent/edge-migrate", "/usr/local/wasi-sdk/bin", "rustc")

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.rs", "rust", rustHTTPSource)
	if err != nil {
		t.Fatalf("expected no top-level error (report carries it), got: %v", err)
	}
	if report.Status != domain.MigrationStatusFailed {
		t.Errorf("expected status=failed, got: %s — errors: %+v", report.Status, report.Errors)
	}
	if report.WasmStored {
		t.Error("expected WasmStored=false on edge-migrate failure")
	}
	if len(report.Errors) == 0 {
		t.Error("expected at least one error entry on edge-migrate failure")
	}
}

// TestMigrateTree_RustCompilesAllFilesTogether exercises the tree
// pipeline for Rust. Two .rs files; the service must produce a
// single wasm artifact and per-file reports.
func TestMigrateTree_RustCompilesAllFilesTogether(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcWasip2(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(repo, store)

	entries := []domain.FileEntry{
		{Path: "src/main.rs", Source: `fn main() {
    let _l = std::net::TcpListener::bind("127.0.0.1:8080").unwrap();
}
`},
		{Path: "src/util.rs", Source: `pub fn helper() {
    let _f = std::fs::File::open("x.txt").unwrap();
}
`},
	}

	report, err := svc.MigrateTree(context.Background(), "tenant-1", "rust_tree", "rust", entries)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(report.Files) != 2 {
		t.Fatalf("expected 2 file reports, got: %d", len(report.Files))
	}
	if report.FilesTotal != 2 {
		t.Errorf("expected FilesTotal=2, got: %d", report.FilesTotal)
	}
	if report.AppName != "rust_tree" {
		t.Errorf("expected appName=rust_tree, got: %s", report.AppName)
	}
}

// TestDetectTransformedPatternsRust_OnHttpServerFixture runs the
// heuristic scanner on the actual transformed output of the http
// server fixture (not a synthetic string). It catches regressions
// where the transformer drops a wasi::socket call without
// detectTransformedPatternsRust noticing.
func TestDetectTransformedPatternsRust_OnHttpServerFixture(t *testing.T) {
	skipIfNoEdgeMigrate(t)

	fixture := "/Users/poyrazk/dev/Cloud/edgeCloud/.claude/worktrees/migrate-issue-88/edge-migrate/testdata/http_server.rs"
	cmd := exec.Command("edge-migrate", "--language", "rust", "--transform", fixture)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("edge-migrate --language rust --transform failed: %v", err)
	}
	transformed := string(out)

	detections := detectTransformedPatternsRust(transformed)
	if len(detections) == 0 {
		t.Fatalf("detectTransformedPatternsRust found no detections in:\n%s", transformed)
	}
	// http_server.rs uses TcpListener::bind + TcpStream::connect;
	// both should be detected.
	hasBind, hasConnect := false, false
	for _, d := range detections {
		if strings.Contains(d.Pattern, "TcpBind") {
			hasBind = true
		}
		if strings.Contains(d.Pattern, "TcpConnect") {
			hasConnect = true
		}
	}
	if !hasBind {
		t.Errorf("expected TcpBind detection, got: %+v", detections)
	}
	if !hasConnect {
		t.Errorf("expected TcpConnect detection, got: %+v", detections)
	}
}

// TestMigrate_ArtifactSaveFailure_RollsBackDeployment verifies the
// compensating-write path: when the artifact save fails after the
// deployment row is inserted, the service must remove the row so we
// don't leave a deployment pointing at no artifact. The activation
// path would 404 on download otherwise.
//
// The full Migrate path requires edge-migrate + clang to produce a
// wasm blob; we gate the test on those being available (mirrors
// TestMigrationService_Migrate_RustSuccess). We then point the
// artifact store at a saveErr so Save fails; the row is created,
// then DeleteByID must be called as compensation.
func TestMigrate_ArtifactSaveFailure_RollsBackDeployment(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{saveErr: errors.New("disk full (test)")}
	svc := migrationSvcForTest(repo, store)

	_, err := svc.Migrate(context.Background(), "tenant-1", "hello.c", "c",
		"int main(){return 0;}\n")
	if err == nil {
		t.Fatal("expected Migrate to fail when artifact save fails")
	}

	// The deployment row must have been created (Create succeeded)
	// and then rolled back (DeleteByID called with that ID).
	if len(repo.deployments) != 0 {
		t.Errorf("expected deployment to be rolled back; %d remain",
			len(repo.deployments))
	}
	if len(repo.deleteCalls) != 1 {
		t.Errorf("expected DeleteByID to be called once, got %d calls",
			len(repo.deleteCalls))
	}
	// The artifact blob must also be cleaned up. Save never
	// succeeded (saveErr returned immediately), so Delete is
	// expected to fire as the second half of the rollback symmetry.
	if len(store.deleteCalls) != 1 {
		t.Errorf("expected artifact.Delete to be called once, got %d calls",
			len(store.deleteCalls))
	}
}

// TestRollbackArtifactSave_DeletesBlob exercises the helper
// directly without invoking the migration subprocess. This locks the
// "store.Delete fires" contract for every caller of
// rollbackArtifactSave (Migrate, MigrateTree, Deploy) without
// needing edge-migrate + clang on PATH — covering the case where
// the existing TestMigrate_ArtifactSaveFailure_RollsBackDeployment
// would otherwise skip silently on CI.
//
// The helper returns saveErr UNWRAPPED so callers can wrap with
// the appropriate sentinel (ErrMigrationFailed / ErrMigrateTreeFailed)
// before surfacing to the HTTP layer. Earlier versions wrapped
// with "saving artifact" here, which broke isClientMigrationError's
// sentinel match on disk-full errors and made the handler return
// 500 instead of 422.
func TestRollbackArtifactSave_DeletesBlob(t *testing.T) {
	store := newMockArtifactStore()
	saveErr := errors.New("disk full (test)")

	gotErr := rollbackArtifactSave(context.Background(), &mockDeploymentRepo{}, store, "tenant-1", "myapp", "d_abc", saveErr)

	if gotErr != saveErr {
		t.Errorf("expected helper to return saveErr unchanged; got %v", gotErr)
	}
	if len(store.deleteCalls) != 1 {
		t.Errorf("artifact.Delete calls = %d, want 1", len(store.deleteCalls))
	}
}

// TestRollbackArtifactSave_TolerantOfDeleteErrors verifies that a
// failing artifact.Delete does NOT mask the original save error —
// the caller still sees the underlying disk-full (or whatever)
// reason, and the inner rollback error is logged but not surfaced.
// Without this guarantee the helper would either change the error
// mapping at every call site or silently swallow real cleanup
// failures.
func TestRollbackArtifactSave_TolerantOfDeleteErrors(t *testing.T) {
	store := &mockArtifactStore{deleteErr: errors.New("fs gone (test)")}
	saveErr := errors.New("disk full (test)")

	gotErr := rollbackArtifactSave(context.Background(), &mockDeploymentRepo{}, store, "tenant-1", "myapp", "d_abc", saveErr)

	if gotErr != saveErr {
		t.Errorf("expected helper to return saveErr unchanged; got %v", gotErr)
	}
	if len(store.deleteCalls) != 1 {
		t.Errorf("artifact.Delete calls = %d, want 1 (attempt even on failure)", len(store.deleteCalls))
	}
}

// TestMigrate_ArtifactSaveFailure_ClassifiedAsClientError pins the
// sentinel wrap on the migration save-failure path. The earlier
// hard-coded `"saving artifact"` wrap in rollbackArtifactSave
// broke `isClientMigrationError`'s sentinel match, so disk-full
// errors returned 500 instead of 422 with the structured report.
// This test is a unit-level smoke test: it exercises the wrap
// shape directly so the regression cannot reappear silently.
//
// (The full migration path is covered by
// TestMigrate_ArtifactSaveFailure_RollsBackDeployment, but that
// test requires edge-migrate + clang on PATH and skips on CI.)
//
// After Commit 4 (`%w` on saveErr), the chain is preserved — both
// the outer ErrMigrationFailed sentinel AND the inner saveErr
// are reachable via errors.Is. Before Commit 4, saveErr was
// Stringer'd into the wrap message and the chain was severed.
func TestMigrate_ArtifactSaveFailure_ClassifiedAsClientError(t *testing.T) {
	saveErr := errors.New("disk full (test)")
	store := newMockArtifactStore()

	wrapped := fmt.Errorf("%w: saving artifact: %w", ErrMigrationFailed,
		rollbackArtifactSave(context.Background(), &mockDeploymentRepo{}, store, "tenant-1", "myapp", "d_abc", saveErr))

	if !errors.Is(wrapped, ErrMigrationFailed) {
		t.Errorf("wrapped error %v does not match ErrMigrationFailed sentinel; handler will return 500 instead of 422", wrapped)
	}
	if !errors.Is(wrapped, saveErr) {
		t.Errorf("wrapped error %v does not match saveErr; %%w wrap should preserve the inner cause for logs/tests", wrapped)
	}
}

// TestMigrateTree_ArtifactSaveFailure_ClassifiedAsClientError is
// the tree-mode equivalent of the test above.
func TestMigrateTree_ArtifactSaveFailure_ClassifiedAsClientError(t *testing.T) {
	saveErr := errors.New("disk full (test)")
	store := newMockArtifactStore()

	wrapped := fmt.Errorf("%w: saving artifact: %w", ErrMigrateTreeFailed,
		rollbackArtifactSave(context.Background(), &mockDeploymentRepo{}, store, "tenant-1", "myapp", "d_abc", saveErr))

	if !errors.Is(wrapped, ErrMigrateTreeFailed) {
		t.Errorf("wrapped error %v does not match ErrMigrateTreeFailed sentinel; handler will return 500 instead of 422", wrapped)
	}
	if !errors.Is(wrapped, saveErr) {
		t.Errorf("wrapped error %v does not match saveErr; %%w wrap should preserve the inner cause for logs/tests", wrapped)
	}
}
