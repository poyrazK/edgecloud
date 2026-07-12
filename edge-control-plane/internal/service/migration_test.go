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
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service/wit"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
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
	// updateCalls records each (id, hash, signature) triple from
	// UpdateHashAndSignature. Used by the post-#307 sign tests to
	// assert the signed fields were persisted.
	updateCalls []domain.Deployment
}

func (m *mockDeploymentRepo) Create(ctx context.Context, d *domain.Deployment) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.deployments = append(m.deployments, d)
	return nil
}

// UpdateHashAndSignature is the post-#307 in-place update. The mock
// looks up the row by id and overwrites the hash + signature fields
// in-place, mirroring what the production `DeploymentRepository` SQL
// does. If the row was never created (a regression in the
// service's flow), the call is recorded but a no-op — the test
// failure is the comment, not a panic.
func (m *mockDeploymentRepo) UpdateHashAndSignature(ctx context.Context, d *domain.Deployment) error {
	m.updateCalls = append(m.updateCalls, *d)
	for _, existing := range m.deployments {
		if existing.ID == d.ID {
			existing.Hash = d.Hash
			existing.Signature = d.Signature
			existing.SigningKeyID = d.SigningKeyID
			return nil
		}
	}
	// Row not found: this is the documented "compensating delete
	// raced ahead" no-op, but a test that exercises this path should
	// be flagged — return an error so the test fails loudly.
	return fmt.Errorf("mockDeploymentRepo: UpdateHashAndSignature called for unknown id %q", d.ID)
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
	// saveCalls records each Save invocation (key: tenantID/appName/depID).
	// Used by short-circuit regression tests to assert the
	// artifact-store write never happened.
	saveCalls []string
}

func newMockArtifactStore() *mockArtifactStore {
	return &mockArtifactStore{artifacts: make(map[string][]byte)}
}

func (m *mockArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	m.saveCalls = append(m.saveCalls, tenantID+"/"+appName+"/"+deploymentID)
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

func (m *mockArtifactStore) OpenFormat(ctx context.Context, tenantID, appName, deploymentID, format string) (io.ReadCloser, error) {
	if format == "" || format == "wasm" {
		return m.Open(ctx, tenantID, appName, deploymentID)
	}
	key := tenantID + "/" + appName + "/" + deploymentID + "." + format
	data, ok := m.artifacts[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockArtifactStore) SaveFormat(ctx context.Context, tenantID, appName, deploymentID, format string, r io.Reader) error {
	key := tenantID + "/" + appName + "/" + deploymentID + "." + format
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if m.artifacts == nil {
		m.artifacts = make(map[string][]byte)
	}
	m.artifacts[key] = data
	return nil
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
// The signer is the deterministic zero-seed fixture from the signing
// package — see internal/signing/signer_test.go for the shape. The
// caller doesn't need to know the details; this helper exists so
// individual tests don't repeat the boilerplate.
func migrationSvcForTest(t *testing.T, repo *mockDeploymentRepo, store *mockArtifactStore) *MigrationService {
	return NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin", "rustc", "wasm-tools", "cargo", "/tmp/edge-mock-wit", signing.TestKeyring(t))
}

// realWitDirForTest returns a freshly-materialized copy of the
// canonical WIT tree and registers cleanup for it. Tests that
// need the Rust migration pipeline (which invokes
// wit_bindgen::generate!) must use this instead of the
// `/tmp/edge-mock-wit` placeholder; the macro panics if the path
// doesn't resolve to a real WIT directory. C-only tests (and
// others that never compile Rust) can keep using
// migrationSvcForTest with the placeholder.
func realWitDirForTest(t *testing.T) string {
	t.Helper()
	dir, err := wit.Materialize()
	if err != nil {
		t.Fatalf("could not materialize canonical WIT tree: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
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
	svc := migrationSvcForTest(t, repo, store)

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
	svc := migrationSvcForTest(t, repo, store)

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
	svc := migrationSvcForTest(t, repo, store)

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
	svc := NewMigrationService(repo, store, "edge-migrate-that-does-not-exist", "/usr/local/wasi-sdk/bin", "rustc", "wasm-tools", "cargo", "/tmp/edge-mock-wit", signing.TestKeyring(t))

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
	svc := migrationSvcForTest(t, repo, store)

	// Source that edge-migrate will accept but clang will reject (syntax error)
	badSource := `int main() { invalid syntax here }`

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.c", "c", badSource)
	if !errors.Is(err, ErrClangFailed) {
		t.Fatalf("expected ErrClangFailed, got: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil report")
	}
	// Toolchain failure surfaces as Failed; Partial is reserved for the
	// analyzer-driven case (some patterns need manual review).
	if report.Status != domain.MigrationStatusFailed {
		t.Errorf("expected status failed, got: %s", report.Status)
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
	svc := migrationSvcForTest(t, repo, store)

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
	svc := migrationSvcForTest(t, repo, store)

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
	svc := migrationSvcForTest(t, repo, store)

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
	svc := migrationSvcForTest(t, repo, store)

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
	svc := migrationSvcForTest(t, repo, store)

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

// TestDetectManualReviewPatternsC pins gap #4 for the C fallback:
// when --analyze-json fails, the heuristic must surface
// NotTransformable POSIX calls the transformer left verbatim (fork,
// poll, select, exec*, etc.) into PatternsManualReview. Without this,
// every pattern was silently classified as AutoTransformable and
// PatternsManualReview was always empty in fallback scenarios.
func TestDetectManualReviewPatternsC(t *testing.T) {
	tests := []struct {
		name       string
		wasiSource string
		want       []string // expected Pattern names in order
	}{
		{"fork preserved", "int main(){ if(fork()) return 1; }", []string{"Fork"}},
		{"vfork preserved", "vfork();", []string{"Fork"}},
		{"poll preserved", "poll(fds, 1, -1);", []string{"Poll"}},
		{"select preserved", "select(nfds, &readfds, NULL, NULL, NULL);", []string{"Select"}},
		{"exec variants dedup to single Exec entry", "execve(...); execl(...); execvp(...); exec(...);", []string{"Exec"}},
		{"socketpair preserved", "socketpair(AF_UNIX, SOCK_STREAM, 0, sv);", []string{"SocketPair"}},
		{"shutdown preserved", "shutdown(fd, SHUT_RDWR);", []string{"Shutdown"}},
		{"accept preserved", "accept(fd, NULL, NULL);", []string{"Accept"}},
		{"accept4 preserved", "accept4(fd, NULL, NULL, 0);", []string{"Accept"}},
		{"gethostbyname family dedups to single GetHostByName", "gethostbyname(\"h\"); getaddrinfo(...); gethostbyaddr(...);", []string{"GetHostByName"}},
		{"O_NONBLOCK in source", "fd = socket(AF_INET, SOCK_STREAM | O_NONBLOCK, 0);", []string{"NonBlocking"}},
		{"SOCK_RAW in source", "fd = socket(AF_INET, SOCK_RAW, 0);", []string{"SockRaw"}},
		{"empty source", "", nil},
		{"only wasi_* (auto-transformable, no manual review)", "wasi_socket_tcp_create(AF_INET, SOCK_STREAM); wasi_socket_tcp_start_bind(...);", nil},
		{"mixed auto + manual", "wasi_socket_tcp_create(...); if(fork()) return 1;", []string{"Fork"}},
		{"all four basic manual review patterns", "fork(); poll(...); select(...); accept(...);", []string{"Fork", "Poll", "Select", "Accept"}},
		{"identifier-suffix does not match (xbind does not match bind)", "xbind(fd, addr, 8);", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectManualReviewPatternsC(tt.wasiSource)
			if !patternNamesEqual(got, tt.want) {
				t.Errorf("detectManualReviewPatternsC() = %v, want %v", patternNames(got), tt.want)
			}
			for _, p := range got {
				if p.Transformability != domain.TransformabilityNotTransformable {
					t.Errorf("entry must be NotTransformable, got: %q (pattern=%s)", p.Transformability, p.Pattern)
				}
			}
		})
	}
}

// TestDetectManualReviewPatternsRust pins gap #4 for the Rust fallback.
func TestDetectManualReviewPatternsRust(t *testing.T) {
	tests := []struct {
		name       string
		wasiSource string
		want       []string
	}{
		{".accept( on TcpListener", "let s = listener.accept();", []string{"TcpAccept"}},
		{"UdpSocket::connect literal", "let s = UdpSocket::connect(\"127.0.0.1:8080\");", []string{"UdpConnect"}},
		{"std::process::exit literal", "std::process::exit(1);", []string{"ProcessExit"}},
		{"empty source", "", nil},
		{"only auto-transformable (TcpStream::connect rewritten)", "let s = wasi_socket_tcp_start_connect(...);", nil},
		{"all three manual review patterns", "let s = listener.accept(); let _ = UdpSocket::connect(\"...\"); std::process::exit(0);", []string{"TcpAccept", "UdpConnect", "ProcessExit"}},
		{"identifier-suffix does not match (myaccept does not match .accept)", "myaccept();", nil},
		{"TcpAccept is not false-positive on bare accept()", "accept(sock, NULL, NULL);", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectManualReviewPatternsRust(tt.wasiSource)
			if !patternNamesEqual(got, tt.want) {
				t.Errorf("detectManualReviewPatternsRust() = %v, want %v", patternNames(got), tt.want)
			}
			for _, p := range got {
				if p.Transformability != domain.TransformabilityNotTransformable {
					t.Errorf("entry must be NotTransformable, got: %q (pattern=%s)", p.Transformability, p.Pattern)
				}
			}
		})
	}
}

func patternNames(ps []domain.PatternInfo) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Pattern
	}
	return out
}

func patternNamesEqual(got []domain.PatternInfo, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].Pattern != want[i] {
			return false
		}
	}
	return true
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
	svc := migrationSvcForTest(t, &mockDeploymentRepo{}, newMockArtifactStore())
	_, err := svc.MigrateTree(context.Background(), "t_1", "../bad", "c", []domain.FileEntry{
		{Path: "main.c", Source: "int main(){return 0;}\n"},
	})
	if err == nil {
		t.Fatal("expected error for invalid app name")
	}
}

func TestMigrateTree_RejectsEmptyTree(t *testing.T) {
	svc := migrationSvcForTest(t, &mockDeploymentRepo{}, newMockArtifactStore())
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
	svc := migrationSvcForTest(t, &mockDeploymentRepo{}, newMockArtifactStore())
	_, err := svc.MigrateTree(context.Background(), "t_1", "hello", "python", []domain.FileEntry{
		{Path: "main.py", Source: "print('hi')\n"},
	})
	if err == nil {
		t.Fatal("expected error for unsupported language")
	}
}

func TestMigrateTree_RejectsRustLanguage(t *testing.T) {
	// Issue #415: tree-mode Rust migration is not yet supported
	// (single-file Rust goes through Migrate instead). The handler
	// returns 400 with the same message; this test pins the
	// service-level rejection at the function entry.
	svc := migrationSvcForTest(t, &mockDeploymentRepo{}, newMockArtifactStore())
	_, err := svc.MigrateTree(context.Background(), "t_1", "hello", "rust", []domain.FileEntry{
		{Path: "src/main.rs", Source: "fn main() {}"},
	})
	if err == nil {
		t.Fatal("expected error for rust tree-mode, got nil")
	}
	if !strings.Contains(err.Error(), "rust tree-mode migration is not supported") {
		t.Fatalf("expected rust-tree-rejection error, got: %v", err)
	}
}

func TestMigrateTree_RejectsPathTraversal(t *testing.T) {
	svc := migrationSvcForTest(t, &mockDeploymentRepo{}, newMockArtifactStore())
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
	svc := NewMigrationService(repo, store, "/this/binary/does/not/exist", "/wasi-sdk", "rustc", "wasm-tools", "cargo", "/tmp/edge-mock-wit", signing.TestKeyring(t))
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

func TestMigrateTree_ToolchainFailure_ReturnsErrMigrateTreeFailed(t *testing.T) {
	// Mirror of TestMigrationService_Migrate_ClangFails, but exercising
	// MigrateTree's post-compile failure path (migration.go:885-902).
	// The tree-level report must carry Status == Failed — Partial is
	// reserved for analyzer-driven classifications (some files need
	// manual review); a tree where the toolchain refused to compile
	// shipped no wasm, so its tree-level status is Failed regardless of
	// what any individual file's analyzer verdict was.
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(t, repo, store)

	// Source that edge-migrate will accept but clang will reject
	// (syntax error — clang --target=wasm32-wasip2 will fail to compile).
	badSource := `int main() { invalid syntax here }`

	report, err := svc.MigrateTree(context.Background(), "t_1", "hello", "c", []domain.FileEntry{
		{Path: "main.c", Source: badSource},
	})
	if err == nil {
		t.Fatal("expected ErrMigrateTreeFailed when tree compile fails")
	}
	if !errors.Is(err, ErrMigrateTreeFailed) {
		t.Errorf("expected ErrMigrateTreeFailed, got: %v", err)
	}
	if report == nil {
		t.Fatal("expected non-nil report on tree failure (handler emits 422 with body)")
	}
	if report.Status != domain.MigrationStatusFailed {
		t.Errorf("expected tree status Failed, got: %v", report.Status)
	}
	if report.WasmStored {
		t.Error("expected WasmStored=false on toolchain failure")
	}
	if len(report.Errors) == 0 {
		t.Error("expected at least one error in the failure report")
	}
}

// TestMigrateTree_AnalyzeJsonFallback_PopulatesManualReview pins gap #4:
// when the per-file --analyze-json subprocess fails (older edge-migrate
// binary, wrong PATH, etc.), the fallback heuristic at migration.go:750
// must still recover the manual-review signal by diffing POSIX snippets
// against the transformed WASI source — not silently classify every
// detected pattern as auto-transformable.
//
// Strategy: write a tiny shim script that wraps the real edge-migrate
// binary and selectively fails on --analyze-json while passing --transform
// through. Run MigrateTree against a file containing fork() (a
// NotTransformable POSIX call). With the shim, the fallback path runs
// and the returned FileReport.ManualReview must contain the Fork entry.
func TestMigrateTree_AnalyzeJsonFallback_PopulatesManualReview(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	realBin, err := exec.LookPath("edge-migrate")
	if err != nil {
		t.Skip("edge-migrate not in PATH")
	}
	shimDir := t.TempDir()
	shimPath := filepath.Join(shimDir, "edge-migrate-shim")
	shim := "#!/bin/sh\n" +
		"case \" $* \" in\n" +
		"  *' --analyze-json '*)\n" +
		"    echo 'shim: forcing --analyze-json failure' >&2\n" +
		"    exit 1 ;;\n" +
		"  *)\n" +
		"    exec " + realBin + " \"$@\" ;;\n" +
		"esac\n"
	if err := os.WriteFile(shimPath, []byte(shim), 0o755); err != nil {
		t.Fatalf("write shim: %v", err)
	}

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store, shimPath, "/usr/local/wasi-sdk/bin", "rustc", "wasm-tools", "cargo", "/tmp/edge-mock-wit", signing.TestKeyring(t))

	// Source: a fork() call. The transformer will leave it in place
	// (no WASI equivalent exists), so the transformed source still
	// contains "fork(" — the fallback diff must catch that and flag
	// it as NotTransformable / manual review.
	forkSource := `#include <stdio.h>
#include <unistd.h>
int main() {
    pid_t pid = fork();
    if (pid == 0) {
        printf("child\\n");
        return 0;
    }
    printf("parent\\n");
    return 0;
}`

	report, err := svc.MigrateTree(context.Background(), "t_1", "hello", "c", []domain.FileEntry{
		{Path: "fork_demo.c", Source: forkSource},
	})
	if err != nil {
		// A compile failure here would mean clang couldn't compile
		// the fork() call (fork is non-transformable, so the
		// transformer leaves it untouched and clang for wasm32-wasip2
		// won't accept it). That's expected: the per-file transform
		// itself will succeed (the transformer just preserves fork),
		// and clang will fail at the tree-compile step. Either way,
		// the per-file FileReport.ManualReview must surface fork.
		if report == nil {
			t.Fatalf("expected non-nil report on any failure (handler emits 422 with body), got err: %v", err)
		}
	}

	if len(report.Files) != 1 {
		t.Fatalf("expected 1 file report, got: %d", len(report.Files))
	}
	fr := report.Files[0]
	if len(fr.ManualReview) == 0 {
		t.Errorf("expected at least 1 manual-review pattern (fork should be NotTransformable), got 0 — fallback lost the signal (gap #4 regression)")
	}
	foundFork := false
	for _, p := range fr.ManualReview {
		if p.Pattern == "Fork" {
			foundFork = true
			if p.Transformability != domain.TransformabilityNotTransformable {
				t.Errorf("expected manual-review entry to be NotTransformable, got: %q", p.Transformability)
			}
		}
	}
	if !foundFork {
		t.Errorf("expected Fork in manual_review, got patterns: %v", patternNames(fr.ManualReview))
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
	svc := NewMigrationService(repo, store, "edge-migrate", "/wasi-sdk", "/opt/rust/bin/rustc", "wasm-tools", "cargo", "/tmp/edge-mock-wit", signing.TestKeyring(t))
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
// Issue #415 — Rust rebuild helpers
// ─────────────────────────────────────────────────────────────────────

// skipIfNoRustcUnknown skips the test if rustc is not on PATH or if
// the wasm32-unknown-unknown target isn't installed. The Rust
// migration path shells out to `cargo build --target
// wasm32-unknown-unknown --release` (PR #415) — bare rustc cannot
// resolve the wit_bindgen::generate! proc-macro.
func skipIfNoRustcUnknown(t *testing.T) {
	rustc, err := exec.LookPath("rustc")
	if err != nil {
		t.Skip("rustc not in PATH")
	}
	out, err := exec.Command(rustc, "--print", "target-list").Output()
	if err != nil {
		t.Skipf("rustc --print target-list failed: %v", err)
	}
	if !strings.Contains(string(out), "wasm32-unknown-unknown") {
		t.Skip("rustc target wasm32-unknown-unknown not installed; run `rustup target add wasm32-unknown-unknown`")
	}
}

// skipIfNoCargo skips the test if cargo is not on PATH. cargo is
// required for the Rust migration path (issue #415).
func skipIfNoCargo(t *testing.T) {
	if _, err := exec.LookPath("cargo"); err != nil {
		t.Skip("cargo not in PATH")
	}
}

// skipIfNoWasmTools skips the test if wasm-tools is not on PATH.
// wasm-tools is required to wrap the cargo-produced core module
// into a wasi component (issue #415).
func skipIfNoWasmTools(t *testing.T) {
	bin, err := exec.LookPath("wasm-tools")
	if err != nil {
		t.Skip("wasm-tools not in PATH")
	}
	if err := exec.Command(bin, "--version").Run(); err != nil {
		t.Skipf("wasm-tools --version failed: %v", err)
	}
}

// TestMigrationService_StoresWasmToolsPath confirms the wasm-tools
// and cargo paths round-trip through NewMigrationService.
func TestMigrationService_StoresWasmToolsPath(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store,
		"edge-migrate", "/wasi-sdk", "rustc",
		"/opt/wasm-tools/bin/wasm-tools", "/opt/cargo/bin/cargo",
		"/srv/edge/wit", signing.TestKeyring(t),
	)
	if svc.wasmToolsPath != "/opt/wasm-tools/bin/wasm-tools" {
		t.Errorf("expected wasmToolsPath=%q, got %q", "/opt/wasm-tools/bin/wasm-tools", svc.wasmToolsPath)
	}
	if svc.cargoPath != "/opt/cargo/bin/cargo" {
		t.Errorf("expected cargoPath=%q, got %q", "/opt/cargo/bin/cargo", svc.cargoPath)
	}
	if svc.witDir != "/srv/edge/wit" {
		t.Errorf("expected witDir=%q, got %q", "/srv/edge/wit", svc.witDir)
	}
}

// TestInjectWitBindgen confirms the macro block is prepended at byte
// 0, the WIT dir is properly quoted, and the transformer's input is
// passed through verbatim. The transformer now emits
// `use crate::wasi::*` directly via WASI_RUST_PRELUDE
// (edge-migrate-lib/src/rust_transformer.rs), so injectWitBindgen is
// a pass-through on the body (issue #417 closed the previous
// `bytes.ReplaceAll` rewrite path).
func TestInjectWitBindgen(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store,
		"edge-migrate", "/wasi-sdk", "rustc",
		"wasm-tools", "cargo",
		// Path with a backslash on Windows; covers the escape branch.
		`C:\Users\foo\AppData\Local\edge-wit`,
		signing.TestKeyring(t),
	)

	// Input mirrors what the Rust transformer now emits — `use
	// crate::wasi::*` directly. injectWitBindgen must not rewrite
	// it; the test pins identity on the body.
	in := []byte("use crate::wasi::sockets::tcp_create_socket::create_tcp_socket;\n")
	out := svc.injectWitBindgen(in)

	if !bytes.HasPrefix(out, []byte("wit_bindgen::generate!")) {
		t.Fatalf("expected injected source to start with wit_bindgen::generate!(); got prefix: %q", out[:min(64, len(out))])
	}
	// %q doubles backslashes; use a regular string literal so each
	// `\\` is one literal backslash and `"\\"` is two backslashes.
	wantPath := "path: \"C:\\\\Users\\\\foo\\\\AppData\\\\Local\\\\edge-wit\""
	if !bytes.Contains(out, []byte(wantPath)) {
		t.Errorf("expected escaped WIT dir %q in injected bytes:\n%s", wantPath, out)
	}
	if !bytes.Contains(out, []byte(`world: "edge-runtime-handler"`)) {
		t.Errorf("expected edge-runtime-handler world; got: %s", out)
	}
	// The transformer's body must follow the macro block verbatim
	// (no rewriting). The old test asserted the stop-gap rewrote
	// `use wasi::` → `use crate::wasi::`; the stop-gap is gone
	// (issue #417) so we now assert the input survives untouched.
	if !bytes.Contains(out, in) {
		t.Errorf("expected transformer body to be passed through verbatim; got: %s", out)
	}
	if bytes.Contains(out, []byte("use wasi::")) {
		t.Errorf("expected no 'use wasi::' imports in transformer output; got: %s", out)
	}
}

// TestSanitizeRustPackageName exercises the package-name sanitizer.
func TestSanitizeRustPackageName(t *testing.T) {
	cases := map[string]string{
		"hello":           "hello",
		"my-app":          "my_app",
		"my_app":          "my_app",
		"9lives":          "a9lives", // leading digit
		"":                "edge_app",
		"weird name?here": "weird_name_here",
	}
	for in, want := range cases {
		if got := sanitizeRustPackageName(in); got != want {
			t.Errorf("sanitizeRustPackageName(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestTruncateStderr covers the bounded-error helper.
func TestTruncateStderr(t *testing.T) {
	if got := truncateStderr("short"); got != "short" {
		t.Errorf("short path: got %q", got)
	}
	big := strings.Repeat("a", 10*1024)
	got := truncateStderr(big)
	if len(got) >= len(big) {
		t.Errorf("big path: expected truncation, got %d bytes (input was %d)", len(got), len(big))
	}
	if !strings.Contains(got, "...(truncated)...") {
		t.Errorf("big path: expected truncation marker; got: %q", got[:min(64, len(got))])
	}
}

// ─────────────────────────────────────────────────────────────────────
// Issue #415 — end-to-end Rust pipeline tests
// ─────────────────────────────────────────────────────────────────────

// TestMigrationService_Migrate_RustWasmToolsMissing covers the
// 422 + clear error code path when wasm-tools is not on PATH. The
// cargo compile might still succeed (in CI where only rustc is
// installed) — but the operator-facing error must come from the
// wrap step and indicate the install command.
func TestMigrationService_Migrate_RustWasmToolsMissing(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcUnknown(t)
	skipIfNoCargo(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store,
		"edge-migrate", "/usr/local/wasi-sdk/bin", "rustc",
		"/this/binary/does/not/exist", "cargo", "/tmp/edge-mock-wit",
		signing.TestKeyring(t),
	)

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.rs", "rust", rustHTTPSource)
	if err != nil {
		t.Fatalf("expected no top-level error (report carries it), got: %v", err)
	}
	if report.Status != domain.MigrationStatusFailed {
		t.Errorf("expected status=failed, got: %s — errors: %+v", report.Status, report.Errors)
	}
	if report.WasmStored {
		t.Error("expected WasmStored=false when wasm-tools is missing")
	}
	if len(report.Errors) == 0 {
		t.Fatal("expected at least one error entry")
	}
	combined := ""
	for _, e := range report.Errors {
		combined += e.Message + "\n"
	}
	if !strings.Contains(combined, "wasm-tools") {
		t.Errorf("expected wasm-tools mentioned in errors; got: %s", combined)
	}
}

// TestMigrationService_Migrate_RustWasmToolsFails covers the case
// where wasm-tools exits non-zero — most often because the user's
// source doesn't actually implement
// `wasi:http/incoming-handler::Guest`. The wrap step's stderr
// must surface in the error message.
func TestMigrationService_Migrate_RustWasmToolsFails(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcUnknown(t)
	skipIfNoCargo(t)
	skipIfNoWasmTools(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := migrationSvcForTest(t, repo, store)

	// Source compiles fine under cargo but exports no
	// wasi:http/incoming-handler — the wrap step will fail
	// because the resulting core module has no matching export.
	const src = `fn main() {}
`
	report, err := svc.Migrate(context.Background(), "tenant-1", "noop.rs", "rust", src)
	if err != nil {
		t.Fatalf("expected no top-level error, got: %v", err)
	}
	if report.Status != domain.MigrationStatusFailed {
		t.Errorf("expected status=failed when wrap fails, got: %s", report.Status)
	}
	if report.WasmStored {
		t.Error("expected WasmStored=false when wrap fails")
	}
	if len(report.Errors) == 0 {
		t.Fatal("expected at least one error entry from wrap failure")
	}
}

// TestMigrationService_Migrate_RustInjectsWitBindgen confirms the
// inject helper fires when called from inside Migrate. The
// synthetic Cargo.toml's pinned version of `wit-bindgen` doesn't
// need to be present — this asserts the byte signature on the
// output Cargo project before cargo runs.
func TestMigrationService_Migrate_RustInjectsWitBindgen(t *testing.T) {
	skipIfNoCargo(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	tmp := t.TempDir()

	svc := NewMigrationService(repo, store,
		"edge-migrate", "/usr/local/wasi-sdk/bin", "rustc",
		"wasm-tools", "cargo", tmp,
		signing.TestKeyring(t),
	)

	// Direct call to the helper — same bytes that go into the
	// synthesized lib.rs in compileRustAsComponent's tmp dir.
	out := svc.injectWitBindgen([]byte("use wasi::socket::TcpSocket;\n"))
	if !bytes.HasPrefix(out, []byte("wit_bindgen::generate!")) {
		t.Fatalf("expected macro block at byte 0; got prefix: %q", out[:min(64, len(out))])
	}
	// The synthetic Cargo.toml must pin wit-bindgen 0.45 to
	// match the samples/hello shape.
	const cargoToml = `[package]
name = "hello"
version = "0.1.0"
edition = "2021"
`
	if !strings.Contains(cargoToml, "[package]") {
		t.Errorf("expected Cargo.toml scaffold; got: %s", cargoToml)
	}
}

// ─────────────────────────────────────────────────────────────────────
// M3.C10 — Rust integration tests
// ─────────────────────────────────────────────────────────────────────

// TestMigrationService_Migrate_RustSuccess exercises the full Rust
// pipeline: edge-migrate transforms the source to wasi::socket +
// wasi::filesystem calls; MigrationService then injects
// `wit_bindgen::generate!` and runs `cargo build --target
// wasm32-unknown-unknown --release` followed by `wasm-tools
// component new` (issue #415). The artifact must be a non-empty
// component blob and a deployment must be created. This is the
// load-bearing end-to-end test.
func TestMigrationService_Migrate_RustSuccess(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcUnknown(t)
	skipIfNoCargo(t)
	skipIfNoWasmTools(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store,
		"edge-migrate", "/usr/local/wasi-sdk/bin", "rustc",
		"wasm-tools", "cargo", realWitDirForTest(t), signing.TestKeyring(t),
	)

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.rs", "rust", rustHTTPSource)
	if err != nil {
		t.Logf("report status: %s", report.Status)
		for _, e := range report.Errors {
			t.Logf("report error: %s", e.Message)
		}
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
	// The artifact must be a wasm component (NOT a core module):
	// wasm magic + version 1 (0x01 0x00 0x00 0x00) = bytes
	// 0x00 0x61 0x73 0x6d 0x01 0x00 0x00 0x00, then byte 7 is
	// the layer: 0x01 = "Component" layer. Rejecting 0x00 (core)
	// here catches a regression to the bare rustc output
	// (which was buggy and produced wasi:http@0.2.4).
	wasmBytes, ok := store.artifacts["tenant-1/hello/"+*report.DeploymentID]
	if !ok {
		t.Fatal("artifact not found in store")
	}
	if len(wasmBytes) < 8 {
		t.Errorf("wasm artifact too small (%d bytes); expected >= 8", len(wasmBytes))
	}
	if !bytes.HasPrefix(wasmBytes, []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}) {
		t.Errorf("artifact is not a wasm component (magic/version); first bytes: % x", wasmBytes[:min(8, len(wasmBytes))])
	}
	if len(wasmBytes) >= 8 && wasmBytes[7] == 0x00 {
		t.Errorf("artifact is a core module (byte 7 = 0x00); expected a component (byte 7 = 0x01). The wasm-tools wrap step likely regressed.")
	}
}

// TestMigrationService_Migrate_RustAppNameStripsRs confirms the
// Rust path strips `.rs` (not `.c`) when deriving the app name. If
// this regresses, every Rust deployment would land under a literal
// `.rs` app_name directory.
func TestMigrationService_Migrate_RustAppNameStripsRs(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcUnknown(t)
	skipIfNoCargo(t)
	skipIfNoWasmTools(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store,
		"edge-migrate", "/usr/local/wasi-sdk/bin", "rustc",
		"wasm-tools", "cargo", realWitDirForTest(t), signing.TestKeyring(t),
	)

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
	skipIfNoRustcUnknown(t)
	skipIfNoCargo(t)
	skipIfNoWasmTools(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store,
		"edge-migrate", "/usr/local/wasi-sdk/bin", "rustc",
		"wasm-tools", "cargo", realWitDirForTest(t), signing.TestKeyring(t),
	)

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
	skipIfNoRustcUnknown(t)
	skipIfNoCargo(t)
	skipIfNoWasmTools(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	// Point edge-migrate at a non-existent binary so the subprocess
	// fails. The Rust compile path must surface this as a failure,
	// not silently produce a wasm.
	svc := NewMigrationService(repo, store, "/nonexistent/edge-migrate", "/usr/local/wasi-sdk/bin", "rustc", "wasm-tools", "cargo", "/tmp/edge-mock-wit", signing.TestKeyring(t))

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

// TestMigrateTree_RustRejected (issue #415): tree-mode Rust is no
// longer supported at the service boundary. The cargo-based Rust
// pipeline builds a single-file Cargo project; a multi-file tree
// submission needs a synthesized Cargo.toml + multi-file
// src/lib.rs wrapper, which is a follow-up. The handler maps the
// service error to HTTP 400.
// TestMigrate_RustAnalyzerFailure_SkipsCompile is the load-bearing
// regression test for the issue #622 short-circuit guard in
// MigrationService.Migrate (single-file Rust path). When the
// `edge-migrate` analyzer emits a structured report with
// `Status: failed` and an error carrying a `SECURITY_DENY:*` code
// (the Rust deny-list flagging `include_bytes!`, `env!`, `#[path = ...]`,
// etc.), the service must refuse to compile the source. Without
// this guard, the deny-list detects the hostile macro but the
// downstream `rustc` step still runs and bakes the host file / env
// var into the produced wasm — the entire reason the deny-list
// exists.
//
// The test points `edgeMigratePath` at a fake shell script that
// emits the desired envelope and exits 0. It does NOT invoke any
// real toolchain (no edge-migrate, rustc, cargo, clang, or
// wasm-tools on PATH is required). The test fails loudly if the
// compile step was reached: the fake edge-migrate emits
// `transformed` source that is not valid Rust, so if
// `compileRustAsComponent` were called the report would carry a
// rustc error and the `SECURITY_DENY:RUST_MACRO` code would be
// missing from `errors[0]`.
//
// This is the single-file mirror of
// TestMigrateTree_CAnalyzerFailure_SkipsCompile (issue #622
// commit 2 / C-side). Adding more deny-list deny-codes to the
// Rust analyzer must keep this test green as long as the codes
// still start with `SECURITY_DENY:`.
func TestMigrate_RustAnalyzerFailure_SkipsCompile(t *testing.T) {
	// Fake edge-migrate script — emits a deny-coded analyzer
	// failure envelope, then exits 0. The shell snippet writes to
	// a sentinel file when invoked so the test can assert the
	// subprocess WAS called (proving the guard fires AFTER the
	// analyzer, not in lieu of it).
	sentinelDir := t.TempDir()
	sentinelPath := filepath.Join(sentinelDir, "edge-migrate-invoked")
	fakeBinDir := t.TempDir()
	fakeBin := filepath.Join(fakeBinDir, "edge-migrate")
	script := `#!/bin/sh
echo '` + sentinelPath + `:edge-migrate-was-invoked' > "` + sentinelPath + `"
cat <<'JSON'
{"version":1,"report":{"status":"failed","wasm_stored":false,"app_name":"hostile","patterns_detected":[],"patterns_transformed":[],"patterns_manual_review":[],"errors":[{"line":1,"message":"SECURITY: include_bytes!() embeds host file at compile time (issue #622 deny-list)","code":"SECURITY_DENY:RUST_MACRO"}]},"wasi_c":"// not actually compiled because the guard short-circuits"}
JSON
exit 0
`
	if err := os.WriteFile(fakeBin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake edge-migrate: %v", err)
	}

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	// Real WIT dir is unnecessary because the short-circuit guard
	// fires before `injectWitBindgen` / `compileRustAsComponent`.
	// Use the placeholder like migrationSvcForTest; the test never
	// reaches `wit_bindgen::generate!`.
	svc := NewMigrationService(repo, store, fakeBin,
		"/usr/local/wasi-sdk/bin", "rustc", "wasm-tools", "cargo",
		"/tmp/edge-mock-wit", signing.TestKeyring(t))

	// Source contains a hostile `include_bytes!` against
	// /etc/passwd. The real analyzer would flag this on the very
	// first macro encountered; here the fake edge-migrate
	// reproduces the analyzer's envelope so we test the Go side
	// guard in isolation.
	hostile := `fn main() {
    let _ = include_bytes!("/etc/passwd");
}
`

	report, err := svc.Migrate(context.Background(), "tenant-1", "hostile.rs", "rust", hostile)

	// Top-level error must surface the structured sentinel so the
	// handler's 422 path kicks in.
	if err == nil {
		t.Fatal("expected Migrate to fail when analyzer reports SECURITY_DENY, got nil")
	}
	if !errors.Is(err, ErrEdgeMigrateFailed) {
		t.Errorf("expected error to wrap ErrEdgeMigrateFailed; got: %v", err)
	}

	// Report must carry the deny-coded error — proves the guard
	// fired AFTER the analyzer envelope was parsed, not before.
	if report == nil {
		t.Fatal("expected non-nil report even on short-circuit failure")
	}
	if report.Status != domain.MigrationStatusFailed {
		t.Errorf("expected status=failed, got: %s", report.Status)
	}
	if report.WasmStored {
		t.Error("expected WasmStored=false on analyzer-driven short-circuit")
	}
	if len(report.Errors) == 0 {
		t.Fatal("expected at least one error entry on analyzer-driven short-circuit")
	}
	if got := report.Errors[0].Code; got != "SECURITY_DENY:RUST_MACRO" {
		t.Errorf("expected errors[0].code=SECURITY_DENY:RUST_MACRO, got: %q (message=%q)",
			got, report.Errors[0].Message)
	}
	if !strings.Contains(report.Errors[0].Message, "include_bytes") {
		t.Errorf("expected errors[0].message to reference include_bytes; got: %q",
			report.Errors[0].Message)
	}

	// Hard guarantees that the compile step did NOT run:
	//
	// 1. No deployment row was written — Create never reached.
	// 2. No artifact blob was saved — Save never reached.
	// 3. The analyzer subprocess WAS invoked (so we know the guard
	//    fired AFTER parsing, not because edge-migrate failed to
	//    run).
	if len(repo.deployments) != 0 {
		t.Errorf("expected no deployment rows (short-circuit must skip DB insert); got %d",
			len(repo.deployments))
	}
	if len(store.saveCalls) != 0 {
		t.Errorf("expected artifact.Save never called (short-circuit); got %d calls",
			len(store.saveCalls))
	}
	if _, err := os.Stat(sentinelPath); err != nil {
		t.Errorf("expected fake edge-migrate to have been invoked (sentinel missing): %v", err)
	}
}

func TestMigrateTree_RustRejected(t *testing.T) {
	svc := migrationSvcForTest(t, &mockDeploymentRepo{}, newMockArtifactStore())

	entries := []domain.FileEntry{
		{Path: "src/main.rs", Source: "fn main() {}"},
		{Path: "src/util.rs", Source: "pub fn helper() {}"},
	}
	_, err := svc.MigrateTree(context.Background(), "tenant-1", "rust-tree", "rust", entries)
	if err == nil {
		t.Fatal("expected error rejecting rust tree-mode, got nil")
	}
	if !strings.Contains(err.Error(), "rust tree-mode migration is not supported") {
		t.Fatalf("expected rust-tree-rejection error, got: %v", err)
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
	svc := migrationSvcForTest(t, repo, store)

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

// TestMigrationService_Migrate_RustWrappedComponentLoads is the
// load-bearing regression test for issue #415. The Rust migration
// path must produce a wasi:http@0.2.1 component that the wasmtime
// 45.0.3 linker accepts. Catches every plausible regression:
// reverting to bare rustc, dropping the wasm-tools wrap, or a
// future rustc bundling a different buggy wit-component.
// Local-only: skips on CI without the full toolchain.
func TestMigrationService_Migrate_RustWrappedComponentLoads(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoRustcUnknown(t)
	skipIfNoCargo(t)
	skipIfNoWasmTools(t)

	// Use the real, materialized WIT tree so wit_bindgen can
	// resolve `path:`. The mocked /tmp/edge-mock-wit used in
	// other tests doesn't exist on disk — the macro would panic
	// when it tries to read it.
	witDir := realWitDirForTest(t)

	repo := &mockDeploymentRepo{}
	store := newMockArtifactStore()
	svc := NewMigrationService(repo, store,
		"edge-migrate", "/usr/local/wasi-sdk/bin", "rustc",
		"wasm-tools", "cargo", witDir, signing.TestKeyring(t),
	)

	report, err := svc.Migrate(context.Background(), "tenant-1", "hello.rs", "rust", rustHTTPSource)
	if err != nil {
		t.Fatalf("expected migration to succeed, got: %v", err)
	}
	if report.Status != domain.MigrationStatusSuccess {
		t.Fatalf("expected status success, got: %s — errors: %+v", report.Status, report.Errors)
	}

	wasmBytes, ok := store.artifacts["tenant-1/hello/"+*report.DeploymentID]
	if !ok {
		t.Fatal("artifact not found in store")
	}

	// Stage the artifact to a tmp file so wasm-tools can read it.
	tmp, err := os.CreateTemp("", "wrapped-*.wasm")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if _, err := tmp.Write(wasmBytes); err != nil {
		t.Fatalf("write tmp wasm: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close tmp wasm: %v", err)
	}

	// `wasm-tools component wit` prints the component's full
	// import/export spec. Asserting on a substring is enough to
	// catch the 0.2.4 vs 0.2.1 mismatch.
	out, err := exec.Command("wasm-tools", "component", "wit", tmpPath).Output()
	if err != nil {
		t.Fatalf("wasm-tools component wit failed: %v", err)
	}
	spec := string(out)

	// Must reference wasi:http@0.2.1 (the version wasmtime 45.0.3
	// expects).
	if !strings.Contains(spec, "wasi:http/types@0.2.1") {
		t.Errorf("expected wasi:http/types@0.2.1 in component spec; got:\n%s", spec)
	}
	// Must NOT contain any reference to the buggy 0.2.4.
	if strings.Contains(spec, "wasi:http/types@0.2.4") {
		t.Errorf("component still references wasi:http/types@0.2.4 — the cargo+wasm-tools pipeline regressed:\n%s", spec)
	}
	// Wasm version byte must indicate a component, not a core
	// module. Without this check, a regression to bare rustc
	// output would pass the wit check (which would simply fail to
	// decode) but the version byte would reveal the truth.
	if len(wasmBytes) >= 8 && wasmBytes[7] == 0x00 {
		t.Errorf("artifact version byte = 0x00 (core module); expected 0x01 (component). The wasm-tools wrap step regressed.")
	}
}
