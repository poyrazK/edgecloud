package handler

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// mockDeploymentRepo implements service.DeploymentRepoInterface for testing.
type mockDeploymentRepo struct {
	deployments []*domain.Deployment
	createErr   error
	// deleteCalls records each DeleteByID invocation. Used by the
	// rollback tests to assert the compensating write fired.
	deleteCalls []string
	// deleteErr returns this error from DeleteByID if non-nil.
	deleteErr error
	// saveErr returns this error from Save if non-nil. The artifact
	// store mock reads it via the `saveErr` field below.
}

func (m *mockDeploymentRepo) Create(ctx context.Context, d *domain.Deployment) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.deployments = append(m.deployments, d)
	return nil
}

// mockArtifactStore implements storage.ArtifactStore for testing.
// Migrate / MigrateTree only call Save, so Open and Delete are
// no-ops — they exist so the mock satisfies the wider interface
// (introduced by issue #127, when ArtifactStoreInterface was
// folded into storage.ArtifactStore).
type mockArtifactStore struct {
	// saveErr makes Save return this error if non-nil. Used by the
	// rollback tests to trigger the compensating-write path.
	saveErr error
}

func (m *mockArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	return m.saveErr
}

func (m *mockArtifactStore) Open(ctx context.Context, tenantID, appName, deploymentID string) (io.ReadCloser, error) {
	return nil, nil
}

func (m *mockArtifactStore) Delete(ctx context.Context, tenantID, appName, deploymentID string) error {
	return nil
}

// skipIfNoEdgeMigrate skips the test if edge-migrate is not in PATH.
func skipIfNoEdgeMigrate(t *testing.T) {
	if _, err := exec.LookPath("edge-migrate"); err != nil {
		t.Skip("edge-migrate not in PATH")
	}
}

// skipIfNoClang skips if wasi-sdk clang is not available.
func skipIfNoClang(t *testing.T) {
	if _, err := exec.LookPath(filepath.Join("/usr/local/wasi-sdk/bin", "clang")); err != nil {
		t.Skip("wasi-sdk clang not available at /usr/local/wasi-sdk/bin/clang")
	}
}

// makeMigrationReq creates a multipart POST request for /api/migrate.
func makeMigrationReq(filename, language, fileContent string) (*http.Request, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("filename", filename); err != nil {
		return nil, err
	}
	if err := writer.WriteField("language", language); err != nil {
		return nil, err
	}
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return nil, err
	}
	if _, err := part.Write([]byte(fileContent)); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	req := httptest.NewRequest("POST", "/api/migrate", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req, nil
}

func TestMigrationHandler_Migrate_Success(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin", "rustc")
	h := NewMigrationHandler(svc)

	source := `#include <stdio.h>
int main() { return 0; }`
	req, err := makeMigrationReq("hello.c", "c", source)
	if err != nil {
		t.Fatalf("makeMigrationReq: %v", err)
	}
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Migrate(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200, got: %d — body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Header().Get("Content-Type"), "application/json") {
		t.Errorf("expected Content-Type application/json, got: %s", rr.Header().Get("Content-Type"))
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"status"`) {
		t.Errorf("expected JSON with status field, got: %s", body)
	}
}

func TestMigrationHandler_Migrate_MissingFile(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin", "rustc")
	h := NewMigrationHandler(svc)

	// Build multipart without a "file" field
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if err := writer.WriteField("filename", "hello.c"); err != nil {
		t.Fatalf("WriteField: %v", err)
	}
	if err := writer.WriteField("language", "c"); err != nil {
		t.Fatalf("WriteField: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close(): %v", err)
	}

	req := httptest.NewRequest("POST", "/api/migrate", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Migrate(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got: %d", rr.Code)
	}
	bodyStr := rr.Body.String()
	if !strings.Contains(bodyStr, "missing file field") {
		t.Errorf("expected 'missing file field' error, got: %s", bodyStr)
	}
}

func TestMigrationHandler_Migrate_AcceptsRustLanguage(t *testing.T) {
	// The handler's language gate widens to c + rust in M3. Without
	// edge-migrate on PATH the handler may then surface a 500 from
	// the service — that's fine, we only assert the gate is open.
	skipIfNoEdgeMigrate(t)

	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin", "rustc")
	h := NewMigrationHandler(svc)

	source := `fn main() {}`
	req, err := makeMigrationReq("hello.rs", "rust", source)
	if err != nil {
		t.Fatalf("makeMigrationReq: %v", err)
	}
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Migrate(rr, req)

	if rr.Code == http.StatusBadRequest {
		t.Errorf("rust language must not hit the language gate, got 400: %s", rr.Body.String())
	}
}

func TestMigrationHandler_Migrate_RejectsUnknownLanguage(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin", "rustc")
	h := NewMigrationHandler(svc)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if err := writer.WriteField("filename", "hello.py"); err != nil {
		t.Fatalf("WriteField: %v", err)
	}
	if err := writer.WriteField("language", "python"); err != nil {
		t.Fatalf("WriteField: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close(): %v", err)
	}

	req := httptest.NewRequest("POST", "/api/migrate", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Migrate(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got: %d", rr.Code)
	}
	bodyStr := rr.Body.String()
	if !strings.Contains(bodyStr, "only c and rust are supported") {
		t.Errorf("expected 'only c and rust are supported', got: %s", bodyStr)
	}
}

func TestMigrationHandler_Migrate_NoMultipart(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin", "rustc")
	h := NewMigrationHandler(svc)

	req := httptest.NewRequest("POST", "/api/migrate", strings.NewReader("not multipart"))
	req.Header.Set("Content-Type", "text/plain")
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Migrate(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got: %d — body: %s", rr.Code, rr.Body.String())
	}
}

func TestMigrationHandler_Migrate_MissingTenantID(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin", "rustc")
	h := NewMigrationHandler(svc)

	source := `#include <stdio.h>
int main() { return 0; }`
	req, err := makeMigrationReq("hello.c", "c", source)
	if err != nil {
		t.Fatalf("makeMigrationReq: %v", err)
	}
	// No tenant ID in context — uses empty context

	rr := httptest.NewRecorder()
	h.Migrate(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got: %d", rr.Code)
	}
}

func TestMigrationHandler_Migrate_PathTraversalFilename(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin", "rustc")
	h := NewMigrationHandler(svc)

	source := `#include <stdio.h>
int main() { return 0; }`
	req, err := makeMigrationReq("../etc.c", "c", source)
	if err != nil {
		t.Fatalf("makeMigrationReq: %v", err)
	}
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Migrate(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got: %d — body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "path-traversal") {
		t.Errorf("expected 'path-traversal' in error, got: %s", rr.Body.String())
	}
	if len(repo.deployments) != 0 {
		t.Errorf("expected 0 deployments created, got: %d", len(repo.deployments))
	}
}

// ─────────────────────────────────────────────────────────────────────
// MigrateTree handler tests (M2.C10)
// ─────────────────────────────────────────────────────────────────────

// makeTreeReq builds a multipart POST with a `tree` JSON manifest and
// one or more `file` parts. `tenant` is set into the request context
// (mimicking middleware.GetTenantID).
func makeTreeReq(t *testing.T, appName, language, manifest string, files map[string]string) *http.Request {
	t.Helper()
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	if appName != "" {
		_ = w.WriteField("app_name", appName)
	}
	if language != "" {
		_ = w.WriteField("language", language)
	}
	if manifest != "" {
		_ = w.WriteField("tree", manifest)
	}
	for name, content := range files {
		fw, err := w.CreateFormFile("file", name)
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		if _, err := fw.Write([]byte(content)); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	req := httptest.NewRequest("POST", "/api/migrate-tree", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req
}

// withTenantID stuffs a tenant ID into the request context the way
// middleware.GetTenantID expects.
func withTenantID(req *http.Request, tenantID string) *http.Request {
	ctx := middleware.WithTenantID(req.Context(), tenantID)
	return req.WithContext(ctx)
}

func TestMigrateTree_RejectsMissingTenantID(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	req := makeTreeReq(t, "hello", "c", `{"files":["main.c"]}`, map[string]string{"main.c": "int main(){}"})
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestMigrateTree_RejectsBadAppName(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	for _, bad := range []string{"../traversal", "Bad-Name", "a/b", ""} {
		req := makeTreeReq(t, bad, "c", `{"files":["main.c"]}`, map[string]string{"main.c": "x"})
		req = withTenantID(req, "t_1")
		rr := httptest.NewRecorder()
		h.MigrateTree(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("app_name=%q: expected 400, got %d: %s", bad, rr.Code, rr.Body.String())
		}
	}
}

func TestMigrateTree_RejectsMissingAppName(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	// Make a request without an app_name field.
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("language", "c")
	_ = w.WriteField("tree", `{"files":["main.c"]}`)
	fw, _ := w.CreateFormFile("file", "main.c")
	if _, err := fw.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	req := httptest.NewRequest("POST", "/api/migrate-tree", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestMigrateTree_AcceptsRustLanguage(t *testing.T) {
	// Same shape as the C multipart test, but with `language: rust`
	// and a `.rs` file. The handler must pass the language gate; the
	// service is stubbed and will produce a 500 if it tries to spawn
	// `edge-migrate` (it doesn't, since the test path doesn't need
	// edge-migrate to run — the gate rejection happens before any
	// service work).
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	req := makeTreeReq(t, "hello", "rust", `{"files":["main.rs"]}`, map[string]string{"main.rs": "fn main(){}"})
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code == http.StatusBadRequest {
		t.Errorf("rust language must not hit the language gate, got 400: %s", rr.Body.String())
	}
}

func TestMigrateTree_RejectsUnknownLanguage(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	req := makeTreeReq(t, "hello", "python", `{"files":["main.py"]}`, map[string]string{"main.py": "x"})
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for python, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "only c and rust are supported") {
		t.Errorf("expected 'only c and rust are supported', got: %s", rr.Body.String())
	}
}

func TestMigrateTree_AcceptsRsInZipVariant(t *testing.T) {
	// The zip variant must accept `.rs` entries without rejecting
	// them at the extension filter. We construct a zip in-memory,
	// POST it, and assert the response is not 400 (the gate is open;
	// the service is stubbed and will 500 if it tries to run the
	// toolchain, which is acceptable for this assertion).
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)

	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	f, err := zw.Create("main.rs")
	if err != nil {
		t.Fatalf("zip.Create: %v", err)
	}
	if _, err := f.Write([]byte("fn main() {}\n")); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip.Close: %v", err)
	}

	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("app_name", "hello")
	_ = w.WriteField("language", "rust")
	treePart, err := w.CreateFormFile("tree", "src.zip")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := treePart.Write(zipBuf.Bytes()); err != nil {
		t.Fatalf("zip write to multipart: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("multipart close: %v", err)
	}

	req := httptest.NewRequest("POST", "/api/migrate-tree", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req = withTenantID(req, "t_1")

	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code == http.StatusBadRequest {
		t.Errorf("rust zip entry must not hit language/extension gate, got 400: %s", rr.Body.String())
	}
}

func TestMigrateTree_RejectsManifestMismatch(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	// Manifest declares 2 files, but only 1 file part.
	req := makeTreeReq(t, "hello", "c",
		`{"files":["main.c","helper.c"]}`,
		map[string]string{"main.c": "x"})
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 manifest mismatch, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestMigrateTree_RejectsPathTraversal(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	// Manifest references a path with `..`.
	req := makeTreeReq(t, "hello", "c",
		`{"files":["../etc/passwd"]}`,
		map[string]string{"passwd": "x"})
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 unsafe path, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestMigrateTree_RejectsTooManyFiles(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	// Build a manifest with maxTreeFiles+1 entries. We don't actually
	// upload that many file parts — the mismatch is caught first, so
	// we use a count over the limit in a valid manifest.
	names := make([]string, maxTreeFiles+1)
	files := make(map[string]string)
	for i := range names {
		names[i] = "f" + itoa(i) + ".c"
		files[names[i]] = "x"
	}
	// JSON marshal the names.
	json := "["
	for i, n := range names {
		if i > 0 {
			json += ","
		}
		json += "\"" + n + "\""
	}
	json += "]"
	manifest := `{"files":` + json + `}`
	req := makeTreeReq(t, "hello", "c", manifest, files)
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 too-many-files, got %d: %s", rr.Code, rr.Body.String())
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}

func TestMigrateTree_RejectsOversizedBody(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	// Build a valid multipart body that's over the cap. We use a
	// single large file part padded past maxTreeBodyBytes.
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("app_name", "hello")
	_ = w.WriteField("language", "c")
	_ = w.WriteField("tree", `{"files":["main.c"]}`)
	fw, _ := w.CreateFormFile("file", "main.c")
	padding := make([]byte, maxTreeBodyBytes+1024)
	for i := range padding {
		padding[i] = 'a'
	}
	if _, err := fw.Write(padding); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	req := httptest.NewRequest("POST", "/api/migrate-tree", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestMigrateTree_RejectsMissingTree(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	// No `tree` field, no `file` parts.
	req := makeTreeReq(t, "hello", "c", "", nil)
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 missing tree, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestMigrateTree_RejectsInvalidManifestJSON(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	req := makeTreeReq(t, "hello", "c", "not json", map[string]string{"main.c": "x"})
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 bad manifest, got %d: %s", rr.Code, rr.Body.String())
	}
}

// TestMigrateTree_PerFileTransformFailure_Status422 confirms the
// handler returns HTTP 422 (Unprocessable Entity) with a
// structured TreeMigrationReport body when the service hits a
// request-level failure (per-file transform, compile, oversized
// artifact, etc.). Before the isClientMigrationError fix, these
// failures returned 200 OK with `status: "failed"` in the body —
// silently treated as success by auto-activating tenants.
func TestMigrateTree_PerFileTransformFailure_Status422(t *testing.T) {
	// Point the service at a non-existent edge-migrate binary so
	// every per-file transform fails. The service then surfaces
	// ErrMigrateTreeFailed + a populated TreeMigrationReport.
	svc := service.NewMigrationService(
		&mockDeploymentRepo{}, &mockArtifactStore{},
		"/this/binary/does/not/exist", "/wasi-sdk", "rustc",
	)
	h := NewMigrationHandler(svc)
	req := makeTreeReq(t, "hello", "c", `{"files":["main.c"]}`,
		map[string]string{"main.c": "int main(){return 0;}\n"})
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 on tree failure, got %d: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json content type, got %q", ct)
	}
	// Body must still be a parseable TreeMigrationReport.
	var report domain.TreeMigrationReport
	if err := json.Unmarshal(rr.Body.Bytes(), &report); err != nil {
		t.Fatalf("response body must be a valid TreeMigrationReport, got: %v\nbody: %s",
			err, rr.Body.String())
	}
	if report.Status != domain.MigrationStatusFailed {
		t.Errorf("expected report status Failed, got: %v", report.Status)
	}
	if len(report.Errors) == 0 {
		t.Error("expected at least one error in the report body")
	}
}

// TestMigrateTree_RejectsUnknownExtensionMultipartPart covers the
// symmetry between the multipart and zip variants: a file part
// whose extension is not in `treeUploadExts` (`.c`/`.h`/`.rs`)
// must be rejected at the handler (400), not silently accepted
// and passed to clang/rustc. The zip variant has filtered on
// `treeUploadExts` since M2; this brings the multipart variant
// in line.
func TestMigrateTree_RejectsUnknownExtensionMultipartPart(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	// .txt is not a recognized source extension — the handler
	// must reject it before reaching the service.
	req := makeTreeReq(t, "hello", "c", `{"files":["notes.txt"]}`,
		map[string]string{"notes.txt": "this is a doc"})
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for .txt part, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "unsupported file extension") {
		t.Errorf("expected 'unsupported file extension' in body, got: %s", rr.Body.String())
	}
}

// TestMigrateTree_RejectsUnknownExtensionInManifest covers the
// manifest-side filter: a manifest entry with an extension outside
// `treeUploadExts` must be rejected, even if the corresponding file
// part is present (e.g. the part is `foo.txt` and the manifest
// also says `foo.txt`).
func TestMigrateTree_RejectsUnknownExtensionInManifest(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)
	req := makeTreeReq(t, "hello", "rust", `{"files":["main.py"]}`,
		map[string]string{"main.py": "print('hi')"})
	req = withTenantID(req, "t_1")
	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for .py manifest entry, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "unsupported file extension") {
		t.Errorf("expected 'unsupported file extension' in body, got: %s", rr.Body.String())
	}
}

// TestMigrateTree_RejectsOversizedPart covers the per-file body
// cap: a single file part > 5 MiB must be rejected with 413,
// even when the total request body is well under the 50 MiB cap.
// The per-part cap protects the server from a malicious caller
// who uploads one huge part to consume the whole body budget
// before the per-file manifest mismatch check runs.
func TestMigrateTree_RejectsOversizedPart(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)

	// Build a multipart request with a single file part of 6 MiB.
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("app_name", "hello")
	_ = w.WriteField("language", "c")
	_ = w.WriteField("tree", `{"files":["big.c"]}`)
	fw, err := w.CreateFormFile("file", "big.c")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	// 6 MiB of zeros. The per-part cap is 5 MiB; this exceeds it.
	if _, err := fw.Write(make([]byte, 6<<20)); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	req := httptest.NewRequest("POST", "/api/migrate-tree", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req = withTenantID(req, "t_1")

	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for 6 MiB part, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "exceeds") {
		t.Errorf("expected 'exceeds' in body, got: %s", rr.Body.String())
	}
}

// TestMigrateTree_RejectsOversizedZipEntry covers the per-entry
// cap on the zip variant: a single zip entry > 5 MiB must be
// rejected even when the total decompressed size is under the
// 50 MiB cap. Same threat model as the multipart per-part cap.
func TestMigrateTree_RejectsOversizedZipEntry(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)

	// Build a zip with a single 6 MiB entry.
	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	f, err := zw.Create("big.c")
	if err != nil {
		t.Fatalf("zip.Create: %v", err)
	}
	if _, err := f.Write(make([]byte, 6<<20)); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip.Close: %v", err)
	}

	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("app_name", "hello")
	_ = w.WriteField("language", "c")
	treePart, err := w.CreateFormFile("tree", "src.zip")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := treePart.Write(zipBuf.Bytes()); err != nil {
		t.Fatalf("zip write to multipart: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("multipart close: %v", err)
	}
	req := httptest.NewRequest("POST", "/api/migrate-tree", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req = withTenantID(req, "t_1")

	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized zip entry, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "exceeds") {
		t.Errorf("expected 'exceeds' in body, got: %s", rr.Body.String())
	}
}

// TestMigrateTree_ZipSlip covers the zip-slip vulnerability: a
// zip entry whose name contains `../` must be rejected before the
// handler writes it to disk. The isSafeFilePath check in
// readZipEntries is the only line of defense — without this test,
// a regression there would silently let attackers overwrite files
// outside the temp dir.
func TestMigrateTree_ZipSlip(t *testing.T) {
	svc := service.NewMigrationService(&mockDeploymentRepo{}, &mockArtifactStore{}, "edge-migrate", "/wasi-sdk", "rustc")
	h := NewMigrationHandler(svc)

	// Build a zip containing a path-traversal entry. The zip
	// library happily stores these names; isSafeFilePath is what
	// must catch them.
	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	f, err := zw.Create("../../etc/passwd.c")
	if err != nil {
		t.Fatalf("zip.Create: %v", err)
	}
	if _, err := f.Write([]byte("int main(){return 0;}\n")); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip.Close: %v", err)
	}

	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	_ = w.WriteField("app_name", "hello")
	_ = w.WriteField("language", "c")
	treePart, err := w.CreateFormFile("tree", "src.zip")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := treePart.Write(zipBuf.Bytes()); err != nil {
		t.Fatalf("zip write to multipart: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("multipart close: %v", err)
	}
	req := httptest.NewRequest("POST", "/api/migrate-tree", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req = withTenantID(req, "t_1")

	rr := httptest.NewRecorder()
	h.MigrateTree(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for zip-slip entry, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "unsafe zip entry") {
		t.Errorf("expected 'unsafe zip entry' in body, got: %s", rr.Body.String())
	}
}
