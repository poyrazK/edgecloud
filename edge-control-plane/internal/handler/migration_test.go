package handler

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
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
}

func (m *mockDeploymentRepo) Create(ctx context.Context, d *domain.Deployment) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.deployments = append(m.deployments, d)
	return nil
}

// mockArtifactStore implements service.ArtifactStoreInterface for testing.
type mockArtifactStore struct{}

func (m *mockArtifactStore) Save(tenantID, appName, deploymentID string, r io.Reader) error {
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
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
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
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
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
	writer.Close()

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

func TestMigrationHandler_Migrate_NonC_Language(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
	h := NewMigrationHandler(svc)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if err := writer.WriteField("filename", "hello.rs"); err != nil {
		t.Fatalf("WriteField: %v", err)
	}
	if err := writer.WriteField("language", "rust"); err != nil {
		t.Fatalf("WriteField: %v", err)
	}
	writer.Close()

	req := httptest.NewRequest("POST", "/api/migrate", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Migrate(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got: %d", rr.Code)
	}
	bodyStr := rr.Body.String()
	if !strings.Contains(bodyStr, "only C language is supported") {
		t.Errorf("expected 'only C language is supported', got: %s", bodyStr)
	}
}

func TestMigrationHandler_Migrate_NoMultipart(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
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
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
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

func TestMigrationHandler_Migrate_ClangFailureReturnsPartial(t *testing.T) {
	skipIfNoEdgeMigrate(t)
	skipIfNoClang(t)

	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
	h := NewMigrationHandler(svc)

	// Source that edge-migrate accepts but clang rejects (syntax error).
	badSource := `int main() { invalid syntax here }`
	req, err := makeMigrationReq("hello.c", "c", badSource)
	if err != nil {
		t.Fatalf("makeMigrationReq: %v", err)
	}
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Migrate(rr, req)

	// Clang failure returns 200 with a partial report (analysis is still useful).
	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200 (partial), got: %d — body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"status"`) {
		t.Errorf("expected JSON report in body, got: %s", rr.Body.String())
	}
}

func TestMigrationHandler_Analyze_Success(t *testing.T) {
	skipIfNoEdgeMigrate(t)

	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
	h := NewMigrationHandler(svc)

	source := `int main() { int fd = socket(AF_INET, SOCK_STREAM, 0); return 0; }`
	req := httptest.NewRequest("GET", "/api/migrate/analyze?source="+url.QueryEscape(source), nil)
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Analyze(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200, got: %d — body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Header().Get("Content-Type"), "application/json") {
		t.Errorf("expected Content-Type application/json, got: %s", rr.Header().Get("Content-Type"))
	}
	if !strings.Contains(rr.Body.String(), `"patterns_detected"`) {
		t.Errorf("expected patterns_detected in response, got: %s", rr.Body.String())
	}
}

func TestMigrationHandler_Analyze_MissingTenantID(t *testing.T) {
	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
	h := NewMigrationHandler(svc)

	req := httptest.NewRequest("GET", "/api/migrate/analyze?source=int+main(){return+0;}", nil)
	// No tenant ID in context

	rr := httptest.NewRecorder()
	h.Analyze(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got: %d", rr.Code)
	}
}

func TestMigrationHandler_Analyze_MultipartFile(t *testing.T) {
	skipIfNoEdgeMigrate(t)

	repo := &mockDeploymentRepo{}
	store := &mockArtifactStore{}
	svc := service.NewMigrationService(repo, store, "edge-migrate", "/usr/local/wasi-sdk/bin")
	h := NewMigrationHandler(svc)

	source := `int main() { int fd = socket(AF_INET, SOCK_STREAM, 0); return 0; }`

	// Build multipart form with file field.
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if err := writer.WriteField("filename", "hello.c"); err != nil {
		t.Fatalf("WriteField: %v", err)
	}
	part, err := writer.CreateFormFile("file", "hello.c")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := part.Write([]byte(source)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	req := httptest.NewRequest("POST", "/api/migrate/analyze", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req = req.WithContext(middleware.WithTenantID(context.Background(), "tenant-test"))

	rr := httptest.NewRecorder()
	h.Analyze(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200, got: %d — body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"patterns_detected"`) {
		t.Errorf("expected patterns_detected in response, got: %s", rr.Body.String())
	}
}
