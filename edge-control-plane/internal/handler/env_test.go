package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

type mockEnvSvc struct {
	setEnvErr    error
	listEnvs     []domain.AppEnv
	listEnvErr   error
	deleteEnvErr error
}

func (m *mockEnvSvc) SetEnv(ctx context.Context, tenantID, appName, key, value string) error {
	return m.setEnvErr
}
func (m *mockEnvSvc) ListEnv(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
	return m.listEnvs, m.listEnvErr
}
func (m *mockEnvSvc) DeleteEnv(ctx context.Context, tenantID, appName, key string) error {
	return m.deleteEnvErr
}

func newEnvMux(svc *mockEnvSvc) *http.ServeMux {
	mux := http.NewServeMux()
	h := NewEnvHandler(svc)
	mux.HandleFunc("PUT /api/apps/{appName}/env", h.Set)
	mux.HandleFunc("GET /api/apps/{appName}/env", h.List)
	mux.HandleFunc("DELETE /api/apps/{appName}/env/{key}", h.Delete)
	return mux
}

func TestEnvHandler_Set_Success(t *testing.T) {
	mux := newEnvMux(&mockEnvSvc{})

	body := `{"key":"LOG_LEVEL","value":"debug"}`
	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello/env", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rr.Code)
	}
}

func TestEnvHandler_Set_EmptyKey(t *testing.T) {
	mux := newEnvMux(&mockEnvSvc{})

	body := `{"key":"","value":"val"}`
	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello/env", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestEnvHandler_Set_InvalidBody(t *testing.T) {
	mux := newEnvMux(&mockEnvSvc{})

	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello/env", strings.NewReader(`bad json`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestEnvHandler_List(t *testing.T) {
	svc := &mockEnvSvc{
		listEnvs: []domain.AppEnv{
			{TenantID: "t_test", AppName: "hello", EnvKey: "LOG_LEVEL", EnvValue: "debug"},
			{TenantID: "t_test", AppName: "hello", EnvKey: "DATABASE_URL", EnvValue: "postgres://localhost"},
		},
	}
	mux := newEnvMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps/hello/env", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["LOG_LEVEL"] != "debug" {
		t.Errorf("LOG_LEVEL = %q, want 'debug'", resp["LOG_LEVEL"])
	}
	if resp["DATABASE_URL"] != "postgres://localhost" {
		t.Errorf("DATABASE_URL = %q", resp["DATABASE_URL"])
	}
}

func TestEnvHandler_Delete(t *testing.T) {
	mux := newEnvMux(&mockEnvSvc{})

	req := httptest.NewRequest(http.MethodDelete, "/api/apps/hello/env/LOG_LEVEL", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rr.Code)
	}
}
