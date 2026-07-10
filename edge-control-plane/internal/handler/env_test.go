package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
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
	var resp []envVarResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp) != 2 {
		t.Fatalf("len(resp) = %d, want 2", len(resp))
	}
	// Sorted alphabetically: DATABASE_URL before LOG_LEVEL.
	if resp[0].Key != "DATABASE_URL" || resp[0].Value != "postgres://localhost" {
		t.Errorf("resp[0] = %+v, want {DATABASE_URL postgres://localhost}", resp[0])
	}
	if resp[1].Key != "LOG_LEVEL" || resp[1].Value != "debug" {
		t.Errorf("resp[1] = %+v, want {LOG_LEVEL debug}", resp[1])
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

// TestEnvHandler_Set_DisabledTenant: issue #560 — service.SetEnv
// returns ErrTenantDisabled, the handler must map it to 409 with
// the canonical CONFLICT envelope (mirrors deployment.go:785).
func TestEnvHandler_Set_DisabledTenant(t *testing.T) {
	mux := newEnvMux(&mockEnvSvc{setEnvErr: service.ErrTenantDisabled})

	body := `{"key":"LOG_LEVEL","value":"debug"}`
	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello/env", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
	assertConflictEnvelope(t, rr)
}

// TestEnvHandler_Delete_DisabledTenant: issue #560 — symmetric to
// the Set path. service.DeleteEnv returns ErrTenantDisabled, the
// handler must map it to 409.
func TestEnvHandler_Delete_DisabledTenant(t *testing.T) {
	mux := newEnvMux(&mockEnvSvc{deleteEnvErr: service.ErrTenantDisabled})

	req := httptest.NewRequest(http.MethodDelete, "/api/apps/hello/env/LOG_LEVEL", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
	assertConflictEnvelope(t, rr)
}

// assertConflictEnvelope decodes the canonical httperror envelope
// and confirms the {code, message} fields match CONFLICT / "tenant
// is disabled". Fails the test loudly if the envelope is malformed.
//
// Mirrors the unmarshal-via-generic-map shape used at
// deployment_activate_test.go:220 — keeps env_test.go free of any
// import cycle through httperror's struct types.
func assertConflictEnvelope(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	var got map[string]map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal envelope: %v (body=%q)", err, rr.Body.String())
	}
	if got["error"]["code"] != string(httperror.CodeConflict) {
		t.Errorf("error.code = %q, want %q; body: %s", got["error"]["code"], httperror.CodeConflict, rr.Body.String())
	}
	if got["error"]["message"] != "tenant is disabled" {
		t.Errorf("error.message = %q, want %q; body: %s", got["error"]["message"], "tenant is disabled", rr.Body.String())
	}
}
