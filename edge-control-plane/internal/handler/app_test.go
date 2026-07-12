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
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

type mockAppSvc struct {
	createApp *domain.App
	createErr error
	listApps  []domain.App
	listErr   error
	getApp    *domain.App
	getErr    error
	updateApp *domain.App
	updateErr error
	deleteErr error
	// L4 port accessors (issue #548). getL4Port is the persisted
	// port (0 = unallocated). allocateL4Port mirrors what the service
	// returns; if unset, the mock echoes the input port.
	getL4Port      uint16
	getL4PortErr   error
	allocateL4Port uint16
	allocateL4Err  error
}

func (m *mockAppSvc) Create(ctx context.Context, tenantID, appName string, req *domain.CreateAppRequest) (*domain.App, error) {
	return m.createApp, m.createErr
}
func (m *mockAppSvc) List(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error) {
	return m.listApps, m.listErr
}
func (m *mockAppSvc) Get(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.getApp, nil
}
func (m *mockAppSvc) Update(ctx context.Context, tenantID, appName string, req *domain.UpdateAppRequest) (*domain.App, error) {
	return m.updateApp, m.updateErr
}
func (m *mockAppSvc) Delete(ctx context.Context, tenantID, appName string) error {
	return m.deleteErr
}
func (m *mockAppSvc) GetL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	return m.getL4Port, m.getL4PortErr
}
func (m *mockAppSvc) AllocateL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	return m.allocateL4Port, m.allocateL4Err
}

func newAppMux(svc *mockAppSvc) *http.ServeMux {
	mux := http.NewServeMux()
	h := &AppHandler{appSvc: svc}
	mux.HandleFunc("POST /api/apps/{appName}", h.Create)
	mux.HandleFunc("GET /api/apps", h.List)
	mux.HandleFunc("GET /api/apps/{appName}", h.Get)
	mux.HandleFunc("GET /api/v1/apps/{appName}/l4-port", h.GetL4Port)
	mux.HandleFunc("POST /api/v1/apps/{appName}/l4-port", h.AllocateL4Port)
	mux.HandleFunc("PUT /api/apps/{appName}", h.Update)
	mux.HandleFunc("DELETE /api/apps/{appName}", h.Delete)
	return mux
}

func TestAppHandler_Create_Success(t *testing.T) {
	svc := &mockAppSvc{createApp: &domain.App{ID: "a_1", TenantID: "t_test", Name: "hello"}}
	mux := newAppMux(svc)

	body := `{"description":"my-app"}`
	req := httptest.NewRequest(http.MethodPost, "/api/apps/hello", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	var app domain.App
	if err := json.Unmarshal(rr.Body.Bytes(), &app); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if app.Name != "hello" {
		t.Errorf("Name = %q, want hello", app.Name)
	}
}

func TestAppHandler_Create_InvalidBody(t *testing.T) {
	mux := newAppMux(&mockAppSvc{})

	req := httptest.NewRequest(http.MethodPost, "/api/apps/hello", strings.NewReader(`not json`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestAppHandler_Create_AlreadyExists(t *testing.T) {
	svc := &mockAppSvc{createErr: service.ErrAppAlreadyExists}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/apps/hello", strings.NewReader(`{}`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", rr.Code)
	}
}

func TestAppHandler_Create_QuotaExceeded(t *testing.T) {
	svc := &mockAppSvc{createErr: service.ErrMaxAppsQuotaExceeded}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/apps/hello", strings.NewReader(`{}`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rr.Code)
	}
}

func TestAppHandler_List(t *testing.T) {
	svc := &mockAppSvc{listApps: []domain.App{}}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["limit"] == nil || resp["offset"] == nil {
		t.Error("response missing limit/offset")
	}
}

func TestAppHandler_Get_Found(t *testing.T) {
	svc := &mockAppSvc{getApp: &domain.App{ID: "a_1", TenantID: "t_test", Name: "hello"}}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps/hello", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
}

func TestAppHandler_Get_NotFound(t *testing.T) {
	svc := &mockAppSvc{getApp: nil}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps/hello", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestAppHandler_Update_Success(t *testing.T) {
	desc := "updated description"
	svc := &mockAppSvc{
		updateApp: &domain.App{ID: "a_1", TenantID: "t_test", Name: "hello", Description: &desc},
	}
	mux := newAppMux(svc)

	body := `{"description":"updated description"}`
	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var app domain.App
	if err := json.Unmarshal(rr.Body.Bytes(), &app); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if app.Description == nil || *app.Description != "updated description" {
		t.Errorf("Description = %v, want 'updated description'", app.Description)
	}
}

func TestAppHandler_Update_ClearsDescription(t *testing.T) {
	empty := ""
	svc := &mockAppSvc{
		updateApp: &domain.App{ID: "a_1", TenantID: "t_test", Name: "hello", Description: &empty},
	}
	mux := newAppMux(svc)

	body := `{"description":""}`
	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var app domain.App
	if err := json.Unmarshal(rr.Body.Bytes(), &app); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if app.Description == nil || *app.Description != "" {
		t.Errorf("Description = %v, want empty string", app.Description)
	}
}

func TestAppHandler_Update_NotFound(t *testing.T) {
	svc := &mockAppSvc{updateErr: service.ErrAppNotFound}
	mux := newAppMux(svc)

	body := `{"description":"doesnt matter"}`
	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestAppHandler_Update_InvalidBody(t *testing.T) {
	mux := newAppMux(&mockAppSvc{})

	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello", strings.NewReader(`not json`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestAppHandler_Delete_Success(t *testing.T) {
	mux := newAppMux(&mockAppSvc{})

	req := httptest.NewRequest(http.MethodDelete, "/api/apps/hello", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rr.Code)
	}
}

func TestAppHandler_Delete_NotFound(t *testing.T) {
	svc := &mockAppSvc{deleteErr: service.ErrAppNotFound}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodDelete, "/api/apps/hello", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

// ── L4 port endpoints (issue #548) ────────────────────────────────────

func TestAppHandler_GetL4Port_Allocated(t *testing.T) {
	svc := &mockAppSvc{getL4Port: 31042}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp struct {
		PublicPort uint16 `json:"public_port"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.PublicPort != 31042 {
		t.Errorf("public_port = %d, want 31042", resp.PublicPort)
	}
}

func TestAppHandler_GetL4Port_Unallocated(t *testing.T) {
	// Mock returns (0, nil) — app exists but port is unset.
	svc := &mockAppSvc{getL4Port: 0}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestAppHandler_GetL4Port_AppNotFound(t *testing.T) {
	svc := &mockAppSvc{getL4PortErr: service.ErrAppNotFound}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestAppHandler_AllocateL4Port_HappyPath(t *testing.T) {
	svc := &mockAppSvc{allocateL4Port: 31042}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp struct {
		PublicPort uint16 `json:"public_port"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.PublicPort != 31042 {
		t.Errorf("public_port = %d, want 31042", resp.PublicPort)
	}
}

func TestAppHandler_AllocateL4Port_RangeExhausted(t *testing.T) {
	svc := &mockAppSvc{allocateL4Err: service.ErrL4PortRangeExhausted}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", rr.Code)
	}
}

func TestAppHandler_AllocateL4Port_AppNotFound(t *testing.T) {
	svc := &mockAppSvc{allocateL4Err: service.ErrAppNotFound}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}
