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
	deleteErr error
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
func (m *mockAppSvc) Delete(ctx context.Context, tenantID, appName string) error {
	return m.deleteErr
}

func newAppMux(svc *mockAppSvc) *http.ServeMux {
	mux := http.NewServeMux()
	h := &AppHandler{appSvc: svc}
	mux.HandleFunc("POST /api/apps/{appName}", h.Create)
	mux.HandleFunc("GET /api/apps", h.List)
	mux.HandleFunc("GET /api/apps/{appName}", h.Get)
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
