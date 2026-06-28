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

type mockTrafficSvc struct {
	setTrafficErr error
	getTraffic    []*domain.TrafficSplit
	getTrafficErr error
}

func (m *mockTrafficSvc) SetTraffic(ctx context.Context, tenantID, appName string, entries []domain.TrafficSplitEntry) error {
	return m.setTrafficErr
}

func (m *mockTrafficSvc) GetTraffic(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error) {
	return m.getTraffic, m.getTrafficErr
}

func newTrafficMux(svc *mockTrafficSvc) *http.ServeMux {
	mux := http.NewServeMux()
	h := NewTrafficHandler(svc)
	mux.HandleFunc("PUT /api/v1/apps/{appName}/traffic", h.SetTraffic)
	mux.HandleFunc("GET /api/v1/apps/{appName}/traffic", h.GetTraffic)
	mux.HandleFunc("GET /api/v1/internal/traffic/{tenantID}/{appName}", h.GetTrafficInternal)
	return mux
}

func TestTrafficHandler_SetTraffic_Success(t *testing.T) {
	mux := newTrafficMux(&mockTrafficSvc{})

	body := `{"splits":[{"deployment_id":"d_1","weight":100}]}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/apps/hello/traffic", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["status"] != "traffic_set" {
		t.Errorf("status = %q, want 'traffic_set'", resp["status"])
	}
}

func TestTrafficHandler_SetTraffic_InvalidAppName(t *testing.T) {
	h := NewTrafficHandler(&mockTrafficSvc{})

	req := httptest.NewRequest(http.MethodPut, "/api/v1/apps/appname/traffic", strings.NewReader(`{"splits":[]}`))
	req.SetPathValue("appName", "..")
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	h.SetTraffic(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestTrafficHandler_SetTraffic_InvalidBody(t *testing.T) {
	mux := newTrafficMux(&mockTrafficSvc{})

	req := httptest.NewRequest(http.MethodPut, "/api/v1/apps/hello/traffic", strings.NewReader(`bad`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestTrafficHandler_GetTraffic_Success(t *testing.T) {
	svc := &mockTrafficSvc{
		getTraffic: []*domain.TrafficSplit{
			{DeploymentID: "d_1", Weight: 80},
			{DeploymentID: "d_2", Weight: 20},
		},
	}
	mux := newTrafficMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/apps/hello/traffic", nil)
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
	if resp["app_name"] != "hello" {
		t.Errorf("app_name = %v, want hello", resp["app_name"])
	}
}

func TestTrafficHandler_GetTrafficInternal_Success(t *testing.T) {
	svc := &mockTrafficSvc{
		getTraffic: []*domain.TrafficSplit{
			{DeploymentID: "d_1", Weight: 100},
		},
	}
	mux := newTrafficMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/traffic/t_test/hello", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
}

func TestTrafficHandler_GetTrafficInternal_InvalidAppName(t *testing.T) {
	h := NewTrafficHandler(&mockTrafficSvc{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/traffic/t_test/appname", nil)
	req.SetPathValue("tenantID", "t_test")
	req.SetPathValue("appName", "..")
	rr := httptest.NewRecorder()
	h.GetTrafficInternal(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestTrafficHandler_GetTrafficInternal_InvalidTenant(t *testing.T) {
	h := NewTrafficHandler(&mockTrafficSvc{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/traffic/t_/appname", nil)
	req.SetPathValue("tenantID", "..")
	req.SetPathValue("appName", "hello")
	rr := httptest.NewRecorder()
	h.GetTrafficInternal(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}
