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

type mockAppRepo struct {
	rateLimit    *domain.AppRateLimit
	rateLimitErr error
}

func (m *mockAppRepo) GetRateLimit(ctx context.Context, tenantID, appName string) (*domain.AppRateLimit, error) {
	return m.rateLimit, m.rateLimitErr
}

func newTrafficMux(svc *mockTrafficSvc) *http.ServeMux {
	mux := http.NewServeMux()
	h := NewTrafficHandler(svc, nil)
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
	h := NewTrafficHandler(&mockTrafficSvc{}, nil)

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
	h := NewTrafficHandler(&mockTrafficSvc{}, nil)

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
	h := NewTrafficHandler(&mockTrafficSvc{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/traffic/t_/appname", nil)
	req.SetPathValue("tenantID", "..")
	req.SetPathValue("appName", "hello")
	rr := httptest.NewRecorder()
	h.GetTrafficInternal(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

// ── GetRateLimitsInternal tests ────────────────────────────────────────

func TestTrafficHandler_GetRateLimitsInternal_Success(t *testing.T) {
	repo := &mockAppRepo{
		rateLimit: &domain.AppRateLimit{RPS: 100, Burst: 200},
	}
	h := NewTrafficHandler(&mockTrafficSvc{}, repo)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/rate-limits/t_test/myapp", nil)
	req.SetPathValue("tenantID", "t_test")
	req.SetPathValue("appName", "myapp")
	rr := httptest.NewRecorder()
	h.GetRateLimitsInternal(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var rl domain.AppRateLimit
	if err := json.NewDecoder(rr.Body).Decode(&rl); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rl.RPS != 100 {
		t.Errorf("rps = %d, want 100", rl.RPS)
	}
	if rl.Burst != 200 {
		t.Errorf("burst = %d, want 200", rl.Burst)
	}
}

func TestTrafficHandler_GetRateLimitsInternal_ZeroValues(t *testing.T) {
	repo := &mockAppRepo{
		rateLimit: &domain.AppRateLimit{RPS: 0, Burst: 0},
	}
	h := NewTrafficHandler(&mockTrafficSvc{}, repo)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/rate-limits/t_test/myapp", nil)
	req.SetPathValue("tenantID", "t_test")
	req.SetPathValue("appName", "myapp")
	rr := httptest.NewRecorder()
	h.GetRateLimitsInternal(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var rl domain.AppRateLimit
	if err := json.NewDecoder(rr.Body).Decode(&rl); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rl.RPS != 0 || rl.Burst != 0 {
		t.Errorf("expected zero values, got rps=%d burst=%d", rl.RPS, rl.Burst)
	}
}

func TestTrafficHandler_GetRateLimitsInternal_NotFound(t *testing.T) {
	repo := &mockAppRepo{rateLimit: nil} // nil = no row
	h := NewTrafficHandler(&mockTrafficSvc{}, repo)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/rate-limits/t_test/myapp", nil)
	req.SetPathValue("tenantID", "t_test")
	req.SetPathValue("appName", "myapp")
	rr := httptest.NewRecorder()
	h.GetRateLimitsInternal(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestTrafficHandler_GetRateLimitsInternal_InvalidAppName(t *testing.T) {
	h := NewTrafficHandler(&mockTrafficSvc{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/rate-limits/t_test/appname", nil)
	req.SetPathValue("tenantID", "t_test")
	req.SetPathValue("appName", "..")
	rr := httptest.NewRecorder()
	h.GetRateLimitsInternal(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestTrafficHandler_GetRateLimitsInternal_InvalidTenant(t *testing.T) {
	h := NewTrafficHandler(&mockTrafficSvc{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/rate-limits/t_/appname", nil)
	req.SetPathValue("tenantID", "..")
	req.SetPathValue("appName", "hello")
	rr := httptest.NewRecorder()
	h.GetRateLimitsInternal(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestTrafficHandler_GetRateLimitsInternal_DBError(t *testing.T) {
	repo := &mockAppRepo{rateLimitErr: context.DeadlineExceeded}
	h := NewTrafficHandler(&mockTrafficSvc{}, repo)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/rate-limits/t_test/myapp", nil)
	req.SetPathValue("tenantID", "t_test")
	req.SetPathValue("appName", "myapp")
	rr := httptest.NewRecorder()
	h.GetRateLimitsInternal(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}
