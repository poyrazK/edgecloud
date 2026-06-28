package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

type mockMetricsAgg struct {
	allStr    string
	tenantStr string
}

func (m *mockMetricsAgg) RenderAll() string        { return m.allStr }
func (m *mockMetricsAgg) RenderTenant(id string) string { return m.tenantStr }

func TestMetricsHandler_GetAllMetrics(t *testing.T) {
	h := NewMetricsHandler(&mockMetricsAgg{allStr: "# HELP test_metric\n"})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	h.GetAllMetrics(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "text/plain; version=0.0.4; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	if rr.Body.String() != "# HELP test_metric\n" {
		t.Errorf("body = %q", rr.Body.String())
	}
}

func TestMetricsHandler_GetTenantMetrics_WithAuth(t *testing.T) {
	h := NewMetricsHandler(&mockMetricsAgg{tenantStr: "tenant_metric 1\n"})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	h.GetTenantMetrics(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if rr.Body.String() != "tenant_metric 1\n" {
		t.Errorf("body = %q", rr.Body.String())
	}
}

func TestMetricsHandler_GetTenantMetrics_NoTenant(t *testing.T) {
	h := NewMetricsHandler(&mockMetricsAgg{tenantStr: "x"})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil)
	rr := httptest.NewRecorder()
	h.GetTenantMetrics(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}
