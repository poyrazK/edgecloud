package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

type mockEgressTenantSvc struct {
	list    []string
	listErr error
	updateErr error
}

func (m *mockEgressTenantSvc) GetEgressAllowlist(ctx context.Context, tenantID string) ([]string, error) {
	return m.list, m.listErr
}
func (m *mockEgressTenantSvc) UpdateEgressAllowlist(ctx context.Context, tenantID string, allowlist []string) error {
	return m.updateErr
}

type mockEgressDeploymentSvc struct {
	republishErr error
}

func (m *mockEgressDeploymentSvc) RepublishActiveDeployments(ctx context.Context, tenantID string) error {
	return m.republishErr
}

func newEgressMux(tenantSvc *mockEgressTenantSvc, deploySvc *mockEgressDeploymentSvc) *http.ServeMux {
	mux := http.NewServeMux()
	h := &EgressHandler{tenantSvc: tenantSvc, deploymentSvc: deploySvc}
	mux.HandleFunc("GET /api/egress", h.Get)
	mux.HandleFunc("PUT /api/egress", h.Update)
	return mux
}

func TestEgressHandler_Get_Success(t *testing.T) {
	svc := &mockEgressTenantSvc{list: []string{"*.example.com", "api.internal"}}
	mux := newEgressMux(svc, &mockEgressDeploymentSvc{})

	req := httptest.NewRequest(http.MethodGet, "/api/egress", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp egressResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Allowlist) != 2 {
		t.Errorf("Allowlist len = %d, want 2", len(resp.Allowlist))
	}
}

func TestEgressHandler_Get_Empty(t *testing.T) {
	svc := &mockEgressTenantSvc{list: []string{}}
	mux := newEgressMux(svc, &mockEgressDeploymentSvc{})

	req := httptest.NewRequest(http.MethodGet, "/api/egress", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp egressResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if len(resp.Allowlist) != 0 {
		t.Errorf("Allowlist len = %d, want 0", len(resp.Allowlist))
	}
}

func TestEgressHandler_Update_Success(t *testing.T) {
	mux := newEgressMux(&mockEgressTenantSvc{}, &mockEgressDeploymentSvc{})

	body := `{"allowlist":["*.example.com"]}`
	req := httptest.NewRequest(http.MethodPut, "/api/egress", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
}

func TestEgressHandler_Update_InvalidBody(t *testing.T) {
	mux := newEgressMux(&mockEgressTenantSvc{}, &mockEgressDeploymentSvc{})

	req := httptest.NewRequest(http.MethodPut, "/api/egress", strings.NewReader(`bad`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestEgressHandler_Update_ValidationError(t *testing.T) {
	valErr := &service.EgressValidationError{}
	svc := &mockEgressTenantSvc{updateErr: valErr}
	mux := newEgressMux(svc, &mockEgressDeploymentSvc{})

	body := `{"allowlist":["bad-cidr"]}`
	req := httptest.NewRequest(http.MethodPut, "/api/egress", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
}

func TestEgressHandler_Update_RepublishFails(t *testing.T) {
	mux := newEgressMux(&mockEgressTenantSvc{}, &mockEgressDeploymentSvc{republishErr: context.DeadlineExceeded})

	body := `{"allowlist":["*.example.com"]}`
	req := httptest.NewRequest(http.MethodPut, "/api/egress", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}
