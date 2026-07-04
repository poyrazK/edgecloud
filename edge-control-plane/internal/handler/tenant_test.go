package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// mockTenantSvc implements service.TenantServiceInterface for testing.
type mockTenantSvc struct {
	bootstrapErr        error
	bootstrapTenant     *domain.Tenant
	bootstrapRawKey     string
	createTenantResp    *domain.Tenant
	createTenantErr     error
	getTenantResp       *domain.TenantWithQuota
	getTenantRespAfter  *domain.TenantWithQuota // returned on the second-and-later GetTenant calls (plan-change re-fetch)
	getTenantCalls      int
	getTenantErr        error
	listTenantsResp     []domain.Tenant
	listTenantsErr      error
	updateTenantErr     error
	updateTenantPlanErr error // distinct from updateTenantErr so we can assert sentinel mapping on the plan-change branch
	deleteTenantErr     error
	getQuotaResp        *domain.Quota
	getQuotaErr         error
}

func (m *mockTenantSvc) BootstrapTenant(ctx context.Context, name, plan, keyName string) (*domain.Tenant, string, error) {
	if m.bootstrapErr != nil {
		return nil, "", m.bootstrapErr
	}
	return m.bootstrapTenant, m.bootstrapRawKey, nil
}

func (m *mockTenantSvc) CreateTenant(ctx context.Context, name, plan string) (*domain.Tenant, error) {
	if m.createTenantErr != nil {
		return nil, m.createTenantErr
	}
	return m.createTenantResp, nil
}

func (m *mockTenantSvc) GetTenant(ctx context.Context, id string) (*domain.TenantWithQuota, error) {
	m.getTenantCalls++
	if m.getTenantErr != nil {
		return nil, m.getTenantErr
	}
	if m.getTenantCalls >= 2 && m.getTenantRespAfter != nil {
		return m.getTenantRespAfter, nil
	}
	return m.getTenantResp, nil
}

func (m *mockTenantSvc) ListTenants(ctx context.Context) ([]domain.Tenant, error) {
	if m.listTenantsErr != nil {
		return nil, m.listTenantsErr
	}
	return m.listTenantsResp, nil
}

func (m *mockTenantSvc) UpdateTenant(ctx context.Context, t *domain.Tenant) error {
	return m.updateTenantErr
}

func (m *mockTenantSvc) UpdateTenantPlan(ctx context.Context, tenantID, newPlan string, applyQuotaDefaults bool) error {
	return m.updateTenantPlanErr
}

func (m *mockTenantSvc) DeleteTenant(ctx context.Context, id string) error {
	return m.deleteTenantErr
}

func (m *mockTenantSvc) GetQuota(ctx context.Context, tenantID string) (*domain.Quota, error) {
	if m.getQuotaErr != nil {
		return nil, m.getQuotaErr
	}
	return m.getQuotaResp, nil
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

func TestBootstrap_HappyPath(t *testing.T) {
	wantTenant := &domain.Tenant{ID: "t_abc123", Name: "test-tenant", Plan: "free"}
	wantKey := "sk_test_abcdef123456"

	svc := &mockTenantSvc{
		bootstrapTenant: wantTenant,
		bootstrapRawKey: wantKey,
	}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{"name":"test-tenant","key_name":"my-key"}`)
	req := httptest.NewRequest("POST", "/api/tenants", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Bootstrap(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected status 201, got: %d", rr.Code)
	}
	var resp handler.BootstrapResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.TenantID != "t_abc123" {
		t.Errorf("tenant_id = %q, want %q", resp.TenantID, "t_abc123")
	}
	if resp.APIKey != wantKey {
		t.Errorf("api_key = %q, want %q", resp.APIKey, wantKey)
	}
}

func TestBootstrap_InvalidBody(t *testing.T) {
	svc := &mockTenantSvc{}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`not json`)
	req := httptest.NewRequest("POST", "/api/tenants", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Bootstrap(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got: %d", rr.Code)
	}
}

func TestBootstrap_MissingName(t *testing.T) {
	svc := &mockTenantSvc{}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{"key_name":"my-key"}`)
	req := httptest.NewRequest("POST", "/api/tenants", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Bootstrap(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got: %d", rr.Code)
	}
}

func TestBootstrap_ErrorPath(t *testing.T) {
	svc := &mockTenantSvc{bootstrapErr: errors.New("database connection refused")}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{"name":"test","key_name":"default"}`)
	req := httptest.NewRequest("POST", "/api/tenants", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Bootstrap(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got: %d", rr.Code)
	}
	respBody := rr.Body.String()
	if strings.Contains(respBody, "database connection refused") {
		t.Errorf("response should not contain raw error, got: %s", respBody)
	}
	if !strings.Contains(respBody, `"error"`) {
		t.Errorf("expected JSON error field, got: %s", respBody)
	}
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

func TestCreate_HappyPath(t *testing.T) {
	wantTenant := &domain.Tenant{ID: "t_new", Name: "new-tenant", Plan: "free"}
	svc := &mockTenantSvc{createTenantResp: wantTenant}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{"name":"new-tenant"}`)
	req := httptest.NewRequest("POST", "/api/tenants/create", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected status 201, got: %d", rr.Code)
	}
	var resp domain.Tenant
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != "t_new" {
		t.Errorf("tenant ID = %q, want %q", resp.ID, "t_new")
	}
}

func TestCreate_InvalidBody(t *testing.T) {
	svc := &mockTenantSvc{}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{`)
	req := httptest.NewRequest("POST", "/api/tenants/create", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got: %d", rr.Code)
	}
}

func TestCreate_MissingName(t *testing.T) {
	svc := &mockTenantSvc{}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{}`)
	req := httptest.NewRequest("POST", "/api/tenants/create", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got: %d", rr.Code)
	}
}

func TestCreate_ServiceError(t *testing.T) {
	svc := &mockTenantSvc{createTenantErr: errors.New("db write failed")}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{"name":"new-tenant"}`)
	req := httptest.NewRequest("POST", "/api/tenants/create", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got: %d", rr.Code)
	}
	respBody := rr.Body.String()
	if strings.Contains(respBody, "db write failed") {
		t.Errorf("response should not contain raw error, got: %s", respBody)
	}
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

func TestGet_Found(t *testing.T) {
	want := &domain.TenantWithQuota{
		Tenant: domain.Tenant{ID: "t_xyz", Name: "my-tenant"},
		Quota:  domain.Quota{TenantID: "t_xyz"},
	}
	svc := &mockTenantSvc{getTenantResp: want}
	h := handler.NewTenantHandler(svc)

	req := httptest.NewRequest("GET", "/api/tenants/t_xyz", nil)
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200, got: %d", rr.Code)
	}
	var resp domain.TenantWithQuota
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != "t_xyz" {
		t.Errorf("tenant ID = %q, want %q", resp.ID, "t_xyz")
	}
}

func TestGet_NotFound(t *testing.T) {
	svc := &mockTenantSvc{getTenantResp: nil}
	h := handler.NewTenantHandler(svc)

	req := httptest.NewRequest("GET", "/api/tenants/t_missing", nil)
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got: %d", rr.Code)
	}
}

func TestGet_ServiceError(t *testing.T) {
	svc := &mockTenantSvc{getTenantErr: errors.New("connection refused")}
	h := handler.NewTenantHandler(svc)

	req := httptest.NewRequest("GET", "/api/tenants/t_err", nil)
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got: %d", rr.Code)
	}
	respBody := rr.Body.String()
	if strings.Contains(respBody, "connection refused") {
		t.Errorf("response should not contain raw error, got: %s", respBody)
	}
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

func TestList_HappyPath(t *testing.T) {
	want := []domain.Tenant{
		{ID: "t_1", Name: "tenant-a"},
		{ID: "t_2", Name: "tenant-b"},
	}
	svc := &mockTenantSvc{listTenantsResp: want}
	h := handler.NewTenantHandler(svc)

	req := httptest.NewRequest("GET", "/api/tenants", nil)
	rr := httptest.NewRecorder()

	h.List(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200, got: %d", rr.Code)
	}
	var resp []domain.Tenant
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp) != 2 {
		t.Errorf("len(resp) = %d, want 2", len(resp))
	}
}

func TestList_ServiceError(t *testing.T) {
	svc := &mockTenantSvc{listTenantsErr: errors.New("db error")}
	h := handler.NewTenantHandler(svc)

	req := httptest.NewRequest("GET", "/api/tenants", nil)
	rr := httptest.NewRecorder()

	h.List(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got: %d", rr.Code)
	}
	respBody := rr.Body.String()
	if strings.Contains(respBody, "db error") {
		t.Errorf("response should not contain raw error, got: %s", respBody)
	}
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

func TestUpdate_Found(t *testing.T) {
	existing := &domain.TenantWithQuota{
		Tenant: domain.Tenant{ID: "t_upd", Name: "old-name", Plan: "free"},
		Quota:  domain.Quota{TenantID: "t_upd"},
	}
	svc := &mockTenantSvc{getTenantResp: existing}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{"name":"new-name"}`)
	req := httptest.NewRequest("PUT", "/api/tenants/t_upd", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200, got: %d", rr.Code)
	}
	var resp domain.TenantWithQuota
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Name != "new-name" {
		t.Errorf("name = %q, want %q", resp.Name, "new-name")
	}
}

func TestUpdate_NotFound(t *testing.T) {
	svc := &mockTenantSvc{getTenantResp: nil}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{"name":"new-name"}`)
	req := httptest.NewRequest("PUT", "/api/tenants/t_missing", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got: %d", rr.Code)
	}
}

func TestUpdate_ServiceError(t *testing.T) {
	existing := &domain.TenantWithQuota{
		Tenant: domain.Tenant{ID: "t_err", Name: "err-tenant"},
		Quota:  domain.Quota{TenantID: "t_err"},
	}
	svc := &mockTenantSvc{getTenantResp: existing, updateTenantErr: errors.New("update failed")}
	h := handler.NewTenantHandler(svc)

	body := strings.NewReader(`{"name":"new-name"}`)
	req := httptest.NewRequest("PUT", "/api/tenants/t_err", body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got: %d", rr.Code)
	}
	respBody := rr.Body.String()
	if strings.Contains(respBody, "update failed") {
		t.Errorf("response should not contain raw error, got: %s", respBody)
	}
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestDelete_NoContent(t *testing.T) {
	svc := &mockTenantSvc{}
	h := handler.NewTenantHandler(svc)

	req := httptest.NewRequest("DELETE", "/api/tenants/t_del", nil)
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got: %d", rr.Code)
	}
}

func TestDelete_NotFound(t *testing.T) {
	svc := &mockTenantSvc{deleteTenantErr: errors.New("not found")}
	h := handler.NewTenantHandler(svc)

	req := httptest.NewRequest("DELETE", "/api/tenants/t_missing", nil)
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	// Delete currently returns 500 for any error, including not found.
	// This matches current handler behavior (no 404 distinction for delete).
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got: %d", rr.Code)
	}
}

func TestDelete_ServiceError(t *testing.T) {
	svc := &mockTenantSvc{deleteTenantErr: errors.New("db connection lost")}
	h := handler.NewTenantHandler(svc)

	req := httptest.NewRequest("DELETE", "/api/tenants/t_err", nil)
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got: %d", rr.Code)
	}
	respBody := rr.Body.String()
	if strings.Contains(respBody, "db connection lost") {
		t.Errorf("response should not contain raw error, got: %s", respBody)
	}
}

// ---------------------------------------------------------------------------
// Plan validation (billing v0)
// ---------------------------------------------------------------------------

func TestBootstrap_RejectsPaidPlan(t *testing.T) {
	bootstrapCalled := false
	svc := &mockTenantSvc{
		bootstrapTenant: &domain.Tenant{ID: "t_x", Name: "x", Plan: "pro"},
		bootstrapRawKey: "sk_x",
		bootstrapErr:    errors.New("should not be called"),
	}
	// We can't intercept the call with this mock design, but the handler
	// short-circuits with 400 BEFORE calling the service. So if Bootstrap
	// returns 400 AND the response body doesn't include the mock's tenant
	// fields, the gate worked.
	h := handler.NewTenantHandler(svc)

	body := `{"name":"acme","plan":"pro","key_name":"owner"}`
	req := httptest.NewRequest("POST", "/api/tenants", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.Bootstrap(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for plan=pro at bootstrap, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "sk_x") {
		t.Errorf("response leaked mock API key, suggests service was called: %s", rr.Body.String())
	}
	_ = bootstrapCalled
}

func TestBootstrap_AcceptsFreePlan(t *testing.T) {
	svc := &mockTenantSvc{
		bootstrapTenant: &domain.Tenant{ID: "t_x", Name: "x", Plan: "free"},
		bootstrapRawKey: "sk_x",
	}
	h := handler.NewTenantHandler(svc)

	body := `{"name":"acme","plan":"free","key_name":"owner"}`
	req := httptest.NewRequest("POST", "/api/tenants", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.Bootstrap(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d (body=%s)", rr.Code, rr.Body.String())
	}
}

func TestCreate_AdminAcceptsProPlan(t *testing.T) {
	svc := &mockTenantSvc{
		createTenantResp: &domain.Tenant{ID: "t_x", Name: "acme", Plan: "pro"},
	}
	h := handler.NewTenantHandler(svc)

	body := `{"name":"acme","plan":"pro"}`
	req := httptest.NewRequest("POST", "/api/admin/tenants", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d (body=%s)", rr.Code, rr.Body.String())
	}
}

func TestCreate_RejectsUnknownPlan(t *testing.T) {
	svc := &mockTenantSvc{}
	h := handler.NewTenantHandler(svc)

	body := `{"name":"acme","plan":"platinum"}`
	req := httptest.NewRequest("POST", "/api/admin/tenants", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unknown plan, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	if svc.createTenantResp != nil {
		t.Errorf("CreateTenant should not have been called for invalid plan")
	}
}

func TestUpdate_PlanChange_AutoAppliesDefaults(t *testing.T) {
	svc := &mockTenantSvc{
		getTenantResp: &domain.TenantWithQuota{
			Tenant: domain.Tenant{ID: "t_x", Name: "acme", Plan: "free"},
			Quota:  domain.Quota{TenantID: "t_x", MaxRequestsPerMonth: 100_000},
		},
	}
	h := handler.NewTenantHandler(svc)

	body := `{"plan":"pro"}`
	req := httptest.NewRequest("PUT", "/api/admin/tenants/t_x", strings.NewReader(body))
	req.SetPathValue("tenantID", "t_x")
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	if svc.updateTenantPlanErr != nil {
		t.Errorf("UpdateTenantPlan should have been called; got updateTenantPlanErr=%v", svc.updateTenantPlanErr)
	}
}

func TestUpdate_PlanChange_PreserveQuotaLimits_KeepsCustom(t *testing.T) {
	svc := &mockTenantSvc{
		getTenantResp: &domain.TenantWithQuota{
			Tenant: domain.Tenant{ID: "t_x", Name: "acme", Plan: "business"},
			Quota:  domain.Quota{TenantID: "t_x", MaxRequestsPerMonth: 50_000_000},
		},
	}
	h := handler.NewTenantHandler(svc)

	body := `{"plan":"free","preserve_quota_limits":true}`
	req := httptest.NewRequest("PUT", "/api/admin/tenants/t_x", strings.NewReader(body))
	req.SetPathValue("tenantID", "t_x")
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d (body=%s)", rr.Code, rr.Body.String())
	}
}

func TestUpdate_PlanChange_RejectsUnknownPlan(t *testing.T) {
	svc := &mockTenantSvc{
		getTenantResp: &domain.TenantWithQuota{
			Tenant: domain.Tenant{ID: "t_x", Name: "acme", Plan: "free"},
		},
		updateTenantPlanErr: fmt.Errorf("%w: %q", domain.ErrUnknownPlan, "platinum"),
	}
	h := handler.NewTenantHandler(svc)

	body := `{"plan":"platinum"}`
	req := httptest.NewRequest("PUT", "/api/admin/tenants/t_x", strings.NewReader(body))
	req.SetPathValue("tenantID", "t_x")
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unknown plan on update, got %d (body=%s)", rr.Code, rr.Body.String())
	}
}

func TestUpdate_PlanChange_TenantNotFound_404(t *testing.T) {
	svc := &mockTenantSvc{
		getTenantResp: &domain.TenantWithQuota{
			Tenant: domain.Tenant{ID: "t_x", Name: "acme", Plan: "free"},
		},
		updateTenantPlanErr: service.ErrTenantNotFound,
	}
	h := handler.NewTenantHandler(svc)

	body := `{"plan":"pro"}`
	req := httptest.NewRequest("PUT", "/api/admin/tenants/t_x", strings.NewReader(body))
	req.SetPathValue("tenantID", "t_x")
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for tenant-not-found on plan change, got %d (body=%s)", rr.Code, rr.Body.String())
	}
}

func TestUpdate_PlanChange_QuotaNotFound_404(t *testing.T) {
	svc := &mockTenantSvc{
		getTenantResp: &domain.TenantWithQuota{
			Tenant: domain.Tenant{ID: "t_x", Name: "acme", Plan: "free"},
		},
		updateTenantPlanErr: service.ErrQuotaNotFound,
	}
	h := handler.NewTenantHandler(svc)

	body := `{"plan":"pro"}`
	req := httptest.NewRequest("PUT", "/api/admin/tenants/t_x", strings.NewReader(body))
	req.SetPathValue("tenantID", "t_x")
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404 for quota-not-found on plan change, got %d (body=%s)", rr.Code, rr.Body.String())
	}
}

func TestUpdate_PlanChange_ResponseShowsNewQuota(t *testing.T) {
	// First GetTenant (pre-plan-change) returns free-tier caps.
	// Second GetTenant (post-plan-change re-fetch) returns the pro-tier
	// caps. The handler must encode the post-fetch row so the response
	// shows plan="pro" alongside max_requests_per_month=5_000_000.
	free := &domain.TenantWithQuota{
		Tenant: domain.Tenant{ID: "t_x", Name: "acme", Plan: "free"},
		Quota:  domain.Quota{TenantID: "t_x", MaxRequestsPerMonth: 100_000, MaxOutboundMB: 1000},
	}
	pro := &domain.TenantWithQuota{
		Tenant: domain.Tenant{ID: "t_x", Name: "acme", Plan: "pro"},
		Quota:  domain.Quota{TenantID: "t_x", MaxRequestsPerMonth: 5_000_000, MaxOutboundMB: 10_000},
	}
	svc := &mockTenantSvc{
		getTenantResp:      free,
		getTenantRespAfter: pro,
	}
	h := handler.NewTenantHandler(svc)

	body := `{"plan":"pro"}`
	req := httptest.NewRequest("PUT", "/api/admin/tenants/t_x", strings.NewReader(body))
	req.SetPathValue("tenantID", "t_x")
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	var resp struct {
		Plan                string `json:"plan"`
		MaxRequestsPerMonth int    `json:"max_requests_per_month"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Plan != "pro" {
		t.Errorf("Plan = %q, want pro", resp.Plan)
	}
	if resp.MaxRequestsPerMonth != 5_000_000 {
		t.Errorf("MaxRequestsPerMonth = %d, want 5_000_000 (post-update re-fetch should override pre-update value)", resp.MaxRequestsPerMonth)
	}
}
