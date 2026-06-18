package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
)

// mockTenantSvc implements service.TenantServiceInterface for testing.
type mockTenantSvc struct {
	bootstrapErr     error
	bootstrapTenant  *domain.Tenant
	bootstrapRawKey  string
	createTenantResp *domain.Tenant
	createTenantErr  error
	getTenantResp    *domain.TenantWithQuota
	getTenantErr     error
	listTenantsResp  []domain.Tenant
	listTenantsErr   error
	updateTenantErr  error
	deleteTenantErr  error
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
	if m.getTenantErr != nil {
		return nil, m.getTenantErr
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

func (m *mockTenantSvc) DeleteTenant(ctx context.Context, id string) error {
	return m.deleteTenantErr
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
