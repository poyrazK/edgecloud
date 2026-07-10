package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// mockTenantSvc is a minimal mock for service.TenantServiceInterface —
// just the GetTenant method used by Whoami. Other methods panic if called
// so we notice if the handler starts using them.
type mockAuthTenantSvc struct {
	getTenantResp *domain.TenantWithQuota
	getTenantErr  error
}

func (m *mockAuthTenantSvc) GetTenant(ctx context.Context, id string) (*domain.TenantWithQuota, error) {
	if m.getTenantErr != nil {
		return nil, m.getTenantErr
	}
	return m.getTenantResp, nil
}

func (m *mockAuthTenantSvc) BootstrapTenant(ctx context.Context, name, plan, keyName string) (*domain.Tenant, string, error) {
	panic("not used by Whoami")
}
func (m *mockAuthTenantSvc) CreateTenant(ctx context.Context, name, plan string) (*domain.Tenant, error) {
	panic("not used by Whoami")
}
func (m *mockAuthTenantSvc) ListTenants(ctx context.Context) ([]domain.Tenant, error) {
	panic("not used by Whoami")
}
func (m *mockAuthTenantSvc) UpdateTenant(ctx context.Context, t *domain.Tenant) error {
	panic("not used by Whoami")
}
func (m *mockAuthTenantSvc) UpdateTenantPlan(ctx context.Context, tenantID, newPlan string, applyQuotaDefaults bool) error {
	panic("not used by Whoami")
}
func (m *mockAuthTenantSvc) DeleteTenant(ctx context.Context, id string) error {
	panic("not used by Whoami")
}

func (m *mockAuthTenantSvc) GetQuota(ctx context.Context, tenantID string) (*domain.Quota, error) {
	panic("not used by Whoami")
}

func (m *mockAuthTenantSvc) GetQuotaForInternal(ctx context.Context, tenantID string) (*domain.Quota, error) {
	panic("not used by Whoami")
}

func (m *mockAuthTenantSvc) OverrideTenantQuota(ctx context.Context, req service.QuotaOverrideRequest) (*domain.TenantWithQuota, error) {
	panic("not used by Whoami")
}

// mockAPIKeySvc is a minimal mock for service.APIKeyServiceInterface —
// just the GetByID method used by Whoami.
type mockAPIKeySvc struct {
	getByIDResp *domain.APIKey
	getByIDErr  error
}

func (m *mockAPIKeySvc) GetByID(ctx context.Context, id string) (*domain.APIKey, error) {
	if m.getByIDErr != nil {
		return nil, m.getByIDErr
	}
	return m.getByIDResp, nil
}

func (m *mockAPIKeySvc) CreateAPIKey(ctx context.Context, tenantID, name, role string, ttlHours *int) (*domain.APIKey, string, error) {
	panic("not used by Whoami")
}
func (m *mockAPIKeySvc) ListAPIKeys(ctx context.Context, tenantID string) ([]domain.APIKey, error) {
	panic("not used by Whoami")
}
func (m *mockAPIKeySvc) DeleteAPIKey(ctx context.Context, tenantID, id string) error {
	panic("not used by Whoami")
}
func (m *mockAPIKeySvc) UpdateAPIKey(ctx context.Context, id, tenantID string, req *domain.UpdateAPIKeyRequest) (*domain.APIKey, error) {
	panic("not used by Whoami")
}
func (m *mockAPIKeySvc) RotateAPIKey(ctx context.Context, tenantID, id string) (*domain.APIKey, string, error) {
	panic("not used by Whoami")
}

// contextWithAuth simulates what AuthMiddleware injects after a successful
// Bearer token validation.
func contextWithAuth(ctx context.Context, tenantID, keyID, role string) context.Context {
	ctx = middleware.WithTenantID(ctx, tenantID)
	ctx = middleware.WithAPIKeyID(ctx, keyID)
	ctx = middleware.WithRole(ctx, role)
	return ctx
}

func TestWhoami_HappyPath(t *testing.T) {
	tenant := &domain.TenantWithQuota{
		Tenant: domain.Tenant{
			ID:        "t_abc",
			Name:      "acme",
			Plan:      "free",
			CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		},
	}
	key := &domain.APIKey{
		ID:       "k_def",
		TenantID: "t_abc",
		Name:     "default",
		Role:     "owner",
	}
	tenantSvc := &mockAuthTenantSvc{getTenantResp: tenant}
	apiKeySvc := &mockAPIKeySvc{getByIDResp: key}
	h := handler.NewAuthHandler(tenantSvc, apiKeySvc)

	req := httptest.NewRequest("GET", "/api/auth/whoami", nil)
	req = req.WithContext(contextWithAuth(req.Context(), "t_abc", "k_def", "owner"))
	rr := httptest.NewRecorder()

	h.Whoami(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got: %d (body: %s)", rr.Code, rr.Body.String())
	}
	var resp handler.WhoamiResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.TenantID != "t_abc" {
		t.Errorf("tenant_id = %q, want %q", resp.TenantID, "t_abc")
	}
	if resp.TenantName != "acme" {
		t.Errorf("tenant_name = %q, want %q", resp.TenantName, "acme")
	}
	if resp.Plan != "free" {
		t.Errorf("plan = %q, want %q", resp.Plan, "free")
	}
	if resp.APIKeyID != "k_def" {
		t.Errorf("api_key_id = %q, want %q", resp.APIKeyID, "k_def")
	}
	if resp.APIKeyName != "default" {
		t.Errorf("api_key_name = %q, want %q", resp.APIKeyName, "default")
	}
	if resp.Role != "owner" {
		t.Errorf("role = %q, want %q", resp.Role, "owner")
	}
	if resp.CreatedAt != "2026-01-02T03:04:05Z" {
		t.Errorf("created_at = %q, want %q", resp.CreatedAt, "2026-01-02T03:04:05Z")
	}
}

func TestWhoami_TenantNotFound(t *testing.T) {
	tenantSvc := &mockAuthTenantSvc{getTenantResp: nil}
	apiKeySvc := &mockAPIKeySvc{}
	h := handler.NewAuthHandler(tenantSvc, apiKeySvc)

	req := httptest.NewRequest("GET", "/api/auth/whoami", nil)
	req = req.WithContext(contextWithAuth(req.Context(), "t_missing", "k_x", "owner"))
	rr := httptest.NewRecorder()

	h.Whoami(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got: %d", rr.Code)
	}
}

func TestWhoami_KeyNotFound(t *testing.T) {
	tenant := &domain.TenantWithQuota{
		Tenant: domain.Tenant{ID: "t_abc", Name: "acme"},
	}
	tenantSvc := &mockAuthTenantSvc{getTenantResp: tenant}
	apiKeySvc := &mockAPIKeySvc{getByIDResp: nil} // key vanished
	h := handler.NewAuthHandler(tenantSvc, apiKeySvc)

	req := httptest.NewRequest("GET", "/api/auth/whoami", nil)
	req = req.WithContext(contextWithAuth(req.Context(), "t_abc", "k_missing", "owner"))
	rr := httptest.NewRecorder()

	h.Whoami(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got: %d", rr.Code)
	}
}

func TestWhoami_TenantServiceError(t *testing.T) {
	tenantSvc := &mockAuthTenantSvc{getTenantErr: errors.New("db connection refused")}
	apiKeySvc := &mockAPIKeySvc{}
	h := handler.NewAuthHandler(tenantSvc, apiKeySvc)

	req := httptest.NewRequest("GET", "/api/auth/whoami", nil)
	req = req.WithContext(contextWithAuth(req.Context(), "t_abc", "k_def", "owner"))
	rr := httptest.NewRecorder()

	h.Whoami(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got: %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"error"`) {
		t.Errorf("expected JSON error field, got: %s", body)
	}
	if strings.Contains(body, "db connection refused") {
		t.Errorf("response should not leak raw error, got: %s", body)
	}
}

func TestWhoami_APIKeyServiceError(t *testing.T) {
	tenant := &domain.TenantWithQuota{
		Tenant: domain.Tenant{ID: "t_abc", Name: "acme"},
	}
	tenantSvc := &mockAuthTenantSvc{getTenantResp: tenant}
	apiKeySvc := &mockAPIKeySvc{getByIDErr: errors.New("db write failed")}
	h := handler.NewAuthHandler(tenantSvc, apiKeySvc)

	req := httptest.NewRequest("GET", "/api/auth/whoami", nil)
	req = req.WithContext(contextWithAuth(req.Context(), "t_abc", "k_def", "owner"))
	rr := httptest.NewRecorder()

	h.Whoami(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got: %d", rr.Code)
	}
}

func TestWhoami_MissingContext(t *testing.T) {
	// Defensive: if the handler is somehow mounted without the auth
	// middleware in front of it, it should not silently return data.
	tenantSvc := &mockAuthTenantSvc{}
	apiKeySvc := &mockAPIKeySvc{}
	h := handler.NewAuthHandler(tenantSvc, apiKeySvc)

	req := httptest.NewRequest("GET", "/api/auth/whoami", nil)
	rr := httptest.NewRecorder()

	h.Whoami(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 when context is empty, got: %d", rr.Code)
	}
}
