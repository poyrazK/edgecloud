package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// mockAPIKeyCreateSvc is a minimal mock for service.APIKeyServiceInterface —
// only the CreateAPIKey method is exercised. Other methods panic so we notice
// if the handler starts using them.
type mockAPIKeyCreateSvc struct {
	createCalls []callCreate
	createResp  *domain.APIKey
	createRaw   string
	createErr   error
}

type callCreate struct {
	tenantID string
	name     string
	role     string
	ttlHours *int
}

func (m *mockAPIKeyCreateSvc) CreateAPIKey(_ context.Context, tenantID, name, role string, ttlHours *int) (*domain.APIKey, string, error) {
	m.createCalls = append(m.createCalls, callCreate{tenantID, name, role, ttlHours})
	if m.createErr != nil {
		return nil, "", m.createErr
	}
	return m.createResp, m.createRaw, nil
}

func (m *mockAPIKeyCreateSvc) ListAPIKeys(_ context.Context, _ string) ([]domain.APIKey, error) {
	panic("not used by Create")
}
func (m *mockAPIKeyCreateSvc) GetByID(_ context.Context, _ string) (*domain.APIKey, error) {
	panic("not used by Create")
}
func (m *mockAPIKeyCreateSvc) DeleteAPIKey(_ context.Context, _, _ string) error {
	panic("not used by Create")
}
func (m *mockAPIKeyCreateSvc) UpdateAPIKey(_ context.Context, _, _ string, _ *domain.UpdateAPIKeyRequest) (*domain.APIKey, error) {
	panic("not used by Create")
}
func (m *mockAPIKeyCreateSvc) RotateAPIKey(_ context.Context, _, _ string) (*domain.APIKey, string, error) {
	panic("not used by Create")
}

func TestCreateAPIKey_HappyPath(t *testing.T) {
	svc := &mockAPIKeyCreateSvc{
		createResp: &domain.APIKey{
			ID:       "k_new",
			TenantID: "t_abc",
			Name:     "ci-key",
			Role:     domain.RoleOwner,
		},
		createRaw: "raw-token-shown-once",
	}
	h := handler.NewAPIKeyHandler(svc)

	body, _ := json.Marshal(handler.CreateAPIKeyRequest{Name: "ci-key", Role: "owner"})
	req := httptest.NewRequest("POST", "/api/keys", bytes.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_abc"))
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (body: %s)", rr.Code, rr.Body.String())
	}
	var resp handler.CreateAPIKeyResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID != "k_new" || resp.Name != "ci-key" || resp.Role != domain.RoleOwner || resp.Token != "raw-token-shown-once" {
		t.Errorf("unexpected response: %+v", resp)
	}
	if len(svc.createCalls) != 1 {
		t.Fatalf("expected 1 service call, got %d", len(svc.createCalls))
	}
	if got := svc.createCalls[0]; got.tenantID != "t_abc" || got.name != "ci-key" || got.role != domain.RoleOwner {
		t.Errorf("service called with %+v, want {t_abc, ci-key, owner}", got)
	}
}

func TestCreateAPIKey_DefaultRole(t *testing.T) {
	svc := &mockAPIKeyCreateSvc{
		createResp: &domain.APIKey{ID: "k_new", Name: "n", Role: domain.RoleDeveloper},
		createRaw:  "tok",
	}
	h := handler.NewAPIKeyHandler(svc)

	// No role in the request → handler must default to developer.
	body, _ := json.Marshal(map[string]string{"name": "n"})
	req := httptest.NewRequest("POST", "/api/keys", bytes.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_abc"))
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}
	if got := svc.createCalls[0].role; got != domain.RoleDeveloper {
		t.Errorf("role passed to service = %q, want %q", got, domain.RoleDeveloper)
	}
}

func TestCreateAPIKey_MissingName(t *testing.T) {
	svc := &mockAPIKeyCreateSvc{}
	h := handler.NewAPIKeyHandler(svc)

	body, _ := json.Marshal(map[string]string{"role": "owner"})
	req := httptest.NewRequest("POST", "/api/keys", bytes.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_abc"))
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
	if len(svc.createCalls) != 0 {
		t.Errorf("service must not be called on bad request; got %d calls", len(svc.createCalls))
	}
}

func TestCreateAPIKey_MissingTenantContext(t *testing.T) {
	// Defensive: if this handler is ever re-registered on a public route
	// by mistake, the guard returns 401 instead of letting the service
	// FK-violate on an empty tenant_id.
	svc := &mockAPIKeyCreateSvc{}
	h := handler.NewAPIKeyHandler(svc)

	body, _ := json.Marshal(handler.CreateAPIKeyRequest{Name: "n", Role: "owner"})
	req := httptest.NewRequest("POST", "/api/keys", bytes.NewReader(body))
	// No WithTenantID call.
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 when tenant context missing, got %d", rr.Code)
	}
	if len(svc.createCalls) != 0 {
		t.Errorf("service must not be called without tenant context; got %d calls", len(svc.createCalls))
	}
}

func TestCreateAPIKey_ServiceError(t *testing.T) {
	svc := &mockAPIKeyCreateSvc{createErr: errors.New("db down")}
	h := handler.NewAPIKeyHandler(svc)

	body, _ := json.Marshal(handler.CreateAPIKeyRequest{Name: "n", Role: "owner"})
	req := httptest.NewRequest("POST", "/api/keys", bytes.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_abc"))
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "db down") {
		t.Errorf("response should not leak raw error, got: %s", rr.Body.String())
	}
}

// mockAPIKeyUpdateSvc implements APIKeyServiceInterface for Update tests.
type mockAPIKeyUpdateSvc struct {
	updateKey *domain.APIKey
	updateErr error
}

func (m *mockAPIKeyUpdateSvc) CreateAPIKey(_ context.Context, _, _, _ string, _ *int) (*domain.APIKey, string, error) {
	panic("not used by Update")
}
func (m *mockAPIKeyUpdateSvc) ListAPIKeys(_ context.Context, _ string) ([]domain.APIKey, error) {
	panic("not used by Update")
}
func (m *mockAPIKeyUpdateSvc) GetByID(_ context.Context, _ string) (*domain.APIKey, error) {
	panic("not used by Update")
}
func (m *mockAPIKeyUpdateSvc) DeleteAPIKey(_ context.Context, _, _ string) error {
	panic("not used by Update")
}
func (m *mockAPIKeyUpdateSvc) UpdateAPIKey(_ context.Context, _, _ string, _ *domain.UpdateAPIKeyRequest) (*domain.APIKey, error) {
	return m.updateKey, m.updateErr
}
func (m *mockAPIKeyUpdateSvc) RotateAPIKey(_ context.Context, _, _ string) (*domain.APIKey, string, error) {
	panic("not used by Update")
}

func TestUpdateAPIKey_Success(t *testing.T) {
	name := "renamed-key"
	svc := &mockAPIKeyUpdateSvc{
		updateKey: &domain.APIKey{ID: "k_1", TenantID: "t_abc", Name: name, Role: domain.RoleDeveloper},
	}
	h := handler.NewAPIKeyHandler(svc)

	body, _ := json.Marshal(map[string]string{"name": name})
	req := httptest.NewRequest("PUT", "/api/keys/k_1", bytes.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_abc"))
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", rr.Code, rr.Body.String())
	}
	var key domain.SafeAPIKeyResponse
	if err := json.NewDecoder(rr.Body).Decode(&key); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if key.Name != name {
		t.Errorf("Name = %q, want %q", key.Name, name)
	}
}

func TestUpdateAPIKey_NotFound(t *testing.T) {
	svc := &mockAPIKeyUpdateSvc{updateErr: service.ErrAPIKeyNotFound}
	h := handler.NewAPIKeyHandler(svc)

	body, _ := json.Marshal(map[string]string{"name": "new-name"})
	req := httptest.NewRequest("PUT", "/api/keys/k_missing", bytes.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_abc"))
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}

func TestUpdateAPIKey_InvalidBody(t *testing.T) {
	h := handler.NewAPIKeyHandler(&mockAPIKeyUpdateSvc{})

	req := httptest.NewRequest("PUT", "/api/keys/k_1", strings.NewReader(`not json`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_abc"))
	rr := httptest.NewRecorder()

	h.Update(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}
