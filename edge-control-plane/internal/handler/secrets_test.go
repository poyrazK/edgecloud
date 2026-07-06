package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

func newSecretsMux(encryptor *service.SecretEncryptor, envSvc *service.EnvService) *http.ServeMux {
	mux := http.NewServeMux()
	h := NewSecretsAdminHandler(encryptor, envSvc)
	mux.HandleFunc("GET /api/v1/admin/secrets/keys", h.ListKeys)
	mux.HandleFunc("POST /api/v1/admin/secrets/re-encrypt", h.ReEncrypt)
	return mux
}

func TestSecretsAdminHandler_ListKeys(t *testing.T) {
	enc, err := service.NewSecretEncryptorFromLegacy("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewSecretEncryptorFromLegacy: %v", err)
	}
	mux := newSecretsMux(enc, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/secrets/keys", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["active_key"] != "legacy" {
		t.Errorf("active_key = %v, want legacy", resp["active_key"])
	}
	if resp["encryption_enabled"] != true {
		t.Errorf("encryption_enabled = %v, want true", resp["encryption_enabled"])
	}
}

func TestSecretsAdminHandler_ReEncrypt_Success(t *testing.T) {
	enc, _ := service.NewSecretEncryptorFromLegacy("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	// Create an EnvService with a no-op mock repo that returns no apps.
	mockRepo := &mockEnvRepo{}
	envSvc := service.NewEnvService(mockRepo)
	envSvc.SetSecretEncryptor(enc)

	mux := newSecretsMux(enc, envSvc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/secrets/re-encrypt", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["re_encrypted"] != float64(0) {
		t.Errorf("re_encrypted = %v, want 0", resp["re_encrypted"])
	}
	if resp["status"] != "ok" {
		t.Errorf("status = %v, want ok", resp["status"])
	}
}

func TestSecretsAdminHandler_ReEncrypt_NoEncryptor(t *testing.T) {
	mux := newSecretsMux(nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/secrets/re-encrypt", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestSecretsAdminHandler_ReEncrypt_WithEncryptionDisabled(t *testing.T) {
	// nil encryptor means encryption is not configured.
	mux := newSecretsMux(nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/secrets/re-encrypt", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

// mockEnvRepo implements service.EnvRepoInterface with no apps.
type mockEnvRepo struct{}

func (m *mockEnvRepo) Set(ctx context.Context, env *domain.AppEnv) error { return nil }
func (m *mockEnvRepo) List(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
	return nil, nil
}
func (m *mockEnvRepo) ListByApps(ctx context.Context, tenantID string, appNames []string) ([]domain.AppEnv, error) {
	return nil, nil
}
func (m *mockEnvRepo) Delete(ctx context.Context, tenantID, appName, key string) error { return nil }
func (m *mockEnvRepo) ListAllApps(ctx context.Context) ([]string, []string, error) {
	return nil, nil, nil
}
