package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestVerifyWorkerJWT_Valid(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:   "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	result, err := VerifyWorkerJWT(tokenString, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WorkerID != "w_fra_abc123" {
		t.Errorf("worker_id = %s, want w_fra_abc123", result.WorkerID)
	}
	if result.TenantID != "t_tenant1" {
		t.Errorf("tenant_id = %s, want t_tenant1", result.TenantID)
	}
}

func TestVerifyWorkerJWT_Expired(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:   "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err == nil {
		t.Error("expected error for expired token, got nil")
	}
}

func TestVerifyWorkerJWT_WrongSecret(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:   "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("wrong-secret"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err == nil {
		t.Error("expected error for wrong secret, got nil")
	}
}

func TestWorkerAuth_MissingToken(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	middleware := WorkerAuth(cfg)(handler)

	req := httptest.NewRequest("GET", "/api/internal/download/d_abc123", nil)
	rec := httptest.NewRecorder()
	middleware.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestWorkerAuth_ValidToken(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:   "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	gotTenantID := ""
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenantID = GetWorkerTenantID(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	middleware := WorkerAuth(cfg)(handler)

	req := httptest.NewRequest("GET", "/api/internal/download/d_abc123", nil)
	req.Header.Set("Authorization", "Bearer "+tokenString)
	rec := httptest.NewRecorder()
	middleware.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if gotTenantID != "t_tenant1" {
		t.Errorf("tenant_id = %s, want t_tenant1", gotTenantID)
	}
}
