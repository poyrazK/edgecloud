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
			Issuer:    "edgecloud",
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
			Issuer:    "edgecloud",
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
			Issuer:    "edgecloud",
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

// TestVerifyWorkerJWT_NoExpRejected pins jwt.WithExpirationRequired:
// a token without an `exp` claim is rejected instead of being accepted
// forever. A leaked token with no expiration used to be valid for the
// lifetime of the worker's signing key.
func TestVerifyWorkerJWT_NoExpRejected(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: "edgecloud",
			// No ExpiresAt set.
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err == nil {
		t.Error("expected error for token without exp, got nil")
	}
}

// TestVerifyWorkerJWT_NoIssRejectedWhenConfigured pins jwt.WithIssuer:
// when cfg.Issuer is set, a token with no `iss` claim is rejected.
// This is the JWT-bodies-need-an-issuer invariant.
func TestVerifyWorkerJWT_NoIssRejectedWhenConfigured(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			// No Issuer set.
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err == nil {
		t.Error("expected error for token without iss when cfg.Issuer is set, got nil")
	}
}

// TestVerifyWorkerJWT_WrongIssRejected pins the issuer-mismatch case:
// a token whose iss doesn't match cfg.Issuer is rejected. (Replaces
// the implicit coverage of the deleted post-parse check.)
func TestVerifyWorkerJWT_WrongIssRejected(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "other-control-plane",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err == nil {
		t.Error("expected error for wrong iss, got nil")
	}
}

// TestVerifyWorkerJWT_EmptyIssuerSkipsIssCheck pins the documented
// behavior: jwt.WithIssuer("") makes the library skip the iss check
// entirely. A token with any iss (or none) is accepted when
// cfg.Issuer is empty. This is the invariant that makes the
// "always call WithIssuer" cleanup safe — the library's internal
// guard handles the empty case. Production callers must NOT rely
// on this: the control-plane config defaults cfg.Issuer to
// "edgecloud", so an empty cfg.Issuer is a misconfiguration. The
// test exists to document the behavior, not to encourage it.
func TestVerifyWorkerJWT_EmptyIssuerSkipsIssCheck(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: ""}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "other-control-plane",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	// Must NOT error: empty cfg.Issuer means iss is not enforced.
	if _, err := VerifyWorkerJWT(tokenString, cfg); err != nil {
		t.Errorf("empty cfg.Issuer should skip iss check; got error: %v", err)
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
			Issuer:    "edgecloud",
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

func TestWorkerAuth_PutsRegionInContext(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Region:   "fra",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	gotRegion := ""
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRegion = GetWorkerRegion(r.Context())
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
	if gotRegion != "fra" {
		t.Errorf("region = %q, want %q", gotRegion, "fra")
	}
}

// TestWorkerAuth_RejectsQueryStringToken pins the header-only contract.
// A token passed via `?jwt=<valid>` in the URL (and no Authorization
// header) must be rejected — it would otherwise leak into access logs,
// browser history, and reverse-proxy error pages.
func TestWorkerAuth_RejectsQueryStringToken(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("downstream handler must not be called when no Authorization header is set")
	})
	mw := WorkerAuth(cfg)(handler)

	// Token in URL only, no header.
	req := httptest.NewRequest("GET", "/api/internal/download/d_abc?jwt="+tokenString, nil)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (query-string token must be rejected)", rec.Code, http.StatusUnauthorized)
	}
}

// TestWorkerAuth_HeaderWinsWhenBothPresent documents the priority:
// when both `?jwt=` and a valid Authorization header are present, the
// header is the source of truth. A request that contains both should
// succeed (assuming the header token is valid).
func TestWorkerAuth_HeaderWinsWhenBothPresent(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret", Issuer: "edgecloud"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mw := WorkerAuth(cfg)(handler)

	req := httptest.NewRequest("GET", "/api/internal/download/d_abc?jwt="+tokenString, nil)
	req.Header.Set("Authorization", "Bearer "+tokenString)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d (header should win when both present)", rec.Code, http.StatusOK)
	}
}
