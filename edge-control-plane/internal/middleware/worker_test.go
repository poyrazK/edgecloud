package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
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

// ---------------------------------------------------------------------------
// kid-based key selection tests — Sprint 2 JWT key rotation
// ---------------------------------------------------------------------------

func TestVerifyWorkerJWT_WithKidKeyring(t *testing.T) {
	cfg := WorkerJWTConfig{
		Issuer:    "edgecloud",
		ActiveKID: "key1",
		Keys: map[string]string{
			"key1": "test-secret-key1-32-bytes-long!!",
			"key2": "test-secret-key2-32-bytes-long!!",
		},
	}
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
	token.Header["kid"] = "key1"
	tokenString, err := token.SignedString([]byte("test-secret-key1-32-bytes-long!!"))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	result, err := VerifyWorkerJWT(tokenString, cfg)
	if err != nil {
		t.Fatalf("VerifyWorkerJWT: %v", err)
	}
	if result.WorkerID != "w_fra_abc123" {
		t.Errorf("worker_id = %s, want w_fra_abc123", result.WorkerID)
	}
}

func TestVerifyWorkerJWT_KidMatchesSecondaryKey(t *testing.T) {
	cfg := WorkerJWTConfig{
		Issuer:    "edgecloud",
		ActiveKID: "key1",
		Keys: map[string]string{
			"key1": "test-secret-key1-32-bytes-long!!",
			"key2": "test-secret-key2-32-bytes-long!!",
		},
	}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = "key2"
	tokenString, _ := token.SignedString([]byte("test-secret-key2-32-bytes-long!!"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err != nil {
		t.Errorf("should verify with key2 when kid=key2: %v", err)
	}
}

func TestVerifyWorkerJWT_UnknownKidRejected(t *testing.T) {
	cfg := WorkerJWTConfig{
		Issuer:    "edgecloud",
		ActiveKID: "key1",
		Keys: map[string]string{
			"key1": "test-secret-key1-32-bytes-long!!",
		},
	}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = "unknown"
	tokenString, _ := token.SignedString([]byte("some-key"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err == nil {
		t.Error("expected error for unknown kid, got nil")
	}
}

func TestVerifyWorkerJWT_KnownKidWrongKeyRejected(t *testing.T) {
	cfg := WorkerJWTConfig{
		Issuer:    "edgecloud",
		ActiveKID: "key1",
		Keys: map[string]string{
			"key1": "test-secret-key1-32-bytes-long!!",
		},
	}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
	}
	// kid=key1 but signed with wrong key
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = "key1"
	tokenString, _ := token.SignedString([]byte("wrong-key-32-bytes-long!!!!!!!"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err == nil {
		t.Error("expected error for wrong key with known kid, got nil")
	}
}

func TestVerifyWorkerJWT_NoKidFallbackToSecret(t *testing.T) {
	cfg := WorkerJWTConfig{
		Secret:    "legacy-secret-32-bytes-long-for-test!",
		Issuer:    "edgecloud",
		ActiveKID: "key1",
		Keys: map[string]string{
			"key1": "keyring-secret-32-bytes-long!!",
		},
	}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
	}
	// No kid header, should fall back to Secret.
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("legacy-secret-32-bytes-long-for-test!"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err != nil {
		t.Errorf("should fall back to Secret when no kid: %v", err)
	}
}

func TestVerifyWorkerJWT_NoKidFallsBackToActiveKey(t *testing.T) {
	cfg := WorkerJWTConfig{
		Issuer:    "edgecloud",
		ActiveKID: "default",
		Keys: map[string]string{
			"default": "default-key-32-bytes-long-in-keys!!",
		},
	}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("default-key-32-bytes-long-in-keys!!"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err != nil {
		t.Errorf("should fall back to active key when no kid and no Secret: %v", err)
	}
}

func TestVerifyWorkerJWT_LegacySecretStillWorks(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "test-secret-32-bytes-long-for-legacy!"}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_tenant1",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret-32-bytes-long-for-legacy!"))

	_, err := VerifyWorkerJWT(tokenString, cfg)
	if err != nil {
		t.Errorf("legacy config (Secret only) should still work: %v", err)
	}
}

func TestResolveSigningKey_ActiveKidReturnsCorrectKey(t *testing.T) {
	cfg := WorkerJWTConfig{
		ActiveKID: "key1",
		Keys: map[string]string{
			"key1": "key1-secret-32-bytes-long-test-abc123",
			"key2": "key2-secret-32-bytes-long-test-xyz789",
		},
	}
	key, err := cfg.ResolveSigningKey()
	if err != nil {
		t.Fatalf("ResolveSigningKey: %v", err)
	}
	if string(key) != "key1-secret-32-bytes-long-test-abc123" {
		t.Errorf("key = %q, want key1 secret", string(key))
	}
}

func TestResolveSigningKey_LegacyFallback(t *testing.T) {
	cfg := WorkerJWTConfig{Secret: "legacy-secret-32-bytes-long-for-test!"}
	key, err := cfg.ResolveSigningKey()
	if err != nil {
		t.Fatalf("ResolveSigningKey: %v", err)
	}
	if string(key) != "legacy-secret-32-bytes-long-for-test!" {
		t.Errorf("key = %q, want legacy secret", string(key))
	}
}

func TestResolveSigningKey_NoConfig(t *testing.T) {
	cfg := WorkerJWTConfig{}
	_, err := cfg.ResolveSigningKey()
	if err == nil {
		t.Error("expected error for empty config, got nil")
	}
}

func TestWorkerAuth_KeyringRoundTrip(t *testing.T) {
	cfg := WorkerJWTConfig{
		Issuer:    "edgecloud",
		ActiveKID: "key1",
		Keys: map[string]string{
			"key1": "keyring-secret-32-bytes-abcdef123456!!",
		},
	}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: "w_fra_abc123",
		TenantID: "t_keyring",
		Apps:     []string{"my-app"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = "key1"
	tokenString, _ := token.SignedString([]byte("keyring-secret-32-bytes-abcdef123456!!"))

	gotTenantID := ""
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenantID = GetWorkerTenantID(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	middleware := WorkerAuth(cfg)(handler)

	req := httptest.NewRequest("GET", "/api/internal/download/d_abc", nil)
	req.Header.Set("Authorization", "Bearer "+tokenString)
	rec := httptest.NewRecorder()
	middleware.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if gotTenantID != "t_keyring" {
		t.Errorf("tenant_id = %s, want t_keyring", gotTenantID)
	}
}

// ── Per-worker key derivation tests (issue #430) ──────────────────────────

// perWorkerTestPubkey is a fixed 64-hex-char Ed25519 public key used
// by the wkr_ kid tests below. The corresponding private key is
// irrelevant — only the public_key half participates in the HKDF
// derivation.
const perWorkerTestPubkey = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"

// perWorkerTestSecret is the cluster master that HKDF consumes as
// IKM. Mirrors the test master in signing/worker_key_test.go.
const perWorkerTestSecret = "test-cluster-master-secret-that-is-long-enough-32-bytes!"

// signWithDerivedSecret is a tiny helper that mints an HS256 JWT
// signed with the per-worker HKDF-derived secret for a given
// (workerID, tenantID, region, pubkey). It exists so the verify-side
// tests don't duplicate the DeriveWorkerSecret call shape.
func signWithDerivedSecret(t *testing.T, workerID, tenantID, region string, pubkey string) string {
	t.Helper()
	derived, err := signing.DeriveWorkerSecret([]byte(perWorkerTestSecret), workerID, tenantID, region, pubkey)
	if err != nil {
		t.Fatalf("DeriveWorkerSecret: %v", err)
	}
	claims := &WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		WorkerID: workerID,
		TenantID: tenantID,
		Region:   region,
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tok.Header["kid"] = signing.WorkerKID(pubkey)
	signed, err := tok.SignedString(derived)
	if err != nil {
		t.Fatalf("SignedString: %v", err)
	}
	return signed
}

// TestVerifyWorkerJWT_WkrKid_Roundtrip pins the issue #430 verify
// path: a token signed with the per-worker derived secret and
// carrying the wkr_ kid verifies when WorkerKeyCache is wired.
func TestVerifyWorkerJWT_WkrKid_Roundtrip(t *testing.T) {
	cache := NewWorkerKeyCache(func(ctx context.Context, workerID string) (string, error) {
		return perWorkerTestPubkey, nil
	})
	cfg := WorkerJWTConfig{
		Secret:         perWorkerTestSecret,
		Issuer:         "edgecloud",
		WorkerKeyCache: cache,
	}
	tok := signWithDerivedSecret(t, "w_fra_x", "t_real", "fra", perWorkerTestPubkey)
	claims, err := VerifyWorkerJWT(tok, cfg)
	if err != nil {
		t.Fatalf("VerifyWorkerJWT: %v", err)
	}
	if claims.WorkerID != "w_fra_x" {
		t.Errorf("WorkerID = %q, want w_fra_x", claims.WorkerID)
	}
	if claims.TenantID != "t_real" {
		t.Errorf("TenantID = %q, want t_real", claims.TenantID)
	}
}

// TestVerifyWorkerJWT_WkrKid_MissingCache pins the fail-closed
// posture: a wkr_-kid token without a wired WorkerKeyCache is
// refused outright. This is the property that prevents an
// operator from accidentally deploying the verify path with the
// loader left nil — the failure mode is loud (401s everywhere)
// rather than silent (every wkr_ token treated as garbage).
func TestVerifyWorkerJWT_WkrKid_MissingCache(t *testing.T) {
	cfg := WorkerJWTConfig{
		Secret: perWorkerTestSecret,
		Issuer: "edgecloud",
		// no WorkerKeyCache
	}
	tok := signWithDerivedSecret(t, "w_fra_x", "t_real", "fra", perWorkerTestPubkey)
	if _, err := VerifyWorkerJWT(tok, cfg); err == nil {
		t.Fatal("VerifyWorkerJWT with wkr_ kid but no cache should fail")
	} else if !strings.Contains(err.Error(), "WorkerKeyCache is not configured") {
		t.Errorf("err = %v, want it to mention 'WorkerKeyCache is not configured'", err)
	}
}

// TestVerifyWorkerJWT_WkrKid_WrongPubkeyRejected pins the swap-defense:
// a token whose kid claims pubkey A but whose signature was derived
// from pubkey B fails verify. The kid-vs-pubkey equality check in
// resolveKey closes this — without it, the cross-worker forgery
// attack becomes possible.
func TestVerifyWorkerJWT_WkrKid_WrongPubkeyRejected(t *testing.T) {
	pubkeyA := perWorkerTestPubkey
	pubkeyB := "0000000000000000000000000000000000000000000000000000000000000001"
	cache := NewWorkerKeyCache(func(ctx context.Context, workerID string) (string, error) {
		return pubkeyA, nil // cache says pubkey A
	})
	cfg := WorkerJWTConfig{
		Secret:         perWorkerTestSecret,
		Issuer:         "edgecloud",
		WorkerKeyCache: cache,
	}
	// Sign with pubkey B (not A) — kid mismatch with cache lookup.
	tok := signWithDerivedSecret(t, "w_fra_x", "t_real", "fra", pubkeyB)
	_, err := VerifyWorkerJWT(tok, cfg)
	if err == nil {
		t.Fatal("VerifyWorkerJWT with mismatched kid/pubkey should fail")
	}
	if !strings.Contains(err.Error(), "kid") {
		t.Errorf("err = %v, want it to mention 'kid'", err)
	}
}

// TestVerifyWorkerJWT_WkrKid_NonexistentWorkerRejected pins the
// "worker has no enrolled public_key" path: the loader returns "",
// WorkerAuth refuses.
func TestVerifyWorkerJWT_WkrKid_NonexistentWorkerRejected(t *testing.T) {
	cache := NewWorkerKeyCache(func(ctx context.Context, workerID string) (string, error) {
		return "", nil // unenrolled
	})
	cfg := WorkerJWTConfig{
		Secret:         perWorkerTestSecret,
		Issuer:         "edgecloud",
		WorkerKeyCache: cache,
	}
	tok := signWithDerivedSecret(t, "w_legacy", "t_real", "fra", perWorkerTestPubkey)
	_, err := VerifyWorkerJWT(tok, cfg)
	if err == nil {
		t.Fatal("VerifyWorkerJWT with unenrolled worker should fail")
	}
	if !strings.Contains(err.Error(), "no enrolled public_key") {
		t.Errorf("err = %v, want it to mention 'no enrolled public_key'", err)
	}
}

// TestResolveSigningKeyForWorker_HappyPath pins the symmetric
// mint-side helper: the same cache + secret + pubkey produces the
// same derived secret that resolveKey recomputes at verify time.
func TestResolveSigningKeyForWorker_HappyPath(t *testing.T) {
	cache := NewWorkerKeyCache(func(ctx context.Context, workerID string) (string, error) {
		return perWorkerTestPubkey, nil
	})
	cfg := WorkerJWTConfig{
		Secret:         perWorkerTestSecret,
		Issuer:         "edgecloud",
		WorkerKeyCache: cache,
	}
	got, err := cfg.ResolveSigningKeyForWorker(context.Background(), "w_mint", "t_tenant", "fra")
	if err != nil {
		t.Fatalf("ResolveSigningKeyForWorker: %v", err)
	}
	want, err := signing.DeriveWorkerSecret([]byte(perWorkerTestSecret), "w_mint", "t_tenant", "fra", perWorkerTestPubkey)
	if err != nil {
		t.Fatalf("DeriveWorkerSecret: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("derived secret mismatch (mint and verify paths diverged)")
	}
}

// TestResolveSigningKeyForWorker_NoCache pins the missing-cache
// error path. Symmetric with the verify-side TestVerifyWorkerJWT_WkrKid_MissingCache.
func TestResolveSigningKeyForWorker_NoCache(t *testing.T) {
	cfg := WorkerJWTConfig{
		Secret: perWorkerTestSecret,
		Issuer: "edgecloud",
	}
	_, err := cfg.ResolveSigningKeyForWorker(context.Background(), "w_mint", "t_tenant", "fra")
	if err == nil {
		t.Fatal("ResolveSigningKeyForWorker without WorkerKeyCache should fail")
	}
	if !strings.Contains(err.Error(), "WorkerKeyCache not configured") {
		t.Errorf("err = %v, want it to mention 'WorkerKeyCache not configured'", err)
	}
}

// TestResolveSigningKeyForWorker_Unenrolled pins the empty-pubkey
// error path. The handler should surface this as a 500, not a 401.
func TestResolveSigningKeyForWorker_Unenrolled(t *testing.T) {
	cache := NewWorkerKeyCache(func(ctx context.Context, workerID string) (string, error) {
		return "", nil
	})
	cfg := WorkerJWTConfig{
		Secret:         perWorkerTestSecret,
		Issuer:         "edgecloud",
		WorkerKeyCache: cache,
	}
	_, err := cfg.ResolveSigningKeyForWorker(context.Background(), "w_unenrolled", "t_tenant", "fra")
	if err == nil {
		t.Fatal("ResolveSigningKeyForWorker with unenrolled worker should fail")
	}
	if !strings.Contains(err.Error(), "no enrolled public_key") {
		t.Errorf("err = %v, want it to mention 'no enrolled public_key'", err)
	}
}
