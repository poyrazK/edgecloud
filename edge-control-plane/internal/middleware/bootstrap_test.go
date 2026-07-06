package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testBootstrapSecret = "test-bootstrap-secret-32-bytes!!"
const testBootstrapJWTSecret = "test-bootstrap-jwt-secret-32-bytes"

func testBootstrapCfg() BootstrapJWTConfig {
	return BootstrapJWTConfig{
		BootstrapSecret:   testBootstrapSecret,
		BootstrapJWTSecret: testBootstrapJWTSecret,
		Issuer:            "edgecloud-test",
	}
}

// computeSignature computes the HMAC-SHA256 signature for a bootstrap request.
func computeSignature(req *BootstrapRequest, secret []byte) string {
	payload := fmt.Sprintf("%s:%s:%s:%s:%s",
		req.WorkerID, req.Region, req.TenantID, req.Timestamp, req.Nonce)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// validBootstrapRequest creates a valid BootstrapRequest with the given params.
func validBootstrapRequest(workerID, region, tenantID string) *BootstrapRequest {
	ts := time.Now().UTC().Format(time.RFC3339)
	req := &BootstrapRequest{
		WorkerID:  workerID,
		Region:    region,
		TenantID:  tenantID,
		Timestamp: ts,
		Nonce:     "random-nonce-123",
	}
	req.Signature = computeSignature(req, []byte(testBootstrapSecret))
	return req
}

// ── ValidateAndVerifyBootstrapRequest Tests ──────────────────────────

func TestValidateAndVerifyBootstrapRequest_Valid(t *testing.T) {
	req := validBootstrapRequest("w_fra_1", "fra", "t_acme")
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestValidateAndVerifyBootstrapRequest_MissingWorkerID(t *testing.T) {
	req := validBootstrapRequest("", "fra", "t_acme")
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for missing worker_id")
	}
}

func TestValidateAndVerifyBootstrapRequest_MissingRegion(t *testing.T) {
	req := validBootstrapRequest("w1", "", "t_acme")
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for missing region")
	}
}

func TestValidateAndVerifyBootstrapRequest_MissingTenantID(t *testing.T) {
	req := validBootstrapRequest("w1", "fra", "")
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for missing tenant_id")
	}
}

func TestValidateAndVerifyBootstrapRequest_MissingTimestamp(t *testing.T) {
	req := validBootstrapRequest("w1", "fra", "t_acme")
	req.Timestamp = ""
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for missing timestamp")
	}
}

func TestValidateAndVerifyBootstrapRequest_MissingNonce(t *testing.T) {
	req := validBootstrapRequest("w1", "fra", "t_acme")
	req.Nonce = ""
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for missing nonce")
	}
}

func TestValidateAndVerifyBootstrapRequest_MissingSignature(t *testing.T) {
	req := validBootstrapRequest("w1", "fra", "t_acme")
	req.Signature = ""
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for missing signature")
	}
}

func TestValidateAndVerifyBootstrapRequest_InvalidSignature(t *testing.T) {
	req := validBootstrapRequest("w1", "fra", "t_acme")
	req.Signature = "0000000000000000000000000000000000000000000000000000000000000000"
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for wrong signature")
	}
}

func TestValidateAndVerifyBootstrapRequest_TimestampTooOld(t *testing.T) {
	ts := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
	req := &BootstrapRequest{
		WorkerID: "w1", Region: "fra", TenantID: "t_acme",
		Timestamp: ts, Nonce: "n1",
	}
	req.Signature = computeSignature(req, []byte(testBootstrapSecret))
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for old timestamp")
	}
}

func TestValidateAndVerifyBootstrapRequest_TimestampInFuture(t *testing.T) {
	ts := time.Now().Add(2 * time.Minute).Format(time.RFC3339)
	req := &BootstrapRequest{
		WorkerID: "w1", Region: "fra", TenantID: "t_acme",
		Timestamp: ts, Nonce: "n1",
	}
	req.Signature = computeSignature(req, []byte(testBootstrapSecret))
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for future timestamp")
	}
}

func TestValidateAndVerifyBootstrapRequest_InvalidTimestampFormat(t *testing.T) {
	req := validBootstrapRequest("w1", "fra", "t_acme")
	req.Timestamp = "not-a-timestamp"
	req.Signature = computeSignature(req, []byte(testBootstrapSecret))
	err := ValidateAndVerifyBootstrapRequest(req, []byte(testBootstrapSecret))
	if err == nil {
		t.Fatal("expected error for invalid timestamp format")
	}
}

func TestValidateAndVerifyBootstrapRequest_WrongSecret(t *testing.T) {
	req := validBootstrapRequest("w1", "fra", "t_acme")
	err := ValidateAndVerifyBootstrapRequest(req, []byte("wrong-secret"))
	if err == nil {
		t.Fatal("expected error for wrong secret")
	}
}

// ── IssueBootstrapJWT Tests ──────────────────────────────────────────

func TestIssueBootstrapJWT_Valid(t *testing.T) {
	cfg := testBootstrapCfg()
	token, err := IssueBootstrapJWT(cfg, "w_fra_1", "t_acme", "fra")
	if err != nil {
		t.Fatalf("IssueBootstrapJWT: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}
}

func TestIssueBootstrapJWT_ContainsCorrectClaims(t *testing.T) {
	cfg := testBootstrapCfg()
	token, err := IssueBootstrapJWT(cfg, "w_fra_1", "t_acme", "fra")
	if err != nil {
		t.Fatalf("IssueBootstrapJWT: %v", err)
	}

	parsed, err := jwt.ParseWithClaims(token, &BootstrapClaims{}, func(token *jwt.Token) (any, error) {
		return []byte(testBootstrapJWTSecret), nil
	})
	if err != nil {
		t.Fatalf("ParseWithClaims: %v", err)
	}
	claims := parsed.Claims.(*BootstrapClaims)
	if claims.WorkerID != "w_fra_1" {
		t.Errorf("WorkerID = %q, want w_fra_1", claims.WorkerID)
	}
	if claims.TenantID != "t_acme" {
		t.Errorf("TenantID = %q, want t_acme", claims.TenantID)
	}
	if claims.Region != "fra" {
		t.Errorf("Region = %q, want fra", claims.Region)
	}
}

func TestIssueBootstrapJWT_CorrectIssuer(t *testing.T) {
	cfg := testBootstrapCfg()
	token, err := IssueBootstrapJWT(cfg, "w1", "t1", "fra")
	if err != nil {
		t.Fatalf("IssueBootstrapJWT: %v", err)
	}
	parsed, err := jwt.ParseWithClaims(token, &BootstrapClaims{}, func(token *jwt.Token) (any, error) {
		return []byte(testBootstrapJWTSecret), nil
	})
	if err != nil {
		t.Fatalf("ParseWithClaims: %v", err)
	}
	claims := parsed.Claims.(*BootstrapClaims)
	if claims.Issuer != "edgecloud-test" {
		t.Errorf("Issuer = %q, want edgecloud-test", claims.Issuer)
	}
}

func TestIssueBootstrapJWT_ExpiryIsShort(t *testing.T) {
	cfg := testBootstrapCfg()
	token, err := IssueBootstrapJWT(cfg, "w1", "t1", "fra")
	if err != nil {
		t.Fatalf("IssueBootstrapJWT: %v", err)
	}
	parsed, err := jwt.ParseWithClaims(token, &BootstrapClaims{}, func(token *jwt.Token) (any, error) {
		return []byte(testBootstrapJWTSecret), nil
	})
	if err != nil {
		t.Fatalf("ParseWithClaims: %v", err)
	}
	claims := parsed.Claims.(*BootstrapClaims)
	exp := claims.ExpiresAt.Time
	iat := claims.IssuedAt.Time
	// The expiry should be ~5 minutes from issued at (allow 10s clock skew).
	if exp.Sub(iat) < 4*time.Minute || exp.Sub(iat) > 6*time.Minute {
		t.Errorf("token lifetime = %v, want ~5m", exp.Sub(iat))
	}
}

func TestIssueBootstrapJWT_HasJTI(t *testing.T) {
	cfg := testBootstrapCfg()
	token, err := IssueBootstrapJWT(cfg, "w1", "t1", "fra")
	if err != nil {
		t.Fatalf("IssueBootstrapJWT: %v", err)
	}
	parsed, err := jwt.ParseWithClaims(token, &BootstrapClaims{}, func(token *jwt.Token) (any, error) {
		return []byte(testBootstrapJWTSecret), nil
	})
	if err != nil {
		t.Fatalf("ParseWithClaims: %v", err)
	}
	claims := parsed.Claims.(*BootstrapClaims)
	if claims.ID == "" {
		t.Error("expected non-empty JTI")
	}
	if !strings.HasPrefix(claims.ID, "bs-") {
		t.Errorf("JTI = %q, want bs- prefix", claims.ID)
	}
}

func TestIssueBootstrapJWT_FallsBackToBootstrapSecret(t *testing.T) {
	cfg := BootstrapJWTConfig{
		BootstrapSecret: testBootstrapSecret,
		// BootstrapJWTSecret intentionally empty
		Issuer: "test",
	}
	token, err := IssueBootstrapJWT(cfg, "w1", "t1", "fra")
	if err != nil {
		t.Fatalf("IssueBootstrapJWT: %v", err)
	}
	// Should verify with BootstrapSecret.
	_, err = jwt.ParseWithClaims(token, &BootstrapClaims{}, func(token *jwt.Token) (any, error) {
		return []byte(testBootstrapSecret), nil
	})
	if err != nil {
		t.Fatalf("ParseWithClaims with BootstrapSecret: %v", err)
	}
}

func TestIssueBootstrapJWT_EmptySecret_ReturnsError(t *testing.T) {
	cfg := BootstrapJWTConfig{Issuer: "test"}
	_, err := IssueBootstrapJWT(cfg, "w1", "t1", "fra")
	if err == nil {
		t.Fatal("expected error for empty secrets")
	}
}

func TestIssueBootstrapJWT_DefaultIssuer(t *testing.T) {
	cfg := BootstrapJWTConfig{
		BootstrapSecret: testBootstrapSecret,
	}
	token, err := IssueBootstrapJWT(cfg, "w1", "t1", "fra")
	if err != nil {
		t.Fatalf("IssueBootstrapJWT: %v", err)
	}
	parsed, err := jwt.ParseWithClaims(token, &BootstrapClaims{}, func(token *jwt.Token) (any, error) {
		return []byte(testBootstrapSecret), nil
	})
	if err != nil {
		t.Fatalf("ParseWithClaims: %v", err)
	}
	claims := parsed.Claims.(*BootstrapClaims)
	if claims.Issuer != "edgecloud-bootstrap" {
		t.Errorf("Issuer = %q, want edgecloud-bootstrap", claims.Issuer)
	}
}

// ── VerifyBootstrapJWT Tests ─────────────────────────────────────────

func TestVerifyBootstrapJWT_Valid(t *testing.T) {
	cfg := testBootstrapCfg()
	token, _ := IssueBootstrapJWT(cfg, "w1", "t1", "fra")
	claims, err := VerifyBootstrapJWT(token, cfg)
	if err != nil {
		t.Fatalf("VerifyBootstrapJWT: %v", err)
	}
	if claims.WorkerID != "w1" {
		t.Errorf("WorkerID = %q, want w1", claims.WorkerID)
	}
}

func TestVerifyBootstrapJWT_WrongSecret(t *testing.T) {
	cfg := testBootstrapCfg()
	token, _ := IssueBootstrapJWT(cfg, "w1", "t1", "fra")
	wrongCfg := BootstrapJWTConfig{
		BootstrapSecret:   "wrong-secret",
		BootstrapJWTSecret: "wrong-jwt-secret",
		Issuer:            "edgecloud-test",
	}
	_, err := VerifyBootstrapJWT(token, wrongCfg)
	if err == nil {
		t.Fatal("expected error for wrong secret")
	}
}

func TestVerifyBootstrapJWT_ExpiredToken(t *testing.T) {
	// Create a token that's already expired.
	claims := BootstrapClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud-test",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
		},
		WorkerID: "w1", TenantID: "t1", Region: "fra",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, _ := token.SignedString([]byte(testBootstrapJWTSecret))
	cfg := testBootstrapCfg()
	_, err := VerifyBootstrapJWT(tokenStr, cfg)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestVerifyBootstrapJWT_TamperedToken(t *testing.T) {
	cfg := testBootstrapCfg()
	token, _ := IssueBootstrapJWT(cfg, "w1", "t1", "fra")
	// Corrupt the signature part of the token.
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	// Replace the signature with garbage.
	tampered := parts[0] + "." + parts[1] + ".invalidsignature"
	_, err := VerifyBootstrapJWT(tampered, cfg)
	if err == nil {
		t.Fatal("expected error for tampered token")
	}
}

func TestVerifyBootstrapJWT_WrongIssuer(t *testing.T) {
	claims := BootstrapClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "wrong-issuer",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		WorkerID: "w1", TenantID: "t1", Region: "fra",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, _ := token.SignedString([]byte(testBootstrapJWTSecret))
	cfg := testBootstrapCfg()
	_, err := VerifyBootstrapJWT(tokenStr, cfg)
	if err == nil {
		t.Fatal("expected error for wrong issuer")
	}
}

func TestVerifyBootstrapJWT_EmptySecret(t *testing.T) {
	cfg := BootstrapJWTConfig{}
	_, err := VerifyBootstrapJWT("some-token", cfg)
	if err == nil {
		t.Fatal("expected error for empty secret")
	}
}

func TestVerifyBootstrapJWT_NoneAlgorithm(t *testing.T) {
	// Token with alg: none should be rejected.
	token := jwt.NewWithClaims(jwt.SigningMethodNone, BootstrapClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "edgecloud-test",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		},
		WorkerID: "w1", TenantID: "t1", Region: "fra",
	})
	tokenStr, _ := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
	cfg := testBootstrapCfg()
	_, err := VerifyBootstrapJWT(tokenStr, cfg)
	if err == nil {
		t.Fatal("expected error for none algorithm token")
	}
}

// ── BootstrapAuth Middleware Tests ────────────────────────────────────

func TestBootstrapAuth_ValidToken(t *testing.T) {
	cfg := testBootstrapCfg()
	token, _ := IssueBootstrapJWT(cfg, "w_fra_1", "t_acme", "fra")

	handler := BootstrapAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that the context has the right values.
		if got := r.Context().Value(WorkerIDKey); got != "w_fra_1" {
			t.Errorf("WorkerIDKey = %v, want w_fra_1", got)
		}
		if got := r.Context().Value(WorkerTenantIDKey); got != "t_acme" {
			t.Errorf("WorkerTenantIDKey = %v, want t_acme", got)
		}
		if got := r.Context().Value(WorkerRegionKey); got != "fra" {
			t.Errorf("WorkerRegionKey = %v, want fra", got)
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/internal/bootstrap", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
}

func TestBootstrapAuth_MissingHeader(t *testing.T) {
	cfg := testBootstrapCfg()
	handler := BootstrapAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/internal/bootstrap", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}

func TestBootstrapAuth_InvalidToken(t *testing.T) {
	cfg := testBootstrapCfg()
	handler := BootstrapAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/internal/bootstrap", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}

func TestBootstrapAuth_NonBearerToken(t *testing.T) {
	cfg := testBootstrapCfg()
	handler := BootstrapAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/internal/bootstrap", nil)
	req.Header.Set("Authorization", "Basic somecreds")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}
