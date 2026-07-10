package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/golang-jwt/jwt/v5"
)

// -----------------------------------------------------------------------
// Mock tenantGetter — exercises MintWorkerToken without a live DB. The
// interface in internal.go is narrow enough that the only behaviors we
// need are "found / not found / disabled".
// -----------------------------------------------------------------------

type mockTenantGetter struct {
	tenants map[string]*domain.Tenant
}

func (m *mockTenantGetter) GetByID(_ context.Context, id string) (*domain.Tenant, error) {
	t, ok := m.tenants[id]
	if !ok {
		return nil, service.ErrTenantNotFound
	}
	return t, nil
}

func enabledTenant(id string) *domain.Tenant {
	return &domain.Tenant{ID: id}
}

func disabledTenant(id string) *domain.Tenant {
	now := time.Now()
	return &domain.Tenant{ID: id, DisabledAt: &now}
}

// -----------------------------------------------------------------------
// Test wiring helpers
// -----------------------------------------------------------------------

const (
	workerTokenTestSecret       = "test-secret-must-be-at-least-32-bytes-long!"
	workerTokenTestIssuer       = "edgecloud"
	workerTokenTestDefaultTTL   = 15 * time.Minute
	workerTokenTestCustomTTL    = 5 * time.Minute
	workerTokenTestBootstrapped = "w_us_fra_1"
)

// bootstrapToken mints a Worker JWT with wildcard tenant_id, exactly
// mirroring how the worker presents itself on the very first mint call
// (before it has any scoped token).
func bootstrapToken(t *testing.T) string {
	t.Helper()
	claims := &middleware.WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    workerTokenTestIssuer,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: workerTokenTestBootstrapped,
		TenantID: "*",
		Role:     middleware.RoleWorker,
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(workerTokenTestSecret))
	if err != nil {
		t.Fatalf("failed to sign bootstrap token: %v", err)
	}
	return signed
}

// newWorkerTokenServer wires MintWorkerToken behind the same WorkerAuth
// middleware the production app.go does. Returns an http.Handler the
// test can drive with httptest.NewRecorder().
func newWorkerTokenServer(tg *mockTenantGetter, ttl time.Duration) http.Handler {
	h := &InternalHandler{
		tenantSvc:      tg,
		issuer:         workerTokenTestIssuer,
		activeKID:      "",
		workerTokenTTL: ttl,
		workerJWTConfig: middleware.WorkerJWTConfig{
			Secret: workerTokenTestSecret,
			Issuer: workerTokenTestIssuer,
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/internal/worker-token", h.MintWorkerToken)
	return middleware.WorkerAuth(middleware.WorkerJWTConfig{
		Secret: workerTokenTestSecret,
		Issuer: workerTokenTestIssuer,
	})(mux)
}

// postToken issues the request and decodes the typed response.
// Returns the recorder so the caller can assert on status / body
// bytes directly when the response isn't a valid WorkerTokenResponse.
func postToken(t *testing.T, srv http.Handler, bearer string, req WorkerTokenRequest) (*httptest.ResponseRecorder, *WorkerTokenResponse) {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}
	r := httptest.NewRequest("POST", "/api/internal/worker-token", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	var resp WorkerTokenResponse
	if w.Code == http.StatusOK && w.Body.Len() > 0 {
		bodyBytes, _ := io.ReadAll(w.Body)
		if err := json.Unmarshal(bodyBytes, &resp); err != nil {
			t.Fatalf("response body is not a valid WorkerTokenResponse: %v (body=%s)", err, bodyBytes)
		}
	}
	return w, &resp
}

// decodeIssuedToken parses the JWT using the production verifier.
// Load-bearing: pins the wire shape (alg=HS256, iss=edgecloud,
// exp/iat present, claims parseable).
func decodeIssuedToken(t *testing.T, signed string) *middleware.WorkerClaims {
	t.Helper()
	claims, err := middleware.VerifyWorkerJWT(signed, middleware.WorkerJWTConfig{
		Secret: workerTokenTestSecret,
		Issuer: workerTokenTestIssuer,
	})
	if err != nil {
		t.Fatalf("issued token failed to verify: %v", err)
	}
	if claims == nil {
		t.Fatalf("VerifyWorkerJWT returned nil claims")
	}
	if claims.Issuer != workerTokenTestIssuer {
		t.Fatalf("expected iss=%q, got %q", workerTokenTestIssuer, claims.Issuer)
	}
	if claims.ExpiresAt == nil {
		t.Fatalf("issued token has no exp claim")
	}
	if claims.IssuedAt == nil {
		t.Fatalf("issued token has no iat claim")
	}
	return claims
}

// -----------------------------------------------------------------------
// Test cases — issue #491 acceptance
// -----------------------------------------------------------------------

// Case 1 (happy path): POST {tenant_id: "t_real"} → 200 + token whose
// claims carry the requested tenant and the production-default TTL.
func TestMintWorkerToken_HappyPath(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)

	w, resp := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	if resp.Token == "" {
		t.Fatalf("expected non-empty token in response")
	}
	if resp.TenantID != "t_real" {
		t.Fatalf("expected echoed tenant_id=t_real, got %q", resp.TenantID)
	}
	if resp.ExpiresAt <= time.Now().Unix() {
		t.Fatalf("expires_at is in the past: %d", resp.ExpiresAt)
	}

	claims := decodeIssuedToken(t, resp.Token)
	if claims.TenantID != "t_real" {
		t.Fatalf("issued token carried tenant_id=%q, want t_real", claims.TenantID)
	}
	if claims.Role != middleware.RoleWorker {
		t.Fatalf("issued token carried role=%q, want %q", claims.Role, middleware.RoleWorker)
	}
	expMinusIat := claims.ExpiresAt.Time.Sub(claims.IssuedAt.Time)
	if expMinusIat < 14*time.Minute || expMinusIat > 16*time.Minute {
		t.Fatalf("exp - iat = %v, want ~15m", expMinusIat)
	}
}

// Case 2 (wildcard refusal): tenant_id="*" is rejected with 400 — the
// entire point of the endpoint's guard. A wildcard token would still
// pass VerifyWorkerJWT, but IsSharedWorker treats it as a "trusted
// shared worker" and Download / AutoRollback escalate access. We must
// not mint that primitive.
func TestMintWorkerToken_WildcardRefused(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"*": enabledTenant("*"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "*"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (body=%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "wildcard") {
		t.Fatalf("expected error to mention wildcard, got body=%s", w.Body.String())
	}
}

// Case 3 (empty refused): tenant_id="" is rejected with 400.
func TestMintWorkerToken_EmptyRefused(t *testing.T) {
	tg := &mockTenantGetter{}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: ""})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty tenant_id, got %d (body=%s)", w.Code, w.Body.String())
	}
}

// Case 4 (path traversal refused): "../etc" and similar shapes are
// rejected. Even though our regex disallows them, this test pins the
// safety net against an accidental loosening.
func TestMintWorkerToken_PathTraversalRefused(t *testing.T) {
	tg := &mockTenantGetter{}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	for _, bad := range []string{"../etc", "../../../", "/etc/passwd", "t_real/extra", "t_real\\bad", "T_UPPER"} {
		w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: bad})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for tenant_id=%q, got %d (body=%s)", bad, w.Code, w.Body.String())
		}
	}
}

// Case 5 (tenant not found): the CP holds the tenant-existence check
// upstream of the signing step so a typo in tenant_id returns 404
// instead of leaking a token into the wild.
func TestMintWorkerToken_TenantNotFound(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_phantom"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing tenant, got %d (body=%s)", w.Code, w.Body.String())
	}
}

// Case 5b (tenant disabled): same 404 surface for disabled tenants —
// DoSing a disabled tenant produces a flat 404 instead of minting a
// token the worker can't use.
func TestMintWorkerToken_TenantDisabled(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_disabled": disabledTenant("t_disabled"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_disabled"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for disabled tenant, got %d (body=%s)", w.Code, w.Body.String())
	}
}

// Case 6 (issued token verifies): the load-bearing wire-shape pin. If
// any future refactor changes the alg, the claim shape, or the
// verifier's expectations of the token, this test fails before the
// breakage reaches the worker.
func TestMintWorkerToken_IssuedTokenVerifies(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	_, resp := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})
	claims := decodeIssuedToken(t, resp.Token)

	if claims.WorkerID != workerTokenTestBootstrapped {
		t.Fatalf("expected worker_id propagated from input JWT, got %q", claims.WorkerID)
	}
	if !claims.ExpiresAt.Time.After(time.Now()) {
		t.Fatalf("issued token is already expired")
	}
}

// Case 6b (default TTL): with no operator override the mint produces
// a token whose exp - iat is within tolerance of 15m. Belt-and-braces
// pin so an environment sneak that flips the default to a different
// value does not silently ship.
func TestMintWorkerToken_DefaultTTL(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	_, resp := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})
	claims := decodeIssuedToken(t, resp.Token)

	expMinusIat := claims.ExpiresAt.Time.Sub(claims.IssuedAt.Time)
	if expMinusIat < 14*time.Minute || expMinusIat > 16*time.Minute {
		t.Fatalf("default TTL: exp - iat = %v, want ~15m", expMinusIat)
	}
}

// Case 7 (custom TTL): pinning the env-override path. With TTL=5m,
// the issued token's lifetime matches.
func TestMintWorkerToken_CustomTTL(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestCustomTTL)
	_, resp := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})
	claims := decodeIssuedToken(t, resp.Token)

	expMinusIat := claims.ExpiresAt.Time.Sub(claims.IssuedAt.Time)
	if expMinusIat < 4*time.Minute || expMinusIat > 6*time.Minute {
		t.Fatalf("custom TTL: exp - iat = %v, want ~5m", expMinusIat)
	}
}

// Case 8 (audit log): every success emits an audit-record with
// action="worker_token_mint" and outcome="success". Wire DefaultAuditor
// to a *service.Auditor whose repo is nil — Record is then a no-op
// (no panic), and we assert the handler doesn't blow up with the
// auditor wired. A full sqlmock capture belongs in an integration test.
func TestMintWorkerToken_AuditLog(t *testing.T) {
	oldAuditor := DefaultAuditor
	DefaultAuditor = service.NewAuditor(nil)
	defer func() { DefaultAuditor = oldAuditor }()

	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}

	// Refusal paths must also audit — smoke-test the failure branch.
	w2, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "*"})
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on wildcard, got %d", w2.Code)
	}
}

// Case 9 (auth gate): the endpoint must reject requests with no
// Bearer header. WorkerAuth already enforces this — confirming here
// pins the integration so a future routing change can't silently move
// the handler outside the auth chain.
func TestMintWorkerToken_RequiresBearer(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, "", WorkerTokenRequest{TenantID: "t_real"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without bearer, got %d", w.Code)
	}
}

// Case 10 (size guard): tenant_id longer than 64 chars is rejected.
// Pins the cap from isSafeTenantID.
func TestMintWorkerToken_LengthGuard(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, workerTokenTestDefaultTTL)
	long := strings.Repeat("a", 65)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: long})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for 65-char tenant_id, got %d", w.Code)
	}
}

// TestIsSafeTenantID exercises the guard helper in isolation — the
// handler-level tests cover the integration, but unit-testing each
// rejection branch directly pins the contract.
func TestIsSafeTenantID(t *testing.T) {
	cases := []struct {
		in         string
		wantReject bool
	}{
		{"t_real", false},
		{"t-tenant_1", false},
		{"a", false},
		{"", true},
		{"*", true},
		{"../etc", true},
		{"/etc", true},
		{`a\b`, true},
		{"T_UPPER", true},    // only [a-z0-9_-]
		{"with space", true}, // space
		{"with\ttab", true},  // tab
		{strings.Repeat("a", 64), false},
		{strings.Repeat("a", 65), true},
	}
	for _, tc := range cases {
		err := isSafeTenantID(tc.in)
		if tc.wantReject && err == nil {
			t.Errorf("isSafeTenantID(%q) = nil, want error", tc.in)
		}
		if !tc.wantReject && err != nil {
			t.Errorf("isSafeTenantID(%q) = %v, want nil", tc.in, err)
		}
	}
}
