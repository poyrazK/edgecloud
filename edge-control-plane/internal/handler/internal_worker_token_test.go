package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// mockHostingGetter exercises the issue #491 constraint #2 gate
// without standing up the full *service.WorkerService. The `tenants`
// slice is the worker's "hosted set" — what TenantsHostedBy returns
// from worker_status.apps where status='running'. Tests construct
// the slice to express either "this worker hosts the requested
// tenant" (test passes) or "this worker does NOT host the requested
// tenant" (test asserts 403).
type mockHostingGetter struct {
	tenants []string
	err     error
}

func (m *mockHostingGetter) TenantsHostedBy(_ context.Context, _ string) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.tenants, nil
}

// hostTenant is the standard "this worker hosts exactly t_real" fixture
// used by every pre-#491 mint endpoint happy-path test. Centralized so
// a future tightening of the hosting check (e.g. requiring > 1 minute
// of running) only needs one fixture update.
func hostTenant(tenants ...string) *mockHostingGetter {
	return &mockHostingGetter{tenants: tenants}
}

// noHostedTenants pins the "worker has never heartbeated" /
// "worker hosts nothing" path used by the 403 tests.
func noHostedTenants() *mockHostingGetter {
	return &mockHostingGetter{tenants: nil}
}

// nilOnMissingTenantGetter mirrors the production
// repository.TenantRepository.GetByID contract: not-found returns
// (nil, nil), not a typed error. Used by
// TestMintWorkerToken_NilTenantDoesNotPanic to regression-pin the
// service-layer (nil, nil) → ErrTenantNotFound translation that
// PR #491 review flagged as a handler-side nil-deref foot-gun.
type nilOnMissingTenantGetter struct{}

func (nilOnMissingTenantGetter) GetByID(_ context.Context, _ string) (*domain.Tenant, error) {
	return nil, nil
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
	workerTokenTestSecret = "test-secret-must-be-at-least-32-bytes-long!"
	workerTokenTestIssuer = "edgecloud"
	// workerTokenTestPubkey is the hex-encoded Ed25519 public key
	// the test server's WorkerKeyCache returns. Tests that need a
	// different pubkey (e.g. for a kid-mismatch case) override
	// the cache loader at the call site.
	workerTokenTestPubkey       = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
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
// test can drive with httptest.NewRecorder(). The tenantGetter and
// hostingGetter interfaces are what production *service.TenantService
// and *service.WorkerService satisfy; accepting them here lets tests
// substitute any narrow contract implementation (mockTenantGetter,
// nilOnMissingTenantGetter, mockHostingGetter, …) without copying
// the full service-graph setup.
func newWorkerTokenServer(tg tenantGetter, hg hostingGetter, ttl time.Duration) http.Handler {
	// Issue #430: every test server needs a WorkerKeyCache backing
	// the per-worker derivation path. The cache's loader returns
	// the same pubkey the bootstrap handshake would have persisted
	// on the workers row, so the derived signing key matches the
	// production computation byte-for-byte. Tests that need a
	// non-enrolled worker can override the cache's loader.
	keyCache := middleware.NewWorkerKeyCache(func(ctx context.Context, workerID string) (string, error) {
		return workerTokenTestPubkey, nil
	})
	h := &InternalHandler{
		tenantSvc:        tg,
		workerHostingSvc: hg,
		issuer:           workerTokenTestIssuer,
		activeKID:        "",
		workerTokenTTL:   ttl,
		workerJWTConfig: middleware.WorkerJWTConfig{
			Secret:         workerTokenTestSecret,
			Issuer:         workerTokenTestIssuer,
			WorkerKeyCache: keyCache,
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/internal/tokens/tenant", h.MintWorkerToken)
	return middleware.WorkerAuth(middleware.WorkerJWTConfig{
		Secret:         workerTokenTestSecret,
		Issuer:         workerTokenTestIssuer,
		WorkerKeyCache: keyCache,
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
	r := httptest.NewRequest("POST", "/api/internal/tokens/tenant", bytes.NewReader(body))
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
	// Issue #430: the minted token carries a wkr_ kid, so verify
	// must use the same WorkerKeyCache the mint path used. The
	// cache loader returns the test pubkey the mint handler also
	// saw — derivation is byte-for-byte symmetric.
	verifyCache := middleware.NewWorkerKeyCache(func(ctx context.Context, workerID string) (string, error) {
		return workerTokenTestPubkey, nil
	})
	claims, err := middleware.VerifyWorkerJWT(signed, middleware.WorkerJWTConfig{
		Secret:         workerTokenTestSecret,
		Issuer:         workerTokenTestIssuer,
		WorkerKeyCache: verifyCache,
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
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestDefaultTTL)

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
	expMinusIat := claims.ExpiresAt.Sub(claims.IssuedAt.Time)
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
	srv := newWorkerTokenServer(tg, hostTenant("*"), workerTokenTestDefaultTTL)
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
	srv := newWorkerTokenServer(tg, noHostedTenants(), workerTokenTestDefaultTTL)
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
	srv := newWorkerTokenServer(tg, noHostedTenants(), workerTokenTestDefaultTTL)
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
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestDefaultTTL)
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
	srv := newWorkerTokenServer(tg, hostTenant("t_disabled"), workerTokenTestDefaultTTL)
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
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestDefaultTTL)
	_, resp := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})
	claims := decodeIssuedToken(t, resp.Token)

	if claims.WorkerID != workerTokenTestBootstrapped {
		t.Fatalf("expected worker_id propagated from input JWT, got %q", claims.WorkerID)
	}
	if !claims.ExpiresAt.After(time.Now()) {
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
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestDefaultTTL)
	_, resp := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})
	claims := decodeIssuedToken(t, resp.Token)

	expMinusIat := claims.ExpiresAt.Sub(claims.IssuedAt.Time)
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
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestCustomTTL)
	_, resp := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})
	claims := decodeIssuedToken(t, resp.Token)

	expMinusIat := claims.ExpiresAt.Sub(claims.IssuedAt.Time)
	if expMinusIat < 4*time.Minute || expMinusIat > 6*time.Minute {
		t.Fatalf("custom TTL: exp - iat = %v, want ~5m", expMinusIat)
	}
}

// Case 8 (audit log): every success emits an audit-record with
// action="worker_token_mint" and outcome="success". Wires
// DefaultAuditor to a custom AuditRecorder spy that captures every
// Record call so the assertion matches the implementation, not the
// "no panic" smoke-test the previous version was actually exercising.
//
// The spy is in-package (declared below) — the AuditRecorder seam
// added to audithelper.go lets us substitute without spinning up
// sqlmock just to assert a single Record call.
func TestMintWorkerToken_AuditLog_SuccessCaptured(t *testing.T) {
	spy := &auditSpy{}
	oldAuditor := DefaultAuditor
	DefaultAuditor = spy
	defer func() { DefaultAuditor = oldAuditor }()

	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if got := len(spy.events); got != 1 {
		t.Fatalf("expected 1 audit event, got %d (%+v)", got, spy.events)
	}
	ev := spy.events[0]
	if ev.Action != "worker_token_mint" {
		t.Errorf("expected Action=worker_token_mint, got %q", ev.Action)
	}
	if ev.Outcome != "success" {
		t.Errorf("expected Outcome=success, got %q", ev.Outcome)
	}
	if ev.ResourceID != "t_real" {
		t.Errorf("expected ResourceID=t_real, got %q", ev.ResourceID)
	}
	if !strings.Contains(ev.Details, workerTokenTestBootstrapped) {
		t.Errorf("expected Details to mention worker_id %q, got %q", workerTokenTestBootstrapped, ev.Details)
	}
}

// Case 8b (audit log — failure path): the wildcard refusal still
// emits an audit record with outcome="failure". Closes the
// audit-log visibility hole on a class of inputs that 400s upstream
// of signing.
func TestMintWorkerToken_AuditLog_FailureCaptured(t *testing.T) {
	spy := &auditSpy{}
	oldAuditor := DefaultAuditor
	DefaultAuditor = spy
	defer func() { DefaultAuditor = oldAuditor }()

	tg := &mockTenantGetter{}
	srv := newWorkerTokenServer(tg, noHostedTenants(), workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "*"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}

	if got := len(spy.events); got != 1 {
		t.Fatalf("expected 1 audit event, got %d (%+v)", got, spy.events)
	}
	if spy.events[0].Outcome != "failure" {
		t.Errorf("expected Outcome=failure, got %q", spy.events[0].Outcome)
	}
}

// auditSpy is the in-package AuditRecorder used by the mint-endpoint
// audit tests. Append-only; tests assert on the captured slice.
type auditSpy struct {
	events []service.AuditInfo
}

func (s *auditSpy) Record(info service.AuditInfo) {
	s.events = append(s.events, info)
}

// TestAuditRecord_NilAuditor pins the no-panic contract from
// audithelper_test.go:38 — moved here because the new
// AuditRecorder interface seam (vs. the old *service.Auditor) could
// regress the nil-check.
func TestAuditRecord_NilAuditor_AfterSeam(t *testing.T) {
	oldAuditor := DefaultAuditor
	DefaultAuditor = nil
	defer func() { DefaultAuditor = oldAuditor }()

	// Should not panic when auditor is nil.
	auditRecord(httptest.NewRequest("POST", "/x", nil), "test", "x", "y", "", "success")
}

// Case 5c (nil-tenant contract): the production
// repository.TenantRepository.GetByID returns (nil, nil) for
// not-found rows (not a typed error). The service-layer
// translation added in the previous commit turns that into
// ErrTenantNotFound, but the handler-side fixture
// nilOnMissingTenantGetter explicitly mirrors the production
// shape so this test catches a regression where the
// service-layer translation is bypassed (e.g. someone wires the
// raw repo back into the handler). Without the translation,
// MintWorkerToken would nil-deref on `t.DisabledAt` and panic
// the request goroutine — which the earlier mockTenantGetter
// (which returns ErrTenantNotFound directly) was hiding.
func TestMintWorkerToken_NilTenantDoesNotPanic(t *testing.T) {
	// nilOnMissingTenantGetter mirrors repository.TenantRepository.GetByID:
	// not-found returns (nil, nil), no error.
	srv := newWorkerTokenServer(nilOnMissingTenantGetter{}, hostTenant("t_phantom"), workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_phantom"})

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nil-tenant not-found, got %d (body=%s)",
			w.Code, w.Body.String())
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
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestDefaultTTL)
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
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestDefaultTTL)
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

// -----------------------------------------------------------------------
// Hosting constraint (issue #491 constraint #2)
// -----------------------------------------------------------------------

// TestMintWorkerToken_Hosting_Hosted_Success pins the happy path:
// the worker IS hosting the requested tenant. The constraint passes,
// signing proceeds, and the test sees the same 200 it would have
// seen before constraint #2 was added. Regression guard: a future
// tightening (e.g. requiring > 1 minute of running time) must keep
// this case green.
func TestMintWorkerToken_Hosting_Hosted_Success(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestDefaultTTL)
	w, resp := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	if resp.Token == "" {
		t.Fatalf("expected non-empty token")
	}
}

// TestMintWorkerToken_Hosting_NotHosted_403 is the load-bearing test:
// a worker asks for a tenant it doesn't host and must be refused
// with 403 (not 200, not 404, not 500). Without this gate, a
// compromised worker could mint tokens for tenants it has no
// relationship with.
func TestMintWorkerToken_Hosting_NotHosted_403(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		// t_real EXISTS (so we get past the 404 existence check) but
		// the worker hosts t_other, not t_real.
		"t_real":  enabledTenant("t_real"),
		"t_other": enabledTenant("t_other"),
	}}
	srv := newWorkerTokenServer(tg, hostTenant("t_other"), workerTokenTestDefaultTTL)
	w, resp := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d (body=%s)", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "not hosted") {
		t.Errorf("expected error to mention 'not hosted', got body=%s", w.Body.String())
	}
	if resp.Token != "" {
		t.Errorf("expected no token in 403 response, got token")
	}
}

// TestMintWorkerToken_Hosting_NoHeartbeatYet_403 pins the freshly-
// bootstrapped worker path: a worker that has registered but never
// heartbeated has no worker_status.apps entries, so
// TenantsHostedBy returns ([]string{}, nil). The handler must treat
// that as 403 for ANY tenant request — the worker must heartbeat
// first.
func TestMintWorkerToken_Hosting_NoHeartbeatYet_403(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, noHostedTenants(), workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for no-heartbeat-yet worker, got %d (body=%s)",
			w.Code, w.Body.String())
	}
}

// TestMintWorkerToken_Hosting_BootstrapWildcard_Allowed pins the
// design decision that the hosting check applies to ALL callers,
// including the inbound wildcard JWT. A worker with a wildcard
// inbound JWT that hosts the requested tenant must still be able to
// mint — the inbound wildcard is the bootstrap case, not an exemption
// from the hosting check.
func TestMintWorkerToken_Hosting_BootstrapWildcard_Allowed(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, hostTenant("t_real"), workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for wildcard inbound JWT that hosts t_real, got %d",
			w.Code)
	}
}

// TestMintWorkerToken_Hosting_BootstrapWildcard_NotHosted pins the
// "wildcard inbound JWT but worker doesn't host" path: the hosting
// check must still fire. Skipping the check for wildcard callers
// would re-open the cross-tenant primitive for every freshly-
// bootstrapped worker, defeating #491's entire goal.
func TestMintWorkerToken_Hosting_BootstrapWildcard_NotHosted(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, noHostedTenants(), workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for wildcard inbound JWT that doesn't host, got %d",
			w.Code)
	}
}

// TestMintWorkerToken_Hosting_ScopedJWTStillRechecks pins the
// "worker previously minted a token for tenant A; now asks for tenant
// B (which it doesn't host)" path. The inbound scoped JWT does NOT
// bypass the hosting check. Mirrors the wildcard case — every
// caller is subject to the constraint.
func TestMintWorkerToken_Hosting_ScopedJWTStillRechecks(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_b": enabledTenant("t_b"),
	}}
	srv := newWorkerTokenServer(tg, hostTenant("t_a"), workerTokenTestDefaultTTL)

	// Inbound JWT scoped to t_a (the worker hosts t_a only).
	claims := &middleware.WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    workerTokenTestIssuer,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		},
		WorkerID: workerTokenTestBootstrapped,
		TenantID: "t_a",
		Role:     middleware.RoleWorker,
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(workerTokenTestSecret))
	if err != nil {
		t.Fatalf("sign scoped JWT: %v", err)
	}

	w, _ := postToken(t, srv, signed, WorkerTokenRequest{TenantID: "t_b"})

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for scoped JWT asking for non-hosted tenant, got %d",
			w.Code)
	}
}

// TestMintWorkerToken_Hosting_DisabledButHosted_404 pins the
// "disabled tenant wins over hosting" invariant: the existing 404
// surface for disabled tenants must NOT be downgraded to 403 just
// because the worker hosts the tenant. Disabling a tenant must
// always produce 404 to mask its existence from probing workers.
func TestMintWorkerToken_Hosting_DisabledButHosted_404(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_disabled": disabledTenant("t_disabled"),
	}}
	srv := newWorkerTokenServer(tg, hostTenant("t_disabled"), workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_disabled"})

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 (disabled wins), got %d", w.Code)
	}
}

// TestMintWorkerToken_Hosting_MissingTenantWinsOverNotHosted pins
// the order: tenant-existence 404 must fire BEFORE the hosting 403.
// A request for a non-existent tenant_id must return 404, not 403 —
// otherwise 403 becomes a tenant-existence oracle ("403 = tenant
// exists but I don't host it; 404 = tenant doesn't exist").
func TestMintWorkerToken_Hosting_MissingTenantWinsOverNotHosted(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, noHostedTenants(), workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_phantom"})

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing tenant (regardless of hosting), got %d", w.Code)
	}
}

// TestMintWorkerToken_Hosting_AuditFailureCaptured pins the audit
// log shape on a 403: the existing AuditRecorder seam must capture
// the event with Action="worker_token_mint", Outcome="failure",
// and a Details string that contains "hosting check failed" so
// operators can grep for the denial reason.
func TestMintWorkerToken_Hosting_AuditFailureCaptured(t *testing.T) {
	spy := &auditSpy{}
	oldAuditor := DefaultAuditor
	DefaultAuditor = spy
	defer func() { DefaultAuditor = oldAuditor }()

	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, noHostedTenants(), workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
	if got := len(spy.events); got != 1 {
		t.Fatalf("expected 1 audit event, got %d (%+v)", got, spy.events)
	}
	ev := spy.events[0]
	if ev.Action != "worker_token_mint" {
		t.Errorf("expected Action=worker_token_mint, got %q", ev.Action)
	}
	if ev.Outcome != "failure" {
		t.Errorf("expected Outcome=failure, got %q", ev.Outcome)
	}
	if !strings.Contains(ev.Details, "hosting check failed") {
		t.Errorf("expected Details to contain 'hosting check failed', got %q", ev.Details)
	}
}

// TestMintWorkerToken_Hosting_RepoError_500 pins the fail-closed
// contract: a DB error from TenantsHostedBy must produce 500, NOT a
// best-guess 403 ("deny when in doubt"). Returning 403 on a DB error
// would silently lock the worker out of legitimate mints during a
// database incident.
func TestMintWorkerToken_Hosting_RepoError_500(t *testing.T) {
	hg := &mockHostingGetter{
		err: errors.New("db unavailable"),
	}
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	srv := newWorkerTokenServer(tg, hg, workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on hosting-repo error, got %d (body=%s)",
			w.Code, w.Body.String())
	}
}

// TestMintWorkerToken_Hosting_NilServiceSkipsGate pins the
// defensive nil-check: the handler is structured so the hosting
// check is skipped (not panicked) when workerHostingSvc is nil. This
// preserves the behavior of tests that build an InternalHandler by
// hand without the full dependency graph, and keeps the production
// wiring as the only required setup path.
func TestMintWorkerToken_Hosting_NilServiceSkipsGate(t *testing.T) {
	tg := &mockTenantGetter{tenants: map[string]*domain.Tenant{
		"t_real": enabledTenant("t_real"),
	}}
	// hg = nil — the handler's nil check should skip the gate.
	srv := newWorkerTokenServer(tg, nil, workerTokenTestDefaultTTL)
	w, _ := postToken(t, srv, bootstrapToken(t), WorkerTokenRequest{TenantID: "t_real"})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 when hosting service is nil (gate skipped), got %d",
			w.Code)
	}
}
