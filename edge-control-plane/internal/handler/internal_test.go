package handler_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

// mockInternalDomainSvc implements handler.InternalDomainServiceInterface
// for testing. We test the handler wire shape (status codes, JSON
// encoding, route parameters) here; the service-layer logic is covered
// in service/domain_test.go.
type mockInternalDomainSvc struct {
	listAllDomainsFn func(ctx context.Context) ([]domain.Domain, error)
	isTlsAllowedFn   func(ctx context.Context, fqdn string) (bool, error)
	updateStatusFn   func(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error
}

func (m *mockInternalDomainSvc) ListAllDomains(ctx context.Context) ([]domain.Domain, error) {
	if m.listAllDomainsFn == nil {
		return nil, nil
	}
	return m.listAllDomainsFn(ctx)
}
func (m *mockInternalDomainSvc) IsTlsAllowed(ctx context.Context, fqdn string) (bool, error) {
	if m.isTlsAllowedFn == nil {
		return false, nil
	}
	return m.isTlsAllowedFn(ctx, fqdn)
}
func (m *mockInternalDomainSvc) UpdateStatus(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error {
	if m.updateStatusFn == nil {
		return nil
	}
	return m.updateStatusFn(ctx, id, status, lastError)
}

// newInternalHandler builds an InternalHandler whose only meaningful
// field is `domainSvc` — the deployment and worker services are nil,
// so the routes that need them are NOT exercised. The custom-domain
// routes under test here don't touch them.
func newInternalHandler(svc handler.InternalDomainServiceInterface) *handler.InternalHandler {
	// The production constructor panics if any service is nil. Tests
	// that exercise ONLY the custom-domain routes need a way to inject
	// only the domain service; the cleanest way is to use the typed
	// constructor and let the deployment/worker service be the zero
	// value (which the routes we test never touch).
	//
	// We rely on the InternalHandler struct's `domainSvc` being the
	// first thing the custom-domain routes read; the deployment
	// service is only read by Download, which is not in this test set.
	return handler.NewInternalHandler(nil, nil, svc, nil, nil, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil)
}

// TestInternal_ListDomains_HappyPath pins the array-shape contract that
// the ingress poller depends on. A future refactor that wraps the
// array in `{"domains": [...]}` would silently break the poller
// (each entry decodes as a map, not a Domain).
func TestInternal_ListDomains_HappyPath(t *testing.T) {
	svc := &mockInternalDomainSvc{
		listAllDomainsFn: func(ctx context.Context) ([]domain.Domain, error) {
			return []domain.Domain{
				{ID: "dom_1", TenantID: "t_a", AppName: "api", FQDN: "api.acme.com"},
				{ID: "dom_2", TenantID: "t_b", AppName: "web", FQDN: "web.acme.com"},
			}, nil
		},
	}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/domains", nil)
	rec := httptest.NewRecorder()
	h.ListDomains(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got []domain.Domain
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2", len(got))
	}
}

// TestInternal_TlsAllowed_HappyPath: the FQDN is registered and
// active, Caddy is asking "may I issue a cert?" — 200.
func TestInternal_TlsAllowed_HappyPath(t *testing.T) {
	svc := &mockInternalDomainSvc{
		isTlsAllowedFn: func(ctx context.Context, fqdn string) (bool, error) {
			if fqdn != "api.acme.com" {
				t.Errorf("fqdn = %q, want api.acme.com", fqdn)
			}
			return true, nil
		},
	}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/tls-allowed?fqdn=api.acme.com", nil)
	rec := httptest.NewRecorder()
	h.TlsAllowed(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

// TestInternal_TlsAllowed_NotFound: the FQDN is NOT registered — 404.
// This is the Caddy ask-URL's most common response for a fresh tenant
// who hasn't yet added the domain.
func TestInternal_TlsAllowed_NotFound(t *testing.T) {
	svc := &mockInternalDomainSvc{
		isTlsAllowedFn: func(ctx context.Context, fqdn string) (bool, error) {
			return false, nil
		},
	}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/tls-allowed?fqdn=api.acme.com", nil)
	rec := httptest.NewRecorder()
	h.TlsAllowed(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// TestInternal_TlsAllowed_AppDeletionCascadesToDomainRow pins the
// post-fix behaviour (PR #133 review finding #4, migration
// 011_domains_cascade.up.sql). Previously an orphaned domain row
// whose underlying (tenant, app) was deleted returned 200 from
// TlsAllowed, because there was no FK cascade from `apps` to
// `domains` — letting Caddy issue a cert for a hostname whose app
// no longer existed.
//
// The cascade now removes the domain row in the same transaction
// as the app deletion. `IsTlsAllowed` therefore sees no row, returns
// false, and the handler must answer 404. The mock here simulates
// the post-cascade state (no row → false); the migration is the
// real-world trigger.
func TestInternal_TlsAllowed_AppDeletionCascadesToDomainRow(t *testing.T) {
	svc := &mockInternalDomainSvc{
		isTlsAllowedFn: func(ctx context.Context, fqdn string) (bool, error) {
			// App deleted → cascade removed the domains row → no
			// match in `GetByFQDN` → IsTlsAllowed returns false.
			return false, nil
		},
	}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/tls-allowed?fqdn=api.acme.com", nil)
	rec := httptest.NewRecorder()
	h.TlsAllowed(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (app deletion must cascade to the domain row)", rec.Code)
	}
}

// TestInternal_TlsAllowed_MissingFQDN: empty query string — 400.
func TestInternal_TlsAllowed_MissingFQDN(t *testing.T) {
	svc := &mockInternalDomainSvc{}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/tls-allowed", nil)
	rec := httptest.NewRecorder()
	h.TlsAllowed(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// TestInternal_UpdateDomainStatus_NotFound_Returns404 is the regression
// pin for the v2 webhook path: a stale id (e.g. the domain was
// deleted between Caddy's first request and the post-issuance
// callback) must NOT silently look like success. The 204 the v1 code
// returned was a contract bug — the operator's "rows in failed
// state" alerts would never fire for these cases.
func TestInternal_UpdateDomainStatus_NotFound_Returns404(t *testing.T) {
	svc := &mockInternalDomainSvc{
		updateStatusFn: func(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error {
			return service.ErrDomainNotFound
		},
	}
	h := newInternalHandler(svc)
	body := `{"status":"active"}`
	req := httptest.NewRequest("POST", "/api/internal/domains/dom_missing/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "dom_missing")
	rec := httptest.NewRecorder()
	h.UpdateDomainStatus(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// TestInternal_UpdateDomainStatus_InvalidStatus_Returns400: only
// active|failed accepted; the v1 code already pins this.
func TestInternal_UpdateDomainStatus_InvalidStatus_Returns400(t *testing.T) {
	svc := &mockInternalDomainSvc{}
	h := newInternalHandler(svc)
	body := `{"status":"weird"}`
	req := httptest.NewRequest("POST", "/api/internal/domains/dom_x/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "dom_x")
	rec := httptest.NewRecorder()
	h.UpdateDomainStatus(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// TestInternal_UpdateDomainStatus_HappyPath: 204 on success.
func TestInternal_UpdateDomainStatus_HappyPath(t *testing.T) {
	called := false
	svc := &mockInternalDomainSvc{
		updateStatusFn: func(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error {
			called = true
			if id != "dom_x" {
				t.Errorf("id = %q, want dom_x", id)
			}
			if status != domain.DomainStatusActive {
				t.Errorf("status = %q, want active", status)
			}
			return nil
		},
	}
	h := newInternalHandler(svc)
	body := `{"status":"active"}`
	req := httptest.NewRequest("POST", "/api/internal/domains/dom_x/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "dom_x")
	rec := httptest.NewRecorder()
	h.UpdateDomainStatus(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
	if !called {
		t.Errorf("UpdateStatus was not called")
	}
}

// TestInternal_UpdateDomainStatus_InternalError_Returns500: any
// non-sentinel error must surface as 500, never as 4xx.
func TestInternal_UpdateDomainStatus_InternalError_Returns500(t *testing.T) {
	svc := &mockInternalDomainSvc{
		updateStatusFn: func(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error {
			return errors.New("db boom")
		},
	}
	h := newInternalHandler(svc)
	body := `{"status":"active"}`
	req := httptest.NewRequest("POST", "/api/internal/domains/dom_x/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "dom_x")
	rec := httptest.NewRecorder()
	h.UpdateDomainStatus(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

// ── Bootstrap handshake tests (issue #104) ───────────────────────────────

// testBootstrapSecret is a shared bootstrap secret for tests.
const testBootstrapSecret = "test-bootstrap-secret-that-is-long-enough-32!"

// signBootstrapPayload computes the HMAC-SHA256 signature for a bootstrap
// request, matching the CP-side verification in internal.go's Bootstrap handler.
func signBootstrapPayload(workerID, region, tenantID, timestamp, nonce, secret string) string {
	payload := workerID + ":" + region + ":" + tenantID + ":" + timestamp + ":" + nonce
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// withBootstrapCtx attaches the same context values BootstrapAuth middleware
// would after validating a bootstrap JWT: worker_id and tenant_id.
func withBootstrapCtx(workerID, tenantID, region string) func(*http.Request) *http.Request {
	return func(r *http.Request) *http.Request {
		ctx := context.WithValue(r.Context(), middleware.WorkerIDKey, workerID)
		ctx = context.WithValue(ctx, middleware.WorkerTenantIDKey, tenantID)
		ctx = context.WithValue(ctx, middleware.WorkerRegionKey, region)
		return r.WithContext(ctx)
	}
}

// TestInternal_Bootstrap_NotConfigured returns 501 when bootstrap secret is empty.
func TestInternal_Bootstrap_NotConfigured(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil)
	body := `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def"}`
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

// TestInternal_Bootstrap_MissingFields returns 400.
func TestInternal_Bootstrap_MissingFields(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "", middleware.WorkerJWTConfig{}, 0, "", "", nil)
	tests := []struct {
		name string
		body string
	}{
		{"empty body", `{}`},
		{"missing worker_id", `{"region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def"}`},
		{"missing region", `{"worker_id":"w_test","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def"}`},
		{"missing tenant_id", `{"worker_id":"w_test","region":"fra","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def"}`},
		{"missing timestamp", `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","nonce":"abc","signature":"def"}`},
		{"missing nonce", `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","signature":"def"}`},
		{"missing signature", `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/internal/bootstrap", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			h.Bootstrap(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
		})
	}
}

// TestInternal_Bootstrap_InvalidTimestampFormat returns 400 when timestamp
// is not valid RFC3339.
func TestInternal_Bootstrap_InvalidTimestampFormat(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "", middleware.WorkerJWTConfig{}, 0, "", "", nil)
	body := `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"not-a-timestamp","nonce":"abc","signature":"def"}`
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// TestInternal_Bootstrap_StaleTimestamp returns 400 when timestamp is >5min old.
func TestInternal_Bootstrap_StaleTimestamp(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "", middleware.WorkerJWTConfig{}, 0, "", "", nil)
	oldTime := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
	sig := signBootstrapPayload("w_test", "fra", "t_test", oldTime, "abc", testBootstrapSecret)
	body := `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"` + oldTime + `","nonce":"abc","signature":"` + sig + `"}`
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// TestInternal_Bootstrap_InvalidSignature returns 401.
func TestInternal_Bootstrap_InvalidSignature(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "", middleware.WorkerJWTConfig{}, 0, "", "", nil)
	now := time.Now().Format(time.RFC3339)
	body := `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"` + now + `","nonce":"abc","signature":"wrong-signature"}`
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// TestInternal_Bootstrap_Success returns 200 with a JWT token.
func TestInternal_Bootstrap_Success(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "real-jwt-secret", middleware.WorkerJWTConfig{}, 0, "", "", nil)
	now := time.Now().Format(time.RFC3339)
	sig := signBootstrapPayload("w_test_abc", "fra", "t_test", now, "unique-nonce", testBootstrapSecret)
	bodyMap := map[string]string{
		"worker_id": "w_test_abc",
		"region":    "fra",
		"tenant_id": "t_test",
		"timestamp": now,
		"nonce":     "unique-nonce",
		"signature": sig,
	}
	bodyBytes, _ := json.Marshal(bodyMap)
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	token, ok := resp["token"]
	if !ok || token == "" {
		t.Fatal("response missing 'token' field")
	}
	// Verify the token is a valid bootstrap JWT signed with the bootstrap secret.
	claims, err := middleware.VerifyBootstrapJWT(token, middleware.BootstrapJWTConfig{
		BootstrapSecret: testBootstrapSecret,
		Issuer:          "edgecloud-bootstrap",
	})
	if err != nil {
		t.Fatalf("verify bootstrap JWT: %v", err)
	}
	if claims.WorkerID != "w_test_abc" {
		t.Errorf("WorkerID = %q, want w_test_abc", claims.WorkerID)
	}
	if claims.TenantID != "t_test" {
		t.Errorf("TenantID = %q, want t_test", claims.TenantID)
	}
	if claims.Region != "fra" {
		t.Errorf("Region = %q, want fra", claims.Region)
	}
}

// TestInternal_WorkerSecret_NotConfigured returns 501 when bootstrap is not set.
func TestInternal_WorkerSecret_NotConfigured(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil)
	req := httptest.NewRequest("GET", "/api/internal/worker-secret", nil)
	rec := httptest.NewRecorder()
	h.WorkerSecret(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

// TestInternal_WorkerSecret_Success returns 200 with the JWT secret.
func TestInternal_WorkerSecret_Success(t *testing.T) {
	jwtSecret := "the-real-jwt-secret-that-is-at-least-32-b"
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, jwtSecret, middleware.WorkerJWTConfig{}, 0, "", "", nil)

	// Issue a bootstrap JWT the same way the Bootstrap handler would.
	cfg := middleware.BootstrapJWTConfig{
		BootstrapSecret: testBootstrapSecret,
		Issuer:          "edgecloud-bootstrap",
	}
	token, err := middleware.IssueBootstrapJWT(cfg, "w_test_abc", "t_test", "fra")
	if err != nil {
		t.Fatalf("issue bootstrap JWT: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/internal/worker-secret", nil)
	req = withBootstrapCtx("w_test_abc", "t_test", "fra")(req)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.WorkerSecret(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	gotSecret, ok := resp["secret"]
	if !ok || gotSecret == "" {
		t.Fatal("response missing 'secret' field")
	}
	if gotSecret != jwtSecret {
		t.Errorf("secret = %q, want %q", gotSecret, jwtSecret)
	}
	// Cache-Control header must be set to no-store.
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
}
