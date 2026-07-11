package handler_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
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
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
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
	return handler.NewInternalHandler(nil, nil, svc, nil, nil, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)
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
// request, matching the CP-side verification in internal.go's Bootstrap
// handler. The public_key argument is the hex-encoded Ed25519 pubkey
// the worker is enrolling — issue #430 adds it to the HMAC coverage
// so a phase-1 / phase-2 swap to a different keypair is detectable
// at HMAC-verify time. Pass an empty string to omit (pre-#430 tests
// that want to exercise the "missing public_key" 400 path).
func signBootstrapPayload(workerID, region, tenantID, timestamp, nonce, publicKey, secret string) string {
	payload := workerID + ":" + region + ":" + tenantID + ":" + timestamp + ":" + nonce
	if publicKey != "" {
		payload += ":" + publicKey
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// testPubkeyHex is a fixed 32-byte (64-hex-char) Ed25519 public key
// used by the bootstrap / enroll tests. The corresponding private
// key is not needed — these tests never sign challenges, only verify
// the request shape and HMAC coverage.
const testPubkeyHex = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"

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
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)
	body := `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def","public_key":"` + testPubkeyHex + `"}`
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

// TestInternal_Bootstrap_MissingFields returns 400.
func TestInternal_Bootstrap_MissingFields(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)
	tests := []struct {
		name string
		body string
	}{
		{"empty body", `{}`},
		{"missing worker_id", `{"region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def","public_key":"` + testPubkeyHex + `"}`},
		{"missing region", `{"worker_id":"w_test","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def","public_key":"` + testPubkeyHex + `"}`},
		{"missing tenant_id", `{"worker_id":"w_test","region":"fra","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def","public_key":"` + testPubkeyHex + `"}`},
		{"missing timestamp", `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","nonce":"abc","signature":"def","public_key":"` + testPubkeyHex + `"}`},
		{"missing nonce", `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","signature":"def","public_key":"` + testPubkeyHex + `"}`},
		{"missing signature", `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","public_key":"` + testPubkeyHex + `"}`},
		// issue #430: public_key is now required (every worker must
		// enroll a keypair during bootstrap). A missing field is a
		// clean 400, not a 401.
		{"missing public_key", `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def"}`},
		// malformed public_key (wrong length) is a 400 from the
		// pre-HMAC validation — we want a clean error here rather
		// than a 401 from a HMAC payload mismatch.
		{"public_key wrong length", `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def","public_key":"deadbeef"}`},
		{"public_key not hex", `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"2026-07-06T12:00:00Z","nonce":"abc","signature":"def","public_key":"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/internal/bootstrap", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			h.Bootstrap(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestInternal_Bootstrap_InvalidTimestampFormat returns 400 when timestamp
// is not valid RFC3339.
func TestInternal_Bootstrap_InvalidTimestampFormat(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)
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
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)
	oldTime := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
	sig := signBootstrapPayload("w_test", "fra", "t_test", oldTime, "abc", testPubkeyHex, testBootstrapSecret)
	body := `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"` + oldTime + `","nonce":"abc","signature":"` + sig + `","public_key":"` + testPubkeyHex + `"}`
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// TestInternal_Bootstrap_InvalidSignature returns 401.
func TestInternal_Bootstrap_InvalidSignature(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)
	now := time.Now().Format(time.RFC3339)
	body := `{"worker_id":"w_test","region":"fra","tenant_id":"t_test","timestamp":"` + now + `","nonce":"abc","signature":"wrong-signature","public_key":"` + testPubkeyHex + `"}`
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// TestInternal_Bootstrap_Success returns 200 with a JWT token + challenge.
func TestInternal_Bootstrap_Success(t *testing.T) {
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, "real-jwt-secret", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)
	now := time.Now().Format(time.RFC3339)
	sig := signBootstrapPayload("w_test_abc", "fra", "t_test", now, "unique-nonce", testPubkeyHex, testBootstrapSecret)
	bodyMap := map[string]string{
		"worker_id":  "w_test_abc",
		"region":     "fra",
		"tenant_id":  "t_test",
		"timestamp":  now,
		"nonce":      "unique-nonce",
		"signature":  sig,
		"public_key": testPubkeyHex,
	}
	bodyBytes, _ := json.Marshal(bodyMap)
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	token, ok := resp["token"].(string)
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

	// Issue #430: phase 1 must also return an enrollment challenge —
	// base64 32 bytes. The challenge is the second authn gate for
	// phase 2; without it the bootstrap JWT alone is useless.
	challenge, ok := resp["enrollment_challenge"].(string)
	if !ok || challenge == "" {
		t.Fatal("response missing 'enrollment_challenge' field")
	}
	challengeBytes, err := base64.RawURLEncoding.DecodeString(challenge)
	if err != nil {
		t.Fatalf("enrollment_challenge is not base64url: %v", err)
	}
	if len(challengeBytes) != 32 {
		t.Errorf("challenge len = %d, want 32", len(challengeBytes))
	}
	expiresAt, ok := resp["challenge_expires_at"].(float64)
	if !ok || expiresAt == 0 {
		t.Fatal("response missing 'challenge_expires_at' field")
	}
	if expiresAt < float64(time.Now().Unix()) {
		t.Errorf("challenge_expires_at = %v, want > now", expiresAt)
	}
}

// ── Worker enrollment tests (issue #430) ───────────────────────────────

// fakeWorkerKeyRepo is the mock workerKeyRepo used by the EnrollWorker
// tests. The handler depends on a single SetPublicKey call (the
// enrollment writes the worker's hex pubkey to the workers row),
// and the affected-row count is what the handler uses to detect
// "worker not registered". The tests exercise both the 1-row and
// 0-row paths.
type fakeWorkerKeyRepo struct {
	setPublicKeyFn func(ctx context.Context, id, publicKeyHex string) (int64, error)
	setCalls       []workerKeySetCall
}

type workerKeySetCall struct {
	id     string
	pubkey string
}

func (f *fakeWorkerKeyRepo) SetPublicKey(ctx context.Context, id, publicKeyHex string) (int64, error) {
	f.setCalls = append(f.setCalls, workerKeySetCall{id: id, pubkey: publicKeyHex})
	if f.setPublicKeyFn == nil {
		return 1, nil
	}
	return f.setPublicKeyFn(ctx, id, publicKeyHex)
}

// makeEnrollmentRequest walks a worker through phase 1 (bootstrap) so
// phase 2 (enroll) has a real challenge to consume. Returns the
// challenge string + a hex-encoded Ed25519 pubkey whose private half
// is also returned — tests that need to sign the challenge call
// ed25519.Sign on the returned privkey.
func makeEnrollmentRequest(t *testing.T, h *handler.InternalHandler, workerID, tenantID, region string) (challengeB64 string, priv ed25519.PrivateKey) {
	t.Helper()
	pub, p, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	pubHex := hex.EncodeToString(pub)
	now := time.Now().Format(time.RFC3339)
	nonce := "nonce-" + workerID + "-" + tenantID
	sig := signBootstrapPayload(workerID, region, tenantID, now, nonce, pubHex, testBootstrapSecret)
	bodyMap := map[string]string{
		"worker_id": workerID, "region": region, "tenant_id": tenantID,
		"timestamp": now, "nonce": nonce, "signature": sig, "public_key": pubHex,
	}
	bodyBytes, _ := json.Marshal(bodyMap)
	req := httptest.NewRequest("POST", "/api/internal/bootstrap", bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()
	h.Bootstrap(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("phase1: status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode phase1 response: %v", err)
	}
	ch, ok := resp["enrollment_challenge"].(string)
	if !ok {
		t.Fatalf("phase1: missing enrollment_challenge; resp=%v", resp)
	}
	return ch, p
}

// signEnrollmentWithPriv computes the Ed25519 signature over
// sha256(public_key || challenge) that the CP verifies at enroll
// time. The priv/pub pair is supplied explicitly because each test
// scenario generates its own keypair — there's no "look up priv by
// pub" path. Kept separate from makeEnrollmentRequest so other
// tests can sign challenges without going through the bootstrap
// phase-1 path (e.g. unit tests on EnrollWorker that want to skip
// the JWT issuance).
func signEnrollmentWithPriv(priv ed25519.PrivateKey, pub ed25519.PublicKey, challenge []byte) []byte {
	h := sha256.New()
	h.Write(pub)
	h.Write(challenge)
	return ed25519.Sign(priv, h.Sum(nil))
}

// TestInternal_EnrollWorker_Success_WithPubkey is the happy path:
// the worker holds a valid bootstrap JWT + a fresh challenge, signs
// the challenge with its Ed25519 private key, and the CP returns
// the HKDF-derived per-worker HS256 secret. Verifies:
//
//  1. Response kid matches the public-key fingerprint (wkr_ + 8 hex).
//  2. Returned secret, when base64-decoded, matches an independent
//     signing.DeriveWorkerSecret recompute — the CP didn't lie about
//     what it derived.
//  3. workers.public_key was persisted (SetPublicKey called once).
func TestInternal_EnrollWorker_Success_WithPubkey(t *testing.T) {
	jwtSecret := "test-cluster-master-secret-that-is-long-enough-32-bytes!"
	repo := &fakeWorkerKeyRepo{}
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, jwtSecret, middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, repo)

	workerID := "w_test_enroll"
	tenantID := "t_test"
	region := "fra"
	challengeB64, priv := makeEnrollmentRequest(t, h, workerID, tenantID, region)

	pub := priv.Public().(ed25519.PublicKey)
	pubHex := hex.EncodeToString(pub)
	challengeBytes, err := base64.RawURLEncoding.DecodeString(challengeB64)
	if err != nil {
		t.Fatalf("decode challenge: %v", err)
	}
	sig := signEnrollmentWithPriv(priv, pub, challengeBytes)

	bodyMap := map[string]string{
		"worker_id":            workerID,
		"public_key":           pubHex,
		"enrollment_challenge": challengeB64,
		"signature":            hex.EncodeToString(sig),
	}
	bodyBytes, _ := json.Marshal(bodyMap)
	req := httptest.NewRequest("POST", "/api/internal/worker-bootstrap/enroll", bytes.NewReader(bodyBytes))
	req = withBootstrapCtx(workerID, tenantID, region)(req)
	req.Header.Set("Authorization", "Bearer fake-bootstrap-jwt")
	rec := httptest.NewRecorder()
	h.EnrollWorker(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	kid, _ := resp["kid"].(string)
	if want := signing.WorkerKID(pubHex); kid != want {
		t.Errorf("kid = %q, want %q", kid, want)
	}
	secretB64, _ := resp["secret"].(string)
	secretBytes, err := base64.RawURLEncoding.DecodeString(secretB64)
	if err != nil {
		t.Fatalf("secret not base64: %v", err)
	}
	if len(secretBytes) != 32 {
		t.Errorf("secret len = %d, want 32", len(secretBytes))
	}
	wantSecret, err := signing.DeriveWorkerSecret([]byte(jwtSecret), workerID, tenantID, region, pubHex)
	if err != nil {
		t.Fatalf("DeriveWorkerSecret: %v", err)
	}
	if !bytes.Equal(secretBytes, wantSecret) {
		t.Error("derived secret does not match an independent recompute (CP lied?)")
	}
	if expiresAt, ok := resp["expires_at"].(float64); !ok || expiresAt < float64(time.Now().Unix()) {
		t.Errorf("expires_at = %v, want > now", expiresAt)
	}
	if len(repo.setCalls) != 1 {
		t.Fatalf("SetPublicKey call count = %d, want 1", len(repo.setCalls))
	}
	if repo.setCalls[0].id != workerID || repo.setCalls[0].pubkey != pubHex {
		t.Errorf("SetPublicKey called with %+v, want {%s %s}", repo.setCalls[0], workerID, pubHex)
	}
}

// TestInternal_EnrollWorker_BodyWorkerIDMismatch pins the swap-defense:
// the body's worker_id doesn't match the bootstrap JWT's worker_id
// claim. Refused with 400. The challenge is NOT consumed (so the
// legitimate worker can retry).
func TestInternal_EnrollWorker_BodyWorkerIDMismatch(t *testing.T) {
	jwtSecret := "test-cluster-master-secret-that-is-long-enough-32-bytes!"
	repo := &fakeWorkerKeyRepo{}
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, jwtSecret, middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, repo)

	workerID := "w_test_enroll"
	tenantID := "t_test"
	region := "fra"
	challengeB64, priv := makeEnrollmentRequest(t, h, workerID, tenantID, region)
	pub := priv.Public().(ed25519.PublicKey)
	pubHex := hex.EncodeToString(pub)
	challengeBytes, _ := base64.RawURLEncoding.DecodeString(challengeB64)
	sig := signEnrollmentWithPriv(priv, pub, challengeBytes)

	bodyMap := map[string]string{
		"worker_id":            "w_other_id", // mismatch!
		"public_key":           pubHex,
		"enrollment_challenge": challengeB64,
		"signature":            hex.EncodeToString(sig),
	}
	bodyBytes, _ := json.Marshal(bodyMap)
	req := httptest.NewRequest("POST", "/api/internal/worker-bootstrap/enroll", bytes.NewReader(bodyBytes))
	req = withBootstrapCtx(workerID, tenantID, region)(req)
	rec := httptest.NewRecorder()
	h.EnrollWorker(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if len(repo.setCalls) != 0 {
		t.Errorf("SetPublicKey called %d times, want 0 (worker_id mismatch must short-circuit before persistence)", len(repo.setCalls))
	}
}

// TestInternal_EnrollWorker_ChallengeSignatureFails pins the Ed25519
// verification gate: the body claims to be the worker but the
// signature doesn't verify under the claimed pubkey. Returns 401.
// The challenge is consumed (single-use) — a retry with the right
// signature would 401 because the challenge was popped, but the
// point is the Ed25519 gate fires first.
func TestInternal_EnrollWorker_ChallengeSignatureFails(t *testing.T) {
	jwtSecret := "test-cluster-master-secret-that-is-long-enough-32-bytes!"
	repo := &fakeWorkerKeyRepo{}
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, jwtSecret, middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, repo)

	workerID := "w_test_enroll"
	tenantID := "t_test"
	region := "fra"
	challengeB64, _ := makeEnrollmentRequest(t, h, workerID, tenantID, region)

	// Use a fresh keypair that wasn't enrolled in phase 1.
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	pubHex := hex.EncodeToString(pub)
	// Sign with a totally different private key.
	_, privWrong, _ := ed25519.GenerateKey(rand.Reader)
	challengeBytes, _ := base64.RawURLEncoding.DecodeString(challengeB64)
	bogusSig := ed25519.Sign(privWrong, append([]byte("not-the-real-digest"), challengeBytes...))

	bodyMap := map[string]string{
		"worker_id":            workerID,
		"public_key":           pubHex,
		"enrollment_challenge": challengeB64,
		"signature":            hex.EncodeToString(bogusSig),
	}
	bodyBytes, _ := json.Marshal(bodyMap)
	req := httptest.NewRequest("POST", "/api/internal/worker-bootstrap/enroll", bytes.NewReader(bodyBytes))
	req = withBootstrapCtx(workerID, tenantID, region)(req)
	rec := httptest.NewRecorder()
	h.EnrollWorker(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401; body=%s", rec.Code, rec.Body.String())
	}
	if len(repo.setCalls) != 0 {
		t.Errorf("SetPublicKey called %d times, want 0 (Ed25519 verify must short-circuit before persistence)", len(repo.setCalls))
	}
}

// TestInternal_EnrollWorker_BootstrapSecretNotConfigured returns 501
// when BOOTSTRAP_SECRET is unset — the new endpoint shares the
// BootstrapAuth dependency, so it inherits the same 501 contract.
func TestInternal_EnrollWorker_BootstrapSecretNotConfigured(t *testing.T) {
	repo := &fakeWorkerKeyRepo{}
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, repo)
	body := `{"worker_id":"w_test","public_key":"` + testPubkeyHex + `","enrollment_challenge":"AAAA","signature":"deadbeef"}`
	req := httptest.NewRequest("POST", "/api/internal/worker-bootstrap/enroll", strings.NewReader(body))
	req = withBootstrapCtx("w_test", "t_test", "fra")(req)
	rec := httptest.NewRecorder()
	h.EnrollWorker(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

// TestInternal_EnrollWorker_WorkerNotRegistered pins the
// SetPublicKey=0 path: the worker never registered via
// /api/internal/workers, so the enrollment update affects 0 rows.
// The handler surfaces 400 and refuses to issue a derived secret.
func TestInternal_EnrollWorker_WorkerNotRegistered(t *testing.T) {
	jwtSecret := "test-cluster-master-secret-that-is-long-enough-32-bytes!"
	repo := &fakeWorkerKeyRepo{
		setPublicKeyFn: func(ctx context.Context, id, publicKeyHex string) (int64, error) {
			return 0, nil
		},
	}
	h := handler.NewInternalHandler(nil, nil, nil, nil, nil, nil, "", testBootstrapSecret, jwtSecret, middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, repo)

	workerID := "w_unregistered"
	tenantID := "t_test"
	region := "fra"
	challengeB64, priv := makeEnrollmentRequest(t, h, workerID, tenantID, region)
	pub := priv.Public().(ed25519.PublicKey)
	pubHex := hex.EncodeToString(pub)
	challengeBytes, _ := base64.RawURLEncoding.DecodeString(challengeB64)
	sig := signEnrollmentWithPriv(priv, pub, challengeBytes)

	bodyMap := map[string]string{
		"worker_id":            workerID,
		"public_key":           pubHex,
		"enrollment_challenge": challengeB64,
		"signature":            hex.EncodeToString(sig),
	}
	bodyBytes, _ := json.Marshal(bodyMap)
	req := httptest.NewRequest("POST", "/api/internal/worker-bootstrap/enroll", bytes.NewReader(bodyBytes))
	req = withBootstrapCtx(workerID, tenantID, region)(req)
	rec := httptest.NewRecorder()
	h.EnrollWorker(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

// TestInternal_EnrollmentChallengeStore_PopRejectsReplay pins the
// single-use property of the challenge store: a second phase-2 call
// with the same challenge 401s, even with the correct signature.
func TestInternal_EnrollmentChallengeStore_PopRejectsReplay(t *testing.T) {
	store := handler.NewEnrollmentChallengeStoreForTest()
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	pubHex := hex.EncodeToString(pub)
	ch := []byte("some-32-byte-challenge-string-1234")
	store.Put("w_test", handler.EnrollmentChallenge{
		Challenge: base64.RawURLEncoding.EncodeToString(ch),
		PublicKey: pubHex,
		ExpiresAt: time.Now().Add(5 * time.Minute),
	})

	got, ok := store.Pop("w_test", pubHex, time.Now())
	if !ok {
		t.Fatal("first Pop returned ok=false")
	}
	if got.PublicKey != pubHex {
		t.Errorf("Pop returned %+v, want PublicKey=%s", got, pubHex)
	}
	if _, ok := store.Pop("w_test", pubHex, time.Now()); ok {
		t.Error("second Pop returned ok=true, want false (single-use)")
	}
}

// TestInternal_EnrollmentChallengeStore_PopRejectsMismatchedPubkey
// pins the swap-defense at the store level: phase 2 supplies a
// different pubkey than phase 1 captured. The challenge is NOT
// consumed — the legitimate worker can retry with the correct body.
func TestInternal_EnrollmentChallengeStore_PopRejectsMismatchedPubkey(t *testing.T) {
	store := handler.NewEnrollmentChallengeStoreForTest()
	pubA, _, _ := ed25519.GenerateKey(rand.Reader)
	pubAHex := hex.EncodeToString(pubA)
	pubB, _, _ := ed25519.GenerateKey(rand.Reader)
	pubBHex := hex.EncodeToString(pubB)
	ch := []byte("some-32-byte-challenge-string-1234")
	store.Put("w_test", handler.EnrollmentChallenge{
		Challenge: base64.RawURLEncoding.EncodeToString(ch),
		PublicKey: pubAHex,
		ExpiresAt: time.Now().Add(5 * time.Minute),
	})

	if _, ok := store.Pop("w_test", pubBHex, time.Now()); ok {
		t.Error("Pop with mismatched pubkey returned ok=true")
	}
	// Legitimate worker can still retrieve with the correct pubkey.
	if _, ok := store.Pop("w_test", pubAHex, time.Now()); !ok {
		t.Error("Pop with correct pubkey returned ok=false after mismatched attempt")
	}
}

// TestInternal_EnrollmentChallengeStore_PopRejectsExpired pins the
// TTL: a challenge past ExpiresAt is rejected and removed.
func TestInternal_EnrollmentChallengeStore_PopRejectsExpired(t *testing.T) {
	store := handler.NewEnrollmentChallengeStoreForTest()
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	pubHex := hex.EncodeToString(pub)
	ch := []byte("some-32-byte-challenge-string-1234")
	store.Put("w_test", handler.EnrollmentChallenge{
		Challenge: base64.RawURLEncoding.EncodeToString(ch),
		PublicKey: pubHex,
		ExpiresAt: time.Now().Add(-1 * time.Minute),
	})

	if _, ok := store.Pop("w_test", pubHex, time.Now()); ok {
		t.Error("Pop of expired challenge returned ok=true")
	}
	// And a follow-up Pop must also return false (entry was removed).
	if _, ok := store.Pop("w_test", pubHex, time.Now()); ok {
		t.Error("second Pop after expiry returned ok=true (entry should have been removed)")
	}
}
