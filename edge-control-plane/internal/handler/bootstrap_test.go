package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// nopCloser turns a bytes.Reader into an io.ReadCloser so we can
// reassign req.Body in tests where the original body was set by
// mintReq. (httptest.NewRequest's Body is io.ReadCloser, not
// io.Reader.)
type nopCloser struct{ io.Reader }

func (nopCloser) Close() error { return nil }

// build a handler with a valid PSK + minter.
func newTestBootstrap(t *testing.T) (*BootstrapHandler, *middleware.BootstrapAuthConfig) {
	t.Helper()
	cfg := config.JWTConfig{
		Secret: "test-secret-32-bytes-minimum-please",
		Issuer: "edgecloud",
		TTL:    24,
	}
	minter := service.NewWorkerJWTMinter(cfg)
	h := NewBootstrapHandler(minter)
	if h == nil {
		t.Fatal("NewBootstrapHandler returned nil")
	}
	psk := []byte("0123456789abcdef0123456789abcdef")
	return h, &middleware.BootstrapAuthConfig{PSKs: map[string][]byte{"t_tenant1": psk}}
}

// Stand up a PSKAuth-wrapped handler for end-to-end tests.
func newTestHandler(t *testing.T) (http.Handler, *middleware.BootstrapAuthConfig) {
	t.Helper()
	h, pskCfg := newTestBootstrap(t)
	return middleware.PSKAuth(*pskCfg)(http.HandlerFunc(h.MintToken)), pskCfg
}

func mintReq(t *testing.T, workerID, region, tenantID, pskStr string) *http.Request {
	t.Helper()
	// Recreate the signature the worker would send. The canonical
	// payload is "{workerID}:{region}:{tenantID}" (finding A1) so a
	// signature captured for one tenant cannot be replayed against
	// another.
	mac := hmac256(pskStr, workerID+":"+region+":"+tenantID)
	body, _ := json.Marshal(map[string]string{
		"worker_id": workerID,
		"region":    region,
		"tenant_id": tenantID,
	})
	req := httptest.NewRequest("POST", "/api/internal/auth/token", bytes.NewReader(body))
	req.Header.Set("X-Worker-Id", workerID)
	req.Header.Set("X-Worker-Region", region)
	req.Header.Set("X-Bootstrap-Signature", mac)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestBootstrapHandler_HappyPath(t *testing.T) {
	h, pskCfg := newTestHandler(t)
	psk := string(pskCfg.PSKs["t_tenant1"])
	req := mintReq(t, "w_fra_abc", "fra", "t_tenant1", psk)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Token         string `json:"token"`
		ExpiresAtUnix int64  `json:"expires_at_unix"`
		TokenType     string `json:"token_type"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Token == "" {
		t.Error("token is empty")
	}
	if resp.TokenType != "Bearer" {
		t.Errorf("token_type = %q, want Bearer", resp.TokenType)
	}
	// expires_at_unix should be ~24h from now (TTL=24 in newTestBootstrap).
	now := time.Now().Unix()
	if resp.ExpiresAtUnix < now || resp.ExpiresAtUnix > now+int64(25*time.Hour/time.Second) {
		t.Errorf("expires_at_unix = %d, want in [%d, %d]", resp.ExpiresAtUnix, now, now+int64(25*time.Hour/time.Second))
	}

	// Verify the returned token is actually a valid WorkerClaims JWT
	// (round-trip with VerifyWorkerJWT).
	claims, err := middleware.VerifyWorkerJWT(resp.Token, middleware.WorkerJWTConfig{
		Secret: "test-secret-32-bytes-minimum-please",
		Issuer: "edgecloud",
	})
	if err != nil {
		t.Fatalf("VerifyWorkerJWT: %v", err)
	}
	if claims.WorkerID != "w_fra_abc" {
		t.Errorf("worker_id = %q, want w_fra_abc", claims.WorkerID)
	}
	if claims.TenantID != "t_tenant1" {
		t.Errorf("tenant_id = %q, want t_tenant1", claims.TenantID)
	}
}

func TestBootstrapHandler_BodyMismatchWorkerID(t *testing.T) {
	h, pskCfg := newTestHandler(t)
	psk := string(pskCfg.PSKs["t_tenant1"])
	// Headers signed for w_a, body claims w_b.
	req := mintReq(t, "w_a", "fra", "t_tenant1", psk)
	body, _ := json.Marshal(map[string]string{
		"worker_id": "w_b",
		"region":    "fra",
		"tenant_id": "t_tenant1",
	})
	req.Body = nopCloser{bytes.NewReader(body)}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestBootstrapHandler_BodyMismatchRegion(t *testing.T) {
	h, pskCfg := newTestHandler(t)
	psk := string(pskCfg.PSKs["t_tenant1"])
	req := mintReq(t, "w_fra_abc", "fra", "t_tenant1", psk)
	body, _ := json.Marshal(map[string]string{
		"worker_id": "w_fra_abc",
		"region":    "nyc",
		"tenant_id": "t_tenant1",
	})
	req.Body = nopCloser{bytes.NewReader(body)}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
}

// Regression for finding A1: a signature captured for one tenant
// cannot mint a JWT for another tenant. The signature header is
// computed over tenant_id="t_alice"; the body claims
// tenant_id="t_victim". PSKAuth reads the body first, then verifies
// the signature over the body's tenant_id — mismatch → 401.
func TestBootstrapHandler_BodyMismatchTenantID(t *testing.T) {
	h, pskCfg := newTestHandler(t)
	psk := string(pskCfg.PSKs["t_tenant1"])
	req := mintReq(t, "w_fra_abc", "fra", "t_alice", psk)
	body, _ := json.Marshal(map[string]string{
		"worker_id": "w_fra_abc",
		"region":    "fra",
		"tenant_id": "t_victim",
	})
	req.Body = nopCloser{bytes.NewReader(body)}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 (signature covers tenant_id, body disagrees), got %d body=%s",
			rr.Code, rr.Body.String())
	}
}

func TestBootstrapHandler_EmptyTenantID(t *testing.T) {
	h, pskCfg := newTestHandler(t)
	psk := string(pskCfg.PSKs["t_tenant1"])
	req := mintReq(t, "w_fra_abc", "fra", "", psk)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestBootstrapHandler_InvalidJSON(t *testing.T) {
	h, pskCfg := newTestHandler(t)
	psk := string(pskCfg.PSKs["t_tenant1"])
	req := mintReq(t, "w_fra_abc", "fra", "t_tenant1", psk)
	req.Body = nopCloser{bytes.NewReader([]byte("not-json-{"))}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestNewBootstrapHandler_NilMinterReturnsNil(t *testing.T) {
	if got := NewBootstrapHandler(nil); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

// F8 (PR #165 review): the bootstrap handler reached WITHOUT
// PSKAuth (i.e. server-side misconfiguration: the route was
// wired to MintToken directly) must return 503 — NOT 500. 503
// distinguishes operator-fixable misconfiguration from genuine
// runtime faults; 500 is reserved for `minter.Mint` failures.
func TestBootstrapHandler_NoPSKAuth_Returns503(t *testing.T) {
	h, _ := newTestBootstrap(t) // unwrapped — direct MintToken call
	body, _ := json.Marshal(map[string]string{
		"worker_id": "w_fra_abc",
		"region":    "fra",
		"tenant_id": "t_tenant1",
	})
	req := httptest.NewRequest("POST", "/api/internal/auth/token", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.MintToken(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (no PSKAuth context → server misconfiguration), got %d body=%s",
			rr.Code, rr.Body.String())
	}
	var resp struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if resp.Error.Code != "SERVICE_UNAVAILABLE" {
		t.Errorf("error.code = %q, want SERVICE_UNAVAILABLE", resp.Error.Code)
	}
	if resp.Error.Message == "" {
		t.Error("error.message must not be empty for 5xx-with-details responses")
	}
}

// F8: partial context (only some of worker_id/region/tenant_id
// populated) must also surface as 503 — the same root cause
// (handler reached without PSKAuth) covers every variant.
func TestBootstrapHandler_PartialContext_Returns503(t *testing.T) {
	h, _ := newTestBootstrap(t)
	body, _ := json.Marshal(map[string]string{})
	req := httptest.NewRequest("POST", "/api/internal/auth/token", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	// Inject only workerID; leave region/tenantID empty.
	ctx := context.WithValue(req.Context(), middleware.BootstrapWorkerIDKey, "w_fra_abc")
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()
	h.MintToken(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for partial bootstrap context, got %d body=%s",
			rr.Code, rr.Body.String())
	}
}
