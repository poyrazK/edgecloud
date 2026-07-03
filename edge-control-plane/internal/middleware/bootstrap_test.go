package middleware

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// helper to compute the same signature the worker does. The canonical
// payload is "{workerID}:{region}:{tenantID}" (finding A1) so a
// signature captured for one tenant cannot be replayed against another.
func signTest(psk []byte, workerID, region, tenantID string) string {
	mac := hmac.New(sha256.New, psk)
	mac.Write([]byte(workerID))
	mac.Write([]byte(":"))
	mac.Write([]byte(region))
	mac.Write([]byte(":"))
	mac.Write([]byte(tenantID))
	return hex.EncodeToString(mac.Sum(nil))
}

// buildPSKAuthRequest builds a PSKAuth-shaped request with valid
// headers, a valid body, and a freshly-computed signature. Tests
// mutate specific fields to exercise negative paths.
func buildPSKAuthRequest(t *testing.T, psk []byte, workerID, region, tenantID string) *http.Request {
	t.Helper()
	body, _ := json.Marshal(map[string]string{
		"worker_id": workerID,
		"region":    region,
		"tenant_id": tenantID,
	})
	req := httptest.NewRequest("POST", "/api/internal/auth/token", bytes.NewReader(body))
	req.Header.Set("X-Worker-Id", workerID)
	req.Header.Set("X-Worker-Region", region)
	req.Header.Set("X-Bootstrap-Signature", signTest(psk, workerID, region, tenantID))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestVerifyPSKSignature_Valid(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	sig := signTest(psk, "w_fra_abc", "fra", "t_tenant1")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", "t_tenant1", sig); err != nil {
		t.Fatalf("expected valid signature, got %v", err)
	}
}

func TestVerifyPSKSignature_WrongPSK(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	wrong := []byte("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
	sig := signTest(wrong, "w_fra_abc", "fra", "t_tenant1")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", "t_tenant1", sig); err == nil {
		t.Fatal("expected error for wrong PSK, got nil")
	}
}

func TestVerifyPSKSignature_WrongWorkerID(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	sig := signTest(psk, "w_fra_abc", "fra", "t_tenant1")
	if err := VerifyPSKSignature(psk, "w_fra_xyz", "fra", "t_tenant1", sig); err == nil {
		t.Fatal("expected error for wrong worker_id, got nil")
	}
}

func TestVerifyPSKSignature_WrongRegion(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	sig := signTest(psk, "w_fra_abc", "fra", "t_tenant1")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "nyc", "t_tenant1", sig); err == nil {
		t.Fatal("expected error for wrong region, got nil")
	}
}

// Regression for finding A1: a signature captured for tenant A
// must NOT verify against tenant B — otherwise an attacker who
// captured one valid `X-Bootstrap-Signature` could replay it to
// mint a JWT for a different tenant.
func TestVerifyPSKSignature_TenantIDMismatch(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	sig := signTest(psk, "w_fra_abc", "fra", "t_alice")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", "t_victim", sig); err == nil {
		t.Fatal("signature for t_alice must NOT verify for t_victim (A1 tenant pivot)")
	}
}

// PR #200 review finding C4: pin the same cross-language HMAC-SHA256
// digest that the Rust `sign_with_psk_matches_known_vector` test
// pins. Both tests fail together if the canonical payload format,
// separator, byte order, or hash algorithm drifts between the two
// languages — production only learns about it at first request,
// which is too late.
//
// Compute via:
//
//	python3 -c "import hmac, hashlib; print(hmac.new( \
//	  b'0123456789abcdef0123456789abcdef', \
//	  b'w_fra_abc123:fra:t_tenant1', hashlib.sha256).hexdigest())"
//
// Note: this uses "w_fra_abc123" (with the trailing 3) to match the
// Rust side exactly. The earlier `TestVerifyPSKSignature_*` tests
// use "w_fra_abc" without the trailing 3 — both digests are
// stable; the cross-language invariant test only needs to match the
// Rust pin.
func TestSignTest_MatchesPinnedRustVector(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	const expectedHex = "3561792155cbfd37ce3cfeb01f31585d6014194e8f1e9ccbbd7f8eb7aa6c2a0c"
	got := signTest(psk, "w_fra_abc123", "fra", "t_tenant1")
	if got != expectedHex {
		t.Fatalf("Go HMAC-SHA256 digest drifted from Rust pin.\n  want: %s\n  got:  %s\nCheck the canonical payload format (%q) and the hash algorithm.",
			expectedHex, got, "w_fra_abc123:fra:t_tenant1")
	}
}

func TestVerifyPSKSignature_EmptySignature(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", "t_tenant1", ""); err == nil {
		t.Fatal("expected error for empty signature, got nil")
	}
}

func TestVerifyPSKSignature_OddLength(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", "t_tenant1", "abc"); err == nil {
		t.Fatal("expected error for odd-length signature, got nil")
	}
}

func TestVerifyPSKSignature_NonHex(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	// 64 chars but contains 'z' (non-hex).
	bad := strings.Repeat("z", 64)
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", "t_tenant1", bad); err == nil {
		t.Fatal("expected error for non-hex signature, got nil")
	}
}

func TestPSKAuth_MissingHeaders(t *testing.T) {
	cfg := BootstrapAuthConfig{PSKs: map[string][]byte{"t_tenant1": []byte("0123456789abcdef0123456789abcdef")}}
	called := false
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/internal/auth/token", nil)
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
	if called {
		t.Error("next handler should NOT be called on missing headers")
	}
}

func TestPSKAuth_MissingBody(t *testing.T) {
	// A request with valid headers + signature but no JSON body —
	// the middleware now reads the body for tenant_id (finding A1)
	// and must reject with 400.
	psk := []byte("0123456789abcdef0123456789abcdef")
	cfg := BootstrapAuthConfig{PSKs: map[string][]byte{"t_tenant1": psk}}
	called := false
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/internal/auth/token", nil)
	req.Header.Set("X-Worker-Id", "w_fra_abc")
	req.Header.Set("X-Worker-Region", "fra")
	req.Header.Set("X-Bootstrap-Signature", signTest(psk, "w_fra_abc", "fra", "t_tenant1"))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
	if called {
		t.Error("next handler should NOT be called on missing body")
	}
}

func TestPSKAuth_InvalidJSONBody(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	cfg := BootstrapAuthConfig{PSKs: map[string][]byte{"t_tenant1": psk}}
	called := false
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/internal/auth/token", bytes.NewReader([]byte("not-json-{")))
	req.Header.Set("X-Worker-Id", "w_fra_abc")
	req.Header.Set("X-Worker-Region", "fra")
	req.Header.Set("X-Bootstrap-Signature", signTest(psk, "w_fra_abc", "fra", "t_tenant1"))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
	if called {
		t.Error("next handler should NOT be called on invalid JSON")
	}
}

func TestPSKAuth_EmptyTenantID(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	cfg := BootstrapAuthConfig{PSKs: map[string][]byte{"t_tenant1": psk}}
	called := false
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	rr := httptest.NewRecorder()
	req := buildPSKAuthRequest(t, psk, "w_fra_abc", "fra", "")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
	if called {
		t.Error("next handler should NOT be called on empty tenant_id")
	}
}

func TestPSKAuth_Valid(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	cfg := BootstrapAuthConfig{PSKs: map[string][]byte{"t_tenant1": psk}}
	var gotID, gotRegion, gotTenant string
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = GetBootstrapWorkerID(r.Context())
		gotRegion = GetBootstrapRegion(r.Context())
		gotTenant = GetBootstrapTenantID(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	rr := httptest.NewRecorder()
	req := buildPSKAuthRequest(t, psk, "w_fra_abc", "fra", "t_tenant1")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if gotID != "w_fra_abc" {
		t.Errorf("worker_id context = %q, want w_fra_abc", gotID)
	}
	if gotRegion != "fra" {
		t.Errorf("region context = %q, want fra", gotRegion)
	}
	if gotTenant != "t_tenant1" {
		t.Errorf("tenant_id context = %q, want t_tenant1", gotTenant)
	}
}

func TestPSKAuth_WrongSignature(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	cfg := BootstrapAuthConfig{PSKs: map[string][]byte{"t_tenant1": psk}}
	called := false
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	rr := httptest.NewRecorder()
	// Build a valid request, then overwrite the signature with one
	// derived from a different PSK.
	req := buildPSKAuthRequest(t, psk, "w_fra_abc", "fra", "t_tenant1")
	wrongPSK := []byte("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
	req.Header.Set("X-Bootstrap-Signature", signTest(wrongPSK, "w_fra_abc", "fra", "t_tenant1"))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
	if called {
		t.Error("next handler should NOT be called on wrong signature")
	}
}

// Regression for finding A1: the middleware must read the body
// FIRST (to get tenant_id) and then verify the signature over the
// body tenant_id. A request whose body says tenant B but whose
// signature was captured for tenant A must be rejected.
func TestPSKAuth_BodyTenantIDMismatch(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	cfg := BootstrapAuthConfig{PSKs: map[string][]byte{"t_tenant1": psk}}
	called := false
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	rr := httptest.NewRecorder()
	// Signature covers tenant_id="t_alice" but body claims
	// tenant_id="t_victim". The middleware reads the body first,
	// then computes the signature over the body's tenant_id —
	// which won't match the header signature.
	req := buildPSKAuthRequest(t, psk, "w_fra_abc", "fra", "t_alice")
	body, _ := json.Marshal(map[string]string{
		"worker_id": "w_fra_abc",
		"region":    "fra",
		"tenant_id": "t_victim",
	})
	req.Body = nopReadCloser{bytes.NewReader(body)}
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
	}
	if called {
		t.Error("next handler should NOT be called on body tenant_id mismatch")
	}
}

func TestPSKAuth_EmptyPSKReturnsServiceUnavailable(t *testing.T) {
	// When BOOTSTRAP_PSK is unset on the server, the route still
	// exists but every request returns 503 (operators see the
	// difference between "wrong-PSK" 401 and "server-side
	// disabled" 503).
	cfg := BootstrapAuthConfig{PSKs: nil}
	called := false
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/internal/auth/token", nil)
	req.Header.Set("X-Worker-Id", "w_fra_abc")
	req.Header.Set("X-Worker-Region", "fra")
	req.Header.Set("X-Bootstrap-Signature", strings.Repeat("a", 64))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rr.Code)
	}
	if called {
		t.Error("next handler should NOT be called when PSK is empty")
	}
}

// ----- Identity character-set tests (finding A3) --------------------

func TestValidateIdentity_AcceptsCanonical(t *testing.T) {
	if err := validateIdentity("w_fra_abc", "fra", "t_tenant1"); err != nil {
		t.Errorf("canonical identity must validate: %v", err)
	}
}

func TestValidateIdentity_RejectsWorkerIDWithColon(t *testing.T) {
	// A colon in worker_id would collide with another worker's
	// signature payload (canonical form joins with ":").
	if err := validateIdentity("w_a:b", "fra", "t_tenant1"); err == nil {
		t.Error("worker_id containing colon must be rejected")
	}
}

func TestValidateIdentity_RejectsUppercaseRegion(t *testing.T) {
	if err := validateIdentity("w_fra_abc", "FRA", "t_tenant1"); err == nil {
		t.Error("uppercase region must be rejected")
	}
}

func TestValidateIdentity_RejectsTooLongWorkerID(t *testing.T) {
	long := "w_" + strings.Repeat("a", 100) // > 64
	if err := validateIdentity(long, "fra", "t_tenant1"); err == nil {
		t.Error("over-length worker_id must be rejected")
	}
}

func TestValidateIdentity_RejectsNewlineInWorkerID(t *testing.T) {
	if err := validateIdentity("w_a\nb", "fra", "t_tenant1"); err == nil {
		t.Error("worker_id containing control char must be rejected")
	}
}

func TestValidateIdentity_RejectsBadTenantID(t *testing.T) {
	if err := validateIdentity("w_fra_abc", "fra", "tenant1"); err == nil {
		t.Error("tenant_id without t_ prefix must be rejected")
	}
}

// nopReadCloser lets tests reassign req.Body to a bytes.Reader
// (which is io.Reader but not io.ReadCloser). Same shape as the
// handler-test helper.
type nopReadCloser struct{ *bytes.Reader }

func (nopReadCloser) Close() error { return nil }

// PR #200 review finding H1: per-tenant PSK binding. A request
// whose body's tenant_id has no entry in the per-tenant PSK map
// must be rejected with the same generic 401 used for a signature
// mismatch — never a different status that would let an attacker
// enumerate which tenants have PSKs configured.
func TestPSKAuth_UnknownTenantRejected(t *testing.T) {
	cfg := BootstrapAuthConfig{
		PSKs: map[string][]byte{
			"t_known1": []byte("known-psk-32-bytes-long-aaaaaaaaaaaa"),
		},
	}
	// Build a request for tenant "t_unknown" using a signature
	// computed with the *known* PSK. The middleware must reject
	// this on the tenant-lookup step, before signature verification.
	sig := signTest([]byte("known-psk-32-bytes-long-aaaaaaaaaaaa"),
		"w_fra_abc", "fra", "t_unknown")
	req := httptest.NewRequest("POST", "/api/internal/auth/token",
		bytes.NewReader([]byte(`{"worker_id":"w_fra_abc","region":"fra","tenant_id":"t_unknown"}`)))
	req.Header.Set("X-Worker-Id", "w_fra_abc")
	req.Header.Set("X-Worker-Region", "fra")
	req.Header.Set("X-Bootstrap-Signature", sig)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	middleware := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must NOT be invoked for unknown tenant")
	}))
	middleware.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (unknown tenant must be 401, not 503/200)",
			rec.Code, http.StatusUnauthorized)
	}
}

// PR #200 review finding H1: the known-tenant path must still work
// after the per-tenant refactor. Pairs with TestPSKAuth_UnknownTenantRejected.
func TestPSKAuth_KnownTenantAccepted(t *testing.T) {
	cfg := BootstrapAuthConfig{
		PSKs: map[string][]byte{
			"t_known1": []byte("known-psk-32-bytes-long-aaaaaaaaaaaa"),
			"t_known2": []byte("other-psk-32-bytes-long-bbbbbbbbbbb"),
		},
	}
	sig := signTest([]byte("known-psk-32-bytes-long-aaaaaaaaaaaa"),
		"w_fra_abc", "fra", "t_known1")
	req := httptest.NewRequest("POST", "/api/internal/auth/token",
		bytes.NewReader([]byte(`{"worker_id":"w_fra_abc","region":"fra","tenant_id":"t_known1"}`)))
	req.Header.Set("X-Worker-Id", "w_fra_abc")
	req.Header.Set("X-Worker-Region", "fra")
	req.Header.Set("X-Bootstrap-Signature", sig)
	req.Header.Set("Content-Type", "application/json")

	called := false
	rec := httptest.NewRecorder()
	middleware := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	middleware.ServeHTTP(rec, req)

	if !called {
		t.Errorf("handler must be invoked for known tenant; status = %d", rec.Code)
	}
}

// PR #200 review finding H1: a captured signature for tenant A
// cannot be replayed against tenant B even when both tenants have
// PSKs configured — the per-tenant lookup selects tenant B's PSK
// which produces a different HMAC than the captured signature.
// (Defense in depth on top of the existing A1 tenant-binding check.)
func TestPSKAuth_CrossTenantReplayBlocked(t *testing.T) {
	cfg := BootstrapAuthConfig{
		PSKs: map[string][]byte{
			"t_alice":  []byte("alice-psk-32-bytes-long-aaaaaaaaaaa"),
			"t_victim": []byte("victim-psk-32-bytes-long-bbbbbbbbbb"),
		},
	}
	// Capture a signature for t_alice using alice's PSK.
	capturedSig := signTest([]byte("alice-psk-32-bytes-long-aaaaaaaaaaa"),
		"w_fra_abc", "fra", "t_alice")
	// Replay against t_victim with the SAME signature.
	req := httptest.NewRequest("POST", "/api/internal/auth/token",
		bytes.NewReader([]byte(`{"worker_id":"w_fra_abc","region":"fra","tenant_id":"t_victim"}`)))
	req.Header.Set("X-Worker-Id", "w_fra_abc")
	req.Header.Set("X-Worker-Region", "fra")
	req.Header.Set("X-Bootstrap-Signature", capturedSig)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	middleware := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler must NOT be invoked for cross-tenant replay")
	}))
	middleware.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (cross-tenant replay must 401)",
			rec.Code, http.StatusUnauthorized)
	}
}
