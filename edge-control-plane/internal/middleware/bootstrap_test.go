package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// helper to compute the same signature the worker does.
func signTest(psk []byte, workerID, region string) string {
	mac := hmac.New(sha256.New, psk)
	mac.Write([]byte(workerID))
	mac.Write([]byte(":"))
	mac.Write([]byte(region))
	return hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyPSKSignature_Valid(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	sig := signTest(psk, "w_fra_abc", "fra")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", sig); err != nil {
		t.Fatalf("expected valid signature, got %v", err)
	}
}

func TestVerifyPSKSignature_WrongPSK(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	wrong := []byte("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
	sig := signTest(wrong, "w_fra_abc", "fra")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", sig); err == nil {
		t.Fatal("expected error for wrong PSK, got nil")
	}
}

func TestVerifyPSKSignature_WrongWorkerID(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	sig := signTest(psk, "w_fra_abc", "fra")
	if err := VerifyPSKSignature(psk, "w_fra_xyz", "fra", sig); err == nil {
		t.Fatal("expected error for wrong worker_id, got nil")
	}
}

func TestVerifyPSKSignature_WrongRegion(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	sig := signTest(psk, "w_fra_abc", "fra")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "nyc", sig); err == nil {
		t.Fatal("expected error for wrong region, got nil")
	}
}

func TestVerifyPSKSignature_EmptySignature(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", ""); err == nil {
		t.Fatal("expected error for empty signature, got nil")
	}
}

func TestVerifyPSKSignature_OddLength(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", "abc"); err == nil {
		t.Fatal("expected error for odd-length signature, got nil")
	}
}

func TestVerifyPSKSignature_NonHex(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	// 64 chars but contains 'z' (non-hex).
	bad := strings.Repeat("z", 64)
	if err := VerifyPSKSignature(psk, "w_fra_abc", "fra", bad); err == nil {
		t.Fatal("expected error for non-hex signature, got nil")
	}
}

func TestPSKAuth_MissingHeaders(t *testing.T) {
	cfg := BootstrapAuthConfig{PSK: []byte("0123456789abcdef0123456789abcdef")}
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

func TestPSKAuth_Valid(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	cfg := BootstrapAuthConfig{PSK: psk}
	var gotID, gotRegion string
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = GetBootstrapWorkerID(r.Context())
		gotRegion = GetBootstrapRegion(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/internal/auth/token", nil)
	req.Header.Set("X-Worker-Id", "w_fra_abc")
	req.Header.Set("X-Worker-Region", "fra")
	req.Header.Set("X-Bootstrap-Signature", signTest(psk, "w_fra_abc", "fra"))
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
}

func TestPSKAuth_WrongSignature(t *testing.T) {
	psk := []byte("0123456789abcdef0123456789abcdef")
	cfg := BootstrapAuthConfig{PSK: psk}
	called := false
	h := PSKAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/internal/auth/token", nil)
	req.Header.Set("X-Worker-Id", "w_fra_abc")
	req.Header.Set("X-Worker-Region", "fra")
	// Different PSK-derived signature.
	wrong := []byte("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
	req.Header.Set("X-Bootstrap-Signature", signTest(wrong, "w_fra_abc", "fra"))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
	if called {
		t.Error("next handler should NOT be called on wrong signature")
	}
}

func TestPSKAuth_EmptyPSKReturnsServiceUnavailable(t *testing.T) {
	// When BOOTSTRAP_PSK is unset on the server, the route still
	// exists but every request returns 503 (operators see the
	// difference between "wrong-PSK" 401 and "server-side
	// disabled" 503).
	cfg := BootstrapAuthConfig{PSK: nil}
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
