package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// quotaWire mirrors handler.quotaResponse for test decoding. Defined locally
// to keep the handler's response shape unexported (it's an implementation
// detail of the HTTP layer) while still letting tests assert on usage_pct.
type quotaWire struct {
	domain.Quota
	UsagePct *float64 `json:"usage_pct,omitempty"`
}

type mockQuotaTenantSvc struct {
	quota    *domain.Quota
	quotaErr error
}

func (m *mockQuotaTenantSvc) GetQuota(ctx context.Context, tenantID string) (*domain.Quota, error) {
	if m.quotaErr != nil {
		return nil, m.quotaErr
	}
	return m.quota, nil
}

func (m *mockQuotaTenantSvc) GetQuotaForInternal(ctx context.Context, tenantID string) (*domain.Quota, error) {
	if m.quotaErr != nil {
		return nil, m.quotaErr
	}
	return m.quota, nil
}

func TestQuotaHandler_GetQuota_Success(t *testing.T) {
	q, err := domain.QuotaForPlan("free")
	if err != nil {
		t.Fatalf("QuotaForPlan(free): %v", err)
	}
	q.TenantID = "t_test"
	q.UsedRequestCount = 25_000 // 25% of 100_000 cap → UsagePct returns ~25.0
	h := NewQuotaHandler(&mockQuotaTenantSvc{quota: &q})

	req := httptest.NewRequest(http.MethodGet, "/api/quotas", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	h.GetQuota(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp quotaWire
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.MaxApps != 5 {
		t.Errorf("MaxApps = %d, want 5", resp.MaxApps)
	}
	if resp.UsagePct == nil {
		t.Fatalf("usage_pct missing for finite-cap tenant; body=%s", rr.Body.String())
	}
	if got := *resp.UsagePct; got != 25.0 {
		t.Errorf("usage_pct = %v, want 25.0 (25000/100000)", got)
	}
}

func TestQuotaHandler_GetQuota_Enterprise_NoUsagePct(t *testing.T) {
	// Enterprise tenant: both caps at sentinel -1 (unlimited).
	// UsagePct() returns nil; omitempty drops the key from the wire.
	q := &domain.Quota{
		TenantID:            "t_enterprise",
		MaxOutboundMB:       -1,
		MaxRequestsPerMonth: -1,
	}
	h := NewQuotaHandler(&mockQuotaTenantSvc{quota: q})

	req := httptest.NewRequest(http.MethodGet, "/api/quotas", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_enterprise"))
	rr := httptest.NewRecorder()
	h.GetQuota(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "usage_pct") {
		t.Errorf("enterprise response should omit usage_pct (both caps unlimited); body=%s", rr.Body.String())
	}
}

func TestQuotaHandler_GetQuota_NotFound(t *testing.T) {
	h := NewQuotaHandler(&mockQuotaTenantSvc{quota: nil})

	req := httptest.NewRequest(http.MethodGet, "/api/quotas", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	h.GetQuota(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

// ---------------------------------------------------------------------------
// GetQuotaInternal (issue #420 — edge-ingress polls this every 30s to drive
// the Caddy 402 injection. Trust model is the X-Internal-Token shared
// secret, mounted under internalAuth in app.go.)
// ---------------------------------------------------------------------------

// quotaInternalWire mirrors handler.quotaInternalResponse for decoding.
// Defined locally so the handler's response shape stays unexported while
// the test can still assert on over_cap and locked_until.
type quotaInternalWire struct {
	domain.Quota
	OverCap     bool       `json:"over_cap"`
	LockedUntil *time.Time `json:"locked_until,omitempty"`
}

// TestQuotaHandler_GetQuotaInternal_Success: under-cap tenant, no grace
// clock. Response should be 200 with over_cap=false and a nil locked_until.
func TestQuotaHandler_GetQuotaInternal_Success(t *testing.T) {
	q, err := domain.QuotaForPlan("free")
	if err != nil {
		t.Fatalf("QuotaForPlan(free): %v", err)
	}
	q.TenantID = "t_test"
	q.UsedRequestCount = 50_000 // 50% of 100_000
	h := NewQuotaHandler(&mockQuotaTenantSvc{quota: &q})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/quota/t_test", nil)
	req.SetPathValue("tenantID", "t_test")
	rr := httptest.NewRecorder()
	h.GetQuotaInternal(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rr.Code, rr.Body.String())
	}
	var resp quotaInternalWire
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.OverCap {
		t.Errorf("over_cap = true at 50%%, want false")
	}
	if resp.LockedUntil != nil {
		t.Errorf("locked_until = %v, want nil (no grace clock set)", resp.LockedUntil)
	}
	if resp.MaxRequestsPerMonth != 100_000 {
		t.Errorf("max_requests_per_month = %d, want 100_000", resp.MaxRequestsPerMonth)
	}
}

// TestQuotaHandler_GetQuotaInternal_OverCap_Requests: used_request_count
// at the cap → over_cap=true. This is the signal edge-ingress uses to
// inject the Caddy static_response 402 block.
func TestQuotaHandler_GetQuotaInternal_OverCap_Requests(t *testing.T) {
	q, err := domain.QuotaForPlan("free")
	if err != nil {
		t.Fatalf("QuotaForPlan(free): %v", err)
	}
	q.TenantID = "t_over"
	q.UsedRequestCount = int64(q.MaxRequestsPerMonth) // 100_000 = 100%
	h := NewQuotaHandler(&mockQuotaTenantSvc{quota: &q})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/quota/t_over", nil)
	req.SetPathValue("tenantID", "t_over")
	rr := httptest.NewRecorder()
	h.GetQuotaInternal(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rr.Code, rr.Body.String())
	}
	var resp quotaInternalWire
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.OverCap {
		t.Errorf("over_cap = false at 100%%, want true (used >= cap)")
	}
}

// TestQuotaHandler_GetQuotaInternal_OverCap_Memory (issue #44, part 2):
// used_memory_mb at MaxMemoryMB → over_cap=true. Edge-ingress reads
// this and injects the request-time 402 block. The deploy-time gate is
// the leading signal (rejects the next activate), but the request-time
// gate is the user-facing backstop that flips a serving tenant's
// already-deployed apps to 402 until they roll back.
func TestQuotaHandler_GetQuotaInternal_OverCap_Memory(t *testing.T) {
	q, err := domain.QuotaForPlan("free")
	if err != nil {
		t.Fatalf("QuotaForPlan(free): %v", err)
	}
	q.TenantID = "t_over_mem"
	q.UsedMemoryMB = int64(q.MaxMemoryMB) // 256 = 100%
	h := NewQuotaHandler(&mockQuotaTenantSvc{quota: &q})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/quota/t_over_mem", nil)
	req.SetPathValue("tenantID", "t_over_mem")
	rr := httptest.NewRecorder()
	h.GetQuotaInternal(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rr.Code, rr.Body.String())
	}
	var resp quotaInternalWire
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.OverCap {
		t.Errorf("over_cap = false at used_memory_mb = MaxMemoryMB, want true")
	}
}

// TestQuotaHandler_GetQuotaInternal_OverCap_MemoryEnterpriseBypasses
// (issue #44, part 2): MaxMemoryMB = -1 is the unlimited sentinel and
// must NOT trip over_cap on the memory axis even when used_memory_mb
// is absurd. Same shape as the existing tests' enterprise bypass for
// requests/outbound.
func TestQuotaHandler_GetQuotaInternal_OverCap_MemoryEnterpriseBypasses(t *testing.T) {
	q := &domain.Quota{
		TenantID:            "t_ent",
		MaxRequestsPerMonth: -1,
		MaxOutboundMB:       -1,
		MaxMemoryMB:         -1,
		UsedMemoryMB:        9_999_999,
	}
	h := NewQuotaHandler(&mockQuotaTenantSvc{quota: q})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/quota/t_ent", nil)
	req.SetPathValue("tenantID", "t_ent")
	rr := httptest.NewRecorder()
	h.GetQuotaInternal(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rr.Code, rr.Body.String())
	}
	var resp quotaInternalWire
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.OverCap {
		t.Errorf("over_cap = true with MaxMemoryMB=-1, want false (unlimited sentinel)")
	}
}

// TestQuotaHandler_GetQuotaInternal_NotFound: GetQuotaForInternal
// returns nil → 404 with a JSON error envelope. Edge-ingress fails
// open on a missing tenant (no 402 injected); the 404 is the
// observable signal that the tenant row was missing at fetch time.
func TestQuotaHandler_GetQuotaInternal_NotFound(t *testing.T) {
	h := NewQuotaHandler(&mockQuotaTenantSvc{quota: nil})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/quota/t_missing", nil)
	req.SetPathValue("tenantID", "t_missing")
	rr := httptest.NewRecorder()
	h.GetQuotaInternal(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (body=%s)", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "tenant not found") {
		t.Errorf("body missing 'tenant not found' envelope: %s", rr.Body.String())
	}
}

// TestQuotaHandler_GetQuotaInternal_PathTraversal rejects a tenantID
// with traversal characters. The handler shares containsPathTraversal
// with other tenant handlers; we lock the behavior here so future
// refactors don't accidentally widen the input.
func TestQuotaHandler_GetQuotaInternal_PathTraversal(t *testing.T) {
	svc := &mockQuotaTenantSvc{quotaErr: errors.New("service should not be called")}
	h := NewQuotaHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/internal/quota/..%2Fetc", nil)
	req.SetPathValue("tenantID", "../etc")
	rr := httptest.NewRecorder()
	h.GetQuotaInternal(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (body=%s)", rr.Code, rr.Body.String())
	}
}
