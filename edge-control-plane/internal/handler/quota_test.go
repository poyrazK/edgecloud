package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
