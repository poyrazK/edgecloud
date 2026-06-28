package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

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

func TestQuotaHandler_GetQuota_Success(t *testing.T) {
	q := domain.DefaultQuota("t_test")
	h := NewQuotaHandler(&mockQuotaTenantSvc{quota: &q})

	req := httptest.NewRequest(http.MethodGet, "/api/quotas", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	h.GetQuota(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp domain.Quota
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.MaxApps != 5 {
		t.Errorf("MaxApps = %d, want 5", resp.MaxApps)
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
