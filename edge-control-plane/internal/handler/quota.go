package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// QuotaServiceInterface is the subset of service.TenantServiceInterface used by QuotaHandler.
type QuotaServiceInterface interface {
	GetQuota(ctx context.Context, tenantID string) (*domain.Quota, error)
}

// QuotaHandler handles quota HTTP requests.
type QuotaHandler struct {
	tenantSvc QuotaServiceInterface
}

func NewQuotaHandler(tenantSvc QuotaServiceInterface) *QuotaHandler {
	return &QuotaHandler{tenantSvc: tenantSvc}
}

// quotaResponse wraps domain.Quota with the derived usage_pct field.
// usage_pct is omitted when both caps are unlimited (sentinel < 0) so
// enterprise tenants don't see a misleading 0%.
type quotaResponse struct {
	domain.Quota
	UsagePct *float64 `json:"usage_pct,omitempty"`
}

// GetQuota handles GET /api/quotas — returns the authenticated tenant's quota limits.
func (h *QuotaHandler) GetQuota(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	quota, err := h.tenantSvc.GetQuota(r.Context(), tenantID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	if quota == nil {
		httperror.NotFoundCtx(w, r, "quota not found")
		return
	}

	resp := quotaResponse{Quota: *quota, UsagePct: quota.UsagePct()}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("GetQuota: failed to encode response: %v", err)
	}
}
