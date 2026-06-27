package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// QuotaHandler handles quota HTTP requests.
type QuotaHandler struct {
	tenantSvc service.TenantServiceInterface
}

func NewQuotaHandler(tenantSvc service.TenantServiceInterface) *QuotaHandler {
	return &QuotaHandler{tenantSvc: tenantSvc}
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

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(quota); err != nil {
		log.Printf("GetQuota: failed to encode response: %v", err)
	}
}
