package handler

import (
	"encoding/json"
	"net/http"

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
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}
	if quota == nil {
		http.Error(w, `{"error": "quota not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(quota)
}
