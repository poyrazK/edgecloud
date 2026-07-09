package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// QuotaServiceInterface is the subset of service.TenantServiceInterface used by QuotaHandler.
type QuotaServiceInterface interface {
	GetQuota(ctx context.Context, tenantID string) (*domain.Quota, error)
	GetQuotaForInternal(ctx context.Context, tenantID string) (*domain.Quota, error)
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

// quotaInternalResponse is the wire shape for
// GET /api/v1/internal/quota/{tenantID}. Mirrors quotaResponse but adds
// the derived over_cap boolean and the locked_until timestamp that the
// edge-ingress Caddy renderer needs to decide whether to inject a
// static_response 402 block. over_cap is true when used_* is at or
// above the cap; locked_until mirrors quotas.quota_lock_grace_until
// (the per-tenant free-tier grace clock) so the ingress can serve
// requests up to that point and then flip to 402 once it expires.
type quotaInternalResponse struct {
	domain.Quota
	OverCap     bool       `json:"over_cap"`
	LockedUntil *time.Time `json:"locked_until,omitempty"`
}

// GetQuotaInternal handles GET /api/v1/internal/quota/{tenantID}.
// Mounted under the `internalAuth` middleware (shared-secret header)
// — same trust model as GetTrafficInternal / GetRateLimitsInternal.
// The edge-ingress polls this endpoint every QUOTA_FETCH_INTERVAL
// (default 30s) to learn whether to inject a Caddy static_response
// 402 block for the tenant's apps.
func (h *QuotaHandler) GetQuotaInternal(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if tenantID == "" || containsPathTraversal(tenantID) {
		http.Error(w, `{"error": "invalid tenant id"}`, http.StatusBadRequest)
		return
	}

	quota, err := h.tenantSvc.GetQuotaForInternal(r.Context(), tenantID)
	if err != nil {
		log.Printf("GetQuotaInternal: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if quota == nil {
		http.Error(w, `{"error": "tenant not found"}`, http.StatusNotFound)
		return
	}

	// over_cap: derived from max_* vs used_*. Sentinel < 0 means
	// "unlimited" — never over-cap on the unlimited axis. A nil
	// grace clock is the same as "no lockdown pending".
	overCap := false
	if quota.MaxRequestsPerMonth > 0 && quota.UsedRequestCount >= int64(quota.MaxRequestsPerMonth) {
		overCap = true
	}
	if quota.MaxOutboundMB > 0 && quota.UsedOutboundBytes >= int64(quota.MaxOutboundMB)*1024*1024 {
		overCap = true
	}

	resp := quotaInternalResponse{
		Quota:       *quota,
		OverCap:     overCap,
		LockedUntil: quota.QuotaLockGraceUntil,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("GetQuotaInternal: failed to encode response: %v", err)
	}
}
