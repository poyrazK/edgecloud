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

// QuotaRepoForRateLimit is the narrow slice of *repository.QuotaRepository
// the new per-tenant rate-limit internal endpoint needs (issue #305).
// Mirrors the AppRepoInterface precedent at traffic.go:30-32 — the
// handler test injects a mock that satisfies only these two methods
// without standing up a full repository.QuotaRepository. The service
// layer is intentionally bypassed here: the endpoint is a thin
// read-through with no business logic, and the existing GetQuotaInternal
// pattern keeps the service layer out of the ingress-poll hot path.
type QuotaRepoForRateLimit interface {
	GetRateLimit(ctx context.Context, tenantID string) (*domain.TenantRateLimitResponse, error)
}

// QuotaHandler handles quota HTTP requests.
type QuotaHandler struct {
	tenantSvc QuotaServiceInterface
	// quotaRepo is the per-tenant rate-limit read surface (issue #305).
	// nil is acceptable for handlers that never call GetTenantRateLimitInternal
	// — production wiring in app.New always sets it.
	quotaRepo QuotaRepoForRateLimit
}

func NewQuotaHandler(tenantSvc QuotaServiceInterface, quotaRepo QuotaRepoForRateLimit) *QuotaHandler {
	return &QuotaHandler{tenantSvc: tenantSvc, quotaRepo: quotaRepo}
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
	//
	// Memory axis (issue #44 part 2): used_memory_mb is the per-tenant
	// aggregate held by active deployments. Unlike used_outbound_bytes /
	// used_request_count, it does not roll over at month boundary — the
	// cap is enforced continuously by the deploy-time gate and now also
	// surfaces here so edge-ingress can flip a serving tenant to 402
	// while they sort out a rollback. Note that a counter that crosses
	// the cap from below does NOT itself disable the tenant — that is
	// the heartbeat applyTenantDelta path's job (issue #420) and is not
	// duplicated here. Over_cap is purely the read-side signal.
	//
	// Resident-seconds axis (issue #484 / #485, third metered dimension):
	// used_resident_seconds is incremented by checkResidentSeconds on each
	// heartbeat for every LongRunning app. Handler (FaaS) apps contribute
	// 0 because the worker stamps ResidentSeconds=null (treated as no
	// contribution by applyTenantDelta). The `> 0` guard mirrors the
	// existing axes' sentinel semantics (-1 = unlimited, 0 = unset);
	// `>=` so the gate fires the heartbeat immediately after the cap
	// lands, matching the other dimensions.
	overCap := (quota.MaxRequestsPerMonth > 0 && quota.UsedRequestCount >= int64(quota.MaxRequestsPerMonth)) ||
		(quota.MaxOutboundMB > 0 && quota.UsedOutboundBytes >= int64(quota.MaxOutboundMB)*1024*1024) ||
		(quota.MaxMemoryMB > 0 && quota.UsedMemoryMB >= int64(quota.MaxMemoryMB)) ||
		(quota.MaxResidentSecondsPerMonth > 0 && quota.UsedResidentSeconds >= int64(quota.MaxResidentSecondsPerMonth))

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

// GetTenantRateLimitInternal handles
// GET /api/v1/internal/rate-limit/{tenantID} (issue #305, sub-feature
// #1+#2+#3+#4+#5 read path). Mounted under the `internalAuth`
// middleware (shared-secret header) — same trust model as
// GetQuotaInternal / GetRateLimitsInternal / GetTrafficInternal. The
// edge-ingress TenantRateLimitCache fetcher polls this endpoint every
// TENANT_RATE_LIMIT_FETCH_INTERVAL (default 30s) to learn the per-tenant
// caps that the Caddy renderer emits as rate_limit routes.
//
// Wire shape is the five rate-limit columns on the quotas row (issue
// #305 storage), not the full domain.Quota — the ingress does not want
// the counter fields, period start, or grace timestamp on this path.
//
// 404 when no quotas row exists (treated by the ingress as "no caps
// known for this tenant" → no rate_limit route emitted, fail-open same
// shape as the quota 402 cache at issue #420). 200 with all-zero caps
// when the row exists but the tenant has never been rate-limit-configured.
func (h *QuotaHandler) GetTenantRateLimitInternal(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if tenantID == "" || containsPathTraversal(tenantID) {
		http.Error(w, `{"error": "invalid tenant id"}`, http.StatusBadRequest)
		return
	}

	rl, err := h.quotaRepo.GetRateLimit(r.Context(), tenantID)
	if err != nil {
		log.Printf("GetTenantRateLimitInternal(%s): %v", tenantID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if rl == nil {
		http.Error(w, `{"error": "tenant not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rl); err != nil {
		log.Printf("GetTenantRateLimitInternal(%s): failed to encode response: %v", tenantID, err)
	}
}
