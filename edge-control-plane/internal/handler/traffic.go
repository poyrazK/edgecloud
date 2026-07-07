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

// TrafficHandler handles traffic split HTTP requests.
type TrafficHandler struct {
	trafficSvc TrafficServiceInterface
	appRepo    AppRepoInterface
}

// TrafficServiceInterface is the subset of service.TrafficService needed by the handler.
type TrafficServiceInterface interface {
	SetTraffic(ctx context.Context, tenantID, appName string, entries []domain.TrafficSplitEntry) error
	GetTraffic(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error)
}

// AppRepoInterface is the narrow contract the handler needs for reading
// per-app rate limits. Defined here so handler tests can inject a mock
// without standing up the full repository (matching the pattern used by
// InternalDomainServiceInterface in internal.go).
type AppRepoInterface interface {
	GetRateLimit(ctx context.Context, tenantID, appName string) (*domain.AppRateLimit, error)
}

// NewTrafficHandler creates a TrafficHandler.
func NewTrafficHandler(trafficSvc TrafficServiceInterface, appRepo AppRepoInterface) *TrafficHandler {
	return &TrafficHandler{trafficSvc: trafficSvc, appRepo: appRepo}
}

// SetTraffic handles PUT /api/v1/apps/{appName}/traffic.
// Body: {"splits": [{"deployment_id": "d_v1", "weight": 95}, {"deployment_id": "d_v2", "weight": 5}]}
func (h *TrafficHandler) SetTraffic(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	if !validateAppName(w, appName) {
		return
	}

	var req domain.TrafficSplitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid JSON body")
		return
	}

	if err := h.trafficSvc.SetTraffic(r.Context(), tenantID, appName, req.Splits); err != nil {
		log.Printf("SetTraffic error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "traffic_set"}); err != nil {
		log.Printf("SetTraffic: failed to encode response: %v", err)
	}
	auditRecord(r, "update", "traffic", appName, "traffic splits updated for app "+appName, "success")
}

// GetTraffic handles GET /api/v1/apps/{appName}/traffic.
func (h *TrafficHandler) GetTraffic(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	if !validateAppName(w, appName) {
		return
	}

	splits, err := h.trafficSvc.GetTraffic(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("GetTraffic error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"app_name": appName,
		"splits":   splits,
	}); err != nil {
		log.Printf("GetTraffic: failed to encode response: %v", err)
	}
}

// GetTrafficInternal handles GET /api/v1/internal/traffic/{tenantID}/{appName}.
// Mounted under the `internalAuth` middleware (shared-secret header), this is
// the read endpoint the edge-ingress polls to apply Caddy weights. Unlike
// GetTraffic, the tenant is not derived from an authenticated context — it
// comes from the URL path because the ingress is a service-to-service caller,
// not a tenant. The split query is the same as GetTraffic's; only the
// authentication and how the tenant is identified differ.
//
// Both `tenantID` and `appName` are validated as if they came from a tenant —
// the internal caller must be authenticated by the shared-secret header, but
// the URL path is still attacker-controlled (a misconfigured ingress proxy
// could let an untrusted tenant construct this URL). The downstream SQL
// query treats them as opaque, but a path-traversal app_name would also
// land in the published TaskMessage.Apps map key — same exposure as the
// tenant-authenticated handler, so we validate the same way.
func (h *TrafficHandler) GetTrafficInternal(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	appName := r.PathValue("appName")
	if !validateAppName(w, appName) {
		return
	}
	if tenantID == "" || containsPathTraversal(tenantID) {
		http.Error(w, `{"error": "invalid tenant id"}`, http.StatusBadRequest)
		return
	}

	splits, err := h.trafficSvc.GetTraffic(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("GetTrafficInternal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"app_name": appName,
		"splits":   splits,
	}); err != nil {
		log.Printf("GetTrafficInternal: failed to encode response: %v", err)
	}
}

// GetRateLimitsInternal handles GET /api/v1/internal/rate-limits/{tenantID}/{appName}.
// Mounted under InternalAuth (shared-secret header), this is the read endpoint
// the edge-ingress ratelimit fetcher polls to discover per-app rate limit overrides.
// Returns the rate limits on 200, empty object {} if both are 0 (no override),
// or 404 if the app doesn't exist.
func (h *TrafficHandler) GetRateLimitsInternal(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	appName := r.PathValue("appName")
	if !validateAppName(w, appName) {
		return
	}
	if tenantID == "" || containsPathTraversal(tenantID) {
		http.Error(w, `{"error": "invalid tenant id"}`, http.StatusBadRequest)
		return
	}

	rl, err := h.appRepo.GetRateLimit(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("GetRateLimitsInternal: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if rl == nil {
		http.Error(w, `{"error": "app not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rl); err != nil {
		log.Printf("GetRateLimitsInternal: failed to encode response: %v", err)
	}
}
