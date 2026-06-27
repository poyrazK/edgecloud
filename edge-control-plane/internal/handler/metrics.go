package handler

import (
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// MetricsHandler serves Prometheus-format metric scrape endpoints.
//
//   - GET /metrics         — all tenants, unauthenticated (operator / Prometheus scrape)
//   - GET /api/v1/metrics  — caller's tenant only, requires Bearer API-key auth
type MetricsHandler struct {
	agg *service.MetricsAggregator
}

// NewMetricsHandler creates a MetricsHandler backed by the given aggregator.
func NewMetricsHandler(agg *service.MetricsAggregator) *MetricsHandler {
	return &MetricsHandler{agg: agg}
}

// GetAllMetrics handles GET /metrics — returns all-tenant Prometheus output.
// Intentionally unauthenticated: operators and Prometheus servers scrape this
// endpoint from within the private network. Do not expose it publicly.
func (h *MetricsHandler) GetAllMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(h.agg.RenderAll())); err != nil {
		log.Printf("GetAllMetrics: failed to write response: %v", err)
	}
}

// GetTenantMetrics handles GET /api/v1/metrics — returns only the calling
// tenant's metrics. The tenant ID is extracted from the API-key auth context
// injected by the auth middleware.
func (h *MetricsHandler) GetTenantMetrics(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	if tenantID == "" {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(h.agg.RenderTenant(tenantID))); err != nil {
		log.Printf("GetTenantMetrics: failed to write response: %v", err)
	}
}
