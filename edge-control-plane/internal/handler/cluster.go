package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// ClusterHandler handles cluster-wide admin endpoints.
type ClusterHandler struct {
	clusterSvc service.ClusterServiceInterface
}

// NewClusterHandler constructs a ClusterHandler.
func NewClusterHandler(clusterSvc service.ClusterServiceInterface) *ClusterHandler {
	return &ClusterHandler{clusterSvc: clusterSvc}
}

// Get handles GET /api/admin/cluster — returns the per-region, per-worker
// snapshot. Owner-only; mounted under /api/admin/ in main.go.
func (h *ClusterHandler) Get(w http.ResponseWriter, r *http.Request) {
	view, err := h.clusterSvc.List(r.Context())
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(view); err != nil {
		log.Printf("encode error: %v", err)
	}
}

// Events handles GET /api/admin/cluster/events — returns the most-recent
// `autoscale_events` rows, newest first. Owner-only; mounted under
// /api/admin/ in main.go.
//
// Query parameters:
//   - `region` (optional): restrict to one region. Empty = all regions.
//   - `limit`  (optional): 1..500, default 50. Clamped server-side.
func (h *ClusterHandler) Events(w http.ResponseWriter, r *http.Request) {
	region := r.URL.Query().Get("region")
	limit := parseLimitQuery(r.URL.Query().Get("limit"), 50)
	list, err := h.clusterSvc.RecentEvents(r.Context(), region, limit)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(list); err != nil {
		log.Printf("encode error: %v", err)
	}
}

// parseLimitQuery parses a `?limit=N` query string. Returns `def` when
// empty, missing, malformed, negative, or zero. The service layer
// also clamps to a sane upper bound (500) — parseLimitQuery only
// handles the "what does the user mean" half of that.
func parseLimitQuery(raw string, def int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
