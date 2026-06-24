package handler

import (
	"encoding/json"
	"log"
	"net/http"

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
