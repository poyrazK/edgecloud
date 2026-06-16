package handler

import (
	"io"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// InternalHandler handles internal worker-facing endpoints.
type InternalHandler struct {
	deploymentSvc *service.DeploymentService
}

func NewInternalHandler(deploymentSvc *service.DeploymentService) *InternalHandler {
	return &InternalHandler{deploymentSvc: deploymentSvc}
}

// Download serves Wasm artifacts to authenticated workers.
// Requires a valid worker JWT via Authorization: Bearer <token> header or ?jwt= query param.
func (h *InternalHandler) Download(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetWorkerTenantID(r.Context())
	deploymentID := r.PathValue("deploymentID")

	deployment, err := h.deploymentSvc.GetDeployment(r.Context(), tenantID, deploymentID)
	if err != nil || deployment == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	artifact, err := h.deploymentSvc.GetArtifact(r.Context(), deployment.TenantID, deployment.AppName, deployment.ID)
	if err != nil {
		http.Error(w, "artifact not found", http.StatusNotFound)
		return
	}
	defer artifact.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, artifact); err != nil {
		// client disconnected, nothing we can do
		return
	}
}
