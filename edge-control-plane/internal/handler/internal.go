package handler

import (
	"io"
	"log"
	"net/http"

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
// WARNING: Worker JWT authentication is not yet implemented.
// This endpoint allows any caller who knows a deployment ID to download the artifact.
func (h *InternalHandler) Download(w http.ResponseWriter, r *http.Request) {
	log.Printf("WARNING: internal download endpoint called without worker JWT auth for deploymentID=%s — auth not yet implemented", r.PathValue("deploymentID"))
	deployment, err := h.deploymentSvc.GetDeployment(r.Context(), "", r.PathValue("deploymentID"))
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
