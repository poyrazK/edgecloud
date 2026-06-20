package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// InternalHandler handles internal worker-facing endpoints.
type InternalHandler struct {
	deploymentSvc *service.DeploymentService
	workerSvc     *service.WorkerService
}

func NewInternalHandler(deploymentSvc *service.DeploymentService, workerSvc *service.WorkerService) *InternalHandler {
	return &InternalHandler{
		deploymentSvc: deploymentSvc,
		workerSvc:     workerSvc,
	}
}

// Download serves Wasm artifacts to authenticated workers.
// Requires a valid worker JWT via Authorization: Bearer <token> header or ?jwt= query param.
func (h *InternalHandler) Download(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetWorkerTenantID(r.Context())
	deploymentID := r.PathValue("deploymentID")

	deployment, err := h.deploymentSvc.GetDeployment(r.Context(), tenantID, deploymentID)
	if err != nil || deployment == nil {
		httperror.NotFoundCtx(w, r, "not found")
		return
	}

	artifact, err := h.deploymentSvc.GetArtifact(r.Context(), deployment.TenantID, deployment.AppName, deployment.ID)
	if err != nil {
		httperror.NotFoundCtx(w, r, "artifact not found")
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

// RegisterWorker handles POST /api/internal/workers — worker registration.
func (h *InternalHandler) RegisterWorker(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetWorkerTenantID(r.Context())
	var req domain.RegisterWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	// Validate required fields.
	if req.WorkerID == "" || req.Region == "" {
		httperror.BadRequestCtx(w, r, "worker_id and region are required")
		return
	}
	if err := h.workerSvc.Register(r.Context(), tenantID, &req); err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidWorkerID):
			httperror.BadRequestCtx(w, r, "invalid worker ID")
		case errors.Is(err, service.ErrRegionMismatch):
			httperror.BadRequestCtx(w, r, "region mismatch")
		case errors.Is(err, service.ErrQuotaExceeded):
			httperror.QuotaExceededCtx(w, r, "quota exceeded")
		default:
			log.Printf("internal error: %v", err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// ListWorkers handles GET /api/internal/workers — list workers for the authenticated tenant.
func (h *InternalHandler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetWorkerTenantID(r.Context())
	workers, err := h.workerSvc.ListByTenant(r.Context(), tenantID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	resp := map[string]interface{}{"workers": workers}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
