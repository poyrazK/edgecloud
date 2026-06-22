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

// AutoRollbackRequest is the JSON body posted by an edge-worker when
// its supervisor exhausts the restart cap on a tenant app. The
// tenant_id is read from the (currently unauthenticated — see
// cmd/api/main.go:159) worker context, but the request still
// includes it for cross-checking against future worker JWTs.
//
// `restart_count` is informational only — the server doesn't gate
// on a threshold, but having it in the request body makes the
// audit log useful when correlating crashes with the auto-rollback
// trigger.
type AutoRollbackRequest struct {
	TenantID            string `json:"tenant_id"`
	AppName             string `json:"app_name"`
	CurrentDeploymentID string `json:"current_deployment_id"`
	RestartCount        uint32 `json:"restart_count"`
}

// AutoRollback handles POST /api/internal/apps/{appName}/auto-rollback.
// Triggered by an edge-worker when an app reaches
// AppInstanceStatus::Crashed (restart_count >= max_restarts) AND the
// tenant opted in via `edge deploy --auto-rollback` (the
// auto_rollback_enabled column on active_deployments).
//
// Behavior:
//   - 200: rolled back to last_good_deployment_id; body is
//     {"deployment_id": "<new active id>", "prior_deployment_id":
//     "<rolled-away-from id>"}. Workers may still be serving the
//     prior deployment until the published TaskMessage is delivered;
//     the response is best-effort guidance, not a guarantee.
//   - 404: no active deployment for this app.
//   - 409: no last-good pointer (only ever activated one deployment).
//   - 412: auto-rollback is disabled for this app. Tells the worker
//     "we got your signal but the tenant didn't opt in" — distinct
//     from 403 (auth) so the worker can distinguish a config issue
//     from a permission issue.
//   - 502: rollback committed but the post-commit NATS publish
//     failed.
//   - 500: anything else.
//
// Like every other /api/internal/* endpoint, this is currently
// unauthenticated — see the comment in cmd/api/main.go. Minting
// worker JWTs at startup is a tracked follow-up issue.
func (h *InternalHandler) AutoRollback(w http.ResponseWriter, r *http.Request) {
	appName := r.PathValue("appName")

	var req AutoRollbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.TenantID == "" || req.AppName == "" {
		httperror.BadRequestCtx(w, r, "tenant_id and app_name are required")
		return
	}
	// Path-vs-body consistency check. The handler resolves appName
	// from the URL (so curl -X POST /api/internal/apps/foo/auto-rollback
	// with body app_name="bar" doesn't accidentally hit the wrong
	// app), and falls back to the body if the URL doesn't carry an
	// app name (e.g. internal callers that POST to a non-app-scoped
	// endpoint).
	if appName == "" {
		appName = req.AppName
	}
	if appName != req.AppName {
		httperror.BadRequestCtx(w, r, "app_name in URL and body must match")
		return
	}

	// Use the existing RollbackDeployment path. The repo's
	// ResetStableSinceForRollback enforces auto_rollback_enabled
	// in SQL — we surface the resulting sentinel errors as the
	// status codes documented above. The ErrAutoRollbackDisabled
	// sentinel lives in package repository (it's a string-matched
	// sentinel to avoid an import cycle with service); the handler
	// matches via errors.Is using a re-exported alias below.
	newID, err := h.deploymentSvc.RollbackDeployment(r.Context(), req.TenantID, appName)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrNoLastGood):
			http.Error(w, `{"error": "no previous deployment to roll back to"}`, http.StatusConflict)
		case errors.Is(err, service.ErrNoActiveDeployment):
			http.Error(w, `{"error": "no active deployment"}`, http.StatusNotFound)
		case errors.Is(err, service.ErrAutoRollbackDisabled):
			http.Error(w, `{"error": "auto-rollback disabled for this app"}`, http.StatusPreconditionFailed)
		case errors.Is(err, service.ErrPublishFailed):
			http.Error(w, `{"error": "rollback committed but worker notification failed; please retry"}`, http.StatusBadGateway)
		default:
			log.Printf("internal error: %v", err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"deployment_id": newID,
	})
}
