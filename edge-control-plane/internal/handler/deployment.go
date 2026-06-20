package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// DeploymentHandler handles deployment HTTP requests.
type DeploymentHandler struct {
	deploymentSvc *service.DeploymentService
	workerSvc     service.AppTargetLookup
}

func NewDeploymentHandler(deploymentSvc *service.DeploymentService, workerSvc service.AppTargetLookup) *DeploymentHandler {
	return &DeploymentHandler{deploymentSvc: deploymentSvc, workerSvc: workerSvc}
}

func (h *DeploymentHandler) Deploy(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	// Validate app name
	if appName == "" || containsPathTraversal(appName) {
		httperror.BadRequestCtx(w, r, "invalid app name")
		return
	}

	// Read artifact from multipart form or raw body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httperror.BadRequestCtx(w, r, "failed to read body")
		return
	}

	deployment, err := h.deploymentSvc.Deploy(r.Context(), tenantID, appName, bytes.NewReader(body))
	if err != nil {
		if errors.Is(err, service.ErrMaxDeploymentsQuotaExceeded) {
			httperror.QuotaExceededCtx(w, r, "max deployments quota exceeded")
			return
		}
		if errors.Is(err, service.ErrMaxAppsQuotaExceeded) {
			httperror.QuotaExceededCtx(w, r, "max apps quota exceeded")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":   deployment.ID,
		"hash": deployment.Hash,
		"url":  "https://" + domain.IngressHost(tenantID, appName),
	})
}

func (h *DeploymentHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	deploymentID := r.PathValue("deploymentID")

	deployment, err := h.deploymentSvc.GetDeployment(r.Context(), tenantID, deploymentID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	if deployment == nil {
		httperror.NotFoundCtx(w, r, "deployment not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(deployment)
}

func (h *DeploymentHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	limit := 20
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	deployments, total, err := h.deploymentSvc.ListDeploymentsPaginatedWithTotal(r.Context(), tenantID, appName, limit, offset)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"items":  deployments,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *DeploymentHandler) Activate(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	deploymentID := r.PathValue("deploymentID")

	if err := h.deploymentSvc.ActivateDeployment(r.Context(), tenantID, appName, deploymentID); err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "activated"})
}

func (h *DeploymentHandler) GetActive(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	deployment, err := h.deploymentSvc.GetActiveDeployment(r.Context(), tenantID, appName)
	if err != nil || deployment == nil {
		httperror.NotFoundCtx(w, r, "no active deployment")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(deployment)
}

// containsPathTraversal blocks the *decoded* traversal shapes ("/", "\\",
// ".."). The caller is responsible for passing a value that has already
// been percent-decoded — e.g. http.Request.PathValue (used by AppIngress
// and Deploy), or an explicit url.PathUnescape for body fields. Encoding
// bypasses (e.g. %2F, %2E%2E) are intentionally not caught here because
// the input is already decoded by the time this helper sees it; the
// helper's job is to reject post-decode traversal, not to decode.
func containsPathTraversal(s string) bool {
	if s == "" {
		return true
	}
	return strings.ContainsAny(s, "/\\") || strings.Contains(s, "..")
}

// AppIngress handles GET /api/apps/{appName}/ingress — tenant-authenticated
// diagnostic that returns whether the calling tenant's app is currently
// routable on a worker (and on which addr/port). Used by the CLI's
// `edge status` to validate that a `live_url` is actually live. The
// tenant filter is applied at the SQL level so a tenant cannot learn
// about another tenant's apps.
func (h *DeploymentHandler) AppIngress(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	if appName == "" || containsPathTraversal(appName) {
		httperror.BadRequestCtx(w, r, "invalid app name")
		return
	}

	target, err := h.workerSvc.GetAppTarget(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if target == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ready":    false,
			"app_name": appName,
			"reason":   "no running app found for this tenant",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ready":       true,
		"app_name":    target.AppName,
		"tenant_id":   target.TenantID,
		"worker_id":   target.WorkerID,
		"region":      target.Region,
		"worker_addr": target.WorkerAddr,
		"port":        target.Port,
	})
}
