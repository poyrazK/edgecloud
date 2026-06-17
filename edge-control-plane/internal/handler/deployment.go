package handler

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// DeploymentHandler handles deployment HTTP requests.
type DeploymentHandler struct {
	deploymentSvc *service.DeploymentService
}

func NewDeploymentHandler(deploymentSvc *service.DeploymentService) *DeploymentHandler {
	return &DeploymentHandler{deploymentSvc: deploymentSvc}
}

func (h *DeploymentHandler) Deploy(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	// Validate app name
	if appName == "" || containsPathTraversal(appName) {
		http.Error(w, `{"error": "invalid app name"}`, http.StatusBadRequest)
		return
	}

	// Read artifact from multipart form or raw body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error": "failed to read body"}`, http.StatusBadRequest)
		return
	}

	deployment, err := h.deploymentSvc.Deploy(r.Context(), tenantID, appName, bytes.NewReader(body))
	if err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":   deployment.ID,
		"hash": deployment.Hash,
		"url":  "https://" + appName + ".edgecloud.dev",
	})
}

func (h *DeploymentHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	deploymentID := r.PathValue("deploymentID")

	deployment, err := h.deploymentSvc.GetDeployment(r.Context(), tenantID, deploymentID)
	if err != nil {
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}
	if deployment == nil {
		http.Error(w, `{"error": "deployment not found"}`, http.StatusNotFound)
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
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
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
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
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
		http.Error(w, `{"error": "no active deployment"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(deployment)
}

func containsPathTraversal(s string) bool {
	if s == "" {
		return true
	}
	return strings.ContainsAny(s, "/\\") || strings.Contains(s, "..")
}
