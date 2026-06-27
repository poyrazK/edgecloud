package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// EnvHandler handles environment variable HTTP requests.
type EnvHandler struct {
	envSvc *service.EnvService
}

func NewEnvHandler(envSvc *service.EnvService) *EnvHandler {
	return &EnvHandler{envSvc: envSvc}
}

type SetEnvRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (h *EnvHandler) Set(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	var req SetEnvRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}

	if req.Key == "" {
		httperror.BadRequestCtx(w, r, "key is required")
		return
	}

	if err := h.envSvc.SetEnv(r.Context(), tenantID, appName, req.Key, req.Value); err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *EnvHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	envs, err := h.envSvc.ListEnv(r.Context(), tenantID, appName)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}

	// Return as map
	result := make(map[string]string)
	for _, e := range envs {
		result[e.EnvKey] = e.EnvValue
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		log.Printf("List envs: failed to encode response: %v", err)
	}
}

func (h *EnvHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	key := r.PathValue("key")

	if err := h.envSvc.DeleteEnv(r.Context(), tenantID, appName, key); err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
