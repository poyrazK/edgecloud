package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// EnvServiceInterface is the subset of *service.EnvService used by EnvHandler.
type EnvServiceInterface interface {
	SetEnv(ctx context.Context, tenantID, appName, key, value string) error
	ListEnv(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error)
	DeleteEnv(ctx context.Context, tenantID, appName, key string) error
}

// EnvHandler handles environment variable HTTP requests.
type EnvHandler struct {
	envSvc EnvServiceInterface
}

func NewEnvHandler(envSvc EnvServiceInterface) *EnvHandler {
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
