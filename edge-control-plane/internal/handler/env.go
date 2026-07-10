package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
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

// envVarResponse is one entry in the array returned by
// `GET /api/v1/apps/{appName}/env`. Ordered alphabetically by key
// for deterministic output (the prior `map[string]string` shape
// relied on Go's randomized map iteration).
type envVarResponse struct {
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
		// Issue #560: surface the disabled-tenant gate as 409 so the
		// CLI / operator tooling can distinguish "tenant is locked,
		// don't retry" from a generic infrastructure 500. Mirrors the
		// deployment.go:785 mapping. Anything else (db unreachable,
		// lock timeout, …) stays a 500 with the canonical envelope.
		if errors.Is(err, service.ErrTenantDisabled) {
			httperror.ConflictCtx(w, r, "tenant is disabled")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	auditRecord(r, "update", "env", appName+"/"+req.Key, "env var "+req.Key+" set for app "+appName, "success")
}

func (h *EnvHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	envs, err := h.envSvc.ListEnv(r.Context(), tenantID, appName)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}

	// Sort alphabetically so the wire body is deterministic and the
	// CLI can render a stable table without re-sorting client-side.
	sort.Slice(envs, func(i, j int) bool { return envs[i].EnvKey < envs[j].EnvKey })

	items := make([]envVarResponse, len(envs))
	for i, e := range envs {
		items[i] = envVarResponse{Key: e.EnvKey, Value: e.EnvValue}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(items); err != nil {
		log.Printf("List envs: failed to encode response: %v", err)
	}
}

func (h *EnvHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	key := r.PathValue("key")

	if err := h.envSvc.DeleteEnv(r.Context(), tenantID, appName, key); err != nil {
		// Issue #560: see EnvHandler.Set above for the rationale.
		if errors.Is(err, service.ErrTenantDisabled) {
			httperror.ConflictCtx(w, r, "tenant is disabled")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	auditRecord(r, "delete", "env", appName+"/"+key, "env var "+key+" deleted from app "+appName, "success")
}
