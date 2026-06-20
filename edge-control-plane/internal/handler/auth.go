package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// AuthHandler handles authentication-related HTTP requests.
type AuthHandler struct {
	tenantSvc service.TenantServiceInterface
	apiKeySvc service.APIKeyServiceInterface
}

func NewAuthHandler(tenantSvc service.TenantServiceInterface, apiKeySvc service.APIKeyServiceInterface) *AuthHandler {
	return &AuthHandler{tenantSvc: tenantSvc, apiKeySvc: apiKeySvc}
}

// WhoamiResponse is returned by GET /api/auth/whoami.
//
// `created_at` reflects when the tenant (account) was created, not when the
// API key was issued. `role` is the role of the calling API key, which may
// differ from other keys on the same tenant.
type WhoamiResponse struct {
	TenantID   string `json:"tenant_id"`
	TenantName string `json:"tenant_name"`
	Plan       string `json:"plan"`
	APIKeyID   string `json:"api_key_id"`
	APIKeyName string `json:"api_key_name"`
	Role       string `json:"role"`
	CreatedAt  string `json:"created_at"`
}

// Whoami handles GET /api/auth/whoami — returns the tenant + API key
// associated with the caller's Bearer token. Mounted under the
// authenticated `api` mux, so the context is already populated by
// AuthMiddleware.
func (h *AuthHandler) Whoami(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	apiKeyID := middleware.GetAPIKeyID(r.Context())
	role := middleware.GetRole(r.Context())

	if tenantID == "" || apiKeyID == "" {
		// Should not happen behind AuthMiddleware; defensive only.
		httperror.UnauthorizedCtx(w, r, "unauthorized")
		return
	}

	tenant, err := h.tenantSvc.GetTenant(r.Context(), tenantID)
	if err != nil {
		log.Printf("whoami: lookup tenant %q failed: %v", tenantID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if tenant == nil {
		httperror.NotFoundCtx(w, r, "tenant not found")
		return
	}

	key, err := h.apiKeySvc.GetByID(r.Context(), apiKeyID)
	if err != nil {
		log.Printf("whoami: lookup api key %q failed: %v", apiKeyID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if key == nil {
		httperror.NotFoundCtx(w, r, "api key not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(WhoamiResponse{
		TenantID:   tenant.ID,
		TenantName: tenant.Name,
		Plan:       tenant.Plan,
		APIKeyID:   key.ID,
		APIKeyName: key.Name,
		Role:       role,
		CreatedAt:  tenant.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	})
}
