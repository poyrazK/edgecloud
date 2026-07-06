package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// APIKeyHandler handles API key HTTP requests.
type APIKeyHandler struct {
	apiKeySvc service.APIKeyServiceInterface
}

func NewAPIKeyHandler(apiKeySvc service.APIKeyServiceInterface) *APIKeyHandler {
	return &APIKeyHandler{apiKeySvc: apiKeySvc}
}

type CreateAPIKeyRequest struct {
	Name     string `json:"name"`
	Role     string `json:"role"`
	TTLHours *int   `json:"ttl_hours,omitempty"` // nil = never expires
}

type CreateAPIKeyResponse struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Role  string `json:"role"`
	Token string `json:"token"` // Raw key shown only once
}

func (h *APIKeyHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	// Defensive guard: this handler must only run behind the auth
	// middleware. If it ever gets re-registered on a public route by
	// mistake, refuse rather than call CreateAPIKey with an empty
	// tenant_id (which would FK-violate the tenants table).
	if tenantID == "" {
		http.Error(w, `{"error": "authentication required"}`, http.StatusUnauthorized)
		return
	}

	var req CreateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.Name == "" {
		httperror.BadRequestCtx(w, r, "name is required")
		return
	}
	role := req.Role
	if role == "" {
		role = domain.RoleDeveloper
	}

	apiKey, rawKey, err := h.apiKeySvc.CreateAPIKey(r.Context(), tenantID, req.Name, role, req.TTLHours)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(CreateAPIKeyResponse{
		ID:    apiKey.ID,
		Name:  apiKey.Name,
		Role:  apiKey.Role,
		Token: rawKey,
	}); err != nil {
		log.Printf("Create API key: failed to encode response: %v", err)
	}
	auditRecord(r, "create", "api_key", apiKey.ID, "api key "+apiKey.Name+" created", "success")
}

func (h *APIKeyHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	keys, err := h.apiKeySvc.ListAPIKeys(r.Context(), tenantID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}

	infos := make([]domain.SafeAPIKeyResponse, len(keys))
	for i, k := range keys {
		infos[i] = k.ToSafeResponse()
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(infos); err != nil {
		log.Printf("List API keys: failed to encode response: %v", err)
	}
}

func (h *APIKeyHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	keyID := r.PathValue("keyID")
	if err := h.apiKeySvc.DeleteAPIKey(r.Context(), tenantID, keyID); err != nil {
		if errors.Is(err, service.ErrAPIKeyNotFound) {
			httperror.NotFoundCtx(w, r, "api key not found")
			return
		}
		httperror.InternalErrorCtx(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	auditRecord(r, "delete", "api_key", keyID, "api key "+keyID+" deleted", "success")
}

// Update handles PUT /api/v1/keys/{keyID} — update mutable fields of an API key.
func (h *APIKeyHandler) Update(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	keyID := r.PathValue("keyID")

	var req domain.UpdateAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}

	key, err := h.apiKeySvc.UpdateAPIKey(r.Context(), keyID, tenantID, &req)
	if err != nil {
		if errors.Is(err, service.ErrAPIKeyNotFound) {
			httperror.NotFoundCtx(w, r, "api key not found")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(key.ToSafeResponse()); err != nil {
		log.Printf("Update API key: failed to encode response: %v", err)
	}
	auditRecord(r, "update", "api_key", keyID, "api key "+keyID+" updated", "success")
}
