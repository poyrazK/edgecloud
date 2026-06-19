package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
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
	Name string `json:"name"`
	Role string `json:"role"`
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
		http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, `{"error": "name is required"}`, http.StatusBadRequest)
		return
	}
	role := req.Role
	if role == "" {
		role = domain.RoleDeveloper
	}

	apiKey, rawKey, err := h.apiKeySvc.CreateAPIKey(r.Context(), tenantID, req.Name, role)
	if err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(CreateAPIKeyResponse{
		ID:    apiKey.ID,
		Name:  apiKey.Name,
		Role:  apiKey.Role,
		Token: rawKey,
	})
}

func (h *APIKeyHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	keys, err := h.apiKeySvc.ListAPIKeys(r.Context(), tenantID)
	if err != nil {
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	// Don't expose key_hash
	type keyInfo struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Role      string `json:"role"`
		CreatedAt string `json:"created_at"`
	}
	infos := make([]keyInfo, len(keys))
	for i, k := range keys {
		infos[i] = keyInfo{
			ID:        k.ID,
			Name:      k.Name,
			Role:      k.Role,
			CreatedAt: k.CreatedAt.Format("2006-01-02T15:04:05Z"),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(infos)
}

func (h *APIKeyHandler) Delete(w http.ResponseWriter, r *http.Request) {
	keyID := r.PathValue("keyID")
	if err := h.apiKeySvc.DeleteAPIKey(r.Context(), keyID); err != nil {
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
