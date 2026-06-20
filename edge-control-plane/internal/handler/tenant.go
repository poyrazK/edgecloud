package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/lib/pq"
)

// TenantHandler handles tenant HTTP requests.
type TenantHandler struct {
	tenantSvc service.TenantServiceInterface
}

func NewTenantHandler(tenantSvc service.TenantServiceInterface) *TenantHandler {
	return &TenantHandler{tenantSvc: tenantSvc}
}

type CreateTenantRequest struct {
	Name string `json:"name"`
	Plan string `json:"plan"`
}

type BootstrapRequest struct {
	Name    string `json:"name"`
	Plan    string `json:"plan"`
	KeyName string `json:"key_name"`
}

type BootstrapResponse struct {
	TenantID string `json:"tenant_id"`
	APIKey   string `json:"api_key"`
}

// Bootstrap handles POST /api/tenants — creates a tenant + first API key atomically.
// This is the self-signup endpoint; no auth required.
func (h *TenantHandler) Bootstrap(w http.ResponseWriter, r *http.Request) {
	var req BootstrapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, `{"error": "name is required"}`, http.StatusBadRequest)
		return
	}
	if req.KeyName == "" {
		req.KeyName = "default"
	}
	plan := req.Plan
	if plan == "" {
		plan = "free"
	}

	tenant, rawKey, err := h.tenantSvc.BootstrapTenant(r.Context(), req.Name, plan, req.KeyName)
	if err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(BootstrapResponse{
		TenantID: tenant.ID,
		APIKey:   rawKey,
	})
}

func (h *TenantHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, `{"error": "name is required"}`, http.StatusBadRequest)
		return
	}
	plan := req.Plan
	if plan == "" {
		plan = "free"
	}

	tenant, err := h.tenantSvc.CreateTenant(r.Context(), req.Name, plan)
	if err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(tenant)
}

func (h *TenantHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	tenant, err := h.tenantSvc.GetTenant(r.Context(), tenantID)
	if err != nil {
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}
	if tenant == nil {
		http.Error(w, `{"error": "tenant not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tenant)
}

func (h *TenantHandler) List(w http.ResponseWriter, r *http.Request) {
	tenants, err := h.tenantSvc.ListTenants(r.Context())
	if err != nil {
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tenants)
}

type UpdateTenantRequest struct {
	Name string `json:"name"`
	Plan string `json:"plan"`
	// AllowlistedDestinations stays []string here — this struct is
	// the JSON request body. The conversion to pq.StringArray happens
	// at the assignment below so the domain stays consistent.
	AllowlistedDestinations []string `json:"allowlisted_destinations"`
}

func (h *TenantHandler) Update(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")

	var req UpdateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		return
	}

	tenant, err := h.tenantSvc.GetTenant(r.Context(), tenantID)
	if err != nil || tenant == nil {
		http.Error(w, `{"error": "tenant not found"}`, http.StatusNotFound)
		return
	}

	if req.Name != "" {
		tenant.Name = req.Name
	}
	if req.Plan != "" {
		tenant.Plan = req.Plan
	}
	if len(req.AllowlistedDestinations) > 0 {
		// Convert []string -> pq.StringArray so the field type matches
		// the domain. The repo wraps the value in pq.Array() on the
		// way to Postgres; the conversion here just gets the Go type
		// right so the assignment compiles.
		tenant.AllowlistedDestinations = pq.StringArray(req.AllowlistedDestinations)
	}

	if err := h.tenantSvc.UpdateTenant(r.Context(), &tenant.Tenant); err != nil {
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tenant)
}

func (h *TenantHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if err := h.tenantSvc.DeleteTenant(r.Context(), tenantID); err != nil {
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
