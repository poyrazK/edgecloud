package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
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
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.Name == "" {
		httperror.BadRequestCtx(w, r, "name is required")
		return
	}
	if req.KeyName == "" {
		req.KeyName = "default"
	}
	plan := req.Plan
	if plan == "" {
		plan = "free"
	}
	// Self-service bootstrap only accepts the free tier — paid plans must
	// go through the Stripe checkout endpoint (follow-up ticket). Reject
	// pro/business/enterprise here so a tenant can't claim paid quotas
	// without payment.
	if plan != "free" {
		httperror.BadRequestCtx(w, r, "plan: only 'free' is accepted at bootstrap; paid tiers require the checkout endpoint (coming soon)")
		return
	}

	tenant, rawKey, err := h.tenantSvc.BootstrapTenant(r.Context(), req.Name, plan, req.KeyName)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(BootstrapResponse{
		TenantID: tenant.ID,
		APIKey:   rawKey,
	}); err != nil {
		log.Printf("Bootstrap tenant: failed to encode response: %v", err)
	}
	auditRecord(r, "bootstrap", "tenant", tenant.ID, "tenant "+tenant.ID+" created via self-signup", "success")
	if DefaultTenantCreationLimiter != nil {
		DefaultTenantCreationLimiter.Record(service.StripPort(r.RemoteAddr))
	}
}

func (h *TenantHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.Name == "" {
		httperror.BadRequestCtx(w, r, "name is required")
		return
	}
	plan := req.Plan
	if plan == "" {
		plan = "free"
	}
	if !domain.IsValidPlan(plan) {
		httperror.BadRequestCtx(w, r, "plan: must be one of free, pro, business, enterprise")
		return
	}

	tenant, err := h.tenantSvc.CreateTenant(r.Context(), req.Name, plan)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(tenant); err != nil {
		log.Printf("Create tenant: failed to encode response: %v", err)
	}
	auditRecord(r, "create", "tenant", tenant.ID, "tenant "+tenant.Name+" created by admin", "success")
}

func (h *TenantHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	tenant, err := h.tenantSvc.GetTenant(r.Context(), tenantID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	if tenant == nil {
		httperror.NotFoundCtx(w, r, "tenant not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tenant); err != nil {
		log.Printf("Get tenant: failed to encode response: %v", err)
	}
}

func (h *TenantHandler) List(w http.ResponseWriter, r *http.Request) {
	tenants, err := h.tenantSvc.ListTenants(r.Context())
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tenants); err != nil {
		log.Printf("List tenants: failed to encode response: %v", err)
	}
}

type UpdateTenantRequest struct {
	Name string `json:"name"`
	Plan string `json:"plan"`
	// AllowlistedDestinations stays []string here — this struct is
	// the JSON request body. The conversion to pq.StringArray happens
	// at the assignment below so the domain stays consistent.
	AllowlistedDestinations []string `json:"allowlisted_destinations"`
	// PreserveQuotaLimits controls plan-change behavior. Default false:
	// changing `plan` re-applies the new tier's quota defaults
	// (max_deployments, max_apps, max_workers, max_memory_mb,
	// max_outbound_mb, max_requests_per_month). Set true to flip just the
	// plan label while leaving the per-tenant ceilings as the admin
	// hand-tuned them.
	PreserveQuotaLimits bool `json:"preserve_quota_limits"`
}

func (h *TenantHandler) Update(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")

	var req UpdateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}

	tenant, err := h.tenantSvc.GetTenant(r.Context(), tenantID)
	if err != nil || tenant == nil {
		httperror.NotFoundCtx(w, r, "tenant not found")
		return
	}

	if req.Name != "" {
		tenant.Name = req.Name
	}
	if len(req.AllowlistedDestinations) > 0 {
		// Convert []string -> domain.StringArrayFrom so the field type
		// matches the domain. The repo wraps the value in pq.Array()
		// on the way to Postgres; the conversion here just gets the Go
		// type right so the assignment compiles.
		tenant.AllowlistedDestinations = domain.StringArrayFrom(req.AllowlistedDestinations)
	}

	// Plan changes go through a dedicated service path because they may
	// also rewrite the quota row. The plan-change branch re-fetches after
	// the service commits so the response's embedded Quota reflects the
	// new tier (the original captured `tenant` would otherwise show the
	// OLD quota caps alongside the new plan).
	planChanged := false
	if req.Plan != "" && req.Plan != tenant.Plan {
		if err := h.tenantSvc.UpdateTenantPlan(r.Context(), tenantID, req.Plan, !req.PreserveQuotaLimits); err != nil {
			switch {
			case errors.Is(err, domain.ErrUnknownPlan):
				httperror.BadRequestCtx(w, r, "plan: must be one of free, pro, business, enterprise")
			case errors.Is(err, service.ErrTenantNotFound), errors.Is(err, service.ErrQuotaNotFound):
				httperror.NotFoundCtx(w, r, "tenant not found")
			default:
				log.Printf("UpdateTenantPlan(%s, %s): %v", tenantID, req.Plan, err)
				httperror.InternalErrorCtx(w, r)
			}
			return
		}
		planChanged = true
	} else if req.Plan != "" {
		// Same plan re-stated — no quota reapply, but validate anyway so
		// an unknown plan string still gets a 400.
		if !domain.IsValidPlan(req.Plan) {
			httperror.BadRequestCtx(w, r, "plan: must be one of free, pro, business, enterprise")
			return
		}
		tenant.Plan = req.Plan
	}

	if planChanged {
		// UpdateTenantPlan already wrote both rows in its own transaction.
		// Re-fetch the canonical post-state so the response payload is
		// consistent (new plan + new quota). Do NOT call UpdateTenant
		// below — it would write a second time with stale fields.
		fresh, err := h.tenantSvc.GetTenant(r.Context(), tenantID)
		if err != nil || fresh == nil {
			log.Printf("Update: post-plan-change re-fetch failed for %s: %v", tenantID, err)
			httperror.InternalErrorCtx(w, r)
			return
		}
		tenant = fresh
	}

	if err := h.tenantSvc.UpdateTenant(r.Context(), &tenant.Tenant); err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tenant); err != nil {
		log.Printf("Update tenant: failed to encode response: %v", err)
	}
	auditRecord(r, "update", "tenant", tenantID, "tenant "+tenantID+" updated", "success")
}

func (h *TenantHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if err := h.tenantSvc.DeleteTenant(r.Context(), tenantID); err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	auditRecord(r, "delete", "tenant", tenantID, "tenant "+tenantID+" deleted", "success")
	w.WriteHeader(http.StatusNoContent)
}
