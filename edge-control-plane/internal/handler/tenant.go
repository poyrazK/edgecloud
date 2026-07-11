package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// TenantHandler handles tenant HTTP requests.
type TenantHandler struct {
	tenantSvc service.TenantServiceInterface
	// quotaRepo is the per-tenant rate-limit write surface (issue #305).
	// The service layer is intentionally bypassed for this admin path —
	// it is a thin write-through with no business logic, and the
	// existing QuotaOverride pattern keeps the service layer out of
	// the audit-logged admin hot path. nil is acceptable for handlers
	// that never call SetTenantRateLimitAdmin; production wiring in
	// app.New always sets it.
	quotaRepo QuotaRepoForAdminWrite
}

// QuotaRepoForAdminWrite is the narrow slice of *repository.QuotaRepository
// the admin rate-limit endpoint needs (issue #305). Mirrors the
// QuotaRepoForRateLimit pattern on QuotaHandler: handler tests inject
// a mock that satisfies only this one method, no full repo stand-up.
type QuotaRepoForAdminWrite interface {
	SetRateLimit(ctx context.Context, tenantID string, req domain.TenantRateLimitRequest) (*domain.TenantRateLimitResponse, error)
}

func NewTenantHandler(tenantSvc service.TenantServiceInterface, quotaRepo QuotaRepoForAdminWrite) *TenantHandler {
	return &TenantHandler{tenantSvc: tenantSvc, quotaRepo: quotaRepo}
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
	// go through the billing checkout endpoint (issue #419, billing
	// abstraction). Reject pro/business/enterprise here so a tenant
	// can't claim paid quotas without payment.
	if plan != "free" {
		httperror.BadRequestCtx(w, r, "plan: only 'free' is accepted at bootstrap; paid tiers require /api/v1/billing/checkout first")
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

// Enable clears tenants.disabled_at for a tenant that the quota
// path previously stamped (issue #440). Owner-only admin
// operation. Idempotent: calling on an already-enabled tenant is
// a 200 with no state change.
//
// Path: POST /api/v1/admin/tenants/{tenantID}/enable
func (h *TenantHandler) Enable(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if err := h.tenantSvc.EnableTenant(r.Context(), tenantID); err != nil {
		if errors.Is(err, service.ErrTenantNotFound) {
			httperror.NotFoundCtx(w, r, "tenant not found")
			return
		}
		httperror.InternalErrorCtx(w, r)
		return
	}
	auditRecord(r, "enable", "tenant", tenantID, "tenant "+tenantID+" re-enabled", "success")
	w.WriteHeader(http.StatusOK)
}

// QuotaOverrideRequest is the wire shape for the admin override
// endpoint (issue #420). All fields are optional; absent fields
// leave the underlying row untouched. RFC3339 timestamps validate
// against time.RFC3339; non-negative ints validate against
// >= 0. The sentinel "-1" for max_requests_per_month and
// max_outbound_mb is the unlimited signal; passing a negative
// value other than -1 returns 400.
type QuotaOverrideRequest struct {
	MaxRequestsPerMonth      *int    `json:"max_requests_per_month,omitempty"`
	MaxOutboundMB            *int    `json:"max_outbound_mb,omitempty"`
	MaxDeployments           *int    `json:"max_deployments,omitempty"`
	SetOverageAllowedUntil   *string `json:"set_overage_allowed_until,omitempty"`
	ClearOverageAllowedUntil bool    `json:"clear_overage_allowed_until,omitempty"`
	ClearDisabledAt          bool    `json:"clear_disabled_at,omitempty"`
	ClearGrace               bool    `json:"clear_grace,omitempty"`
}

// QuotaOverride handles POST /api/v1/admin/tenants/{tenantID}/quota-override.
// Owner-role admin endpoint for manual recovery when a tenant crosses
// a cap and the period hasn't reset yet. Every call is audit-logged
// via auditRecord — operators and tenants can trace exactly which
// fields were changed and when.
func (h *TenantHandler) QuotaOverride(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if tenantID == "" || containsPathTraversal(tenantID) {
		httperror.BadRequestCtx(w, r, "invalid tenant id")
		return
	}

	var req QuotaOverrideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}

	// Validate int fields. -1 is the unlimited sentinel; any other
	// negative value is rejected so a typo doesn't quietly turn
	// into a large unsigned.
	if req.MaxRequestsPerMonth != nil && *req.MaxRequestsPerMonth < -1 {
		httperror.BadRequestCtx(w, r, "max_requests_per_month: must be -1 (unlimited) or >= 0")
		return
	}
	if req.MaxOutboundMB != nil && *req.MaxOutboundMB < -1 {
		httperror.BadRequestCtx(w, r, "max_outbound_mb: must be -1 (unlimited) or >= 0")
		return
	}
	if req.MaxDeployments != nil && *req.MaxDeployments < 0 {
		httperror.BadRequestCtx(w, r, "max_deployments: must be >= 0")
		return
	}
	// set_overage_allowed_until and clear_overage_allowed_until are
	// mutually exclusive — operators either set a new grace clock
	// or clear the existing one, not both.
	if req.SetOverageAllowedUntil != nil && req.ClearOverageAllowedUntil {
		httperror.BadRequestCtx(w, r, "set_overage_allowed_until and clear_overage_allowed_until are mutually exclusive")
		return
	}

	svcReq := service.QuotaOverrideRequest{
		TenantID:                 tenantID,
		MaxRequestsPerMonth:      req.MaxRequestsPerMonth,
		MaxOutboundMB:            req.MaxOutboundMB,
		MaxDeployments:           req.MaxDeployments,
		ClearOverageAllowedUntil: req.ClearOverageAllowedUntil,
		ClearDisabledAt:          req.ClearDisabledAt,
		ClearGrace:               req.ClearGrace,
	}
	if req.SetOverageAllowedUntil != nil {
		ts, err := time.Parse(time.RFC3339, *req.SetOverageAllowedUntil)
		if err != nil {
			httperror.BadRequestCtx(w, r, "set_overage_allowed_until: must be RFC3339")
			return
		}
		svcReq.SetOverageAllowedUntil = &ts
	}

	tq, err := h.tenantSvc.OverrideTenantQuota(r.Context(), svcReq)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrTenantNotFound), errors.Is(err, service.ErrQuotaNotFound):
			httperror.NotFoundCtx(w, r, "tenant not found")
		default:
			log.Printf("QuotaOverride(%s): %v", tenantID, err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}

	// Build a one-line details string so the audit row is
	// grep-friendly. The full request body could include PII
	// (operator notes) so we don't echo it.
	auditRecord(r, "quota.override", "tenant", tenantID,
		"quota override applied to tenant "+tenantID, "success")

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tq); err != nil {
		log.Printf("QuotaOverride: failed to encode response: %v", err)
	}
}

// SetTenantRateLimitAdmin handles
// PUT /api/v1/admin/tenants/{tenantID}/rate-limit (issue #305, owner-role
// admin endpoint for per-tenant data-plane rate limiting). Mirrors the
// shape of QuotaOverride at internal/handler/tenant.go:304-380 but for
// the four tenant-rate-limit columns instead of the existing monthly
// quota caps. Every call is audit-logged via auditRecord.
//
// Sentinel semantics match the existing Max* columns on the quotas
// table: any field < 0 is the unlimited signal; == 0 means "unset /
// admin-cleared" (skip the cap check); > 0 is the cap. The renderer
// at edge-ingress/src/caddy.rs treats < 0 as "no cap" identically to
// == 0 (defensive — both should disable the rate_limit route), so the
// admin UI is free to send either. We validate >= -1 at the handler
// boundary so a typo doesn't quietly turn into a large unsigned.
//
// Returns 404 when the tenant has no quotas row (callers must
// provision the tenant first via the existing tenant-create path).
func (h *TenantHandler) SetTenantRateLimitAdmin(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if tenantID == "" || containsPathTraversal(tenantID) {
		httperror.BadRequestCtx(w, r, "invalid tenant id")
		return
	}

	var req domain.TenantRateLimitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}

	// -1 is the unlimited sentinel for the integer axes. Any other
	// negative value is rejected so a typo doesn't quietly turn into
	// a large unsigned when the value crosses the JSON int boundary
	// into the SQL INTEGER column. BandwidthBPS is int64 — same -1
	// rule applies.
	if req.RPS < -1 {
		httperror.BadRequestCtx(w, r, "rps: must be -1 (unlimited) or >= 0")
		return
	}
	if req.Burst < -1 {
		httperror.BadRequestCtx(w, r, "burst: must be -1 (unlimited) or >= 0")
		return
	}
	if req.ConcurrentLimit < -1 {
		httperror.BadRequestCtx(w, r, "concurrent_limit: must be -1 (unlimited) or >= 0")
		return
	}
	if req.BandwidthBPS < -1 {
		httperror.BadRequestCtx(w, r, "bandwidth_bps: must be -1 (unlimited) or >= 0")
		return
	}

	rl, err := h.quotaRepo.SetRateLimit(r.Context(), tenantID, req)
	if err != nil {
		log.Printf("SetTenantRateLimitAdmin(%s): %v", tenantID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if rl == nil {
		// No quotas row — the tenant exists but the platform hasn't
		// provisioned a quotas row for them yet. Surface as 404 so
		// operators know to fix the upstream provisioning rather
		// than retrying.
		httperror.NotFoundCtx(w, r, "tenant not found")
		return
	}

	auditRecord(r, "rate_limit.set", "tenant", tenantID,
		"per-tenant data-plane rate limit updated for tenant "+tenantID, "success")

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rl); err != nil {
		log.Printf("SetTenantRateLimitAdmin(%s): failed to encode response: %v", tenantID, err)
	}
}
