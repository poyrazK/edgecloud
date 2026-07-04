package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// EgressTenantServiceInterface is the tenant-service subset needed by EgressHandler.
type EgressTenantServiceInterface interface {
	GetEgressAllowlist(ctx context.Context, tenantID string) ([]string, error)
	UpdateEgressAllowlist(ctx context.Context, tenantID string, allowlist []string) error
}

// EgressDeploymentServiceInterface is the deployment-service subset needed by EgressHandler.
type EgressDeploymentServiceInterface interface {
	RepublishActiveDeployments(ctx context.Context, tenantID string) error
}

// EgressHandler exposes the tenant self-service egress allowlist API.
//
// GET  /api/egress  — return the current allowlist for the authenticated tenant
// PUT  /api/egress  — replace the allowlist; triggers republish of active deployments
type EgressHandler struct {
	tenantSvc     EgressTenantServiceInterface
	deploymentSvc EgressDeploymentServiceInterface
}

func NewEgressHandler(tenantSvc EgressTenantServiceInterface, deploymentSvc EgressDeploymentServiceInterface) *EgressHandler {
	return &EgressHandler{tenantSvc: tenantSvc, deploymentSvc: deploymentSvc}
}

type egressResponse struct {
	Allowlist []string `json:"allowlist"`
}

type updateEgressRequest struct {
	Allowlist []string `json:"allowlist"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		log.Printf("writeJSON: marshal failed: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(buf.Bytes()); err != nil {
		log.Printf("writeJSON: write failed: %v", err)
	}
}

// Get returns the authenticated tenant's current outbound allowlist.
func (h *EgressHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	list, err := h.tenantSvc.GetEgressAllowlist(r.Context(), tenantID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	writeJSON(w, http.StatusOK, egressResponse{Allowlist: list})
}

// Update replaces the authenticated tenant's outbound allowlist and immediately
// republishes TaskMessages for all active deployments so workers enforce the
// new policy without a manual re-activate.
func (h *EgressHandler) Update(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	var req updateEgressRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	// Treat a missing field the same as an explicit empty list (allow-all).
	if req.Allowlist == nil {
		req.Allowlist = []string{}
	}
	// Normalize to lowercase here so the response body matches what is stored.
	// The service also lowercases before writing, but operates on a local copy;
	// normalizing in the handler keeps req.Allowlist consistent with stored state.
	for i := range req.Allowlist {
		req.Allowlist[i] = strings.ToLower(req.Allowlist[i])
	}

	if err := h.tenantSvc.UpdateEgressAllowlist(r.Context(), tenantID, req.Allowlist); err != nil {
		var valErr *service.EgressValidationError
		if errors.As(err, &valErr) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		} else {
			log.Printf("egress update: DB error for tenant %s: %v", tenantID, err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		}
		return
	}

	// Republish so running workers pick up the new policy immediately.
	// Failure here means workers will lag until their next re-activate;
	// we surface it as 500 so the client knows to retry.
	if err := h.deploymentSvc.RepublishActiveDeployments(r.Context(), tenantID); err != nil {
		log.Printf("egress update: republish failed for tenant %s: %v", tenantID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "allowlist saved but worker propagation failed; retry to push to workers",
		})
		return
	}

	writeJSON(w, http.StatusOK, egressResponse(req))
	auditRecord(r, "update", "egress", tenantID, "egress allowlist updated for tenant "+tenantID, "success")
}
