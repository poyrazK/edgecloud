package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// DeploymentHandler handles deployment HTTP requests.
type DeploymentHandler struct {
	deploymentSvc *service.DeploymentService
	workerSvc     service.AppTargetLookup
}

func NewDeploymentHandler(deploymentSvc *service.DeploymentService, workerSvc service.AppTargetLookup) *DeploymentHandler {
	return &DeploymentHandler{deploymentSvc: deploymentSvc, workerSvc: workerSvc}
}

// deployResponse is the JSON shape returned by `POST /api/deploy/{appName}`.
// Typed (vs the prior anonymous `map[string]interface{}`) so the contract
// is explicit and the test asserts against a struct, not a string match.
type deployResponse struct {
	ID      string   `json:"id"`
	Hash    string   `json:"hash"`
	URL     string   `json:"url"`
	Regions []string `json:"regions"`
}

func (h *DeploymentHandler) Deploy(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	// Validate app name
	if appName == "" || containsPathTraversal(appName) {
		http.Error(w, `{"error": "invalid app name"}`, http.StatusBadRequest)
		return
	}

	// Parse `?regions=us-east,eu-west`. Split on `,`, trim whitespace,
	// drop empties, dedupe (preserving first-seen order so the response
	// is stable). Invalid regions are caught at the service layer
	// (`IsValidRegion`) — we still return 400 from the handler with a
	// caller-friendly message because the service error string is
	// also surfaced via the internal log.
	regions, perr := parseRegions(r.URL.Query().Get("regions"))
	if perr != nil {
		http.Error(w, `{"error": "`+perr.Error()+`"}`, http.StatusBadRequest)
		return
	}

	// Read artifact from multipart form or raw body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error": "failed to read body"}`, http.StatusBadRequest)
		return
	}

	deployment, err := h.deploymentSvc.Deploy(r.Context(), tenantID, appName, bytes.NewReader(body), regions)
	if err != nil {
		if errors.Is(err, service.ErrMaxDeploymentsQuotaExceeded) {
			http.Error(w, `{"error": "max deployments quota exceeded"}`, http.StatusTooManyRequests)
			return
		}
		if errors.Is(err, service.ErrMaxAppsQuotaExceeded) {
			http.Error(w, `{"error": "max apps quota exceeded"}`, http.StatusTooManyRequests)
			return
		}
		log.Printf("internal error: %v", err)
		// Service-layer region validation surfaces as 400. The string
		// is the only signal we have today; the dedicated test
		// (deployment_test.go) pins the contract so a future
		// refactor can't silently demote this to 500.
		if strings.HasPrefix(err.Error(), "invalid region") {
			http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(deployResponse{
		ID:      deployment.ID,
		Hash:    deployment.Hash,
		URL:     "https://" + domain.IngressHost(tenantID, appName),
		Regions: []string(deployment.Regions),
	})
}

// parseRegions turns the `?regions=` query value into a deduped slice.
// Returns an error for entries that don't match `[a-z0-9-]{1,64}` so
// the caller can return 400 with a precise message. Empty input or
// all-empty-after-trim returns a nil slice and no error — the service
// layer treats nil/empty as "use the control plane's default region".
func parseRegions(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t == "" {
			continue
		}
		if _, dup := seen[t]; dup {
			continue
		}
		// Validate charset and length at the handler boundary so a
		// malformed value never reaches the service layer. Mirrors
		// service.IsValidRegion — the duplication is deliberate
		// (handler gives a clean 400 message; service is a second
		// line of defense).
		if !service.IsValidRegion(t) {
			return nil, fmt.Errorf("invalid region %q: must match [a-z0-9-]{1,64}", t)
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) == 0 {
		// All entries were blank/dupes — equivalent to no regions.
		return nil, nil
	}
	return out, nil
}

func (h *DeploymentHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	deploymentID := r.PathValue("deploymentID")

	deployment, err := h.deploymentSvc.GetDeployment(r.Context(), tenantID, deploymentID)
	if err != nil {
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}
	if deployment == nil {
		http.Error(w, `{"error": "deployment not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(deployment)
}

func (h *DeploymentHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	limit := 20
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	deployments, total, err := h.deploymentSvc.ListDeploymentsPaginatedWithTotal(r.Context(), tenantID, appName, limit, offset)
	if err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"items":  deployments,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *DeploymentHandler) Activate(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	deploymentID := r.PathValue("deploymentID")

	if err := h.deploymentSvc.ActivateDeployment(r.Context(), tenantID, appName, deploymentID); err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "activated"})
}

func (h *DeploymentHandler) GetActive(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	deployment, err := h.deploymentSvc.GetActiveDeployment(r.Context(), tenantID, appName)
	if err != nil || deployment == nil {
		http.Error(w, `{"error": "no active deployment"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(deployment)
}

// containsPathTraversal blocks the *decoded* traversal shapes ("/", "\\",
// ".."). The caller is responsible for passing a value that has already
// been percent-decoded — e.g. http.Request.PathValue (used by AppIngress
// and Deploy), or an explicit url.PathUnescape for body fields. Encoding
// bypasses (e.g. %2F, %2E%2E) are intentionally not caught here because
// the input is already decoded by the time this helper sees it; the
// helper's job is to reject post-decode traversal, not to decode.
func containsPathTraversal(s string) bool {
	if s == "" {
		return true
	}
	return strings.ContainsAny(s, "/\\") || strings.Contains(s, "..")
}

// AppIngress handles GET /api/apps/{appName}/ingress — tenant-authenticated
// diagnostic that returns whether the calling tenant's app is currently
// routable on a worker (and on which addr/port). Used by the CLI's
// `edge status` to validate that a `live_url` is actually live. The
// tenant filter is applied at the SQL level so a tenant cannot learn
// about another tenant's apps.
func (h *DeploymentHandler) AppIngress(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	if appName == "" || containsPathTraversal(appName) {
		http.Error(w, `{"error": "invalid app name"}`, http.StatusBadRequest)
		return
	}

	target, err := h.workerSvc.GetAppTarget(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}
	if target == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ready":    false,
			"app_name": appName,
			"reason":   "no running app found for this tenant",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ready":       true,
		"app_name":    target.AppName,
		"tenant_id":   target.TenantID,
		"worker_id":   target.WorkerID,
		"region":      target.Region,
		"worker_addr": target.WorkerAddr,
		"port":        target.Port,
	})
}
