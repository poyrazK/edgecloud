package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// DeploymentHandler handles deployment HTTP requests.
type DeploymentHandler struct {
	deploymentSvc *service.DeploymentService
	workerSvc     service.AppTargetLookup
	// rollbackSvc is a narrow contract for the rollback handler so the
	// test can stub it without standing up the full *service.DeploymentService
	// (DB + NATS + publisher + artifact store). The concrete
	// *service.DeploymentService satisfies it.
	rollbackSvc deploymentRollbacker
	// activateSvc mirrors rollbackSvc for the Activate handler — narrow
	// contract lets tests stub the activate path without the full service
	// surface. Concrete *service.DeploymentService satisfies it.
	activateSvc deploymentActivator
}

// deploymentRollbacker is the narrow contract the Rollback handler needs.
// Kept package-local so handler tests can implement it inline without
// having to mock the full DeploymentService surface.
type deploymentRollbacker interface {
	RollbackDeployment(ctx context.Context, tenantID, appName string) (string, error)
}

// deploymentActivator is the narrow contract the Activate handler needs.
// Mirrors deploymentRollbacker for the activate path.
type deploymentActivator interface {
	ActivateDeployment(ctx context.Context, tenantID, appName, deploymentID string) error
}

func NewDeploymentHandler(deploymentSvc *service.DeploymentService, workerSvc service.AppTargetLookup) *DeploymentHandler {
	return &DeploymentHandler{
		deploymentSvc: deploymentSvc,
		workerSvc:     workerSvc,
		// Concrete *service.DeploymentService satisfies the narrow interfaces.
		// nil is also fine for tests that only exercise the workerSvc path
		// (e.g. AppIngress) — those methods never touch rollbackSvc /
		// activateSvc.
		rollbackSvc: deploymentSvc,
		activateSvc: deploymentSvc,
	}
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
		httperror.BadRequestCtx(w, r, "invalid app name")
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
		httperror.BadRequestCtx(w, r, "failed to read body")
		return
	}

	deployment, err := h.deploymentSvc.Deploy(r.Context(), tenantID, appName, bytes.NewReader(body), regions)
	if err != nil {
		if errors.Is(err, service.ErrMaxDeploymentsQuotaExceeded) {
			httperror.QuotaExceededCtx(w, r, "max deployments quota exceeded")
			return
		}
		if errors.Is(err, service.ErrMaxAppsQuotaExceeded) {
			httperror.QuotaExceededCtx(w, r, "max apps quota exceeded")
			return
		}
		if errors.Is(err, service.ErrInvalidRegion) {
			http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		if errors.Is(err, service.ErrTooManyRegions) {
			http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(deployResponse{
		ID:      deployment.ID,
		Hash:    deployment.Hash,
		URL:     "https://" + domain.IngressHost(tenantID, appName),
		Regions: domain.StringArrayTo(deployment.Regions),
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
	// Enforce the per-deployment cap AFTER dedupe so duplicate values
	// don't count toward the limit. The service also enforces this as
	// defense-in-depth for non-HTTP callers.
	if len(out) > service.MaxRegionsPerDeployment {
		return nil, fmt.Errorf("too many regions: %d (max %d)", len(out), service.MaxRegionsPerDeployment)
	}
	return out, nil
}

func (h *DeploymentHandler) GetStatus(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	deploymentID := r.PathValue("deploymentID")

	deployment, err := h.deploymentSvc.GetDeployment(r.Context(), tenantID, deploymentID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	if deployment == nil {
		httperror.NotFoundCtx(w, r, "deployment not found")
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
		httperror.InternalErrorCtx(w, r)
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

// Activate handles POST /api/apps/{appName}/activate/{deploymentID}.
//
// Status codes:
//   - 200: activated; body is {"status": "activated"}
//   - 502: activation committed but the post-commit NATS publish of
//     the TaskMessage failed — workers may still be serving the prior
//     deployment. Client should re-activate the desired deployment
//     (a plain retry will 409 because the row is already in the
//     desired state, or 404 if the deploy was deleted).
//   - 500: anything else (DB error, etc.).
func (h *DeploymentHandler) Activate(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	deploymentID := r.PathValue("deploymentID")

	if err := h.activateSvc.ActivateDeployment(r.Context(), tenantID, appName, deploymentID); err != nil {
		if errors.Is(err, service.ErrPublishFailed) {
			http.Error(w,
				`{"error": "activation committed but worker notification failed; please retry"}`,
				http.StatusBadGateway)
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "activated"})
}

// Rollback handles POST /api/apps/{appName}/rollback. Swaps the active
// deployment back to the stored last_good_deployment_id and republishes a
// TaskMessage so workers reconcile.
//
// Status codes:
//   - 200: rolled back; body is {"deployment_id": "<new active id>"}
//   - 404: no active deployment for this app (user never activated)
//   - 409: app is active but has no last-good pointer (only ever activated
//     one deployment, so there is nothing to roll back to)
//   - 502: rollback committed but the post-commit NATS publish failed —
//     workers may still be serving the prior deployment. Client should
//     re-activate the desired deployment; a plain retry will 409
//     because last_good was cleared on this attempt.
//   - 500: anything else (DB error, etc.).
func (h *DeploymentHandler) Rollback(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	if appName == "" || containsPathTraversal(appName) {
		http.Error(w, `{"error": "invalid app name"}`, http.StatusBadRequest)
		return
	}

	newID, err := h.rollbackSvc.RollbackDeployment(r.Context(), tenantID, appName)
	if err != nil {
		if errors.Is(err, service.ErrNoLastGood) {
			http.Error(w, `{"error": "no previous deployment to roll back to"}`, http.StatusConflict)
			return
		}
		if errors.Is(err, service.ErrNoActiveDeployment) {
			http.Error(w, `{"error": "no active deployment"}`, http.StatusNotFound)
			return
		}
		if errors.Is(err, service.ErrPublishFailed) {
			http.Error(w,
				`{"error": "rollback committed but worker notification failed; please retry"}`,
				http.StatusBadGateway)
			return
		}
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "rollback failed"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"deployment_id": newID})
}

func (h *DeploymentHandler) GetActive(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	deployment, err := h.deploymentSvc.GetActiveDeployment(r.Context(), tenantID, appName)
	if err != nil || deployment == nil {
		httperror.NotFoundCtx(w, r, "no active deployment")
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
		httperror.BadRequestCtx(w, r, "invalid app name")
		return
	}

	target, err := h.workerSvc.GetAppTarget(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
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
