package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/provenance"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
)

// DeploymentHandler handles deployment HTTP requests.
type DeploymentHandler struct {
	deploymentSvc *service.DeploymentService
	workerSvc     service.AppTargetLookup
	trafficSvc    *service.TrafficService
	rollbackSvc   deploymentRollbacker
	activateSvc   deploymentActivator
	promoteSvc    deploymentPromoter
	// artifactStore is used by the precompile step to read .wasm and write .cwasm.
	artifactStore storage.ArtifactStore
	// wasm2cwasmPath is the path to the wasm2cwasm binary for AOT pre-compilation.
	// Empty = skip pre-compilation.
	wasm2cwasmPath string
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

// deploymentPromoter is the narrow contract the Promote handler needs.
// PromoteDeployment activates a deployment under a different app name
// than it was originally deployed under (preview → production).
type deploymentPromoter interface {
	PromoteDeployment(ctx context.Context, tenantID, targetAppName, deploymentID string) error
}

func NewDeploymentHandler(deploymentSvc *service.DeploymentService, workerSvc service.AppTargetLookup, trafficSvc *service.TrafficService, artifactStore storage.ArtifactStore, wasm2cwasmPath string) *DeploymentHandler {
	return &DeploymentHandler{
		deploymentSvc:  deploymentSvc,
		workerSvc:      workerSvc,
		trafficSvc:     trafficSvc,
		rollbackSvc:    deploymentSvc,
		activateSvc:    deploymentSvc,
		promoteSvc:     deploymentSvc,
		artifactStore:  artifactStore,
		wasm2cwasmPath: wasm2cwasmPath,
	}
}

// deployResponse is the JSON shape returned by `POST /api/deploy/{appName}`.
// Typed (vs the prior anonymous `map[string]interface{}`) so the contract
// is explicit and the test asserts against a struct, not a string match.
type deployResponse struct {
	ID                  string   `json:"id"`
	Hash                string   `json:"hash"`
	URL                 string   `json:"url"`
	Regions             []string `json:"regions"`
	AutoRollbackEnabled bool     `json:"auto_rollback_enabled"`
	DesiredReplicas     int      `json:"desired_replicas"`
}

func (h *DeploymentHandler) Deploy(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	// Validate app name
	if !validateAppName(w, appName) {
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

	// Parse `?auto-rollback=true|false`. Defaults to false. Uses
	// `strconv.ParseBool` so the user can pass any of the canonical
	// truthy strings ("1", "t", "true", "TRUE", …); a non-boolean
	// value returns 400 rather than being silently coerced to false
	// (silent coercion would let a typo like `?auto-rollback=ture`
	// disable a feature the tenant thought they had enabled).
	autoRollback, aperr := parseBoolQuery(r.URL.Query().Get("auto-rollback"), false)
	if aperr != nil {
		http.Error(w, `{"error": "`+aperr.Error()+`"}`, http.StatusBadRequest)
		return
	}

	// Parse `?replicas=N` (issue #316). Defaults to 0 (no threshold).
	// Must be a non-negative integer.
	desiredReplicas, rerr := parseIntQuery(r.URL.Query().Get("replicas"), 0)
	if rerr != nil {
		http.Error(w, `{"error": "`+rerr.Error()+`"}`, http.StatusBadRequest)
		return
	}

	// PR2 — wire-format break: the deploy request is now
	// `multipart/form-data` with one required `file` part (the wasm
	// artifact bytes) and one optional `build_metadata` part (a JSON
	// object the CLI captured at build time). The raw-octet-stream
	// shape used by older CLIs is rejected with 415 — the CLI ships
	// alongside the server, so a wire break is acceptable per the
	// release notes for issue #307 PR2.
	//
	// The streaming multipart.Reader approach keeps the file
	// bytes flowing directly into the service's io.Reader, no
	// RAM buffering of the artifact. The optional `build_metadata`
	// part is parsed in-memory (a few KiB) so the service can
	// construct the SLSA L1 Statement envelope with toolchain
	// info — PR2.6 threads it through the service signature.
	mediaType, params, mterr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mterr != nil || !strings.HasPrefix(mediaType, "multipart/") {
		http.Error(w, `{"error":"deploy now requires multipart/form-data with a 'file' part (issue #307 PR2); upgrade the edge CLI"}`,
			http.StatusUnsupportedMediaType)
		return
	}
	boundary := params["boundary"]
	if boundary == "" {
		http.Error(w, `{"error":"missing multipart boundary"}`, http.StatusBadRequest)
		return
	}

	// Cap the request body at MaxArtifactSize. MaxBytesReader
	// surfaces *http.MaxBytesError mid-stream that we map to 413.
	r.Body = http.MaxBytesReader(w, r.Body, service.MaxArtifactSize)
	mr := multipart.NewReader(r.Body, boundary)

	filePart, buildMetadata, mperr := extractDeployParts(mr)
	if mperr != nil {
		// *http.MaxBytesError surfaces here when the multipart
		// body exceeds the cap. Map to 413 first; everything else
		// is 400 (malformed multipart).
		var maxErr *http.MaxBytesError
		if errors.As(mperr, &maxErr) {
			http.Error(w, `{"error":"artifact exceeds maximum size"}`, http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, `{"error":"`+mperr.Error()+`"}`, http.StatusBadRequest)
		return
	}
	defer func() { _ = filePart.Close() }()

	// Decode the optional `build_metadata` form field into a
	// CLISideMetadata struct. A missing or malformed value is
	// best-effort: the service builds an envelope with "unknown"
	// toolchain fields rather than refusing the deploy. We
	// intentionally swallow the error here — anything > 0 bytes
	// is a valid JSON object or an operator-typo; either way,
	// the deploy proceeds and the audit pipeline can flag the
	// missing provenance later.
	var cliMeta *provenance.CLISideMetadata
	if len(buildMetadata) > 0 {
		var parsed provenance.CLISideMetadata
		if jerr := json.Unmarshal(buildMetadata, &parsed); jerr == nil {
			cliMeta = &parsed
		}
	}

	deployment, err := h.deploymentSvc.Deploy(r.Context(), tenantID, appName, filePart, regions, autoRollback, desiredReplicas, cliMeta)
	if err != nil {
		// *http.MaxBytesError surfaces from the service's streaming
		// reads when the body exceeds the cap (chunked uploads
		// without a Content-Length header). Map to 413.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, `{"error":"artifact exceeds maximum size"}`, http.StatusRequestEntityTooLarge)
			return
		}
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
		// ErrInvalidWasm is what the service returns when the
		// 4-byte magic check inside the tx callback fails. Map to
		// 400 (not 422 or 500) — the request body was syntactically
		// valid but the content wasn't a wasm module.
		if errors.Is(err, service.ErrInvalidWasm) {
			http.Error(w, `{"error": "invalid wasm artifact: missing magic bytes"}`, http.StatusBadRequest)
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(deployResponse{
		ID:                  deployment.ID,
		Hash:                deployment.Hash,
		URL:                 "https://" + domain.IngressHost(tenantID, appName),
		Regions:             domain.StringArrayTo(deployment.Regions),
		AutoRollbackEnabled: deployment.AutoRollbackEnabled,
	}); err != nil {
		log.Printf("Deploy: failed to encode response: %v", err)
	}
	auditRecord(r, "deploy", "deployment", deployment.ID, "deployment "+deployment.ID+" created for app "+appName, "success")
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

// parseBoolQuery parses a query-string boolean with a default when the
// parameter is absent. Returns an error for unparseable values so the
// caller can return 400 — silently coercing to the default would let
// a typo disable a feature the tenant thought they had enabled
// (e.g. `?auto-rollback=ture` ≠ `?auto-rollback=true`).
func parseBoolQuery(raw string, defaultVal bool) (bool, error) {
	if raw == "" {
		return defaultVal, nil
	}
	return strconv.ParseBool(raw)
}

// parseIntQuery parses a query-string integer with a default when the
// parameter is absent. Returns an error for unparseable values so the
// caller can return 400. Negative values are rejected.
func parseIntQuery(raw string, defaultVal int) (int, error) {
	if raw == "" {
		return defaultVal, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid integer %q", raw)
	}
	if n < 0 {
		return 0, fmt.Errorf("negative value not allowed: %d", n)
	}
	return n, nil
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
	if err := json.NewEncoder(w).Encode(deployment); err != nil {
		log.Printf("GetStatus: failed to encode response: %v", err)
	}
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
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"items":  deployments,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	}); err != nil {
		log.Printf("List deployments: failed to encode response: %v", err)
	}
}

// writePublishFailureEnvelope writes the 502 Bad Gateway response for
// a partially-failed Activate / Rollback / AutoRollback publish. The
// body carries the per-region breakdown so the operator can see
// exactly which regions got the message and which are pending retry.
//
// Used by all three 502 sites in this file + handler/internal.go.
// If err is not a *service.PublishError (e.g. a future regression
// that bypasses the typed wrapper), the body falls back to an
// empty arrays + the static error message — the 502 contract
// holds regardless. errors.Is(err, service.ErrPublishFailed) is
// still matched by the caller before this helper is reached, so
// callers don't need to repeat the sentinel check.
//
// Issue #332: the envelope additionally surfaces the per-region
// artifact-cache push outcome (`regions_cached_succeeded`,
// `regions_cached_skipped`, `regions_cache_failed`) so operators
// can distinguish "NATS publish failed" from "cache push failed"
// from the same 502 body. Pre-#332 clients parsing
// `regions_published` / `regions_failed` see no change. The
// `regions_cached` key is preserved as the union of
// `regions_cached_succeeded` + `regions_cached_skipped` for
// backward-compat with PR-2 clients that read it.
func writePublishFailureEnvelope(w http.ResponseWriter, r *http.Request, err error, staticMessage string) {
	details := map[string]any{
		"regions_published":        []string{},
		"regions_failed":           []string{},
		"regions_cached_succeeded": []string{},
		"regions_cached_skipped":   []string{},
		"regions_cached":           []string{},
		"regions_cache_failed":     []string{},
	}
	var pubErr *service.PublishError
	if errors.As(err, &pubErr) {
		details["regions_published"] = pubErr.Published
		details["regions_failed"] = pubErr.Failed
		details["regions_cached_succeeded"] = pubErr.CachedSucceeded
		details["regions_cached_skipped"] = pubErr.CachedSkipped
		// Backward-compat: union the two Cached slices into the
		// pre-PR-2-follow-up `regions_cached` key.
		merged := make([]string, 0, len(pubErr.CachedSucceeded)+len(pubErr.CachedSkipped))
		merged = append(merged, pubErr.CachedSucceeded...)
		merged = append(merged, pubErr.CachedSkipped...)
		details["regions_cached"] = merged
		details["regions_cache_failed"] = pubErr.CacheFailed
	}
	httperror.BadGatewayCtx(w, r, staticMessage, details)
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

	// Validate both path parameters. The deployment id flows into the
	// /registry/{tenant}/{app}/{deployment}.wasm path on the worker
	// (see Download handler) — a ".." or "/" in the id lets a caller
	// reference arbitrary files on the worker's filesystem. Reject
	// 400 here rather than 500 from the storage layer.
	if !validateAppName(w, appName) {
		return
	}
	if !validateDeploymentID(w, deploymentID) {
		return
	}

	weightStr := r.URL.Query().Get("weight")
	// Omitting ?weight entirely means atomic activation (weight=100) — the
	// legacy default. Parsing only overrides the value when the query
	// string is non-empty, so weight=100 also covers the `?weight=100`
	// explicit case (which is the same operation, not a canary).
	weight := 100
	if weightStr != "" {
		parsed, err := strconv.Atoi(weightStr)
		if err != nil || parsed < 0 || parsed > 100 {
			httperror.BadRequestCtx(w, r, "weight must be an integer between 0 and 100")
			return
		}
		weight = parsed
	}

	// weight == 100 (explicit or omitted): atomic activation. Goes through
	// deploymentSvc.ActivateDeployment so active_deployments is updated and
	// rollback / auto-rollback stability evaluation target the right row.
	// Treats ?weight=100 as identical to omitting ?weight= entirely (the
	// canary path is for partial weights only).
	if weight == 100 {
		if err := h.activateSvc.ActivateDeployment(r.Context(), tenantID, appName, deploymentID); err != nil {
			if errors.Is(err, service.ErrPublishFailed) {
				writePublishFailureEnvelope(w, r, err,
					"activation committed but worker notification failed; please retry")
				return
			}
			log.Printf("internal error: %v", err)
			httperror.InternalErrorCtx(w, r)
			return
		}

		// Fire-and-forget precompilation in the background so the
		// activation response is not blocked by compilation time.
		if h.wasm2cwasmPath != "" && h.artifactStore != nil {
			ctx := context.WithoutCancel(r.Context())
			go service.PrecompileCwasm(ctx, h.artifactStore, h.wasm2cwasmPath, tenantID, appName, deploymentID)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{"status": "activated"}); err != nil {
			log.Printf("Activate: failed to encode response: %v", err)
		}
		auditRecord(r, "activate", "deployment", deploymentID, "deployment "+deploymentID+" activated for app "+appName, "success")
		return
	}

	// Partial weight: canary activation. Requires an existing active
	// deployment to act as the remainder — a canary staged against
	// nothing is the same as a plain activation, which is what
	// ActivateDeployment above already does. Reject with 400 rather than
	// silently producing a single-entry split whose sum != 100 (which
	// would 500 at ValidateSum).
	current, err := h.deploymentSvc.GetActiveDeployment(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("internal error: GetActiveDeployment: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if current == nil {
		httperror.BadRequestCtx(w, r, "canary activation requires an existing active deployment; activate one first")
		return
	}
	if current.ID == deploymentID {
		httperror.BadRequestCtx(w, r, "deployment is already active; pick a different deployment for the canary")
		return
	}
	splits := []domain.TrafficSplitEntry{
		{DeploymentID: deploymentID, Weight: weight},
		{DeploymentID: current.ID, Weight: 100 - weight},
	}

	if err := h.trafficSvc.SetTraffic(r.Context(), tenantID, appName, splits); err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "traffic_set"}); err != nil {
		log.Printf("Canary activate: failed to encode response: %v", err)
	}
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
	if !validateAppName(w, appName) {
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
			writePublishFailureEnvelope(w, r, err,
				"rollback committed but worker notification failed; please retry")
			return
		}
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "rollback failed"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"deployment_id": newID}); err != nil {
		log.Printf("Rollback: failed to encode response: %v", err)
	}
	auditRecord(r, "rollback", "deployment", newID, "app "+appName+" rolled back to deployment "+newID, "success")
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
	if err := json.NewEncoder(w).Encode(deployment); err != nil {
		log.Printf("GetActive: failed to encode response: %v", err)
	}
}

// Promote handles POST /api/v1/apps/{appName}/promote/{deploymentID} —
// activates a deployment under a different app name than it was originally
// deployed under (preview → production workflow).
func (h *DeploymentHandler) Promote(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	targetAppName := r.PathValue("appName")
	deploymentID := r.PathValue("deploymentID")

	if !validateAppName(w, targetAppName) {
		return
	}
	if !validateDeploymentID(w, deploymentID) {
		return
	}

	if err := h.promoteSvc.PromoteDeployment(r.Context(), tenantID, targetAppName, deploymentID); err != nil {
		if errors.Is(err, service.ErrDeploymentNotFound) {
			httperror.NotFoundCtx(w, r, "deployment not found")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "promoted"}); err != nil {
		log.Printf("Promote: failed to encode response: %v", err)
	}
	auditRecord(r, "promote", "deployment", deploymentID, "deployment "+deploymentID+" promoted to app "+targetAppName, "success")
}

// validateAppName writes a 400 with {"error": "invalid app name"} and
// returns false if appName is empty or contains path-traversal
// characters. Callers should `return` immediately when this is false.
func validateAppName(w http.ResponseWriter, appName string) bool {
	if appName == "" || containsPathTraversal(appName) {
		http.Error(w, `{"error": "invalid app name"}`, http.StatusBadRequest)
		return false
	}
	return true
}

// validateDeploymentID writes a 400 with {"error": "invalid deployment
// id"} and returns false if deploymentID is empty or contains
// path-traversal characters. Callers should `return` when this is false.
// The deployment id flows into the /registry/{tenant}/{app}/{deployment}
// .wasm path on the worker (see Download handler) — a ".." or "/" in the
// id lets a caller reference arbitrary files on the worker's filesystem.
// Reject 400 here rather than 500 from the storage layer.
func validateDeploymentID(w http.ResponseWriter, deploymentID string) bool {
	if deploymentID == "" || containsPathTraversal(deploymentID) {
		http.Error(w, `{"error": "invalid deployment id"}`, http.StatusBadRequest)
		return false
	}
	return true
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
	if !validateAppName(w, appName) {
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
		if err := json.NewEncoder(w).Encode(map[string]interface{}{
			"ready":    false,
			"app_name": appName,
			"reason":   "no running app found for this tenant",
		}); err != nil {
			log.Printf("AppIngress ready false: failed to encode response: %v", err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"ready":       true,
		"app_name":    target.AppName,
		"tenant_id":   target.TenantID,
		"worker_id":   target.WorkerID,
		"region":      target.Region,
		"worker_addr": target.WorkerAddr,
		"port":        target.Port,
	}); err != nil {
		log.Printf("AppIngress ready true: failed to encode response: %v", err)
	}
}

// extractDeployParts walks the multipart body looking for the required
// `file` part and the optional `build_metadata` part. Returns:
//   - the file `*multipart.Part` (already positioned at the first byte
//     of the wasm artifact), so the caller can stream it directly into
//     the service's io.Reader;
//   - the build_metadata bytes (possibly nil if the part was absent or
//     malformed — non-fatal; the service will use a default envelope);
//   - a non-nil error only when the request is structurally broken
//     (missing file part, malformed MIME headers, etc.). A missing
//     `build_metadata` is NOT an error.
//
// Parts other than `file` and `build_metadata` are silently drained
// and discarded, so the request body's bytes don't leak through to
// the file part stream.
func extractDeployParts(mr *multipart.Reader) (*multipart.Part, []byte, error) {
	var (
		filePart       *multipart.Part
		buildMetaBytes []byte
	)
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read multipart: %w", err)
		}
		switch p.FormName() {
		case "file":
			if filePart != nil {
				_ = p.Close()
				return nil, nil, fmt.Errorf("multiple 'file' parts")
			}
			filePart = p
		case "build_metadata":
			if buildMetaBytes != nil {
				_ = p.Close()
				return nil, nil, fmt.Errorf("multiple 'build_metadata' parts")
			}
			// Cap the metadata payload at 64 KiB. A real entry is
			// ~300 bytes; 64 KiB is generous headroom for forward
			// compat without permitting a multi-MiB JSON DoS.
			const maxBuildMeta = 64 * 1024
			lr := io.LimitReader(p, maxBuildMeta+1)
			meta, readErr := io.ReadAll(lr)
			_ = p.Close()
			if readErr != nil {
				// Non-fatal: PR2 envelope builder drops unknown
				// metadata rather than rejecting the deploy.
				meta = nil
			}
			if int64(len(meta)) > maxBuildMeta {
				// Oversize metadata is non-fatal too — drop and
				// let the envelope be built with "unknown"
				// toolchain fields. Errs on the side of
				// availability per the deploy path's SLA.
				meta = nil
			}
			buildMetaBytes = meta
		default:
			// Silently drain and discard unknown parts so the
			// `file` part's bytes remain contiguous.
			_, _ = io.Copy(io.Discard, p)
			_ = p.Close()
		}
	}
	if filePart == nil {
		return nil, nil, fmt.Errorf("missing 'file' part")
	}
	return filePart, buildMetaBytes, nil
}
