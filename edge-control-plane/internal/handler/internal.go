package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
)

// InternalDomainServiceInterface is the slice of *service.DomainService
// that InternalHandler needs (issue #83). Distinct from the
// tenant-facing `DomainServiceInterface` in `domain.go`; the two
// interfaces have no methods in common. Defined as an interface so
// handler tests can inject a mock without spinning up a real DB
// (matching the pattern used by DeploymentServiceInterface /
// WorkerServiceInterface in other handler files).
type InternalDomainServiceInterface interface {
	ListAllDomains(ctx context.Context) ([]domain.Domain, error)
	IsTlsAllowed(ctx context.Context, fqdn string) (bool, error)
	UpdateStatus(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error
}

// Compile-time check that the service still satisfies the interface.
// The error mapping for `service.ErrDomainNotFound → 404` in
// `UpdateDomainStatus` depends on this contract; if a future refactor
// changes the signature, the handler will fail to compile and the
// 404 path will not silently regress to 204.
var _ InternalDomainServiceInterface = (*service.DomainService)(nil)

// InternalHandler handles internal worker-facing endpoints.
//
// `deploymentSvc` is held as the narrow autoRollbacker interface so
// tests can stub AutoRollback without standing up the full
// *service.DeploymentService (DB + NATS + publisher + artifact store).
// Download uses two methods of the same underlying service
// (GetDeployment, GetArtifact); these also live on the interface so
// the production code path is unchanged. The concrete
// *service.DeploymentService satisfies it, so the existing caller in
// cmd/api/main.go compiles unchanged.
//
// `domainSvc` is nil when the binary is built without custom-domain
// support (default-only mode) — every method that needs it returns
// 501. `logEntryRepo` is the worker log ingest path (issue #76);
// required by AutoRollback's audit trail and any future log-bearing
// endpoint.
//
// `reconcileSvc` is the on-demand full_sync publisher used by
// RegisterWorker (issue #53). When nil, registration does not trigger
// a sync — the periodic timer in cmd/api/main.go will catch up
// within RECONCILE_INTERVAL. Set via NewInternalHandler. Tests pass
// nil so they don't have to wire a publisher.
//
// `bootstrapSecret` is the HMAC secret for the bootstrap handshake
// (issue #104). When empty, the bootstrap endpoint returns 501
// (not configured). Set via `BOOTSTRAP_SECRET` env var.
type InternalHandler struct {
	deploymentSvc  autoRollbacker
	workerSvc      workerRegisterer
	domainSvc      InternalDomainServiceInterface
	logEntryRepo   logEntryRepo
	reconcileSvc   syncRequester
	syncBuilder    syncPayloadBuilder
	cpRegion       string
	bootstrapSecret string
	// jwtSecret is the JWT_SECRET the bootstrap handshake delivers to workers.
	jwtSecret string
}

// autoRollbacker is the narrow contract InternalHandler's endpoints
// need. Combines AutoRollback's RollbackDeployment with Download's
// GetDeployment + GetArtifact so the production code path doesn't need
// to switch field accessors. Mirrors the pattern in DeploymentHandler
// (deploymentRollbacker in deployment.go).
type autoRollbacker interface {
	RollbackDeployment(ctx context.Context, tenantID, appName string) (string, error)
	GetDeployment(ctx context.Context, tenantID, deploymentID string) (*domain.Deployment, error)
	GetArtifact(ctx context.Context, tenantID, appName, deploymentID string, format string) (io.ReadCloser, error)
}

// workerRegisterer is the narrow contract the RegisterWorker endpoint
// needs. Holding the concrete *service.WorkerService made it
// impossible to test the success path without standing up the full
// worker service (DB + NATS conn + metrics aggregator). Production
// caller (cmd/api/main.go) still passes *service.WorkerService, which
// satisfies the interface. ListWorkers isn't covered here because no
// other endpoint currently exercises the worker service — if a future
// endpoint needs ListByTenant, add it to this interface.
type workerRegisterer interface {
	Register(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) error
	ListByTenant(ctx context.Context, tenantID string) ([]domain.Worker, error)
	// Get resolves a worker_id (from the URL path of /api/internal/workers/{workerID}/sync)
	// to a *domain.Worker so the handler can scope the sync payload to
	// that worker's (tenant, region). Issue #53.
	Get(ctx context.Context, workerID string) (*domain.Worker, error)
}

// syncRequester is the narrow contract RegisterWorker uses to trigger
// an on-register full_sync (issue #53). Defining it as an interface
// (instead of taking *service.ReconcileService directly) keeps handler
// tests mockable without standing up the full ReconcileService — the
// only production caller is `*service.ReconcileService`, set in
// cmd/api/main.go.
//
// nil means "no on-register sync" — tests pass nil, the periodic
// timer in cmd/api/main.go is the durable safety net.
type syncRequester interface {
	RequestSync(ctx context.Context, tenantID, region string)
}

// syncPayloadBuilder is the read-only counterpart: it computes the
// per-region AppConfig map without publishing, so the HTTP /sync
// fallback endpoint (issue #53) can return the same payload the
// periodic loop would publish. *service.ReconcileService satisfies it.
type syncPayloadBuilder interface {
	BuildFullSync(ctx context.Context, tenantID, region string) (map[string]nats.AppConfig, error)
}

func NewInternalHandler(
	deploymentSvc autoRollbacker,
	workerSvc workerRegisterer,
	domainSvc InternalDomainServiceInterface,
	logEntryRepo logEntryRepo,
	reconcileSvc syncRequester,
	syncBuilder syncPayloadBuilder,
	cpRegion string,
	bootstrapSecret string,
	jwtSecret string,
) *InternalHandler {
	return &InternalHandler{
		deploymentSvc: deploymentSvc,
		workerSvc:     workerSvc,
		domainSvc:     domainSvc,
		logEntryRepo:  logEntryRepo,
		reconcileSvc:  reconcileSvc,
		syncBuilder:   syncBuilder,
		cpRegion:      cpRegion,
		bootstrapSecret: bootstrapSecret,
		jwtSecret:       jwtSecret,
	}
}

// Download serves Wasm artifacts to authenticated workers.
// Requires a valid worker JWT via Authorization: Bearer <token> header or ?jwt= query param.
func (h *InternalHandler) Download(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetWorkerTenantID(r.Context())
	deploymentID := r.PathValue("deploymentID")

	lookupTenant := tenantID
	if middleware.IsSharedWorker(r.Context()) {
		lookupTenant = "*"
	}
	deployment, err := h.deploymentSvc.GetDeployment(r.Context(), lookupTenant, deploymentID)
	if err != nil || deployment == nil {
		httperror.NotFoundCtx(w, r, "not found")
		return
	}

	format := r.URL.Query().Get("format")
	artifact, err := h.deploymentSvc.GetArtifact(r.Context(), deployment.TenantID, deployment.AppName, deployment.ID, format)
	if err != nil {
		if errors.Is(err, storage.ErrArtifactTooLarge) {
			httperror.PayloadTooLargeCtx(w, r, "artifact exceeds maximum size")
			return
		}
		httperror.NotFoundCtx(w, r, "artifact not found")
		return
	}
	defer func() {
		if err := artifact.Close(); err != nil {
			log.Printf("Download: failed to close Wasm artifact: %v", err)
		}
	}()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, artifact); err != nil {
		// client disconnected, nothing we can do
		return
	}
}

// RegisterWorker handles POST /api/internal/workers — worker registration.
func (h *InternalHandler) RegisterWorker(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetWorkerTenantID(r.Context())
	var req domain.RegisterWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	// Validate required fields.
	if req.WorkerID == "" || req.Region == "" {
		httperror.BadRequestCtx(w, r, "worker_id and region are required")
		return
	}
	if err := h.workerSvc.Register(r.Context(), tenantID, &req); err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidWorkerID):
			httperror.BadRequestCtx(w, r, "invalid worker ID")
		case errors.Is(err, service.ErrRegionMismatch):
			httperror.BadRequestCtx(w, r, "region mismatch")
		case errors.Is(err, service.ErrQuotaExceeded):
			httperror.QuotaExceededCtx(w, r, "quota exceeded")
		default:
			log.Printf("internal error: %v", err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}
	w.WriteHeader(http.StatusCreated)

	// Return the CP's own region so the worker can validate it matches
	// its local REGION at startup (issue #254). A mismatch would cause
	// the worker to subscribe to the wrong NATS subject, silently never
	// receiving any task messages.
	resp := map[string]string{
		"cp_region": h.cpRegion,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)

	// Fire-and-forget full_sync so the worker comes up populated
	// immediately instead of waiting up to RECONCILE_INTERVAL (issue #53).
	// Best-effort: the periodic timer is the durable safety net, so a
	// publish failure here is logged and dropped — the worker's next
	// reconcile sweep (or the on-register fallback in cmd/api/main.go's
	// process restart) catches up.
	//
	// We capture (tenantID, region) and use a fresh background ctx with
	// a short deadline because r.Context() is cancelled the moment
	// WriteHeader returns, and we don't want the publish to inherit
	// that cancellation. NATS publishes have their own internal
	// timeout; the 5s cap here just bounds the goroutine lifetime.
	if h.reconcileSvc != nil {
		go func(t, region string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			h.reconcileSvc.RequestSync(ctx, t, region)
		}(tenantID, req.Region)
	}
}

// ListWorkers handles GET /api/internal/workers — list workers for the authenticated tenant.
func (h *InternalHandler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetWorkerTenantID(r.Context())
	workers, err := h.workerSvc.ListByTenant(r.Context(), tenantID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	resp := map[string]interface{}{"workers": workers}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("ListWorkers: failed to encode response: %v", err)
	}
}

// AutoRollbackRequest is the JSON body posted by an edge-worker when
// its supervisor exhausts the restart cap on a tenant app. The
// tenant_id is read from the (currently unauthenticated — see
// cmd/api/main.go:159) worker context, but the request still
// includes it for cross-checking against future worker JWTs.
//
// `restart_count` is informational only — the server doesn't gate
// on a threshold, but having it in the request body makes the
// audit log useful when correlating crashes with the auto-rollback
// trigger.
type AutoRollbackRequest struct {
	TenantID            string `json:"tenant_id"`
	AppName             string `json:"app_name"`
	CurrentDeploymentID string `json:"current_deployment_id"`
	RestartCount        uint32 `json:"restart_count"`
}

// AutoRollback handles POST /api/internal/apps/{appName}/auto-rollback.
// Triggered by an edge-worker when an app reaches
// AppInstanceStatus::Crashed (restart_count >= max_restarts) AND the
// tenant opted in via `edge deploy --auto-rollback` (the
// auto_rollback_enabled column on active_deployments).
//
// Behavior:
//   - 200: rolled back to last_good_deployment_id; body is
//     {"deployment_id": "<new active id>", "prior_deployment_id":
//     "<rolled-away-from id>"}. Workers may still be serving the
//     prior deployment until the published TaskMessage is delivered;
//     the response is best-effort guidance, not a guarantee.
//   - 404: no active deployment for this app.
//   - 409: no last-good pointer (only ever activated one deployment).
//   - 412: auto-rollback is disabled for this app. Tells the worker
//     "we got your signal but the tenant didn't opt in" — distinct
//     from 403 (auth) so the worker can distinguish a config issue
//     from a permission issue.
//   - 502: rollback committed but the post-commit NATS publish
//     failed.
//   - 500: anything else.
//
// Like every other /api/internal/* endpoint, this is currently
// unauthenticated — see the comment in cmd/api/main.go. Minting
// worker JWTs at startup is a tracked follow-up issue.
func (h *InternalHandler) AutoRollback(w http.ResponseWriter, r *http.Request) {
	appName := r.PathValue("appName")

	var req AutoRollbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.TenantID == "" || req.AppName == "" {
		httperror.BadRequestCtx(w, r, "tenant_id and app_name are required")
		return
	}
	// Path-vs-body consistency check. The handler resolves appName
	// from the URL (so curl -X POST /api/internal/apps/foo/auto-rollback
	// with body app_name="bar" doesn't accidentally hit the wrong
	// app), and falls back to the body if the URL doesn't carry an
	// app name (e.g. internal callers that POST to a non-app-scoped
	// endpoint).
	if appName == "" {
		appName = req.AppName
	}
	if appName != req.AppName {
		httperror.BadRequestCtx(w, r, "app_name in URL and body must match")
		return
	}

	jwtTenant := middleware.GetWorkerTenantID(r.Context())
	if jwtTenant != "" && jwtTenant != "*" && jwtTenant != req.TenantID {
		httperror.ForbiddenCtx(w, r, "tenant mismatch")
		return
	}

	// Use the existing RollbackDeployment path. The repo's
	// ResetStableSinceForRollback enforces auto_rollback_enabled
	// in SQL — we surface the resulting sentinel errors as the
	// status codes documented above. The ErrAutoRollbackDisabled
	// sentinel lives in package repository (it's a string-matched
	// sentinel to avoid an import cycle with service); the handler
	// matches via errors.Is using a re-exported alias below.
	newID, err := h.deploymentSvc.RollbackDeployment(r.Context(), req.TenantID, appName)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrNoLastGood):
			http.Error(w, `{"error": "no previous deployment to roll back to"}`, http.StatusConflict)
		case errors.Is(err, service.ErrNoActiveDeployment):
			http.Error(w, `{"error": "no active deployment"}`, http.StatusNotFound)
		case errors.Is(err, service.ErrAutoRollbackDisabled):
			http.Error(w, `{"error": "auto-rollback disabled for this app"}`, http.StatusPreconditionFailed)
		case errors.Is(err, service.ErrPublishFailed):
			writePublishFailureEnvelope(w, r, err,
				"rollback committed but worker notification failed; please retry")
		default:
			log.Printf("internal error: %v", err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"deployment_id": newID,
	}); err != nil {
		log.Printf("AutoRollback: failed to encode response: %v", err)
	}
}

// Sync handles GET /api/internal/workers/{workerID}/sync — HTTP
// fallback for when a worker hasn't received any NATS message on
// edgecloud.tasks.<region> for > N seconds (the worker's own
// watchdog decides when to call; this endpoint just answers).
//
// Worker-authenticated: any valid worker JWT works, BUT the URL-path
// workerID must belong to the JWT's tenant — otherwise a worker
// registered for tenant A could enumerate workerIDs (they follow the
// documented `w_<region>_<n>` prefix, also visible in heartbeats)
// and read tenant B's full app set (deployment IDs, hashes, env
// vars, allowlists). tenant_id is derived from the JWT claim, never
// from the URL.
//
// Returns the same TaskMessage payload the periodic full_sync
// publish would emit, so the worker's existing handle_task_message
// can apply it without any new logic. The wire type is "full_sync"
// so the worker can log/metric it distinctly from event-driven
// task_update messages.
//
// 501 when the control plane was built without the sync builder
// wired in (tests, or an operator who explicitly disabled the
// fallback). 404 when the workerID isn't registered OR doesn't
// belong to the authenticated tenant (the same response in both
// cases prevents workerID enumeration via differential responses).
// 500 on a DB error from syncBuilder.BuildFullSync.
//
// Cross-tenant authorization: a worker authenticated as tenant A cannot
// read tenant B's app set via /sync. The URL-path workerID must equal
// the JWT's worker_id claim — both "no worker_id claim" and "URL
// mismatch" return 404 with the same body so an attacker can't
// enumerate workerIDs belonging to other tenants. The JWT's worker_id
// is the caller's own (signed by the CP at registration); a worker
// cannot request /sync for any worker other than itself.
func (h *InternalHandler) Sync(w http.ResponseWriter, r *http.Request) {
	if h.syncBuilder == nil {
		http.Error(w, "sync fallback not enabled", http.StatusNotImplemented)
		return
	}
	workerID := r.PathValue("workerID")
	if workerID == "" {
		http.Error(w, "worker_id path parameter required", http.StatusBadRequest)
		return
	}

	jwtWorkerID := middleware.GetWorkerID(r.Context())
	if jwtWorkerID == "" || jwtWorkerID != workerID {
		// Same response shape as the (no-longer-existing) "worker
		// not found" branch — a malformed/missing worker_id claim
		// is indistinguishable from a URL mismatch from the
		// attacker's vantage point.
		log.Printf("Sync(%s): jwt worker_id=%q does not match URL; denying",
			workerID, jwtWorkerID)
		http.Error(w, `{"error": "worker not found"}`, http.StatusNotFound)
		return
	}

	jwtTenantID := middleware.GetWorkerTenantID(r.Context())
	jwtRegion := middleware.GetWorkerRegion(r.Context())
	// Guard every JWT-derived identity claim symmetrically. A JWT
	// that carries a valid worker_id but no tenant_id/region should
	// not reach BuildFullSync — the DB query keyed on "" would
	// either return an empty payload (silently under-syncing) or
	// surface a 500 from the repo layer (information-leak about
	// the table shape). Returning 404 keeps the failure mode
	// identical to a URL mismatch and prevents either outcome.
	if jwtTenantID == "" || jwtRegion == "" {
		log.Printf("Sync(%s): jwt missing tenant_id=%q or region=%q; denying",
			workerID, jwtTenantID, jwtRegion)
		http.Error(w, `{"error": "worker not found"}`, http.StatusNotFound)
		return
	}

	apps, err := h.syncBuilder.BuildFullSync(r.Context(), jwtTenantID, jwtRegion)
	if err != nil {
		log.Printf("Sync(%s): build: %v", workerID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if apps == nil {
		// BuildFullSync is documented to return an empty map, not nil,
		// when there's nothing to publish. Defensively normalize so the
		// worker's JSON deserializer never sees `"apps": null`.
		apps = map[string]nats.AppConfig{}
	}

	payload := map[string]interface{}{
		"type":      "full_sync",
		"timestamp": time.Now().UTC(),
		"tenant_id": jwtTenantID,
		"apps":      apps,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("Sync(%s): encode: %v", workerID, err)
	}
}

// ListDomains handles GET /api/internal/domains — full domain list
// for the ingress poller. JWT-authenticated; the ingress uses a
// `role: "ingest"` token, but we accept any valid worker JWT (the
// data is non-tenant-scoped; only the ingress + admins ever call
// this). Response is a flat JSON array — not paginated, because
// domain counts per platform are small.
//
// 501 when the binary is built without custom-domain support.
func (h *InternalHandler) ListDomains(w http.ResponseWriter, r *http.Request) {
	if h.domainSvc == nil {
		http.Error(w, "custom domains not enabled", http.StatusNotImplemented)
		return
	}
	domains, err := h.domainSvc.ListAllDomains(r.Context())
	if err != nil {
		log.Printf("ListDomains: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(domains); err != nil {
		log.Printf("ListDomains: failed to encode response: %v", err)
	}
}

// TlsAllowed handles GET /api/internal/tls-allowed?fqdn=X — Caddy's
// `on_demand.ask` callback. Returns 200 if the FQDN is registered
// (any status — pending still authorizes the challenge), 404
// otherwise. Body is empty in both cases; Caddy only checks the
// status code.
//
// 501 when the binary is built without custom-domain support.
func (h *InternalHandler) TlsAllowed(w http.ResponseWriter, r *http.Request) {
	if h.domainSvc == nil {
		http.Error(w, "custom domains not enabled", http.StatusNotImplemented)
		return
	}
	fqdn := r.URL.Query().Get("fqdn")
	if fqdn == "" {
		http.Error(w, "fqdn query parameter required", http.StatusBadRequest)
		return
	}
	allowed, err := h.domainSvc.IsTlsAllowed(r.Context(), fqdn)
	if err != nil {
		log.Printf("TlsAllowed(%s): %v", fqdn, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !allowed {
		http.Error(w, "fqdn not registered", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// UpdateDomainStatus handles POST /api/internal/domains/{id}/status —
// server-driven status updates. v1 stub; v2 wires this to a Caddy
// event hook so the platform learns about renewal failures.
//
// JWT-authenticated; the body is `{"status": "active"|"failed",
// "last_error": "..."}`. The 404 path is critical for the v2 webhook:
// a Caddy event for a deleted/stale domain id must NOT be silently
// acknowledged, or the operator's "rows in failed state" alerts
// become wrong.
func (h *InternalHandler) UpdateDomainStatus(w http.ResponseWriter, r *http.Request) {
	if h.domainSvc == nil {
		http.Error(w, "custom domains not enabled", http.StatusNotImplemented)
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id path parameter required", http.StatusBadRequest)
		return
	}
	var body struct {
		Status    domain.DomainStatus `json:"status"`
		LastError *string             `json:"last_error"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if body.Status != domain.DomainStatusActive && body.Status != domain.DomainStatusFailed {
		http.Error(w, "status must be 'active' or 'failed'", http.StatusBadRequest)
		return
	}
	if err := h.domainSvc.UpdateStatus(r.Context(), id, body.Status, body.LastError); err != nil {
		if errors.Is(err, service.ErrDomainNotFound) {
			http.Error(w, `{"error": "domain not found"}`, http.StatusNotFound)
			return
		}
		log.Printf("UpdateDomainStatus(%s): %v", id, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// WorkerBootstrapRequest is the JSON body for the bootstrap handshake.
type WorkerBootstrapRequest struct {
	WorkerID  string `json:"worker_id"`
	Region    string `json:"region"`
	TenantID  string `json:"tenant_id"`
	Timestamp string `json:"timestamp"`  // RFC3339, for replay protection
	Nonce     string `json:"nonce"`      // random value for replay protection
	Signature string `json:"signature"`  // HMAC-SHA256 of worker_id+":"+region+":"+tenant_id+":"+timestamp+":"+nonce
}

// Bootstrap handles POST /api/internal/bootstrap — the first phase of
// the bootstrap handshake (issue #104). The worker sends a request
// signed with the shared BOOTSTRAP_SECRET and receives a short-lived
// JWT (5 minutes) that it can use to fetch the real JWT_SECRET.
//
// Returns 501 when BOOTSTRAP_SECRET is not configured on the CP.
// Returns 401 on invalid signature.
// Returns 400 on malformed request.
// Returns 200 with {"token": "<bootstrap_jwt>"} on success.
func (h *InternalHandler) Bootstrap(w http.ResponseWriter, r *http.Request) {
	if h.bootstrapSecret == "" {
		http.Error(w, `{"error": "bootstrap not configured"}`, http.StatusNotImplemented)
		return
	}

	var req WorkerBootstrapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.WorkerID == "" || req.Region == "" || req.TenantID == "" {
		httperror.BadRequestCtx(w, r, "worker_id, region, and tenant_id are required")
		return
	}
	if req.Timestamp == "" || req.Nonce == "" || req.Signature == "" {
		httperror.BadRequestCtx(w, r, "timestamp, nonce, and signature are required")
		return
	}

	// Verify timestamp is within 5 minutes (replay protection).
	ts, err := time.Parse(time.RFC3339, req.Timestamp)
	if err != nil {
		httperror.BadRequestCtx(w, r, "invalid timestamp format (use RFC3339)")
		return
	}
	if time.Since(ts) > 5*time.Minute || time.Since(ts) < -1*time.Minute {
		httperror.BadRequestCtx(w, r, "timestamp is too old or in the future")
		return
	}

	// Verify HMAC-SHA256 signature.
	payload := fmt.Sprintf("%s:%s:%s:%s:%s",
		req.WorkerID, req.Region, req.TenantID, req.Timestamp, req.Nonce)
	mac := hmac.New(sha256.New, []byte(h.bootstrapSecret))
	mac.Write([]byte(payload))
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(req.Signature), []byte(expectedSig)) {
		log.Printf("bootstrap: invalid signature for worker %s (tenant=%s, region=%s)",
			req.WorkerID, req.TenantID, req.Region)
		httperror.UnauthorizedCtx(w, r, "invalid signature")
		return
	}

	// Issue short-lived bootstrap JWT.
	cfg := middleware.BootstrapJWTConfig{
		BootstrapSecret: h.bootstrapSecret,
		Issuer:          "edgecloud-bootstrap",
	}
	token, err := middleware.IssueBootstrapJWT(cfg, req.WorkerID, req.TenantID, req.Region)
	if err != nil {
		log.Printf("bootstrap: failed to issue JWT for worker %s: %v", req.WorkerID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	// Audit log the bootstrap.
	auditRecord(r, "bootstrap", "worker", req.WorkerID,
		fmt.Sprintf("worker %s (tenant=%s, region=%s) bootstrap", req.WorkerID, req.TenantID, req.Region),
		"success")

	log.Printf("bootstrap: worker %s (tenant=%s, region=%s) authenticated via bootstrap secret",
		req.WorkerID, req.TenantID, req.Region)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"token": token,
	})
}

// WorkerSecret handles GET /api/internal/worker-secret — the second
// phase of the bootstrap handshake (issue #104). The worker presents
// its short-lived bootstrap JWT (obtained from POST /api/internal/bootstrap)
// and receives the real JWT_SECRET in return.
//
// Protected by the BootstrapAuth middleware (separate from WorkerAuth).
// Returns 200 with {"secret": "<jwt_secret>"} on success.
func (h *InternalHandler) WorkerSecret(w http.ResponseWriter, r *http.Request) {
	if h.bootstrapSecret == "" || h.jwtSecret == "" {
		http.Error(w, `{"error": "bootstrap not configured"}`, http.StatusNotImplemented)
		return
	}

	workerID := middleware.GetWorkerID(r.Context())
	tenantID := middleware.GetWorkerTenantID(r.Context())

	// Audit log the secret fetch.
	auditRecord(r, "secret_fetch", "worker", workerID,
		fmt.Sprintf("worker %s (tenant=%s) fetched JWT secret", workerID, tenantID),
		"success")

	log.Printf("worker-secret: worker %s (tenant=%s) fetched JWT secret", workerID, tenantID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"secret": h.jwtSecret,
	})
}
