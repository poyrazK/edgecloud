package handler

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/golang-jwt/jwt/v5"
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
//
// The trailing mintWorkerToken fields (workerJWTConfig / workerTokenTTL /
// issuer / activeKID / tenantSvc) are issue #491 — they power
// POST /api/internal/tokens/tenant, the per-tenant JWT mint endpoint.
// `workerJWTConfig` is the resolver for signing keys (keyring-aware —
// issue #307 follow-up). `tenantSvc` is held as a narrow interface
// so handler tests can swap in a mock without dragging in the full
// *service.TenantService (DB + tenant repo + quota repo + api key repo).
//
// `workerKeyRepo` and `enrollmentChallenges` are issue #430: the
// /worker-bootstrap/enroll handler persists the worker's Ed25519
// public key via workerKeyRepo.SetPublicKey, and the bootstrap phase-1
// handler stashes a one-shot challenge in enrollmentChallenges that
// phase-2 must echo back (proving possession of the matching private
// key). Both are required — the challenge store is what closes the
// "stolen bootstrap JWT" attack surface even before the Ed25519
// signature is checked.
type InternalHandler struct {
	deploymentSvc   autoRollbacker
	workerSvc       workerRegisterer
	domainSvc       InternalDomainServiceInterface
	logEntryRepo    logEntryRepo
	reconcileSvc    syncRequester
	syncBuilder     syncPayloadBuilder
	cpRegion        string
	bootstrapSecret string
	// jwtSecret is the JWT_SECRET the bootstrap handshake delivers to workers.
	jwtSecret string
	// Issue #491 — per-tenant mint endpoint.
	workerJWTConfig middleware.WorkerJWTConfig
	workerTokenTTL  time.Duration
	issuer          string
	activeKID       string
	tenantSvc       tenantGetter
	// workerHostingSvc answers "which tenants is this worker hosting?"
	// — the issue #491 constraint #2 gate. See hostingGetter.
	workerHostingSvc hostingGetter
	// Issue #430 — per-worker key enrollment (HKDF-derived HS256
	// secrets; replaces the cluster-wide /worker-secret leak).
	workerKeyRepo        workerKeyRepo
	enrollmentChallenges *enrollmentChallengeStore
}

// autoRollbacker is the narrow contract InternalHandler's endpoints
// need. Combines AutoRollback's RollbackDeployment with Download's
// GetDeployment + GetArtifact so the production code path doesn't need
// to switch field accessors. Mirrors the pattern in DeploymentHandler
// (deploymentRollbacker in deployment.go).
//
// Issue #439: trailing idempotencyKey (always "" for internal callers
// today — AutoRollback is worker-driven, never carries an
// Idempotency-Key header; the parameter exists only so the contract
// matches the one in deployment.go).
type autoRollbacker interface {
	RollbackDeployment(ctx context.Context, tenantID, appName, idempotencyKey string) (string, error)
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

// tenantGetter is the narrow contract the per-tenant worker-token mint
// endpoint needs (issue #491). The handler only needs to know whether
// a tenant exists and whether it is disabled — listing / pagination /
// quota math are out of scope here. Holding the concrete
// *service.TenantService would force every handler test to construct
// a DB, tenant repo, quota repo, and api-key repo just to assert that
// a "tenant not found" returns 404. *service.TenantService satisfies
// this (it has GetByID at service/tenant.go:129), so the production
// call site in app.go compiles unchanged.
type tenantGetter interface {
	GetByID(ctx context.Context, id string) (*domain.Tenant, error)
}

// hostingGetter is the narrow contract POST /api/internal/tokens/tenant
// uses to answer "which tenants is this worker currently hosting?"
// (issue #491 constraint #2). Returns a deduplicated slice of tenant
// IDs derived from worker_status.apps where status = 'running'.
// Satisfied by *service.WorkerService.
//
// Kept as a separate interface from tenantGetter so handler tests can
// substitute just the hosting logic without standing up the full
// *service.WorkerService (DB + NATS conn + metrics aggregator).
type hostingGetter interface {
	TenantsHostedBy(ctx context.Context, workerID string) ([]string, error)
}

// workerKeyRepo is the narrow contract /worker-bootstrap/enroll needs
// (issue #430). Satisfied by *repository.WorkerRepository. Kept as an
// interface so handler tests can inject a mock without a DB. Set/Get
// mirror repository/worker.go's SetPublicKey/GetPublicKey — see
// migration 032 for the column.
type workerKeyRepo interface {
	SetPublicKey(ctx context.Context, id, publicKeyHex string) (int64, error)
}

// EnrollmentChallenge is the server-side record for a single
// challenge issued during /api/internal/bootstrap phase 1 and
// consumed by /worker-bootstrap/enroll phase 2 (issue #430).
//
// `Challenge` is the random base64-encoded 32 bytes the worker
// must echo back in phase 2 (after signing it with the worker's
// Ed25519 private key). `PublicKey` is the worker's claimed pubkey
// (captured at phase 1 so phase 2 can re-verify the body matches
// the original request — closes the swap-attack where an attacker
// supplies their own keypair in phase 2). `ExpiresAt` matches the
// bootstrap JWT TTL (5 minutes) — see BootstrapClaims.ExpiresAt.
//
// Exported so handler_test.go (external test package) can drive
// the store directly without round-tripping through the HTTP
// handlers; the type has no sensitive fields (the challenge is a
// nonce, not a key).
type EnrollmentChallenge struct {
	Challenge string
	PublicKey string
	ExpiresAt time.Time
}

// enrollmentChallengeStore keeps phase-1 challenges in memory so
// phase-2 can verify the worker is the same caller that completed
// phase 1. In-memory + TTL is fine for the threat model: the
// challenges are single-use nonce-equivalents, and a CP restart
// simply forces every worker to re-bootstrap (the same recovery
// path as a stolen bootstrap JWT — the operator just waits 5
// minutes for the old challenge to expire on its own).
//
// `mu` guards the map; reads take a snapshot under the lock so
// phase-2 verification is consistent against concurrent phase-1
// writes. Cleanup happens lazily on every read — no background
// goroutine, no time.Ticker — because the store is bounded by
// (concurrent workers × 1 entry) and entries self-evict on
// next access.
type enrollmentChallengeStore struct {
	mu         sync.Mutex
	challenges map[string]EnrollmentChallenge // key: worker_id
}

func newEnrollmentChallengeStore() *enrollmentChallengeStore {
	return &enrollmentChallengeStore{challenges: make(map[string]EnrollmentChallenge)}
}

// NewEnrollmentChallengeStoreForTest returns a fresh store for
// use in handler_test.go. Production code never calls this — the
// only constructor in the prod path is newEnrollmentChallengeStore
// inside NewInternalHandler.
func NewEnrollmentChallengeStoreForTest() *enrollmentChallengeStore {
	return newEnrollmentChallengeStore()
}

// Put records a fresh challenge for workerID, replacing any prior
// entry (the worker re-bootstrapped and the old challenge is dead).
func (s *enrollmentChallengeStore) Put(workerID string, ch EnrollmentChallenge) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.challenges[workerID] = ch
}

// Pop atomically reads and removes the challenge for workerID,
// returning ok=false when no entry exists, when the entry has
// expired, or when the caller supplied a mismatching public_key.
// One-shot consumption is what closes the replay window — a
// second phase-2 attempt with the same challenge 401s.
func (s *enrollmentChallengeStore) Pop(workerID, publicKey string, now time.Time) (EnrollmentChallenge, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.challenges[workerID]
	if !ok {
		return EnrollmentChallenge{}, false
	}
	if !now.Before(ch.ExpiresAt) {
		delete(s.challenges, workerID)
		return EnrollmentChallenge{}, false
	}
	if ch.PublicKey != publicKey {
		// Body's public_key does not match phase-1's claim. Don't
		// consume the challenge — the legitimate worker can retry
		// with the correct body.
		return EnrollmentChallenge{}, false
	}
	delete(s.challenges, workerID)
	return ch, true
}

// GC sweeps expired entries. Called from phase-2 under the lock,
// so it amortizes to zero extra cost on the steady-state hot
// path (every Pop is its own tiny GC).
func (s *enrollmentChallengeStore) GC(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for k, ch := range s.challenges {
		if !now.Before(ch.ExpiresAt) {
			delete(s.challenges, k)
			removed++
		}
	}
	return removed
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
	workerJWTConfig middleware.WorkerJWTConfig,
	workerTokenTTL time.Duration,
	issuer string,
	activeKID string,
	tenantSvc tenantGetter,
	workerHostingSvc hostingGetter,
	workerKeyRepo workerKeyRepo,
) *InternalHandler {
	return &InternalHandler{
		deploymentSvc:        deploymentSvc,
		workerSvc:            workerSvc,
		domainSvc:            domainSvc,
		logEntryRepo:         logEntryRepo,
		reconcileSvc:         reconcileSvc,
		syncBuilder:          syncBuilder,
		cpRegion:             cpRegion,
		bootstrapSecret:      bootstrapSecret,
		jwtSecret:            jwtSecret,
		workerJWTConfig:      workerJWTConfig,
		workerTokenTTL:       workerTokenTTL,
		issuer:               issuer,
		activeKID:            activeKID,
		tenantSvc:            tenantSvc,
		workerHostingSvc:     workerHostingSvc,
		workerKeyRepo:        workerKeyRepo,
		enrollmentChallenges: newEnrollmentChallengeStore(),
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
	//
	// Note (issue #42): the previous 502 case for service.ErrPublishFailed
	// is removed — the post-commit publish is now durable via the outbox.
	newID, err := h.deploymentSvc.RollbackDeployment(r.Context(), req.TenantID, appName, "")
	if err != nil {
		switch {
		case errors.Is(err, service.ErrNoLastGood):
			http.Error(w, `{"error": "no previous deployment to roll back to"}`, http.StatusConflict)
		case errors.Is(err, service.ErrNoActiveDeployment):
			http.Error(w, `{"error": "no active deployment"}`, http.StatusNotFound)
		case errors.Is(err, service.ErrAutoRollbackDisabled):
			http.Error(w, `{"error": "auto-rollback disabled for this app"}`, http.StatusPreconditionFailed)
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
//
// PublicKey is the hex-encoded Ed25519 public key (64 lowercase ASCII
// chars) the worker is enrolling. Required as of issue #430 — every
// worker must present a keypair during bootstrap so the CP can derive
// a per-worker HS256 secret via HKDF. The HMAC-SHA256 payload now
// covers the public_key, so a swap-attack between phase 1 and phase 2
// is detectable at HMAC-verify time.
type WorkerBootstrapRequest struct {
	WorkerID  string `json:"worker_id"`
	Region    string `json:"region"`
	TenantID  string `json:"tenant_id"`
	Timestamp string `json:"timestamp"`  // RFC3339, for replay protection
	Nonce     string `json:"nonce"`      // random value for replay protection
	Signature string `json:"signature"`  // HMAC-SHA256 of worker_id:region:tenant_id:timestamp:nonce:public_key
	PublicKey string `json:"public_key"` // hex-encoded Ed25519 public key (64 ASCII chars)
}

// enrollmentChallengeLen is the size of the random challenge the CP
// generates during bootstrap phase 1 and the worker must sign during
// phase 2. 32 bytes matches the Ed25519 signature size, so the signed
// payload cannot be smaller than the signature — closes accidental
// truncation attacks on the signed string.
const enrollmentChallengeLen = 32

// Bootstrap handles POST /api/internal/bootstrap — the first phase of
// the bootstrap handshake (issue #104, hardened by issue #430).
// The worker sends a request signed with the shared BOOTSTRAP_SECRET
// and receives:
//
//  1. A short-lived JWT (5 minutes) — the bootstrap bearer for phase 2.
//  2. An enrollment_challenge — a random 32-byte nonce the worker
//     must sign with its Ed25519 private key in phase 2 to prove
//     possession of the matching public_key.
//
// The challenge is stored server-side (enrollmentChallengeStore) and
// is single-use: phase 2's Pop atomically removes it. A stolen
// bootstrap JWT alone is useless — without the challenge that the CP
// issued in this response, the attacker cannot complete enrollment.
//
// Returns 501 when BOOTSTRAP_SECRET is not configured on the CP.
// Returns 401 on invalid signature.
// Returns 400 on malformed request.
// Returns 200 with {"token", "enrollment_challenge", "challenge_expires_at"} on success.
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
	if req.PublicKey == "" {
		httperror.BadRequestCtx(w, r, "public_key is required (issue #430 per-worker enrollment)")
		return
	}
	// Validate the public_key shape now (before HMAC) so a malformed
	// claim produces a clean 400 rather than a 401 from HMAC failure.
	// We require 64 lowercase hex chars (32 bytes — the Ed25519
	// public-key size).
	if len(req.PublicKey) != 64 {
		httperror.BadRequestCtx(w, r, "public_key must be 64 lowercase hex chars (32-byte Ed25519 key)")
		return
	}
	if _, err := hex.DecodeString(req.PublicKey); err != nil {
		httperror.BadRequestCtx(w, r, "public_key must be hex-encoded")
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

	// Verify HMAC-SHA256 signature. Payload now covers public_key so a
	// phase-2 swap to a different keypair is detectable here — without
	// this coverage, an attacker who knows BOOTSTRAP_SECRET could mint
	// a bootstrap JWT for victim_worker_id, then enroll their own
	// keypair and walk away with a derived secret.
	payload := fmt.Sprintf("%s:%s:%s:%s:%s:%s",
		req.WorkerID, req.Region, req.TenantID, req.Timestamp, req.Nonce, req.PublicKey)
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

	// Generate the phase-2 challenge. 32 random bytes — encoded as
	// base64 (URL-safe, no padding) for inclusion in the JSON response
	// and the phase-2 request body. The challenge's expires_at matches
	// the bootstrap JWT TTL so phase-2 cannot outlive the JWT.
	challengeBytes := make([]byte, enrollmentChallengeLen)
	if _, err := rand.Read(challengeBytes); err != nil {
		log.Printf("bootstrap: rand.Read failed: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	challengeB64 := base64.RawURLEncoding.EncodeToString(challengeBytes)
	expiresAt := time.Now().Add(5 * time.Minute)

	h.enrollmentChallenges.Put(req.WorkerID, EnrollmentChallenge{
		Challenge: challengeB64,
		PublicKey: req.PublicKey,
		ExpiresAt: expiresAt,
	})

	// Audit log the bootstrap.
	auditRecord(r, "bootstrap", "worker", req.WorkerID,
		fmt.Sprintf("worker %s (tenant=%s, region=%s) bootstrap", req.WorkerID, req.TenantID, req.Region),
		"success")

	log.Printf("bootstrap: worker %s (tenant=%s, region=%s) authenticated via bootstrap secret",
		req.WorkerID, req.TenantID, req.Region)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"token":                token,
		"enrollment_challenge": challengeB64,
		"challenge_expires_at": expiresAt.Unix(),
	})
}

// EnrollmentRequest is the JSON body for POST /worker-bootstrap/enroll
// (issue #430, phase 2 of the bootstrap handshake).
//
// WorkerID MUST equal the bootstrap JWT's worker_id claim — verified
// in EnrollWorker. Mismatch is a 400.
//
// Challenge is the base64 challenge the CP returned in phase 1. The
// CP looks it up server-side and refuses the request if it has
// expired, been consumed, or was issued for a different public_key.
//
// PublicKey is the worker's hex-encoded Ed25519 public key. Must
// equal the public_key from phase 1 (the CP's challenge store
// re-checks this — see enrollmentChallengeStore.Pop).
//
// Signature is the Ed25519 signature over sha256(public_key || challenge),
// hex-encoded (128 ASCII chars / 64 bytes raw). Verification:
//
//	payload = sha256(public_key_bytes || challenge_bytes)
//	signature = ed25519.Sign(private_key, payload)
//
// The CP verifies the signature with the claimed public_key. A
// successful verification proves the caller knows the private key
// corresponding to the claimed public_key — which is exactly what
// issue #430 needs to bind the derived secret to a single worker.
type EnrollmentRequest struct {
	WorkerID  string `json:"worker_id"`
	PublicKey string `json:"public_key"`
	Challenge string `json:"enrollment_challenge"`
	Signature string `json:"signature"`
}

// EnrollmentResponse is the success body for /worker-bootstrap/enroll.
// `kid` is the per-worker JWT header value ("wkr_" + 8 hex chars of
// sha256(public_key)) — WorkerAuth's wkr_ branch (commit 4) routes
// verification through DeriveWorkerSecret. `secret` is base64-encoded
// 32 bytes — the HS256 signing key. `expires_at` is the worker
// secret's recommended rotation deadline; the worker persists this
// alongside (kid, secret) so an upcoming deadline can trigger
// re-enrollment.
type EnrollmentResponse struct {
	Kid       string `json:"kid"`
	Secret    string `json:"secret"`
	ExpiresAt int64  `json:"expires_at"`
}

// EnrollWorker handles POST /worker-bootstrap/enroll — the second
// phase of the bootstrap handshake (issue #430). Replaces the
// cluster-leaking GET /worker-secret endpoint.
//
// The worker presents:
//  1. A valid bootstrap JWT (issued in phase 1) — auth via
//     BootstrapAuth middleware.
//  2. The phase-1 enrollment_challenge (single-use, server-side
//     record bound to worker_id + public_key).
//  3. An Ed25519 signature over sha256(public_key || challenge),
//     proving possession of the matching private key.
//
// On success the CP:
//  1. Persists the worker's public_key (idempotent — re-enrollment
//     with a new keypair overwrites).
//  2. Derives the worker's HS256 signing secret via HKDF
//     (signing.DeriveWorkerSecret).
//  3. Returns the derived secret + KID to the worker. The cluster
//     master secret never leaves the CP.
//
// Status codes:
//
//	200 — enrollment succeeded
//	400 — malformed body, mismatched worker_id, invalid signature
//	      hex, bad public_key shape
//	401 — bootstrap JWT invalid (set by BootstrapAuth middleware),
//	      challenge unknown / expired / replayed, Ed25519 signature
//	      verification failed
//	500 — persistence or HKDF failure
func (h *InternalHandler) EnrollWorker(w http.ResponseWriter, r *http.Request) {
	if h.bootstrapSecret == "" {
		http.Error(w, `{"error": "bootstrap not configured"}`, http.StatusNotImplemented)
		return
	}

	workerID := middleware.GetWorkerID(r.Context())
	tenantID := middleware.GetWorkerTenantID(r.Context())
	// region comes from the JWT claim that BootstrapAuth set on
	// the context. Phase 1 used the body's `region` field to mint
	// the JWT, so the value here is whatever the worker claimed at
	// phase 1. Reading from the context (not h.cpRegion, which is the
	// CP's own region) keeps the derived secret bound to the worker's
	// claimed region — a worker that lies about its region in phase 1
	// ends up with a secret that no WorkerAuth resolver can verify,
	// because the HKDF info string won't match the verify path.
	region := middleware.GetWorkerRegion(r.Context())

	var req EnrollmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.WorkerID == "" || req.PublicKey == "" || req.Challenge == "" || req.Signature == "" {
		httperror.BadRequestCtx(w, r, "worker_id, public_key, enrollment_challenge, and signature are required")
		return
	}
	// The body worker_id must match the bootstrap JWT's worker_id
	// claim. A swap to a different worker_id in phase 2 means the
	// attacker has a valid bootstrap JWT for SOME worker and is
	// trying to enroll for a DIFFERENT one — refuse.
	if req.WorkerID != workerID {
		log.Printf("enroll: body worker_id %q != JWT worker_id %q (refused)",
			req.WorkerID, workerID)
		httperror.BadRequestCtx(w, r, "worker_id must match the bootstrap JWT")
		return
	}

	// Decode public_key (must be 32 raw bytes = 64 hex chars) and
	// signature (must be 64 raw bytes = 128 hex chars).
	pubBytes, err := hex.DecodeString(req.PublicKey)
	if err != nil || len(pubBytes) != ed25519.PublicKeySize {
		httperror.BadRequestCtx(w, r, "public_key must be 64 lowercase hex chars (32-byte Ed25519 key)")
		return
	}
	sigBytes, err := hex.DecodeString(req.Signature)
	if err != nil || len(sigBytes) != ed25519.SignatureSize {
		httperror.BadRequestCtx(w, r, "signature must be 128 lowercase hex chars (64-byte Ed25519 signature)")
		return
	}
	challengeBytes, err := base64.RawURLEncoding.DecodeString(req.Challenge)
	if err != nil {
		httperror.BadRequestCtx(w, r, "enrollment_challenge must be base64url (no padding)")
		return
	}

	// Pop the challenge atomically — closes the replay window. Pop
	// also re-verifies that the body's public_key matches the one
	// captured at phase 1.
	stored, ok := h.enrollmentChallenges.Pop(req.WorkerID, req.PublicKey, time.Now())
	if !ok {
		auditRecord(r, "worker_enroll", "worker", workerID,
			fmt.Sprintf("worker %s enrollment challenge missing or replayed", workerID),
			"failure")
		httperror.UnauthorizedCtx(w, r, "enrollment challenge missing or replayed")
		return
	}
	// Double-check the challenge bytes match (the store stores the
	// base64 string, but a future change might normalize). Comparing
	// the decoded bytes closes a tampering vector where the worker
	// re-encodes a different challenge and slips it past the store.
	storedBytes, err := base64.RawURLEncoding.DecodeString(stored.Challenge)
	if err != nil || !bytesEqual(challengeBytes, storedBytes) {
		httperror.UnauthorizedCtx(w, r, "enrollment challenge mismatch")
		return
	}

	// Verify the Ed25519 signature over sha256(public_key || challenge).
	// The hash binds signature to both inputs — without the hash, an
	// attacker could lift the signature onto a (public_key', challenge')
	// pair and (if they controlled public_key') impersonate the
	// original worker against any verifier that didn't pin both inputs.
	h2 := sha256.New()
	h2.Write(pubBytes)
	h2.Write(challengeBytes)
	digest := h2.Sum(nil)
	if !ed25519.Verify(pubBytes, digest, sigBytes) {
		auditRecord(r, "worker_enroll", "worker", workerID,
			fmt.Sprintf("worker %s enrollment Ed25519 signature failed to verify", workerID),
			"failure")
		log.Printf("enroll: worker %s Ed25519 signature verification failed", workerID)
		httperror.UnauthorizedCtx(w, r, "signature verification failed")
		return
	}

	// Persist the public key. workers.public_key (migration 032) is
	// the column WorkerAuth re-derives from at verify time. The
	// affected-row count lets us detect a worker that enrolled
	// without ever registering (logic error in the worker flow).
	affected, err := h.workerKeyRepo.SetPublicKey(r.Context(), workerID, req.PublicKey)
	if err != nil {
		log.Printf("enroll: SetPublicKey for worker %s failed: %v", workerID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	// Drop any cached entry so the next inbound request reloads
	// from the freshly-persisted public_key. Without this a
	// re-enrolling worker would 401 until the TTL elapsed —
	// confusing during a planned rotation.
	if h.workerJWTConfig.WorkerKeyCache != nil {
		h.workerJWTConfig.WorkerKeyCache.Invalidate(workerID)
	}
	if affected == 0 {
		// No matching workers row — refuse. The worker must register
		// via POST /api/internal/workers first (a public, unauthenticated
		// handshake that establishes the worker_id without any
		// sensitive key material). The CP design intentionally
		// separates "worker can identify itself" (register) from
		// "worker proves identity" (enroll).
		auditRecord(r, "worker_enroll", "worker", workerID,
			fmt.Sprintf("worker %s enrollment failed: no registered workers row", workerID),
			"failure")
		httperror.BadRequestCtx(w, r, "worker must register before enrolling")
		return
	}

	// Derive the per-worker HS256 secret. Master key is the cluster
	// JWT_SECRET; salt and info bind the derivation to this specific
	// worker identity. The output is 32 bytes — the HS256 key size.
	derived, err := signing.DeriveWorkerSecret(
		[]byte(h.jwtSecret), workerID, tenantID, region, req.PublicKey)
	if err != nil {
		log.Printf("enroll: DeriveWorkerSecret for worker %s failed: %v", workerID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	kid := signing.WorkerKID(req.PublicKey)
	// Recommended rotation deadline — the worker should re-enroll
	// before this timestamp. 24h matches the historical bootstrap-
	// derived JWT TTL (issue #76), so the operational rhythm doesn't
	// change even though the underlying secret material now rotates.
	expiresAt := time.Now().Add(24 * time.Hour)

	auditRecord(r, "worker_enroll", "worker", workerID,
		fmt.Sprintf("worker %s (tenant=%s, region=%s) enrolled kid=%s",
			workerID, tenantID, region, kid),
		"success")
	log.Printf("enroll: worker %s (tenant=%s, region=%s) enrolled kid=%s",
		workerID, tenantID, region, kid)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(EnrollmentResponse{
		Kid:       kid,
		Secret:    base64.RawURLEncoding.EncodeToString(derived),
		ExpiresAt: expiresAt.Unix(),
	})
}

// bytesEqual is a tiny local helper to avoid pulling in a `bytes` import
// for what would otherwise be a one-line use of bytes.Equal. Keeping
// it local reduces the import surface and makes the
// "constant-time-ish" intent more visible at the call site.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// WorkerTokenRequest is the body shape for POST /api/internal/tokens/tenant
// (issue #491). The worker presents its existing (wildcard or scoped)
// bearer JWT to authenticate, supplies the tenant_id it wants a token
// for, and receives a freshly-signed JWT whose TenantID claim is bound
// to that tenant. The wildcard (`*`) and empty string are refused
// upstream — see MintWorkerToken for the rationale.
type WorkerTokenRequest struct {
	TenantID string `json:"tenant_id"`
}

// WorkerTokenResponse is the success shape. The CP echoes the bound
// tenant_id alongside the signed token so the worker can detect a
// silent re-scope (defense in depth — the worker's own scope is
// carried in the JWT itself, but a mismatch in the response body is
// the easier diagnostic when things go wrong).
//
// `expires_at` is Unix seconds (matches how JWT `exp` is encoded by
// the golang-jwt library) so the worker can decide locally whether to
// adopt or re-mint without parsing the token.
type WorkerTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expires_at"`
	TenantID  string `json:"tenant_id"`
}

// isSafeTenantID gates tenant_id values on POST /api/internal/tokens/tenant.
// Returns nil when the value is a legal EdgeCloud tenant identifier and
// a descriptive error otherwise. Mirrors `supervisor.rs::is_safe_tenant_id`
// — same regex spirit (`^[a-z0-9_-]{1,64}$`, no path-traversal chars),
// same defense-in-depth rationale: the worker enforces this same check
// before constructing outbound paths, so a CP-side match keeps the
// verifier's failure modes consistent across both ends.
//
// The charset ([a-z0-9_-]) is the same shape the `tenants.id` DB
// column uses in practice (see CLAUDE.md — IDs are prefixed `t_`,
// lowercase alnum + underscore + dash). A future PR that loosens the
// column regex MUST also loosen this regex AND revisit the audit-log
// `Details` strings in MintWorkerToken — the validator is what keeps
// worker-controlled tenant_id values from smuggling log-injection
// content into the audit table.
//
// The wildcard (`*`) and empty string checks live here, not in
// IsSharedWorker — the refusal-to-mint guard's job is to shut down a
// class of misuse upstream, before the verifier sees a token whose
// `TenantID` is the wildcard. `Download` and `AutoRollback` both treat
// wildcard as "trusted" (their IsSharedWorker branch escalates access);
// the mint endpoint must never produce such a token. The regex itself
// would also reject `*` (the character class doesn't include it), so
// the explicit check is defense-in-depth for the failure mode where a
// future PR loosens the charset.
func isSafeTenantID(s string) error {
	if s == "" {
		return errors.New("tenant_id is required")
	}
	if s == "*" {
		return errors.New("tenant_id \"*\" is refused: wildcard tokens would grant cross-tenant access")
	}
	if len(s) > 64 {
		return fmt.Errorf("tenant_id must be at most 64 characters (got %d)", len(s))
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-'
		if !ok {
			return fmt.Errorf("tenant_id must match ^[a-z0-9_-]+$ (got %q)", s)
		}
	}
	return nil
}

// MintWorkerToken handles POST /api/internal/tokens/tenant (issue #491).
//
// The worker presents an existing valid bearer JWT (the bootstrap-derived
// 24h JWT today, or any previously-minted scoped token later) and asks
// for a fresh token bound to a specific tenant. The CP mints an HS256
// JWT with the same `kid` header used for the bootstrap / ingress
// tokens so a single keyring verifies every token in the fleet.
//
// Refusal-to-mint-wildcard: `tenant_id == "*"` (and `""`, and any value
// that fails isSafeTenantID) is rejected with 400. The wildcard would
// pass VerifyWorkerJWT (its TenantID claim is a normal string), but
// `IsSharedWorker` (middleware/worker.go:251-254) treats it as a
// "trusted shared worker" — Download and AutoRollback both escalate
// cross-tenant access for shared workers. The CP must not mint the
// (very effective) attack primitive that the worker already has the
// ability to construct locally, but might still issue by mistake.
//
// Request:
//
//	POST /api/internal/tokens/tenant
//	Authorization: Bearer <bootstrap or previously-minted scoped JWT>
//	Content-Type: application/json
//	{"tenant_id": "t_abc123"}
//
// Response (200):
//
//	{"token": "<signed JWT>", "expires_at": 1749400000, "tenant_id": "t_abc123"}
//
// Status codes:
//
//	200 — token issued
//	400 — malformed body / missing or invalid tenant_id
//	401 — caller has no / expired bearer (set by WorkerAuth middleware)
//	403 — caller is not currently hosting the requested tenant
//	      (issue #491 constraint #2; see workerHostingSvc gate below)
//	404 — tenant_id does not exist or is disabled
//	500 — signing key resolution, hosting lookup, or signature
//	      construction failure
//
// resolvePerWorkerSigningKey returns the (HS256 secret, kid) tuple
// the MintWorkerToken handler uses to mint a per-tenant, per-worker
// JWT (issue #430).
//
// Failure modes:
//   - WorkerKeyCache not configured: 500 — operator hasn't wired
//     the loader. Closing the failure mode in the same way as
//     WorkerAuth's wkr_ branch keeps the verify path symmetric
//     with the mint path.
//   - Worker has no enrolled public_key: 500 — the worker hasn't
//     completed the bootstrap handshake. This is a defensive
//     error: the production flow guarantees enrollment happens
//     before any token-mint call, so a non-enrolled worker is a
//     logic bug somewhere upstream. 500 (not 401) keeps the
//     caller from re-trying on what is effectively a server
//     configuration error.
func (h *InternalHandler) resolvePerWorkerSigningKey(ctx context.Context, workerID, tenantID string) ([]byte, string, error) {
	region := middleware.GetWorkerRegion(ctx)
	signingKey, err := h.workerJWTConfig.ResolveSigningKeyForWorker(ctx, workerID, tenantID, region)
	if err != nil {
		return nil, "", err
	}
	// Re-derive the kid. We can't rely on the cache key alone —
	// the kid is the worker's wkr_ fingerprint, which depends on
	// the public_key, which the cache layer keeps opaque.
	pubkey, err := h.workerJWTConfig.WorkerKeyCache.GetOrLoad(ctx, workerID)
	if err != nil {
		return nil, "", err
	}
	if pubkey == "" {
		return nil, "", fmt.Errorf("worker %s has no enrolled public_key", workerID)
	}
	return signingKey, signing.WorkerKID(pubkey), nil
}

func (h *InternalHandler) MintWorkerToken(w http.ResponseWriter, r *http.Request) {
	workerID := middleware.GetWorkerID(r.Context())

	var req WorkerTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if err := isSafeTenantID(req.TenantID); err != nil {
		auditRecord(r, "worker_token_mint", "tenant", req.TenantID,
			fmt.Sprintf("worker %s refused tenant_id: %v", workerID, err), "failure")
		httperror.BadRequestCtx(w, r, err.Error())
		return
	}

	// Tenant-existence check. A worker asking for "t_typo" must not
	// get a valid token issued — even if the verifier would later 404
	// the request anyway, minting a token for a non-existent tenant
	// wastes a slot in the CP's mint-rate limiter and produces audit
	// spam. Failing fast here also closes a (hypothetical) side
	// channel via audit-log observation.
	tenant, err := h.tenantSvc.GetByID(r.Context(), req.TenantID)
	if err != nil {
		if errors.Is(err, service.ErrTenantNotFound) {
			auditRecord(r, "worker_token_mint", "tenant", req.TenantID,
				fmt.Sprintf("worker %s requested token for missing tenant %s", workerID, req.TenantID), "failure")
			httperror.NotFoundCtx(w, r, "tenant not found")
			return
		}
		log.Printf("worker-token: tenant lookup for %s failed: %v", req.TenantID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	// Defense-in-depth: the production *service.TenantService translates
	// repo-side (nil, nil) into ErrTenantNotFound (see tenant.go:129),
	// but the tenantGetter interface permits other implementations
	// (test mocks, custom deployments). Treat a nil tenant with nil
	// error as not-found so a future call-site change can't reopen
	// the nil-deref that PR #491 review caught.
	if tenant == nil {
		auditRecord(r, "worker_token_mint", "tenant", req.TenantID,
			fmt.Sprintf("worker %s requested token for missing tenant %s", workerID, req.TenantID), "failure")
		httperror.NotFoundCtx(w, r, "tenant not found")
		return
	}
	// Disabled tenants get the same treatment as missing ones — the
	// token would technically verify, but issuing it costs the CP
	// nothing useful and lets the worker downstream 402-storm on
	// every per-tenant call instead of failing fast at mint time.
	// Issue #440 already prevents a deployment against a disabled
	// tenant; this guard extends the same protection to per-tenant
	// JWT mint.
	if tenant.IsDisabled() {
		auditRecord(r, "worker_token_mint", "tenant", req.TenantID,
			fmt.Sprintf("worker %s requested token for disabled tenant %s", workerID, req.TenantID), "failure")
		httperror.NotFoundCtx(w, r, "tenant not found")
		return
	}

	// Hosting constraint (issue #491 constraint #2): a worker can
	// only mint for tenants currently in worker_status.apps with
	// status = 'running' for this worker_id. Without this gate, a
	// compromised worker calling POST /tokens/tenant could walk
	// away with a 15-minute bearer for any tenant it has no
	// relationship with — functionally identical to today's
	// wildcard JWT, defeating the entire security goal of #491.
	//
	// Order rationale: tenant-existence (404) first prevents the 403
	// from being a tenant-existence oracle; disabled-tenant (404)
	// preserves the existing "disabled → 404" invariant; the hosting
	// check is the last gate before HMAC signing — fail fast before
	// burning CPU.
	//
	// Applies to ALL callers, including inbound wildcard JWTs —
	// skipping the check for wildcards would re-open the cross-tenant
	// primitive for every freshly-bootstrapped worker.
	if h.workerHostingSvc != nil {
		hosted, err := h.workerHostingSvc.TenantsHostedBy(r.Context(), workerID)
		if err != nil {
			log.Printf("worker-token: hosting lookup for worker %s failed: %v", workerID, err)
			httperror.InternalErrorCtx(w, r)
			return
		}
		hostedNow := false
		for _, t := range hosted {
			if t == req.TenantID {
				hostedNow = true
				break
			}
		}
		if !hostedNow {
			auditRecord(r, "worker_token_mint", "tenant", req.TenantID,
				fmt.Sprintf("worker %s denied token for tenant %s: hosting check failed (worker hosts %v)",
					workerID, req.TenantID, hosted),
				"failure")
			httperror.ForbiddenCtx(w, r, "tenant not hosted by this worker")
			return
		}
	}

	// Per-worker signing key (issue #430): the minted per-tenant
	// token must verify against the same HKDF-derived secret the
	// WorkerAuth wkr_ branch computes at verify time, otherwise
	// the worker would 401 on its very next inbound call. The
	// resolver also stamps the kid header with the worker's
	// wkr_ fingerprint, so a future WorkerAuth implementation can
	// rely on the kid for routing without re-running the cache.
	signingKey, kid, err := h.resolvePerWorkerSigningKey(r.Context(), workerID, req.TenantID)
	if err != nil {
		log.Printf("worker-token: failed to resolve signing key for worker %s: %v", workerID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	now := time.Now()
	exp := now.Add(h.workerTokenTTL)
	claims := middleware.WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    h.issuer,
			Subject:   workerID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
			NotBefore: jwt.NewNumericDate(now.Add(-1 * time.Minute)),
		},
		WorkerID: workerID,
		TenantID: req.TenantID,
		Region:   middleware.GetWorkerRegion(r.Context()),
		Role:     middleware.RoleWorker,
		// Apps stays empty — per-app scoping is out of scope for this
		// issue; a follow-up PR extends the claim shape.
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(signingKey)
	if err != nil {
		log.Printf("worker-token: failed to sign token for worker %s tenant=%s: %v", workerID, req.TenantID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	auditRecord(r, "worker_token_mint", "tenant", req.TenantID,
		fmt.Sprintf("worker %s minted scoped token for tenant %s (ttl=%s)", workerID, req.TenantID, h.workerTokenTTL),
		"success")

	log.Printf("worker-token: minted token for worker %s tenant=%s ttl=%s", workerID, req.TenantID, h.workerTokenTTL)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(WorkerTokenResponse{
		Token:     signed,
		ExpiresAt: exp.Unix(),
		TenantID:  req.TenantID,
	})
}
