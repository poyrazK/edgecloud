// Package app assembles the complete dependency graph of the edge-cloud
// control plane. It is the only package that imports every internal
// sub-package; every other package communicates through narrow interfaces.
package app

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/autoscale"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing/noop"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing/stripe"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/cache"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/loophealth"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service/wit"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/jmoiron/sqlx"
)

// App holds all assembled dependencies. After New returns, the caller has
// a fully-wired application ready to serve HTTP traffic and run background
// goroutines.
type App struct {
	// Handler is the complete http.Handler with all middleware and routes applied.
	Handler http.Handler

	// Region, WorkerJWTConfig, and ArtifactPath are exposed so main.go
	// can mint the ingress service token at startup.
	Region          string
	WorkerJWTConfig middleware.WorkerJWTConfig
	ArtifactPath    string

	// Background service references. RunBackground starts all three.
	WorkerSvc         *service.WorkerService
	ReconcileSvc      *service.ReconcileService
	LogGC             *service.LogGCService
	WorkerGC          *service.WorkerGCService
	DeploymentGC      *service.DeploymentGCService
	AuditGC           *service.AuditGCService           // issue #574 retention GC
	WebhookDeliveryGC *service.WebhookDeliveryGCService // issue #574 retention GC
	AutoscaleEventGC  *service.AutoscaleEventGCService  // issue #574 retention GC
	// PreviewGC (issue #308) reclaims expired preview deployments
	// and their artifact blobs. Tunable via env
	// (PREVIEW_GC_INTERVAL, PREVIEW_RETENTION). Disabled by
	// invalid interval/retention — PreviewGCService.Run refuses
	// to start with non-positive values.
	PreviewGC *service.PreviewGCService
	// IdempotencyGC (issue #439 follow-up) deletes
	// active_deployment_idempotency_keys rows whose created_at is
	// older than the cache TTL (24h — see repository.IdempotencyTTL).
	// Without a sweeper, the table grows unbounded over a
	// deployment's lifetime: the Lookup-side TTL filter makes aged-
	// out rows invisible to the replay path, but they still occupy
	// disk + are visited by every INSERT's index update.
	// Tunable via env IDEMPOTENCY_GC_INTERVAL. Disabled by an
	// invalid interval — IdempotencyKeyGCService.Run refuses to
	// start on a non-positive value (matches PreviewGC's safety
	// check).
	IdempotencyGC *service.IdempotencyKeyGCService
	// CacheRetrySweep (issue #501) re-attempts per-region
	// artifact-cache pushes for deployments whose previous push
	// attempt landed in regions_cache_failed. Tunable via env
	// REGION_CACHE_RETRY_INTERVAL (default 5m). Disabled by an
	// invalid interval — CacheRetrySweepService.Run refuses to
	// start on a non-positive value (matches PreviewGC's safety
	// check). Reads the live pusher + region map on every sweep
	// tick via getter closures, so operator-set config is honored
	// at runtime.
	CacheRetrySweep *service.CacheRetrySweepService
	// DeploymentSvc is exposed so main.go can inject the per-region
	// artifact-cache pusher (issue #332) after construction. Optional
	// post-New wiring — when not set, the deployment service runs
	// without cache push (existing behavior).
	DeploymentSvc *service.DeploymentService
	AutoscaleSvc  *autoscale.Service
	// OutboxDrainer (issue #42) relays durable-publish rows from the
	// `outbox` table to NATS. Activated by RunBackground alongside the
	// reconcile loop. Multi-instance safe via FOR UPDATE SKIP LOCKED +
	// a 30s claimed_until window on each claimed row.
	OutboxDrainer *service.OutboxDrainer

	// MeteringDrainer (issue #485) relays metering-ledger rows from
	// `billing_usage_events` to the configured MeteringProvider.
	// Multi-instance safe via FOR UPDATE SKIP LOCKED + processed_at
	// stamping. The drainer is the SOLE caller of
	// MeteringProvider.RecordUsage; the heartbeat pipeline only writes
	// to billing_usage_events (Commit 4 of this PR adds that
	// dual-write call site).
	MeteringDrainer *service.MeteringDrainer

	// loopHealth tracks liveness of every background goroutine spawned
	// by RunBackground (heartbeat, log_gc, reconcile, worker_gc,
	// deployment_gc, autoscale). It also drives the per-loop map on
	// /health so operators can see degraded state without scraping
	// logs. See internal/loophealth for the package contract. Related
	// to issue #443.
	loopHealth *loophealth.Tracker

	// cacheRetryIntervalS is the cache-retry sweep's tick interval
	// in seconds, captured from cfg.CacheRetry.IntervalS at
	// construction time. Stored as a field so RunBackground
	// doesn't need the original *config.Config after New returns
	// (matches the "post-New wiring" pattern documented in the
	// CacheRetrySweep doc — only the pusher and region map are
	// late-bound getters, the interval is fixed for process
	// lifetime). Issue #501.
	cacheRetryIntervalS int
}

// New creates a fully-wired App from the given infrastructure dependencies.
// It instantiates all 13 repositories, 14 services, 18 handlers, middleware,
// and registers every route on a single http.Handler.
func New(
	cfg *config.Config,
	db *sqlx.DB,
	publisher *nats.NATSPublisher,
	artifactStore storage.ArtifactStore,
	openAPISpec embed.FS,
) *App {
	// ── Repositories ──────────────────────────────────────────────
	tenantRepo := repository.NewTenantRepository(db)
	quotaRepo := repository.NewQuotaRepository(db)
	apiKeyRepo := repository.NewAPIKeyRepository(db)
	deploymentRepo := repository.NewDeploymentRepository(db)
	activeDeploymentRepo := repository.NewActiveDeploymentRepository(db)
	appEnvRepo := repository.NewAppEnvRepository(db)
	appRepo := repository.NewAppRepository(db)
	workerRepo := repository.NewWorkerRepository(db)
	trafficSplitRepo := repository.NewTrafficSplitRepository(db)
	logEntryRepo := repository.NewLogEntryRepository(db)
	domainRepo := repository.NewDomainRepository(db)
	autoscaleEventRepo := repository.NewAutoscaleRepository(db)
	// Billing (issue #419): billing_subscriptions + billing_events.
	billingRepo := repository.NewBillingRepository(db)
	// Idempotency-Key replay cache (issue #52). The repo is
	// optional from the service's perspective — when not
	// injected, Deploy behaves exactly as it did pre-#52
	// (always mints a fresh deployment_id). We always wire
	// it here so production gets the replay cache by default
	// and only test harnesses that explicitly want the
	// pre-#52 behavior omit it.
	idempotencyRepo := repository.NewIdempotencyKeyRepo(db)
	// Issue #439: replay cache for the activate / promote / rollback
	// paths. When neither is injected, the activate path falls back
	// to pre-#439 fresh-publish semantics (same shape as Deploy's
	// nil-check on idempotencyRepo). Production always wires it so
	// concurrent retries carrying the same Idempotency-Key
	// short-circuit inside the tx without enqueueing a duplicate
	// task_update outbox row.
	activateIdempotencyRepo := repository.NewActiveDeploymentIdempotencyKeyRepo(db)
	// Outbox (issue #42): durable-publish queue for `task_update`
	// NATS messages. Rows are written in the same tx as the
	// active_deployments mutation; the OutboxDrainer (below) relays
	// them after commit.
	outboxRepo := repository.NewOutboxRepository(db)
	// BillingUsageRepository (issue #485): the metering ledger the
	// heartbeat pipeline dual-writes into. The MeteringDrainer
	// (constructed further down) reads from this repo via the same
	// *sqlx.DB instance.
	meteringRepo := repository.NewBillingUsageRepository(db)

	// ── Services ──────────────────────────────────────────────────
	// Load the Ed25519 signing keyring (issue #307 PR1). The config
	// validator already enforced that at least one of KeyringPath /
	// Keyring / KeyPath / Key is set; here we resolve the actual
	// keyring and surface any load-time errors (malformed key, missing
	// file, empty keyring) as a fatal startup condition rather than a
	// runtime failure on the first Deploy.
	keyring, err := loadKeyring(&cfg.Signing)
	if err != nil {
		log.Fatalf("loading signing keyring: %v", err)
	}
	if cfg.Signing.KeyID == "" {
		log.Printf("WARNING: EDGE_SIGNING_KEY_ID is empty; rotation semantics will be ambiguous. Set a logical key id (e.g. \"k1\") before shipping rotation code.")
	}

	tenantSvc := service.NewTenantService(db, tenantRepo, quotaRepo, apiKeyRepo, appRepo, outboxRepo, cfg.Region)
	apiKeySvc := service.NewAPIKeyService(apiKeyRepo)
	appSvc := service.NewAppService(
		db, appRepo, deploymentRepo, activeDeploymentRepo, appEnvRepo, artifactStore, quotaRepo,
		outboxRepo, cfg.Region,
		uint16(cfg.L4.PortRangeStart), uint16(cfg.L4.PortRangeEnd),
	)
	deploymentSvc := service.NewDeploymentService(
		db, deploymentRepo, activeDeploymentRepo, appEnvRepo,
		quotaRepo, repository.NewMemoryQuotaRepository, tenantRepo, outboxRepo, artifactStore, publisher, cfg.Region, keyring,
	)
	// OutboxDrainer (issue #42): the SOLE caller of
	// Publisher.PublishTaskUpdate for `task_update` messages. Tunable
	// via OUTBOX_DRAIN_INTERVAL / OUTBOX_MAX_ATTEMPTS. Defaults: 2s
	// tick, 50 rows/batch, 10 retries before flipping the row to
	// status='failed' for operator inspection.
	outboxDrainer := service.NewOutboxDrainer(
		outboxRepo, publisher,
		parseDurationEnv("OUTBOX_DRAIN_INTERVAL", 2*time.Second),
		parseIntEnv("OUTBOX_BATCH_SIZE", 50),
		parseIntEnv("OUTBOX_MAX_ATTEMPTS", 10),
	)
	deploymentSvc.SetAppService(appSvc)
	envSvc := service.NewEnvService(appEnvRepo)
	// Issue #560: shared TaskMessage-marshaling helper used by both
	// DeploymentService (activate / rollback) and EnvService (set /
	// delete). Constructed once and threaded into both services so
	// the wire format stays single-source.
	publishBuilder := service.NewPublishBuilder()
	deploymentSvc.SetPublishBuilder(publishBuilder)
	envSvc.SetPublishDeps(
		db, tenantRepo, activeDeploymentRepo, deploymentRepo,
		quotaRepo, outboxRepo, appEnvRepo, publishBuilder,
	)
	metricsAgg := service.NewMetricsAggregator()
	// loopHealth tracks liveness of every background goroutine. It must
	// be constructed before the services that need to feed it (so the
	// heartbeat drain and autoscaler can call Beat/RecordPanic), and
	// the same instance is reused by the /health closure and the App
	// struct (issue #443 review findings #3 and #4).
	loopHealth := newLoopHealth()
	workerSvc := service.NewWorkerService(
		db, workerRepo, quotaRepo, meteringRepo, activeDeploymentRepo, tenantRepo,
		publisher.Conn(), stableWindowFromEnv(), metricsAgg,
		loopHealth,
	)
	clusterSvc := service.NewClusterService(workerRepo, autoscaleEventRepo)
	// Materialize the canonical WIT tree at startup. The MigrationService
	// passes this absolute path as the `path:` argument to
	// wit_bindgen::generate!() in the synthetic Cargo project used by the
	// `edge-migrate --language rust` path (issue #415). The tree is embedded
	// into the binary at compile time (internal/service/wit/embed.go) and
	// sync'd against the top-level wit/ directory by the wit-drift-check CI
	// job.
	witDir, witErr := wit.Materialize()
	if witErr != nil {
		log.Fatalf("materializing embedded WIT tree: %v", witErr)
	}
	migrationSvc := service.NewMigrationService(
		deploymentRepo, artifactStore,
		cfg.Migration.EdgeMigratePath, cfg.Migration.WasiSdkPath, cfg.Migration.RustcPath,
		cfg.Migration.WasmToolsPath, cfg.Migration.CargoPath, witDir,
		keyring,
	)
	trafficSvc := service.NewTrafficService(
		db, trafficSplitRepo, deploymentRepo, activeDeploymentRepo,
		appEnvRepo, tenantRepo, quotaRepo, publisher, cfg.Region,
	)
	// PR #195 dropped the deploymentRepo arg — the reconcile now
	// pulls deployment hash + regions via ListByTenantWithDeployment
	// in a single round trip (N+1 elimination).
	reconcileSvc := service.NewReconcileService(
		tenantRepo, activeDeploymentRepo, appEnvRepo, quotaRepo, workerRepo, publisher, cfg.Region,
	)

	// Wire secrets encryption (if configured). Supports both legacy
	// (secrets_master_key) and keyring (secrets.active_key_id + keys) config.
	var secretsEnc *service.SecretEncryptor
	var encErr error
	if cfg.Secrets.ActiveKeyID != "" {
		secretsEnc, encErr = service.NewSecretEncryptorFromConfig(cfg.Secrets.ActiveKeyID, cfg.Secrets.Keys)
	} else if cfg.SecretsMasterKey != "" {
		secretsEnc, encErr = service.NewSecretEncryptorFromLegacy(cfg.SecretsMasterKey)
	}
	if encErr != nil {
		log.Fatalf("failed to create secrets encryptor: %v", encErr)
	}
	if secretsEnc != nil {
		envSvc.SetSecretEncryptor(secretsEnc)
		deploymentSvc.SetEnvService(envSvc)
		trafficSvc.SetEnvDecrypter(secretsEnc)
		reconcileSvc.SetEnvDecrypter(secretsEnc)

		// Issue #441: refuse to boot when plaintext app_env rows exist.
		// Operators seed these by SQL migration (legacy) or attacker
		// with DB write (injection elsewhere). Either way, the CP
		// must not silently accept them — runtime Decrypt now errors
		// (commits 1+2), so a plaintext row makes every publish of
		// that app fail. Better to fail at boot, point operators at
		// the migration path, and let them explicitly opt-in via
		// EDGE_ALLOW_LEGACY_PLAINTEXT_ENV=true for the migration window.
		//
		// context.Background() because the request context hasn't been
		// minted at startup; a slow COUNT shouldn't be cancellable.
		n, err := envSvc.CountPlaintextRows(context.Background())
		if err != nil {
			log.Fatalf("counting plaintext app_env rows at startup: %v", err)
		}
		if n > 0 {
			if !cfg.Secrets.AllowLegacyPlaintextEnv {
				log.Fatalf("found %d plaintext app_env rows at startup (issue #441); re-encrypt via POST /api/v1/admin/secrets/re-encrypt or set EDGE_ALLOW_LEGACY_PLAINTEXT_ENV=true for the migration window", n)
			}
			log.Printf("WARNING: found %d plaintext app_env rows at startup; EDGE_ALLOW_LEGACY_PLAINTEXT_ENV=true (issue #441 migration window). Re-encrypt via POST /api/v1/admin/secrets/re-encrypt.", n)
		}
	}

	webhookRepo := repository.NewWebhookRepository(db)
	webhookSvc := service.NewWebhookService(webhookRepo)
	deploymentSvc.SetWebhookService(webhookSvc)
	deploymentSvc.SetIdempotencyRepo(idempotencyRepo)
	deploymentSvc.SetActivateIdempotencyRepo(activateIdempotencyRepo)
	// Issue #641: wire the worker repo into the DeploymentService so
	// the Pre-check 6 region_at_capacity gate can ask "is the target
	// region's port pool saturated?" before opening the deploy tx.
	// The seam is optional — SetWorkerRepo(nil) skips the gate —
	// but in production we always want the gate wired so a
	// fleet-wide saturation signal surfaces as a 402 rather than a
	// stuck-pending deployment that the reconcile loop can't recover.
	deploymentSvc.SetWorkerRepo(workerRepo)
	webhookHandler := handler.NewWebhookHandler(webhookSvc)

	// Billing service (issue #419). The provider is selected by
	// cfg.Billing.Provider; validateBillingConfig has already
	// enforced presence of the required credentials. The factory
	// pattern keeps stripe-go scoped to its sub-package — only
	// app.go's import graph knows about both providers.
	billingProvider := newBillingProvider(cfg.Billing)
	billingSvc := billing.NewService(
		db, billingRepo, billingProvider, tenantSvc,
		cfg.Billing.SuccessURL, cfg.Billing.CancelURL,
	)
	billingHandler := handler.NewBillingHandler(billingSvc)

	// Metering provider + drainer (issue #485). Sibling to the
	// BillingProvider above — same factory shape, different lifecycle
	// (fire-and-record vs request/response). The drainer ticks at
	// cfg.Billing.Metering.IntervalS (default 30s = heartbeat cadence)
	// and claims up to cfg.Billing.Metering.BatchSize rows per tick.
	// Rate card zero-fallback happens inside the drainer itself, so a
	// fresh install with no METERING_RATE_* env vars is fully
	// billing-neutral out of the box.
	meteringProvider := newMeteringProvider(cfg.Billing)
	meteringDrainer := service.NewMeteringDrainer(
		meteringRepo, meteringProvider,
		time.Duration(cfg.Billing.Metering.IntervalS)*time.Second,
		cfg.Billing.Metering.BatchSize,
		cfg.Billing.Metering.MaxAttempts,
		cfg.Billing.Metering.Rates,
	)

	migrationHandler := handler.NewMigrationHandler(migrationSvc, metricsAgg.NewMigratePreflightSink())
	logSvc := service.NewLogService(logEntryRepo)
	domainSvc := service.NewDomainService(db, domainRepo, appRepo)

	var autoscaleSvc *autoscale.Service
	if cfg.Autoscale.Enabled {
		cloud, err := autoscale.NewCloudProvider(cfg.Autoscale.ProviderKind, nil)
		if err != nil {
			log.Fatalf("autoscale: invalid provider_kind %q: %v", cfg.Autoscale.ProviderKind, err)
		}
		autoscaleSvc = autoscale.NewService(autoscale.Deps{
			Cfg: autoscale.Config{
				Enabled:            cfg.Autoscale.Enabled,
				MinWorkers:         cfg.Autoscale.MinWorkers,
				MaxWorkers:         cfg.Autoscale.MaxWorkers,
				TargetHeadroomPct:  cfg.Autoscale.TargetHeadroomPct,
				ScaleUpCooldownS:   cfg.Autoscale.ScaleUpCooldownS,
				ScaleDownCooldownS: cfg.Autoscale.ScaleDownCooldownS,
				DecisionIntervalS:  cfg.Autoscale.DecisionIntervalS,
			},
			NC:         publisher.Conn(),
			DeployRepo: activeDeploymentRepo,
			EventRepo:  autoscaleEventRepo,
			Cloud:      cloud,
			Tracker:    loopHealth,
		})
	}

	// ── Handlers ──────────────────────────────────────────────────
	auditRepo := repository.NewAuditRepository(db)
	auditor := service.NewAuditor(auditRepo)
	handler.DefaultAuditor = auditor

	tenantHandler := handler.NewTenantHandler(tenantSvc, quotaRepo)
	apiKeyHandler := handler.NewAPIKeyHandler(apiKeySvc)
	deploymentHandler := handler.NewDeploymentHandler(deploymentSvc, workerSvc, trafficSvc, artifactStore, cfg.Migration.Wasm2CwasmPath)
	envHandler := handler.NewEnvHandler(envSvc)
	// PR #195 / commit 2d61f94 (fold SetSyncBuilder into NewInternalHandler)
	// passes reconcileSvc as both arg 5 (syncRequester) and arg 6
	// (syncPayloadBuilder) — same *service.ReconcileService satisfies
	// both interfaces.
	//
	// Issue #491: pass workerJWTConfig + workerTokenTTL + issuer +
	// activeKID + tenantSvc for the POST /api/internal/tokens/tenant
	// mint endpoint. The four-value tuple of
	// {JWTConfig + WorkerTokenTTL + Issuer + ActiveKID} mirrors the
	// workerJWTConfig literal constructed at app.go:649-654 below;
	// kept as a local struct literal rather than hoisted so the
	// InternalHandler dependency stays explicit.
	//
	// `workerSvc` doubles as the hostingGetter — it has a
	// TenantsHostedBy(ctx, workerID) method (issue #491 constraint
	// #2) that the gate uses to refuse tokens for tenants the worker
	// isn't currently hosting.
	internalHandler := handler.NewInternalHandler(
		deploymentSvc, workerSvc, domainSvc, logEntryRepo,
		reconcileSvc, reconcileSvc,
		cfg.Region, cfg.BootstrapSecret, cfg.JWT.Secret,
		middleware.WorkerJWTConfig{
			Secret:    cfg.JWT.Secret,
			Issuer:    cfg.JWT.Issuer,
			ActiveKID: cfg.JWT.ActiveKID,
			Keys:      cfg.JWT.Keys,
		},
		cfg.JWT.WorkerTokenTTL,
		cfg.JWT.Issuer,
		cfg.JWT.ActiveKID,
		tenantSvc,
		workerSvc,
		workerRepo,                       // issue #430 — per-worker key enrollment (SetPublicKey)
		metricsAgg.NewWorkerEnrollSink(), // issue #430 — edge_worker_enroll_* metrics
	)
	appHandler := handler.NewAppHandler(appSvc)
	authHandler := handler.NewAuthHandler(tenantSvc, apiKeySvc)
	clusterHandler := handler.NewClusterHandler(clusterSvc)
	quotaHandler := handler.NewQuotaHandler(tenantSvc, quotaRepo)

	// Usage handler (issue #421): the tenant-facing usage dashboard.
	// Composes quota + billing reads with a 10s SWR cache so dashboard
	// refresh doesn't hammer the DB. upgrade_options is hardcoded
	// here as the single source of truth for the dashboard pricing
	// display; derive from a BillingProvider.ListPlans() call when
	// the pricing source-of-truth moves off the static plan table.
	usageCache := cache.NewUsageCache(10*time.Second, 60*time.Second)
	usageSvc := service.NewUsageServiceFromBillingProvider(
		quotaRepo,
		billingRepo,
		billingProvider,
		usageCache,
		service.UsageServiceConfig{},
	)
	usageHandler := handler.NewUsageHandler(usageSvc)
	trafficHandler := handler.NewTrafficHandler(trafficSvc, appRepo)
	egressHandler := handler.NewEgressHandler(tenantSvc, deploymentSvc)
	logHandler := handler.NewLogHandler(logSvc)
	workerStatusHandler := handler.NewWorkerStatusHandler(workerSvc)
	metricsHandler := handler.NewMetricsHandler(metricsAgg)
	domainHandler := handler.NewDomainHandler(domainSvc)

	// ── Middleware ─────────────────────────────────────────────────
	authMiddleware := middleware.NewAuthMiddleware(apiKeySvc)

	// ── Rate Limiters ─────────────────────────────────────────────
	// Tenant rate limiter: applied after auth on all /api/v1/* routes.
	// Zero-value configs use defaults set in config.Load().
	tenantLimiter := middleware.NewRateLimiter(cfg.RateLimit.TenantRate, cfg.RateLimit.TenantBurst)
	// Bootstrap rate limiter: tight limit for self-signup abuse prevention.
	bootstrapLimiter := middleware.NewRateLimiter(2, 5)
	// Tenant creation limiter: per-IP cap (10 per hour) to prevent DB fill.
	handler.DefaultTenantCreationLimiter = middleware.NewTenantCreationLimiter(10, 1*time.Hour)

	// workerTokenTenantKey is the per-tenant key-extractor for the
	// POST /api/internal/tokens/tenant rate limiter (issue #491). It
	// reads the request body's `tenant_id` JSON field without
	// consuming the body so the handler's own decode still sees the
	// full payload.
	//
	// Fallback (PR #491 review): if the body is empty / malformed /
	// missing tenant_id / >128 bytes, fall back to the worker_id
	// from the JWT context instead of returning "". The
	// RateLimiter.Middleware treats "" as "skip limiting" (see
	// middleware/ratelimit.go:135-139), which would let a worker
	// flood malformed bodies past the per-tenant bucket. Falling
	// back to worker_id routes those requests into the per-worker
	// bucket (10/5), which is tight enough to bound the blast while
	// letting genuine malformed bodies through at a sensible rate.
	//
	// Body restoration is necessary because rate limiters run
	// before the handler — without it, the handler's
	// json.NewDecoder would see EOF instead of the original bytes.
	// We use a buffered peek + TeeReader so the limiter's read does
	// not steal the body from the downstream handler. The buffer is
	// tiny (128 bytes is enough to span "tenant_id":"<value>" for
	// any reasonable ID — anything longer falls through to the
	// worker_id fallback but the handler still verifies length
	// ≤ 64 chars upstream).
	workerTokenTenantKey := workerTokenTenantKeyFromBody

	// ── Router ────────────────────────────────────────────────────
	mux := http.NewServeMux()

	// Global Prometheus metrics scrape endpoint — unauthenticated, intended for
	// internal operator access only. Do not expose this path on the public LB.
	mux.HandleFunc("GET /metrics", metricsHandler.GetAllMetrics)

	// Health check — pings the database (and optionally NATS) so load
	// balancers and orchestrators stop routing traffic to a control plane
	// instance with a dead database connection (issue #142).
	//
	// On success the body includes a per-loop "loops" map sourced from
	// the loophealth tracker (issue #443): if any background loop has
	// panicked or gone stale, status becomes "degraded" (still 200 so
	// load balancers don't pull the CP from rotation for a non-fatal
	// heartbeat panic) and degraded_reasons lists the affected loop
	// names. 503 is reserved for DB/NATS failures, same as before.
	//
	// The tracker is built once near the top of New() and threaded
	// into the services that feed it; the closure below captures the
	// same instance to read liveness for the response body.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := db.PingContext(r.Context()); err != nil {
			log.Printf("Health check: DB ping failed: %v", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "error": err.Error()})
			return
		}
		// Optional NATS connectivity check. A NATS outage doesn't kill
		// the API server (it still serves reads), but surfacing it in
		// the health check gives operators an early signal.
		if nc := publisher.Conn(); nc != nil {
			if err := nc.FlushTimeout(2 * time.Second); err != nil {
				log.Printf("Health check: NATS ping failed: %v", err)
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "error": err.Error()})
				return
			}
		}
		// Build the extended healthy body. Even on the "ok" path we
		// include the per-loop map so operators always see the same
		// shape — the existing `{"status":"ok"}` field stays first for
		// backward compatibility with clients that parse the JSON.
		snapshot := loopHealth.Snapshot()
		loops := make(map[string]loophealth.State, len(snapshot))
		var degradedReasons []string
		for _, s := range snapshot {
			loops[s.Name] = s
			if s.Panics > 0 || s.Stale {
				degradedReasons = append(degradedReasons, s.Name)
			}
		}
		status := "ok"
		if len(degradedReasons) > 0 {
			status = "degraded"
		}
		body := map[string]any{
			"status": status,
			"loops":  loops,
		}
		if len(degradedReasons) > 0 {
			body["degraded_reasons"] = degradedReasons
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(body)
	})

	// OpenAPI spec — served as raw YAML
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		f, err := openAPISpec.Open("docs/api/openapi.yaml")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer func() {
			if err := f.Close(); err != nil {
				log.Printf("Failed to close OpenAPI spec file: %v", err)
			}
		}()
		w.Header().Set("Content-Type", "application/x-yaml")
		if _, err := io.Copy(w, f); err != nil {
			log.Printf("Failed to copy OpenAPI spec to response: %v", err)
		}
	})

	// Swagger UI — serves the interactive API docs at /docs/
	// Redirect /docs (no trailing slash) to /docs/ so the relative assets load.
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/docs/", http.StatusMovedPermanently)
	})
	swaggerUIHTML := `<!DOCTYPE html><html><head><title>edgeCloud API</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.20.0/swagger-ui.css"/>
<script src="https://unpkg.com/swagger-ui-dist@5.20.0/swagger-ui-bundle.js"></script></head><body>
<div id="swagger-ui"></div><script>
window.onload=function(){window.ui=SwaggerUIBundle({
url:"/openapi.yaml",dom_id:"#swagger-ui",
presets:[SwaggerUIBundle.presets.apis,SwaggerUIBundle.SwaggerUIStandalonePreset]})};
</script></body></html>`
	mux.HandleFunc("GET /docs/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(swaggerUIHTML)); err != nil {
			log.Printf("Docs check: failed to write swagger UI: %v", err)
		}
	})

	// Public endpoints (no auth required) — IP rate limited
	mux.Handle("POST /api/v1/tenants",
		handler.DefaultTenantCreationLimiter.Middleware(
			bootstrapLimiter.Middleware(middleware.ClientIP)(
				http.HandlerFunc(tenantHandler.Bootstrap)),
		))

	// Deprecated: redirect old /api/... paths to /api/v1/... for clients still
	// on the old contract. Workers use /api/internal/... (unversioned).
	sunsetDate := "2026-09-20"
	redirectTo := func(to string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", to)
			w.Header().Set("X-Redirected-From", r.URL.Path)
			w.Header().Set("Sunset", sunsetDate)
			w.WriteHeader(http.StatusMovedPermanently)
		}
	}
	mux.HandleFunc("GET /api/tenants", redirectTo("/api/v1/tenants"))
	mux.HandleFunc("POST /api/tenants", redirectTo("/api/v1/tenants"))
	mux.HandleFunc("GET /api/keys", redirectTo("/api/v1/keys"))
	mux.HandleFunc("POST /api/keys", redirectTo("/api/v1/keys"))
	mux.HandleFunc("DELETE /api/keys/{keyID}", redirectTo("/api/v1/keys/"+"{keyID}"))
	mux.HandleFunc("PUT /api/keys/{keyID}", redirectTo("/api/v1/keys/"+"{keyID}"))
	mux.HandleFunc("GET /api/apps", redirectTo("/api/v1/apps"))
	mux.HandleFunc("GET /api/apps/{appName}", redirectTo("/api/v1/apps/"+"{appName}"))
	mux.HandleFunc("POST /api/apps/{appName}", redirectTo("/api/v1/apps/"+"{appName}"))
	mux.HandleFunc("PUT /api/apps/{appName}", redirectTo("/api/v1/apps/"+"{appName}"))
	mux.HandleFunc("DELETE /api/apps/{appName}", redirectTo("/api/v1/apps/"+"{appName}"))
	mux.HandleFunc("GET /api/apps/{appName}/active", redirectTo("/api/v1/apps/"+"{appName}/active"))
	mux.HandleFunc("GET /api/apps/{appName}/ingress", redirectTo("/api/v1/apps/"+"{appName}/ingress"))
	mux.HandleFunc("GET /api/apps/{appName}/l4-port", redirectTo("/api/v1/apps/"+"{appName}/l4-port"))
	mux.HandleFunc("POST /api/apps/{appName}/l4-port", redirectTo("/api/v1/apps/"+"{appName}/l4-port"))
	mux.HandleFunc("GET /api/apps/{appName}/env", redirectTo("/api/v1/apps/"+"{appName}/env"))
	mux.HandleFunc("POST /api/apps/{appName}/env", redirectTo("/api/v1/apps/"+"{appName}/env"))
	mux.HandleFunc("DELETE /api/apps/{appName}/env/{key}", redirectTo("/api/v1/apps/"+"{appName}/env/"+"{key}"))
	mux.HandleFunc("POST /api/apps/{appName}/activate/{deploymentID}", redirectTo("/api/v1/apps/"+"{appName}/activate/"+"{deploymentID}"))
	mux.HandleFunc("GET /api/apps/{appName}/logs", redirectTo("/api/v1/apps/"+"{appName}/logs"))
	mux.HandleFunc("GET /api/apps/{appName}/status", redirectTo("/api/v1/apps/"+"{appName}/status"))
	mux.HandleFunc("POST /api/apps/{appName}/domains", redirectTo("/api/v1/apps/"+"{appName}/domains"))
	mux.HandleFunc("GET /api/apps/{appName}/domains", redirectTo("/api/v1/apps/"+"{appName}/domains"))
	mux.HandleFunc("GET /api/apps/{appName}/domains/{fqdn}", redirectTo("/api/v1/apps/"+"{appName}/domains/"+"{fqdn}"))
	mux.HandleFunc("DELETE /api/apps/{appName}/domains/{fqdn}", redirectTo("/api/v1/apps/"+"{appName}/domains/"+"{fqdn}"))
	mux.HandleFunc("POST /api/deploy/{appName}", redirectTo("/api/v1/deploy/"+"{appName}"))
	mux.HandleFunc("GET /api/status/{deploymentID}", redirectTo("/api/v1/status/"+"{deploymentID}"))
	mux.HandleFunc("GET /api/list/{appName}", redirectTo("/api/v1/list/"+"{appName}"))
	mux.HandleFunc("GET /api/auth/whoami", redirectTo("/api/v1/auth/whoami"))
	mux.HandleFunc("GET /api/quotas", redirectTo("/api/v1/quotas"))
	mux.HandleFunc("GET /api/usage", redirectTo("/api/v1/usage"))
	mux.HandleFunc("POST /api/migrate", redirectTo("/api/v1/migrate"))
	mux.HandleFunc("GET /api/admin/tenants", redirectTo("/api/v1/admin/tenants"))
	mux.HandleFunc("POST /api/admin/tenants", redirectTo("/api/v1/admin/tenants"))
	mux.HandleFunc("GET /api/admin/tenants/{tenantID}", redirectTo("/api/v1/admin/tenants/"+"{tenantID}"))
	mux.HandleFunc("PUT /api/admin/tenants/{tenantID}", redirectTo("/api/v1/admin/tenants/"+"{tenantID}"))
	mux.HandleFunc("DELETE /api/admin/tenants/{tenantID}", redirectTo("/api/v1/admin/tenants/"+"{tenantID}"))
	mux.HandleFunc("POST /api/admin/tenants/{tenantID}/enable", redirectTo("/api/v1/admin/tenants/"+"{tenantID}/enable"))
	mux.HandleFunc("DELETE /api/admin/apps/{appName}", redirectTo("/api/v1/admin/apps/"+"{appName}"))
	mux.HandleFunc("GET /api/admin/cluster", redirectTo("/api/v1/admin/cluster"))

	// Protected API routes
	api := http.NewServeMux()
	api.HandleFunc("POST /api/v1/deploy/{appName}", deploymentHandler.Deploy)
	api.HandleFunc("POST /api/v1/migrate", migrationHandler.Migrate)
	api.HandleFunc("POST /api/v1/migrate-tree", migrationHandler.MigrateTree)
	api.HandleFunc("GET /api/v1/status/{deploymentID}", deploymentHandler.GetStatus)
	api.HandleFunc("GET /api/v1/list/{appName}", deploymentHandler.List)
	api.HandleFunc("POST /api/v1/apps/{appName}/activate/{deploymentID}", deploymentHandler.Activate)
	api.HandleFunc("POST /api/v1/apps/{appName}/rollback", deploymentHandler.Rollback)
	api.HandleFunc("POST /api/v1/apps/{appName}/promote/{deploymentID}", deploymentHandler.Promote)
	api.HandleFunc("GET /api/v1/apps/{appName}/active", deploymentHandler.GetActive)
	api.HandleFunc("GET /api/v1/apps/{appName}/status", workerStatusHandler.Get)
	api.HandleFunc("GET /api/v1/auth/whoami", authHandler.Whoami)
	api.HandleFunc("POST /api/v1/apps/{appName}/env", envHandler.Set)
	api.HandleFunc("GET /api/v1/apps/{appName}/env", envHandler.List)
	api.HandleFunc("DELETE /api/v1/apps/{appName}/env/{key}", envHandler.Delete)
	api.HandleFunc("GET /api/v1/quotas", quotaHandler.GetQuota)
	api.HandleFunc("GET /api/v1/usage", usageHandler.GetUsage)
	api.HandleFunc("POST /api/v1/apps/{appName}", appHandler.Create)
	api.HandleFunc("GET /api/v1/apps", appHandler.List)
	api.HandleFunc("GET /api/v1/apps/{appName}", appHandler.Get)
	api.HandleFunc("PUT /api/v1/apps/{appName}", appHandler.Update)
	api.HandleFunc("POST /api/v1/keys", apiKeyHandler.Create)
	api.HandleFunc("GET /api/v1/apps/{appName}/ingress", deploymentHandler.AppIngress)
	// L4/TCP public-port surface (issue #548). Both endpoints are
	// tenant-authenticated — the ingress uses GET to refresh its
	// L4PortCache (every 30s) and POST to force allocation on
	// demand when the implicit-on-first-heartbeat path doesn't fire
	// (e.g. the app has been seen by an ingress before any
	// heartbeat ran the L4 branch).
	api.HandleFunc("GET /api/v1/apps/{appName}/l4-port", appHandler.GetL4Port)
	api.HandleFunc("POST /api/v1/apps/{appName}/l4-port", appHandler.AllocateL4Port)
	api.HandleFunc("GET /api/v1/apps/{appName}/traffic", trafficHandler.GetTraffic)
	api.HandleFunc("PUT /api/v1/apps/{appName}/traffic", trafficHandler.SetTraffic)
	api.HandleFunc("GET /api/v1/keys", apiKeyHandler.List)
	api.HandleFunc("PUT /api/v1/keys/{keyID}", apiKeyHandler.Update)
	api.HandleFunc("DELETE /api/v1/keys/{keyID}", apiKeyHandler.Delete)
	api.HandleFunc("POST /api/v1/keys/{keyID}/rotate", apiKeyHandler.Rotate)
	api.HandleFunc("GET /api/v1/egress", egressHandler.Get)
	api.HandleFunc("PUT /api/v1/egress", egressHandler.Update)
	api.HandleFunc("GET /api/v1/apps/{appName}/logs", logHandler.List)
	api.HandleFunc("GET /api/v1/metrics", metricsHandler.GetTenantMetrics)
	// Custom-domain routes (issue #83)
	api.HandleFunc("POST /api/v1/apps/{appName}/domains", domainHandler.Add)
	api.HandleFunc("GET /api/v1/apps/{appName}/domains", domainHandler.List)
	api.HandleFunc("GET /api/v1/apps/{appName}/domains/{fqdn}", domainHandler.Get)
	api.HandleFunc("DELETE /api/v1/apps/{appName}/domains/{fqdn}", domainHandler.Remove)

	// Webhook CRUD routes
	api.HandleFunc("POST /api/v1/webhooks", webhookHandler.Create)
	api.HandleFunc("GET /api/v1/webhooks", webhookHandler.List)
	api.HandleFunc("PUT /api/v1/webhooks/{webhookID}", webhookHandler.Update)
	api.HandleFunc("DELETE /api/v1/webhooks/{webhookID}", webhookHandler.Delete)
	// Per-webhook delivery history (issue #659). Sibling route on the
	// same `api` mux — inherits Bearer API-key auth + tenant rate
	// limiting. Cursor pagination only; no offset to deprecate.
	api.HandleFunc("GET /api/v1/webhooks/{webhookID}/deliveries", webhookHandler.ListDeliveries)

	// Billing routes (issue #419). Three are auth-required; the
	// webhook below is mounted on the public mux because the
	// provider's VerifyWebhook checks the signature inline.
	api.HandleFunc("POST /api/v1/billing/checkout", billingHandler.StartCheckout)
	api.HandleFunc("POST /api/v1/billing/portal", billingHandler.OpenPortal)
	api.HandleFunc("GET /api/v1/billing/subscription", billingHandler.GetSubscription)

	// Admin routes (require owner role)
	admin := http.NewServeMux()
	admin.HandleFunc("GET /api/v1/admin/tenants", tenantHandler.List)
	admin.HandleFunc("POST /api/v1/admin/tenants", tenantHandler.Create)
	admin.HandleFunc("GET /api/v1/admin/tenants/{tenantID}", tenantHandler.Get)
	admin.HandleFunc("PUT /api/v1/admin/tenants/{tenantID}", tenantHandler.Update)
	admin.HandleFunc("DELETE /api/v1/admin/tenants/{tenantID}", tenantHandler.Delete)
	admin.HandleFunc("POST /api/v1/admin/tenants/{tenantID}/enable", tenantHandler.Enable)
	admin.HandleFunc("POST /api/v1/admin/tenants/{tenantID}/quota-override", tenantHandler.QuotaOverride)
	// Per-tenant data-plane rate limit write endpoint (issue #305).
	// Mirrors quota-override above but writes the four
	// tenant_rate_limit_* columns on the quotas row instead of the
	// existing monthly cap columns. Owner-role gated by the admin
	// mux auth chain (authMiddleware.Authenticate + RequireRole).
	admin.HandleFunc("PUT /api/v1/admin/tenants/{tenantID}/rate-limit", tenantHandler.SetTenantRateLimitAdmin)
	admin.HandleFunc("DELETE /api/v1/admin/apps/{appName}", appHandler.Delete)
	admin.HandleFunc("GET /api/v1/admin/cluster", clusterHandler.Get)
	admin.HandleFunc("GET /api/v1/admin/cluster/events", clusterHandler.Events)

	apiWithAuth := authMiddleware.Authenticate(api)
	apiWithOwner := authMiddleware.Authenticate(
		middleware.RequireRole("owner")(admin),
	)

	// Apply tenant rate limiter after auth on all authenticated routes.
	tenantRateLimit := tenantLimiter.Middleware(func(r *http.Request) string {
		return middleware.GetTenantID(r.Context())
	})
	mux.Handle("/api/v1/", tenantRateLimit(apiWithAuth))
	mux.Handle("/api/v1/admin/", tenantRateLimit(apiWithOwner))

	// Service-to-service read endpoint that the edge-ingress polls to
	// apply Caddy weights for canary/blue-green traffic splits.
	mux.HandleFunc("GET /api/v1/internal/traffic/{tenantID}/{appName}", func(w http.ResponseWriter, r *http.Request) {
		middleware.InternalAuth(cfg.InternalToken)(http.HandlerFunc(trafficHandler.GetTrafficInternal)).ServeHTTP(w, r)
	})

	// Per-app rate limit overrides for the ingress ratelimit fetcher (issue #305).
	mux.HandleFunc("GET /api/v1/internal/rate-limits/{tenantID}/{appName}", func(w http.ResponseWriter, r *http.Request) {
		middleware.InternalAuth(cfg.InternalToken)(http.HandlerFunc(trafficHandler.GetRateLimitsInternal)).ServeHTTP(w, r)
	})

	// Per-tenant quota state for the ingress Caddy 402 renderer
	// (issue #420). Polled every QUOTA_FETCH_INTERVAL (default 30s);
	// the response includes over_cap + locked_until so the ingress
	// can decide whether to inject a static_response 402 block for
	// the tenant's apps.
	mux.HandleFunc("GET /api/v1/internal/quota/{tenantID}", func(w http.ResponseWriter, r *http.Request) {
		middleware.InternalAuth(cfg.InternalToken)(http.HandlerFunc(quotaHandler.GetQuotaInternal)).ServeHTTP(w, r)
	})

	// Per-tenant data-plane rate limit read endpoint (issue #305).
	// The edge-ingress TenantRateLimitCache fetcher polls this every
	// TENANT_RATE_LIMIT_FETCH_INTERVAL (default 30s). Same
	// X-Internal-Token trust model as the per-app
	// /rate-limits/{tenantID}/{appName} endpoint at line ~731 and
	// the /quota/{tenantID} endpoint above.
	mux.HandleFunc("GET /api/v1/internal/rate-limit/{tenantID}", func(w http.ResponseWriter, r *http.Request) {
		middleware.InternalAuth(cfg.InternalToken)(http.HandlerFunc(quotaHandler.GetTenantRateLimitInternal)).ServeHTTP(w, r)
	})

	// Per-(tenant,app) L4/TCP public-port assignment (issue #548).
	// The ingress `L4PortCache` polls this every QUOTA_FETCH_INTERVAL
	// so two ingress instances in the same region can converge on
	// the same persisted port for each app. Mounted under
	// InternalAuth like the other /api/v1/internal/* endpoints.
	mux.HandleFunc("GET /api/v1/internal/l4-port/{tenantID}/{appName}", func(w http.ResponseWriter, r *http.Request) {
		middleware.InternalAuth(cfg.InternalToken)(http.HandlerFunc(appHandler.GetL4PortInternal)).ServeHTTP(w, r)
	})

	// Secrets admin endpoints (X-Internal-Token auth).
	secretsHandler := handler.NewSecretsAdminHandler(secretsEnc, envSvc)
	mux.HandleFunc("GET /api/v1/admin/secrets/keys", func(w http.ResponseWriter, r *http.Request) {
		middleware.InternalAuth(cfg.InternalToken)(http.HandlerFunc(secretsHandler.ListKeys)).ServeHTTP(w, r)
	})
	mux.HandleFunc("POST /api/v1/admin/secrets/re-encrypt", func(w http.ResponseWriter, r *http.Request) {
		middleware.InternalAuth(cfg.InternalToken)(http.HandlerFunc(secretsHandler.ReEncrypt)).ServeHTTP(w, r)
	})

	// Internal endpoints (worker-facing, JWT auth).
	internalMux := http.NewServeMux()
	internalMux.HandleFunc("GET /api/internal/download/{deploymentID}", internalHandler.Download)
	internalMux.HandleFunc("POST /api/internal/workers", internalHandler.RegisterWorker)
	internalMux.HandleFunc("GET /api/internal/workers", internalHandler.ListWorkers)
	internalMux.HandleFunc("GET /api/internal/workers/{workerID}/sync", internalHandler.Sync)
	internalMux.HandleFunc("POST /api/internal/logs", internalHandler.IngestLogs)
	internalMux.HandleFunc("POST /api/internal/apps/{appName}/auto-rollback", internalHandler.AutoRollback)
	// Per-tenant worker-token mint endpoint (issue #491). Three
	// rate limiters chain on the route — order matters: per-IP
	// (cheapest) catches scanner floods first; per-worker is keyed
	// off the JWT worker_id and bounds runaway clients; per-tenant
	// is keyed off the request body's tenant_id and bounds a
	// compromised worker trying to enumerate the tenant space.
	workerTokenPerIP := middleware.NewRateLimiter(60, 30)
	workerTokenPerWorker := middleware.NewRateLimiter(10, 5)
	workerTokenPerTenant := middleware.NewRateLimiter(30, 10)
	internalMux.Handle("POST /api/internal/tokens/tenant",
		workerTokenPerIP.Middleware(middleware.ClientIP)(
			workerTokenPerWorker.Middleware(func(r *http.Request) string {
				return middleware.GetWorkerID(r.Context())
			})(
				workerTokenPerTenant.Middleware(workerTokenTenantKey)(
					http.HandlerFunc(internalHandler.MintWorkerToken),
				),
			),
		),
	)
	// Custom-domain routes (issue #83). All three are gated to RoleIngest ONLY.
	internalMux.Handle("GET /api/internal/domains", middleware.RequireWorkerRole(
		middleware.RoleIngest,
	)(http.HandlerFunc(internalHandler.ListDomains)))
	internalMux.Handle("GET /api/internal/tls-allowed", middleware.RequireWorkerRole(
		middleware.RoleIngest,
	)(http.HandlerFunc(internalHandler.TlsAllowed)))
	internalMux.Handle("POST /api/internal/domains/{id}/status", middleware.RequireWorkerRole(
		middleware.RoleIngest,
	)(http.HandlerFunc(internalHandler.UpdateDomainStatus)))

	workerJWTConfig := middleware.WorkerJWTConfig{
		Secret:    cfg.JWT.Secret,
		Issuer:    cfg.JWT.Issuer,
		ActiveKID: cfg.JWT.ActiveKID,
		Keys:      cfg.JWT.Keys,
		// Issue #430: wire the per-worker public-key cache. The
		// loader hits workers.public_key (migration 032) once
		// per cache miss; subsequent inbound requests for the
		// same worker_id short-circuit and re-derive the
		// verification secret via HKDF without touching the DB.
		// EnrollWorker calls Invalidate after a fresh
		// SetPublicKey, so a worker that just re-enrolled
		// doesn't have to wait out the 5-minute TTL.
		WorkerKeyCache: middleware.NewWorkerKeyCache(workerRepo.GetPublicKey),
	}

	// Bootstrap endpoint (issue #104): no auth middleware — uses HMAC
	// signature verification directly in the handler. Rate-limited to
	// 5 req/min per IP.
	if cfg.BootstrapSecret != "" {
		mux.Handle("POST /api/internal/bootstrap",
			bootstrapLimiter.Middleware(middleware.ClientIP)(
				http.HandlerFunc(internalHandler.Bootstrap),
			),
		)

		// Worker-enroll (issue #430): replaces the cluster-leaking
		// /worker-secret endpoint. Protected by BootstrapAuth (phase-1
		// JWT issued above) — the actual authn gating is the Ed25519
		// signature inside the handler, which closes the
		// "bootstrap-JWT alone proves nothing" property.
		//
		// Rate limiters mirror issue #491's /tokens/tenant posture at
		// lines 767-779: per-IP first (cheapest, catches scanners),
		// then per-worker (keyed off the JWT worker_id claim so a
		// single noisy worker can't exhaust the cluster-wide budget).
		bootstrapJWTConfig := middleware.BootstrapJWTConfig{
			BootstrapSecret: cfg.BootstrapSecret,
			Issuer:          "edgecloud-bootstrap",
		}
		workerEnrollPerIP := middleware.NewRateLimiter(60, 30)
		workerEnrollPerWorker := middleware.NewRateLimiter(10, 5)
		mux.Handle("POST /api/internal/worker-bootstrap/enroll",
			workerEnrollPerIP.Middleware(middleware.ClientIP)(
				workerEnrollPerWorker.Middleware(func(r *http.Request) string {
					return middleware.GetWorkerID(r.Context())
				})(
					middleware.BootstrapAuth(bootstrapJWTConfig)(
						http.HandlerFunc(internalHandler.EnrollWorker),
					),
				),
			),
		)
	}

	// Billing webhook (issue #419): no auth middleware — the provider
	// verifies the signature inline. Mirrors the bootstrap mount
	// above. Public surface; protected only by the webhook secret
	// configured under billing.stripe.webhook_secret.
	mux.HandleFunc("POST /api/v1/billing/webhook", billingHandler.StripeWebhook)

	// /api/internal/download is mounted under a separate middleware
	// chain that accepts either a worker JWT OR an X-Internal-Token header.
	downloadMux := http.NewServeMux()
	downloadMux.HandleFunc("GET /api/internal/download/{deploymentID}", internalHandler.Download)
	mux.Handle("GET /api/internal/download/", middleware.InternalOrWorkerAuth(
		workerJWTConfig, cfg.InternalToken,
	)(downloadMux))
	mux.Handle("/api/internal/", middleware.WorkerAuth(workerJWTConfig)(internalMux))

	// Wrap with request ID tracing (outermost) and a body-cap floor.
	wrappedHandler := middleware.RequestID(
		middleware.MaxBodyBytes(service.MaxArtifactSize)(mux),
	)

	return &App{
		Handler:           wrappedHandler,
		Region:            cfg.Region,
		WorkerJWTConfig:   workerJWTConfig,
		ArtifactPath:      cfg.Storage.ArtifactPath,
		WorkerSvc:         workerSvc,
		ReconcileSvc:      reconcileSvc,
		LogGC:             service.NewLogGCService(logEntryRepo, metricsAgg.NewLogGCSink()),
		WorkerGC:          service.NewWorkerGCService(workerRepo),
		DeploymentGC:      service.NewDeploymentGCService(deploymentRepo, artifactStore),
		AuditGC:           service.NewAuditGCService(auditRepo, metricsAgg.NewAuditGCSink()),
		WebhookDeliveryGC: service.NewWebhookDeliveryGCService(webhookRepo, metricsAgg.NewWebhookDeliveryGCSink()),
		AutoscaleEventGC:  service.NewAutoscaleEventGCService(autoscaleEventRepo, metricsAgg.NewAutoscaleEventGCSink()),
		// Preview GC (issue #308). Wired with the deployment
		// repo (for ListExpiredPreviewBlobs +
		// DeleteExpiredPreviewsByIDs) and the artifact store
		// (for blob unlink). See service/preview_gc.go for
		// the run loop and ordering invariants.
		PreviewGC: service.NewPreviewGCService(deploymentRepo, artifactStore, metricsAgg.NewPreviewGCSink(), metricsAgg.NewPreviewBlobFailureRecorder()),
		// Issue #439 idempotency cache sweeper. Wires the same
		// repo the DeploymentService uses for activate / promote /
		// rollback replay. The GC runs in its own goroutine from
		// RunBackground below.
		IdempotencyGC: service.NewIdempotencyKeyGCService(activateIdempotencyRepo),
		// Cache-retry sweep (issue #501). Re-attempts cache pushes
		// that landed in regions_cache_failed. The three getters
		// read the live pusher + regionArtifactCaches map +
		// MaxAttempts on every tick so the sweep honors operator-
		// set config at runtime (cmd/api/main.go calls
		// SetCachePusher and SetRegionArtifactCaches AFTER app.New
		// returns — the getter closures defer the read to the
		// first sweep tick). MaxAttempts is read directly from
		// the typed config because it's loaded once at startup
		// (no post-New setter).
		CacheRetrySweep: service.NewCacheRetrySweepService(
			activeDeploymentRepo,
			deploymentSvc.GetCachePusher,
			deploymentSvc.GetRegionArtifactCaches,
			func() int { return cfg.CacheRetry.MaxAttempts },
			metricsAgg.NewCacheRetrySweepSink(),
		),
		DeploymentSvc:   deploymentSvc,
		AutoscaleSvc:    autoscaleSvc,
		OutboxDrainer:   outboxDrainer,
		MeteringDrainer: meteringDrainer,
		loopHealth:      loopHealth,

		// Captured at construction; see the field doc for why we
		// don't use a getter closure like the pusher / region map.
		cacheRetryIntervalS: cfg.CacheRetry.IntervalS,
	}
}

// newLoopHealth constructs the per-loop liveness tracker used by
// RunBackground and the /health handler. The staleness threshold is
// tunable via LOOP_STALE_AFTER (default loophealth.DefaultStaleAfter).
func newLoopHealth() *loophealth.Tracker {
	tr := loophealth.NewTracker()
	if d := parseDurationEnv("LOOP_STALE_AFTER", 0); d > 0 {
		tr.SetStaleAfter(d)
	}
	return tr
}

// RunBackground starts all background goroutines. Call once the HTTP server
// is running. Cancelling ctx tears all goroutines down cleanly.
//
// Every loop is wrapped in loophealth.Tracker.Run / RunErr, which spawns
// the body in its own goroutine, recovers panics, logs the stack via
// the loop's per-service prefix, and bumps a per-loop counter that the
// /health handler surfaces. Without this, a panic in the heartbeat
// consumer (issue #443's most-critical loop) would kill the goroutine
// silently while /health kept reporting "ok".
func (a *App) RunBackground(ctx context.Context) {
	logPrintf := log.Printf // local alias for the stdlib log adapter

	// Log retention GC. Tunable via env (LOG_GC_INTERVAL, LOG_RETENTION).
	logGCInterval := parseDurationEnv("LOG_GC_INTERVAL", time.Hour)
	logRetention := parseDurationEnv("LOG_RETENTION", 7*24*time.Hour)
	a.loopHealth.Run(ctx, "log_gc", "log_gc: ", logPrintf, func(c context.Context) {
		a.LogGC.Run(c, logGCInterval, logRetention)
	})

	// Retention GC trio (issue #574). One per append-only table —
	// audit_logs, webhook_deliveries, autoscale_events. Each runs
	// an immediate-first-sweep then ticks at its interval; refuses to
	// run with non-positive values. Defaults match the pre-baked
	// retention windows documented in the GC service files.
	auditGCInterval := parseDurationEnv("AUDIT_GC_INTERVAL", time.Hour)
	auditRetention := parseDurationEnv("AUDIT_RETENTION", 90*24*time.Hour)
	a.loopHealth.Run(ctx, "audit_gc", "audit_gc: ", logPrintf, func(c context.Context) {
		a.AuditGC.Run(c, auditGCInterval, auditRetention)
	})

	webhookGCInterval := parseDurationEnv("WEBHOOK_DELIVERY_GC_INTERVAL", time.Hour)
	webhookRetention := parseDurationEnv("WEBHOOK_DELIVERY_RETENTION", 30*24*time.Hour)
	a.loopHealth.Run(ctx, "webhook_delivery_gc", "webhook_delivery_gc: ", logPrintf, func(c context.Context) {
		a.WebhookDeliveryGC.Run(c, webhookGCInterval, webhookRetention)
	})

	autoscaleGCInterval := parseDurationEnv("AUTOSCALE_EVENT_GC_INTERVAL", time.Hour)
	autoscaleRetention := parseDurationEnv("AUTOSCALE_EVENT_RETENTION", 14*24*time.Hour)
	a.loopHealth.Run(ctx, "autoscale_event_gc", "autoscale_event_gc: ", logPrintf, func(c context.Context) {
		a.AutoscaleEventGC.Run(c, autoscaleGCInterval, autoscaleRetention)
	})

	// Periodic full-state reconcile (issue #53). Tunable via RECONCILE_INTERVAL.
	reconcileInterval := parseDurationEnv("RECONCILE_INTERVAL", 5*time.Minute)
	a.loopHealth.Run(ctx, "reconcile", "reconcile: ", logPrintf, func(c context.Context) {
		a.ReconcileSvc.Run(c, reconcileInterval)
	})

	// Stale worker GC. Tunable via env (WORKER_GC_INTERVAL, WORKER_MAX_AGE).
	workerGCInterval := parseDurationEnv("WORKER_GC_INTERVAL", 5*time.Minute)
	workerMaxAge := parseDurationEnv("WORKER_MAX_AGE", 15*time.Minute)
	a.loopHealth.Run(ctx, "worker_gc", "worker_gc: ", logPrintf, func(c context.Context) {
		a.WorkerGC.Run(c, workerGCInterval, workerMaxAge)
	})

	// Deployment GC. Deletes old deployments that are no longer active.
	// Tunable via env (DEPLOY_GC_INTERVAL, DEPLOY_RETENTION).
	deployGCInterval := parseDurationEnv("DEPLOY_GC_INTERVAL", 1*time.Hour)
	deployRetention := parseDurationEnv("DEPLOY_RETENTION", 7*24*time.Hour)
	a.loopHealth.Run(ctx, "deployment_gc", "deployment_gc: ", logPrintf, func(c context.Context) {
		a.DeploymentGC.Run(c, deployGCInterval, deployRetention)
	})

	// NATS heartbeat subscriber for worker lifecycle management.
	// SubscribeHeartbeats spawns two inner goroutines itself (channel
	// monitor + drain). The inner drain also has its own recover
	// helper in service/worker.go (heartbeatRecover) since the outer
	// wrapper here can't see panics inside it.
	a.loopHealth.RunErr(ctx, "heartbeat", "heartbeat: ", logPrintf, func(c context.Context) error {
		return a.WorkerSvc.SubscribeHeartbeats(c)
	})

	// Preview GC (issue #308). Reclaims expired preview
	// deployments (rows + their artifact blobs). The retention
	// env var is currently a no-op on the GC itself (per-row
	// expiry is stamped at deploy time via PreviewOpts.ExpiresAt)
	// but is read here for parity with the other GC knobs and
	// as a forward-compatible hook.
	previewGCInterval := parseDurationEnv("PREVIEW_GC_INTERVAL", 1*time.Hour)
	previewRetention := parseDurationEnv("PREVIEW_RETENTION", 7*24*time.Hour)
	go a.PreviewGC.Run(ctx, previewGCInterval, previewRetention)

	// Idempotency-Key cache GC (issue #439 follow-up). Deletes
	// active_deployment_idempotency_keys rows older than the
	// cache TTL (24h — matches repository.IdempotencyTTL). Uses
	// the same raw-goroutine shape as PreviewGC: the sweep is
	// expected to run forever and a panic in the loop would be
	// surfaced through the loopHealth wrapper; for now we
	// follow the established convention.
	idempotencyGCInterval := parseDurationEnv("IDEMPOTENCY_GC_INTERVAL", 1*time.Hour)
	go a.IdempotencyGC.Run(ctx, idempotencyGCInterval, repository.IdempotencyTTL)

	// Cache-retry sweep (issue #501). Re-attempts per-region
	// artifact-cache pushes stranded in regions_cache_failed.
	// Uses the same raw-goroutine shape as PreviewGC: the sweep
	// is "best-effort" — a transient DB blip logs and returns,
	// the next tick re-attempts — and is not on the loopHealth
	// critical path (the worker still serves requests by pulling
	// from /api/internal/download, so a stuck sweep is an
	// observability hit but not a service-level outage).
	//
	// DB-load note: the sweep makes up to 3 DB calls per row
	// (AppendRegionsCacheState for success + AppendRegionsCacheState
	// for still-failing + RemoveFromCacheFailed for configMissing).
	// At gcBatchSize=10_000 rows per batch and gcMaxBatches=1000
	// per tick (10M rows worst-case), lowering the interval
	// proportionally multiplies the per-minute DB load — the
	// default 5m keeps a worst-case tick under the row cap; an
	// operator who lowers the interval during an incident should
	// expect ~interval/5m × default DB pressure.
	// Use the typed config (issue #501 acceptance item: "config in
	// internal/config/config.go"). The interval is integer seconds
	// in the config (CacheRetry.IntervalS) — converted to a
	// time.Duration for the Run signature. parseDurationEnv is
	// intentionally NOT used here so the config struct is the
	// single source of truth (callers in cmd/api/main.go can
	// override via REGION_CACHE_RETRY_INTERVAL env var, which
	// config.Load translates to CacheRetry.IntervalS).
	//
	// We capture the interval at construction (via the App's
	// stored interval) so RunBackground doesn't need to read
	// the config struct post-construction. The interval is
	// fixed for the process lifetime — same as
	// previewGCInterval / logGCInterval, which are also captured
	// into locals at RunBackground time.
	cacheRetryInterval := time.Duration(a.cacheRetryIntervalS) * time.Second
	go a.CacheRetrySweep.Run(ctx, cacheRetryInterval)

	// Outbox drainer (issue #42). The drainer is the sole owner of
	// `task_update` NATS publishes for activate / rollback. Its tick
	// interval + batch size + max attempts are tunable via env (see
	// OUTBOX_DRAIN_INTERVAL / OUTBOX_BATCH_SIZE / OUTBOX_MAX_ATTEMPTS
	// in NewApp). The drainer is multi-instance safe via FOR UPDATE
	// SKIP LOCKED + a 30s claimed_until window, so spinning up extra
	// replicas just for the drainer is fine.
	a.loopHealth.Run(ctx, "outbox_drainer", "outbox_drainer: ", logPrintf, func(c context.Context) {
		a.OutboxDrainer.Run(c)
	})

	// Metering drainer (issue #485). Mirrors the outbox drainer's
	// loop shape: claims a batch, dispatches each row through the
	// configured MeteringProvider, marks processed. Rate-card
	// zero-fallback happens inside the drainer's processRow so a
	// fresh install with empty Rates ticks cleanly without dispatch.
	// On a fresh install with no rows, Tick is a no-op — no log noise.
	a.loopHealth.Run(ctx, "metering_drainer", "metering_drainer: ", logPrintf, func(c context.Context) {
		a.MeteringDrainer.Run(c)
	})

	// Cluster autoscaler (issue #85). No-op when cfg.Autoscale.Enabled
	// is false — Subscribe returns nil immediately. The autoscale
	// package uses log/slog rather than stdlib log, so we route the
	// recovered-panic log through slog.Default() with structured attrs.
	// slog.Logger.Error treats msg as a literal — the panic value and
	// stack must be pre-formatted and passed as a "err" attr (review
	// finding #2).
	if a.AutoscaleSvc != nil {
		slogErr := func(format string, args ...any) {
			slog.Default().Error("autoscale: loop panic recovered",
				"loop", "autoscale",
				"err", fmt.Sprintf(format, args...),
			)
		}
		a.loopHealth.RunErr(ctx, "autoscale", "autoscale: ", slogErr, func(c context.Context) error {
			return a.AutoscaleSvc.Subscribe(c)
		})
	}
}

// stableWindowFromEnv reads STABLE_WINDOW_SECONDS from the environment,
// returning 0 (= "use service default") when unset or unparseable.
func stableWindowFromEnv() time.Duration {
	raw := os.Getenv("STABLE_WINDOW_SECONDS")
	if raw == "" {
		return 0
	}
	secs, err := strconv.Atoi(raw)
	if err != nil || secs < 0 {
		log.Printf("STABLE_WINDOW_SECONDS=%q invalid; using service default", raw)
		return 0
	}
	return time.Duration(secs) * time.Second
}

// parseDurationEnv reads a duration-valued env var or returns the default.
func parseDurationEnv(envName string, def time.Duration) time.Duration {
	v := os.Getenv(envName)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		log.Printf("%s=%q is not a valid positive duration; using default %s", envName, v, def)
		return def
	}
	return d
}

// parseIntEnv reads a positive integer-valued env var or returns the
// default. Mirrors parseDurationEnv's contract: empty / unparseable /
// non-positive values fall back to `def` with a log line. Used by
// OUTBOX_BATCH_SIZE and OUTBOX_MAX_ATTEMPTS in app.go (issue #42).
func parseIntEnv(envName string, def int) int {
	v := os.Getenv(envName)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Printf("%s=%q is not a valid positive integer; using default %d", envName, v, def)
		return def
	}
	return n
}

// loadKeyring constructs a *signing.Keyring from the validated config.
// Precedence matches `signing.LoadFromEnv`: keyring file (KeyringPath)
// > inline keyring (Keyring) > legacy single-key file (KeyPath) >
// inline single-key (Key). The legacy forms are kept as a one-release
// deprecation fallback and produce a 1-entry keyring with kid
// "default" (a deprecation warning is logged here so operators see
// it from one place). The config validator has already rejected the
// case where all four are empty, so at least one path here is taken
// — but this function is also reachable from tests that bypass Load
// (the bundled-config regression guard bypasses Load and asserts the
// error is logged in a stable way). The empty-guard here returns the
// same sentinel error the validator would emit, so callers see one
// consistent message regardless of which layer caught it.
func loadKeyring(cfg *config.SigningConfig) (*signing.Keyring, error) {
	if cfg.KeyPath == "" && cfg.Key == "" && cfg.KeyringPath == "" && cfg.Keyring == "" {
		return nil, fmt.Errorf("%w: EDGE_SIGNING_KEYRING_PATH (or EDGE_SIGNING_KEYRING, EDGE_SIGNING_KEY_PATH, EDGE_SIGNING_KEY) is required (issue #307 PR1)", signing.ErrInvalidKey)
	}
	if cfg.KeyringPath != "" {
		return signing.LoadKeyringFromFile(cfg.KeyringPath, cfg.KeyID)
	}
	if cfg.Keyring != "" {
		return signing.LoadKeyringFromInline(cfg.Keyring, cfg.KeyID)
	}
	// Legacy single-key fallback (deprecated).
	if cfg.KeyID != "" && cfg.KeyID != signing.DefaultKeyID {
		return nil, fmt.Errorf("%w: legacy single-key cannot satisfy EDGE_SIGNING_KEY_ID=%q; use EDGE_SIGNING_KEYRING", signing.ErrInvalidKey, cfg.KeyID)
	}
	log.Printf("signing: EDGE_SIGNING_KEY[_PATH] is deprecated; use EDGE_SIGNING_KEYRING[_PATH] (issue #307 PR1)")
	if cfg.KeyPath != "" {
		s, err := signing.LoadFromFile(cfg.KeyPath, signing.DefaultKeyID)
		if err != nil {
			return nil, err
		}
		return signing.KeyringFromSigner(s, signing.DefaultKeyID), nil
	}
	s, err := signing.LoadFromRaw([]byte(cfg.Key), signing.DefaultKeyID)
	if err != nil {
		return nil, err
	}
	return signing.KeyringFromSigner(s, signing.DefaultKeyID), nil
}

// newBillingProvider returns the BillingProvider implementation
// selected by cfg.Billing.Provider. validateBillingConfig has
// already enforced the per-provider prerequisites; here we just
// translate the config block into a typed provider.
//
// Adding a new provider = append a new case here + add a new
// sub-package under internal/billing/<name>/. The interface
// boundary means no service / handler code changes.
func newBillingProvider(cfg config.BillingConfig) billing.BillingProvider {
	switch cfg.Provider {
	case "stripe":
		return stripe.New(billing.StripeConfig{
			SecretKey:      cfg.Stripe.SecretKey,
			WebhookSecret:  cfg.Stripe.WebhookSecret,
			PublishableKey: cfg.Stripe.PublishableKey,
			PriceIDs:       cfg.Stripe.PriceIDs,
		})
	default:
		// "noop" or empty (defaulted to "noop" by validateBillingConfig
		// in dev|test environments).
		return noop.New()
	}
}

// newMeteringProvider mirrors newBillingProvider above but selects a
// MeteringProvider. The MeteringProvider seam is separate from the
// BillingProvider seam because their lifecycles differ (fire-and-
// record vs request/response) — see internal/billing/metering_provider.go
// for the full rationale.
//
// Note: unlike newBillingProvider, this factory does NOT fail-closed
// when metering.provider=stripe but no MeterSubscriptionItemIDs are
// configured. The metering path is opt-in per dimension — an operator
// who enables Stripe metering but forgets to wire IDs gets a
// per-row terminal log + MarkProcessed (so the row stops cycling)
// rather than a process-level crash. The `subscribed` test will
// catch the config gap; the production path keeps serving traffic.
func newMeteringProvider(cfg config.BillingConfig) billing.MeteringProvider {
	switch cfg.Metering.Provider {
	case "stripe":
		meterEventNames := map[domain.MeterKind]string{}
		for k, v := range cfg.Metering.MeterEventNames {
			meterEventNames[domain.MeterKind(k)] = v
		}
		return stripe.NewMetering(billing.StripeConfig{
			SecretKey: cfg.Stripe.SecretKey,
			// WebhookSecret / PublishableKey / PriceIDs are not
			// used by the metering path; left blank. This keeps the
			// constructor happy without exposing them via env on
			// the metering-only install shape.
			MeterSubscriptionItemIDs: cfg.Metering.SubscriptionItemIDs,
		}, meterEventNames)
	default:
		// "noop" or empty (no production gate — metering is opt-in).
		return noop.NewMetering()
	}
}

// workerTokenTenantKeyFromBody is the package-level helper extracted
// from the inline closure at the route mount (issue #491 + PR
// review). It serves two roles:
//
//  1. Production use: bound to POST /api/internal/tokens/tenant as
//     the key-extractor for the per-tenant RateLimiter.
//  2. Test use: exercised directly by
//     TestWorkerTokenTenantKeyFromBody in app_test.go so the
//     malformed-body / missing-field / oversize-body / nested-shape
//     edge cases are pinned without standing up the full
//     WorkerAuth + InternalHandler chain.
//
// Buffer size (128 bytes) covers `"tenant_id":"<value>"` for any
// legal tenant_id (up to 64 chars + 16 chars of JSON syntax +
// margin). Anything larger is truncated at the peek; the handler's
// own validator rejects tenant_id > 64 chars upstream, so a
// truncated peek that misses a >64-char value still falls through
// to the worker_id fallback (which is the safe behavior).
//
// Fallback path: when the body peek cannot produce a tenant_id
// (empty body / malformed JSON / missing field / oversize), we
// return the worker_id from the JWT context. This routes
// malformed-body floods into the per-worker (10/5) bucket instead
// of the empty-key pass-through in RateLimiter.Middleware
// (middleware/ratelimit.go:135-139).
func workerTokenTenantKeyFromBody(r *http.Request) string {
	var buf [128]byte
	n, _ := io.ReadFull(io.LimitReader(r.Body, int64(len(buf))), buf[:])
	r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(buf[:n]), r.Body))
	var peek struct {
		TenantID string `json:"tenant_id"`
	}
	if n == 0 {
		return middleware.GetWorkerID(r.Context())
	}
	if err := json.Unmarshal(buf[:n], &peek); err != nil || peek.TenantID == "" {
		return middleware.GetWorkerID(r.Context())
	}
	return peek.TenantID
}
