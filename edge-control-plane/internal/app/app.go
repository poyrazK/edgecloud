// Package app assembles the complete dependency graph of the edge-cloud
// control plane. It is the only package that imports every internal
// sub-package; every other package communicates through narrow interfaces.
package app

import (
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
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
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
	WorkerSvc    *service.WorkerService
	ReconcileSvc *service.ReconcileService
	LogGC        *service.LogGCService
	WorkerGC     *service.WorkerGCService
	DeploymentGC *service.DeploymentGCService
	// PreviewGC (issue #308) reclaims expired preview deployments
	// and their artifact blobs. Tunable via env
	// (PREVIEW_GC_INTERVAL, PREVIEW_RETENTION). Disabled by
	// invalid interval/retention — PreviewGCService.Run refuses
	// to start with non-positive values.
	PreviewGC *service.PreviewGCService
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

	// loopHealth tracks liveness of every background goroutine spawned
	// by RunBackground (heartbeat, log_gc, reconcile, worker_gc,
	// deployment_gc, autoscale). It also drives the per-loop map on
	// /health so operators can see degraded state without scraping
	// logs. See internal/loophealth for the package contract. Related
	// to issue #443.
	loopHealth *loophealth.Tracker
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
	// Outbox (issue #42): durable-publish queue for `task_update`
	// NATS messages. Rows are written in the same tx as the
	// active_deployments mutation; the OutboxDrainer (below) relays
	// them after commit.
	outboxRepo := repository.NewOutboxRepository(db)

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

	tenantSvc := service.NewTenantService(db, tenantRepo, quotaRepo, apiKeyRepo)
	apiKeySvc := service.NewAPIKeyService(apiKeyRepo)
	appSvc := service.NewAppService(
		db, appRepo, deploymentRepo, activeDeploymentRepo, appEnvRepo, artifactStore, quotaRepo,
	)
	deploymentSvc := service.NewDeploymentService(
		db, deploymentRepo, activeDeploymentRepo, appEnvRepo,
		quotaRepo, tenantRepo, outboxRepo, artifactStore, publisher, cfg.Region, keyring,
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
	metricsAgg := service.NewMetricsAggregator()
	// loopHealth tracks liveness of every background goroutine. It must
	// be constructed before the services that need to feed it (so the
	// heartbeat drain and autoscaler can call Beat/RecordPanic), and
	// the same instance is reused by the /health closure and the App
	// struct (issue #443 review findings #3 and #4).
	loopHealth := newLoopHealth()
	workerSvc := service.NewWorkerService(
		db, workerRepo, quotaRepo, activeDeploymentRepo, tenantRepo,
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

	migrationHandler := handler.NewMigrationHandler(migrationSvc)
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

	tenantHandler := handler.NewTenantHandler(tenantSvc)
	apiKeyHandler := handler.NewAPIKeyHandler(apiKeySvc)
	deploymentHandler := handler.NewDeploymentHandler(deploymentSvc, workerSvc, trafficSvc, artifactStore, cfg.Migration.Wasm2CwasmPath)
	envHandler := handler.NewEnvHandler(envSvc)
	// PR #195 / commit 2d61f94 (fold SetSyncBuilder into NewInternalHandler)
	// passes reconcileSvc as both arg 5 (syncRequester) and arg 6
	// (syncPayloadBuilder) — same *service.ReconcileService satisfies
	// both interfaces.
	internalHandler := handler.NewInternalHandler(deploymentSvc, workerSvc, domainSvc, logEntryRepo, reconcileSvc, reconcileSvc, cfg.Region, cfg.BootstrapSecret, cfg.JWT.Secret)
	appHandler := handler.NewAppHandler(appSvc)
	authHandler := handler.NewAuthHandler(tenantSvc, apiKeySvc)
	clusterHandler := handler.NewClusterHandler(clusterSvc)
	quotaHandler := handler.NewQuotaHandler(tenantSvc)
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
	api.HandleFunc("POST /api/v1/apps/{appName}", appHandler.Create)
	api.HandleFunc("GET /api/v1/apps", appHandler.List)
	api.HandleFunc("GET /api/v1/apps/{appName}", appHandler.Get)
	api.HandleFunc("PUT /api/v1/apps/{appName}", appHandler.Update)
	api.HandleFunc("POST /api/v1/keys", apiKeyHandler.Create)
	api.HandleFunc("GET /api/v1/apps/{appName}/ingress", deploymentHandler.AppIngress)
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

		// Worker-secret: protected by BootstrapAuth (separate key from WorkerAuth).
		bootstrapJWTConfig := middleware.BootstrapJWTConfig{
			BootstrapSecret: cfg.BootstrapSecret,
			Issuer:          "edgecloud-bootstrap",
		}
		mux.Handle("GET /api/internal/worker-secret",
			middleware.BootstrapAuth(bootstrapJWTConfig)(
				http.HandlerFunc(internalHandler.WorkerSecret),
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
		Handler:         wrappedHandler,
		Region:          cfg.Region,
		WorkerJWTConfig: workerJWTConfig,
		ArtifactPath:    cfg.Storage.ArtifactPath,
		WorkerSvc:       workerSvc,
		ReconcileSvc:    reconcileSvc,
		LogGC:           service.NewLogGCService(logEntryRepo),
		WorkerGC:        service.NewWorkerGCService(workerRepo),
		DeploymentGC:    service.NewDeploymentGCService(deploymentRepo, artifactStore),
		// Preview GC (issue #308). Wired with the deployment
		// repo (for ListExpiredPreviewBlobs +
		// DeleteExpiredPreviewsByIDs) and the artifact store
		// (for blob unlink). See service/preview_gc.go for
		// the run loop and ordering invariants.
		PreviewGC:     service.NewPreviewGCService(deploymentRepo, artifactStore),
		DeploymentSvc: deploymentSvc,
		AutoscaleSvc:  autoscaleSvc,
		OutboxDrainer: outboxDrainer,
		loopHealth:    loopHealth,
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
