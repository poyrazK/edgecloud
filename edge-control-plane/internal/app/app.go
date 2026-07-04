// Package app assembles the complete dependency graph of the edge-cloud
// control plane. It is the only package that imports every internal
// sub-package; every other package communicates through narrow interfaces.
package app

import (
	"context"
	"embed"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/autoscale"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/jmoiron/sqlx"
)

// App holds all assembled dependencies. After New returns, the caller has
// a fully-wired application ready to serve HTTP traffic and run background
// goroutines.
type App struct {
	// Handler is the complete http.Handler with all middleware and routes applied.
	Handler http.Handler

	// Region, JWTSecret, JWTIssuer, and ArtifactPath are exposed so
	// main.go can mint the ingress service token at startup.
	Region       string
	JWTSecret    string
	JWTIssuer    string
	ArtifactPath string

	// Background service references. RunBackground starts all three.
	WorkerSvc    *service.WorkerService
	ReconcileSvc *service.ReconcileService
	LogGC        *service.LogGCService
	WorkerGC     *service.WorkerGCService
	AutoscaleSvc *autoscale.Service
}

// New creates a fully-wired App from the given infrastructure dependencies.
// It instantiates all 11 repositories, 12 services, 16 handlers, middleware,
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

	// ── Services ──────────────────────────────────────────────────
	tenantSvc := service.NewTenantService(db, tenantRepo, quotaRepo, apiKeyRepo)
	apiKeySvc := service.NewAPIKeyService(apiKeyRepo)
	appSvc := service.NewAppService(
		db, appRepo, deploymentRepo, activeDeploymentRepo, appEnvRepo, artifactStore, quotaRepo,
	)
	deploymentSvc := service.NewDeploymentService(
		db, deploymentRepo, activeDeploymentRepo, appEnvRepo,
		quotaRepo, tenantRepo, artifactStore, publisher, cfg.Region,
	)
	deploymentSvc.SetAppService(appSvc)
	envSvc := service.NewEnvService(appEnvRepo)
	metricsAgg := service.NewMetricsAggregator()
	workerSvc := service.NewWorkerService(
		workerRepo, quotaRepo, activeDeploymentRepo,
		publisher.Conn(), stableWindowFromEnv(), metricsAgg,
	)
	clusterSvc := service.NewClusterService(workerRepo, autoscaleEventRepo)
	migrationSvc := service.NewMigrationService(
		deploymentRepo, artifactStore,
		cfg.Migration.EdgeMigratePath, cfg.Migration.WasiSdkPath, cfg.Migration.RustcPath,
	)
	trafficSvc := service.NewTrafficService(
		db, trafficSplitRepo, deploymentRepo, activeDeploymentRepo,
		appEnvRepo, tenantRepo, quotaRepo, publisher, cfg.Region,
	)
	// PR #195 dropped the deploymentRepo arg — the reconcile now
	// pulls deployment hash + regions via ListByTenantWithDeployment
	// in a single round trip (N+1 elimination).
	reconcileSvc := service.NewReconcileService(
		tenantRepo, activeDeploymentRepo, appEnvRepo, quotaRepo, publisher, cfg.Region,
	)

	// Wire secrets encryption (if configured).
	secretsEnc, encErr := service.NewSecretEncryptor(cfg.SecretsMasterKey)
	if encErr != nil {
		log.Fatalf("failed to create secrets encryptor: %v", encErr)
	}
	if secretsEnc != nil {
		envSvc.SetSecretEncryptor(secretsEnc)
		deploymentSvc.SetEnvService(envSvc)
		trafficSvc.SetEnvDecrypter(secretsEnc)
		reconcileSvc.SetEnvDecrypter(secretsEnc)
	}

	webhookRepo := repository.NewWebhookRepository(db)
	webhookSvc := service.NewWebhookService(webhookRepo)
	deploymentSvc.SetWebhookService(webhookSvc)
	webhookHandler := handler.NewWebhookHandler(webhookSvc)

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
		})
	}

	// ── Handlers ──────────────────────────────────────────────────
	auditRepo := repository.NewAuditRepository(db)
	auditor := service.NewAuditor(auditRepo)
	handler.DefaultAuditor = auditor

	tenantHandler := handler.NewTenantHandler(tenantSvc)
	apiKeyHandler := handler.NewAPIKeyHandler(apiKeySvc)
	deploymentHandler := handler.NewDeploymentHandler(deploymentSvc, workerSvc, trafficSvc)
	envHandler := handler.NewEnvHandler(envSvc)
	// PR #195 / commit 2d61f94 (fold SetSyncBuilder into NewInternalHandler)
	// passes reconcileSvc as both arg 5 (syncRequester) and arg 6
	// (syncPayloadBuilder) — same *service.ReconcileService satisfies
	// both interfaces.
	internalHandler := handler.NewInternalHandler(deploymentSvc, workerSvc, domainSvc, logEntryRepo, reconcileSvc, reconcileSvc, cfg.Region)
	appHandler := handler.NewAppHandler(appSvc)
	authHandler := handler.NewAuthHandler(tenantSvc, apiKeySvc)
	clusterHandler := handler.NewClusterHandler(clusterSvc)
	quotaHandler := handler.NewQuotaHandler(tenantSvc)
	trafficHandler := handler.NewTrafficHandler(trafficSvc)
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

	// Health check
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"status":"ok"}`)); err != nil {
			log.Printf("Health check: failed to write response: %v", err)
		}
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
	mux.HandleFunc("DELETE /api/admin/apps/{appName}", redirectTo("/api/v1/admin/apps/"+"{appName}"))
	mux.HandleFunc("GET /api/admin/cluster", redirectTo("/api/v1/admin/cluster"))
	mux.HandleFunc("GET /api/internal/download/{deploymentID}", redirectTo("/api/internal/download/"+"{deploymentID}"))
	mux.HandleFunc("POST /api/internal/workers", redirectTo("/api/internal/workers"))
	mux.HandleFunc("GET /api/internal/workers", redirectTo("/api/internal/workers"))

	// Protected API routes
	api := http.NewServeMux()
	api.HandleFunc("POST /api/v1/deploy/{appName}", deploymentHandler.Deploy)
	api.HandleFunc("POST /api/v1/migrate", migrationHandler.Migrate)
	api.HandleFunc("POST /api/v1/migrate-tree", migrationHandler.MigrateTree)
	api.HandleFunc("GET /api/v1/status/{deploymentID}", deploymentHandler.GetStatus)
	api.HandleFunc("GET /api/v1/list/{appName}", deploymentHandler.List)
	api.HandleFunc("POST /api/v1/apps/{appName}/activate/{deploymentID}", deploymentHandler.Activate)
	api.HandleFunc("POST /api/v1/apps/{appName}/rollback", deploymentHandler.Rollback)
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

	// Admin routes (require owner role)
	admin := http.NewServeMux()
	admin.HandleFunc("GET /api/v1/admin/tenants", tenantHandler.List)
	admin.HandleFunc("POST /api/v1/admin/tenants", tenantHandler.Create)
	admin.HandleFunc("GET /api/v1/admin/tenants/{tenantID}", tenantHandler.Get)
	admin.HandleFunc("PUT /api/v1/admin/tenants/{tenantID}", tenantHandler.Update)
	admin.HandleFunc("DELETE /api/v1/admin/tenants/{tenantID}", tenantHandler.Delete)
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
		Secret: cfg.JWT.Secret,
		Issuer: cfg.JWT.Issuer,
	}
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
		Handler:      wrappedHandler,
		Region:       cfg.Region,
		JWTSecret:    cfg.JWT.Secret,
		JWTIssuer:    cfg.JWT.Issuer,
		ArtifactPath: cfg.Storage.ArtifactPath,
		WorkerSvc:    workerSvc,
		ReconcileSvc: reconcileSvc,
		LogGC:        service.NewLogGCService(logEntryRepo),
		WorkerGC:     service.NewWorkerGCService(workerRepo),
		AutoscaleSvc: autoscaleSvc,
	}
}

// RunBackground starts all background goroutines. Call once the HTTP server
// is running. Cancelling ctx tears all goroutines down cleanly.
func (a *App) RunBackground(ctx context.Context) {
	// NATS heartbeat subscriber for worker lifecycle management
	go func() {
		if err := a.WorkerSvc.SubscribeHeartbeats(ctx); err != nil {
			log.Printf("Worker heartbeat subscription error: %v", err)
		}
	}()

	// Log retention GC. Tunable via env (LOG_GC_INTERVAL, LOG_RETENTION).
	logGCInterval := parseDurationEnv("LOG_GC_INTERVAL", time.Hour)
	logRetention := parseDurationEnv("LOG_RETENTION", 7*24*time.Hour)
	go a.LogGC.Run(ctx, logGCInterval, logRetention)

	// Periodic full-state reconcile (issue #53). Tunable via RECONCILE_INTERVAL.
	reconcileInterval := parseDurationEnv("RECONCILE_INTERVAL", 5*time.Minute)
	go a.ReconcileSvc.Run(ctx, reconcileInterval)

	// Stale worker GC. Tunable via env (WORKER_GC_INTERVAL, WORKER_MAX_AGE).
	workerGCInterval := parseDurationEnv("WORKER_GC_INTERVAL", 5*time.Minute)
	workerMaxAge := parseDurationEnv("WORKER_MAX_AGE", 15*time.Minute)
	go a.WorkerGC.Run(ctx, workerGCInterval, workerMaxAge)

	// Cluster autoscaler (issue #85). No-op when cfg.Autoscale.Enabled
	// is false — Subscribe returns nil immediately.
	if a.AutoscaleSvc != nil {
		go func() {
			if err := a.AutoscaleSvc.Subscribe(ctx); err != nil {
				log.Printf("autoscale subscription error: %v", err)
			}
		}()
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
