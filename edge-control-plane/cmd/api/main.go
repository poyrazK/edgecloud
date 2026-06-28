package main

import (
	"context"
	"embed"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	natsio "github.com/nats-io/nats.go"
)

//go:generate cp ../../docs/api/openapi.yaml docs/api/openapi.yaml

// openAPISpec is embedded at build time. The canonical source is
// docs/api/openapi.yaml (the repo root). The copy at cmd/api/docs/api/openapi.yaml
// exists solely for //go:embed (which resolves paths relative to this file).
// Run `go generate` after updating the spec to keep them in sync.
var openAPISpec embed.FS

// stableWindowFromEnv reads STABLE_WINDOW_SECONDS from the
// environment, returning 0 (= "use service default") when unset or
// unparseable. Defaults live in service.NewWorkerService; this
// helper is purely an env → Duration bridge so cmd/api/main.go
// doesn't have to repeat the strconv + log-on-error dance inline.
//
// The env var is the operator-facing knob for the auto-rollback
// stability window. Lowering it (e.g. to 5s) makes freshly-activated
// deployments eligible to be promoted to last_good faster, at the
// cost of being more sensitive to transient blips. The default
// (30s) matches the worker's heartbeat_interval_secs default so
// promotion can fire on the second heartbeat after activation.
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

func main() {
	// Load configuration
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize database
	db, err := repository.NewDB(cfg.Database.DSN())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	// Initialize repositories
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

	// Initialize NATS publisher
	publisher, err := nats.NewNATSPublisher(cfg.NATS.URL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer publisher.Close()

	// Ensure the task stream exists with the shape workers expect
	// (workqueue retention, 24h max age, RF=3). Idempotent — workers also
	// call ensure_task_stream at startup, but having the control plane
	// own the stream creation makes startup ordering deterministic.
	// See issue #86.
	if err := publisher.EnsureStream(nats.StreamConfig{
		Name:      nats.TaskStreamName,
		Subjects:  []string{"edgecloud.tasks.>"},
		Retention: natsio.WorkQueuePolicy,
		MaxAge:    24 * time.Hour,
		Replicas:  3,
	}); err != nil {
		log.Fatalf("Failed to ensure NATS stream: %v", err)
	}

	// Initialize artifact storage via the backend factory. An empty
	// ArtifactBackend selects the filesystem implementation so existing
	// deployments need no config change. The per-backend fields
	// (S3*, Peer*) are forwarded as-is; the factory and per-backend
	// constructors are responsible for validating them. Load() already
	// rejected unknown backends and missing required fields at startup.
	//
	// Pass cfg.Storage directly — storage.New takes config.StorageConfig
	// so there's no drift surface between the operator-facing struct and
	// what the constructors see.
	artifactStore, err := storage.New(context.Background(), cfg.Storage)
	if err != nil {
		log.Fatalf("Failed to initialize artifact storage: %v", err)
	}

	// Initialize services
	tenantSvc := service.NewTenantService(db, tenantRepo, quotaRepo, apiKeyRepo)
	apiKeySvc := service.NewAPIKeyService(apiKeyRepo)
	appSvc := service.NewAppService(db, appRepo, deploymentRepo, activeDeploymentRepo, appEnvRepo, artifactStore, quotaRepo)
	deploymentSvc := service.NewDeploymentService(
		db, deploymentRepo, activeDeploymentRepo, appEnvRepo, quotaRepo, tenantRepo, artifactStore, publisher, cfg.Region,
	)
	deploymentSvc.SetAppService(appSvc)
	envSvc := service.NewEnvService(appEnvRepo)
	metricsAgg := service.NewMetricsAggregator()
	workerSvc := service.NewWorkerService(workerRepo, quotaRepo, activeDeploymentRepo, publisher.Conn(), stableWindowFromEnv(), metricsAgg)
	clusterSvc := service.NewClusterService(workerRepo)
	migrationSvc := service.NewMigrationService(deploymentRepo, artifactStore, cfg.Migration.EdgeMigratePath, cfg.Migration.WasiSdkPath, cfg.Migration.RustcPath)
	trafficSvc := service.NewTrafficService(db, trafficSplitRepo, deploymentRepo, activeDeploymentRepo, appEnvRepo, tenantRepo, quotaRepo, publisher, cfg.Region)
	// ReconcileService is constructed here (alongside the other
	// services) because InternalHandler needs a reference for the
	// RegisterWorker hook (issue #53 on-register sync). The
	// background loop is started later in runServer so the cancel
	// from rootCancel actually tears it down.
	reconcileSvc := service.NewReconcileService(
		tenantRepo, activeDeploymentRepo, deploymentRepo, appEnvRepo, quotaRepo, publisher, cfg.Region,
	)
	migrationHandler := handler.NewMigrationHandler(migrationSvc)
	logSvc := service.NewLogService(logEntryRepo)
	// Custom-domain service (issue #83). The migration from
	// `apps`-FK-or-not is deferred; the service is built with the
	// existing repos and the quota check (MaxDomainsPerApp=50) is
	// enforced in the service layer. DomainService needs an
	// appLookupForDomain (for the (tenant, app) existence check +
	// row lock in AddDomain) AND a *sqlx.DB so it can wrap
	// count+insert in a transaction that holds a SELECT … FOR
	// UPDATE on the parent `apps` row. We pass the *AppRepository
	// (not the *AppService wrapper) so WithTx binds the lock
	// acquisition to the same tx that runs the count and the
	// insert. Without the tx wrap, two concurrent AddDomain calls
	// can both observe count==49 and both insert — closing that race
	// is the fix from the second-pass PR #133 review.
	domainSvc := service.NewDomainService(db, domainRepo, appRepo)

	// Initialize handlers
	tenantHandler := handler.NewTenantHandler(tenantSvc)
	apiKeyHandler := handler.NewAPIKeyHandler(apiKeySvc)
	deploymentHandler := handler.NewDeploymentHandler(deploymentSvc, workerSvc, trafficSvc)
	envHandler := handler.NewEnvHandler(envSvc)
	internalHandler := handler.NewInternalHandler(deploymentSvc, workerSvc, domainSvc, logEntryRepo, reconcileSvc)
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

	// Initialize middleware. The auth path delegates to APIKeyService
	// (which dispatches to the algorithm-specific verifier) rather than
	// calling the repo directly — see middleware/auth.go for why.
	authMiddleware := middleware.NewAuthMiddleware(apiKeySvc)

	// Setup router
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
	// Serve Swagger UI from CDN. To self-host: download swagger-ui-dist, place it in
	// internal/swaggerui/, and replace this with http.FileServer(http.FS(swaggerui.FS)).
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

	// Public endpoints (no auth required)
	mux.HandleFunc("POST /api/v1/tenants", tenantHandler.Bootstrap) // Self-signup: create tenant + first API key
	mux.HandleFunc("POST /api/v1/keys", apiKeyHandler.Create)       // Create API key (would need tenant creation first)

	// Deprecated: redirect old /api/... paths to /api/v1/... for clients still
	// on the old contract. Workers use /api/internal/... (unversioned).
	// These are registered on the mux before the sub-muxes so they intercept
	// old-path requests; Go 1.22 mux matches longest pattern first, so
	// /api/v1/... sub-mux routes still win for new-path requests.
	sunsetDate := "2026-09-20"
	redirectTo := func(to string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", to)
			w.Header().Set("X-Redirected-From", r.URL.Path)
			w.Header().Set("Sunset", sunsetDate)
			w.WriteHeader(http.StatusMovedPermanently)
		}
	}
	// Tenant routes
	mux.HandleFunc("GET /api/tenants", redirectTo("/api/v1/tenants"))
	mux.HandleFunc("POST /api/tenants", redirectTo("/api/v1/tenants"))
	// Tenant+API key combined
	mux.HandleFunc("GET /api/keys", redirectTo("/api/v1/keys"))
	mux.HandleFunc("DELETE /api/keys/{keyID}", redirectTo("/api/v1/keys/"+"{keyID}"))
	// Apps
	mux.HandleFunc("GET /api/apps", redirectTo("/api/v1/apps"))
	mux.HandleFunc("GET /api/apps/{appName}", redirectTo("/api/v1/apps/"+"{appName}"))
	mux.HandleFunc("POST /api/apps/{appName}", redirectTo("/api/v1/apps/"+"{appName}"))
	mux.HandleFunc("DELETE /api/apps/{appName}", redirectTo("/api/v1/apps/"+"{appName}"))
	// App sub-resources
	mux.HandleFunc("GET /api/apps/{appName}/active", redirectTo("/api/v1/apps/"+"{appName}/active"))
	mux.HandleFunc("GET /api/apps/{appName}/ingress", redirectTo("/api/v1/apps/"+"{appName}/ingress"))
	mux.HandleFunc("GET /api/apps/{appName}/env", redirectTo("/api/v1/apps/"+"{appName}/env"))
	mux.HandleFunc("POST /api/apps/{appName}/env", redirectTo("/api/v1/apps/"+"{appName}/env"))
	mux.HandleFunc("DELETE /api/apps/{appName}/env/{key}", redirectTo("/api/v1/apps/"+"{appName}/env/"+"{key}"))
	mux.HandleFunc("POST /api/apps/{appName}/activate/{deploymentID}", redirectTo("/api/v1/apps/"+"{appName}/activate/"+"{deploymentID}"))
	mux.HandleFunc("GET /api/apps/{appName}/logs", redirectTo("/api/v1/apps/"+"{appName}/logs"))
	mux.HandleFunc("GET /api/apps/{appName}/status", redirectTo("/api/v1/apps/"+"{appName}/status"))
	// Custom domains (issue #83)
	mux.HandleFunc("POST /api/apps/{appName}/domains", redirectTo("/api/v1/apps/"+"{appName}/domains"))
	mux.HandleFunc("GET /api/apps/{appName}/domains", redirectTo("/api/v1/apps/"+"{appName}/domains"))
	mux.HandleFunc("GET /api/apps/{appName}/domains/{fqdn}", redirectTo("/api/v1/apps/"+"{appName}/domains/"+"{fqdn}"))
	mux.HandleFunc("DELETE /api/apps/{appName}/domains/{fqdn}", redirectTo("/api/v1/apps/"+"{appName}/domains/"+"{fqdn}"))
	// Deploy & status
	mux.HandleFunc("POST /api/deploy/{appName}", redirectTo("/api/v1/deploy/"+"{appName}"))
	mux.HandleFunc("GET /api/status/{deploymentID}", redirectTo("/api/v1/status/"+"{deploymentID}"))
	mux.HandleFunc("GET /api/list/{appName}", redirectTo("/api/v1/list/"+"{appName}"))
	// Auth & quota
	mux.HandleFunc("GET /api/auth/whoami", redirectTo("/api/v1/auth/whoami"))
	mux.HandleFunc("GET /api/quotas", redirectTo("/api/v1/quotas"))
	// Migration
	mux.HandleFunc("POST /api/migrate", redirectTo("/api/v1/migrate"))
	// Admin: tenants
	mux.HandleFunc("GET /api/admin/tenants", redirectTo("/api/v1/admin/tenants"))
	mux.HandleFunc("POST /api/admin/tenants", redirectTo("/api/v1/admin/tenants"))
	mux.HandleFunc("GET /api/admin/tenants/{tenantID}", redirectTo("/api/v1/admin/tenants/"+"{tenantID}"))
	mux.HandleFunc("PUT /api/admin/tenants/{tenantID}", redirectTo("/api/v1/admin/tenants/"+"{tenantID}"))
	mux.HandleFunc("DELETE /api/admin/tenants/{tenantID}", redirectTo("/api/v1/admin/tenants/"+"{tenantID}"))
	// Admin: apps & cluster
	mux.HandleFunc("DELETE /api/admin/apps/{appName}", redirectTo("/api/v1/admin/apps/"+"{appName}"))
	mux.HandleFunc("GET /api/admin/cluster", redirectTo("/api/v1/admin/cluster"))
	// Internal: redirect old /api/internal/ paths to the new unversioned /api/internal/
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
	api.HandleFunc("POST /api/v1/keys", apiKeyHandler.Create)
	api.HandleFunc("GET /api/v1/apps/{appName}/ingress", deploymentHandler.AppIngress)
	api.HandleFunc("GET /api/v1/apps/{appName}/traffic", trafficHandler.GetTraffic)
	api.HandleFunc("PUT /api/v1/apps/{appName}/traffic", trafficHandler.SetTraffic)
	api.HandleFunc("GET /api/v1/keys", apiKeyHandler.List)
	api.HandleFunc("DELETE /api/v1/keys/{keyID}", apiKeyHandler.Delete)
	api.HandleFunc("GET /api/v1/egress", egressHandler.Get)
	api.HandleFunc("PUT /api/v1/egress", egressHandler.Update)
	api.HandleFunc("GET /api/v1/apps/{appName}/logs", logHandler.List)
	api.HandleFunc("GET /api/v1/metrics", metricsHandler.GetTenantMetrics)

	// Custom-domain routes (issue #83). Tenant-authenticated; the
	// handler validates the FQDN shape and enforces the per-app
	// quota. Routed through the same auth middleware as the rest of
	// `/api/v1/`.
	api.HandleFunc("POST /api/v1/apps/{appName}/domains", domainHandler.Add)
	api.HandleFunc("GET /api/v1/apps/{appName}/domains", domainHandler.List)
	api.HandleFunc("GET /api/v1/apps/{appName}/domains/{fqdn}", domainHandler.Get)
	api.HandleFunc("DELETE /api/v1/apps/{appName}/domains/{fqdn}", domainHandler.Remove)

	// Admin routes (require owner role)
	admin := http.NewServeMux()
	admin.HandleFunc("GET /api/v1/admin/tenants", tenantHandler.List)
	admin.HandleFunc("POST /api/v1/admin/tenants", tenantHandler.Create)
	admin.HandleFunc("GET /api/v1/admin/tenants/{tenantID}", tenantHandler.Get)
	admin.HandleFunc("PUT /api/v1/admin/tenants/{tenantID}", tenantHandler.Update)
	admin.HandleFunc("DELETE /api/v1/admin/tenants/{tenantID}", tenantHandler.Delete)
	admin.HandleFunc("DELETE /api/v1/admin/apps/{appName}", appHandler.Delete)
	admin.HandleFunc("GET /api/v1/admin/cluster", clusterHandler.Get)

	// Chain auth + role middleware
	apiWithAuth := authMiddleware.Authenticate(api)
	apiWithOwner := authMiddleware.Authenticate(
		middleware.RequireRole("owner")(admin),
	)

	mux.Handle("/api/v1/", apiWithAuth)
	mux.Handle("/api/v1/admin/", apiWithOwner)

	// Service-to-service read endpoint that the edge-ingress polls to
	// apply Caddy weights for canary/blue-green traffic splits. Registered
	// on the parent mux with a more specific pattern than the /api/v1/
	// catch-all so Go 1.22's longest-match rule routes the request here
	// rather than into apiWithAuth (where an unauthenticated ingress
	// would 401 and the canary split would never reach Caddy).
	mux.Handle(
		"GET /api/v1/internal/traffic/{tenantID}/{appName}",
		middleware.InternalAuth(cfg.InternalToken)(http.HandlerFunc(trafficHandler.GetTrafficInternal)),
	)

	// Internal endpoints (worker-facing, JWT auth).
	//
	// Workers consume the latest contract; these paths are intentionally unversioned.
	//
	// The four custom-domain internal endpoints (`/api/internal/domains`,
	// `/api/internal/tls-allowed`, `/api/internal/domains/{id}/status`)
	// accept either `role: "worker"` or `role: "ingest"` tokens. The
	// ingress uses a long-lived `ingest` JWT (printed at startup below);
	// admins can also call them with a worker JWT.
	// Workers consume the latest contract; these paths are intentionally unversioned.
	//
	// The four custom-domain internal endpoints (`/api/internal/domains`,
	// `/api/internal/tls-allowed`, `/api/internal/domains/{id}/status`)
	// accept either `role: "worker"` or `role: "ingest"` tokens. The
	// ingress uses a long-lived `ingest` JWT (printed at startup below);
	// admins can also call them with a worker JWT.
	internalMux := http.NewServeMux()
	internalMux.HandleFunc("GET /api/internal/download/{deploymentID}", internalHandler.Download)
	internalMux.HandleFunc("POST /api/internal/workers", internalHandler.RegisterWorker)
	internalMux.HandleFunc("GET /api/internal/workers", internalHandler.ListWorkers)
	internalMux.HandleFunc("POST /api/internal/logs", internalHandler.IngestLogs)
	// Worker-driven auto-rollback: an edge-worker POSTs here when its
	// supervisor exhausts the restart cap on a tenant app. The
	// handler swaps the active deployment back to last_good and
	// publishes a TaskMessage so all regions reconcile. Like every
	// other /api/internal/* endpoint, this is currently
	// unauthenticated — see the comment on internalMux above.
	internalMux.HandleFunc("POST /api/internal/apps/{appName}/auto-rollback", internalHandler.AutoRollback)
	// Custom-domain routes (issue #83). All three are gated to
	// RoleIngest ONLY — the cross-tenant reads (ListDomains,
	// TlsAllowed) return data for every tenant and would leak the
	// entire tenant→FQDN→app mapping to any worker JWT in the
	// platform (every v1 worker JWT defaults to RoleWorker, so
	// RoleIngest+RoleWorker allowed = effectively no gate). The
	// 30s ingress poller mints a long-lived ingest token at
	// startup (cmd/api/mint.go), so no legitimate worker caller
	// needs RoleWorker access here. UpdateDomainStatus is also
	// Ingest-only because it mutates a row in any tenant's domain
	// (intended receiver: the v2 Caddy event hook, which runs on
	// the same ingress process as the poller).
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
	// chain that accepts either a worker JWT (existing behavior) OR
	// an X-Internal-Token header (new — lets a peer control plane
	// pull artifacts through this CP for its own region). Other
	// /api/internal/* routes stay WorkerAuth-only; the token lane is
	// intentionally narrow (issue #127 step 3).
	downloadMux := http.NewServeMux()
	downloadMux.HandleFunc("GET /api/internal/download/{deploymentID}", internalHandler.Download)
	mux.Handle("GET /api/internal/download/", middleware.InternalOrWorkerAuth(
		workerJWTConfig, cfg.InternalToken,
	)(downloadMux))
	mux.Handle("/api/internal/", middleware.WorkerAuth(workerJWTConfig)(internalMux))

	// Mint a long-lived service token for the edge-ingress poller
	// (issue #83). The token is written to a 0600 file (NOT logged
	// in plaintext) so the operator can copy it into the ingress's
	// INGRESS_SERVICE_TOKEN env var. Region is sourced from the
	// APP_REGION env var (the config file doesn't carry it because
	// the control plane is region-agnostic — it's the ingress and
	// workers that have region identity). If unset, skip — no
	// ingress is going to talk to this control plane.
	//
	// The token file lands in the same dir as the artifact store
	// (`cfg.Storage.ArtifactPath`) so operators have a single,
	// predictable place to find all per-region secrets.
	region := os.Getenv("APP_REGION")
	if region != "" {
		tok, err := mintIngressToken(cfg.JWT.Secret, cfg.JWT.Issuer, region)
		if err != nil {
			log.Fatalf("failed to mint ingress token: %v", err)
		}
		path, err := writeIngressTokenFile(cfg.Storage.ArtifactPath, region, tok)
		if err != nil {
			log.Fatalf("failed to write ingress token file: %v", err)
		}
		log.Printf("INGRESS_SERVICE_TOKEN written to %s (region=%s, expires in 1y, mode 0600)", path, region)
	} else {
		log.Printf("APP_REGION not set; skipping ingress service token mint (default-only mode)")
	}

	// Start server with graceful shutdown
	addr := fmt.Sprintf("%s:%d", cfg.App.Host, cfg.App.Port)
	// Wrap with request ID tracing — outermost middleware runs for every request.
	srv := &http.Server{Addr: addr, Handler: middleware.RequestID(mux)}

	go func() {
		log.Printf("Starting edge-cloud control plane on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// rootCtx is the parent context for every background goroutine spawned
	// by main(); cancelling it makes them exit cleanly when the HTTP server
	// shuts down. Using context.Background() here would leak the goroutines
	// across reloads/restarts — the goroutines would only exit when main()
	// returned, by which point the DB and NATS connections are already
	// closed and the next iteration of the loop would error.
	rootCtx, rootCancel := context.WithCancel(context.Background())

	// Start NATS heartbeat subscriber for worker lifecycle management
	go func() {
		if err := workerSvc.SubscribeHeartbeats(rootCtx); err != nil {
			log.Printf("Worker heartbeat subscription error: %v", err)
		}
	}()

	// Start log retention GC. Tunable via env (LOG_GC_INTERVAL, LOG_RETENTION);
	// defaults to a 1-hour sweep with 7-day retention.
	logGC := service.NewLogGCService(logEntryRepo)
	logGCInterval := parseDurationEnv("LOG_GC_INTERVAL", time.Hour)
	logRetention := parseDurationEnv("LOG_RETENTION", 7*24*time.Hour)
	go logGC.Run(rootCtx, logGCInterval, logRetention)

	// Start periodic full-state reconcile (issue #53). Tunable via
	// RECONCILE_INTERVAL; default 5min. The first sweep fires
	// immediately so a fresh boot catches up workers that missed
	// messages, then ticks on the configured interval. Idempotent —
	// the worker-side diff treats identical AppConfig as a no-op.
	reconcileInterval := parseDurationEnv("RECONCILE_INTERVAL", 5*time.Minute)
	go reconcileSvc.Run(rootCtx, reconcileInterval)

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	// Cancel the root context so the background goroutines (log GC, NATS
	// heartbeat subscriber) exit before main() returns and closes the DB
	// and NATS connections out from under them.
	rootCancel()
	log.Println("Server exited")
}

// parseDurationEnv reads a duration-valued env var or returns the default.
// On a missing, malformed, or non-positive value it logs a warning and
// returns the default — the GC service should never busy-loop or wipe
// the logs table because of an operator typo. Non-positive values
// (including zero and negative durations) are rejected in addition to
// malformed strings; LogGCService.Run also defends against them.
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
