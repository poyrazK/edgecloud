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
	defer db.Close()

	// Initialize repositories
	tenantRepo := repository.NewTenantRepository(db)
	quotaRepo := repository.NewQuotaRepository(db)
	apiKeyRepo := repository.NewAPIKeyRepository(db)
	deploymentRepo := repository.NewDeploymentRepository(db)
	activeDeploymentRepo := repository.NewActiveDeploymentRepository(db)
	appEnvRepo := repository.NewAppEnvRepository(db)
	appRepo := repository.NewAppRepository(db)
	workerRepo := repository.NewWorkerRepository(db)

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

	// Initialize artifact storage
	artifactStore := storage.NewArtifactStore(cfg.Storage.ArtifactPath)

	// Initialize services
	tenantSvc := service.NewTenantService(db, tenantRepo, quotaRepo, apiKeyRepo)
	apiKeySvc := service.NewAPIKeyService(apiKeyRepo)
	appSvc := service.NewAppService(db, appRepo, deploymentRepo, activeDeploymentRepo, appEnvRepo, artifactStore, quotaRepo)
	deploymentSvc := service.NewDeploymentService(
		deploymentRepo, activeDeploymentRepo, appEnvRepo, quotaRepo, tenantRepo, artifactStore, publisher, cfg.Region,
	)
	deploymentSvc.SetAppService(appSvc)
	envSvc := service.NewEnvService(appEnvRepo)
	workerSvc := service.NewWorkerService(workerRepo, quotaRepo, publisher.Conn())
	clusterSvc := service.NewClusterService(workerRepo)
	migrationSvc := service.NewMigrationService(deploymentRepo, artifactStore, cfg.Migration.EdgeMigratePath, cfg.Migration.WasiSdkPath, cfg.Migration.RustcPath)
	migrationHandler := handler.NewMigrationHandler(migrationSvc)

	// Initialize handlers
	tenantHandler := handler.NewTenantHandler(tenantSvc)
	apiKeyHandler := handler.NewAPIKeyHandler(apiKeySvc)
	deploymentHandler := handler.NewDeploymentHandler(deploymentSvc, workerSvc)
	envHandler := handler.NewEnvHandler(envSvc)
	internalHandler := handler.NewInternalHandler(deploymentSvc, workerSvc)
	appHandler := handler.NewAppHandler(appSvc)
	authHandler := handler.NewAuthHandler(tenantSvc, apiKeySvc)
	clusterHandler := handler.NewClusterHandler(clusterSvc)
	quotaHandler := handler.NewQuotaHandler(tenantSvc)
	egressHandler := handler.NewEgressHandler(tenantSvc, deploymentSvc)

	// Initialize middleware. The auth path delegates to APIKeyService
	// (which dispatches to the algorithm-specific verifier) rather than
	// calling the repo directly — see middleware/auth.go for why.
	authMiddleware := middleware.NewAuthMiddleware(apiKeySvc)

	// Setup router
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// OpenAPI spec — served as raw YAML
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		f, err := openAPISpec.Open("docs/api/openapi.yaml")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "application/x-yaml")
		io.Copy(w, f)
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
		w.Write([]byte(swaggerUIHTML))
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
	api.HandleFunc("GET /api/v1/apps/{appName}/active", deploymentHandler.GetActive)
	api.HandleFunc("GET /api/v1/auth/whoami", authHandler.Whoami)
	api.HandleFunc("POST /api/v1/apps/{appName}/env", envHandler.Set)
	api.HandleFunc("GET /api/v1/apps/{appName}/env", envHandler.List)
	api.HandleFunc("DELETE /api/v1/apps/{appName}/env/{key}", envHandler.Delete)
	api.HandleFunc("GET /api/v1/quotas", quotaHandler.GetQuota)
	api.HandleFunc("POST /api/v1/apps/{appName}", appHandler.Create)
	api.HandleFunc("GET /api/v1/apps", appHandler.List)
	api.HandleFunc("GET /api/v1/apps/{appName}", appHandler.Get)
	api.HandleFunc("GET /api/v1/apps/{appName}/ingress", deploymentHandler.AppIngress)
	api.HandleFunc("GET /api/v1/keys", apiKeyHandler.List)
	api.HandleFunc("DELETE /api/v1/keys/{keyID}", apiKeyHandler.Delete)
	api.HandleFunc("GET /api/v1/egress", egressHandler.Get)
	api.HandleFunc("PUT /api/v1/egress", egressHandler.Update)

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

	// Internal endpoints (worker-facing, JWT auth)
	// Workers consume the latest contract; these paths are intentionally unversioned.
	internalMux := http.NewServeMux()
	internalMux.HandleFunc("GET /api/internal/download/{deploymentID}", internalHandler.Download)
	internalMux.HandleFunc("POST /api/internal/workers", internalHandler.RegisterWorker)
	internalMux.HandleFunc("GET /api/internal/workers", internalHandler.ListWorkers)
	workerJWTConfig := middleware.WorkerJWTConfig{
		Secret: cfg.JWT.Secret,
		Issuer: cfg.JWT.Issuer,
	}
	mux.Handle("/api/internal/", middleware.WorkerAuth(workerJWTConfig)(internalMux))

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

	// Start NATS heartbeat subscriber for worker lifecycle management
	go func() {
		if err := workerSvc.SubscribeHeartbeats(context.Background()); err != nil {
			log.Printf("Worker heartbeat subscription error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Println("Server exited")
}
