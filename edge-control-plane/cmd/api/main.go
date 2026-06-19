package main

import (
	"context"
	"fmt"
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
	appSvc := service.NewAppService(db, appRepo, deploymentRepo, activeDeploymentRepo, appEnvRepo, artifactStore)
	deploymentSvc := service.NewDeploymentService(
		deploymentRepo, activeDeploymentRepo, appEnvRepo, quotaRepo, tenantRepo, artifactStore, publisher,
	)
	deploymentSvc.SetAppService(appSvc)
	envSvc := service.NewEnvService(appEnvRepo)
	workerSvc := service.NewWorkerService(workerRepo, quotaRepo, publisher.Conn())
	clusterSvc := service.NewClusterService(workerRepo)
	migrationSvc := service.NewMigrationService(deploymentRepo, artifactStore, cfg.Migration.EdgeMigratePath, cfg.Migration.WasiSdkPath)
	migrationHandler := handler.NewMigrationHandler(migrationSvc)

	// Initialize handlers
	tenantHandler := handler.NewTenantHandler(tenantSvc)
	apiKeyHandler := handler.NewAPIKeyHandler(apiKeySvc)
	deploymentHandler := handler.NewDeploymentHandler(deploymentSvc)
	envHandler := handler.NewEnvHandler(envSvc)
	internalHandler := handler.NewInternalHandler(deploymentSvc, workerSvc)
	appHandler := handler.NewAppHandler(appSvc)
	clusterHandler := handler.NewClusterHandler(clusterSvc)

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

	// Public endpoints (no auth required)
	mux.HandleFunc("POST /api/tenants", tenantHandler.Bootstrap) // Self-signup: create tenant + first API key
	mux.HandleFunc("POST /api/keys", apiKeyHandler.Create)       // Create API key (would need tenant creation first)

	// Protected API routes
	api := http.NewServeMux()
	api.HandleFunc("POST /api/deploy/{appName}", deploymentHandler.Deploy)
	api.HandleFunc("POST /api/migrate", migrationHandler.Migrate)
	api.HandleFunc("GET /api/status/{deploymentID}", deploymentHandler.GetStatus)
	api.HandleFunc("GET /api/list/{appName}", deploymentHandler.List)
	api.HandleFunc("POST /api/apps/{appName}/activate/{deploymentID}", deploymentHandler.Activate)
	api.HandleFunc("GET /api/apps/{appName}/active", deploymentHandler.GetActive)
	api.HandleFunc("POST /api/apps/{appName}/env", envHandler.Set)
	api.HandleFunc("GET /api/apps/{appName}/env", envHandler.List)
	api.HandleFunc("DELETE /api/apps/{appName}/env/{key}", envHandler.Delete)
	api.HandleFunc("POST /api/apps/{appName}", appHandler.Create)
	api.HandleFunc("GET /api/apps", appHandler.List)
	api.HandleFunc("GET /api/apps/{appName}", appHandler.Get)
	api.HandleFunc("GET /api/keys", apiKeyHandler.List)
	api.HandleFunc("DELETE /api/keys/{keyID}", apiKeyHandler.Delete)

	// Admin routes (require owner role)
	admin := http.NewServeMux()
	admin.HandleFunc("GET /api/admin/tenants", tenantHandler.List)
	admin.HandleFunc("POST /api/admin/tenants", tenantHandler.Create)
	admin.HandleFunc("GET /api/admin/tenants/{tenantID}", tenantHandler.Get)
	admin.HandleFunc("PUT /api/admin/tenants/{tenantID}", tenantHandler.Update)
	admin.HandleFunc("DELETE /api/admin/tenants/{tenantID}", tenantHandler.Delete)
	admin.HandleFunc("DELETE /api/admin/apps/{appName}", appHandler.Delete)
	admin.HandleFunc("GET /api/admin/cluster", clusterHandler.Get)

	// Chain auth + role middleware
	apiWithAuth := authMiddleware.Authenticate(api)
	apiWithOwner := authMiddleware.Authenticate(
		middleware.RequireRole("owner")(admin),
	)

	mux.Handle("/api/", apiWithAuth)
	mux.Handle("/api/admin/", apiWithOwner)

	// Internal endpoints (worker-facing, JWT auth)
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
	srv := &http.Server{Addr: addr, Handler: mux}

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
