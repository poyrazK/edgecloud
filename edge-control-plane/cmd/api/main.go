package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
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
	_ = repository.NewWorkerRepository(db) // workerRepo initialized for future use

	// Initialize NATS publisher (mock for now)
	var publisher nats.Publisher = &nats.MockPublisher{}

	// Initialize artifact storage
	artifactStore := storage.NewArtifactStore(cfg.Storage.ArtifactPath)

	// Initialize services
	tenantSvc := service.NewTenantService(db, tenantRepo, quotaRepo)
	apiKeySvc := service.NewAPIKeyService(apiKeyRepo)
	deploymentSvc := service.NewDeploymentService(
		deploymentRepo, activeDeploymentRepo, appEnvRepo, quotaRepo, tenantRepo, artifactStore, publisher,
	)
	envSvc := service.NewEnvService(appEnvRepo)

	// Initialize handlers
	tenantHandler := handler.NewTenantHandler(tenantSvc)
	apiKeyHandler := handler.NewAPIKeyHandler(apiKeySvc)
	deploymentHandler := handler.NewDeploymentHandler(deploymentSvc)
	envHandler := handler.NewEnvHandler(envSvc)
	internalHandler := handler.NewInternalHandler(deploymentSvc)

	// Initialize middleware
	authMiddleware := middleware.NewAuthMiddleware(apiKeyRepo)

	// Setup router
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Public endpoints (no auth required)
	mux.HandleFunc("POST /api/keys", apiKeyHandler.Create) // Create API key (would need tenant creation first)

	// Protected API routes
	api := http.NewServeMux()
	api.HandleFunc("POST /api/deploy/{appName}", deploymentHandler.Deploy)
	api.HandleFunc("GET /api/status/{deploymentID}", deploymentHandler.GetStatus)
	api.HandleFunc("GET /api/list/{appName}", deploymentHandler.List)
	api.HandleFunc("POST /api/apps/{appName}/activate/{deploymentID}", deploymentHandler.Activate)
	api.HandleFunc("GET /api/apps/{appName}/active", deploymentHandler.GetActive)
	api.HandleFunc("POST /api/apps/{appName}/env", envHandler.Set)
	api.HandleFunc("GET /api/apps/{appName}/env", envHandler.List)
	api.HandleFunc("DELETE /api/apps/{appName}/env/{key}", envHandler.Delete)
	api.HandleFunc("GET /api/keys", apiKeyHandler.List)
	api.HandleFunc("DELETE /api/keys/{keyID}", apiKeyHandler.Delete)

	// Admin routes (require owner role)
	admin := http.NewServeMux()
	admin.HandleFunc("GET /api/admin/tenants", tenantHandler.List)
	admin.HandleFunc("POST /api/admin/tenants", tenantHandler.Create)
	admin.HandleFunc("GET /api/admin/tenants/{tenantID}", tenantHandler.Get)
	admin.HandleFunc("PUT /api/admin/tenants/{tenantID}", tenantHandler.Update)
	admin.HandleFunc("DELETE /api/admin/tenants/{tenantID}", tenantHandler.Delete)

	// Chain auth + role middleware
	apiWithAuth := authMiddleware.Authenticate(api)
	apiWithOwner := authMiddleware.Authenticate(
		middleware.RequireRole("owner")(admin),
	)

	mux.Handle("/api/", apiWithAuth)
	mux.Handle("/api/admin/", apiWithOwner)

	// Internal endpoints (worker-facing, would need JWT auth)
	mux.HandleFunc("GET /api/internal/download/{deploymentID}", internalHandler.Download)

	// Start server
	addr := fmt.Sprintf("%s:%d", cfg.App.Host, cfg.App.Port)
	log.Printf("Starting edge-cloud control plane on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func init() {
	// Ensure artifact storage directory exists
	if err := os.MkdirAll("/var/edgecloud/registry", 0755); err != nil {
		log.Printf("Warning: could not create artifact directory: %v", err)
	}
}
