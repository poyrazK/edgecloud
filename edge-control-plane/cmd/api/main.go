package main

import (
	"context"
	"embed"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/app"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
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
	// ── Infrastructure Setup ──────────────────────────────────────
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	db, err := repository.NewDB(cfg.Database.DSN())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	publisher, err := nats.NewNATSPublisher(cfg.NATS.URL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer publisher.Close()

	if err := publisher.EnsureStream(nats.StreamConfig{
		Name:      nats.TaskStreamName,
		Subjects:  []string{"edgecloud.tasks.>"},
		Retention: natsio.InterestPolicy,
		MaxAge:    24 * time.Hour,
		Replicas:  cfg.NATS.Replicas,
	}); err != nil {
		log.Fatalf("Failed to ensure NATS stream: %v", err)
	}

	artifactStore, err := storage.New(context.Background(), cfg.Storage)
	if err != nil {
		log.Fatalf("Failed to initialize artifact storage: %v", err)
	}

	// ── Application Assembly ──────────────────────────────────────
	application := app.New(cfg, db, publisher, artifactStore, openAPISpec)

	// Issue #332 (Layer 3: Push-to-Edge). When `region_artifact_caches`
	// is configured + a non-empty `artifact_cache_internal_token` is set,
	// wire the per-region artifact-cache pusher into the deployment
	// service. Both are optional — when absent, the cache-push step
	// in publishSwap is a no-op and the existing pull-from-CP
	// behavior is unchanged.
	if len(cfg.Storage.RegionArtifactCaches) > 0 {
		application.DeploymentSvc.SetRegionArtifactCaches(cfg.Storage.RegionArtifactCaches)
		application.DeploymentSvc.SetCachePusher(
			service.NewHTTPArtifactCachePusher(artifactStore, cfg.Storage.ArtifactCacheInternalToken),
		)
		log.Printf("region artifact cache: enabled for %d region(s)", len(cfg.Storage.RegionArtifactCaches))
	}

	// ── Ingress Service Token ─────────────────────────────────────
	region := os.Getenv("APP_REGION")
	if region != "" {
		tok, err := mintIngressToken(application.WorkerJWTConfig, region)
		if err != nil {
			log.Fatalf("failed to mint ingress token: %v", err)
		}
		path, err := writeIngressTokenFile(application.ArtifactPath, region, tok)
		if err != nil {
			log.Fatalf("failed to write ingress token file: %v", err)
		}
		log.Printf("INGRESS_SERVICE_TOKEN written to %s (region=%s, expires in 1y, mode 0600)", path, region)
	} else {
		log.Printf("APP_REGION not set; skipping ingress service token mint (default-only mode)")
	}

	// ── HTTP Server ───────────────────────────────────────────────
	addr := fmt.Sprintf("%s:%d", cfg.App.Host, cfg.App.Port)
	srv := &http.Server{Addr: addr, Handler: application.Handler}

	go func() {
		log.Printf("Starting edge-cloud control plane on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// ── Background Goroutines ─────────────────────────────────────
	rootCtx, rootCancel := context.WithCancel(context.Background())
	go application.RunBackground(rootCtx)

	// ── Graceful Shutdown ─────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	rootCancel()
	log.Println("Server exited")
}
