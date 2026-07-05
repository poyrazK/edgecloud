package service

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// deploymentRepoForGC is the subset of *repository.DeploymentRepository used
// by DeploymentGCService. Defined locally so tests are mockable.
//
// The batched delete returns the deleted (id, tenant_id, app_name) rows so
// the service can call ArtifactStore.Delete for each — without this the
// GC would leak artifacts at /registry/{tenant_id}/{app_name}/{id}.wasm
// forever (the previous version of this service deleted DB rows but left
// the artifacts behind).
type deploymentRepoForGC interface {
	DeleteOlderThanBatched(
		ctx context.Context, retention time.Duration, batchSize, maxBatches int,
	) ([]repository.DeletedDeployment, error)
}

// DeploymentGCService periodically deletes old deployment rows that are
// not currently active AND the artifact blobs associated with them.
// Matches the LogGCService / WorkerGCService pattern: immediate first
// sweep, then a ticker loop.
type DeploymentGCService struct {
	repo          deploymentRepoForGC
	artifactStore ArtifactStoreInterface
}

func NewDeploymentGCService(repo deploymentRepoForGC, store ArtifactStoreInterface) *DeploymentGCService {
	return &DeploymentGCService{repo: repo, artifactStore: store}
}

// Run blocks until ctx is cancelled. First sweep fires immediately.
//
// interval and retention are passed in by main.go so they can be tuned via
// env vars (DEPLOY_GC_INTERVAL, DEPLOY_RETENTION) without changing this
// struct. Defaults wired in app.go's RunBackground are 1h and 7d.
//
// If either duration is non-positive the service refuses to run:
// interval<=0 would busy-loop (ticker fires immediately on every
// iteration) and retention<=0 would compute a future cutoff and wipe
// the entire deployments table. The operator sees a clear log line and
// the GC stays disabled until the env vars are fixed and the process
// restarted. The repo-layer guard at DeploymentRepository.DeleteOlderThanBatched
// is defense-in-depth in case a future caller bypasses this check.
func (s *DeploymentGCService) Run(ctx context.Context, interval, retention time.Duration) {
	if interval <= 0 || retention <= 0 {
		log.Printf("deployment_gc: invalid interval=%s retention=%s; refusing to run", interval, retention)
		return
	}

	// GC tunables for the batched DELETE. Same shape as
	// LogGCService: 10k rows per batch amortizes round-trip cost while
	// bounding worst-case lock duration on the deployments table; 1000
	// batches/sweep caps a worst-case first-sweep at 10M rows (well
	// above any realistic backlog).
	const (
		gcBatchSize  = 10_000
		gcMaxBatches = 1000
	)

	// runOnce is a closure so the immediate-first-sweep path and the
	// ticker path use the same delete-then-cleanup logic.
	runOnce := func() {
		// Skip the DB roundtrip if we're already shutting down. The
		// repo short-circuits on a cancelled ctx too, but checking
		// here avoids a wasted pool acquire + log on the shutdown
		// path and keeps the immediate-first-sweep from issuing a
		// DELETE we're about to cancel.
		if ctx.Err() != nil {
			return
		}
		deleted, err := s.repo.DeleteOlderThanBatched(ctx, retention, gcBatchSize, gcMaxBatches)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down — expected
			}
			log.Printf("deployment_gc: delete failed (retention=%s): %v", retention, err)
			return
		}
		if len(deleted) == 0 {
			return
		}
		// Best-effort artifact cleanup. The DB rows are already gone
		// (committed in the same statement as the RETURNING) — an
		// artifact delete failure is not worth halting the GC over,
		// since the next sweep will pick the same row up by id
		// (well, it won't, because the row is gone, but an orphaned
		// blob is bounded by disk capacity, not by correctness).
		// We log so the operator can investigate.
		var orphans int
		for _, d := range deleted {
			if err := s.artifactStore.Delete(ctx, d.TenantID, d.AppName, d.ID); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					// Artifact was already gone (e.g. operator rm, or a
					// previous sweep raced). Not an error.
					continue
				}
				orphans++
				log.Printf("deployment_gc: orphan artifact (deployment_id=%s tenant=%s app=%s): %v",
					d.ID, d.TenantID, d.AppName, err)
			}
		}
		if orphans > 0 {
			log.Printf("deployment_gc: deleted %d deployments; %d orphan artifact(s) left for operator cleanup",
				len(deleted), orphans)
		} else {
			log.Printf("deployment_gc: deleted %d deployments older than %s", len(deleted), retention)
		}
	}

	runOnce()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}
