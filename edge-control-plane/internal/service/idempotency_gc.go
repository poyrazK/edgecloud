package service

import (
	"context"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// activeDeploymentIdempotencyKeyRepoForGC is the subset of
// *repository.ActiveDeploymentIdempotencyKeyRepo used by
// IdempotencyKeyGCService. Defining it locally keeps tests mockable
// without a live DB.
type activeDeploymentIdempotencyKeyRepoForGC interface {
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)
}

// IdempotencyKeyGCService periodically deletes
// active_deployment_idempotency_keys rows whose created_at is older than
// the issue #439 cache TTL (24h — see repository.IdempotencyTTL). It
// runs as a background goroutine for the lifetime of the control-plane
// process and exits cleanly when ctx is cancelled.
//
// Why a sweeper: the Lookup-side TTL filter (`created_at > NOW() -
// make_interval(secs => IdempotencyTTL.Seconds())`) makes aged-out
// rows invisible to the replay path, but they still occupy disk + are
// visited by every INSERT's index update. Without a sweeper, the
// table grows unbounded over the deployment's lifetime. Mirrors the
// LogGC / DeploymentGC / PreviewGC shape at deployment_gc.go,
// log_gc.go, preview_gc.go.
//
// Errors are logged and the loop continues — a transient DB failure
// should not silently halt the GC forever, but the operator should
// also see it.
type IdempotencyKeyGCService struct {
	repo activeDeploymentIdempotencyKeyRepoForGC
}

func NewIdempotencyKeyGCService(repo activeDeploymentIdempotencyKeyRepoForGC) *IdempotencyKeyGCService {
	return &IdempotencyKeyGCService{repo: repo}
}

// Run blocks until ctx is cancelled. The first sweep fires immediately
// (operationally useful — when the process restarts we don't want to
// wait `interval` before deleting old rows); subsequent sweeps tick at
// `interval`.
//
// `interval` is the sweep tick (default 1h, env IDEMPOTENCY_GC_INTERVAL).
// `age` is the retention threshold — rows older than this are deleted
// (default matches repository.IdempotencyTTL = 24h so the GC keeps the
// lookup-visible window fully populated).
//
// If either duration is non-positive the service refuses to run:
// interval<=0 would busy-loop (ticker fires immediately on every
// iteration) and age<=0 would compute a future cutoff and wipe the
// table. The operator sees a clear log line and the GC stays disabled
// until the env vars are fixed and the process restarted.
func (s *IdempotencyKeyGCService) Run(ctx context.Context, interval, age time.Duration) {
	if interval <= 0 || age <= 0 {
		log.Printf("idempotency_gc: invalid interval=%s age=%s; refusing to run", interval, age)
		return
	}

	// runOnce is a closure so the immediate-first-sweep path and the
	// ticker path use the same delete-and-log logic.
	runOnce := func() {
		// Skip the DELETE roundtrip if we're already shutting down.
		// The repository itself short-circuits on a cancelled ctx,
		// but checking here avoids a wasted pool acquire + log on
		// the shutdown path and keeps the immediate-first-sweep
		// from issuing a DELETE we're about to cancel.
		if ctx.Err() != nil {
			return
		}
		// Pass `age` (a Duration) to the repo. The repo computes
		// the cutoff server-side via NOW() - make_interval(...), so
		// the DB clock — not the Go process clock — is the time
		// authority. This protects against clock skew between the
		// control plane host and the DB host.
		deleted, err := s.repo.DeleteOlderThan(ctx, age)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down — expected
			}
			log.Printf("idempotency_gc: delete failed (age=%s): %v", age, err)
			return
		}
		if deleted > 0 {
			log.Printf("idempotency_gc: deleted %d rows older than %s", deleted, age)
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

// Compile-time assertion that the real repo satisfies the GC interface.
// Wired via NewActiveDeploymentIdempotencyKeyRepo at app.go construction.
var _ activeDeploymentIdempotencyKeyRepoForGC = (*repository.ActiveDeploymentIdempotencyKeyRepo)(nil)
