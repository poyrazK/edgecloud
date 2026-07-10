package service

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// previewRepoForGC is the subset of *repository.DeploymentRepository
// used by PreviewGCService. Defining it locally keeps tests mockable
// without a live DB — the same pattern as `logEntryRepoForGC` in
// log_gc.go:11.
type previewRepoForGC interface {
	// ListExpiredPreviewBlobs returns up to `batchSize` preview
	// deployments whose preview_expires_at is in the past. Used to
	// discover which artifact blobs need to be unlinked from
	// /registry/{tenant_id}/{app_name}/{id}.wasm before the DB row
	// is deleted. Returned in oldest-expiry-first order so a
	// thundering herd is reclaimed predictably.
	ListExpiredPreviewBlobs(ctx context.Context, batchSize int) ([]repository.PreviewBlobRef, error)
	// DeleteExpiredPreviewsByIDs deletes the rows whose IDs are
	// in `ids` AND whose preview_expires_at is still in the past
	// (the second predicate is a defense-in-depth idempotency
	// guard — a row that was extended by a fresh preview deploy
	// between List and Delete won't be removed).
	DeleteExpiredPreviewsByIDs(ctx context.Context, ids []string) ([]repository.DeletedDeployment, error)
}

// previewBlobDeleter is the subset of storage.ArtifactStore used by
// PreviewGCService. Defining it locally keeps the service unit-test
// mockable — the same reason log_gc.go keeps logEntryRepoForGC
// package-local.
type previewBlobDeleter interface {
	Delete(ctx context.Context, tenantID, appName, deploymentID string) error
}

// PreviewGCService (issue #308) periodically deletes preview
// deployment rows + their artifact blobs once the preview's
// `preview_expires_at` is in the past. Runs as a background
// goroutine for the lifetime of the control-plane process and
// exits cleanly when ctx is cancelled.
//
// Order matters: the artifact blob at
// /registry/{tenant_id}/{app_name}/{deployment_id}.wasm is
// deleted FIRST; the DB row is deleted SECOND. If the blob
// delete fails after the row is gone, we leak a blob (an orphan
// on disk, cheap to operator-clean). If the order were reversed,
// a row could point at a missing blob, and the worker's download
// handler would have to handle a 404 mid-stream — a worse
// failure mode.
//
// Errors are logged and the loop continues — a transient DB or
// storage failure should not silently halt the GC forever, but
// the operator should also see it.
//
// Concurrency: only one PreviewGCService.Run is expected per
// control-plane process (it's wired in app.RunBackground, next
// to LogGC.Run). Two concurrent sweeps would race on the same
// rows but `DeleteExpiredPreviewsByIDs` is idempotent (the
// `WHERE preview_expires_at < NOW()` predicate filters rows
// that are no longer expired), so the worst case is wasted work.
type PreviewGCService struct {
	repo  previewRepoForGC
	blobs previewBlobDeleter
	// firstSweepDone is closed at the end of the first runOnce()
	// inside Run. Tests wait on this channel instead of
	// time.Sleep(N) to synchronize on the immediate-first-sweep
	// (issue #586 — replaces the liveness-racy time.Sleep pattern
	// with a deterministic done-channel handshake, mirroring the
	// Loop.Done() pattern from the loophealth PR #585 fix). The
	// channel is allocated in NewPreviewGCService so tests can
	// grab a reference to it BEFORE calling Run in a goroutine
	// and start the wait immediately — no race on "has Run
	// started yet". Never closed if Run is never called.
	firstSweepDone chan struct{}
}

func NewPreviewGCService(repo previewRepoForGC, blobs previewBlobDeleter) *PreviewGCService {
	return &PreviewGCService{
		repo:           repo,
		blobs:          blobs,
		firstSweepDone: make(chan struct{}),
	}
}

// FirstSweepDone returns a channel that closes at the end of the
// first runOnce inside Run. Tests use it to synchronize on the
// immediate-first-sweep without racing on time.Sleep(N) (issue
// #586). Always returns the same channel; never closes if Run
// isn't called.
//
// Receivers are free to wait on the returned channel even before
// Run is invoked — the channel is allocated at construction time
// so the goroutine that calls Run in the background and the test
// goroutine that waits on the channel can be scheduled in either
// order without liveness races.
func (s *PreviewGCService) FirstSweepDone() <-chan struct{} {
	return s.firstSweepDone
}

// Run blocks until ctx is cancelled. The first sweep fires
// immediately (operationally useful — when the process restarts
// we don't want to wait `interval` before deleting expired
// previews); subsequent sweeps tick at `interval`.
//
// interval and retention are passed in by main.go (via
// app.RunBackground) so they can be tuned via env vars
// (`PREVIEW_GC_INTERVAL`, `PREVIEW_RETENTION`) without changing
// this struct.
//
// If either duration is non-positive the service refuses to run:
// interval<=0 would busy-loop (ticker fires immediately on every
// iteration) and retention<=0 would compute a future cutoff and
// wipe every preview row. The operator sees a clear log line and
// the GC stays disabled until the env vars are fixed and the
// process restarted. This mirrors LogGCService.Run's safety
// check (log_gc.go:42) — same failure modes, same defense.
//
// The retention parameter is currently unused on this service
// (the per-row expiry is already stamped on each row at upload
// time via Deploy's previewOpts.ExpiresAt). It is kept in the
// signature for parity with LogGCService and as a forward-
// compatible hook in case a future operator wants a global
// retention floor that overrides per-row expiries.
func (s *PreviewGCService) Run(ctx context.Context, interval, _ time.Duration) {
	if interval <= 0 {
		log.Printf("preview_gc: invalid interval=%s; refusing to run", interval)
		return
	}

	// GC tunables for the batched sweep. 10k rows per batch
	// amortizes round-trip cost while bounding worst-case lock
	// duration; 1000 batches/sweep caps a worst-case first-sweep
	// at 10M rows (well above any realistic backlog). Same
	// numbers as log_gc.go:51 so the two GCs have symmetric
	// tail-latency behavior.
	const (
		gcBatchSize  = 10_000
		gcMaxBatches = 1000
	)

	// runOnce is a closure so the immediate-first-sweep path and
	// the ticker path use the same delete-and-log logic. Same
	// shape as log_gc.go:58.
	runOnce := func() {
		// Skip the DELETE roundtrip if we're already shutting
		// down. The repo itself short-circuits on a cancelled
		// ctx, but checking here avoids a wasted pool acquire +
		// log on the shutdown path.
		if ctx.Err() != nil {
			return
		}
		var (
			totalBlobsDeleted int
			totalRowsDeleted  int
			totalBatchesSwept int
		)
		for batch := 0; batch < gcMaxBatches; batch++ {
			if ctx.Err() != nil {
				return
			}
			// Step 1: list the expired preview rows so we
			// can unlink their /registry/ blobs. The
			// partial index `idx_deployments_preview_expires_at`
			// (migration 021) keeps this SELECT cheap even
			// with millions of non-preview rows present.
			refs, err := s.repo.ListExpiredPreviewBlobs(ctx, gcBatchSize)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("preview_gc: list expired blobs failed: %v", err)
				return
			}
			if len(refs) == 0 {
				break // DB has no more expired previews.
			}
			// Step 2: delete the artifact blobs FIRST. A
			// blob-delete failure logs and continues —
			// skipping the row delete for that batch is
			// safer than the reverse order. The operator
			// can re-run the sweep (idempotent) once the
			// underlying storage issue is fixed.
			ids := make([]string, 0, len(refs))
			for _, ref := range refs {
				if delErr := s.blobs.Delete(ctx, ref.TenantID, ref.AppName, ref.ID); delErr != nil {
					log.Printf("preview_gc: deleting artifact blob %s/%s/%s failed: %v", ref.TenantID, ref.AppName, ref.ID, delErr)
					continue
				}
				totalBlobsDeleted++
				ids = append(ids, ref.ID)
			}
			if len(ids) == 0 {
				// All blob deletes failed — bail out
				// of this sweep so the operator sees a
				// log line per batch rather than a tight
				// retry loop.
				log.Printf("preview_gc: all %d blob deletes failed in batch %d; skipping row deletes", len(refs), batch)
				return
			}
			// Step 3: delete the DB rows (and let
			// DeleteExpiredPreviewsByIDs return the
			// actual deleted set so we can log + count).
			deleted, err := s.repo.DeleteExpiredPreviewsByIDs(ctx, ids)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("preview_gc: deleting expired preview rows failed (batch %d): %v", batch, err)
				return
			}
			totalRowsDeleted += len(deleted)
			totalBatchesSwept++
			if len(refs) < gcBatchSize {
				// Last batch was short — DB has no
				// more matching rows.
				break
			}
		}
		if totalBatchesSwept > 0 {
			log.Printf("preview_gc: sweep complete: %d rows + %d blobs deleted across %d batches", totalRowsDeleted, totalBlobsDeleted, totalBatchesSwept)
		}
	}

	// Signal "first runOnce completed" to FirstSweepDone() waiters.
	// The defer-before-runOnce placement guarantees the channel closes
	// even if the first sweep panics; the explicit close after runOnce
	// is redundant with the defer but keeps the happy path obvious.
	// See the firstSweepDone field doc on the struct for rationale.
	var firstSweepOnce sync.Once
	defer func() {
		firstSweepOnce.Do(func() {
			close(s.firstSweepDone)
		})
	}()

	runOnce()
	firstSweepOnce.Do(func() {
		close(s.firstSweepDone)
	})

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
