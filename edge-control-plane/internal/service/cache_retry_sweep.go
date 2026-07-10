package service

import (
	"context"
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// cacheRetryRepoForSweep is the subset of *repository.ActiveDeploymentRepository
// used by CacheRetrySweepService. Defining it locally keeps the service
// unit-testable without a live DB — the same pattern as
// previewRepoForGC in preview_gc.go:11 and logEntryRepoForGC in
// log_gc.go:11.
type cacheRetryRepoForSweep interface {
	// ListCacheFailed returns up to `batchSize` rows whose
	// `regions_cache_failed` array is non-empty — the iteration
	// target of every sweep tick. Backed by migration 027's partial
	// btree so a healthy fleet (no stranded pushes) is cheap to scan.
	ListCacheFailed(ctx context.Context, batchSize int) ([]repository.CacheFailedRow, error)
	// AppendRegionsCacheState persists a row's outcome partition
	// (succeeded regions go to regions_cached; still-failing
	// regions re-append to regions_cache_failed via the dedup-merge).
	// Reused from the activation path — no new repo surface.
	AppendRegionsCacheState(ctx context.Context, tenantID, appName string, succeeded, failed []string, ts time.Time) error
	// RemoveFromCacheFailed drops `regions` from regions_cache_failed
	// WITHOUT adding them to regions_cached — used for the
	// configMissing partition (the bytes never made it; the region
	// must remain eligible for a fresh push on the next activate).
	RemoveFromCacheFailed(ctx context.Context, tenantID, appName string, regions []string) error
}

// CacheRetrySweepService (issue #501) periodically re-attempts
// per-region artifact-cache pushes for deployments whose previous push
// attempt landed in regions_cache_failed. On a successful re-push the
// region moves to regions_cached via AppendRegionsCacheState; on a
// repeated push error it stays in regions_cache_failed (the dedup-merge
// in AppendRegionsCacheState is idempotent for the same region); on a
// missing cache configuration the region is dropped from
// regions_cache_failed via RemoveFromCacheFailed (the bytes never
// made it, so the region must remain eligible for a fresh push on the
// next activation — adding it to regions_cached would skip the next
// publishSwap attempt and freeze the broken state).
//
// Runs as a background goroutine for the lifetime of the control-plane
// process and exits cleanly when ctx is cancelled.
//
// Concurrency: only one CacheRetrySweepService.Run is expected per
// control-plane process (wired in app.RunBackground alongside LogGC /
// PreviewGC). Two concurrent sweeps would race on the same rows but
// both AppendRegionsCacheState and RemoveFromCacheFailed are
// idempotent (UNNEST || $N::text[] + array_agg(DISTINCT r)), so the
// worst case is wasted work — never a corrupted array.
//
// Behavior delta vs. publishSwap: publishSwap short-circuits on
// (pusher == nil) || (len(regionArtifactCaches) == 0) (deployment.go
// :1434) and never touches stranded rows from prior activations. The
// sweep, in contrast, actively clears stranded rows whose config is
// gone — those rows would otherwise stay "failed" forever and would
// silently mask a now-fixed cache binary at the next activation.
type CacheRetrySweepService struct {
	repo               cacheRetryRepoForSweep
	pusherGetter       func() artifactCachePusher // late-bound — reads live pusher
	regionCachesGetter func() map[string]string   // late-bound — reads live map
}

// NewCacheRetrySweepService wires the sweep against its three
// collaborators. The two getters are read on every sweep tick (not
// only at construction) so an operator who rotates the cache pusher
// or regionArtifactCaches map at runtime via SetCachePusher /
// SetRegionArtifactCaches sees the new values within one sweep
// interval. Capturing the current values at construction would freeze
// the sweep to the bootstrap config — see the late-binding comment
// in app.New for why cmd/api/main.go calls the setters AFTER New.
//
// The pusherGetter returns the existing artifactCachePusher interface
// (defined alongside httpArtifactCachePusher in cache_pusher.go) —
// reusing the production interface keeps the production wiring
// dependency-free (no adapter) while still allowing tests to mock
// the cache layer without standing up an HTTP server.
func NewCacheRetrySweepService(
	repo cacheRetryRepoForSweep,
	pusherGetter func() artifactCachePusher,
	regionCachesGetter func() map[string]string,
) *CacheRetrySweepService {
	return &CacheRetrySweepService{
		repo:               repo,
		pusherGetter:       pusherGetter,
		regionCachesGetter: regionCachesGetter,
	}
}

// Run blocks until ctx is cancelled. The first sweep fires
// immediately (operationally useful — when the process restarts we
// don't want to wait `interval` before re-attempting stranded pushes);
// subsequent sweeps tick at `interval`.
//
// interval is passed in by app.RunBackground so it can be tuned via
// the env var REGION_CACHE_RETRY_INTERVAL without changing this struct.
//
// If interval is non-positive the service refuses to run: an
// interval<=0 would busy-loop (the immediate-first-sweep always runs,
// then the ticker would fire immediately on the next iteration).
// The operator sees a clear log line and the sweep stays disabled
// until the env var is fixed and the process restarted. This mirrors
// PreviewGCService.Run's safety check (preview_gc.go:97) and
// LogGCService.Run's safety check (log_gc.go:42) — same failure modes,
// same defense.
func (s *CacheRetrySweepService) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		log.Printf("cache_retry_sweep: invalid interval=%s; refusing to run", interval)
		return
	}

	// GC tunables for the batched sweep. 10k rows per batch amortizes
	// round-trip cost while bounding worst-case lock duration; 1000
	// batches/sweep caps a worst-case first-sweep at 10M rows (well
	// above any realistic backlog). Same numbers as preview_gc.go:109
	// and log_gc.go:51 so the three background sweeps have
	// symmetric tail-latency behavior.
	const (
		gcBatchSize  = 10_000
		gcMaxBatches = 1000
	)

	// runOnce is a closure so the immediate-first-sweep path and the
	// ticker path use the same sweep-and-log logic. Same shape as
	// preview_gc.go:117.
	runOnce := func() {
		if ctx.Err() != nil {
			return
		}
		// Read the live config map + pusher ONCE per sweep. Operators
		// may rotate them at runtime; we don't want to hold a stale
		// snapshot for the duration of the sweep, but re-reading per
		// row would be N round-trips to in-process state. A single
		// read per sweep matches what publishSwap does (it captures
		// the field access at deployment.go:1434 once per call).
		regionCaches := s.regionCachesGetter()
		pusher := s.pusherGetter()
		var (
			totalBatchesSwept  int
			totalRowsTouched   int
			totalPushedOK      int
			totalPushedFailed  int
			totalConfigMissing int
		)
		for batch := 0; batch < gcMaxBatches; batch++ {
			if ctx.Err() != nil {
				return
			}
			rows, err := s.repo.ListCacheFailed(ctx, gcBatchSize)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("cache_retry_sweep: list cache-failed rows failed: %v", err)
				// Match preview_gc.go:142 — log and return, the
				// next tick (next interval) re-attempts. A tight
				// retry loop would amplify a transient DB blip into
				// CPU pressure; the operator sees the log line.
				return
			}
			if len(rows) == 0 {
				break // DB has no more cache-failed rows.
			}
			for _, row := range rows {
				if ctx.Err() != nil {
					return
				}
				ok, failed, missing := s.retryRow(ctx, row, pusher, regionCaches)
				totalPushedOK += len(ok)
				totalPushedFailed += len(failed)
				totalConfigMissing += len(missing)
				totalRowsTouched++
			}
			totalBatchesSwept++
			if len(rows) < gcBatchSize {
				// Last batch was short — DB has no more matching rows.
				break
			}
		}
		if totalBatchesSwept > 0 {
			log.Printf("cache_retry_sweep: sweep complete: %d rows touched, %d regions pushed OK, %d regions still failing, %d regions dropped (config missing) across %d batches",
				totalRowsTouched, totalPushedOK, totalPushedFailed, totalConfigMissing, totalBatchesSwept)
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

// retryRow runs the per-row retry loop and persists the outcome.
// Returns (ok, failed, missing) — the three partitions of the input
// row's `regions_cache_failed` slice after the sweep.
//
// Partition rules mirror publishSwap at deployment.go:1455-1476 so
// the SAME skip semantics apply — a missing cache config never
// becomes a permanent "failed" entry on disk:
//
//   - success: pusher.Push returned nil. AppendRegionsCacheState
//     moves the region from regions_cache_failed to regions_cached
//     (via the dedup-merge removing it from the failed half). The
//     next activate's publishSwap cache-skip optimization now
//     correctly fires on this region.
//   - stillFailing: pusher.Push returned an error. Re-append via
//     AppendRegionsCacheState(succeeded=nil, failed=[region]) — the
//     dedup-merge is a no-op for the array contents but exercises the
//     row lock for serialization against any concurrent publishSwap
//     that's mid-flight updating the same row.
//   - configMissing: pusher is nil, or regionCaches[region] is unset
//     or empty. Drop via RemoveFromCacheFailed([region]). The bytes
//     never made it, so we MUST NOT add the region to regions_cached
//     — otherwise the next activate's cache-skip would freeze the
//     broken state and the operator could never recover without a
//     manual row edit.
//
// Each persistence call is independent — a failure on one row's
// append must not affect another's (publishSwap's per-row behavior).
func (s *CacheRetrySweepService) retryRow(
	ctx context.Context,
	row repository.CacheFailedRow,
	pusher artifactCachePusher,
	regionCaches map[string]string,
) (ok, failed, missing []string) {
	var (
		success       []string
		stillFailing  []string
		configMissing []string
	)

	for _, region := range row.RegionsCacheFailed {
		cacheURL, hasCache := regionCaches[region]
		if pusher == nil || !hasCache || cacheURL == "" {
			// Configuration missing — never going to push. Drop
			// from regions_cache_failed cleanly via the dedicated
			// RemoveFromCacheFailed method (do NOT add to
			// regions_cached; see the doc above for why).
			configMissing = append(configMissing, region)
			continue
		}
		if err := pusher.Push(ctx, cacheURL, row.TenantID, row.AppName, row.DeploymentID); err != nil {
			log.Printf("cache_retry_sweep: push failed for region %q (tenant=%s app=%s deployment=%s): %v",
				region, row.TenantID, row.AppName, row.DeploymentID, err)
			stillFailing = append(stillFailing, region)
			continue
		}
		success = append(success, region)
	}

	now := time.Now()
	if len(success) > 0 {
		if err := s.repo.AppendRegionsCacheState(ctx, row.TenantID, row.AppName, success, nil, now); err != nil {
			log.Printf("cache_retry_sweep: append success for %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
	}
	if len(stillFailing) > 0 {
		if err := s.repo.AppendRegionsCacheState(ctx, row.TenantID, row.AppName, nil, stillFailing, now); err != nil {
			log.Printf("cache_retry_sweep: append still-failing for %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
	}
	if len(configMissing) > 0 {
		if err := s.repo.RemoveFromCacheFailed(ctx, row.TenantID, row.AppName, configMissing); err != nil {
			log.Printf("cache_retry_sweep: remove config-missing for %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
	}
	return success, stillFailing, configMissing
}
