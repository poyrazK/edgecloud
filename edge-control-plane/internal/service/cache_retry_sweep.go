package service

import (
	"context"
	"encoding/json"
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
	// The row projection also includes the JSONB
	// `region_cache_retry_count` (migration 028) so the sweep can
	// enforce the per-region retry cap without a second round trip.
	ListCacheFailed(ctx context.Context, batchSize int) ([]repository.CacheFailedRow, error)
	// AppendRegionsCacheState persists a row's outcome partition
	// (succeeded regions go to regions_cached; still-failing
	// regions re-append to regions_cache_failed via the dedup-merge).
	// Reused from the activation path — no new repo surface.
	AppendRegionsCacheState(ctx context.Context, tenantID, appName string, succeeded, failed []string, ts time.Time) error
	// RemoveFromCacheFailed drops `regions` from regions_cache_failed
	// WITHOUT adding them to regions_cached — used for the
	// configMissing and giveUp partitions (the bytes never made it;
	// the region must remain eligible for a fresh push on the next
	// activate after configMissing, or have been given up on after
	// giveUp — in both cases adding the region to regions_cached
	// would mask the broken state).
	RemoveFromCacheFailed(ctx context.Context, tenantID, appName string, regions []string) error
	// IncrementRegionCacheRetryCount (issue #501 retry cap)
	// atomically increments the per-region attempt counter in
	// `region_cache_retry_count` for every region in `regions`.
	// Called on the stillFailing partition so the counter climbs
	// toward MaxAttempts.
	IncrementRegionCacheRetryCount(ctx context.Context, tenantID, appName string, regions []string) error
	// RemoveFromCacheRetryCount drops the per-region entries from
	// `region_cache_retry_count`. Called when a region exits the
	// retry loop (success / configMissing / giveUp) so a future
	// re-entry into regions_cache_failed starts with a fresh
	// counter (the entry is removed; a new failure would re-add it
	// at 1).
	RemoveFromCacheRetryCount(ctx context.Context, tenantID, appName string, regions []string) error
}

// CacheRetrySweepService (issue #501) periodically re-attempts
// per-region artifact-cache pushes for deployments whose previous push
// attempt landed in regions_cache_failed. The sweep partitions each
// row's stranded regions into four buckets:
//
//   - success: pusher.Push returned nil. AppendRegionsCacheState moves
//     the region from regions_cache_failed to regions_cached (via the
//     dedup-merge). The per-region attempt counter is reset (the
//     region is no longer in the failed pool).
//   - stillFailing: pusher.Push returned an error. Re-append via
//     AppendRegionsCacheState and INCREMENT the per-region counter.
//     The dedup-merge is idempotent for the array contents.
//   - configMissing: pusher is nil, or regionCaches[region] is unset
//     or empty. Drop via RemoveFromCacheFailed. The per-region
//     counter is reset (the bytes never made it; the region must
//     remain eligible for a fresh push on the next activate).
//   - giveUp: the per-region counter has reached MaxAttempts. Drop
//     via RemoveFromCacheFailed (NOT added to regions_cached). A
//     WARN log line marks the give-up so operators can investigate
//     (the bytes never made it; the region must remain eligible
//     for a fresh push only on a NEW activation, which is the
//     path that resets the counter). The per-region counter entry
//     is removed.
//
// Runs as a background goroutine for the lifetime of the control-plane
// process and exits cleanly when ctx is cancelled.
//
// Concurrency: only one CacheRetrySweepService.Run is expected per
// control-plane process (wired in app.RunBackground alongside LogGC /
// PreviewGC). Two concurrent sweeps would race on the same rows but
// AppendRegionsCacheState, RemoveFromCacheFailed, and the retry-count
// operations are all idempotent (UNNEST || $N::text[] +
// array_agg(DISTINCT r) for arrays; jsonb - text[] for the counter
// map; jsonb_build_object(k, COALESCE+1) for increments), so the
// worst case is wasted work — never a corrupted array or counter.
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
	maxAttemptsGetter  func() int                 // late-bound — reads live MaxAttempts
	sink               CacheRetrySweepSink        // issue #581 — metrics sink
}

// NewCacheRetrySweepService wires the sweep against its four
// collaborators. The three getters are read on every sweep tick (not
// only at construction) so an operator who rotates the cache pusher,
// the regionArtifactCaches map, or the MaxAttempts knob at runtime
// sees the new values within one sweep interval. Capturing the
// current values at construction would freeze the sweep to the
// bootstrap config — see the late-binding comment in app.New for why
// cmd/api/main.go calls the setters AFTER New.
//
// The pusherGetter returns the existing artifactCachePusher interface
// (defined alongside httpArtifactCachePusher in cache_pusher.go) —
// reusing the production interface keeps the production wiring
// dependency-free (no adapter) while still allowing tests to mock
// the cache layer without standing up an HTTP server.
//
// maxAttemptsGetter is typically wired to a closure that reads
// `cfg.CacheRetry.MaxAttempts` once per tick; a non-positive value
// disables the cap (treat every region as still eligible for
// another attempt). This matches the operator intent of "I want
// retries forever" — the default is 10, but a `MaxAttempts=0` flip
// is the documented escape hatch.
//
// The optional `sink` records one sweep-tick outcome (issue #581).
// nil sink is a no-op so tests can construct the service without an
// aggregator. The sink is called on every sweep tick (success or
// error) but NOT when the run is refused-to-run or when the context
// is pre-cancelled — same rationale as LogGCService.
func NewCacheRetrySweepService(
	repo cacheRetryRepoForSweep,
	pusherGetter func() artifactCachePusher,
	regionCachesGetter func() map[string]string,
	maxAttemptsGetter func() int,
	sink CacheRetrySweepSink,
) *CacheRetrySweepService {
	if sink == nil {
		sink = func(int, int, int, int, int, int, bool) {}
	}
	return &CacheRetrySweepService{
		repo:               repo,
		pusherGetter:       pusherGetter,
		regionCachesGetter: regionCachesGetter,
		maxAttemptsGetter:  maxAttemptsGetter,
		sink:               sink,
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
		// Read the live config map + pusher + max attempts ONCE per
		// sweep. Operators may rotate them at runtime; we don't want
		// to hold a stale snapshot for the duration of the sweep, but
		// re-reading per row would be N round-trips to in-process
		// state. A single read per sweep matches what publishSwap
		// does (it captures the field access at deployment.go:1434
		// once per call).
		regionCaches := s.regionCachesGetter()
		pusher := s.pusherGetter()
		maxAttempts := s.maxAttemptsGetter()
		var (
			totalBatchesSwept  int
			totalRowsTouched   int
			totalPushedOK      int
			totalPushedFailed  int
			totalConfigMissing int
			totalGivenUp       int
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
				s.sink(totalRowsTouched, totalPushedOK, totalPushedFailed, totalConfigMissing, totalGivenUp, totalBatchesSwept, true) // issue #581
				return
			}
			if len(rows) == 0 {
				break // DB has no more cache-failed rows.
			}
			for _, row := range rows {
				if ctx.Err() != nil {
					return
				}
				counts, err := parseRetryCounts(row.RegionCacheRetryCount)
				if err != nil {
					log.Printf("cache_retry_sweep: parse region_cache_retry_count for %s/%s failed: %v (skipping row)",
						row.TenantID, row.AppName, err)
					continue
				}
				ok, failed, missing, givenUp := s.retryRow(ctx, row, pusher, regionCaches, counts, maxAttempts)
				totalPushedOK += len(ok)
				totalPushedFailed += len(failed)
				totalConfigMissing += len(missing)
				totalGivenUp += len(givenUp)
				totalRowsTouched++
			}
			totalBatchesSwept++
			if len(rows) < gcBatchSize {
				// Last batch was short — DB has no more matching rows.
				break
			}
		}
		if totalBatchesSwept > 0 {
			log.Printf("cache_retry_sweep: sweep complete: %d rows touched, %d regions pushed OK, %d regions still failing, %d regions dropped (config missing), %d regions given up across %d batches",
				totalRowsTouched, totalPushedOK, totalPushedFailed, totalConfigMissing, totalGivenUp, totalBatchesSwept)
		}
		// Record per-tick metrics (issue #581). One sink call per
		// sweep, regardless of whether any rows were touched. The
		// error flag is false here because every error path above
		// returns BEFORE this point.
		s.sink(totalRowsTouched, totalPushedOK, totalPushedFailed, totalConfigMissing, totalGivenUp, totalBatchesSwept, false)
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

// parseRetryCounts decodes the JSONB `region_cache_retry_count` map
// (raw bytes from the database) into a `map[string]int`. An empty
// or nil byte slice yields an empty map. A malformed JSON value
// returns an error so the caller can skip the row with a clear log
// line — a corrupt map should not silently disable the cap.
func parseRetryCounts(raw []byte) (map[string]int, error) {
	if len(raw) == 0 {
		return map[string]int{}, nil
	}
	var counts map[string]int
	if err := json.Unmarshal(raw, &counts); err != nil {
		return nil, err
	}
	if counts == nil {
		counts = map[string]int{}
	}
	return counts, nil
}

// retryRow runs the per-row retry loop and persists the outcome.
// Returns (ok, failed, missing, givenUp) — the four partitions of
// the input row's `regions_cache_failed` slice after the sweep.
//
// Partition rules mirror publishSwap at deployment.go:1455-1476
// with the retry-cap add-on (issue #501 follow-up):
//
//   - success: pusher.Push returned nil. AppendRegionsCacheState
//     moves the region from regions_cache_failed to regions_cached
//     (via the dedup-merge removing it from the failed half). The
//     counter entry is removed — the region is no longer in the
//     failed pool.
//
//   - stillFailing: pusher.Push returned an error AND the per-region
//     counter is below MaxAttempts. Re-append via
//     AppendRegionsCacheState(succeeded=nil, failed=[region]) — the
//     dedup-merge is a no-op for the array contents but exercises
//     the row lock. INCREMENT the counter; on the next sweep tick
//     the same region will be re-classified as either stillFailing
//     (if the next push also fails) or giveUp (if the count
//     crosses MaxAttempts).
//
//   - configMissing: pusher is nil, or regionCaches[region] is
//     unset or empty. Drop via RemoveFromCacheFailed([region]).
//     The counter entry is removed (a future entry would re-add
//     it at 1).
//
//   - giveUp: the per-region counter is already >= MaxAttempts. The
//     sweep DOES NOT call pusher.Push — the cap is enforced
//     unconditionally, regardless of whether the cache binary
//     might have recovered in the meantime. Drop via
//     RemoveFromCacheFailed([region]) (the bytes never made it; we
//     MUST NOT add the region to regions_cached). The counter
//     entry is removed (it's been given up on). A WARN log line
//     carries the (tenant, app, deployment, region, count) tuple
//     so operators can investigate.
//
//     The MaxAttempts<=0 escape hatch disables the cap: every region
//     is treated as stillFailing or success, never giveUp. This
//     matches the operator intent of "I want retries forever."
//
// Each persistence call is independent — a failure on one
// partition must not affect another's (publishSwap's per-row
// behavior). The counts map is mutated in place and a shallow copy
// is returned only for tests; the sweep's log totals are computed
// from the four return slices.
func (s *CacheRetrySweepService) retryRow(
	ctx context.Context,
	row repository.CacheFailedRow,
	pusher artifactCachePusher,
	regionCaches map[string]string,
	counts map[string]int,
	maxAttempts int,
) (ok, failed, missing, givenUp []string) {
	var (
		success       []string
		stillFailing  []string
		configMissing []string
		giveUp        []string
	)

	for _, region := range row.RegionsCacheFailed {
		cacheURL, hasCache := regionCaches[region]
		if pusher == nil || !hasCache || cacheURL == "" {
			// Configuration missing — never going to push. Drop
			// from regions_cache_failed cleanly via the dedicated
			// RemoveFromCacheFailed method (do NOT add to
			// regions_cached; see the doc above for why). The
			// counter entry is also removed: a future re-entry
			// would re-add it at 1.
			configMissing = append(configMissing, region)
			delete(counts, region)
			continue
		}
		// Retry cap: if the region has already failed
		// MaxAttempts times (and the cap is enabled), route to
		// giveUp without calling pusher.Push. We log at WARN so
		// operators see the exhaustion, and we still try the
		// OTHER regions on the row.
		if maxAttempts > 0 && counts[region] >= maxAttempts {
			log.Printf("cache_retry_sweep: GIVING UP on region %q (tenant=%s app=%s deployment=%s) — %d consecutive failed attempts (cap=%d); drop from regions_cache_failed and remove the counter entry. Operator should investigate the region cache binary; a new activation will reset the counter and re-arm the retry path.",
				region, row.TenantID, row.AppName, row.DeploymentID, counts[region], maxAttempts)
			giveUp = append(giveUp, region)
			delete(counts, region)
			continue
		}
		if err := pusher.Push(ctx, cacheURL, row.TenantID, row.AppName, row.DeploymentID); err != nil {
			log.Printf("cache_retry_sweep: push failed for region %q (tenant=%s app=%s deployment=%s): %v",
				region, row.TenantID, row.AppName, row.DeploymentID, err)
			stillFailing = append(stillFailing, region)
			continue
		}
		success = append(success, region)
		// Counter entry is removed on success (the region is no
		// longer in the failed pool).
		delete(counts, region)
	}

	now := time.Now()
	if len(success) > 0 {
		if err := s.repo.AppendRegionsCacheState(ctx, row.TenantID, row.AppName, success, nil, now); err != nil {
			log.Printf("cache_retry_sweep: append success for %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
		// Remove the counter entries for successful regions —
		// a re-failure would re-add them at 1.
		if err := s.repo.RemoveFromCacheRetryCount(ctx, row.TenantID, row.AppName, success); err != nil {
			log.Printf("cache_retry_sweep: remove retry-count for success regions of %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
	}
	if len(stillFailing) > 0 {
		if err := s.repo.AppendRegionsCacheState(ctx, row.TenantID, row.AppName, nil, stillFailing, now); err != nil {
			log.Printf("cache_retry_sweep: append still-failing for %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
		// INCREMENT the per-region counter so the next sweep tick
		// sees the bumped count and routes to giveUp once
		// MaxAttempts is reached.
		if err := s.repo.IncrementRegionCacheRetryCount(ctx, row.TenantID, row.AppName, stillFailing); err != nil {
			log.Printf("cache_retry_sweep: increment retry-count for still-failing regions of %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
	}
	if len(configMissing) > 0 {
		if err := s.repo.RemoveFromCacheFailed(ctx, row.TenantID, row.AppName, configMissing); err != nil {
			log.Printf("cache_retry_sweep: remove config-missing for %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
		// Counter entries for config-missing regions are also
		// removed (a future re-entry would re-add them at 1).
		if err := s.repo.RemoveFromCacheRetryCount(ctx, row.TenantID, row.AppName, configMissing); err != nil {
			log.Printf("cache_retry_sweep: remove retry-count for config-missing regions of %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
	}
	if len(giveUp) > 0 {
		// giveUp drops the region from regions_cache_failed via
		// the same path as configMissing — the bytes never made
		// it (the sweep gave up), so the region must not appear
		// in regions_cached. The WARN log line above carries the
		// investigation trail.
		if err := s.repo.RemoveFromCacheFailed(ctx, row.TenantID, row.AppName, giveUp); err != nil {
			log.Printf("cache_retry_sweep: remove giveUp regions for %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
		// Counter entries are already deleted in the loop above;
		// the explicit RemoveFromCacheRetryCount here is a
		// belt-and-suspenders defense in case a future refactor
		// moves the delete() out of the loop.
		if err := s.repo.RemoveFromCacheRetryCount(ctx, row.TenantID, row.AppName, giveUp); err != nil {
			log.Printf("cache_retry_sweep: remove retry-count for giveUp regions of %s/%s failed: %v",
				row.TenantID, row.AppName, err)
		}
	}
	return success, stillFailing, configMissing, giveUp
}
