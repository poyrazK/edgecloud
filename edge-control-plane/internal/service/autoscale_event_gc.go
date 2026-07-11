package service

import (
	"context"
	"log"
	"time"
)

// autoscaleRepoForGC is the subset of *repository.AutoscaleRepository
// used by AutoscaleEventGCService. Defining it locally keeps tests
// mockable without a live DB.
type autoscaleRepoForGC interface {
	DeleteOlderThanBatched(ctx context.Context, retention time.Duration, batchSize, maxBatches int) (int64, error)
}

// AutoscaleEventGCService periodically deletes autoscale_events rows
// older than the configured retention. Mirrors LogGCService verbatim
// (issue #574 retention GC trio). Runs as a background goroutine for
// the lifetime of the control-plane process and exits cleanly when
// ctx is cancelled.
//
// Errors are logged and the loop continues — a transient DB failure
// should not silently halt the GC forever, but the operator should
// also see it.
//
// The optional `sink` records one tick outcome into the
// MetricsAggregator (issue #581). nil sink is a no-op so tests can
// construct the service without an aggregator. The sink is called on
// every sweep tick (success or error) but NOT when the run is
// refused-to-run (zero/negative interval or negative retention) or
// when the context is pre-cancelled — those don't represent a tick
// attempt, and an "alert on never-ticked" rule should fire in that case.
type AutoscaleEventGCService struct {
	repo autoscaleRepoForGC
	sink AutoscaleEventGCSink
}

func NewAutoscaleEventGCService(repo autoscaleRepoForGC, sink AutoscaleEventGCSink) *AutoscaleEventGCService {
	if sink == nil {
		sink = func(int64, bool) {} // nil-safe no-op for tests
	}
	return &AutoscaleEventGCService{repo: repo, sink: sink}
}

// Run blocks until ctx is cancelled. The first sweep fires immediately
// (operationally useful — when the process restarts we don't want to wait
// `interval` before deleting old rows); subsequent sweeps tick at `interval`.
//
// interval and retention are passed in by app.go so they can be tuned via
// env vars (AUTOSCALE_EVENT_GC_INTERVAL, AUTOSCALE_EVENT_RETENTION)
// without changing this struct.
//
// If either duration is non-positive the service refuses to run:
// interval<=0 would busy-loop (ticker fires immediately on every iteration)
// and retention<=0 would compute a future cutoff and wipe the entire
// autoscale_events table. The operator sees a clear log line and the
// GC stays disabled until the env vars are fixed and the process
// restarted.
func (s *AutoscaleEventGCService) Run(ctx context.Context, interval, retention time.Duration) {
	if interval <= 0 || retention <= 0 {
		log.Printf("autoscale_event_gc: invalid interval=%s retention=%s; refusing to run", interval, retention)
		return
	}

	// GC tunables for the batched DELETE. 10k rows per batch amortizes
	// round-trip cost while bounding worst-case lock duration; 1000
	// batches/sweep caps a worst-case first-sweep at 10M rows (well
	// above any realistic backlog).
	const (
		gcBatchSize  = 10_000
		gcMaxBatches = 1000
	)

	// runOnce is a closure so the immediate-first-sweep path and the
	// ticker path use the same delete-and-log logic.
	runOnce := func() {
		// Skip the DELETE roundtrip if we're already shutting down. The
		// repository itself short-circuits on a cancelled ctx, but
		// checking here avoids a wasted pool acquire + log on the
		// shutdown path and keeps the immediate-first-sweep from
		// issuing a DELETE we're about to cancel.
		if ctx.Err() != nil {
			return
		}
		// Pass `retention` (a Duration) to the repo. The repo computes
		// the cutoff server-side via NOW() - make_interval(...), so
		// the DB clock — not the Go process clock — is the time
		// authority. This protects against clock skew between the
		// control plane host and the DB host.
		deleted, err := s.repo.DeleteOlderThanBatched(ctx, retention, gcBatchSize, gcMaxBatches)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down — expected
			}
			log.Printf("autoscale_event_gc: delete failed (retention=%s): %v", retention, err)
			s.sink(0, true) // issue #581 — record the failed tick
			return
		}
		if deleted > 0 {
			log.Printf("autoscale_event_gc: deleted %d rows older than %s", deleted, retention)
		}
		s.sink(deleted, false) // issue #581 — record the successful tick
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
