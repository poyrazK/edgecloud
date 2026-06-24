package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// -----------------------------------------------------------------------
// Mock repo — exercises the GC service without a live DB.
// -----------------------------------------------------------------------

type mockLogGCRepo struct {
	mu    sync.Mutex
	calls []time.Duration // retention durations passed to each DeleteOlderThanBatched call
	err   error
	delay time.Duration // optional sleep before returning (simulates slow DB)
}

func (m *mockLogGCRepo) DeleteOlderThanBatched(ctx context.Context, retention time.Duration, batchSize, maxBatches int) (int64, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, retention)
	if m.err != nil {
		return 0, m.err
	}
	return 0, nil
}

func (m *mockLogGCRepo) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockLogGCRepo) lastRetention() (time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return 0, false
	}
	return m.calls[len(m.calls)-1], true
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

// TestLogGC_DeletesOldRows: Run fires immediately, then once per interval.
// We use a long interval so only the immediate sweep happens in the test
// window, and we cancel the context before the first tick would fire.
func TestLogGC_DeletesOldRows(t *testing.T) {
	repo := &mockLogGCRepo{}
	svc := NewLogGCService(repo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		interval  = 10 * time.Second // far longer than the test duration
		retention = 7 * 24 * time.Hour
	)

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, interval, retention)
		close(done)
	}()

	// The Run loop's immediate sweep runs synchronously before returning
	// to the select, so we don't strictly need to wait — but a small
	// yield makes the assertion deterministic on busy CI.
	time.Sleep(20 * time.Millisecond)

	gotRetention, ok := repo.lastRetention()
	if !ok {
		t.Fatal("DeleteOlderThanBatched was not called on first sweep")
	}
	if got := repo.callCount(); got != 1 {
		t.Errorf("DeleteOlderThanBatched call count = %d, want 1 (interval hasn't elapsed yet)", got)
	}
	if gotRetention != retention {
		t.Errorf("retention = %s, want %s", gotRetention, retention)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after ctx cancellation")
	}
}

// TestLogGC_RetentionFromConfig: different retention values are
// plumbed through to the repo. Verifies the retention parameter
// (now passed as a Duration, not a Go-computed cutoff) is preserved.
func TestLogGC_RetentionFromConfig(t *testing.T) {
	repo := &mockLogGCRepo{}
	svc := NewLogGCService(repo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const interval = 10 * time.Second

	// First run with 1-hour retention.
	go svc.Run(ctx, interval, 1*time.Hour)
	time.Sleep(20 * time.Millisecond)
	got1, _ := repo.lastRetention()
	cancel()
	time.Sleep(20 * time.Millisecond) // give Run a moment to exit

	// Second run with 7-day retention.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go svc.Run(ctx2, interval, 7*24*time.Hour)
	time.Sleep(20 * time.Millisecond)
	got2, _ := repo.lastRetention()
	cancel2()

	if got1 != 1*time.Hour {
		t.Errorf("first retention = %s, want 1h", got1)
	}
	if got2 != 7*24*time.Hour {
		t.Errorf("second retention = %s, want 7d", got2)
	}
}

// TestLogGC_TickerFiresAtInterval: with a short interval, Run should call
// DeleteOlderThan multiple times within a small window. Validates that the
// ticker path is actually wired (not just the immediate sweep).
func TestLogGC_TickerFiresAtInterval(t *testing.T) {
	repo := &mockLogGCRepo{}
	svc := NewLogGCService(repo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		interval  = 30 * time.Millisecond
		retention = 1 * time.Hour
	)

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, interval, retention)
		close(done)
	}()

	// Wait long enough for several ticks. The first sweep is immediate;
	// each subsequent sweep is every 30ms. Over 150ms we expect 4-6 calls.
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	got := repo.callCount()
	if got < 3 {
		t.Errorf("DeleteOlderThan call count = %d, want at least 3 (immediate + 2+ ticks)", got)
	}
}

// TestLogGC_RepoErrorDoesNotStopLoop: a transient DB error is logged and the
// loop continues. The next tick should still attempt the delete.
func TestLogGC_RepoErrorDoesNotStopLoop(t *testing.T) {
	repo := &mockLogGCRepo{err: errors.New("simulated DB outage")}
	svc := NewLogGCService(repo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		interval  = 30 * time.Millisecond
		retention = 1 * time.Hour
	)

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, interval, retention)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Multiple attempts must have happened despite the error.
	if got := repo.callCount(); got < 2 {
		t.Errorf("DeleteOlderThan call count = %d, want >= 2 (loop must continue after errors)", got)
	}
}

// TestLogGC_ZeroIntervalRefusesToRun: a misconfigured LOG_GC_INTERVAL=0
// must not busy-loop. Run should return immediately without touching the
// repo and without scheduling a ticker. This locks in the defense-in-depth
// guard alongside parseDurationEnv in main.go.
func TestLogGC_ZeroIntervalRefusesToRun(t *testing.T) {
	repo := &mockLogGCRepo{}
	svc := NewLogGCService(repo)

	done := make(chan struct{})
	go func() {
		svc.Run(context.Background(), 0, 1*time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return for interval=0; should refuse to start")
	}
	if got := repo.callCount(); got != 0 {
		t.Errorf("DeleteOlderThan call count = %d, want 0 (zero interval must not run)", got)
	}
}

// TestLogGC_NegativeRetentionRefusesToRun: a misconfigured LOG_RETENTION=-1h
// must not run — a negative retention cutoff would land in the future, and
// the resulting "delete every row older than <future>" would wipe the table.
// This guards the boundary in addition to parseDurationEnv.
func TestLogGC_NegativeRetentionRefusesToRun(t *testing.T) {
	repo := &mockLogGCRepo{}
	svc := NewLogGCService(repo)

	done := make(chan struct{})
	go func() {
		svc.Run(context.Background(), time.Hour, -1*time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return for retention=-1h; should refuse to start")
	}
	if got := repo.callCount(); got != 0 {
		t.Errorf("DeleteOlderThan call count = %d, want 0 (negative retention must not run)", got)
	}
}

// TestLogGC_PreemptsOnCancelledContext: if the context is already cancelled
// when a sweep fires (e.g. main() is mid-shutdown), runOnce must skip the
// DELETE roundtrip. The check is at the top of runOnce so the immediate-
// first-sweep path also honors it.
func TestLogGC_PreemptsOnCancelledContext(t *testing.T) {
	repo := &mockLogGCRepo{}
	svc := NewLogGCService(repo)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before Run starts

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Millisecond, 1*time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit; should short-circuit on pre-cancelled ctx")
	}
	if got := repo.callCount(); got != 0 {
		t.Errorf("DeleteOlderThan call count = %d, want 0 (pre-cancelled ctx must skip DELETE)", got)
	}
}
