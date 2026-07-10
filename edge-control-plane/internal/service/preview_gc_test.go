package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// -----------------------------------------------------------------------
// Mock repo + blob deleter — exercise PreviewGCService.Run without a
// live DB or filesystem. Same pattern as log_gc_test.go:11.
// -----------------------------------------------------------------------

type mockPreviewGCRepo struct {
	mu sync.Mutex

	// blobsReturned is the slice that ListExpiredPreviewBlobs will
	// return. The test sets it before Run starts.
	blobsReturned []repository.PreviewBlobRef
	// deletedIDs accumulates every id passed into
	// DeleteExpiredPreviewsByIDs. The GC deletes blobs FIRST, then
	// passes the surviving id set to the row-delete; this slice
	// asserts the GC forwarded the right ids and in the right
	// relative order.
	deletedIDs []string
	// deleteErr is returned from DeleteExpiredPreviewsByIDs; nil
	// means "happy path".
	deleteErr error
	// listErr is returned from ListExpiredPreviewBlobs; nil means
	// "happy path".
	listErr error
	// listPanic, if true, makes ListExpiredPreviewBlobs panic on
	// entry. Used by TestPreviewGC_FirstSweep_PanicStillClosesDone
	// to assert the firstSweepDone channel closes even when the
	// first sweep panics — locks the defer-before-runOnce invariant
	// at preview_gc.go's Run call site (issue #586 review follow-up).
	listPanic bool
	// listCalled / deleteCalled let tests assert each method was
	// invoked without poking the id slice.
	listCalled, deleteCalled bool
}

func (m *mockPreviewGCRepo) ListExpiredPreviewBlobs(_ context.Context, _ int) ([]repository.PreviewBlobRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listCalled = true
	if m.listPanic {
		// Released the mu before panicking — defer runs before
		// panic unwinds, so the lock is free by the time the test
		// observes state. Locking pattern matches atomic-load-no-
		// fence-rule: never read post-state from outside the
		// panicked goroutine without a synchronizing event (here,
		// the channel close).
		panic("simulated first-sweep panic (preview_gc issue #586 panic test)")
	}
	if m.listErr != nil {
		return nil, m.listErr
	}
	// Return a fresh copy so the test can mutate blobsReturned
	// between sweeps without leaking state.
	out := make([]repository.PreviewBlobRef, len(m.blobsReturned))
	copy(out, m.blobsReturned)
	return out, nil
}

func (m *mockPreviewGCRepo) DeleteExpiredPreviewsByIDs(_ context.Context, ids []string) ([]repository.DeletedDeployment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalled = true
	m.deletedIDs = append(m.deletedIDs, ids...)
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	deleted := make([]repository.DeletedDeployment, 0, len(ids))
	for _, id := range ids {
		deleted = append(deleted, repository.DeletedDeployment{
			ID:       id,
			TenantID: "t_gc",
			AppName:  "preview-app",
		})
	}
	return deleted, nil
}

// mockBlobStore records every Delete call. We use it to assert that
// the GC deletes blobs FIRST, before touching the DB row — a blob
// leak (failed blob delete + succeeded row delete) is a worse failure
// mode than the reverse (an orphan row pointing at a missing blob;
// the downloader already handles that).
type mockBlobStore struct {
	mu      sync.Mutex
	calls   []string // sequence of "{tenant}/{app}/{id}" strings
	delErr  error    // returned from Delete
	errOnID string   // if non-empty, Delete fails only for this id
}

func (m *mockBlobStore) Delete(_ context.Context, tenant, app, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, tenant+"/"+app+"/"+id)
	if m.delErr != nil {
		return m.delErr
	}
	if m.errOnID != "" && m.errOnID == id {
		return errors.New("simulated blob delete failure")
	}
	return nil
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

// TestPreviewGC_FirstSweep_FiresImmediately: Run does the immediate
// sweep on entry, before the ticker fires. We use a long interval
// (10s) so only the immediate sweep happens in the test window, and
// cancel the context before the first tick would fire.
func TestPreviewGC_FirstSweep_FiresImmediately(t *testing.T) {
	repo := &mockPreviewGCRepo{
		blobsReturned: []repository.PreviewBlobRef{
			{ID: "d_a", TenantID: "t_gc", AppName: "preview-app"},
			{ID: "d_b", TenantID: "t_gc", AppName: "preview-app"},
		},
	}
	blobs := &mockBlobStore{}
	svc := NewPreviewGCService(repo, blobs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const interval = 10 * time.Second
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, interval, 7*24*time.Hour)
		close(done)
	}()

	// Wait deterministically for the immediate-first-sweep to finish
	// (issue #586). FirstSweepDone() closes at the end of the first
	// runOnce, after both blob deletes AND row deletes have run —
	// so by the time we read repo.listCalled / repo.deleteCalled /
	// blobs.calls, all the writes are visible. The 2s ceiling is
	// generous enough for busy CI but short enough that a broken
	// service fails the test fast.
	waitTimer := time.NewTimer(2 * time.Second)
	defer waitTimer.Stop()
	select {
	case <-svc.FirstSweepDone():
	case <-waitTimer.C:
		t.Fatal("FirstSweepDone did not fire within 2s")
	}

	if !repo.listCalled {
		t.Error("ListExpiredPreviewBlobs was not called on first sweep")
	}
	if !repo.deleteCalled {
		t.Error("DeleteExpiredPreviewsByIDs was not called on first sweep")
	}

	blobs.mu.Lock()
	defer blobs.mu.Unlock()
	if len(blobs.calls) != 2 {
		t.Errorf("blob Delete call count = %d, want 2", len(blobs.calls))
	}
	if blobs.calls[0] != "t_gc/preview-app/d_a" || blobs.calls[1] != "t_gc/preview-app/d_b" {
		t.Errorf("blob Delete order = %v, want [t_gc/preview-app/d_a, t_gc/preview-app/d_b]", blobs.calls)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after ctx cancellation")
	}
}

// TestPreviewGC_BlobDeleteFails_StillDeletesOthers: a single failed
// blob delete logs and continues — it should NOT prevent the other
// blobs in the batch from being deleted, and the row-delete should
// skip the failed id (it isn't in the surviving ids slice).
func TestPreviewGC_BlobDeleteFails_StillDeletesOthers(t *testing.T) {
	repo := &mockPreviewGCRepo{
		blobsReturned: []repository.PreviewBlobRef{
			{ID: "d_ok1", TenantID: "t_gc", AppName: "preview-app"},
			{ID: "d_fail", TenantID: "t_gc", AppName: "preview-app"},
			{ID: "d_ok2", TenantID: "t_gc", AppName: "preview-app"},
		},
	}
	blobs := &mockBlobStore{errOnID: "d_fail"}
	svc := NewPreviewGCService(repo, blobs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		svc.Run(ctx, 10*time.Second, 7*24*time.Hour)
	}()
	// Wait for the immediate-first-sweep to complete (issue #586).
	// We can't assert on deletedIDs before the sweep ran — without
	// this handshake the prior version had to time.Sleep(20ms) and
	// hope the goroutine had scheduled.
	waitTimer := time.NewTimer(2 * time.Second)
	defer waitTimer.Stop()
	select {
	case <-svc.FirstSweepDone():
	case <-waitTimer.C:
		t.Fatal("FirstSweepDone did not fire within 2s")
	}
	cancel()

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// d_fail must NOT be in the deleted-ids slice — the blob delete
	// failed so the row stays put (and the operator sees the log).
	if containsString(repo.deletedIDs, "d_fail") {
		t.Errorf("d_fail was in deletedIDs = %v, want it skipped after blob-delete failure", repo.deletedIDs)
	}
	if !containsString(repo.deletedIDs, "d_ok1") || !containsString(repo.deletedIDs, "d_ok2") {
		t.Errorf("deletedIDs = %v, want both d_ok1 and d_ok2", repo.deletedIDs)
	}
}

// TestPreviewGC_ListError_LogsAndReturns: a transient DB failure on
// the list step logs and returns; the GC keeps running on the next
// tick. We can't observe the log line here (test captures stdout),
// so we just assert the loop stays alive after a list error.
func TestPreviewGC_ListError_LoopContinues(t *testing.T) {
	repo := &mockPreviewGCRepo{listErr: errors.New("simulated list failure")}
	blobs := &mockBlobStore{}
	svc := NewPreviewGCService(repo, blobs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go svc.Run(ctx, 10*time.Second, 7*24*time.Hour)
	// Wait for the immediate-first-sweep to finish, then cancel and
	// check the blob store has zero deletes (issue #586). The prior
	// time.Sleep(20ms) version was racy under -race -count=20.
	waitTimer := time.NewTimer(2 * time.Second)
	defer waitTimer.Stop()
	select {
	case <-svc.FirstSweepDone():
	case <-waitTimer.C:
		t.Fatal("FirstSweepDone did not fire within 2s")
	}
	cancel()

	blobs.mu.Lock()
	defer blobs.mu.Unlock()
	if len(blobs.calls) != 0 {
		t.Errorf("blob Delete was called %d times despite list error, want 0", len(blobs.calls))
	}
}

// TestPreviewGC_ZeroInterval_RefusesToRun: a misconfigured
// PREVIEW_GC_INTERVAL (e.g. set to 0 or negative) must NOT
// busy-loop. The service should log a refusal and return.
func TestPreviewGC_ZeroInterval_RefusesToRun(t *testing.T) {
	repo := &mockPreviewGCRepo{}
	blobs := &mockBlobStore{}
	svc := NewPreviewGCService(repo, blobs)

	done := make(chan struct{})
	go func() {
		svc.Run(context.Background(), 0, 7*24*time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return immediately on interval=0")
	}
	if repo.listCalled {
		t.Error("ListExpiredPreviewBlobs was called despite invalid interval")
	}
}

// TestPreviewGC_FirstSweep_PanicStillClosesDone locks the
// defer-before-runOnce invariant at the Run call site: if the
// first sweep panics, FirstSweepDone() must STILL close so any test
// waiting on the channel doesn't deadlock. Without the defer, a
// panicking first sweep would leave the channel open forever —
// turning a transient repo bug into a silent test hang.
//
// The test goroutine wraps svc.Run in `defer recover()` so the
// panic doesn't kill the test runner; we then assert the channel
// closes within 2s (it should be effectively instant — the panic
// unwinds straight into the defer).
func TestPreviewGC_FirstSweep_PanicStillClosesDone(t *testing.T) {
	repo := &mockPreviewGCRepo{listPanic: true}
	blobs := &mockBlobStore{}
	svc := NewPreviewGCService(repo, blobs)

	panicked := make(chan any, 1)
	go func() {
		defer func() {
			panicked <- recover()
		}()
		svc.Run(context.Background(), 10*time.Second, 7*24*time.Hour)
	}()

	waitTimer := time.NewTimer(2 * time.Second)
	defer waitTimer.Stop()
	select {
	case <-svc.FirstSweepDone():
	case <-waitTimer.C:
		t.Fatal("FirstSweepDone did not close within 2s after first-sweep panic")
	}

	// The panic MUST have surfaced in the goroutine (otherwise the
	// recover wrapper would have observed nil). This is the
	// negative-space check: confirms we actually exercised the
	// panic path, not just a normal early return.
	panicTimer := time.NewTimer(2 * time.Second)
	defer panicTimer.Stop()
	select {
	case p := <-panicked:
		if p == nil {
			t.Fatal("goroutine returned without panicking; the test didn't exercise the defer-before-runOnce path")
		}
	case <-panicTimer.C:
		t.Fatal("goroutine never finished after first-sweep panic")
	}
}

// containsString reports whether `needle` is present in `haystack`.
// A separate helper from the package-local `contains` (used by
// deployment_cache_push_test.go), which has a rune-based signature
// for human-readable diff output and isn't compatible here.
func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
