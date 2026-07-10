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
	// listCalled / deleteCalled let tests assert each method was
	// invoked without poking the id slice.
	listCalled, deleteCalled bool
}

func (m *mockPreviewGCRepo) ListExpiredPreviewBlobs(_ context.Context, _ int) ([]repository.PreviewBlobRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listCalled = true
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
	svc := NewPreviewGCService(repo, blobs, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const interval = 10 * time.Second
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, interval, 7*24*time.Hour)
		close(done)
	}()

	// Immediate sweep runs synchronously before Run returns to the
	// select, so a tiny yield is enough to make the assertions
	// deterministic on busy CI.
	time.Sleep(20 * time.Millisecond)

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
	svc := NewPreviewGCService(repo, blobs, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		svc.Run(ctx, 10*time.Second, 7*24*time.Hour)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	// Give the goroutine a moment to exit; it's already inside
	// runOnce when we cancel so the loop bails at the next
	// ctx.Err() check.
	time.Sleep(20 * time.Millisecond)

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
	svc := NewPreviewGCService(repo, blobs, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go svc.Run(ctx, 10*time.Second, 7*24*time.Hour)
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

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
	svc := NewPreviewGCService(repo, blobs, nil, nil)

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

// ---------------------------------------------------------------------------
// Metrics sink integration (issue #581).
// ---------------------------------------------------------------------------

// recordingPreviewSink records every sink call (per-tick) and every
// blob-failure-recorder call so tests can assert the call sites fired.
type recordingPreviewSink struct {
	mu               sync.Mutex
	sinkCalls        []previewSinkCall
	blobFailureCalls int
}

type previewSinkCall struct {
	blobsDeleted int
	rowsDeleted  int
	batchesSwept int
	hadError     bool
}

func (r *recordingPreviewSink) record(blobsDeleted, rowsDeleted, batchesSwept int, hadError bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinkCalls = append(r.sinkCalls, previewSinkCall{blobsDeleted, rowsDeleted, batchesSwept, hadError})
}

func (r *recordingPreviewSink) recordBlobFailure() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blobFailureCalls++
}

func (r *recordingPreviewSink) sinkCallCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sinkCalls)
}

func (r *recordingPreviewSink) blobFailureCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.blobFailureCalls
}

// makeRecordingPreviewSink returns (PreviewGCSink, PreviewBlobFailureRecorder, recorder).
func makeRecordingPreviewSink() (PreviewGCSink, PreviewBlobFailureRecorder, *recordingPreviewSink) {
	r := &recordingPreviewSink{}
	var sink PreviewGCSink = r.record
	var recorder PreviewBlobFailureRecorder = r.recordBlobFailure
	return sink, recorder, r
}

// TestPreviewGC_RecordsMetrics_HappyPath: a 2-blob sweep emits one sink
// call with (blobsDeleted=2, rowsDeleted=2, batchesSwept=1, hadError=false).
func TestPreviewGC_RecordsMetrics_HappyPath(t *testing.T) {
	repo := &mockPreviewGCRepo{
		blobsReturned: []repository.PreviewBlobRef{
			{ID: "d_a", TenantID: "t_gc", AppName: "preview-app"},
			{ID: "d_b", TenantID: "t_gc", AppName: "preview-app"},
		},
	}
	blobs := &mockBlobStore{}
	sink, recorder, rec := makeRecordingPreviewSink()
	svc := NewPreviewGCService(repo, blobs, sink, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		svc.Run(ctx, 10*time.Second, 7*24*time.Hour)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	if got := rec.sinkCallCount(); got != 1 {
		t.Fatalf("sink call count = %d, want 1", got)
	}
	got := rec.sinkCalls[0]
	if got.blobsDeleted != 2 || got.rowsDeleted != 2 || got.batchesSwept != 1 || got.hadError {
		t.Errorf("sink call = %+v, want {2,2,1,false}", got)
	}
	if got := rec.blobFailureCount(); got != 0 {
		t.Errorf("blobFailureCount = %d, want 0", got)
	}
}

// TestPreviewGC_RecordsMetrics_BlobFailureCountedSeparately: a 3-blob
// sweep with one failed blob delete records one sink call (with the
// 2 successful blobs + 2 successful rows) AND one blobFailureRecorder
// call.
func TestPreviewGC_RecordsMetrics_BlobFailureCountedSeparately(t *testing.T) {
	repo := &mockPreviewGCRepo{
		blobsReturned: []repository.PreviewBlobRef{
			{ID: "d_ok1", TenantID: "t_gc", AppName: "preview-app"},
			{ID: "d_fail", TenantID: "t_gc", AppName: "preview-app"},
			{ID: "d_ok2", TenantID: "t_gc", AppName: "preview-app"},
		},
	}
	blobs := &mockBlobStore{errOnID: "d_fail"}
	sink, recorder, rec := makeRecordingPreviewSink()
	svc := NewPreviewGCService(repo, blobs, sink, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		svc.Run(ctx, 10*time.Second, 7*24*time.Hour)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	if got := rec.sinkCallCount(); got != 1 {
		t.Fatalf("sink call count = %d, want 1", got)
	}
	got := rec.sinkCalls[0]
	if got.blobsDeleted != 2 || got.rowsDeleted != 2 || got.batchesSwept != 1 || got.hadError {
		t.Errorf("sink call = %+v, want {2,2,1,false}", got)
	}
	if got := rec.blobFailureCount(); got != 1 {
		t.Errorf("blobFailureCount = %d, want 1", got)
	}
}

// TestPreviewGC_RecordsMetrics_AllBlobsFailed_IncrementsErrors: when all
// blobs in a batch fail, the sweep bails (no row delete) and records one
// sink call with hadError=true AND N blobFailureRecorder calls.
func TestPreviewGC_RecordsMetrics_AllBlobsFailed_IncrementsErrors(t *testing.T) {
	repo := &mockPreviewGCRepo{
		blobsReturned: []repository.PreviewBlobRef{
			{ID: "d_a", TenantID: "t_gc", AppName: "preview-app"},
			{ID: "d_b", TenantID: "t_gc", AppName: "preview-app"},
		},
	}
	blobs := &mockBlobStore{delErr: errors.New("blob store down")}
	sink, recorder, rec := makeRecordingPreviewSink()
	svc := NewPreviewGCService(repo, blobs, sink, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		svc.Run(ctx, 10*time.Second, 7*24*time.Hour)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	if got := rec.sinkCallCount(); got != 1 {
		t.Fatalf("sink call count = %d, want 1", got)
	}
	got := rec.sinkCalls[0]
	if !got.hadError {
		t.Error("sink call hadError = false, want true on all-blobs-failed")
	}
	if got := rec.blobFailureCount(); got != 2 {
		t.Errorf("blobFailureCount = %d, want 2", got)
	}
}

// TestPreviewGC_RecordsMetrics_ListError_IncrementsErrors: a ListExpiredPreviewBlobs
// failure records one sink call with hadError=true and 0 blobFailureRecorder calls.
func TestPreviewGC_RecordsMetrics_ListError_IncrementsErrors(t *testing.T) {
	repo := &mockPreviewGCRepo{listErr: errors.New("simulated list failure")}
	blobs := &mockBlobStore{}
	sink, recorder, rec := makeRecordingPreviewSink()
	svc := NewPreviewGCService(repo, blobs, sink, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		svc.Run(ctx, 10*time.Second, 7*24*time.Hour)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	if got := rec.sinkCallCount(); got != 1 {
		t.Fatalf("sink call count = %d, want 1", got)
	}
	got := rec.sinkCalls[0]
	if !got.hadError {
		t.Error("sink call hadError = false, want true on list error")
	}
	if got := rec.blobFailureCount(); got != 0 {
		t.Errorf("blobFailureCount = %d, want 0", got)
	}
}

// TestPreviewGC_ZeroInterval_NoMetrics: refused-to-run (interval<=0) does
// NOT bump any metrics.
func TestPreviewGC_ZeroInterval_NoMetrics(t *testing.T) {
	repo := &mockPreviewGCRepo{}
	blobs := &mockBlobStore{}
	sink, recorder, rec := makeRecordingPreviewSink()
	svc := NewPreviewGCService(repo, blobs, sink, recorder)

	done := make(chan struct{})
	go func() {
		svc.Run(context.Background(), 0, 7*24*time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on interval=0")
	}
	if got := rec.sinkCallCount(); got != 0 {
		t.Errorf("sink call count = %d, want 0 (refused-to-run must not tick)", got)
	}
	if got := rec.blobFailureCount(); got != 0 {
		t.Errorf("blobFailureCount = %d, want 0", got)
	}
}

// TestPreviewGC_NilSink_NoPanic: passing nil sinks to NewPreviewGCService
// must not panic. The constructor nil-guards.
func TestPreviewGC_NilSink_NoPanic(t *testing.T) {
	repo := &mockPreviewGCRepo{
		blobsReturned: []repository.PreviewBlobRef{
			{ID: "d_a", TenantID: "t_gc", AppName: "preview-app"},
		},
	}
	blobs := &mockBlobStore{}
	svc := NewPreviewGCService(repo, blobs, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 30*time.Millisecond, 7*24*time.Hour)
		close(done)
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit on nil sinks")
	}
}
