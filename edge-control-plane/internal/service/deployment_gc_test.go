package service

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// -----------------------------------------------------------------------
// Mock repo — exercises the GC service without a live DB.
// -----------------------------------------------------------------------

type mockDeploymentGCRepo struct {
	mu    sync.Mutex
	calls []deleteOlderThanCall
	// rows is what DeleteOlderThanBatched returns (nil = empty result).
	rows []repository.DeletedDeployment
	err  error
}

type deleteOlderThanCall struct {
	retention  time.Duration
	batchSize  int
	maxBatches int
}

func (m *mockDeploymentGCRepo) DeleteOlderThanBatched(
	ctx context.Context, retention time.Duration, batchSize, maxBatches int,
) ([]repository.DeletedDeployment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, deleteOlderThanCall{retention, batchSize, maxBatches})
	if m.err != nil {
		return nil, m.err
	}
	return m.rows, nil
}

func (m *mockDeploymentGCRepo) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockDeploymentGCRepo) lastCall() (deleteOlderThanCall, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return deleteOlderThanCall{}, false
	}
	return m.calls[len(m.calls)-1], true
}

// -----------------------------------------------------------------------
// Mock ArtifactStore — captures Delete calls and supports error injection.
// -----------------------------------------------------------------------

type deleteCall struct {
	TenantID, AppName, DeploymentID string
}

type mockDeploymentGCArtifactStore struct {
	mu       sync.Mutex
	calls    []deleteCall
	err      error // returned by every Delete call (unless nil)
	notExist bool  // when true, every Delete returns os.ErrNotExist
}

func (m *mockDeploymentGCArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	return nil
}

func (m *mockDeploymentGCArtifactStore) SaveAndHash(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) ([]byte, error) {
	return nil, nil
}

func (m *mockDeploymentGCArtifactStore) SaveFormat(ctx context.Context, tenantID, appName, deploymentID, format string, r io.Reader) error {
	return nil
}

func (m *mockDeploymentGCArtifactStore) Delete(ctx context.Context, tenantID, appName, deploymentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, deleteCall{tenantID, appName, deploymentID})
	if m.notExist {
		return os.ErrNotExist
	}
	return m.err
}

// DeleteFormat is a no-op for the GC: the deployment-row GC only
// removes the .wasm blob. Companion .cwasm cleanup is bound to the
// app lifecycle (AppService.Delete, issue #60), not the row TTL.
func (m *mockDeploymentGCArtifactStore) DeleteFormat(ctx context.Context, tenantID, appName, deploymentID, format string) error {
	return nil
}

func (m *mockDeploymentGCArtifactStore) deleteCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockDeploymentGCArtifactStore) deleteIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.calls))
	for i, c := range m.calls {
		out[i] = c.TenantID + "/" + c.AppName + "/" + c.DeploymentID
	}
	return out
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

// TestDeploymentGC_ImmediateFirstSweep: Run fires immediately on start;
// the first call lands before the first tick. Cancelling before the
// interval elapses keeps the test deterministic without time.Sleep.
func TestDeploymentGC_ImmediateFirstSweep(t *testing.T) {
	repo := &mockDeploymentGCRepo{rows: nil}
	store := &mockDeploymentGCArtifactStore{}
	svc := NewDeploymentGCService(repo, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		interval  = 10 * time.Second
		retention = 7 * 24 * time.Hour
	)

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, interval, retention)
		close(done)
	}()

	// Yield so the immediate-sweep path completes on busy CI.
	time.Sleep(20 * time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after ctx cancellation")
	}

	if got := repo.callCount(); got != 1 {
		t.Errorf("DeleteOlderThanBatched call count = %d, want 1 (immediate sweep only)", got)
	}
	if got := store.deleteCount(); got != 0 {
		t.Errorf("artifactStore.Delete count = %d, want 0 (no rows returned)", got)
	}
	last, ok := repo.lastCall()
	if !ok {
		t.Fatal("repo was not called")
	}
	if last.retention != retention {
		t.Errorf("retention = %s, want %s", last.retention, retention)
	}
	if last.batchSize != 10_000 {
		t.Errorf("batchSize = %d, want 10_000 (hard-coded GC batch)", last.batchSize)
	}
	if last.maxBatches != 1000 {
		t.Errorf("maxBatches = %d, want 1000 (hard-coded GC cap)", last.maxBatches)
	}
}

// TestDeploymentGC_ArtifactsDeletedForEachReturnedRow: when the repo
// returns N rows, the service calls ArtifactStore.Delete once per row
// with the matching (tenantID, appName, id). Locks in the orphan-artifact
// fix from review #329: the GC must not leak /registry blobs.
func TestDeploymentGC_ArtifactsDeletedForEachReturnedRow(t *testing.T) {
	repo := &mockDeploymentGCRepo{rows: []repository.DeletedDeployment{
		{ID: "d_1", TenantID: "t_acme", AppName: "myapp"},
		{ID: "d_2", TenantID: "t_acme", AppName: "myapp"},
		{ID: "d_3", TenantID: "t_beta", AppName: "otherapp"},
	}}
	store := &mockDeploymentGCArtifactStore{}
	svc := NewDeploymentGCService(repo, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, time.Hour, 7*24*time.Hour)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	want := []string{
		"t_acme/myapp/d_1",
		"t_acme/myapp/d_2",
		"t_beta/otherapp/d_3",
	}
	got := store.deleteIDs()
	if len(got) != len(want) {
		t.Fatalf("artifactStore.Delete count = %d, want %d (calls: %v)", len(got), len(want), got)
	}
	for i, id := range want {
		if got[i] != id {
			t.Errorf("Delete call %d = %q, want %q", i, got[i], id)
		}
	}
}

// TestDeploymentGC_ToleratesOsErrNotExistFromArtifactStore: an artifact
// that's already gone (operator rm, race with another sweep) must not
// halt the GC. The service should log nothing and continue cleanly.
func TestDeploymentGC_ToleratesOsErrNotExistFromArtifactStore(t *testing.T) {
	repo := &mockDeploymentGCRepo{rows: []repository.DeletedDeployment{
		{ID: "d_1", TenantID: "t_acme", AppName: "myapp"},
	}}
	store := &mockDeploymentGCArtifactStore{notExist: true}
	svc := NewDeploymentGCService(repo, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, time.Hour, 24*time.Hour)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	// Run must exit cleanly despite the per-row os.ErrNotExist.
	if got := store.deleteCount(); got != 1 {
		t.Errorf("artifactStore.Delete count = %d, want 1", got)
	}
}

// TestDeploymentGC_NonNotExistArtifactErrorKeepsLoopRunning: a transient
// artifact-store error (e.g. permission denied, ENOSPC) is logged and
// does not abort the GC. The DB rows are already committed by the
// DELETE...RETURNING; an orphan blob is bounded by disk capacity and
// can be cleaned up manually. Halting the GC over a non-ErrNotExist
// artifact delete would block future sweeps and leak more rows.
func TestDeploymentGC_NonNotExistArtifactErrorKeepsLoopRunning(t *testing.T) {
	repo := &mockDeploymentGCRepo{rows: []repository.DeletedDeployment{
		{ID: "d_1", TenantID: "t_acme", AppName: "myapp"},
	}}
	store := &mockDeploymentGCArtifactStore{err: errors.New("simulated EACCES")}
	svc := NewDeploymentGCService(repo, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 20*time.Millisecond, 24*time.Hour)
		close(done)
	}()
	// Run multiple ticks so we can verify the loop survives the error.
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done

	// The repo was called multiple times despite the per-row error.
	if got := repo.callCount(); got < 2 {
		t.Errorf("DeleteOlderThanBatched call count = %d, want >= 2 (loop must survive artifact error)", got)
	}
	if got := store.deleteCount(); got < 1 {
		t.Errorf("artifactStore.Delete count = %d, want >= 1", got)
	}
}

// TestDeploymentGC_TickerFiresAtInterval: with a short interval, Run
// calls DeleteOlderThanBatched multiple times. Validates that the
// ticker path is actually wired.
func TestDeploymentGC_TickerFiresAtInterval(t *testing.T) {
	repo := &mockDeploymentGCRepo{}
	store := &mockDeploymentGCArtifactStore{}
	svc := NewDeploymentGCService(repo, store)

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
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if got := repo.callCount(); got < 3 {
		t.Errorf("call count = %d, want >= 3 (immediate + 2+ ticks)", got)
	}
}

// TestDeploymentGC_RepoErrorDoesNotStopLoop: a transient DB error is
// logged and the loop continues. Matches LogGCService's invariant.
func TestDeploymentGC_RepoErrorDoesNotStopLoop(t *testing.T) {
	repo := &mockDeploymentGCRepo{err: errors.New("simulated DB outage")}
	store := &mockDeploymentGCArtifactStore{}
	svc := NewDeploymentGCService(repo, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 30*time.Millisecond, 1*time.Hour)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	if got := repo.callCount(); got < 2 {
		t.Errorf("call count = %d, want >= 2 (loop must survive repo errors)", got)
	}
	if got := store.deleteCount(); got != 0 {
		t.Errorf("artifactStore.Delete count = %d, want 0 (repo errored before returning rows)", got)
	}
}

// TestDeploymentGC_ZeroIntervalRefusesToRun: a misconfigured
// DEPLOY_GC_INTERVAL=0 must not busy-loop. Run returns immediately
// without touching the repo or the artifact store.
func TestDeploymentGC_ZeroIntervalRefusesToRun(t *testing.T) {
	repo := &mockDeploymentGCRepo{}
	store := &mockDeploymentGCArtifactStore{}
	svc := NewDeploymentGCService(repo, store)

	done := make(chan struct{})
	go func() {
		svc.Run(context.Background(), 0, 24*time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return for interval=0; should refuse to start")
	}
	if got := repo.callCount(); got != 0 {
		t.Errorf("call count = %d, want 0 (zero interval must not run)", got)
	}
	if got := store.deleteCount(); got != 0 {
		t.Errorf("artifactStore.Delete count = %d, want 0", got)
	}
}

// TestDeploymentGC_NegativeRetentionRefusesToRun: a misconfigured
// DEPLOY_RETENTION=-1h must not run — a negative cutoff lands in the
// future and would wipe the entire deployments table. This guards the
// boundary alongside the repo-layer retention<=0 check at
// DeploymentRepository.DeleteOlderThanBatched.
func TestDeploymentGC_NegativeRetentionRefusesToRun(t *testing.T) {
	repo := &mockDeploymentGCRepo{}
	store := &mockDeploymentGCArtifactStore{}
	svc := NewDeploymentGCService(repo, store)

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
		t.Errorf("call count = %d, want 0 (negative retention must not run)", got)
	}
}

// TestDeploymentGC_PreemptsOnCancelledContext: if ctx is already
// cancelled when a sweep fires, runOnce must skip the DB roundtrip.
// Matches LogGCService's invariant.
func TestDeploymentGC_PreemptsOnCancelledContext(t *testing.T) {
	repo := &mockDeploymentGCRepo{}
	store := &mockDeploymentGCArtifactStore{}
	svc := NewDeploymentGCService(repo, store)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

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
		t.Errorf("call count = %d, want 0 (pre-cancelled ctx must skip DELETE)", got)
	}
}
