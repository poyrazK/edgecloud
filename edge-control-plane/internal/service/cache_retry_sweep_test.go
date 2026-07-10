package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// -----------------------------------------------------------------------
// Mock repo + pusher — exercise CacheRetrySweepService.Run without a
// live DB or HTTP cache. Mirrors the preview_gc_test.go mock-shape
// pattern (mockPreviewGCRepo + mockBlobStore).
// -----------------------------------------------------------------------

// mockCacheRetryRepo satisfies cacheRetryRepoForSweep. Mutex-guarded
// because Run can be tested with multiple ticks; we keep the GC's
// append / remove intent observable through the call-recording
// slices.
type mockCacheRetryRepo struct {
	mu sync.Mutex

	// listResult is what ListCacheFailed returns per call. Tests
	// mutate this between ticks by polling a slice with channels or
	// by sequencing the timeline with a short interval.
	listResult []repository.CacheFailedRow
	// listErr returned from ListCacheFailed; nil means "happy path".
	listErr error
	// listCalledCount increments on every ListCacheFailed call so
	// tests can assert the loop retried after a transient error.
	listCalledCount int

	// captured calls
	appendCalls         []appendRegionsCacheStateCall
	removeCalls         []removeFromCacheFailedCall
	incrementCountCalls []incrementRegionCacheRetryCountCall
	removeCountCalls    []removeFromCacheRetryCountCall

	// appendErr / removeErr override the happy-path nil return for
	// a single call to verify the loop logs and continues.
	appendErr      error
	removeErr      error
	incrementErr   error
	removeCountErr error
}

type appendRegionsCacheStateCall struct {
	TenantID, AppName string
	Succeeded, Failed []string
}

type removeFromCacheFailedCall struct {
	TenantID, AppName string
	Regions           []string
}

type incrementRegionCacheRetryCountCall struct {
	TenantID, AppName string
	Regions           []string
}

type removeFromCacheRetryCountCall struct {
	TenantID, AppName string
	Regions           []string
}

func (m *mockCacheRetryRepo) ListCacheFailed(_ context.Context, _ int) ([]repository.CacheFailedRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listCalledCount++
	if m.listErr != nil {
		return nil, m.listErr
	}
	// Return a fresh copy so the test can mutate listResult between
	// sweeps without leaking state.
	out := make([]repository.CacheFailedRow, len(m.listResult))
	copy(out, m.listResult)
	return out, nil
}

func (m *mockCacheRetryRepo) AppendRegionsCacheState(_ context.Context, tenant, app string, succ, fail []string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendCalls = append(m.appendCalls, appendRegionsCacheStateCall{tenant, app, succ, fail})
	return m.appendErr
}

func (m *mockCacheRetryRepo) RemoveFromCacheFailed(_ context.Context, tenant, app string, regions []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeCalls = append(m.removeCalls, removeFromCacheFailedCall{tenant, app, regions})
	return m.removeErr
}

func (m *mockCacheRetryRepo) IncrementRegionCacheRetryCount(_ context.Context, tenant, app string, regions []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incrementCountCalls = append(m.incrementCountCalls, incrementRegionCacheRetryCountCall{tenant, app, regions})
	return m.incrementErr
}

func (m *mockCacheRetryRepo) RemoveFromCacheRetryCount(_ context.Context, tenant, app string, regions []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeCountCalls = append(m.removeCountCalls, removeFromCacheRetryCountCall{tenant, app, regions})
	return m.removeCountErr
}

// mockArtifactCachePusher satisfies artifactCachePusher. err is the result
// every Push returns; tests set it to nil for happy-path and to a
// non-nil error to simulate a transient cache-binary failure.
type mockArtifactCachePusher struct {
	mu    sync.Mutex
	calls []pushCall
	err   error
}

type pushCall struct {
	CacheURL, TenantID, AppName, DeploymentID string
}

func (m *mockArtifactCachePusher) Push(_ context.Context, url, tenant, app, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, pushCall{url, tenant, app, id})
	return m.err
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

// TestCacheRetrySweep_NoRows_NoWork asserts the happy-path empty-fleet
// case: a row-less DB produces exactly one ListCacheFailed call and
// no AppendRegionsCacheState / RemoveFromCacheFailed calls.
func TestCacheRetrySweep_NoRows_NoWork(t *testing.T) {
	repo := &mockCacheRetryRepo{}
	pusher := &mockArtifactCachePusher{}

	var pusherPtr artifactCachePusher = pusher
	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{"fra": "http://cache.fra"} },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	// Immediate-first-sweep runs synchronously; a tiny yield is
	// enough to make the assertions deterministic.
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.listCalledCount != 1 {
		t.Errorf("listCalledCount = %d, want 1 (immediate first sweep only)", repo.listCalledCount)
	}
	if len(repo.appendCalls) != 0 {
		t.Errorf("appendCalls = %d, want 0 on empty fleet", len(repo.appendCalls))
	}
	if len(repo.removeCalls) != 0 {
		t.Errorf("removeCalls = %d, want 0 on empty fleet", len(repo.removeCalls))
	}
	pusher.mu.Lock()
	defer pusher.mu.Unlock()
	if len(pusher.calls) != 0 {
		t.Errorf("pusher.calls = %d, want 0 on empty fleet", len(pusher.calls))
	}
}

// TestCacheRetrySweep_OneRowOneRegion_Succeeds_MovesToCached covers
// the simplest happy path: one region stranded, pusher nil-error,
// map has the region. Expect one AppendRegionsCacheState call with
// `succeeded=[region]`. The dedup-merge in AppendRegionsCacheState
// atomically moves the region from regions_cache_failed to
// regions_cached.
func TestCacheRetrySweep_OneRowOneRegion_Succeeds_MovesToCached(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{{
			TenantID: "t_test", AppName: "myapp", DeploymentID: "d_v1",
			RegionsCacheFailed: []string{"fra"},
		}},
	}
	pusher := &mockArtifactCachePusher{} // err = nil
	regionCaches := map[string]string{"fra": "http://cache.fra"}

	var pusherPtr artifactCachePusher = pusher
	svc := NewCacheRetrySweepService(repo, func() artifactCachePusher { return pusherPtr }, func() map[string]string { return regionCaches }, func() int { return 10 })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.appendCalls) != 1 {
		t.Fatalf("appendCalls = %d, want 1", len(repo.appendCalls))
	}
	got := repo.appendCalls[0]
	if got.TenantID != "t_test" || got.AppName != "myapp" {
		t.Errorf("appendCall target = %s/%s, want t_test/myapp", got.TenantID, got.AppName)
	}
	if !reflect.DeepEqual(got.Succeeded, []string{"fra"}) || len(got.Failed) != 0 {
		t.Errorf("appendCall partition = success=%v fail=%v, want success=[fra] fail=[]", got.Succeeded, got.Failed)
	}
	if len(repo.removeCalls) != 0 {
		t.Errorf("removeCalls = %d, want 0 on success", len(repo.removeCalls))
	}
	pusher.mu.Lock()
	defer pusher.mu.Unlock()
	if len(pusher.calls) != 1 {
		t.Fatalf("pusher.calls = %d, want 1", len(pusher.calls))
	}
	if pusher.calls[0].CacheURL != "http://cache.fra" || pusher.calls[0].TenantID != "t_test" ||
		pusher.calls[0].AppName != "myapp" || pusher.calls[0].DeploymentID != "d_v1" {
		t.Errorf("pusher call = %+v, want {http://cache.fra t_test myapp d_v1}", pusher.calls[0])
	}
}

// TestCacheRetrySweep_OneRowOneRegion_FailsTwice_RemainsInCacheFailed
// covers the simplest transient-failure retry: pusher returns an
// error every time. Expect one AppendRegionsCacheState with
// `failed=[region]` and zero RemoveFromCacheFailed (a single-region
// retry that stays failed). The dedup-merge makes this idempotent
// across repeated sweep ticks.
func TestCacheRetrySweep_OneRowOneRegion_FailsTwice_RemainsInCacheFailed(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{{
			TenantID: "t_test", AppName: "myapp", DeploymentID: "d_v1",
			RegionsCacheFailed: []string{"iad"},
		}},
	}
	pusher := &mockArtifactCachePusher{err: errors.New("503 Service Unavailable")}
	regionCaches := map[string]string{"iad": "http://cache.iad"}

	var pusherPtr artifactCachePusher = pusher
	svc := NewCacheRetrySweepService(repo, func() artifactCachePusher { return pusherPtr }, func() map[string]string { return regionCaches }, func() int { return 10 })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.appendCalls) != 1 {
		t.Fatalf("appendCalls = %d, want 1", len(repo.appendCalls))
	}
	got := repo.appendCalls[0]
	if len(got.Succeeded) != 0 || !reflect.DeepEqual(got.Failed, []string{"iad"}) {
		t.Errorf("partition = success=%v fail=%v, want success=[] fail=[iad]", got.Succeeded, got.Failed)
	}
	if len(repo.removeCalls) != 0 {
		t.Errorf("removeCalls = %d, want 0 on transient failure (region stays failed)", len(repo.removeCalls))
	}
}

// TestCacheRetrySweep_OneRowOneRegion_MissingCacheConfig_RemovedFromCacheFailed
// covers the "operator removed the region from config" partition.
// The pusher is never called; RemoveFromCacheFailed removes the
// region cleanly (NOT via AppendRegionsCacheState, which would
// have wrongly added it to regions_cached).
func TestCacheRetrySweep_OneRowOneRegion_MissingCacheConfig_RemovedFromCacheFailed(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{{
			TenantID: "t_test", AppName: "myapp", DeploymentID: "d_v1",
			RegionsCacheFailed: []string{"sin"},
		}},
	}
	pusher := &mockArtifactCachePusher{}
	regionCaches := map[string]string{} // sin absent — config gap

	var pusherPtr artifactCachePusher = pusher
	svc := NewCacheRetrySweepService(repo, func() artifactCachePusher { return pusherPtr }, func() map[string]string { return regionCaches }, func() int { return 10 })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.removeCalls) != 1 {
		t.Fatalf("removeCalls = %d, want 1 (configMissing -> RemoveFromCacheFailed)", len(repo.removeCalls))
	}
	rm := repo.removeCalls[0]
	if rm.TenantID != "t_test" || rm.AppName != "myapp" ||
		!reflect.DeepEqual(rm.Regions, []string{"sin"}) {
		t.Errorf("removeCall = %+v, want {t_test myapp [sin]}", rm)
	}
	if len(repo.appendCalls) != 0 {
		t.Errorf("appendCalls = %d, want 0 (configMissing must NOT add to regions_cached)", len(repo.appendCalls))
	}
	pusher.mu.Lock()
	defer pusher.mu.Unlock()
	if len(pusher.calls) != 0 {
		t.Errorf("pusher.calls = %d, want 0 (no config = no push attempt)", len(pusher.calls))
	}
}

// TestCacheRetrySweep_PusherNil_AllRegionsRemovedAsConfigMissing
// covers the asymmetric behavior delta vs. publishSwap: when the
// pusher is nil at sweep time (operator disabled cache mid-flight),
// every stranded row's regions are removed via RemoveFromCacheFailed
// instead of left stuck "failed" forever. This is a behavioral delta
// — publishSwap short-circuits on a nil pusher without touching
// stranded rows; the sweep instead treats the nil pusher as
// "config missing" for every row it has.
func TestCacheRetrySweep_PusherNil_AllRegionsRemovedAsConfigMissing(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{{
			TenantID: "t_test", AppName: "myapp", DeploymentID: "d_v1",
			RegionsCacheFailed: []string{"fra", "iad"},
		}},
	}
	regionCaches := map[string]string{"fra": "http://cache.fra", "iad": "http://cache.iad"}

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return nil }, // pusher disabled
		func() map[string]string { return regionCaches },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.removeCalls) != 1 {
		t.Fatalf("removeCalls = %d, want 1", len(repo.removeCalls))
	}
	gotRegions := append([]string(nil), repo.removeCalls[0].Regions...)
	if !reflect.DeepEqual(gotRegions, []string{"fra", "iad"}) {
		t.Errorf("removeCall regions = %v, want [fra iad]", gotRegions)
	}
	if len(repo.appendCalls) != 0 {
		t.Errorf("appendCalls = %d, want 0 (nil pusher -> never regions_cached)", len(repo.appendCalls))
	}
}

// TestCacheRetrySweep_MixedRowOutcomes is the partition-coverage
// test: one row with three regions covering all three branches.
// Expect one AppendRegionsCacheState for success, one for
// still-failing, and one RemoveFromCacheFailed for configMissing.
// Verifies the per-row partition logic without overlap.
func TestCacheRetrySweep_MixedRowOutcomes(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{{
			TenantID: "t_test", AppName: "myapp", DeploymentID: "d_v1",
			RegionsCacheFailed: []string{"fra", "iad", "sin"},
		}},
	}
	// Only `iad` errors; `fra` succeeds; `sin` is missing from the
	// regionCaches map (config gap). Default mockArtifactCachePusher
	// applies the same err to every call, so we use a per-URL
	// regionRoutingPusher to vary the outcome by region.
	regionCaches := map[string]string{
		"fra": "http://cache.fra",
		"iad": "http://cache.iad",
		// sin deliberately absent
	}
	customPusher := &regionRoutingPusher{results: map[string]error{
		"http://cache.fra": nil,
		"http://cache.iad": errors.New("503 Service Unavailable"),
	}}
	var pusherPtr artifactCachePusher = customPusher

	svc := NewCacheRetrySweepService(repo, func() artifactCachePusher { return pusherPtr }, func() map[string]string { return regionCaches }, func() int { return 10 })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// We expect two AppendRegionsCacheState calls: one with
	// success=[fra], and one with failed=[iad]. The order is
	// determined by the input order to retryRow (fra, iad, sin) —
	// the sweeper writes success first, then still-failing. So the
	// appendCalls slice is [success, failed].
	if len(repo.appendCalls) != 2 {
		t.Fatalf("appendCalls = %d, want 2 (success + still-failing)", len(repo.appendCalls))
	}
	if !reflect.DeepEqual(repo.appendCalls[0].Succeeded, []string{"fra"}) || len(repo.appendCalls[0].Failed) != 0 {
		t.Errorf("appendCalls[0] = %+v, want success=[fra] fail=[]", repo.appendCalls[0])
	}
	if len(repo.appendCalls[1].Succeeded) != 0 || !reflect.DeepEqual(repo.appendCalls[1].Failed, []string{"iad"}) {
		t.Errorf("appendCalls[1] = %+v, want success=[] fail=[iad]", repo.appendCalls[1])
	}
	if len(repo.removeCalls) != 1 {
		t.Fatalf("removeCalls = %d, want 1 (configMissing)", len(repo.removeCalls))
	}
	if !reflect.DeepEqual(repo.removeCalls[0].Regions, []string{"sin"}) {
		t.Errorf("removeCalls[0].Regions = %v, want [sin]", repo.removeCalls[0].Regions)
	}
	customPusher.mu.Lock()
	defer customPusher.mu.Unlock()
	if len(customPusher.calls) != 2 {
		t.Errorf("pusher.calls = %d, want 2 (fra + iad; sin must NOT be pushed)", len(customPusher.calls))
	}
}

// regionRoutingPusher is a per-region mock pusher that returns a
// per-region error from the `results` map. Used by the mixed-outcome
// test where one row has different outcomes per region.
type regionRoutingPusher struct {
	mu      sync.Mutex
	calls   []pushCall
	results map[string]error // keyed by cacheBaseURL or region
}

func (p *regionRoutingPusher) Push(_ context.Context, url, tenant, app, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, pushCall{url, tenant, app, id})
	// Look up by URL — the test sets regionCache["fra"]="http://cache.fra"
	// and results["http://cache.fra"]=nil, etc. Falls back to the
	// map's last entry on collision (only one URL per region here).
	if e, ok := p.results[url]; ok {
		return e
	}
	return errors.New("regionRoutingPusher: no result configured for " + url)
}

// TestCacheRetrySweep_RepoListError_LoopSurvives asserts that a
// transient DB error on ListCacheFailed logs and returns from the
// runOnce, then the loop ticks again on the next interval and
// retries the list. We use a 30ms interval and observe multiple
// listCalledCount increments within the test window.
func TestCacheRetrySweep_RepoListError_LoopSurvives(t *testing.T) {
	repo := &mockCacheRetryRepo{listErr: errors.New("simulated db blip")}
	pusher := &mockArtifactCachePusher{}
	var pusherPtr artifactCachePusher = pusher

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{"fra": "http://cache.fra"} },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 30*time.Millisecond)
		close(done)
	}()
	// Let the loop tick ~3 times before cancelling.
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.listCalledCount < 2 {
		t.Errorf("listCalledCount = %d, want >= 2 (loop must retry across ticks)", repo.listCalledCount)
	}
}

// TestCacheRetrySweep_ZeroInterval_RefusesToRun asserts the
// busy-loop defense: a misconfigured REGION_CACHE_RETRY_INTERVAL=0
// causes Run to log and return without ever calling the repo.
func TestCacheRetrySweep_ZeroInterval_RefusesToRun(t *testing.T) {
	repo := &mockCacheRetryRepo{}
	pusher := &mockArtifactCachePusher{}
	var pusherPtr artifactCachePusher = pusher

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{} },
		func() int { return 10 },
	)

	done := make(chan struct{})
	go func() {
		svc.Run(context.Background(), 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return immediately on interval=0")
	}
	if repo.listCalledCount != 0 {
		t.Errorf("listCalledCount = %d, want 0 on invalid interval", repo.listCalledCount)
	}
}

// TestCacheRetrySweep_PreemptsOnCancelledContext asserts that
// Run exits cleanly when ctx is cancelled, even if the immediate
// first sweep is mid-iteration.
func TestCacheRetrySweep_PreemptsOnCancelledContext(t *testing.T) {
	repo := &mockCacheRetryRepo{} // empty results — Run returns after one tick of sweep
	pusher := &mockArtifactCachePusher{}
	var pusherPtr artifactCachePusher = pusher

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{} },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	// Yield so the immediate sweep completes, then cancel before
	// the first tick fires.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after ctx cancellation")
	}
}

// TestCacheRetrySweep_ImmediateFirstSweep asserts that Run kicks the
// sweep synchronously before the ticker. We pick a long interval and
// cancel before any tick would fire, then assert listCalledCount == 1.
func TestCacheRetrySweep_ImmediateFirstSweep(t *testing.T) {
	repo := &mockCacheRetryRepo{}
	pusher := &mockArtifactCachePusher{}
	var pusherPtr artifactCachePusher = pusher

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{} },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.listCalledCount != 1 {
		t.Errorf("listCalledCount = %d, want 1 (only the immediate first sweep before cancel)", repo.listCalledCount)
	}
}

// TestCacheRetrySweep_ConcurrentPublishSwap_AppendRegionsCacheStateCalled
// simulates a concurrent publishSwap in another goroutine that pushes
// an AppendRegionsCacheState while the sweep is mid-iteration. Both
// must complete; the dedup-merge in AppendRegionsCacheState is what
// makes this safe (the array contents collapse server-side via
// `unnest || $N::text[]` + DISTINCT). The test asserts both calls
// were captured by the mock without asserting contents, because the
// safe interleaving is a server-side property.
func TestCacheRetrySweep_ConcurrentPublishSwap_AppendRegionsCacheStateCalled(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{{
			TenantID: "t_test", AppName: "myapp", DeploymentID: "d_v1",
			RegionsCacheFailed: []string{"fra"},
		}},
	}
	pusher := &mockArtifactCachePusher{}
	var pusherPtr artifactCachePusher = pusher

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{"fra": "http://cache.fra"} },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	// Interleave a concurrent AppendRegionsCacheState from a
	// fictional publishSwap-side caller. The sweep must complete
	// without race-detector complaints.
	go func() {
		_ = repo.AppendRegionsCacheState(ctx, "t_test", "myapp",
			[]string{"iad"}, nil, time.Now())
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// Expect: at least one sweep-side AppendRegionsCacheState
	// (success=[fra]) AND the concurrent caller's call
	// (success=[iad]). The order is racy; just count.
	totalAppends := 0
	for _, c := range repo.appendCalls {
		if c.TenantID != "t_test" || c.AppName != "myapp" {
			continue
		}
		totalAppends++
	}
	if totalAppends != 2 {
		t.Errorf("totalAppends (sweep + concurrent) = %d, want 2", totalAppends)
	}
	// Sweep's pusher call for "fra" must still have been made —
	// a regression that drops the sweep's pusher call under
	// concurrent repo pressure should be caught here.
	pusher.mu.Lock()
	defer pusher.mu.Unlock()
	if len(pusher.calls) != 1 {
		t.Errorf("pusher.calls = %d, want 1 (sweep's push for fra; concurrent caller is repo-only)", len(pusher.calls))
	}
	if pusher.calls[0].CacheURL != "http://cache.fra" {
		t.Errorf("pusher.calls[0].CacheURL = %q, want %q", pusher.calls[0].CacheURL, "http://cache.fra")
	}
}

// -----------------------------------------------------------------------
// Retry-cap tests (issue #501 follow-up — MaxAttempts + per-region counter).
// These exercise the giveUp partition + the stillFailing/success/missing
// counter mutations. Concurrency-safe mocks above are reused.
// -----------------------------------------------------------------------

// rowWithCounts builds a CacheFailedRow with a populated
// RegionCacheRetryCount JSONB byte slice. Mirrors what
// the repo's ListCacheFailed SELECT returns from
// `region_cache_retry_count::jsonb`.
func rowWithCounts(tenant, app, deploymentID string, failed []string, counts map[string]int) repository.CacheFailedRow {
	var raw []byte
	if len(counts) == 0 {
		raw = []byte("{}")
	} else {
		b, err := json.Marshal(counts)
		if err != nil {
			panic(err) // tests only
		}
		raw = b
	}
	return repository.CacheFailedRow{
		TenantID:              tenant,
		AppName:               app,
		DeploymentID:          deploymentID,
		RegionsCacheFailed:    failed,
		RegionCacheRetryCount: raw,
	}
}

// TestCacheRetrySweep_RegionOverCap_GiveUpWithWarn: when a region has
// already failed MaxAttempts times, the sweep MUST NOT call
// pusher.Push for it, MUST route it to the giveUp partition
// (RemoveFromCacheFailed, NOT AppendRegionsCacheState with succeeded=),
// and MUST remove the counter entry so a future re-entry starts
// fresh.
func TestCacheRetrySweep_RegionOverCap_GiveUpWithWarn(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{
			rowWithCounts("t_test", "myapp", "d_v1",
				[]string{"fra"},           // one stranded region
				map[string]int{"fra": 10}, // already at the cap (MaxAttempts=10)
			),
		},
	}
	pusher := &mockArtifactCachePusher{}
	var pusherPtr artifactCachePusher = pusher

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{"fra": "http://cache.fra"} },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// giveUp: RemoveFromCacheFailed(["fra"]) — NOT AppendRegionsCacheState.
	if len(repo.removeCalls) != 1 {
		t.Fatalf("removeCalls = %d, want 1 (giveUp routes here, not append)", len(repo.removeCalls))
	}
	if !reflect.DeepEqual(repo.removeCalls[0].Regions, []string{"fra"}) {
		t.Errorf("removeCalls[0].Regions = %v, want [fra]", repo.removeCalls[0].Regions)
	}
	if len(repo.appendCalls) != 0 {
		t.Errorf("appendCalls = %d, want 0 (giveUp must NOT add to regions_cached)", len(repo.appendCalls))
	}
	// Counter entry must be removed as belt-and-suspenders.
	if len(repo.removeCountCalls) != 1 {
		t.Fatalf("removeCountCalls = %d, want 1 (giveUp clears the counter)", len(repo.removeCountCalls))
	}
	if !reflect.DeepEqual(repo.removeCountCalls[0].Regions, []string{"fra"}) {
		t.Errorf("removeCountCalls[0].Regions = %v, want [fra]", repo.removeCountCalls[0].Regions)
	}
	// The cap MUST short-circuit before pusher.Push.
	pusher.mu.Lock()
	defer pusher.mu.Unlock()
	if len(pusher.calls) != 0 {
		t.Errorf("pusher.calls = %d, want 0 (cap must short-circuit pusher.Push)", len(pusher.calls))
	}
}

// TestCacheRetrySweep_StillFailing_IncrementsCounter: a transient
// pusher error (count below cap) routes to stillFailing and MUST
// call IncrementRegionCacheRetryCount so the next tick sees the
// bumped count.
func TestCacheRetrySweep_StillFailing_IncrementsCounter(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{
			rowWithCounts("t_test", "myapp", "d_v1",
				[]string{"fra"},
				map[string]int{"fra": 3}, // below cap (10)
			),
		},
	}
	pusher := &mockArtifactCachePusher{err: errors.New("cache binary 503")}
	var pusherPtr artifactCachePusher = pusher

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{"fra": "http://cache.fra"} },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// stillFailing routes to AppendRegionsCacheState(failed=["fra"], succeeded=nil).
	if len(repo.appendCalls) != 1 {
		t.Fatalf("appendCalls = %d, want 1 (stillFailing → append failed=[fra])", len(repo.appendCalls))
	}
	if !reflect.DeepEqual(repo.appendCalls[0].Failed, []string{"fra"}) ||
		len(repo.appendCalls[0].Succeeded) != 0 {
		t.Errorf("appendCalls[0] = %+v, want Succeeded=nil Failed=[fra]", repo.appendCalls[0])
	}
	// Counter is incremented.
	if len(repo.incrementCountCalls) != 1 {
		t.Fatalf("incrementCountCalls = %d, want 1 (stillFailing bumps the counter)", len(repo.incrementCountCalls))
	}
	if !reflect.DeepEqual(repo.incrementCountCalls[0].Regions, []string{"fra"}) {
		t.Errorf("incrementCountCalls[0].Regions = %v, want [fra]", repo.incrementCountCalls[0].Regions)
	}
	if len(repo.removeCountCalls) != 0 {
		t.Errorf("removeCountCalls = %d, want 0 (only giveUp/success/missing remove)", len(repo.removeCountCalls))
	}
	// Pusher DID see the call (cap short-circuit only fires at >= MaxAttempts).
	pusher.mu.Lock()
	defer pusher.mu.Unlock()
	if len(pusher.calls) != 1 {
		t.Errorf("pusher.calls = %d, want 1 (below-cap stillFailing must attempt push)", len(pusher.calls))
	}
}

// TestCacheRetrySweep_Success_RemovesCounterEntry: a successful push
// moves the region from regions_cache_failed → regions_cached AND
// MUST remove the counter entry so a future re-failure starts at 1.
func TestCacheRetrySweep_Success_RemovesCounterEntry(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{
			rowWithCounts("t_test", "myapp", "d_v1",
				[]string{"fra"},
				map[string]int{"fra": 3}, // any count below cap
			),
		},
	}
	pusher := &mockArtifactCachePusher{err: nil}
	var pusherPtr artifactCachePusher = pusher

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{"fra": "http://cache.fra"} },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// Success routes to AppendRegionsCacheState(succeeded=["fra"], failed=nil).
	if len(repo.appendCalls) != 1 {
		t.Fatalf("appendCalls = %d, want 1 (success → append succeeded=[fra])", len(repo.appendCalls))
	}
	if !reflect.DeepEqual(repo.appendCalls[0].Succeeded, []string{"fra"}) ||
		len(repo.appendCalls[0].Failed) != 0 {
		t.Errorf("appendCalls[0] = %+v, want Succeeded=[fra] Failed=nil", repo.appendCalls[0])
	}
	// Counter entry is REMOVED (success resets, not persists).
	if len(repo.removeCountCalls) != 1 {
		t.Fatalf("removeCountCalls = %d, want 1 (success removes counter entry)", len(repo.removeCountCalls))
	}
	if !reflect.DeepEqual(repo.removeCountCalls[0].Regions, []string{"fra"}) {
		t.Errorf("removeCountCalls[0].Regions = %v, want [fra]", repo.removeCountCalls[0].Regions)
	}
	if len(repo.incrementCountCalls) != 0 {
		t.Errorf("incrementCountCalls = %d, want 0 (success does NOT increment)", len(repo.incrementCountCalls))
	}
	pusher.mu.Lock()
	defer pusher.mu.Unlock()
	if len(pusher.calls) != 1 || pusher.calls[0].CacheURL != "http://cache.fra" {
		t.Errorf("pusher.calls = %v, want exactly one call to http://cache.fra", pusher.calls)
	}
}

// TestCacheRetrySweep_ConfigMissing_RemovesCounterEntry: a region
// that's listed in regions_cache_failed but absent from
// regionCaches (config gap) MUST be dropped via RemoveFromCacheFailed
// AND its counter entry MUST be removed — a future re-entry would
// re-add it at 1.
func TestCacheRetrySweep_ConfigMissing_RemovesCounterEntry(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{
			rowWithCounts("t_test", "myapp", "d_v1",
				[]string{"fra"},          // stranded region
				map[string]int{"fra": 7}, // counter populated (the row pre-existed)
			),
		},
	}
	pusher := &mockArtifactCachePusher{}
	var pusherPtr artifactCachePusher = pusher

	// regionCaches DOES NOT contain "fra" — config gap, swept as missing.
	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{} },
		func() int { return 10 },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// configMissing → RemoveFromCacheFailed(["fra"]); no append, no increment.
	if len(repo.removeCalls) != 1 {
		t.Fatalf("removeCalls = %d, want 1 (configMissing → remove)", len(repo.removeCalls))
	}
	if !reflect.DeepEqual(repo.removeCalls[0].Regions, []string{"fra"}) {
		t.Errorf("removeCalls[0].Regions = %v, want [fra]", repo.removeCalls[0].Regions)
	}
	if len(repo.appendCalls) != 0 {
		t.Errorf("appendCalls = %d, want 0 (configMissing does NOT promote to cached)", len(repo.appendCalls))
	}
	if len(repo.incrementCountCalls) != 0 {
		t.Errorf("incrementCountCalls = %d, want 0 (configMissing does NOT increment)", len(repo.incrementCountCalls))
	}
	// Counter is also wiped.
	if len(repo.removeCountCalls) != 1 {
		t.Fatalf("removeCountCalls = %d, want 1 (configMissing clears counter)", len(repo.removeCountCalls))
	}
	if !reflect.DeepEqual(repo.removeCountCalls[0].Regions, []string{"fra"}) {
		t.Errorf("removeCountCalls[0].Regions = %v, want [fra]", repo.removeCountCalls[0].Regions)
	}
	// Pusher is NEVER called when config is missing — saves an HTTP
	// round trip on a region that wouldn't go anywhere.
	pusher.mu.Lock()
	defer pusher.mu.Unlock()
	if len(pusher.calls) != 0 {
		t.Errorf("pusher.calls = %d, want 0 (configMissing must not push)", len(pusher.calls))
	}
}

// TestCacheRetrySweep_MaxAttemptsZero_DisablesCap: the escape hatch.
// MaxAttempts<=0 MUST disable the cap unconditionally — every region
// is routed through success/stillFailing/configMissing, never giveUp,
// regardless of its counter value. This matches the operator intent
// of "I want retries forever."
func TestCacheRetrySweep_MaxAttemptsZero_DisablesCap(t *testing.T) {
	repo := &mockCacheRetryRepo{
		listResult: []repository.CacheFailedRow{
			// count=999 would normally route to giveUp; with cap=0 it must
			// attempt the push instead.
			rowWithCounts("t_test", "myapp", "d_v1",
				[]string{"fra"},
				map[string]int{"fra": 999},
			),
		},
	}
	pusher := &mockArtifactCachePusher{err: errors.New("transient cache 503")}
	var pusherPtr artifactCachePusher = pusher

	svc := NewCacheRetrySweepService(
		repo,
		func() artifactCachePusher { return pusherPtr },
		func() map[string]string { return map[string]string{"fra": "http://cache.fra"} },
		func() int { return 0 }, // cap disabled
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 10*time.Second)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	repo.mu.Lock()
	defer repo.mu.Unlock()
	// With cap disabled, the region is stillFailing (push tried, errored),
	// NOT giveUp.
	if len(repo.removeCalls) != 0 {
		t.Errorf("removeCalls = %d, want 0 (cap disabled — no giveUp routing)", len(repo.removeCalls))
	}
	if len(repo.removeCountCalls) != 0 {
		t.Errorf("removeCountCalls = %d, want 0 (no giveUp branch)", len(repo.removeCountCalls))
	}
	if len(repo.appendCalls) != 1 || !reflect.DeepEqual(repo.appendCalls[0].Failed, []string{"fra"}) {
		t.Errorf("appendCalls = %v, want one call with Failed=[fra]", repo.appendCalls)
	}
	// Counter is bumped once (stillFailing path), not zeroed.
	if len(repo.incrementCountCalls) != 1 {
		t.Errorf("incrementCountCalls = %d, want 1 (stillFailing path increments)", len(repo.incrementCountCalls))
	}
	// The pusher DID see a call (cap didn't fire).
	pusher.mu.Lock()
	defer pusher.mu.Unlock()
	if len(pusher.calls) != 1 {
		t.Errorf("pusher.calls = %d, want 1 (cap disabled — push attempted)", len(pusher.calls))
	}
}
