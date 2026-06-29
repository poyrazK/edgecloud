package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
)

// fakeTenantRepo, fakeActiveRepo, fakeDeploymentRepo, fakeAppEnvRepo,
// fakeQuotaRepo are the in-memory test doubles for the narrow
// interfaces ReconcileService depends on. Each is hand-rolled (no
// sqlmock) because the reconcile loop is mostly fan-out logic — the
// repo behavior we want to exercise is "what does this query return",
// not "did the SQL parse correctly".
//
// getByIDFunc lets individual tests inject a custom return (e.g.
// `(nil, errors.New("db boom"))`) without mutating the static
// `tenants` slice. Defaults to "look up by ID in the slice, return
// (nil, nil) if missing" — matching the (nil, nil) contract of the
// real TenantRepository.GetByID.
type fakeTenantRepo struct {
	tenants      []domain.Tenant
	getByIDFunc  func(ctx context.Context, id string) (*domain.Tenant, error)
	getByIDCalls []string // every ID looked up, in order — useful for asserting "was GetByID called?"
}

func (f *fakeTenantRepo) List(_ context.Context) ([]domain.Tenant, error) {
	return f.tenants, nil
}

func (f *fakeTenantRepo) GetByID(ctx context.Context, id string) (*domain.Tenant, error) {
	f.getByIDCalls = append(f.getByIDCalls, id)
	if f.getByIDFunc != nil {
		return f.getByIDFunc(ctx, id)
	}
	for i := range f.tenants {
		if f.tenants[i].ID == id {
			return &f.tenants[i], nil
		}
	}
	return nil, nil
}

type fakeActiveRepo struct {
	byTenant map[string][]domain.ActiveDeployment
}

func (f *fakeActiveRepo) ListByTenant(_ context.Context, tenantID string) ([]domain.ActiveDeployment, error) {
	return f.byTenant[tenantID], nil
}

type fakeDeploymentRepo struct {
	byID map[string]*domain.Deployment
}

func (f *fakeDeploymentRepo) GetByID(_ context.Context, id string) (*domain.Deployment, error) {
	return f.byID[id], nil
}

type fakeAppEnvRepo struct {
	byApp map[string][]domain.AppEnv
}

func (f *fakeAppEnvRepo) List(_ context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
	return f.byApp[tenantID+"/"+appName], nil
}

type fakeQuotaRepo struct {
	byTenant map[string]*domain.Quota
}

func (f *fakeQuotaRepo) GetByTenantID(_ context.Context, tenantID string) (*domain.Quota, error) {
	q, ok := f.byTenant[tenantID]
	if !ok {
		return nil, nil
	}
	return q, nil
}

// capturingPublisher is a no-op NATS publisher that records every
// PublishFullSync call. We don't need to capture PublishTaskUpdate /
// PublishHeartbeat because ReconcileService never calls them.
type capturingPublisher struct {
	mu    sync.Mutex
	calls []capturedPublish
}

type capturedPublish struct {
	region string
	msg    *nats.TaskMessage
}

func (p *capturingPublisher) PublishFullSync(region string, msg *nats.TaskMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, capturedPublish{region: region, msg: msg})
	return nil
}

func (p *capturingPublisher) PublishTaskUpdate(string, *nats.TaskMessage) error     { return nil }
func (p *capturingPublisher) PublishHeartbeat(string, *nats.HeartbeatMessage) error { return nil }
func (p *capturingPublisher) EnsureStream(nats.StreamConfig) error                  { return nil }

func (p *capturingPublisher) callsByRegion() map[string]*nats.TaskMessage {
	out := map[string]*nats.TaskMessage{}
	for _, c := range p.calls {
		out[c.region] = c.msg
	}
	return out
}

// reconcileSvcForTest wires a ReconcileService against the fakes
// with default sane values; individual tests override fields.
func reconcileSvcForTest(t *testing.T, tenants []domain.Tenant, active map[string][]domain.ActiveDeployment, deps map[string]*domain.Deployment, envs map[string][]domain.AppEnv, quotas map[string]*domain.Quota, pub nats.Publisher) *ReconcileService {
	t.Helper()
	if pub == nil {
		pub = &capturingPublisher{}
	}
	return NewReconcileService(
		&fakeTenantRepo{tenants: tenants},
		&fakeActiveRepo{byTenant: active},
		&fakeDeploymentRepo{byID: deps},
		&fakeAppEnvRepo{byApp: envs},
		&fakeQuotaRepo{byTenant: quotas},
		pub,
		"global",
	)
}

// --- RunOnce ----------------------------------------------------------

func TestRunOnce_NoTenants_PublishesNothing(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t, nil, nil, nil, nil, nil, pub)

	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := len(pub.calls); got != 0 {
		t.Errorf("calls=%d, want 0 (no tenants)", got)
	}
}

func TestRunOnce_TenantWithNoActiveDeployments_PublishesNothing(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a", AllowlistedDestinations: nil}},
		map[string][]domain.ActiveDeployment{"t_a": {}}, // empty list
		nil, nil, nil, pub)

	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := len(pub.calls); got != 0 {
		t.Errorf("calls=%d, want 0 (empty active list)", got)
	}
}

func TestRunOnce_OneTenantOneAppTwoRegions_GroupsByRegion(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a", AllowlistedDestinations: []string{"api.stripe.com"}}},
		map[string][]domain.ActiveDeployment{
			"t_a": {{TenantID: "t_a", AppName: "myapp", DeploymentID: "d_1"}},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"us-east", "eu-west"}},
		},
		map[string][]domain.AppEnv{
			"t_a/myapp": {{TenantID: "t_a", AppName: "myapp", EnvKey: "K", EnvValue: "v"}},
		},
		nil, pub)

	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	byRegion := pub.callsByRegion()
	if got := len(byRegion); got != 2 {
		t.Fatalf("regions published=%d, want 2 (us-east + eu-west)", got)
	}
	for region, msg := range byRegion {
		if msg.Type != "full_sync" {
			t.Errorf("region=%s type=%q, want full_sync", region, msg.Type)
		}
		if msg.TenantID != "t_a" {
			t.Errorf("region=%s tenant=%q, want t_a", region, msg.TenantID)
		}
		if len(msg.Apps) != 1 {
			t.Errorf("region=%s apps=%d, want 1", region, len(msg.Apps))
		}
		cfg := msg.Apps["myapp"]
		if cfg.DeploymentID != "d_1" || cfg.DeploymentHash != "h1" {
			t.Errorf("region=%s cfg=%+v, want deployment_id=d_1 hash=h1", region, cfg)
		}
		if cfg.Env["K"] != "v" {
			t.Errorf("region=%s env[K]=%q, want v", region, cfg.Env["K"])
		}
		if len(cfg.Allowlist) != 1 || cfg.Allowlist[0] != "api.stripe.com" {
			t.Errorf("region=%s allowlist=%v, want [api.stripe.com]", region, cfg.Allowlist)
		}
	}
}

func TestRunOnce_MultipleAppsSameRegion_GroupedInOneMessage(t *testing.T) {
	// Two active apps on the same deployment with the same region —
	// the reconcile path must emit ONE message per region, not one per app.
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{
			"t_a": {
				{TenantID: "t_a", AppName: "app1", DeploymentID: "d_1"},
				{TenantID: "t_a", AppName: "app2", DeploymentID: "d_2"},
			},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"global"}},
			"d_2": {ID: "d_2", Hash: "h2", Regions: []string{"global"}},
		},
		nil, nil, pub)

	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := len(pub.calls); got != 1 {
		t.Fatalf("calls=%d, want 1 (one message covering both apps)", got)
	}
	msg := pub.calls[0].msg
	if len(msg.Apps) != 2 {
		t.Errorf("apps=%d, want 2", len(msg.Apps))
	}
}

func TestRunOnce_EmptyDeploymentRegions_FallsBackToDefaultRegion(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{
			"t_a": {{TenantID: "t_a", AppName: "legacy", DeploymentID: "d_1"}},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: nil}, // pre-migration-008 shape
		},
		nil, nil, pub)

	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	byRegion := pub.callsByRegion()
	if _, ok := byRegion["global"]; !ok {
		t.Errorf("calls=%v, want one publish to default region 'global'", byRegion)
	}
}

func TestRunOnce_MissingDeployment_SkipsAppWithoutFailingSweep(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{
			"t_a": {
				{TenantID: "t_a", AppName: "missing", DeploymentID: "d_gone"},
				{TenantID: "t_a", AppName: "present", DeploymentID: "d_1"},
			},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"global"}},
			// d_gone intentionally absent — simulates a deleted deployment row.
		},
		nil, nil, pub)

	if err := svc.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := len(pub.calls); got != 1 {
		t.Fatalf("calls=%d, want 1", got)
	}
	msg := pub.calls[0].msg
	if _, ok := msg.Apps["missing"]; ok {
		t.Error("missing app should have been skipped, but is in published payload")
	}
	if _, ok := msg.Apps["present"]; !ok {
		t.Error("present app missing from published payload")
	}
}

// --- RequestSync ------------------------------------------------------

func TestRequestSync_FiltersToRegion(t *testing.T) {
	// The on-register path passes a non-empty region. The reconcile
	// service must publish only to that region, not to every region
	// the tenant's deployments target.
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{
			"t_a": {{TenantID: "t_a", AppName: "myapp", DeploymentID: "d_1"}},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"us-east", "eu-west"}},
		},
		nil, nil, pub)

	svc.RequestSync(context.Background(), "t_a", "us-east")

	byRegion := pub.callsByRegion()
	if got := len(byRegion); got != 1 {
		t.Fatalf("regions=%d, want 1 (us-east only)", got)
	}
	if _, ok := byRegion["us-east"]; !ok {
		t.Errorf("regions=%v, want us-east", byRegion)
	}
}

func TestRequestSync_NoMatchingRegion_PublishesNothing(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{
			"t_a": {{TenantID: "t_a", AppName: "myapp", DeploymentID: "d_1"}},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"us-east"}},
		},
		nil, nil, pub)

	// Region the deployment doesn't target — should be a no-op, not an error.
	svc.RequestSync(context.Background(), "t_a", "ap-south")

	if got := len(pub.calls); got != 0 {
		t.Errorf("calls=%d, want 0 (no matching region)", got)
	}
}

// TestRequestSync_TenantNotFound_NoPublish exercises the
// tenant-not-found branch added in the GetByID refactor (review of
// PR #166, finding #2). The previous implementation relied on the
// broken predicate `len(tenant) == 0` (where `tenant` was the whole
// List result, not a single row) — so this case silently fell
// through with allowlist=nil, which would strip egress rules for a
// tenant whose row was missing for any reason. Now: GetByID returns
// (nil, nil), RequestSync logs and returns, publisher is never
// called.
func TestRequestSync_TenantNotFound_NoPublish(t *testing.T) {
	pub := &capturingPublisher{}
	// Even though the tenants slice contains t_a, we inject a
	// getByIDFunc that returns (nil, nil) — simulating a tenant row
	// the DB can't find for this ID (deleted between Register and
	// the periodic sweep, or stale workerID).
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_other"}}, // t_a is NOT in the slice
		nil, nil, nil, nil, pub)
	svc.tenantRepo.(*fakeTenantRepo).getByIDFunc = func(_ context.Context, _ string) (*domain.Tenant, error) {
		return nil, nil
	}

	svc.RequestSync(context.Background(), "t_a", "us-east")

	if got := len(pub.calls); got != 0 {
		t.Errorf("calls=%d, want 0 (tenant not found must not publish)", got)
	}
	// Verify GetByID was actually consulted (not just bypassed by a
	// short-circuit).
	if got := len(svc.tenantRepo.(*fakeTenantRepo).getByIDCalls); got != 1 {
		t.Errorf("GetByID calls=%d, want 1", got)
	}
}

// TestRequestSync_TenantRepoError_LogsAndReturns covers the
// "GetByID returned an error" branch separately from the
// "GetByID returned (nil, nil)" branch above. Both must fail closed
// (no publish); the log line is the only signal an operator gets.
func TestRequestSync_TenantRepoError_NoPublish(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t, nil, nil, nil, nil, nil, pub)
	svc.tenantRepo.(*fakeTenantRepo).getByIDFunc = func(_ context.Context, _ string) (*domain.Tenant, error) {
		return nil, errors.New("connection reset")
	}

	svc.RequestSync(context.Background(), "t_a", "us-east")

	if got := len(pub.calls); got != 0 {
		t.Errorf("calls=%d, want 0 (repo error must not publish)", got)
	}
}

// --- BuildFullSync ----------------------------------------------------

func TestBuildFullSync_ReturnsSameShapeAsPublish(t *testing.T) {
	// The HTTP fallback endpoint (commit 4) calls BuildFullSync and
	// returns the result as JSON. The published TaskMessage and the
	// returned map must carry the same AppConfig values for a given
	// tenant/region — otherwise a worker pulling /sync sees a different
	// state than one receiving via NATS, and the "differential reset"
	// problem issue #53 was raised about gets worse, not better.
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a", AllowlistedDestinations: []string{"api.example.com"}}},
		map[string][]domain.ActiveDeployment{
			"t_a": {{TenantID: "t_a", AppName: "myapp", DeploymentID: "d_1"}},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"us-east"}},
		},
		map[string][]domain.AppEnv{
			"t_a/myapp": {{TenantID: "t_a", AppName: "myapp", EnvKey: "K", EnvValue: "v"}},
		},
		nil, pub)

	apps, err := svc.BuildFullSync(context.Background(), "t_a", "us-east")
	if err != nil {
		t.Fatalf("BuildFullSync: %v", err)
	}
	cfg, ok := apps["myapp"]
	if !ok {
		t.Fatalf("apps=%v, want myapp", apps)
	}
	if cfg.DeploymentID != "d_1" || cfg.DeploymentHash != "h1" {
		t.Errorf("cfg=%+v, want deployment_id=d_1 hash=h1", cfg)
	}
	if cfg.Env["K"] != "v" {
		t.Errorf("env[K]=%q, want v", cfg.Env["K"])
	}
	if len(cfg.Allowlist) != 1 || cfg.Allowlist[0] != "api.example.com" {
		t.Errorf("allowlist=%v, want [api.example.com]", cfg.Allowlist)
	}
	if cfg.MaxMemoryMB != 256 {
		t.Errorf("maxMemoryMB=%d, want 256 (default)", cfg.MaxMemoryMB)
	}
}

func TestBuildFullSync_HonorsQuotaMaxMemory(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{
			"t_a": {{TenantID: "t_a", AppName: "myapp", DeploymentID: "d_1"}},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"global"}},
		},
		nil,
		map[string]*domain.Quota{"t_a": {TenantID: "t_a", MaxMemoryMB: 1024}},
		pub)

	apps, err := svc.BuildFullSync(context.Background(), "t_a", "global")
	if err != nil {
		t.Fatalf("BuildFullSync: %v", err)
	}
	if got := apps["myapp"].MaxMemoryMB; got != 1024 {
		t.Errorf("maxMemoryMB=%d, want 1024 (from quota)", got)
	}
}

func TestBuildFullSync_NoActiveDeployments_ReturnsEmptyMap(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{"t_a": {}}, // none
		nil, nil, nil, pub)

	apps, err := svc.BuildFullSync(context.Background(), "t_a", "global")
	if err != nil {
		t.Fatalf("BuildFullSync: %v", err)
	}
	if len(apps) != 0 {
		t.Errorf("apps=%v, want empty", apps)
	}
}

// TestBuildFullSync_TenantNotFound_ReturnsError exercises the
// tenant-not-found branch added in the GetByID refactor (review of
// PR #166, finding #2). The previous implementation silently
// proceeded with allowlist=nil when the tenant row was missing —
// which would have stripped egress rules for an inconsistent
// (worker registered, tenant deleted) state. Now: GetByID returns
// (nil, nil), BuildFullSync returns ErrTenantNotFound so the HTTP
// handler can map it to a logged error instead of a stripped
// payload. We assert errors.Is (not ==) because the service wraps
// no context, but a future revision might.
func TestBuildFullSync_TenantNotFound_ReturnsError(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_other"}}, // t_a deliberately absent
		nil, nil, nil, nil, pub)
	svc.tenantRepo.(*fakeTenantRepo).getByIDFunc = func(_ context.Context, _ string) (*domain.Tenant, error) {
		return nil, nil
	}

	apps, err := svc.BuildFullSync(context.Background(), "t_a", "global")
	if !errors.Is(err, ErrTenantNotFound) {
		t.Errorf("err=%v, want ErrTenantNotFound", err)
	}
	if apps != nil {
		t.Errorf("apps=%v, want nil on tenant-not-found", apps)
	}
	if got := len(pub.calls); got != 0 {
		t.Errorf("calls=%d, want 0 (BuildFullSync doesn't publish)", got)
	}
}

// TestBuildFullSync_TenantRepoError_Propagates ensures DB errors
// from GetByID propagate cleanly to the caller (the HTTP handler
// already maps them to 500 via httperror.InternalErrorCtx).
func TestBuildFullSync_TenantRepoError_Propagates(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t, nil, nil, nil, nil, nil, pub)
	want := errors.New("tenant table unreachable")
	svc.tenantRepo.(*fakeTenantRepo).getByIDFunc = func(_ context.Context, _ string) (*domain.Tenant, error) {
		return nil, want
	}

	apps, err := svc.BuildFullSync(context.Background(), "t_a", "global")
	if !errors.Is(err, want) {
		t.Errorf("err=%v, want %v", err, want)
	}
	if apps != nil {
		t.Errorf("apps=%v, want nil on repo error", apps)
	}
}

// --- Run loop ---------------------------------------------------------

func TestRun_InvalidInterval_RefusesToStart(t *testing.T) {
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{
			"t_a": {{TenantID: "t_a", AppName: "x", DeploymentID: "d_1"}},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"global"}},
		},
		nil, nil, pub)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 0 interval must NOT busy-loop, must NOT publish.
	svc.Run(ctx, 0)
	if got := len(pub.calls); got != 0 {
		t.Errorf("calls=%d, want 0 (invalid interval should refuse)", got)
	}
}

func TestRun_FiresImmediatelyThenRespectsCancellation(t *testing.T) {
	// First sweep runs synchronously inside Run; second sweep would
	// fire after `interval`. We cancel during the gap to verify the
	// loop respects ctx (no leaked goroutine, no second publish).
	pub := &capturingPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{
			"t_a": {{TenantID: "t_a", AppName: "x", DeploymentID: "d_1"}},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"global"}},
		},
		nil, nil, pub)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		svc.Run(ctx, 1*time.Hour) // long enough that no second tick fires during the test
		close(done)
	}()

	// Wait for the immediate-first-sweep to publish.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(pub.calls) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(pub.calls); got != 1 {
		t.Fatalf("after immediate sweep: calls=%d, want 1", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after ctx cancel")
	}
}

// Sanity check: errors from the publisher must not panic the loop.
// We don't exercise this on Run (the goroutine makes timing
// brittle); we do it on RequestSync, which is the synchronous on-demand
// path that operators might hit during incident response.
type errPublisher struct {
	capturingPublisher
}

func (p *errPublisher) PublishFullSync(string, *nats.TaskMessage) error {
	return errors.New("simulated NATS outage")
}

func TestRequestSync_PublisherError_DoesNotPanic(t *testing.T) {
	pub := &errPublisher{}
	svc := reconcileSvcForTest(t,
		[]domain.Tenant{{ID: "t_a"}},
		map[string][]domain.ActiveDeployment{
			"t_a": {{TenantID: "t_a", AppName: "x", DeploymentID: "d_1"}},
		},
		map[string]*domain.Deployment{
			"d_1": {ID: "d_1", Hash: "h1", Regions: []string{"global"}},
		},
		nil, nil, pub)

	// Must not panic, must not propagate the error.
	svc.RequestSync(context.Background(), "t_a", "global")
}
