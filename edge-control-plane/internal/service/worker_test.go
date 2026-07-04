package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/nats-io/nats.go"
)

// mockWorkerRepo implements workerRepoInterface for testing.
type mockWorkerRepo struct {
	upsertFunc               func(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error)
	countByTenantFunc        func(ctx context.Context, tenantID string) (int, error)
	deleteFunc               func(ctx context.Context, id string) error
	listByTenantFunc         func(ctx context.Context, tenantID string) ([]domain.Worker, error)
	updateLastSeenFunc       func(ctx context.Context, id string) error
	updateAddrFunc           func(ctx context.Context, id, addr string) error
	upsertStatusFunc         func(ctx context.Context, ws *domain.WorkerStatus) error
	listRunningAppTargetFunc func(ctx context.Context, tenantID, appName string) ([]domain.AppTarget, error)
	getAppStatusFunc         func(ctx context.Context, tenantID, appName string) (*domain.AppWorkerStatus, error)
	getByIDFunc              func(ctx context.Context, id string) (*domain.Worker, error)
}

func (m *mockWorkerRepo) Upsert(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error) {
	return m.upsertFunc(ctx, tenantID, req)
}
func (m *mockWorkerRepo) CountByTenant(ctx context.Context, tenantID string) (int, error) {
	return m.countByTenantFunc(ctx, tenantID)
}
func (m *mockWorkerRepo) Delete(ctx context.Context, id string) error {
	return m.deleteFunc(ctx, id)
}
func (m *mockWorkerRepo) ListByTenant(ctx context.Context, tenantID string) ([]domain.Worker, error) {
	return m.listByTenantFunc(ctx, tenantID)
}
func (m *mockWorkerRepo) UpdateLastSeen(ctx context.Context, id string) error {
	return m.updateLastSeenFunc(ctx, id)
}
func (m *mockWorkerRepo) UpdateAddr(ctx context.Context, id, addr string) error {
	if m.updateAddrFunc == nil {
		return nil
	}
	return m.updateAddrFunc(ctx, id, addr)
}
func (m *mockWorkerRepo) UpsertStatus(ctx context.Context, ws *domain.WorkerStatus) error {
	return m.upsertStatusFunc(ctx, ws)
}
func (m *mockWorkerRepo) ListRunningAppTarget(ctx context.Context, tenantID, appName string) ([]domain.AppTarget, error) {
	if m.listRunningAppTargetFunc == nil {
		return nil, nil
	}
	return m.listRunningAppTargetFunc(ctx, tenantID, appName)
}
func (m *mockWorkerRepo) GetAppStatus(ctx context.Context, tenantID, appName string) (*domain.AppWorkerStatus, error) {
	if m.getAppStatusFunc == nil {
		return nil, nil
	}
	return m.getAppStatusFunc(ctx, tenantID, appName)
}
func (m *mockWorkerRepo) GetByID(ctx context.Context, id string) (*domain.Worker, error) {
	if m.getByIDFunc == nil {
		return nil, nil
	}
	return m.getByIDFunc(ctx, id)
}

func (m *mockWorkerRepo) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	return 0, nil
}

// mockQuotaRepo implements quotaRepoInterface for testing.
type mockQuotaRepo struct {
	getByTenantIDFunc    func(ctx context.Context, tenantID string) (*domain.Quota, error)
	addOutboundBytesFunc func(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error)
	addRequestCountFunc  func(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error)
}

func (m *mockQuotaRepo) GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error) {
	if m.getByTenantIDFunc != nil {
		return m.getByTenantIDFunc(ctx, tenantID)
	}
	return &domain.Quota{}, nil
}

func (m *mockQuotaRepo) AddOutboundBytes(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
	if m.addOutboundBytesFunc != nil {
		return m.addOutboundBytesFunc(ctx, tenantID, delta)
	}
	return &domain.Quota{}, nil
}

func (m *mockQuotaRepo) AddRequestCount(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
	if m.addRequestCountFunc != nil {
		return m.addRequestCountFunc(ctx, tenantID, delta)
	}
	return &domain.Quota{}, nil
}

// mockActiveRepo implements activeRepoInterface for testing the
// stability-window evaluator. Each method records its args so tests
// can assert the wire shape; default funcs return zero values
// (nil/empty) so tests that only exercise one method don't need to
// stub the others.
type mockTenantRepo struct {
    tenantRepoInterface
}

func (m *mockTenantRepo) SetDisabledAt(_ context.Context, _ string, _ time.Time) error {
    return nil
}

func (m *mockTenantRepo) ClearDisabledAt(_ context.Context, _ string) error {
    return nil
}


type mockActiveRepo struct {
	getFunc               func(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error)
	setStableSinceFunc    func(ctx context.Context, tenantID, appName, deploymentID string, ts time.Time) error
	clearStableSinceFunc  func(ctx context.Context, tenantID, appName string) error
	promoteToLastGoodFunc func(ctx context.Context, tenantID, appName, deploymentID string) error
	listByTenantFunc      func(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error)
}

func (m *mockActiveRepo) Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	if m.getFunc == nil {
		return nil, nil
	}
	return m.getFunc(ctx, tenantID, appName)
}
func (m *mockActiveRepo) SetStableSince(ctx context.Context, tenantID, appName, deploymentID string, ts time.Time) error {
	if m.setStableSinceFunc == nil {
		return nil
	}
	return m.setStableSinceFunc(ctx, tenantID, appName, deploymentID, ts)
}
func (m *mockActiveRepo) ClearStableSince(ctx context.Context, tenantID, appName string) error {
	if m.clearStableSinceFunc == nil {
		return nil
	}
	return m.clearStableSinceFunc(ctx, tenantID, appName)
}
func (m *mockActiveRepo) PromoteToLastGood(ctx context.Context, tenantID, appName, deploymentID string) error {
	if m.promoteToLastGoodFunc == nil {
		return nil
	}
	return m.promoteToLastGoodFunc(ctx, tenantID, appName, deploymentID)
}
func (m *mockActiveRepo) ListByTenant(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error) {
	if m.listByTenantFunc == nil {
		return nil, nil
	}
	return m.listByTenantFunc(ctx, tenantID)
}

// workerSvcForTest builds a WorkerService with mock dependencies.
// activeRepo is the new parameter the stability-window evaluator
// needs; pre-existing tests pass nil and the evaluator is only
// called from handleHeartbeat when a tenant_id is present, which
// the pre-existing tests don't send.
func workerSvcForTest(wr *mockWorkerRepo, qr *mockQuotaRepo) *WorkerService {
	return &WorkerService{
		workerRepo: wr,
		quotaRepo:  qr,
		tenantRepo: &mockTenantRepo{},
		nc:         nil,
		activeRepo: nil,
	}
}

// workerSvcForStabilityTest builds a WorkerService with a 1-second
// stability window and the supplied activeRepo. Used by the four
// TestEvaluateStability_* cases below — a short window keeps the
// math (now.Sub(stable_since)) readable in test code.
func workerSvcForStabilityTest(ar *mockActiveRepo) *WorkerService {
	return &WorkerService{
		workerRepo:   &mockWorkerRepo{},
		quotaRepo:    &mockQuotaRepo{},
		tenantRepo:   &mockTenantRepo{},
		activeRepo:   ar,
		nc:           nil,
		stableWindow: 1 * time.Second,
	}
}

// ---------------------------------------------------------------------------
// Stability-window evaluator
// ---------------------------------------------------------------------------

// appsPayload is a helper to construct the apps JSON shape the
// stability evaluator parses. Status + deployment_id are the only
// fields the evaluator looks at; other fields (port, request_count)
// are passed through verbatim and ignored.
func appsPayload(apps map[string]struct {
	Status       string
	DeploymentID string
}) json.RawMessage {
	m := make(map[string]map[string]interface{}, len(apps))
	for k, v := range apps {
		m[k] = map[string]interface{}{
			"status":        v.Status,
			"deployment_id": v.DeploymentID,
		}
	}
	b, _ := json.Marshal(m)
	return b
}

// TestEvaluateStability_FirstRunningArmsTheClock pins the contract
// that the very first observation of "running" for a deployment
// sets stable_since to NOW. Subsequent observations (covered by
// other tests in this file) do NOT overwrite stable_since.
func TestEvaluateStability_FirstRunningArmsTheClock(t *testing.T) {
	var setStableSinceCalled bool
	var gotDeploymentID string
	ar := &mockActiveRepo{
		getFunc: func(_ context.Context, _, _ string) (*domain.ActiveDeployment, error) {
			return &domain.ActiveDeployment{
				TenantID:            "t_test",
				AppName:             "myapp",
				DeploymentID:        "d_v1",
				AutoRollbackEnabled: true,
				StableSince:         nil, // not yet armed
			}, nil
		},
		setStableSinceFunc: func(_ context.Context, _, _, deploymentID string, _ time.Time) error {
			setStableSinceCalled = true
			gotDeploymentID = deploymentID
			return nil
		},
	}
	svc := workerSvcForStabilityTest(ar)

	svc.evaluateStability(context.Background(), "t_test", appsPayload(map[string]struct {
		Status       string
		DeploymentID string
	}{"myapp": {Status: "running", DeploymentID: "d_v1"}}))

	if !setStableSinceCalled {
		t.Fatal("SetStableSince was not called on first running observation")
	}
	if gotDeploymentID != "d_v1" {
		t.Errorf("SetStableSince deployment = %q, want d_v1", gotDeploymentID)
	}
}

// TestEvaluateStability_RunningForLongEnoughPromotesToLastGood
// pins the contract that a deployment observed running for ≥
// stableWindow AND with auto_rollback_enabled=true gets promoted to
// last_good_deployment_id. Without a successful promote, a future
// crash has nothing to roll back to.
func TestEvaluateStability_RunningForLongEnoughPromotesToLastGood(t *testing.T) {
	armedAt := time.Now().Add(-2 * time.Second) // older than stableWindow
	var promoteCalled bool
	var promoteDeploymentID string
	ar := &mockActiveRepo{
		getFunc: func(_ context.Context, _, _ string) (*domain.ActiveDeployment, error) {
			return &domain.ActiveDeployment{
				TenantID:            "t_test",
				AppName:             "myapp",
				DeploymentID:        "d_v1",
				AutoRollbackEnabled: true,
				StableSince:         &armedAt,
			}, nil
		},
		promoteToLastGoodFunc: func(_ context.Context, _, _, deploymentID string) error {
			promoteCalled = true
			promoteDeploymentID = deploymentID
			return nil
		},
	}
	svc := workerSvcForStabilityTest(ar)

	svc.evaluateStability(context.Background(), "t_test", appsPayload(map[string]struct {
		Status       string
		DeploymentID string
	}{"myapp": {Status: "running", DeploymentID: "d_v1"}}))

	if !promoteCalled {
		t.Fatal("PromoteToLastGood was not called for a long-stable deployment")
	}
	if promoteDeploymentID != "d_v1" {
		t.Errorf("PromoteToLastGood deployment = %q, want d_v1", promoteDeploymentID)
	}
}

// TestEvaluateStability_CrashedResetsTheClock pins the contract
// that a non-running status clears stable_since so the next
// "running" observation has to start the window from scratch.
// Without this, a flapping app could accumulate partial-window
// credit across flaps and never actually be observed stable.
func TestEvaluateStability_CrashedResetsTheClock(t *testing.T) {
	armedAt := time.Now().Add(-2 * time.Second)
	var clearStableSinceCalled bool
	ar := &mockActiveRepo{
		getFunc: func(_ context.Context, _, _ string) (*domain.ActiveDeployment, error) {
			return &domain.ActiveDeployment{
				TenantID:            "t_test",
				AppName:             "myapp",
				DeploymentID:        "d_v1",
				AutoRollbackEnabled: true,
				StableSince:         &armedAt, // armed but app is now crashed
			}, nil
		},
		clearStableSinceFunc: func(_ context.Context, _, _ string) error {
			clearStableSinceCalled = true
			return nil
		},
		promoteToLastGoodFunc: func(_ context.Context, _, _, _ string) error {
			t.Fatal("PromoteToLastGood must NOT be called for a non-running app")
			return nil
		},
	}
	svc := workerSvcForStabilityTest(ar)

	svc.evaluateStability(context.Background(), "t_test", appsPayload(map[string]struct {
		Status       string
		DeploymentID string
	}{"myapp": {Status: "crashed", DeploymentID: "d_v1"}}))

	if !clearStableSinceCalled {
		t.Error("ClearStableSince was not called on non-running observation")
	}
}

// TestEvaluateStability_AutoRollbackDisabledSkipsPromotion pins
// the contract that even a long-stable deployment is NOT promoted
// when the tenant opted out of auto-rollback. The reasoning is in
// the doc on evaluateStability: without an auto-rollback consumer
// of last_good, flipping the pointer could surprise a manual
// rollback.
func TestEvaluateStability_AutoRollbackDisabledSkipsPromotion(t *testing.T) {
	armedAt := time.Now().Add(-2 * time.Second)
	ar := &mockActiveRepo{
		getFunc: func(_ context.Context, _, _ string) (*domain.ActiveDeployment, error) {
			return &domain.ActiveDeployment{
				TenantID:            "t_test",
				AppName:             "myapp",
				DeploymentID:        "d_v1",
				AutoRollbackEnabled: false, // opted out
				StableSince:         &armedAt,
			}, nil
		},
		promoteToLastGoodFunc: func(_ context.Context, _, _, _ string) error {
			t.Fatal("PromoteToLastGood must NOT be called when auto_rollback_enabled=false")
			return nil
		},
	}
	svc := workerSvcForStabilityTest(ar)

	svc.evaluateStability(context.Background(), "t_test", appsPayload(map[string]struct {
		Status       string
		DeploymentID string
	}{"myapp": {Status: "running", DeploymentID: "d_v1"}}))

	// Nothing to assert beyond the absence of the Fatal call above;
	// the test passing IS the assertion. (If we add an event channel
	// to the mock later, this test would assert on it.)
}

func TestWorkerService_Register_InvalidWorkerID(t *testing.T) {
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{})
	tests := []struct {
		name string
		req  domain.RegisterWorkerRequest
	}{
		{"empty worker_id", domain.RegisterWorkerRequest{WorkerID: "", Region: "fra"}},
		{"invalid format", domain.RegisterWorkerRequest{WorkerID: "invalid", Region: "fra"}},
		{"w_ only", domain.RegisterWorkerRequest{WorkerID: "w_", Region: "fra"}},
		{"w_fra missing uuid", domain.RegisterWorkerRequest{WorkerID: "w_fra", Region: "fra"}},
		{"w__uuid empty region", domain.RegisterWorkerRequest{WorkerID: "w__uuid", Region: ""}},
		{"w_fra_ empty uuid", domain.RegisterWorkerRequest{WorkerID: "w_fra_", Region: "fra"}},
		{"wrong prefix", domain.RegisterWorkerRequest{WorkerID: "x_fra_abc", Region: "fra"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := svc.Register(context.Background(), "t_tenant1", &tt.req)
			if !errors.Is(err, ErrInvalidWorkerID) {
				t.Errorf("Register() error = %v, want ErrInvalidWorkerID", err)
			}
		})
	}
}

func TestWorkerService_Register_RegionMismatch(t *testing.T) {
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{})
	req := &domain.RegisterWorkerRequest{WorkerID: "w_fra_abc123", Region: "ams"}
	err := svc.Register(context.Background(), "t_tenant1", req)
	if !errors.Is(err, ErrRegionMismatch) {
		t.Errorf("Register() error = %v, want ErrRegionMismatch", err)
	}
}

func TestWorkerService_Register_HappyPath(t *testing.T) {
	deleteCalled := false
	wr := &mockWorkerRepo{
		upsertFunc: func(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error) {
			return true, nil // new worker inserted
		},
		countByTenantFunc: func(ctx context.Context, tenantID string) (int, error) {
			return 1, nil // one worker (this one) after insert
		},
		deleteFunc: func(ctx context.Context, id string) error {
			deleteCalled = true
			return nil
		},
	}
	qr := &mockQuotaRepo{
		getByTenantIDFunc: func(ctx context.Context, tenantID string) (*domain.Quota, error) {
			return &domain.Quota{MaxWorkers: 5}, nil
		},
	}
	svc := workerSvcForTest(wr, qr)

	err := svc.Register(context.Background(), "t_tenant1", &domain.RegisterWorkerRequest{
		WorkerID: "w_fra_abc123", Region: "fra",
	})
	if err != nil {
		t.Errorf("Register() error = %v, want nil", err)
	}
	if deleteCalled {
		t.Error("Delete should not have been called")
	}
}

func TestWorkerService_Register_IdempotentReRegistration(t *testing.T) {
	deleteCalled := false
	countCalled := false
	wr := &mockWorkerRepo{
		upsertFunc: func(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error) {
			return false, nil // worker already existed — re-registration
		},
		countByTenantFunc: func(ctx context.Context, tenantID string) (int, error) {
			countCalled = true
			return 0, nil
		},
		deleteFunc: func(ctx context.Context, id string) error {
			deleteCalled = true
			return nil
		},
	}
	qr := &mockQuotaRepo{
		getByTenantIDFunc: func(ctx context.Context, tenantID string) (*domain.Quota, error) {
			return &domain.Quota{MaxWorkers: 5}, nil
		},
	}
	svc := workerSvcForTest(wr, qr)

	err := svc.Register(context.Background(), "t_tenant1", &domain.RegisterWorkerRequest{
		WorkerID: "w_fra_abc123", Region: "fra",
	})
	if err != nil {
		t.Errorf("Register() error = %v, want nil", err)
	}
	if deleteCalled {
		t.Error("Delete should not have been called for re-registration")
	}
	if countCalled {
		t.Error("CountByTenant should not have been called for re-registration")
	}
}

func TestWorkerService_Register_QuotaExceeded(t *testing.T) {
	deleteCalled := false
	deletedID := ""
	wr := &mockWorkerRepo{
		upsertFunc: func(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error) {
			return true, nil // new worker inserted
		},
		countByTenantFunc: func(ctx context.Context, tenantID string) (int, error) {
			return 3, nil // exceeds MaxWorkers=2
		},
		deleteFunc: func(ctx context.Context, id string) error {
			deleteCalled = true
			deletedID = id
			return nil
		},
	}
	qr := &mockQuotaRepo{
		getByTenantIDFunc: func(ctx context.Context, tenantID string) (*domain.Quota, error) {
			return &domain.Quota{MaxWorkers: 2}, nil
		},
	}
	svc := workerSvcForTest(wr, qr)

	err := svc.Register(context.Background(), "t_tenant1", &domain.RegisterWorkerRequest{
		WorkerID: "w_fra_abc123", Region: "fra",
	})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("Register() error = %v, want ErrQuotaExceeded", err)
	}
	if !deleteCalled {
		t.Error("Delete should have been called to rollback new worker")
	}
	if deletedID != "w_fra_abc123" {
		t.Errorf("Delete called with id = %q, want w_fra_abc123", deletedID)
	}
}

func TestWorkerService_Register_UpsertDBError(t *testing.T) {
	wr := &mockWorkerRepo{
		upsertFunc: func(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error) {
			return false, errors.New("db error")
		},
	}
	qr := &mockQuotaRepo{}
	svc := workerSvcForTest(wr, qr)

	err := svc.Register(context.Background(), "t_tenant1", &domain.RegisterWorkerRequest{
		WorkerID: "w_fra_abc123", Region: "fra",
	})
	if err == nil {
		t.Error("Register() error = nil, want non-nil")
	}
}

func TestWorkerService_Register_QuotaDBError(t *testing.T) {
	deleteCalled := false
	wr := &mockWorkerRepo{
		upsertFunc: func(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error) {
			return true, nil
		},
		countByTenantFunc: func(ctx context.Context, tenantID string) (int, error) {
			return 1, nil
		},
		deleteFunc: func(ctx context.Context, id string) error {
			deleteCalled = true
			return nil
		},
	}
	qr := &mockQuotaRepo{
		getByTenantIDFunc: func(ctx context.Context, tenantID string) (*domain.Quota, error) {
			return nil, errors.New("quota db error")
		},
	}
	svc := workerSvcForTest(wr, qr)

	err := svc.Register(context.Background(), "t_tenant1", &domain.RegisterWorkerRequest{
		WorkerID: "w_fra_abc123", Region: "fra",
	})
	if err == nil {
		t.Error("Register() error = nil, want non-nil")
	}
	if !deleteCalled {
		t.Error("Delete should have been called to rollback")
	}
}

func TestWorkerService_ListByTenant(t *testing.T) {
	workers := []domain.Worker{
		{ID: "w_fra_abc123", TenantID: "t_tenant1", Region: "fra"},
		{ID: "w_ams_def456", TenantID: "t_tenant1", Region: "ams"},
	}
	wr := &mockWorkerRepo{
		listByTenantFunc: func(ctx context.Context, tenantID string) ([]domain.Worker, error) {
			return workers, nil
		},
	}
	qr := &mockQuotaRepo{}
	svc := workerSvcForTest(wr, qr)

	got, err := svc.ListByTenant(context.Background(), "t_tenant1")
	if err != nil {
		t.Fatalf("ListByTenant() error = %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len(got) = %d, want 2", len(got))
	}
	if got[0].ID != "w_fra_abc123" {
		t.Errorf("got[0].ID = %q, want w_fra_abc123", got[0].ID)
	}
}

func TestWorkerService_ListByTenant_Empty(t *testing.T) {
	wr := &mockWorkerRepo{
		listByTenantFunc: func(ctx context.Context, tenantID string) ([]domain.Worker, error) {
			return nil, nil
		},
	}
	qr := &mockQuotaRepo{}
	svc := workerSvcForTest(wr, qr)

	got, err := svc.ListByTenant(context.Background(), "t_tenant1")
	if err != nil {
		t.Fatalf("ListByTenant() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(got) = %d, want 0", len(got))
	}
}

func TestIsValidWorkerID(t *testing.T) {
	tests := []struct {
		id    string
		valid bool
	}{
		{"w_fra_abc123", true},
		{"w_ams_xYz123", true},
		{"w_us_1", true},
		{"w_sgp_a", true},
		{"invalid", false},
		{"w_", false},
		{"w_fra", false},
		{"w__uuid", false}, // empty region
		{"x_fra_abc", false},
		{"", false},
		{"w_fra_", false}, // empty uuid
	}
	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			got := domain.IsValidWorkerID(tt.id)
			if got != tt.valid {
				t.Errorf("IsValidWorkerID(%q) = %v, want %v", tt.id, got, tt.valid)
			}
		})
	}
}

func TestWorkerService_SubscribeHeartbeats_NilNATS(t *testing.T) {
	wr := &mockWorkerRepo{}
	qr := &mockQuotaRepo{}
	svc := workerSvcForTest(wr, qr)

	err := svc.SubscribeHeartbeats(context.Background())
	if err != nil {
		t.Errorf("SubscribeHeartbeats() error = %v, want nil (nil NATS)", err)
	}
}

func TestWorkerService_HandleHeartbeat(t *testing.T) {
	const appsJSON = `{"myapp":{"status":"running","exit_code":null,"deployment_id":"d_1","tenant_id":"t_test","port":8081}}`

	tests := []struct {
		name            string
		payload         string
		wantUpdateAddr  bool
		wantAddr        string
		wantAppsPersist string
	}{
		{
			name: "addr and apps present",
			payload: `{"type":"heartbeat","timestamp":"2026-06-17T12:00:00Z",` +
				`"worker_id":"w_fra_abc","region":"fra","worker_addr":"203.0.113.10",` +
				`"apps":` + appsJSON + `}`,
			wantUpdateAddr:  true,
			wantAddr:        "203.0.113.10",
			wantAppsPersist: appsJSON,
		},
		{
			name: "addr absent (legacy worker) — must not clobber",
			payload: `{"type":"heartbeat","timestamp":"2026-06-17T12:00:00Z",` +
				`"worker_id":"w_fra_abc","region":"fra",` +
				`"apps":` + appsJSON + `}`,
			wantUpdateAddr:  false,
			wantAppsPersist: appsJSON,
		},
		{
			name: "apps empty",
			payload: `{"type":"heartbeat","timestamp":"2026-06-17T12:00:00Z",` +
				`"worker_id":"w_fra_abc","region":"fra","worker_addr":"203.0.113.10",` +
				`"apps":{}}`,
			wantUpdateAddr:  true,
			wantAddr:        "203.0.113.10",
			wantAppsPersist: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var updateAddrCalled bool
			var gotAddr string
			var gotAppsPersist string

			wr := &mockWorkerRepo{
				updateLastSeenFunc: func(ctx context.Context, id string) error { return nil },
				updateAddrFunc: func(ctx context.Context, id, addr string) error {
					updateAddrCalled = true
					gotAddr = addr
					return nil
				},
				upsertStatusFunc: func(ctx context.Context, ws *domain.WorkerStatus) error {
					gotAppsPersist = string(ws.Apps)
					return nil
				},
			}
			qr := &mockQuotaRepo{}
			svc := workerSvcForTest(wr, qr)

			svc.handleHeartbeat(context.Background(),
				&nats.Msg{Subject: "edgecloud.heartbeats.fra", Data: []byte(tt.payload)})

			if updateAddrCalled != tt.wantUpdateAddr {
				t.Errorf("UpdateAddr called = %v, want %v", updateAddrCalled, tt.wantUpdateAddr)
			}
			if tt.wantUpdateAddr && gotAddr != tt.wantAddr {
				t.Errorf("UpdateAddr addr = %q, want %q", gotAddr, tt.wantAddr)
			}
			// Persisted apps blob must include the new port and tenant_id fields
			// so the ingress query (`ListRunningAppTarget`) can extract them.
			if gotAppsPersist != tt.wantAppsPersist {
				t.Errorf("persisted apps = %s, want %s", gotAppsPersist, tt.wantAppsPersist)
			}
		})
	}
}

func TestWorkerService_GetAppTarget(t *testing.T) {
	want := domain.AppTarget{
		AppName: "myapp", TenantID: "t_test", WorkerID: "w_fra_abc", Region: "fra", WorkerAddr: "203.0.113.10", Port: 8081,
	}
	tests := []struct {
		name      string
		repoRows  []domain.AppTarget
		wantNil   bool
		wantFirst *domain.AppTarget
	}{
		{
			name:      "single row found",
			repoRows:  []domain.AppTarget{want},
			wantNil:   false,
			wantFirst: &want,
		},
		{
			name:     "no rows found",
			repoRows: nil,
			wantNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotTenantID, gotAppName string
			wr := &mockWorkerRepo{
				listRunningAppTargetFunc: func(ctx context.Context, tenantID, appName string) ([]domain.AppTarget, error) {
					gotTenantID, gotAppName = tenantID, appName
					return tt.repoRows, nil
				},
			}
			qr := &mockQuotaRepo{}
			svc := workerSvcForTest(wr, qr)

			got, err := svc.GetAppTarget(context.Background(), "t_test", "myapp")
			if err != nil {
				t.Fatalf("GetAppTarget() error = %v", err)
			}
			if gotTenantID != "t_test" || gotAppName != "myapp" {
				t.Errorf("repo called with (%q, %q), want (t_test, myapp)", gotTenantID, gotAppName)
			}
			if tt.wantNil {
				if got != nil {
					t.Errorf("GetAppTarget() = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("GetAppTarget() = nil, want %+v", tt.wantFirst)
			}
			if *got != *tt.wantFirst {
				t.Errorf("GetAppTarget() = %+v, want %+v", *got, *tt.wantFirst)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetAppStatus — the read path behind GET /api/v1/apps/{appName}/status
// ---------------------------------------------------------------------------

// TestWorkerService_GetAppStatus_PassesThroughRow pins the
// happy path: repo returns a row, service echoes it with AppName
// normalized to the input. The CLI's `edge logs` crashed-hint
// logic depends on Status == "crashed" reaching the wire verbatim.
func TestWorkerService_GetAppStatus_PassesThroughRow(t *testing.T) {
	hb := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	repoRow := &domain.AppWorkerStatus{
		// AppName intentionally empty to verify the service
		// normalizes it from the input parameter.
		Status:        "crashed",
		LastHeartbeat: &hb,
		Region:        "us-east-1",
		WorkerID:      "w_us-east-1_h01",
	}
	var gotTenant, gotApp string
	wr := &mockWorkerRepo{
		getAppStatusFunc: func(_ context.Context, tenantID, appName string) (*domain.AppWorkerStatus, error) {
			gotTenant, gotApp = tenantID, appName
			return repoRow, nil
		},
	}
	svc := workerSvcForTest(wr, &mockQuotaRepo{})

	got, err := svc.GetAppStatus(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetAppStatus: %v", err)
	}
	if gotTenant != "t_test" || gotApp != "myapp" {
		t.Errorf("repo called with (%q, %q), want (t_test, myapp)", gotTenant, gotApp)
	}
	if got == nil {
		t.Fatal("got nil, want *AppWorkerStatus")
	}
	if got.AppName != "myapp" {
		t.Errorf("AppName = %q, want myapp (normalized from input)", got.AppName)
	}
	if got.Status != "crashed" {
		t.Errorf("Status = %q, want crashed", got.Status)
	}
	if got.LastHeartbeat == nil || !got.LastHeartbeat.Equal(hb) {
		t.Errorf("LastHeartbeat = %v, want %v", got.LastHeartbeat, hb)
	}
}

// TestWorkerService_GetAppStatus_NilRowYieldsUnknown pins the
// no-data path: when the repo returns nil (no worker has reported
// on this app, OR a cross-tenant request), the service returns
// `{AppName: <input>, Status: "unknown"}` with everything else
// zero. The handler encodes this as 200 — a probing tenant cannot
// distinguish "no such app" from "not yours" because both paths
// produce the same envelope.
func TestWorkerService_GetAppStatus_NilRowYieldsUnknown(t *testing.T) {
	wr := &mockWorkerRepo{
		getAppStatusFunc: func(_ context.Context, _, _ string) (*domain.AppWorkerStatus, error) {
			return nil, nil
		},
	}
	svc := workerSvcForTest(wr, &mockQuotaRepo{})

	got, err := svc.GetAppStatus(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetAppStatus: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want *AppWorkerStatus{Status: unknown}")
	}
	if got.Status != "unknown" {
		t.Errorf("Status = %q, want unknown", got.Status)
	}
	if got.AppName != "myapp" {
		t.Errorf("AppName = %q, want myapp", got.AppName)
	}
	if got.Region != "" {
		t.Errorf("Region = %q, want empty (zero-value on unknown)", got.Region)
	}
	if got.LastHeartbeat != nil {
		t.Errorf("LastHeartbeat = %v, want nil on unknown", got.LastHeartbeat)
	}
}

// TestWorkerService_GetAppStatus_PropagatesRepoError pins the
// pass-through: any non-nil error from the repo (DB outage, etc.)
// reaches the handler unchanged so the handler can map to 500.
func TestWorkerService_GetAppStatus_PropagatesRepoError(t *testing.T) {
	wantErr := errors.New("db unreachable")
	wr := &mockWorkerRepo{
		getAppStatusFunc: func(_ context.Context, _, _ string) (*domain.AppWorkerStatus, error) {
			return nil, wantErr
		},
	}
	svc := workerSvcForTest(wr, &mockQuotaRepo{})

	_, err := svc.GetAppStatus(context.Background(), "t_test", "myapp")
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
}

// captureLogger redirects log output to a buffer for the duration of the
// returned restore function. Used to assert log lines from applyTenantDelta
// without leaking the global logger.
func captureLogger(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(buf)
	log.SetFlags(0)
	return buf, func() {
		log.SetOutput(prev)
		log.SetFlags(prevFlags)
	}
}

func TestApplyTenantDelta_Requests_ExceedsCap_Logs(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, delta uint64) (*domain.Quota, error) {
			return &domain.Quota{
				MaxRequestsPerMonth: 100,
				UsedRequestCount:    101, // breach
			}, nil
		},
	})
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_a", RequestCount: 1, OutboundBytes: 0},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		svc.quotaRepo.AddRequestCount,
	)

	out := buf.String()
	if !strings.Contains(out, "used 101 requests") {
		t.Errorf("log output missing breach line; got %q", out)
	}
	if !strings.Contains(out, "exceeds monthly limit 100") {
		t.Errorf("log output missing limit; got %q", out)
	}
}

func TestApplyTenantDelta_OutboundBytes_Unlimited_NoLog(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addOutboundBytesFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			// MaxOutboundMB = -1 (unlimited sentinel)
			return &domain.Quota{MaxOutboundMB: -1, UsedOutboundBytes: 9999}, nil
		},
	})
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_a", OutboundBytes: 1, RequestCount: 0},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.OutboundBytes },
		func(q *domain.Quota) int64 { return int64(q.MaxOutboundMB) * 1024 * 1024 },
		func(q *domain.Quota) int64 { return q.UsedOutboundBytes },
		"outbound bytes",
		svc.quotaRepo.AddOutboundBytes,
	)

	if buf.Len() != 0 {
		t.Errorf("unlimited tenant produced log output: %q", buf.String())
	}
}

func TestApplyTenantDelta_SkipsZeroDelta(t *testing.T) {
	called := false
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			called = true
			return &domain.Quota{}, nil
		},
	})
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_a", RequestCount: 0, OutboundBytes: 0},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		svc.quotaRepo.AddRequestCount,
	)

	if called {
		t.Errorf("AddRequestCount called despite zero delta (should be skipped)")
	}
}

func TestApplyTenantDelta_RepositoryError_LogsAndContinues(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			return nil, errors.New("db down")
		},
	})
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_a", RequestCount: 5, OutboundBytes: 0},
	}
	appsRaw, _ := json.Marshal(apps)

	// Should not panic; should log the error.
	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		svc.quotaRepo.AddRequestCount,
	)

	if !strings.Contains(buf.String(), "failed to record requests for tenant t_a") {
		t.Errorf("expected error log; got %q", buf.String())
	}
}

// Unused import suppression: strconv is used in tests for tenant ID assertions
// but a few of the helper-only tests above don't reference it. Keep the
// import explicit so adding new tests is friction-free.
var _ = strconv.Itoa
