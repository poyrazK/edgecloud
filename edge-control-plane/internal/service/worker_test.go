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
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
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
	setGraceUntilFunc    func(ctx context.Context, tenantID string, until *time.Time) error
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

// SetGraceUntil (issue #420) is the grace clock write called by
// applyTenantDelta on free-tier first-cross. Tests that don't care
// about grace timing can omit setGraceUntilFunc.
func (m *mockQuotaRepo) SetGraceUntil(ctx context.Context, tenantID string, until *time.Time) error {
	if m.setGraceUntilFunc != nil {
		return m.setGraceUntilFunc(ctx, tenantID, until)
	}
	return nil
}

// mockActiveRepo implements activeRepoInterface for testing the
// stability-window evaluator. Each method records its args so tests
// can assert the wire shape; default funcs return zero values
// (nil/empty) so tests that only exercise one method don't need to
// stub the others.
type mockTenantRepo struct {
	tenantRepoInterface
	getByIDFunc func(ctx context.Context, tenantID string) (*domain.Tenant, error)
}

func (m *mockTenantRepo) GetByID(ctx context.Context, tenantID string) (*domain.Tenant, error) {
	if m.getByIDFunc != nil {
		return m.getByIDFunc(ctx, tenantID)
	}
	return &domain.Tenant{ID: tenantID, Plan: "free"}, nil
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

// WithTx returns nil because the existing stability-window tests never
// exercise disableTenantAtomically (which is the only call site that
// uses WithTx). The dedicated disable-path tests live in
// deployment_disable_test.go and use a sqlmock-backed harness instead.
func (m *mockActiveRepo) WithTx(_ *sqlx.Tx) *repository.ActiveDeploymentRepository {
	return nil
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

// TestApplyTenantDelta_Requests_ExceedsCap_PublishesEmpty extends the
// breach test to assert that an empty task_update is published per
// region when the tenant has active deployments (issue #440). Without
// the publish, workers in every region keep running the tenant's apps
// until the 5-minute reconcile cycle — the very regression this test
// pins.
func TestApplyTenantDelta_Requests_ExceedsCap_PublishesEmpty(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	js := &recordingJetStream{}
	regions := []string{"us-east", "eu-west"}
	svc := &WorkerService{
		workerRepo: &mockWorkerRepo{},
		quotaRepo: &mockQuotaRepo{
			addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
				return &domain.Quota{
					MaxRequestsPerMonth: 100,
					UsedRequestCount:    101, // breach
				}, nil
			},
		},
		tenantRepo: &mockTenantRepo{},
		activeRepo: &mockActiveRepo{
			listByTenantFunc: func(_ context.Context, _ string) ([]domain.ActiveDeployment, error) {
				return []domain.ActiveDeployment{
					{TenantID: "t_a", AppName: "myapp", DeploymentID: "d_1", RegionsPublished: regions},
				}, nil
			},
		},
		jsForTest: js,
		nc:        nil,
	}
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

	if len(js.publishes) != len(regions) {
		t.Fatalf("publish count = %d, want %d (one per region)", len(js.publishes), len(regions))
	}
	got := make(map[string][]byte, len(js.publishes))
	for _, p := range js.publishes {
		got[p.subject] = p.data
	}
	for _, r := range regions {
		if _, ok := got["edgecloud.tasks."+r]; !ok {
			t.Errorf("missing publish to edgecloud.tasks.%s; got subjects %v", r, keysOf(got))
		}
	}
	out := buf.String()
	if !strings.Contains(out, "published empty task_update for disabled tenant t_a") {
		t.Errorf("missing published-empty log line; got %q", out)
	}
}

// TestApplyTenantDelta_Requests_ExceedsCap_FreeTier_SetsGraceClock
// (issue #420) verifies the dual-write: on first-cross of a free-tier
// cap, applyTenantDelta must call SetGraceUntil with a future
// timestamp (the grace window) AND SetDisabledAt. The previous test
// only checks the log line; this one locks the wire contract by
// capturing the SetGraceUntil call args via the mock seam.
func TestApplyTenantDelta_Requests_ExceedsCap_FreeTier_SetsGraceClock(t *testing.T) {
	var (
		gotTenantID  string
		gotGraceAt   *time.Time
		gotGraceAtOK bool
	)
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			return &domain.Quota{
				MaxRequestsPerMonth: 100,
				UsedRequestCount:    101, // breach
			}, nil
		},
		setGraceUntilFunc: func(_ context.Context, tenantID string, until *time.Time) error {
			gotTenantID = tenantID
			gotGraceAt = until
			gotGraceAtOK = true
			return nil
		},
	})
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_free", RequestCount: 1, OutboundBytes: 0},
	}
	appsRaw, _ := json.Marshal(apps)

	before := time.Now()
	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		svc.quotaRepo.AddRequestCount,
	)

	if !gotGraceAtOK {
		t.Fatal("SetGraceUntil was not called on free-tier first-cross")
	}
	if gotTenantID != "t_free" {
		t.Errorf("SetGraceUntil tenantID = %q, want %q", gotTenantID, "t_free")
	}
	if gotGraceAt == nil {
		t.Fatal("SetGraceUntil received nil timestamp")
	}
	// Grace clock must be strictly in the future (operator can rely on
	// the deploy-time check returning 402 immediately, while request-time
	// 402 fires only after this clock expires).
	if !gotGraceAt.After(before) {
		t.Errorf("grace clock %v is not after test-start %v", gotGraceAt, before)
	}
	// And the grace window should be in the expected ballpark (default
	// 1h — generous so we only assert the lower bound).
	if gotGraceAt.Sub(before) < 30*time.Minute {
		t.Errorf("grace window %v < 30m (want ≥ 30m, default 1h)", gotGraceAt.Sub(before))
	}
}

// TestApplyTenantDelta_Requests_ExceedsCap_PaidTenant_SkipsGraceClock
// (issue #420) verifies the shortcut: a paid tenant (plan != "free")
// crossing the cap goes straight to SetDisabledAt. The grace clock
// is a free-tier affordance only — paid tenants have the admin
// quota-override endpoint as their recovery path.
func TestApplyTenantDelta_Requests_ExceedsCap_PaidTenant_SkipsGraceClock(t *testing.T) {
	graceCalled := false
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			return &domain.Quota{
				MaxRequestsPerMonth: 100,
				UsedRequestCount:    101, // breach
			}, nil
		},
		setGraceUntilFunc: func(_ context.Context, _ string, _ *time.Time) error {
			graceCalled = true
			return nil
		},
	})
	// Override the default free-tier tenant with a paid plan. The
	// free-tier default exists so other tests don't have to stub it;
	// here we explicitly want the paid-tenant shortcut.
	svc.tenantRepo = &mockTenantRepo{
		getByIDFunc: func(_ context.Context, id string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: id, Plan: "pro"}, nil
		},
	}
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_pro", RequestCount: 1, OutboundBytes: 0},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		svc.quotaRepo.AddRequestCount,
	)

	if graceCalled {
		t.Error("SetGraceUntil must NOT be called for paid tenants — paid tenants have the admin override as their recovery path")
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

// ── Dedupe-skip tests (issue #418) ────────────────────────────────────

// TestApplyTenantDelta_SkipsDuplicateDelivery verifies that a heartbeat
// carrying a DedupeID the CP has already seen within the cache TTL does
// NOT trigger another `add` call. This is the JetStream redelivery path:
// the same logical heartbeat arrives twice and we must not double-count.
func TestApplyTenantDelta_SkipsDuplicateDelivery(t *testing.T) {
	calls := 0
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, delta uint64) (*domain.Quota, error) {
			calls++
			return &domain.Quota{MaxRequestsPerMonth: 1_000_000, UsedRequestCount: int64(delta)}, nil
		},
	})
	apps := map[string]domain.AppStatus{
		"myapp": {
			TenantID:     "t_a",
			RequestCount: 10,
			DedupeID:     "w_fra_1:d_abc:12345",
		},
	}
	appsRaw, _ := json.Marshal(apps)

	// First delivery — cache miss, must call add() once.
	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		svc.quotaRepo.AddRequestCount,
	)
	if calls != 1 {
		t.Fatalf("first delivery: addRequestCount called %d times, want 1", calls)
	}

	// Second delivery with the same DedupeID — cache hit, must NOT call add().
	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		svc.quotaRepo.AddRequestCount,
	)
	if calls != 1 {
		t.Errorf("duplicate delivery: addRequestCount called %d times total, want 1 (redelivery must be skipped)", calls)
	}
}

// TestApplyTenantDelta_DistinctDeployments verifies that two deployments
// with different DedupeIDs both contribute their deltas. A redelivery
// from one must not block a legitimate delivery from the other.
func TestApplyTenantDelta_DistinctDeployments(t *testing.T) {
	calls := 0
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, delta uint64) (*domain.Quota, error) {
			calls++
			return &domain.Quota{MaxRequestsPerMonth: 1_000_000, UsedRequestCount: int64(delta)}, nil
		},
	})
	// Two apps, same tenant, distinct DedupeIDs — both must apply.
	apps := map[string]domain.AppStatus{
		"app1": {TenantID: "t_a", RequestCount: 5, DedupeID: "w:d1:100"},
		"app2": {TenantID: "t_a", RequestCount: 7, DedupeID: "w:d2:100"},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		svc.quotaRepo.AddRequestCount,
	)

	if calls != 1 {
		t.Errorf("addRequestCount called %d times, want 1 (one per tenant, not per app)", calls)
	}
	// The delta is the sum of both apps (5+7=12) — verified by the
	// single add() call landing at one tenant. No dedupe skip here
	// because each app carries a distinct DedupeID.
}

// TestApplyTenantDelta_DistinctTenants verifies that two tenants each
// get their own quota row added to, even when their apps share a DedupeID.
// (Defensive — DedupeIDs are scoped to deployment, but the dedupe path
// must not let one tenant block another's delta.)
func TestApplyTenantDelta_DistinctTenants(t *testing.T) {
	calls := 0
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
			calls++
			return &domain.Quota{MaxRequestsPerMonth: 1_000_000, UsedRequestCount: int64(delta)}, nil
		},
	})
	// Two apps, two tenants, distinct DedupeIDs — both must apply (one
	// add() per tenant). The dedupe contract is "same ID = same delta";
	// in practice the worker emits a different DedupeID for each
	// `(worker_id, deployment_id, bucket)` triple so cross-tenant
	// collisions cannot happen. This test pins the multi-tenant happy
	// path so a future refactor doesn't accidentally collapse tenants.
	apps := map[string]domain.AppStatus{
		"app1": {TenantID: "t_a", RequestCount: 5, DedupeID: "w:d1:100"},
		"app2": {TenantID: "t_b", RequestCount: 7, DedupeID: "w:d2:100"},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		svc.quotaRepo.AddRequestCount,
	)

	if calls != 2 {
		t.Errorf("addRequestCount called %d times, want 2 (one per tenant)", calls)
	}
}

// TestApplyTenantDelta_LegacyWorkerNoDedupe verifies backward compat:
// a heartbeat without DedupeID applies on every delivery (the legacy
// behaviour). This is what pre-#418 workers emit.
func TestApplyTenantDelta_LegacyWorkerNoDedupe(t *testing.T) {
	calls := 0
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			calls++
			return &domain.Quota{MaxRequestsPerMonth: 1_000_000, UsedRequestCount: 1}, nil
		},
	})
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_a", RequestCount: 5, DedupeID: ""}, // no dedupe ID — legacy
	}
	appsRaw, _ := json.Marshal(apps)

	// Two deliveries — both apply because DedupeID is empty.
	for i := 0; i < 2; i++ {
		svc.applyTenantDelta(context.Background(), appsRaw,
			func(a *domain.AppStatus) uint64 { return a.RequestCount },
			func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
			func(q *domain.Quota) int64 { return q.UsedRequestCount },
			"requests",
			svc.quotaRepo.AddRequestCount,
		)
	}
	if calls != 2 {
		t.Errorf("addRequestCount called %d times, want 2 (legacy workers with no DedupeID apply every delivery)", calls)
	}
}

// TestDedupeSeen_RecordsAndEvicts verifies the cache primitive directly:
// first call returns false (not seen, and records); second call within
// TTL returns true; after expiry it returns false again.
func TestDedupeSeen_RecordsAndEvicts(t *testing.T) {
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{})

	// First observation — fresh.
	if seen := svc.dedupeSeen("key-1"); seen {
		t.Errorf("first dedupeSeen(\"key-1\") = true, want false (fresh entry)")
	}
	// Second observation within TTL — cached.
	if seen := svc.dedupeSeen("key-1"); !seen {
		t.Errorf("second dedupeSeen(\"key-1\") = false, want true (cache hit)")
	}
	// A different key is independent.
	if seen := svc.dedupeSeen("key-2"); seen {
		t.Errorf("dedupeSeen(\"key-2\") = true, want false (different key)")
	}

	// Force expiry by manipulating the entry's expiresAt backwards.
	svc.dedupeCache.Store("key-1", dedupeEntry{expiresAt: time.Now().Add(-time.Second)})
	if seen := svc.dedupeSeen("key-1"); seen {
		t.Errorf("dedupeSeen(\"key-1\") after expiry = true, want false (expired entry evicted on access)")
	}
}

// Unused import suppression: strconv is used in tests for tenant ID assertions
// but a few of the helper-only tests above don't reference it. Keep the
// import explicit so adding new tests is friction-free.
var _ = strconv.Itoa

// ── Heartbeat handler tests (issue #297) ─────────────────────────────────

// TestExtractHeartbeatWorkerID_Valid parses a partial JSON to extract worker_id.
func TestExtractHeartbeatWorkerID_Valid(t *testing.T) {
	data := []byte(`{"worker_id":"w_fra_abc","type":"heartbeat"}`)
	got := extractHeartbeatWorkerID(data)
	if got != "w_fra_abc" {
		t.Errorf("got %q, want w_fra_abc", got)
	}
}

// TestExtractHeartbeatWorkerID_Invalid returns "unknown" on unparseable JSON.
func TestExtractHeartbeatWorkerID_Invalid(t *testing.T) {
	got := extractHeartbeatWorkerID([]byte(`broken json`))
	if got != "unknown" {
		t.Errorf("got %q, want unknown", got)
	}
}

// TestExtractHeartbeatWorkerID_Empty returns "unknown" on empty input.
func TestExtractHeartbeatWorkerID_Empty(t *testing.T) {
	got := extractHeartbeatWorkerID([]byte(`{}`))
	if got != "" {
		t.Errorf("got %q, want empty string (worker_id absent)", got)
	}
}

// TestExtractTenantIDFromApps_ReturnsFirstTenantID picks the tenant_id from
// the first app it encounters (map iteration order is non-deterministic in Go,
// so we just assert it returns a non-empty tenant_id).
func TestExtractTenantIDFromApps_ReturnsFirstTenantID(t *testing.T) {
	appsRaw := json.RawMessage(`{"app1":{"tenant_id":"t_abc","status":"running"},"app2":{"tenant_id":"t_xyz","status":"starting"}}`)
	got := extractTenantIDFromApps(appsRaw)
	if got == "" {
		t.Errorf("got empty string, expected a non-empty tenant_id")
	}
}

// TestExtractTenantIDFromApps_ReturnsEmptyOnNoApps returns "" when apps is empty.
func TestExtractTenantIDFromApps_ReturnsEmptyOnNoApps(t *testing.T) {
	appsRaw := json.RawMessage(`{}`)
	got := extractTenantIDFromApps(appsRaw)
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

// TestExtractTenantIDFromApps_SingleApp returns the exact tenant_id when
// there is only one app (deterministic).
func TestExtractTenantIDFromApps_SingleApp(t *testing.T) {
	appsRaw := json.RawMessage(`{"myapp":{"tenant_id":"t_single","status":"running"}}`)
	got := extractTenantIDFromApps(appsRaw)
	if got != "t_single" {
		t.Errorf("got %q, want t_single", got)
	}
}

// TestExtractTenantIDFromApps_ReturnsEmptyOnInvalidJSON returns "" on garbage.
func TestExtractTenantIDFromApps_ReturnsEmptyOnInvalidJSON(t *testing.T) {
	got := extractTenantIDFromApps(json.RawMessage(`not json`))
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

// TestExtractTenantIDFromApps_ReturnsEmptyWhenNoTenantIDField returns ""
// when the app statuses don't have a tenant_id field (pre-#297 workers).
func TestExtractTenantIDFromApps_ReturnsEmptyWhenNoTenantIDField(t *testing.T) {
	appsRaw := json.RawMessage(`{"app1":{"status":"running","deployment_id":"d_1"}}`)
	got := extractTenantIDFromApps(appsRaw)
	if got != "" {
		t.Errorf("got %q, want empty (no tenant_id field)", got)
	}
}

// TestHandleHeartbeat_MalformedJSON_DoesNotPanic sends garbage JSON and
// verifies the handler doesn't panic and the log mentions the error.
func TestHandleHeartbeat_MalformedJSON_DoesNotPanic(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	svc := workerSvcForTest(&mockWorkerRepo{
		updateLastSeenFunc: func(_ context.Context, _ string) error { return nil },
		upsertStatusFunc:   func(_ context.Context, _ *domain.WorkerStatus) error { return nil },
	}, &mockQuotaRepo{})

	msg := &nats.Msg{Data: []byte(`{broken json`)}
	svc.handleHeartbeat(context.Background(), msg)

	out := buf.String()
	if !strings.Contains(out, "failed to parse") {
		t.Errorf("expected log line with 'failed to parse'; got %q", out)
	}
}

// TestHandleHeartbeat_ExtractsTenantFromApps verifies the tenant_id fallback:
// when the top-level tenant_id is absent, the handler extracts it from the
// first app's status and uses it for auto-register.
func TestHandleHeartbeat_ExtractsTenantFromApps(t *testing.T) {
	var upsertedTenant string
	svc := workerSvcForTest(&mockWorkerRepo{
		updateLastSeenFunc: func(_ context.Context, _ string) error { return nil },
		upsertStatusFunc: func(_ context.Context, ws *domain.WorkerStatus) error {
			return errors.New("worker_status_worker_id_fkey")
		},
		upsertFunc: func(_ context.Context, tenantID string, _ *domain.RegisterWorkerRequest) (bool, error) {
			upsertedTenant = tenantID
			return true, nil
		},
	}, &mockQuotaRepo{})

	heartbeatJSON := `{
		"type": "heartbeat",
		"timestamp": "2026-07-04T12:00:00Z",
		"worker_id": "w_test_fallback",
		"region": "fra",
		"worker_addr": "203.0.113.10",
		"apps": {
			"myapp": {
				"tenant_id": "t_from_app",
				"deployment_id": "d_1",
				"status": "running",
				"request_count": 0,
				"outbound_bytes": 0,
				"port": 8081
			}
		}
	}`
	msg := &nats.Msg{Data: []byte(heartbeatJSON)}
	svc.handleHeartbeat(context.Background(), msg)

	if upsertedTenant != "t_from_app" {
		t.Errorf("worker upsert called with tenant %q, want t_from_app", upsertedTenant)
	}
}

// TestHandleHeartbeat_WithTopLevelTenantID prefers the top-level tenant_id
// over the one embedded in app statuses.
func TestHandleHeartbeat_WithTopLevelTenantID(t *testing.T) {
	var upsertedTenant string
	svc := workerSvcForTest(&mockWorkerRepo{
		updateLastSeenFunc: func(_ context.Context, _ string) error { return nil },
		upsertStatusFunc: func(_ context.Context, ws *domain.WorkerStatus) error {
			return errors.New("worker_status_worker_id_fkey")
		},
		upsertFunc: func(_ context.Context, tenantID string, _ *domain.RegisterWorkerRequest) (bool, error) {
			upsertedTenant = tenantID
			return true, nil
		},
	}, &mockQuotaRepo{})

	heartbeatJSON := `{
		"type": "heartbeat",
		"timestamp": "2026-07-04T12:00:00Z",
		"worker_id": "w_test_top",
		"region": "fra",
		"worker_addr": "203.0.113.10",
		"tenant_id": "t_top_level",
		"apps": {
			"myapp": {
				"tenant_id": "t_from_app",
				"deployment_id": "d_1",
				"status": "running",
				"request_count": 0,
				"outbound_bytes": 0,
				"port": 8081
			}
		}
	}`
	msg := &nats.Msg{Data: []byte(heartbeatJSON)}
	svc.handleHeartbeat(context.Background(), msg)

	if upsertedTenant != "t_top_level" {
		t.Errorf("worker upsert called with tenant %q, want t_top_level (top-level should take priority)", upsertedTenant)
	}
}

// TestHandleHeartbeat_SuccessPath logs "heartbeat: processed from" on success.
func TestHandleHeartbeat_SuccessPath(t *testing.T) {
	buf, restore := captureLogger(t)
	defer restore()

	svc := workerSvcForTest(&mockWorkerRepo{
		updateLastSeenFunc: func(_ context.Context, _ string) error { return nil },
		upsertStatusFunc:   func(_ context.Context, _ *domain.WorkerStatus) error { return nil },
	}, &mockQuotaRepo{})

	heartbeatJSON := `{
		"type": "heartbeat",
		"timestamp": "2026-07-04T12:00:00Z",
		"worker_id": "w_test_success",
		"region": "fra",
		"worker_addr": "203.0.113.10",
		"tenant_id": "t_success",
		"apps": {
			"myapp": {
				"tenant_id": "t_success",
				"deployment_id": "d_1",
				"status": "running",
				"request_count": 0,
				"outbound_bytes": 0,
				"port": 8081
			}
		}
	}`
	msg := &nats.Msg{Data: []byte(heartbeatJSON)}
	svc.handleHeartbeat(context.Background(), msg)

	out := buf.String()
	if !strings.Contains(out, "heartbeat: processed from") {
		t.Errorf("expected success log line; got %q", out)
	}
	if !strings.Contains(out, "w_test_success") {
		t.Errorf("expected worker_id in log; got %q", out)
	}
}

// TestHandleHeartbeat_AutoRegister_OnFKError verifies the full auto-register
// path: UpsertStatus returns FK error → Upsert is called → UpsertStatus
// retried.
func TestHandleHeartbeat_AutoRegister_OnFKError(t *testing.T) {
	var autoRegCalled bool
	var autoRegTenant string

	svc := workerSvcForTest(&mockWorkerRepo{
		updateLastSeenFunc: func(_ context.Context, _ string) error { return nil },
		upsertStatusFunc: func(_ context.Context, ws *domain.WorkerStatus) error {
			// First call returns FK error; second (retry) succeeds
			if !autoRegCalled {
				return errors.New("worker_status_worker_id_fkey")
			}
			return nil
		},
		upsertFunc: func(_ context.Context, tenantID string, _ *domain.RegisterWorkerRequest) (bool, error) {
			autoRegCalled = true
			autoRegTenant = tenantID
			return true, nil
		},
	}, &mockQuotaRepo{})

	heartbeatJSON := `{
		"type": "heartbeat",
		"timestamp": "2026-07-04T12:00:00Z",
		"worker_id": "w_test_autoreg",
		"region": "fra",
		"worker_addr": "203.0.113.10",
		"tenant_id": "t_autoreg",
		"apps": {
			"myapp": {
				"tenant_id": "t_autoreg",
				"deployment_id": "d_1",
				"status": "running",
				"request_count": 0,
				"outbound_bytes": 0,
				"port": 8081
			}
		}
	}`
	msg := &nats.Msg{Data: []byte(heartbeatJSON)}
	svc.handleHeartbeat(context.Background(), msg)

	if !autoRegCalled {
		t.Error("auto-register Upsert was not called after FK error")
	}
	if autoRegTenant != "t_autoreg" {
		t.Errorf("auto-register called with tenant %q, want t_autoreg", autoRegTenant)
	}
}
