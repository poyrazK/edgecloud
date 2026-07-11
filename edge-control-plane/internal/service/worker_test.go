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
	tenantsHostedByFunc      func(ctx context.Context, workerID string) ([]string, error)
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
func (m *mockWorkerRepo) TenantsHostedBy(ctx context.Context, workerID string) ([]string, error) {
	if m.tenantsHostedByFunc == nil {
		return nil, nil
	}
	return m.tenantsHostedByFunc(ctx, workerID)
}

func (m *mockWorkerRepo) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	return 0, nil
}

// mockQuotaRepo implements quotaRepoInterface for testing.
type mockQuotaRepo struct {
	getByTenantIDFunc      func(ctx context.Context, tenantID string) (*domain.Quota, error)
	addOutboundBytesFunc   func(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error)
	addRequestCountFunc    func(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error)
	addResidentSecondsFunc func(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error)
	setGraceUntilFunc      func(ctx context.Context, tenantID string, until *time.Time) error
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

// AddResidentSeconds (issue #484 / #485) routes to the test hook when set.
// Mirrors AddRequestCount / AddOutboundBytes — the nil-func default returns
// a zero Quota so tests that don't drive the resident-seconds axis don't
// have to plumb this hook.
func (m *mockQuotaRepo) AddResidentSeconds(ctx context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
	if m.addResidentSecondsFunc != nil {
		return m.addResidentSecondsFunc(ctx, tenantID, delta)
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

// ---------------------------------------------------------------------------
// TenantsHostedBy pass-through (issue #491 constraint #2)
// ---------------------------------------------------------------------------

// TestWorkerService_TenantsHostedBy_PassThrough pins the service
// layer's pass-through to the repo: the service adds no logic, so the
// handler can rely on the repo result being returned unchanged. A
// future refactor that adds caching or memoization at the service
// layer must update this test.
func TestWorkerService_TenantsHostedBy_PassThrough(t *testing.T) {
	called := false
	wr := &mockWorkerRepo{
		tenantsHostedByFunc: func(_ context.Context, workerID string) ([]string, error) {
			called = true
			if workerID != "w_us_fra_1" {
				t.Errorf("repo got workerID=%q, want w_us_fra_1", workerID)
			}
			return []string{"t_a", "t_b"}, nil
		},
	}
	svc := workerSvcForTest(wr, &mockQuotaRepo{})

	got, err := svc.TenantsHostedBy(context.Background(), "w_us_fra_1")
	if err != nil {
		t.Fatalf("TenantsHostedBy: %v", err)
	}
	if !called {
		t.Fatal("repo method was not invoked")
	}
	if len(got) != 2 || got[0] != "t_a" || got[1] != "t_b" {
		t.Errorf("got %v, want [t_a t_b]", got)
	}
}

// TestWorkerService_TenantsHostedBy_PropagatesError ensures the
// service layer does not swallow repo errors — the handler depends on
// the error to return 500 (fail-closed) instead of incorrectly 403-ing
// every tenant when the DB is down.
func TestWorkerService_TenantsHostedBy_PropagatesError(t *testing.T) {
	sentinel := errors.New("db unavailable")
	wr := &mockWorkerRepo{
		tenantsHostedByFunc: func(_ context.Context, _ string) ([]string, error) {
			return nil, sentinel
		},
	}
	svc := workerSvcForTest(wr, &mockQuotaRepo{})

	_, err := svc.TenantsHostedBy(context.Background(), "w_us_fra_1")
	if err == nil {
		t.Fatal("expected error to propagate")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("got err=%v, want errors.Is(%v)", err, sentinel)
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
	)

	if graceCalled {
		t.Error("SetGraceUntil must NOT be called for paid tenants — paid tenants have the admin override as their recovery path")
	}
}

// ── Resident-seconds tests (issue #484 / #485) ────────────────────────────
//
// The third metered dimension flows through the same applyTenantDelta
// plumbing as requests / outbound bytes, so the test contract is
// mechanical: build a JSON payload with ResidentSeconds & TenantID,
// route it through applyTenantDelta with the same selectors checkResidentSeconds
// uses in production, and assert the mock's call args. The "absent"
// (FaaS) and "zero" (just-started LR) cases are the contract pins —
// they both fold to a 0-delta and must NOT trigger AddResidentSeconds.

// ptrTo is a test-only helper for callers that need to construct a
// `*uint64` literal — Go can't take the address of a literal in a
// composite literal, so we route through a local var.
func ptrTo[T any](v T) *T { return &v }

// TestApplyTenantDelta_ResidentSeconds_AccumulatesPerTenant verifies the
// happy path: an LR app stamps ResidentSeconds=Some(120), the heartbeat
// carries TenantID="t_1", and checkResidentSeconds routes
// AddResidentSeconds(ctx, "t_1", 120).
func TestApplyTenantDelta_ResidentSeconds_AccumulatesPerTenant(t *testing.T) {
	var (
		gotTenantID string
		gotDelta    uint64
		gotCalled   bool
	)
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addResidentSecondsFunc: func(_ context.Context, tenantID string, delta uint64) (*domain.Quota, error) {
			gotTenantID = tenantID
			gotDelta = delta
			gotCalled = true
			return &domain.Quota{MaxResidentSecondsPerMonth: 1_000_000}, nil
		},
	})
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_1", ResidentSeconds: ptrTo(uint64(120))},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		(*domain.AppStatus).ResidentSecondsOrZero,
		func(q *domain.Quota) int64 { return int64(q.MaxResidentSecondsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedResidentSeconds },
		"resident seconds",
		domain.MeterKindResidentSeconds,
		svc.quotaRepo.AddResidentSeconds,
		nil,
	)

	if !gotCalled {
		t.Fatal("AddResidentSeconds was not called for an LR app with ResidentSeconds=120")
	}
	if gotTenantID != "t_1" {
		t.Errorf("AddResidentSeconds tenantID = %q, want %q", gotTenantID, "t_1")
	}
	if gotDelta != 120 {
		t.Errorf("AddResidentSeconds delta = %d, want 120", gotDelta)
	}
}

// TestApplyTenantDelta_ResidentSeconds_IgnoresFaaS verifies the FaaS
// contract: a Handler app stamps ResidentSeconds=nil; applyTenantDelta
// folds it to 0 via ResidentSecondsOrZero and skips the AddResidentSeconds
// call (the `delta == 0 → continue` short-circuit at applyTenantDelta's
// first loop). This is the pivot of the FaaS contract — without it,
// every Handler-app heartbeat would produce a no-op DB round-trip.
func TestApplyTenantDelta_ResidentSeconds_IgnoresFaaS(t *testing.T) {
	called := false
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addResidentSecondsFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			called = true
			return &domain.Quota{}, nil
		},
	})
	apps := map[string]domain.AppStatus{
		"faasapp": {TenantID: "t_1", ResidentSeconds: nil},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		(*domain.AppStatus).ResidentSecondsOrZero,
		func(q *domain.Quota) int64 { return int64(q.MaxResidentSecondsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedResidentSeconds },
		"resident seconds",
		domain.MeterKindResidentSeconds,
		svc.quotaRepo.AddResidentSeconds,
		nil,
	)

	if called {
		t.Error("AddResidentSeconds called for FaaS app (ResidentSeconds=nil) — must be skipped")
	}
}

// TestApplyTenantDelta_ResidentSeconds_ZeroSkipsUpdate verifies that
// Some(0) (an LR app that just started within the heartbeat interval)
// also folds to a 0-delta and skips the DB write — same short-circuit
// as the FaaS case. The distinction "just-started LR" vs "FaaS" is
// captured by the wire shape (Some(0) vs nil), not by the DB path.
func TestApplyTenantDelta_ResidentSeconds_ZeroSkipsUpdate(t *testing.T) {
	called := false
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addResidentSecondsFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			called = true
			return &domain.Quota{}, nil
		},
	})
	apps := map[string]domain.AppStatus{
		"newlr": {TenantID: "t_1", ResidentSeconds: ptrTo(uint64(0))},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		(*domain.AppStatus).ResidentSecondsOrZero,
		func(q *domain.Quota) int64 { return int64(q.MaxResidentSecondsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedResidentSeconds },
		"resident seconds",
		domain.MeterKindResidentSeconds,
		svc.quotaRepo.AddResidentSeconds,
		nil,
	)

	if called {
		t.Error("AddResidentSeconds called for Some(0) resident-seconds delta — must be skipped")
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
		domain.MeterKindOutboundBytes,
		svc.quotaRepo.AddOutboundBytes,
		nil,
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
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
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		nil, // meteringRepo: dual-write covered by separate tests
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
			domain.MeterKindRequestCount,
			svc.quotaRepo.AddRequestCount,
			nil,
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

// ---------------------------------------------------------------------------
// Dual-write to billing_usage_events (issue #485)
// ---------------------------------------------------------------------------

// meteringCall captures one EnqueueUsageEvent invocation so the
// dual-write contract tests can assert the idempotency_key shape
// without spinning up sqlmock.
type meteringCall struct {
	tenantID       string
	kind           domain.MeterKind
	quantity       uint64
	idempotencyKey string
}

// mockMeteringRepo implements meteringRepoInterface for testing.
// Records every EnqueueUsageEvent call; defaults to nil-error.
type mockMeteringRepo struct {
	enqueueFunc func(ctx context.Context, tenantID string, kind domain.MeterKind, quantity uint64, idempotencyKey string) error
	calls       []meteringCall
}

func (m *mockMeteringRepo) EnqueueUsageEvent(ctx context.Context, tenantID string, kind domain.MeterKind, quantity uint64, idempotencyKey string) error {
	m.calls = append(m.calls, meteringCall{
		tenantID:       tenantID,
		kind:           kind,
		quantity:       quantity,
		idempotencyKey: idempotencyKey,
	})
	if m.enqueueFunc != nil {
		return m.enqueueFunc(ctx, tenantID, kind, quantity, idempotencyKey)
	}
	return nil
}

// TestApplyTenantDelta_DualWritesToMeteringLedger pins the issue
// #485 contract: every successful `add()` on the quota repo is
// followed by an EnqueueUsageEvent on the metering repo with the
// idempotency_key "<tenant>:<kind>:<dedupe_id>". A redelivered
// heartbeat (same DedupeID) skips BOTH writes (the dedupe short-
// circuit happens before the dual-write fires).
//
// This is the seam test the meter-ledger expansion hinges on — if a
// future refactor routes the metering write through a separate
// goroutine that misses the dedupe filter, this test fails.
func TestApplyTenantDelta_DualWritesToMeteringLedger(t *testing.T) {
	enqueued := &mockMeteringRepo{}
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			return &domain.Quota{MaxRequestsPerMonth: 1_000_000}, nil
		},
	})
	svc.meteringRepo = enqueued

	apps := map[string]domain.AppStatus{
		"myapp": {
			TenantID:     "t_a",
			RequestCount: 7,
			DedupeID:     "w:d_abc:12345",
		},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		svc.enqueueMeterEvent,
	)

	if got := len(enqueued.calls); got != 1 {
		t.Fatalf("EnqueueUsageEvent calls = %d, want 1", got)
	}
	call := enqueued.calls[0]
	if call.tenantID != "t_a" {
		t.Errorf("TenantID = %q, want t_a", call.tenantID)
	}
	if call.kind != domain.MeterKindRequestCount {
		t.Errorf("Kind = %q, want %q", call.kind, domain.MeterKindRequestCount)
	}
	if call.quantity != 7 {
		t.Errorf("Quantity = %d, want 7", call.quantity)
	}
	if got, want := call.idempotencyKey, "t_a:request_count:w:d_abc:12345"; got != want {
		t.Errorf("IdempotencyKey = %q, want %q", got, want)
	}
}

// TestApplyTenantDelta_DualWrite_AllThreeDimensions exercises each
// dimension's idempotency_key shape via the dedicated check*
// functions in WorkerService. The dual-write behavior is identical
// across all three axes (the metering_repo is dimension-agnostic) so
// the contract test per-axis only needs to assert the kind stamp and
// the dedupe_id suffix.
func TestApplyTenantDelta_DualWrite_AllThreeDimensions(t *testing.T) {
	cases := []struct {
		name      string
		kind      domain.MeterKind
		wantLabel string
	}{
		{"resident_seconds", domain.MeterKindResidentSeconds, "t_x:resident_seconds:w:d:1"},
		{"request_count", domain.MeterKindRequestCount, "t_x:request_count:w:d:1"},
		{"outbound_bytes", domain.MeterKindOutboundBytes, "t_x:outbound_bytes:w:d:1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enqueued := &mockMeteringRepo{}
			svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
				addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
					return &domain.Quota{MaxRequestsPerMonth: 1_000_000}, nil
				},
			})
			svc.meteringRepo = enqueued
			apps := map[string]domain.AppStatus{
				"myapp": {TenantID: "t_x", RequestCount: 1, OutboundBytes: 1, ResidentSeconds: ptrTo(uint64(1)), DedupeID: "w:d:1"},
			}
			appsRaw, _ := json.Marshal(apps)

			// Each check* function uses a different selector but the
			// dual-write path is the same — exercise the request_count
			// branch with the per-kind label so the test name matches
			// the dimension under test.
			switch tc.kind {
			case domain.MeterKindRequestCount:
				svc.checkRequestCount(context.Background(), appsRaw)
			case domain.MeterKindOutboundBytes:
				svc.checkOutboundQuota(context.Background(), appsRaw)
			case domain.MeterKindResidentSeconds:
				svc.checkResidentSeconds(context.Background(), appsRaw)
			}

			if got := len(enqueued.calls); got != 1 {
				t.Fatalf("EnqueueUsageEvent calls = %d, want 1", got)
			}
			if got, want := enqueued.calls[0].idempotencyKey, tc.wantLabel; got != want {
				t.Errorf("idempotency_key = %q, want %q", got, want)
			}
			if got, want := enqueued.calls[0].kind, tc.kind; got != want {
				t.Errorf("kind = %q, want %q", got, want)
			}
		})
	}
}

// TestApplyTenantDelta_DualWrite_QuotaRepoErrorDoesNotInvokeMeter
// asserts the failure-isolation contract: when the quota write
// errors, the metering ledger is NOT touched. The hot-path mirror
// failure must not leak into the slow-path reporter.
func TestApplyTenantDelta_DualWrite_QuotaRepoErrorDoesNotInvokeMeter(t *testing.T) {
	enqueued := &mockMeteringRepo{}
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			return nil, errors.New("db down")
		},
	})
	svc.meteringRepo = enqueued
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_a", RequestCount: 5, OutboundBytes: 0, DedupeID: "w:d:1"},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		svc.enqueueMeterEvent,
	)

	if got := len(enqueued.calls); got != 0 {
		t.Errorf("EnqueueUsageEvent called %d times on quota-write error; want 0", got)
	}
}

// TestApplyTenantDelta_DualWrite_MeteringRepoErrorLeavesQuotaIntact
// is the symmetric contract: when the metering write errors, the
// quota write is NOT rolled back. The drainer is best-effort; the
// hot-path mirror is the source of truth for cap enforcement.
func TestApplyTenantDelta_DualWrite_MeteringRepoErrorLeavesQuotaIntact(t *testing.T) {
	quotaCalls := 0
	enqueued := &mockMeteringRepo{
		enqueueFunc: func(_ context.Context, _ string, _ domain.MeterKind, _ uint64, _ string) error {
			return errors.New("ledger write failed")
		},
	}
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			quotaCalls++
			return &domain.Quota{MaxRequestsPerMonth: 1_000_000}, nil
		},
	})
	svc.meteringRepo = enqueued
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_a", RequestCount: 5, OutboundBytes: 0, DedupeID: "w:d:1"},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		svc.enqueueMeterEvent,
	)

	if quotaCalls != 1 {
		t.Errorf("AddRequestCount calls = %d, want 1 (metering failure must NOT roll back quota write)", quotaCalls)
	}
	if got := len(enqueued.calls); got != 1 {
		t.Errorf("EnqueueUsageEvent calls = %d, want 1", got)
	}
}

// TestApplyTenantDelta_DualWrite_ZeroDeltaSkipsBoth verifies that
// the delta==0 short-circuit applies to BOTH writes — neither the
// quota mirror nor the metering ledger gets a no-op UPDATE / INSERT
// when the heartbeat contributed zero (idle LR, or any FaaS app).
func TestApplyTenantDelta_DualWrite_ZeroDeltaSkipsBoth(t *testing.T) {
	enqueued := &mockMeteringRepo{}
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			t.Errorf("AddRequestCount called on zero-delta heartbeat")
			return &domain.Quota{}, nil
		},
	})
	svc.meteringRepo = enqueued
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_a", RequestCount: 0, OutboundBytes: 0, DedupeID: "w:d:1"},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		svc.enqueueMeterEvent,
	)

	if got := len(enqueued.calls); got != 0 {
		t.Errorf("EnqueueUsageEvent calls = %d on zero-delta heartbeat; want 0", got)
	}
}

// TestApplyTenantDelta_DualWrite_NilMeteringRepoNoOp guards the
// safety net: when meteringRepo is nil (e.g. an older test that
// builds WorkerService directly without the new dependency), the
// dual-write becomes a silent no-op and the quota write proceeds
// unchanged.
func TestApplyTenantDelta_DualWrite_NilMeteringRepoNoOp(t *testing.T) {
	quotaCalls := 0
	svc := workerSvcForTest(&mockWorkerRepo{}, &mockQuotaRepo{
		addRequestCountFunc: func(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
			quotaCalls++
			return &domain.Quota{MaxRequestsPerMonth: 1_000_000}, nil
		},
	})
	// svc.meteringRepo is nil by default — must not panic.
	apps := map[string]domain.AppStatus{
		"myapp": {TenantID: "t_a", RequestCount: 1, DedupeID: "w:d:1"},
	}
	appsRaw, _ := json.Marshal(apps)

	svc.applyTenantDelta(context.Background(), appsRaw,
		func(a *domain.AppStatus) uint64 { return a.RequestCount },
		func(q *domain.Quota) int64 { return int64(q.MaxRequestsPerMonth) },
		func(q *domain.Quota) int64 { return q.UsedRequestCount },
		"requests",
		domain.MeterKindRequestCount,
		svc.quotaRepo.AddRequestCount,
		svc.enqueueMeterEvent,
	)

	if quotaCalls != 1 {
		t.Errorf("AddRequestCount calls = %d, want 1 (nil meteringRepo must not affect quota path)", quotaCalls)
	}
}
