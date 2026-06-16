package service

import (
	"context"
	"errors"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// mockWorkerRepo implements workerRepoInterface for testing.
type mockWorkerRepo struct {
	upsertFunc         func(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error)
	countByTenantFunc  func(ctx context.Context, tenantID string) (int, error)
	deleteFunc         func(ctx context.Context, id string) error
	listByTenantFunc   func(ctx context.Context, tenantID string) ([]domain.Worker, error)
	updateLastSeenFunc func(ctx context.Context, id string) error
	upsertStatusFunc   func(ctx context.Context, ws *domain.WorkerStatus) error
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
func (m *mockWorkerRepo) UpsertStatus(ctx context.Context, ws *domain.WorkerStatus) error {
	return m.upsertStatusFunc(ctx, ws)
}

// mockQuotaRepo implements quotaRepoInterface for testing.
type mockQuotaRepo struct {
	getByTenantIDFunc func(ctx context.Context, tenantID string) (*domain.Quota, error)
}

func (m *mockQuotaRepo) GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error) {
	return m.getByTenantIDFunc(ctx, tenantID)
}

// workerSvcForTest builds a WorkerService with mock dependencies.
func workerSvcForTest(wr *mockWorkerRepo, qr *mockQuotaRepo) *WorkerService {
	return &WorkerService{workerRepo: wr, quotaRepo: qr, nc: nil}
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
