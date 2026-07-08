package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
	"github.com/jmoiron/sqlx"
)

var errTestDecryptFail = errors.New("decrypt failed")

func TestValidateSum_Empty(t *testing.T) {
	err := ValidateSum(nil)
	if err == nil {
		t.Fatal("expected error for empty splits")
	}
}

func TestValidateSum_Single100(t *testing.T) {
	err := ValidateSum([]*domain.TrafficSplit{{Weight: 100}})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidateSum_TwoEntriesSum100(t *testing.T) {
	err := ValidateSum([]*domain.TrafficSplit{{Weight: 70}, {Weight: 30}})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidateSum_Sum50(t *testing.T) {
	err := ValidateSum([]*domain.TrafficSplit{{Weight: 50}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateSum_Sum99(t *testing.T) {
	err := ValidateSum([]*domain.TrafficSplit{{Weight: 50}, {Weight: 49}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateSum_Sum101(t *testing.T) {
	err := ValidateSum([]*domain.TrafficSplit{{Weight: 80}, {Weight: 21}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateSum_Single0(t *testing.T) {
	err := ValidateSum([]*domain.TrafficSplit{{Weight: 0}})
	if err == nil {
		t.Fatal("expected error")
	}
}

// --------------------------------------------------------------------------
// Mock helpers for TrafficService
// --------------------------------------------------------------------------

func newMockTrafficDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	return sqlx.NewDb(mockDB, "postgres"), mock, func() { _ = mockDB.Close() }
}

type trafficDeployRepo struct {
	getByIDFn func(ctx context.Context, id string) (*domain.Deployment, error)
}

func (m *trafficDeployRepo) GetByID(ctx context.Context, id string) (*domain.Deployment, error) {
	return m.getByIDFn(ctx, id)
}

type trafficSplitRepo struct {
	getFn       func(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error)
	deleteAllFn func(ctx context.Context, tenantID, appName string) error
}

func (m *trafficSplitRepo) Get(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error) {
	if m.getFn == nil {
		return nil, nil
	}
	return m.getFn(ctx, tenantID, appName)
}
func (m *trafficSplitRepo) DeleteAllForApp(ctx context.Context, tenantID, appName string) error {
	if m.deleteAllFn == nil {
		return nil
	}
	return m.deleteAllFn(ctx, tenantID, appName)
}

type trafficActiveRepo struct {
	getFn func(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error)
}

func (m *trafficActiveRepo) Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	return m.getFn(ctx, tenantID, appName)
}

type trafficTenantRepo struct {
	getByIDFn func(ctx context.Context, id string) (*domain.Tenant, error)
}

func (m *trafficTenantRepo) GetByID(ctx context.Context, id string) (*domain.Tenant, error) {
	if m.getByIDFn == nil {
		return nil, nil
	}
	return m.getByIDFn(ctx, id)
}

type trafficQuotaRepo struct {
	getByTenantIDFn func(ctx context.Context, tenantID string) (*domain.Quota, error)
}

func (m *trafficQuotaRepo) GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error) {
	if m.getByTenantIDFn == nil {
		return nil, nil
	}
	return m.getByTenantIDFn(ctx, tenantID)
}

type trafficAppEnvRepo struct {
	listFn func(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error)
}

func (m *trafficAppEnvRepo) List(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
	if m.listFn == nil {
		return nil, nil
	}
	return m.listFn(ctx, tenantID, appName)
}

type trafficPublisher struct {
	publishFn func(region string, msg *nats.TaskMessage) error
}

func (m *trafficPublisher) PublishTaskUpdate(region string, msg *nats.TaskMessage) error {
	return m.publishFn(region, msg)
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

func TestSetTraffic_DeploymentNotFound(t *testing.T) {
	svc := &TrafficService{
		deploymentRepo: &trafficDeployRepo{
			getByIDFn: func(ctx context.Context, id string) (*domain.Deployment, error) { return nil, nil },
		},
	}
	err := svc.SetTraffic(context.Background(), "t_1", "hello", []domain.TrafficSplitEntry{
		{DeploymentID: "d_missing", Weight: 100},
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %v", err)
	}
}

func TestSetTraffic_DeploymentNotOwned(t *testing.T) {
	svc := &TrafficService{
		deploymentRepo: &trafficDeployRepo{
			getByIDFn: func(ctx context.Context, id string) (*domain.Deployment, error) {
				return &domain.Deployment{ID: "d_1", TenantID: "t_other", AppName: "other", Hash: "abc"}, nil
			},
		},
	}
	err := svc.SetTraffic(context.Background(), "t_1", "hello", []domain.TrafficSplitEntry{
		{DeploymentID: "d_1", Weight: 100},
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %v", err)
	}
}

func TestSetTraffic_ValidateSumFailsBeforeDB(t *testing.T) {
	svc := &TrafficService{
		deploymentRepo: &trafficDeployRepo{
			getByIDFn: func(ctx context.Context, id string) (*domain.Deployment, error) {
				return &domain.Deployment{ID: "d_1", TenantID: "t_1", AppName: "hello", Hash: "abc"}, nil
			},
		},
	}
	err := svc.SetTraffic(context.Background(), "t_1", "hello", []domain.TrafficSplitEntry{
		{DeploymentID: "d_1", Weight: 50},
	})
	if err == nil || !strings.Contains(err.Error(), "must sum to 100") {
		t.Errorf("error = %v", err)
	}
}

func TestSetTraffic_ValidSingleSplit(t *testing.T) {
	db, mock, cleanup := newMockTrafficDB(t)
	defer cleanup()

	svc := &TrafficService{
		db:        db,
		splitRepo: &trafficSplitRepo{},
		deploymentRepo: &trafficDeployRepo{
			getByIDFn: func(ctx context.Context, id string) (*domain.Deployment, error) {
				return &domain.Deployment{ID: "d_1", TenantID: "t_1", AppName: "hello", Hash: "abc"}, nil
			},
		},
		activeRepo:    &trafficActiveRepo{},
		appEnvRepo:    &trafficAppEnvRepo{},
		tenantRepo:    &trafficTenantRepo{},
		quotaRepo:     &trafficQuotaRepo{},
		publisher:     &trafficPublisher{publishFn: func(region string, msg *nats.TaskMessage) error { return nil }},
		defaultRegion: "fra",
	}

	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM app_traffic_splits`).WithArgs("t_1", "hello").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO app_traffic_splits`).WithArgs("t_1", "hello", "d_1", 100).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := svc.SetTraffic(context.Background(), "t_1", "hello", []domain.TrafficSplitEntry{
		{DeploymentID: "d_1", Weight: 100},
	})
	if err != nil {
		t.Fatalf("SetTraffic: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestSetTraffic_PartialRegionPublishFailure(t *testing.T) {
	db, mock, cleanup := newMockTrafficDB(t)
	defer cleanup()

	pubCalls := 0
	svc := &TrafficService{
		db: db,
		splitRepo: &trafficSplitRepo{
			getFn: func(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error) {
				return []*domain.TrafficSplit{{DeploymentID: "d_1", Weight: 100}}, nil
			},
		},
		deploymentRepo: &trafficDeployRepo{
			getByIDFn: func(ctx context.Context, id string) (*domain.Deployment, error) {
				return &domain.Deployment{ID: "d_1", TenantID: "t_1", AppName: "hello", Hash: "abc"}, nil
			},
		},
		activeRepo: &trafficActiveRepo{},
		appEnvRepo: &trafficAppEnvRepo{},
		tenantRepo: &trafficTenantRepo{
			getByIDFn: func(ctx context.Context, id string) (*domain.Tenant, error) {
				return &domain.Tenant{ID: "t_1"}, nil
			},
		},
		quotaRepo: &trafficQuotaRepo{},
		publisher: &trafficPublisher{publishFn: func(region string, msg *nats.TaskMessage) error {
			pubCalls++
			return nil
		}},
		defaultRegion: "fra",
	}

	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM app_traffic_splits`).WithArgs("t_1", "hello").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO app_traffic_splits`).WithArgs("t_1", "hello", "d_1", 100).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := svc.SetTraffic(context.Background(), "t_1", "hello", []domain.TrafficSplitEntry{
		{DeploymentID: "d_1", Weight: 100},
	})
	if err != nil {
		t.Fatalf("SetTraffic: %v", err)
	}
	if pubCalls != 1 {
		t.Errorf("publish calls = %d, want 1", pubCalls)
	}
}

func TestGetTraffic_Delegates(t *testing.T) {
	want := []*domain.TrafficSplit{{DeploymentID: "d_1", Weight: 80}, {DeploymentID: "d_2", Weight: 20}}
	svc := &TrafficService{
		splitRepo: &trafficSplitRepo{
			getFn: func(ctx context.Context, tenantID, appName string) ([]*domain.TrafficSplit, error) { return want, nil },
		},
	}
	got, err := svc.GetTraffic(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("GetTraffic: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2", len(got))
	}
}

func TestClearTraffic_NoActiveDeployment_Noop(t *testing.T) {
	svc := &TrafficService{
		splitRepo: &trafficSplitRepo{deleteAllFn: func(ctx context.Context, tenantID, appName string) error { return nil }},
		activeRepo: &trafficActiveRepo{
			getFn: func(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) { return nil, nil },
		},
	}
	err := svc.ClearTraffic(context.Background(), "t_1", "hello")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

type mockTrafficEnvDecrypter struct {
	decryptFn func(value string) (string, error)
}

func (m *mockTrafficEnvDecrypter) Decrypt(value string) (string, error) {
	if m.decryptFn != nil {
		return m.decryptFn(value)
	}
	return value, nil
}

func TestBuildEnvMap_WithDecrypter(t *testing.T) {
	envs := []domain.AppEnv{
		{EnvKey: "KEY_A", EnvValue: "encrypted_a"},
		{EnvKey: "KEY_B", EnvValue: "encrypted_b"},
	}
	dec := &mockTrafficEnvDecrypter{
		decryptFn: func(v string) (string, error) {
			return "decrypted_" + v[len(v)-1:], nil
		},
	}
	m := buildEnvMap(envs, dec)
	if m["KEY_A"] != "decrypted_a" {
		t.Errorf("KEY_A = %q, want decrypted_a", m["KEY_A"])
	}
	if m["KEY_B"] != "decrypted_b" {
		t.Errorf("KEY_B = %q, want decrypted_b", m["KEY_B"])
	}
}

func TestBuildEnvMap_DecrypterErrorFallsBackToPlaintext(t *testing.T) {
	envs := []domain.AppEnv{
		{EnvKey: "K", EnvValue: "plaintext"},
	}
	dec := &mockTrafficEnvDecrypter{
		decryptFn: func(v string) (string, error) {
			return "", errTestDecryptFail
		},
	}
	m := buildEnvMap(envs, dec)
	if m["K"] != "plaintext" {
		t.Errorf("K = %q, want plaintext (fallback)", m["K"])
	}
}

func TestBuildEnvMap_NoDecrypter(t *testing.T) {
	envs := []domain.AppEnv{
		{EnvKey: "K", EnvValue: "raw"},
	}
	m := buildEnvMap(envs, nil)
	if m["K"] != "raw" {
		t.Errorf("K = %q, want raw", m["K"])
	}
}

func TestBuildEnvMap_EmptyEnvs(t *testing.T) {
	m := buildEnvMap(nil, nil)
	if len(m) != 0 {
		t.Errorf("len = %d, want 0", len(m))
	}
}
