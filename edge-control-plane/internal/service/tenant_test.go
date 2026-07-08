package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/hashutil"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// ---------------------------------------------------------------------------
// Mock types for TenantService (non-tx paths). Tx-bound tests use sqlmock.
// ---------------------------------------------------------------------------

// mockTenantSvcRepo mocks tenantRepoForTenantSvc. Note the name is distinct
// from worker_test.go's mockTenantRepo.
type mockTenantSvcRepo struct {
	createFn  func(ctx context.Context, tenant *domain.Tenant) error
	getByIDFn func(ctx context.Context, id string) (*domain.Tenant, error)
	listFn    func(ctx context.Context) ([]domain.Tenant, error)
	updateFn  func(ctx context.Context, tenant *domain.Tenant) error
	deleteFn  func(ctx context.Context, id string) error
}

var _ tenantRepoForTenantSvc = (*mockTenantSvcRepo)(nil)

func (m *mockTenantSvcRepo) WithTx(tx *sqlx.Tx) *repository.TenantRepository { return nil }
func (m *mockTenantSvcRepo) Create(ctx context.Context, tenant *domain.Tenant) error {
	if m.createFn != nil {
		return m.createFn(ctx, tenant)
	}
	return nil
}
func (m *mockTenantSvcRepo) GetByID(ctx context.Context, id string) (*domain.Tenant, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	return nil, nil
}
func (m *mockTenantSvcRepo) List(ctx context.Context) ([]domain.Tenant, error) {
	if m.listFn != nil {
		return m.listFn(ctx)
	}
	return nil, nil
}
func (m *mockTenantSvcRepo) Update(ctx context.Context, tenant *domain.Tenant) error {
	if m.updateFn != nil {
		return m.updateFn(ctx, tenant)
	}
	return nil
}
func (m *mockTenantSvcRepo) Delete(ctx context.Context, id string) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}
	return nil
}

type mockQuotaSvcRepo struct {
	createFn        func(ctx context.Context, quota *domain.Quota) error
	getByTenantIDFn func(ctx context.Context, tenantID string) (*domain.Quota, error)
	updateFn        func(ctx context.Context, quota *domain.Quota) error
}

var _ quotaRepoForTenantSvc = (*mockQuotaSvcRepo)(nil)

func (m *mockQuotaSvcRepo) WithTx(tx *sqlx.Tx) *repository.QuotaRepository { return nil }
func (m *mockQuotaSvcRepo) Create(ctx context.Context, quota *domain.Quota) error {
	if m.createFn != nil {
		return m.createFn(ctx, quota)
	}
	return nil
}
func (m *mockQuotaSvcRepo) GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error) {
	if m.getByTenantIDFn != nil {
		return m.getByTenantIDFn(ctx, tenantID)
	}
	return nil, nil
}
func (m *mockQuotaSvcRepo) Update(ctx context.Context, quota *domain.Quota) error {
	if m.updateFn != nil {
		return m.updateFn(ctx, quota)
	}
	return nil
}

type mockAPIKeySvcRepo struct {
	createFn func(ctx context.Context, k *domain.APIKey) error
}

var _ apiKeyRepoForTenantSvc = (*mockAPIKeySvcRepo)(nil)

func (m *mockAPIKeySvcRepo) WithTx(tx *sqlx.Tx) *repository.APIKeyRepository { return nil }
func (m *mockAPIKeySvcRepo) Create(ctx context.Context, k *domain.APIKey) error {
	if m.createFn != nil {
		return m.createFn(ctx, k)
	}
	return nil
}

// tenantSvcForTest wires mock repos into a TenantService. Sqlmock users
// should construct &TenantService{db: db, ...} directly.
func tenantSvcForTest(tr tenantRepoForTenantSvc, qr quotaRepoForTenantSvc, ar apiKeyRepoForTenantSvc) *TenantService {
	return &TenantService{tenantRepo: tr, quotaRepo: qr, apiKeyRepo: ar}
}

// newTenantMockDB returns a sqlmock-backed *sqlx.DB for tx-bound tests.
func newTenantMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	return sqlx.NewDb(mockDB, "postgres"), mock, func() { _ = mockDB.Close() }
}

// ---------------------------------------------------------------------------
// Existing mintAPIKey tests (unchanged)
// ---------------------------------------------------------------------------

func TestMintAPIKey_PopulatesAllFields(t *testing.T) {
	raw, k, err := mintAPIKey("t_test", "my-key", domain.RoleDeveloper)
	if err != nil {
		t.Fatalf("mintAPIKey: %v", err)
	}
	if raw == "" {
		t.Error("rawKey is empty")
	}
	if len(raw) != 64 {
		t.Errorf("rawKey length = %d, want 64 (32 bytes hex-encoded)", len(raw))
	}
	if !isLowerHex(raw) {
		t.Errorf("rawKey %q is not lowercase hex", raw)
	}
	if !strings.HasPrefix(k.ID, "k_") {
		t.Errorf("ID = %q, want prefix 'k_'", k.ID)
	}
	if k.HashAlgorithm != domain.HashAlgorithmArgon2ID {
		t.Errorf("HashAlgorithm = %q, want %q", k.HashAlgorithm, domain.HashAlgorithmArgon2ID)
	}
	if !strings.HasPrefix(k.KeyHash, "$argon2id$") {
		t.Errorf("KeyHash = %q, want PHC prefix '$argon2id$'", k.KeyHash)
	}
	if k.LookupHash == "" {
		t.Error("LookupHash is empty")
	}
	if k.LookupHash != hashutil.SHA256Hex(raw) {
		t.Errorf("LookupHash = %q, want SHA256Hex(rawKey) = %q", k.LookupHash, hashutil.SHA256Hex(raw))
	}
	if k.TenantID != "t_test" {
		t.Errorf("TenantID = %q, want t_test", k.TenantID)
	}
	if k.Name != "my-key" {
		t.Errorf("Name = %q, want my-key", k.Name)
	}
	if k.Role != domain.RoleDeveloper {
		t.Errorf("Role = %q, want %q", k.Role, domain.RoleDeveloper)
	}
}

func TestMintAPIKey_UniqueRawKeys(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		raw, _, err := mintAPIKey("t_test", "k", domain.RoleDeveloper)
		if err != nil {
			t.Fatalf("mintAPIKey: %v", err)
		}
		if seen[raw] {
			t.Fatalf("rawKey %q minted twice — randomness failure", raw)
		}
		seen[raw] = true
	}
}

func TestMintAPIKey_OwnerRoleUsedByBootstrap(t *testing.T) {
	_, k, err := mintAPIKey("t_test", "default", domain.RoleOwner)
	if err != nil {
		t.Fatalf("mintAPIKey: %v", err)
	}
	if k.Role != domain.RoleOwner {
		t.Errorf("Role = %q, want %q (BootstrapTenant relies on this role)", k.Role, domain.RoleOwner)
	}
	if !domain.IsValidRole(k.Role) {
		t.Errorf("Role %q is not valid per domain.IsValidRole", k.Role)
	}
}

func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// TenantService tests
// ---------------------------------------------------------------------------

func TestTenantService_GetTenant_Found(t *testing.T) {
	tr := &mockTenantSvcRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: id, Name: "acme", Plan: "hobby"}, nil
		},
	}
	qr := &mockQuotaSvcRepo{
		getByTenantIDFn: func(_ context.Context, id string) (*domain.Quota, error) {
			return &domain.Quota{TenantID: id, MaxApps: 5}, nil
		},
	}
	svc := tenantSvcForTest(tr, qr, &mockAPIKeySvcRepo{})
	twq, err := svc.GetTenant(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if twq.Tenant.ID != "t_1" || twq.Tenant.Name != "acme" {
		t.Errorf("unexpected tenant: %+v", twq.Tenant)
	}
	if twq.Quota.MaxApps != 5 {
		t.Errorf("unexpected quota: %+v", twq.Quota)
	}
}

func TestTenantService_GetTenant_NotFound(t *testing.T) {
	svc := tenantSvcForTest(&mockTenantSvcRepo{}, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	twq, err := svc.GetTenant(context.Background(), "t_missing")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if twq != nil {
		t.Errorf("expected nil, got %+v", twq)
	}
}

func TestTenantService_GetTenant_QuotaNil(t *testing.T) {
	tr := &mockTenantSvcRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: id, Name: "acme"}, nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	_, err := svc.GetTenant(context.Background(), "t_1")
	if err == nil {
		t.Fatal("expected error for nil quota")
	}
}

func TestTenantService_GetQuota_Found(t *testing.T) {
	qr := &mockQuotaSvcRepo{
		getByTenantIDFn: func(_ context.Context, id string) (*domain.Quota, error) {
			return &domain.Quota{TenantID: id, MaxApps: 10}, nil
		},
	}
	svc := tenantSvcForTest(&mockTenantSvcRepo{}, qr, &mockAPIKeySvcRepo{})
	q, err := svc.GetQuota(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("GetQuota: %v", err)
	}
	if q.MaxApps != 10 {
		t.Errorf("MaxApps = %d, want 10", q.MaxApps)
	}
}

func TestTenantService_GetQuota_NotFound(t *testing.T) {
	svc := tenantSvcForTest(&mockTenantSvcRepo{}, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	q, err := svc.GetQuota(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("GetQuota: %v", err)
	}
	if q != nil {
		t.Errorf("expected nil, got %+v", q)
	}
}

func TestTenantService_ListTenants(t *testing.T) {
	tr := &mockTenantSvcRepo{
		listFn: func(_ context.Context) ([]domain.Tenant, error) {
			return []domain.Tenant{
				{ID: "t_1", Name: "acme"},
				{ID: "t_2", Name: "beta"},
			}, nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	tenants, err := svc.ListTenants(context.Background())
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	if len(tenants) != 2 {
		t.Fatalf("got %d tenants, want 2", len(tenants))
	}
}

func TestTenantService_ListTenants_Empty(t *testing.T) {
	tr := &mockTenantSvcRepo{
		listFn: func(_ context.Context) ([]domain.Tenant, error) {
			return []domain.Tenant{}, nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	tenants, err := svc.ListTenants(context.Background())
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	if len(tenants) != 0 {
		t.Fatalf("got %d tenants, want 0", len(tenants))
	}
}

func TestTenantService_UpdateTenant(t *testing.T) {
	var updated *domain.Tenant
	tr := &mockTenantSvcRepo{
		updateFn: func(_ context.Context, tenant *domain.Tenant) error {
			updated = tenant
			return nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	tnt := &domain.Tenant{ID: "t_1", Name: "acme"}
	if err := svc.UpdateTenant(context.Background(), tnt); err != nil {
		t.Fatalf("UpdateTenant: %v", err)
	}
	if updated != tnt {
		t.Error("Update was not called with the expected tenant")
	}
}

func TestTenantService_DeleteTenant(t *testing.T) {
	var deletedID string
	tr := &mockTenantSvcRepo{
		deleteFn: func(_ context.Context, id string) error {
			deletedID = id
			return nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	if err := svc.DeleteTenant(context.Background(), "t_1"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	if deletedID != "t_1" {
		t.Errorf("deletedID = %q, want t_1", deletedID)
	}
}

func TestTenantService_CreateTenant_HappyPath(t *testing.T) {
	db, mock, cleanup := newTenantMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO tenants`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO quotas`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	svc := &TenantService{
		db:         db,
		tenantRepo: repository.NewTenantRepository(db),
		quotaRepo:  repository.NewQuotaRepository(db),
	}
	// "free" is a valid plan; "hobby" is not defined in planTiers.
	tenant, err := svc.CreateTenant(context.Background(), "acme", "free")
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if tenant.Name != "acme" {
		t.Errorf("Name = %q, want acme", tenant.Name)
	}
	if !strings.HasPrefix(tenant.ID, "t_") {
		t.Errorf("ID = %q, want prefix t_", tenant.ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

func TestTenantService_CreateTenant_UnknownPlan(t *testing.T) {
	db, mock, cleanup := newTenantMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO tenants`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()

	svc := &TenantService{
		db:         db,
		tenantRepo: repository.NewTenantRepository(db),
		quotaRepo:  repository.NewQuotaRepository(db),
	}
	_, err := svc.CreateTenant(context.Background(), "acme", "platinum")
	if err == nil {
		t.Fatal("expected error for unknown plan")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

func TestTenantService_GetEgressAllowlist_HasEntries(t *testing.T) {
	tr := &mockTenantSvcRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Tenant, error) {
			return &domain.Tenant{
				ID:                      id,
				Name:                    "acme",
				AllowlistedDestinations: pq.StringArray{"api.example.com", "*.trusted.io"},
			}, nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	entries, err := svc.GetEgressAllowlist(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("GetEgressAllowlist: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
}

func TestTenantService_GetEgressAllowlist_Empty(t *testing.T) {
	tr := &mockTenantSvcRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Tenant, error) {
			return &domain.Tenant{
				ID:   id,
				Name: "acme",
			}, nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	entries, err := svc.GetEgressAllowlist(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("GetEgressAllowlist: %v", err)
	}
	if entries == nil {
		t.Fatal("expected non-nil empty slice")
	}
	if len(entries) != 0 {
		t.Fatalf("got %d entries, want 0", len(entries))
	}
}

func TestTenantService_GetEgressAllowlist_TenantNotFound(t *testing.T) {
	svc := tenantSvcForTest(&mockTenantSvcRepo{}, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	_, err := svc.GetEgressAllowlist(context.Background(), "t_missing")
	if err == nil {
		t.Fatal("expected error for missing tenant")
	}
}

func TestTenantService_UpdateEgressAllowlist_HappyPath(t *testing.T) {
	var updated *domain.Tenant
	tr := &mockTenantSvcRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: id, Name: "acme"}, nil
		},
		updateFn: func(_ context.Context, t *domain.Tenant) error {
			updated = t
			return nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	err := svc.UpdateEgressAllowlist(context.Background(), "t_1", []string{"api.example.com"})
	if err != nil {
		t.Fatalf("UpdateEgressAllowlist: %v", err)
	}
	if len(updated.AllowlistedDestinations) != 1 {
		t.Fatalf("got %d entries, want 1", len(updated.AllowlistedDestinations))
	}
}

func TestTenantService_UpdateEgressAllowlist_ClearsAllowlist(t *testing.T) {
	var updated *domain.Tenant
	tr := &mockTenantSvcRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: id, Name: "acme"}, nil
		},
		updateFn: func(_ context.Context, t *domain.Tenant) error {
			updated = t
			return nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	err := svc.UpdateEgressAllowlist(context.Background(), "t_1", []string{})
	if err != nil {
		t.Fatalf("UpdateEgressAllowlist: %v", err)
	}
	if len(updated.AllowlistedDestinations) != 0 {
		t.Errorf("expected empty allowlist, got %v", updated.AllowlistedDestinations)
	}
}

func TestTenantService_UpdateEgressAllowlist_TooManyEntries(t *testing.T) {
	entries := make([]string, MaxEgressAllowlistEntries+1)
	for i := range entries {
		entries[i] = "api.example.com"
	}
	var eErr *EgressValidationError
	err := validateEgressAllowlist(entries)
	if !errors.As(err, &eErr) {
		t.Fatalf("expected EgressValidationError, got %v", err)
	}
}

func TestValidateEgressAllowlist_RejectsBareStar(t *testing.T) {
	err := validateEgressAllowlist([]string{"*"})
	var eErr *EgressValidationError
	if !errors.As(err, &eErr) {
		t.Fatalf("expected EgressValidationError for bare '*', got %v", err)
	}
}

func TestValidateEgressAllowlist_RejectsScheme(t *testing.T) {
	err := validateEgressAllowlist([]string{"http://example.com"})
	var eErr *EgressValidationError
	if !errors.As(err, &eErr) {
		t.Fatalf("expected EgressValidationError, got %v", err)
	}
	err = validateEgressAllowlist([]string{"https://example.com/path"})
	if !errors.As(err, &eErr) {
		t.Fatalf("expected EgressValidationError for https+path, got %v", err)
	}
}

func TestValidateEgressAllowlist_RejectsPath(t *testing.T) {
	err := validateEgressAllowlist([]string{"example.com/foo"})
	var eErr *EgressValidationError
	if !errors.As(err, &eErr) {
		t.Fatalf("expected EgressValidationError, got %v", err)
	}
}

func TestValidateEgressAllowlist_RejectsIPAddress(t *testing.T) {
	err := validateEgressAllowlist([]string{"192.168.1.1"})
	var eErr *EgressValidationError
	if !errors.As(err, &eErr) {
		t.Fatalf("expected EgressValidationError for IP, got %v", err)
	}
}

func TestValidateEgressAllowlist_RejectsSingleLabel(t *testing.T) {
	err := validateEgressAllowlist([]string{"localhost"})
	var eErr *EgressValidationError
	if !errors.As(err, &eErr) {
		t.Fatalf("expected EgressValidationError for single label, got %v", err)
	}
}

func TestValidateEgressAllowlist_RejectsHalfWildcard(t *testing.T) {
	err := validateEgressAllowlist([]string{"*.com"})
	var eErr *EgressValidationError
	if !errors.As(err, &eErr) {
		t.Fatalf("expected EgressValidationError for '*.' with single suffix, got %v", err)
	}
}

func TestValidateEgressAllowlist_AcceptsValidWildcard(t *testing.T) {
	err := validateEgressAllowlist([]string{"*.example.com"})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateEgressAllowlist_AcceptsValidFQDN(t *testing.T) {
	err := validateEgressAllowlist([]string{"api.example.com"})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestEgressValidationError_ImplementsError(t *testing.T) {
	err := egressValidationErr("test error")
	if err.Error() != "test error" {
		t.Errorf("Error() = %q, want 'test error'", err.Error())
	}
	var eErr *EgressValidationError
	if !errors.As(err, &eErr) {
		t.Error("egressValidationErr should produce *EgressValidationError")
	}
}

func TestNewTenantService(t *testing.T) {
	svc := NewTenantService(nil, nil, nil, nil)
	if svc == nil {
		t.Fatal("NewTenantService returned nil")
	}
}

func TestTenantService_BootstrapTenant_HappyPath(t *testing.T) {
	db, mock, cleanup := newTenantMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO tenants`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO quotas`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO api_keys`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	svc := &TenantService{
		db:         db,
		tenantRepo: repository.NewTenantRepository(db),
		quotaRepo:  repository.NewQuotaRepository(db),
		apiKeyRepo: repository.NewAPIKeyRepository(db),
	}
	tenant, raw, err := svc.BootstrapTenant(context.Background(), "acme", "free", "default")
	if err != nil {
		t.Fatalf("BootstrapTenant: %v", err)
	}
	if tenant.Name != "acme" || raw == "" {
		t.Errorf("unexpected tenant=%+v raw=%q", tenant, raw)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

func TestTenantService_BootstrapTenant_UnknownPlan(t *testing.T) {
	db, mock, cleanup := newTenantMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO tenants`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()

	svc := &TenantService{
		db:         db,
		tenantRepo: repository.NewTenantRepository(db),
		quotaRepo:  repository.NewQuotaRepository(db),
		apiKeyRepo: repository.NewAPIKeyRepository(db),
	}
	_, _, err := svc.BootstrapTenant(context.Background(), "acme", "platinum", "default")
	if err == nil {
		t.Fatal("expected error for unknown plan")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

func TestTenantService_UpdateTenantPlan_HappyPath(t *testing.T) {
	db, mock, cleanup := newTenantMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at FROM tenants`).
		WithArgs("t_1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow("t_1", "acme", "free", "{}", time.Now()))
	mock.ExpectExec(`UPDATE tenants SET`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT tenant_id, max_deployments`).
		WithArgs("t_1").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "quota_period_start"}).
			AddRow("t_1", 50, 20, 10, 512, 10000, 5000000, 0, 0, time.Now()))
	mock.ExpectExec(`UPDATE quotas SET`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	svc := &TenantService{
		db:         db,
		tenantRepo: repository.NewTenantRepository(db),
		quotaRepo:  repository.NewQuotaRepository(db),
	}
	err := svc.UpdateTenantPlan(context.Background(), "t_1", "pro", true)
	if err != nil {
		t.Fatalf("UpdateTenantPlan: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

func TestTenantService_UpdateTenantPlan_UnknownPlan(t *testing.T) {
	svc := tenantSvcForTest(&mockTenantSvcRepo{}, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	err := svc.UpdateTenantPlan(context.Background(), "t_1", "platinum", false)
	if err == nil {
		t.Fatal("expected error for unknown plan")
	}
}
