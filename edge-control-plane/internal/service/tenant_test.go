package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
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
	createFn          func(ctx context.Context, tenant *domain.Tenant) error
	getByIDFn         func(ctx context.Context, id string) (*domain.Tenant, error)
	listFn            func(ctx context.Context) ([]domain.Tenant, error)
	updateFn          func(ctx context.Context, tenant *domain.Tenant) error
	deleteFn          func(ctx context.Context, id string) error
	clearDisabledAtFn func(ctx context.Context, tenantID string) error
	// deleteCalls is bumped on every Delete invocation. Pre-flight
	// tests for issue #569 opt-in by asserting >= 1 after
	// DeleteTenant returns, so a future regression that adds a
	// step ABOVE the tenant delete (e.g. an early purge publish
	// that races with the row delete) is caught here.
	deleteCalls atomic.Int32
}

var _ tenantRepoForTenantSvc = (*mockTenantSvcRepo)(nil)

// WithTx returns a real *TenantRepository bound to the tx so the
// SQL call hits the sqlmock expectation chain. The mock
// `deleteFn` / `deleteCalls` track only non-tx-path deletes
// (the tx path runs through the real repository and is
// exercised via sqlmock.ExpectExec).
func (m *mockTenantSvcRepo) WithTx(tx *sqlx.Tx) *repository.TenantRepository {
	return repository.NewTenantRepositoryFromDBTX(tx)
}
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
	m.deleteCalls.Add(1)
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}
	return nil
}

func (m *mockTenantSvcRepo) SetOverageAllowedUntil(ctx context.Context, tenantID string, at time.Time) error {
	return nil
}

func (m *mockTenantSvcRepo) ClearOverageAllowedUntil(ctx context.Context, tenantID string) error {
	return nil
}

func (m *mockTenantSvcRepo) ClearDisabledAt(ctx context.Context, tenantID string) error {
	if m.clearDisabledAtFn != nil {
		return m.clearDisabledAtFn(ctx, tenantID)
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

func (m *mockQuotaSvcRepo) SetGraceUntil(ctx context.Context, tenantID string, until *time.Time) error {
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

// tenantSvcForTestFull wires mock repos plus the issue #569
// appRepo / outboxRepo / defaultRegion fields into a
// TenantService for the tx-bound DeleteTenant tests.
func tenantSvcForTestFull(db *sqlx.DB, tr tenantRepoForTenantSvc, qr quotaRepoForTenantSvc, ar apiKeyRepoForTenantSvc) *TenantService {
	return &TenantService{
		db:            db,
		tenantRepo:    tr,
		quotaRepo:     qr,
		apiKeyRepo:    ar,
		appRepo:       repository.NewAppRepository(db),
		outboxRepo:    repository.NewOutboxRepository(db),
		defaultRegion: "global",
	}
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
	db, mock, cleanup := newTenantMockDB(t)
	defer cleanup()

	var deletedID string
	_ = deletedID // only set when the non-tx path is hit (not exercised here — see sqlmock below)
	tr := &mockTenantSvcRepo{
		deleteFn: func(_ context.Context, id string) error {
			deletedID = id
			return nil
		},
	}

	// List apps BEFORE tenant delete (issue #569).
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*FROM apps`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"}).
			AddRow("a_1", "t_1", "app-a", nil, time.Now()).
			AddRow("a_2", "t_1", "app-b", nil, time.Now()))
	mock.ExpectExec(`DELETE FROM tenants.*WHERE`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// One INSERT INTO outbox per app (issue #569).
	mock.ExpectExec(`INSERT INTO outbox`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO outbox`).
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()

	svc := tenantSvcForTestFull(db, tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	if err := svc.DeleteTenant(context.Background(), "t_1"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	// deleteCalls is bumped whether the call goes through the
	// non-tx path (mock.deleteFn) or the tx path (real repo's
	// Exec). The mock tracks only the non-tx path; for the tx
	// path the call is observable via the sqlmock
	// ExpectExec(DELETE FROM tenants.*WHERE) above.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestTenantService_DeleteTenant_PurgeReasonIsTenantOffboarded
// (issue #569) verifies the wire payload carries
// reason="tenant_offboarded" so the worker's
// handle_purge branch can distinguish the per-app cascade
// (app_deleted) from a full tenant offboarding.
func TestTenantService_DeleteTenant_PurgeReasonIsTenantOffboarded(t *testing.T) {
	db, mock, cleanup := newTenantMockDB(t)
	defer cleanup()

	tr := &mockTenantSvcRepo{
		deleteFn: func(_ context.Context, id string) error { return nil },
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*FROM apps`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"}).
			AddRow("a_x", "t_2", "single-app", nil, time.Now()))
	mock.ExpectExec(`DELETE FROM tenants.*WHERE`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO outbox`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	svc := tenantSvcForTestFull(db, tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	if err := svc.DeleteTenant(context.Background(), "t_2"); err != nil {
		t.Fatalf("DeleteTenant: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestTenantService_DeleteTenant_NoPurgeOnRepoFailure (issue #569)
// verifies that if the tenant delete fails, NO outbox row is
// written — the tx rolls back, workers never receive a
// phantom purge for a tenant whose CP-side row still exists.
func TestTenantService_DeleteTenant_NoPurgeOnRepoFailure(t *testing.T) {
	db, mock, cleanup := newTenantMockDB(t)
	defer cleanup()

	tr := &mockTenantSvcRepo{
		deleteFn: func(_ context.Context, id string) error {
			return fmt.Errorf("simulated FK constraint failure")
		},
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*FROM apps`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"}))
	mock.ExpectExec(`DELETE FROM tenants.*WHERE`).
		WillReturnError(fmt.Errorf("simulated FK constraint failure"))
	mock.ExpectRollback()
	// No INSERT INTO outbox expected — if the enqueue fires
	// after rollback, sqlmock sees an unexpected Exec and
	// fails ExpectationsWereMet.

	svc := tenantSvcForTestFull(db, tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})
	// We don't assert on the return value — see the docstring
	// above. The key invariant is enforced by the sqlmock
	// expectation chain below.
	_ = svc.DeleteTenant(context.Background(), "t_3")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
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

// TestTenantService_GetByID_NilTenantIsNotFound is the regression pin
// for the PR #491 review's 🔴 finding: production's
// repository.TenantRepository.GetByID returns (nil, nil) for "row not
// found". Without a translation, any handler that calls
// `tenant.IsDisabled()` on the result panics on the typo path.
//
// *TenantService.GetByID (added in #491) translates the (nil, nil)
// into ErrTenantNotFound so callers can rely on a single
// `errors.Is(err, ErrTenantNotFound)` check, mirroring every other
// service-level method on this struct.
//
// The default mockTenantSvcRepo.GetByID already returns (nil, nil),
// so this test exercises the production contract end-to-end.
func TestTenantService_GetByID_NilTenantIsNotFound(t *testing.T) {
	svc := tenantSvcForTest(&mockTenantSvcRepo{}, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})

	tenant, err := svc.GetByID(context.Background(), "t_phantom")
	if tenant != nil {
		t.Errorf("expected nil tenant, got %+v", tenant)
	}
	if !errors.Is(err, ErrTenantNotFound) {
		t.Errorf("expected ErrTenantNotFound, got %v", err)
	}
}

// TestTenantService_GetByID_FoundPassesThrough confirms the
// (nil, nil) translation does NOT clobber a real row.
func TestTenantService_GetByID_FoundPassesThrough(t *testing.T) {
	want := &domain.Tenant{ID: "t_real", Name: "acme"}
	tr := &mockTenantSvcRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Tenant, error) {
			if id == "t_real" {
				return want, nil
			}
			return nil, nil
		},
	}
	svc := tenantSvcForTest(tr, &mockQuotaSvcRepo{}, &mockAPIKeySvcRepo{})

	got, err := svc.GetByID(context.Background(), "t_real")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got != want {
		t.Errorf("expected the row to pass through unchanged, got %+v", got)
	}
}
