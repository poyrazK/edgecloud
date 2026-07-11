package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/jmoiron/sqlx"
)

// Mock types for testing non-tx deployment methods.

type mockDeployDeploymentRepo struct {
	getByIDFn            func(ctx context.Context, id string) (*domain.Deployment, error)
	listByAppFn          func(ctx context.Context, tenantID, appName string) ([]domain.Deployment, error)
	countByAppFn         func(ctx context.Context, tenantID, appName string) (int, error)
	listByAppPaginatedFn func(ctx context.Context, tenantID, appName string, limit, offset int) ([]domain.Deployment, error)
	createFn             func(ctx context.Context, deployment *domain.Deployment) error
	deleteByIDFn         func(ctx context.Context, id string) error
}

func (m *mockDeployDeploymentRepo) WithTx(tx *sqlx.Tx) *repository.DeploymentRepository { return nil }
func (m *mockDeployDeploymentRepo) GetByID(ctx context.Context, id string) (*domain.Deployment, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	return nil, nil
}
func (m *mockDeployDeploymentRepo) ListByApp(ctx context.Context, tenantID, appName string) ([]domain.Deployment, error) {
	if m.listByAppFn != nil {
		return m.listByAppFn(ctx, tenantID, appName)
	}
	return nil, nil
}
func (m *mockDeployDeploymentRepo) CountByApp(ctx context.Context, tenantID, appName string) (int, error) {
	if m.countByAppFn != nil {
		return m.countByAppFn(ctx, tenantID, appName)
	}
	return 0, nil
}
func (m *mockDeployDeploymentRepo) ListByAppPaginated(ctx context.Context, tenantID, appName string, limit, offset int) ([]domain.Deployment, error) {
	if m.listByAppPaginatedFn != nil {
		return m.listByAppPaginatedFn(ctx, tenantID, appName, limit, offset)
	}
	return nil, nil
}
func (m *mockDeployDeploymentRepo) Create(ctx context.Context, deployment *domain.Deployment) error {
	if m.createFn != nil {
		return m.createFn(ctx, deployment)
	}
	return nil
}
func (m *mockDeployDeploymentRepo) DeleteByID(ctx context.Context, id string) error {
	if m.deleteByIDFn != nil {
		return m.deleteByIDFn(ctx, id)
	}
	return nil
}

type mockDeployActiveRepo struct {
	getFn          func(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error)
	listByTenantFn func(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error)
}

func (m *mockDeployActiveRepo) WithTx(tx *sqlx.Tx) *repository.ActiveDeploymentRepository { return nil }
func (m *mockDeployActiveRepo) Get(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	if m.getFn != nil {
		return m.getFn(ctx, tenantID, appName)
	}
	return nil, nil
}
func (m *mockDeployActiveRepo) GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.ActiveDeployment, error) {
	return nil, nil
}
func (m *mockDeployActiveRepo) Set(ctx context.Context, ad *domain.ActiveDeployment) error {
	return nil
}
func (m *mockDeployActiveRepo) ClearStableSince(ctx context.Context, tenantID, appName string) error {
	return nil
}
func (m *mockDeployActiveRepo) ListByTenant(ctx context.Context, tenantID string) ([]domain.ActiveDeployment, error) {
	if m.listByTenantFn != nil {
		return m.listByTenantFn(ctx, tenantID)
	}
	return nil, nil
}
func (m *mockDeployActiveRepo) AppendRegionsPublished(ctx context.Context, tenantID, appName string, regions []string, attemptID string, ts time.Time) error {
	return nil
}
func (m *mockDeployActiveRepo) AppendRegionsFailed(ctx context.Context, tenantID, appName string, regions []string, attemptID string, ts time.Time) error {
	return nil
}
func (m *mockDeployActiveRepo) AppendRegionsCacheState(ctx context.Context, tenantID, appName string, succeeded, failed []string, ts time.Time) error {
	return nil
}

// newDeploymentMockDB wires a sqlmock-backed *sqlx.DB for deployment tests.
func newDeploymentMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return sqlxDB, mock, func() { _ = mockDB.Close() }
}

// ── Issue #420: deploy-time 402 PAYMENT_REQUIRED tests ───────────────
// The four pre-checks use different seams (billingRepo, tenantRepo,
// quotaRepo.VerifyUnderCap), so each test mocks only the seam it
// exercises. The mock types satisfy the seam interfaces declared in
// deployment.go and are reusable across all of them.

// mockDeployQuotaRepo satisfies quotaRepoForDeploymentSvc.
type mockDeployQuotaRepo struct {
	getByTenantIDFn        func(ctx context.Context, tenantID string) (*domain.Quota, error)
	verifyUnderCapFn       func(ctx context.Context, tenantID string, projectedReqs, projectedBytes int64) (bool, error)
	verifyMemoryUnderCapFn func(ctx context.Context, tenantID string, perAppMemoryMB int64) (bool, error)
	// verifyMemoryCalls is bumped on every VerifyMemoryUnderCap
	// invocation. Pre-flight tests opt-in by asserting >= 1 after
	// Deploy returns, so a future regression that adds a gate ABOVE
	// pre-check 5 and short-circuits before the memory gate fires is
	// caught here (the mock's default VerifyMemoryUnderCap returns
	// (true, nil) — silent acceptance is invisible without the
	// counter).
	verifyMemoryCalls atomic.Int32
}

func (m *mockDeployQuotaRepo) WithTx(_ *sqlx.Tx) *repository.QuotaRepository { return nil }
func (m *mockDeployQuotaRepo) GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error) {
	if m.getByTenantIDFn != nil {
		return m.getByTenantIDFn(ctx, tenantID)
	}
	// Default to "loose cap" — used by tests that don't care about
	// VerifyUnderCap and only need a non-nil row.
	return &domain.Quota{MaxDeployments: 100, MaxMemoryMB: 256, UsedMemoryMB: 0}, nil
}
func (m *mockDeployQuotaRepo) VerifyUnderCap(ctx context.Context, tenantID string, projectedReqs, projectedBytes int64) (bool, error) {
	if m.verifyUnderCapFn != nil {
		return m.verifyUnderCapFn(ctx, tenantID, projectedReqs, projectedBytes)
	}
	return true, nil
}

// verifyMemoryUnderCapFn defaults to (true, nil) so the existing
// tests don't accidentally regress on memory — only the new memory
// quota tests opt in via the hook. Every invocation bumps the
// verifyMemoryCalls counter so tests can assert the gate was reached.
func (m *mockDeployQuotaRepo) VerifyMemoryUnderCap(ctx context.Context, tenantID string, perAppMemoryMB int64) (bool, error) {
	m.verifyMemoryCalls.Add(1)
	if m.verifyMemoryUnderCapFn != nil {
		return m.verifyMemoryUnderCapFn(ctx, tenantID, perAppMemoryMB)
	}
	return true, nil
}

// AddMemoryMB is required to satisfy quotaRepoForDeploymentSvc but
// is only called inside the activate / rollback tx paths (which the
// mock-backed Deploy tests never reach). Returning (nil, nil) keeps
// the interface satisfied without seeding an active_deployments
// mutation story into these unit tests. Activate/rollback are
// integration-tested via deployment_regions_test.go.
func (m *mockDeployQuotaRepo) AddMemoryMB(ctx context.Context, tenantID string, delta int64) (*domain.Quota, error) {
	return nil, nil
}

// mockDeployTenantRepo satisfies tenantRepoForDeploymentSvc.
type mockDeployTenantRepo struct {
	getByIDFn func(ctx context.Context, id string) (*domain.Tenant, error)
}

func (m *mockDeployTenantRepo) WithTx(_ *sqlx.Tx) *repository.TenantRepository { return nil }
func (m *mockDeployTenantRepo) GetByID(ctx context.Context, id string) (*domain.Tenant, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	// Default to "free tier, not disabled" — disables the tenant /
	// disabled short-circuit but lets quota-based tests proceed.
	return &domain.Tenant{ID: id, Plan: "free"}, nil
}

// GetForUpdate mirrors GetByID — the existing tests don't care about
// the disable-vs-activate race; they just need the gate to pass.
// (The dedicated *_TenantGate_* tests in deployment_regions_test.go
// exercise the disabled / not-found arms directly via sqlmock.)
func (m *mockDeployTenantRepo) GetForUpdate(ctx context.Context, id string) (*domain.Tenant, error) {
	return m.GetByID(ctx, id)
}

// mockDeployBillingRepo satisfies billingRepoForDeploymentSvc.
type mockDeployBillingRepo struct {
	getSubscriptionStatusFn func(ctx context.Context, tenantID string) (domain.SubscriptionStatus, error)
}

func (m *mockDeployBillingRepo) GetSubscriptionStatus(ctx context.Context, tenantID string) (domain.SubscriptionStatus, error) {
	if m.getSubscriptionStatusFn != nil {
		return m.getSubscriptionStatusFn(ctx, tenantID)
	}
	return domain.SubscriptionActive, nil
}

// errIsPaymentRequired confirms the error from Deploy is a
// PaymentRequiredError (i.e., returns 402) and surfaces the reason
// string for test assertion.
func errIsPaymentRequired(t *testing.T, err error) (string, bool) {
	t.Helper()
	if err == nil {
		return "", false
	}
	var pr *PaymentRequiredError
	if errors.As(err, &pr) {
		return pr.Reason, true
	}
	// Fallback: maybe wrapped
	if errors.Is(err, ErrPaymentRequired) {
		return "wrapped", true
	}
	return "", false
}

// validWasmBytes is the smallest sequence that passes validateWasm: the
// 4-byte magic (\0asm) is enough for a first-line check. Real modules
// need more, but the guard's job is to catch obviously-non-wasm inputs,
// not to validate the full module.
var validWasmBytes = []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}

// TestDeploy_RejectsNonWasmBytes exercises the 4-byte magic-byte
// peek inside the tx callback. Without the peek, a non-wasm payload
// would be hashed, stored on disk, and shipped to workers — failing
// only at execution time and wasting storage on broken deployments.
//
// The test sets up sqlmock expectations for everything Deploy does
// up to the tx (quota lookup, deployment count, Begin), then asserts
// that the tx is rolled back (Rollback) and the deployment INSERT is
// NOT issued: the magic-byte peek fails first, returning ErrInvalidWasm.
func TestDeploy_RejectsNonWasmBytes(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	tmpDir := t.TempDir()

	// pinned UTC month-start so the month-rollover CASE in the
	// accumulator SQL is inert (issue #44 part 2 added used_memory_mb).
	periodStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	// Quota lookup happens first; mock a quota row that allows deploys.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024, 100_000, 0, 0, 0, periodStart, nil))
	// Issue #420: deploy-time cap verification runs after the quota
	// lookup and before the CountByApp call. Return a single row so
	// the cap passes.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_request_count = used_request_count + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	// Issue #44 part 2: memory gate runs after VerifyUnderCap and
	// before CountByApp. Mock the verifying UPDATE returning the
	// tenant_id row (within-cap); rejection paths are covered by
	// TestDeploy_MemoryQuota_Rejects402 below.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))

	// CountByApp is the second DB call.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM deployments`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	// The tx begins, the magic-byte peek fails, the tx rolls back.
	mock.ExpectBegin()
	mock.ExpectRollback()

	svc := &DeploymentService{
		db:             db, // enable the tx-wrap path
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewFSArtifactStore(tmpDir),
		keyring:        signing.TestKeyring(t),
	}

	bad := bytes.NewReader([]byte("this is not a wasm binary — no magic bytes"))
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp", bad, nil, false, 0, nil, nil, "", [32]byte{})
	if err == nil {
		t.Fatal("expected error for non-wasm bytes, got nil")
	}
	if !errors.Is(err, ErrInvalidWasm) {
		t.Errorf("err = %v, want errors.Is(err, ErrInvalidWasm) == true", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestDeploy_AcceptsWasmBytes sanity-checks the happy path: a payload
// that starts with the wasm magic bytes passes the peek and proceeds
// to the deployment INSERT inside the tx. The test asserts the tx
// commits (Begin + INSERT + Commit).
func TestDeploy_AcceptsWasmBytes(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	tmpDir := t.TempDir()

	// pinned UTC month-start so the month-rollover CASE in the
	// accumulator SQL is inert (issue #44 part 2 added used_memory_mb).
	periodStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024, 100_000, 0, 0, 0, periodStart, nil))
	// Issue #420: deploy-time cap verification runs before
	// CountByApp. Return a single row so the projection (1 request,
	// 0 bytes) passes the cap.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_request_count = used_request_count + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	// Issue #44 part 2: memory gate runs after VerifyUnderCap and
	// before CountByApp. Mock the verifying UPDATE returning the
	// tenant_id row (within-cap); rejection paths are covered by
	// TestDeploy_MemoryQuota_Rejects402 below.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM deployments`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	svc := &DeploymentService{
		db:             db, // enable the tx-wrap path
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewFSArtifactStore(tmpDir),
		keyring:        signing.TestKeyring(t),
	}

	good := bytes.NewReader(validWasmBytes)
	dep, fromCache, err := svc.Deploy(context.Background(), "t_test", "myapp", good, nil, false, 0, nil, nil, "", [32]byte{})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if fromCache {
		t.Errorf("fromCache = true, want false on a fresh deploy with no Idempotency-Key")
	}
	if dep == nil || !strings.HasPrefix(dep.ID, "d_") {
		t.Errorf("deployment.ID = %v, want prefix 'd_'", dep)
	}
	if !time.Now().After(dep.CreatedAt.Add(-1 * time.Minute)) {
		t.Errorf("deployment.CreatedAt = %v, want recent", dep.CreatedAt)
	}
	if dep.Hash == "" {
		t.Error("deployment.Hash = \"\", want populated (SaveAndHash should set it)")
	}

	// Issue #307: the happy path must stamp a base64url Ed25519
	// signature onto the deployment row plus the signing key id.
	// Without these, a worker running with EDGE_REQUIRE_SIGNATURE=true
	// would reject the artifact at instantiation time — a silent
	// regression from the previous behavior.
	keyring := signing.TestKeyring(t)
	if dep.Signature == "" {
		t.Error("deployment.Signature = \"\", want populated (Keyring.Sign should set it)")
	}
	if dep.SigningKeyID != keyring.ActiveKeyID() {
		t.Errorf("deployment.SigningKeyID = %q, want %q", dep.SigningKeyID, keyring.ActiveKeyID())
	}
	// And the signature must verify against the same keypair —
	// round-trip check that catches any future drift in the signed
	// message layout (the canonical closure of issue #307).
	ok, vErr := keyring.Verify(dep.Hash, dep.ID, dep.Signature, keyring.ActiveKeyID())
	if vErr != nil {
		t.Fatalf("Verify: %v", vErr)
	}
	if !ok {
		t.Error("signature produced by Deploy did not verify against the test key")
	}
}

// TestValidateWasm_AlreadyCoveredInMigrationTest points readers at the
// existing comprehensive test for the validateWasm function itself
// (see migration_test.go). The test here asserts only the integration:
// the guard is wired into the Deploy hot path.
var _ = domain.Deployment{} // keep domain import alive if future tests remove usage

// ---------------------------------------------------------------------------
// Region validation — service-layer sentinels
//
// Region validation runs BEFORE any DB or storage I/O. The tests below
// confirm that a non-HTTP caller invoking Deploy directly gets a typed
// error (matchable via errors.Is) for the two cases the handler also
// surfaces: invalid charset and over-cap. Belt-and-braces: the handler
// is the user-facing 400 path; this is the defense-in-depth path for
// any future non-HTTP caller (CLI, RPC, internal).
// ---------------------------------------------------------------------------

// TestDeploy_InvalidRegion_ReturnsErrInvalidRegion pins the typed-error
// contract: a region that fails IsValidRegion (e.g. uppercase) makes
// Deploy return an error that matches ErrInvalidRegion via errors.Is.
// The pre-PR #116 string-prefix check on the handler side is gone; this
// test prevents a future regression that drops the sentinel.
func TestDeploy_InvalidRegion_ReturnsErrInvalidRegion(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	tmpDir := t.TempDir()

	svc := &DeploymentService{
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewFSArtifactStore(tmpDir),
		// defaultRegion unset — defensive "global" default in the
		// constructor doesn't matter for this test (validation
		// fires before the default-region fallback is consulted).
		keyring: signing.TestKeyring(t),
	}

	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"us-east", "US-EAST"}, // second is invalid
		false,
		0,
		nil,
		nil, // previewOpts
		"",  // idemKey
		[32]byte{},
	)
	if err == nil {
		t.Fatal("expected error for invalid region, got nil")
	}
	if !errors.Is(err, ErrInvalidRegion) {
		t.Errorf("err = %v, want errors.Is(err, ErrInvalidRegion) == true", err)
	}
}

// TestDeploy_ReportsFirstInvalidRegion pins the behavior introduced in
// the #116 review follow-up: when the input contains multiple invalid
// regions, the error names the FIRST one (not the last). The pre-PR
// loop fell through and reported only the trailing invalid entry; this
// test would have caught that bug if it had existed when the loop was
// written.
func TestDeploy_ReportsFirstInvalidRegion(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	tmpDir := t.TempDir()

	svc := &DeploymentService{
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewFSArtifactStore(tmpDir),
		keyring:        signing.TestKeyring(t),
	}

	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"us-east", "BAD-1", "BAD-2", "eu-west"},
		false,
		0,
		nil,
		nil, // previewOpts
		"",  // idemKey
		[32]byte{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "BAD-1") {
		t.Errorf("err = %q, want it to mention the first invalid region 'BAD-1'", err)
	}
	if strings.Contains(err.Error(), "BAD-2") {
		t.Errorf("err = %q, should NOT mention the second invalid region 'BAD-2'", err)
	}
}

// TestDeploy_TooManyRegions_ReturnsErrTooManyRegions pins the typed-
// error contract for the cap. Defense-in-depth: the handler enforces
// the same cap in parseRegions, but a non-HTTP caller must also get
// a typed error rather than a 500-causing string match.
func TestDeploy_TooManyRegions_ReturnsErrTooManyRegions(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	tmpDir := t.TempDir()

	svc := &DeploymentService{
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewFSArtifactStore(tmpDir),
		keyring:        signing.TestKeyring(t),
	}

	// Build 17 valid regions (a..q) — the cap is 16.
	regions := make([]string, 0, 17)
	for _, c := range "abcdefghijklmnopq" {
		regions = append(regions, string(c))
	}

	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		regions,
		false,
		0,
		nil,
		nil, // previewOpts
		"",  // idemKey
		[32]byte{},
	)
	if err == nil {
		t.Fatal("expected error for over-cap regions, got nil")
	}
	if !errors.Is(err, ErrTooManyRegions) {
		t.Errorf("err = %v, want errors.Is(err, ErrTooManyRegions) == true", err)
	}
	if !strings.Contains(err.Error(), "17") {
		t.Errorf("err = %q, want it to mention the count '17'", err)
	}
	if !strings.Contains(err.Error(), "16") {
		t.Errorf("err = %q, want it to mention the cap '16'", err)
	}
}

// TestDeploy_AtCap_Succeeds pins the boundary: exactly 16 regions
// passes the cap and proceeds (the test mocks the quota + INSERT so
// Deploy can run to completion; we don't assert the row contents
// beyond "no error").
func TestDeploy_AtCap_Succeeds(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	tmpDir := t.TempDir()

	// pinned UTC month-start so the month-rollover CASE in the
	// accumulator SQL is inert (issue #44 part 2 added used_memory_mb).
	periodStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024, 100_000, 0, 0, 0, periodStart, nil))
	// Issue #420: deploy-time cap verification runs before
	// CountByApp. Return a single row so the projection (1 request,
	// 0 bytes) passes the cap.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_request_count = used_request_count + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	// Issue #44 part 2: memory gate runs after VerifyUnderCap and
	// before CountByApp. Mock the verifying UPDATE returning the
	// tenant_id row (within-cap); rejection paths are covered by
	// TestDeploy_MemoryQuota_Rejects402 below.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM deployments`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	svc := &DeploymentService{
		db:             db, // enable the tx-wrap path
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewFSArtifactStore(tmpDir),
		keyring:        signing.TestKeyring(t),
	}

	regions := make([]string, 0, 16)
	for _, c := range "abcdefghijklmnop" {
		regions = append(regions, string(c))
	}

	dep, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		regions,
		false,
		0,
		nil,
		nil, // previewOpts
		"",  // idemKey
		[32]byte{},
	)
	if err != nil {
		t.Fatalf("Deploy at cap: %v", err)
	}
	if dep == nil || !strings.HasPrefix(dep.ID, "d_") {
		t.Errorf("deployment.ID = %v, want prefix 'd_'", dep)
	}
}

// TestDeploy_ArtifactSaveFailure_TxRollsBack verifies that when the
// artifact save fails inside the tx callback, the deployment row
// INSERT is never issued — the tx rolls back, which is the only
// rollback needed. The pre-Commit-2 compensating DELETE FROM
// deployments is no longer required.
//
// Order inside the tx callback: peek magic → SaveAndHash → Create.
// SaveAndHash fails (MkdirAll: parent is a file), so Create never
// runs. The tx aborts and the deployment row is implicitly gone —
// there was never a row to compensate.
func TestDeploy_ArtifactSaveFailure_TxRollsBack(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	// Quota lookup happens first.
	// pinned UTC month-start so the month-rollover CASE in the
	// accumulator SQL is inert (issue #44 part 2 added used_memory_mb).
	periodStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024, 100_000, 0, 0, 0, periodStart, nil))
	// Issue #420: deploy-time cap verification runs before
	// CountByApp. Return a single row so the projection (1 request,
	// 0 bytes) passes the cap.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_request_count = used_request_count + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	// Issue #44 part 2: memory gate runs after VerifyUnderCap and
	// before CountByApp. Mock the verifying UPDATE returning the
	// tenant_id row (within-cap); rejection paths are covered by
	// TestDeploy_MemoryQuota_Rejects402 below.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM deployments`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	// Tx begins; SaveAndHash fails; tx rolls back. NO INSERT INTO
	// deployments — the failure happens before Create is reached.
	mock.ExpectBegin()
	mock.ExpectRollback()

	// Artifact store pointed at a path that does not exist and
	// cannot be created (parent is a file, not a directory).
	tmpDir := t.TempDir()
	blocker := tmpDir + "/blocker"
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	badDir := blocker + "/subdir"

	svc := &DeploymentService{
		db:             db, // enable the tx-wrap path
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewFSArtifactStore(badDir),
		keyring:        signing.TestKeyring(t),
	}

	good := bytes.NewReader(validWasmBytes)
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp", good, nil, false, 0, nil, nil, "", [32]byte{})
	if err == nil {
		t.Fatal("expected Deploy to fail when artifact save fails")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestDeploy_ArtifactSaveFailure_TxPath_CleansUpAppsRow pins the
// Commit-3 invariant: when SaveAndHash fails inside the tx, the
// apps row inserted by CreateIfNotExists must also be cleaned up
// (best-effort, guarded by NOT EXISTS to be safe under concurrent
// deploys). Without this, a failed first deploy would orphan the
// apps row forever, counting against the tenant's max_apps quota.
//
// sqlmock expectations in order:
//  1. CreateIfNotExists: SELECT COUNT(*) FROM apps + INSERT INTO apps
//  2. Deploy: SELECT quota + SELECT count
//  3. Tx: Begin; SaveAndHash fails; Rollback
//  4. POST-tx apps-row cleanup: DELETE FROM apps (NOT EXISTS guard)
func TestDeploy_ArtifactSaveFailure_TxPath_CleansUpAppsRow(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	// CreateIfNotExists: count-by-tenant (under the SELECT FOR UPDATE tx).
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM apps`)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO apps`)).
		WillReturnRows(sqlmock.NewRows([]string{"xmax"}).AddRow(true))

	// DeploymentService.Deploy: quota + count lookups, then tx begins.
	// pinned UTC month-start so the month-rollover CASE in the
	// accumulator SQL is inert (issue #44 part 2 added used_memory_mb).
	periodStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024, 100_000, 0, 0, 0, periodStart, nil))
	// Issue #420: deploy-time cap verification runs before
	// CountByApp. Return a single row so the projection (1 request,
	// 0 bytes) passes the cap.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_request_count = used_request_count + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	// Issue #44 part 2: memory gate runs after VerifyUnderCap and
	// before CountByApp. Mock the verifying UPDATE returning the
	// tenant_id row (within-cap); rejection paths are covered by
	// TestDeploy_MemoryQuota_Rejects402 below.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM deployments`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	// Tx begins; SaveAndHash fails; tx rolls back.
	mock.ExpectBegin()
	mock.ExpectRollback()
	// Apps-row cleanup AFTER the failed tx — guarded DELETE with
	// NOT EXISTS subquery. The repo uses GetContext with
	// `RETURNING true`, so this is a Query, not an Exec.
	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM apps`)).
		WillReturnRows(sqlmock.NewRows([]string{"deleted"}).AddRow(true))

	// Artifact save fails at MkdirAll because the parent is a file.
	tmpDir := t.TempDir()
	blocker := tmpDir + "/blocker"
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	badDir := blocker + "/subdir"

	appSvc := &AppService{
		appRepo:   repository.NewAppRepository(db),
		quotaRepo: &mockQuotaRepoForApps{},
	}

	svc := &DeploymentService{
		db:             db, // enable the tx-wrap path
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewFSArtifactStore(badDir),
		appSvc:         appSvc,
		keyring:        signing.TestKeyring(t),
	}

	good := bytes.NewReader(validWasmBytes)
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp", good, nil, false, 0, nil, nil, "", [32]byte{})
	if err == nil {
		t.Fatal("expected Deploy to fail when artifact save fails")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met (apps-row cleanup may be missing): %v", err)
	}
}

// TestDeploy_PersistsSignedAttestation verifies PR2: a successful
// Deploy attaches a DSSE-wrapped in-toto Statement envelope to
// the deployment row. The deployment_hash matches the artifact
// bytes (we use validWasmBytes, which has a known SHA-256), and
// the envelope's subject digest is that hash. Sanity-checks the
// envelope shape (DSSE payloadType, base64url payload string).
func TestDeploy_PersistsSignedAttestation(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	tmpDir := t.TempDir()

	// pinned UTC month-start so the month-rollover CASE in the
	// accumulator SQL is inert (issue #44 part 2 added used_memory_mb).
	periodStart := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb", "max_requests_per_month", "used_outbound_bytes", "used_request_count", "used_memory_mb", "quota_period_start", "quota_lock_grace_until",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024, 100_000, 0, 0, 0, periodStart, nil))
	// Issue #420: deploy-time cap verification runs before
	// CountByApp. Return a single row so the projection (1 request,
	// 0 bytes) passes the cap.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_request_count = used_request_count + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	// Issue #44 part 2: memory gate runs after VerifyUnderCap and
	// before CountByApp. Mock the verifying UPDATE returning the
	// tenant_id row (within-cap); rejection paths are covered by
	// TestDeploy_MemoryQuota_Rejects402 below.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + 0`)).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("t_test"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM deployments`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	svc := &DeploymentService{
		db:             db,
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewFSArtifactStore(tmpDir),
		keyring:        signing.TestKeyring(t),
	}

	good := bytes.NewReader(validWasmBytes)
	dep, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		good, nil, false, 0, nil, nil, "", [32]byte{})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if len(dep.BuildAttestation) == 0 {
		t.Fatalf("BuildAttestation is empty; want non-empty JSONB")
	}
	var env map[string]any
	if jerr := json.Unmarshal(dep.BuildAttestation, &env); jerr != nil {
		t.Fatalf("BuildAttestation is not valid JSON: %v", jerr)
	}
	if env["payloadType"] != "application/vnd.in-toto+json" {
		t.Errorf("payloadType = %v, want 'application/vnd.in-toto+json'", env["payloadType"])
	}
	pl, ok := env["payload"].(string)
	if !ok || len(pl) == 0 {
		t.Fatalf("payload missing or not a string: %v", env["payload"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestGetDeployment_FoundAndTenantMatches(t *testing.T) {
	repo := &mockDeployDeploymentRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Deployment, error) {
			return &domain.Deployment{ID: id, TenantID: "t_test"}, nil
		},
	}
	svc := &DeploymentService{deploymentRepo: repo}
	d, err := svc.GetDeployment(context.Background(), "t_test", "d_1")
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if d == nil || d.ID != "d_1" {
		t.Errorf("unexpected deployment: %+v", d)
	}
}

func TestGetDeployment_TenantMismatch(t *testing.T) {
	repo := &mockDeployDeploymentRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Deployment, error) {
			return &domain.Deployment{ID: id, TenantID: "t_other"}, nil
		},
	}
	svc := &DeploymentService{deploymentRepo: repo}
	d, err := svc.GetDeployment(context.Background(), "t_test", "d_1")
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if d != nil {
		t.Errorf("expected nil for tenant mismatch, got %+v", d)
	}
}

func TestGetDeployment_NotFound(t *testing.T) {
	svc := &DeploymentService{deploymentRepo: &mockDeployDeploymentRepo{}}
	d, err := svc.GetDeployment(context.Background(), "t_test", "d_missing")
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if d != nil {
		t.Errorf("expected nil, got %+v", d)
	}
}

func TestListDeployments_ReturnsRows(t *testing.T) {
	repo := &mockDeployDeploymentRepo{
		listByAppFn: func(_ context.Context, tenantID, appName string) ([]domain.Deployment, error) {
			return []domain.Deployment{
				{ID: "d_1", TenantID: tenantID, AppName: appName},
				{ID: "d_2", TenantID: tenantID, AppName: appName},
			}, nil
		},
	}
	svc := &DeploymentService{deploymentRepo: repo}
	list, err := svc.ListDeployments(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("ListDeployments: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("got %d, want 2", len(list))
	}
}

func TestListDeploymentsPaginated_AppliesDefaults(t *testing.T) {
	var capturedLimit, capturedOffset int
	repo := &mockDeployDeploymentRepo{
		listByAppPaginatedFn: func(_ context.Context, _, _ string, limit, offset int) ([]domain.Deployment, error) {
			capturedLimit, capturedOffset = limit, offset
			return nil, nil
		},
	}
	svc := &DeploymentService{deploymentRepo: repo}
	_, _ = svc.ListDeploymentsPaginated(context.Background(), "t_test", "myapp", 0, -1)
	if capturedLimit != 20 {
		t.Errorf("limit = %d, want 20", capturedLimit)
	}
	if capturedOffset != 0 {
		t.Errorf("offset = %d, want 0", capturedOffset)
	}
}

func TestListDeploymentsPaginated_CapsAt100(t *testing.T) {
	var capturedLimit int
	repo := &mockDeployDeploymentRepo{
		listByAppPaginatedFn: func(_ context.Context, _, _ string, limit, offset int) ([]domain.Deployment, error) {
			capturedLimit = limit
			return nil, nil
		},
	}
	svc := &DeploymentService{deploymentRepo: repo}
	_, _ = svc.ListDeploymentsPaginated(context.Background(), "t_test", "myapp", 200, 0)
	if capturedLimit != 100 {
		t.Errorf("limit = %d, want 100", capturedLimit)
	}
}

func TestListDeploymentsPaginatedWithTotal(t *testing.T) {
	repo := &mockDeployDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) {
			return 42, nil
		},
		listByAppPaginatedFn: func(_ context.Context, _, _ string, _, _ int) ([]domain.Deployment, error) {
			return []domain.Deployment{{ID: "d_1"}}, nil
		},
	}
	svc := &DeploymentService{deploymentRepo: repo}
	list, total, err := svc.ListDeploymentsPaginatedWithTotal(context.Background(), "t_test", "myapp", 20, 0)
	if err != nil {
		t.Fatalf("ListDeploymentsPaginatedWithTotal: %v", err)
	}
	if total != 42 {
		t.Errorf("total = %d, want 42", total)
	}
	if len(list) != 1 {
		t.Errorf("len(list) = %d, want 1", len(list))
	}
}

func TestGetActiveDeployment_Found(t *testing.T) {
	deploymentRepo := &mockDeployDeploymentRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Deployment, error) {
			return &domain.Deployment{ID: id, TenantID: "t_test"}, nil
		},
	}
	activeRepo := &mockDeployActiveRepo{
		getFn: func(_ context.Context, _, _ string) (*domain.ActiveDeployment, error) {
			return &domain.ActiveDeployment{DeploymentID: "d_1"}, nil
		},
	}
	svc := &DeploymentService{deploymentRepo: deploymentRepo, activeRepo: activeRepo}
	d, err := svc.GetActiveDeployment(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetActiveDeployment: %v", err)
	}
	if d == nil || d.ID != "d_1" {
		t.Errorf("unexpected deployment: %+v", d)
	}
}

func TestGetActiveDeployment_NoActiveRow(t *testing.T) {
	svc := &DeploymentService{activeRepo: &mockDeployActiveRepo{}}
	d, err := svc.GetActiveDeployment(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetActiveDeployment: %v", err)
	}
	if d != nil {
		t.Errorf("expected nil, got %+v", d)
	}
}

func TestGetArtifact_FoundAndTenantMatches(t *testing.T) {
	repo := &mockDeployDeploymentRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Deployment, error) {
			return &domain.Deployment{ID: id, TenantID: "t_test", AppName: "myapp"}, nil
		},
	}
	// Use a real filesystem store for the artifact read.
	tmpDir := t.TempDir()
	store := storage.NewFSArtifactStore(tmpDir)
	path, _ := store.Path("t_test", "myapp", "d_1")
	if err := os.MkdirAll(path[:len(path)-len("/d_1.wasm")], 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte("wasm bytes"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	svc := &DeploymentService{deploymentRepo: repo, artifactStore: store}
	rc, err := svc.GetArtifact(context.Background(), "t_test", "myapp", "d_1", "wasm")
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}
	defer rc.Close()
}

func TestGetArtifact_NotFound(t *testing.T) {
	svc := &DeploymentService{deploymentRepo: &mockDeployDeploymentRepo{}}
	_, err := svc.GetArtifact(context.Background(), "t_test", "myapp", "d_missing", "wasm")
	if err == nil {
		t.Fatal("expected error for missing deployment")
	}
}

func TestGetArtifact_TenantMismatch(t *testing.T) {
	repo := &mockDeployDeploymentRepo{
		getByIDFn: func(_ context.Context, id string) (*domain.Deployment, error) {
			return &domain.Deployment{ID: id, TenantID: "t_other", AppName: "myapp"}, nil
		},
	}
	svc := &DeploymentService{deploymentRepo: repo}
	_, err := svc.GetArtifact(context.Background(), "t_test", "myapp", "d_1", "wasm")
	if err == nil {
		t.Fatal("expected error for tenant mismatch")
	}
}

// TestIsValidAppName pins the issue #438 unified contract:
// `^[a-z0-9][a-z0-9.\-_]{0,62}$` — 1–63 chars, lowercase alphanumerics,
// dots, underscores, hyphens; first char is a lowercase letter or digit.
// This is the single source of truth for app-name shape across the
// control plane (deploy/activate/rollback/promote/traffic handlers,
// AppService.Create, MigrationService.MigrateTree, and
// MigrationHandler.MigrateTree). The Rust mirror in
// `edge-migrate-lib/src/patterns.rs::is_valid_app_name` carries the
// same accept/reject partitions in lockstep.
func TestIsValidAppName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		// Valid — basic charset
		{"single char", "a", true},
		{"alphanumeric", "hello", true},
		{"with hyphen", "hello-world", true},
		{"with underscore", "hello_world", true},
		{"with dot", "foo.bar", true},
		{"semver-ish suffix", "myapp.v2", true},
		{"underscore suffix", "app_v2", true},
		{"trailing digit", "app123", true},
		{"starts with digit", "0app", true},

		// Valid — length boundary
		{"63 chars", strings.Repeat("a", 63), true},
		{"63 chars mixed", "a" + strings.Repeat("b", 61) + "c", true},

		// Invalid — length
		{"empty", "", false},
		{"64 chars", strings.Repeat("a", 64), false},

		// Invalid — character class
		{"uppercase", "Hello", false},
		{"all uppercase", "HELLO", false},
		{"leading underscore", "_foo", false},
		{"leading hyphen", "-hello", false},
		{"leading dot", ".foo", false},

		// Invalid — path-traversal / charset
		{"slash", "a/b", false},
		{"backslash", `a\b`, false},
		{"space", "hello world", false},
		{"path traversal", "../traversal", false},
		{"path with bad segment", "a/../b", false},

		// Invalid — middle-of-string `..` passes THIS regex's first-char
		// check; the handler layer's `containsPathTraversal`
		// (`internal/handler/deployment.go`) and the storage layer's
		// `validatePathComponent` (`internal/storage/artifact.go`) are
		// the defense-in-depth guards that reject it. The validator
		// alone does not — flag here so the layered contract stays
		// visible to reviewers.
		{"double dot middle passes regex alone", "a..b", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidAppName(tt.input); got != tt.want {
				t.Errorf("IsValidAppName(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsValidRegion(t *testing.T) {
	if !IsValidRegion("us-east-1") {
		t.Error("expected valid")
	}
	if IsValidRegion("") {
		t.Error("expected invalid for empty")
	}
	if IsValidRegion("UPPER") {
		t.Error("expected invalid for uppercase")
	}
	if IsValidRegion("has space") {
		t.Error("expected invalid for space")
	}
	if IsValidRegion("has.dot") {
		t.Error("expected invalid for dot")
	}
}

// ── Issue #420 — deploy-time 402 PAYMENT_REQUIRED tests ───────────────
// These tests directly construct DeploymentService with mock seam
// types (no DB) so we can exercise each pre-check in isolation. They
// do NOT run through the full artifact-save / tx-callback path — the
// 402 returns BEFORE any of that work happens (so no quota UPDATE, no
// artifact write, no COUNT, no INSERT), and we assert that nothing
// happened past the failing check.

// TestDeploy_SubscriptionPastDue_Returns402 covers Pre-check 1:
// billing subscription is past_due → 402 PAYMENT_REQUIRED,
// reason="subscription_past_due".
func TestDeploy_SubscriptionPastDue_Returns402(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	q := &mockDeployQuotaRepo{
		getByTenantIDFn: func(_ context.Context, _ string) (*domain.Quota, error) {
			return &domain.Quota{MaxDeployments: 100}, nil
		},
		verifyUnderCapFn: func(_ context.Context, _ string, _, _ int64) (bool, error) {
			t.Error("VerifyUnderCap must not be called when subscription is past_due (pre-check 1 short-circuits)")
			return false, nil
		},
	}
	b := &mockDeployBillingRepo{
		getSubscriptionStatusFn: func(_ context.Context, _ string) (domain.SubscriptionStatus, error) {
			return domain.SubscriptionPastDue, nil
		},
	}
	svc := &DeploymentService{
		db:             db,
		quotaRepo:      q,
		billingRepo:    b,
		tenantRepo:     &mockDeployTenantRepo{},
		deploymentRepo: &mockDeployDeploymentRepo{},
		artifactStore:  storage.NewFSArtifactStore(t.TempDir()),
	}
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"fra"}, false, 0, nil, nil, "", [32]byte{})
	reason, ok := errIsPaymentRequired(t, err)
	if !ok {
		t.Fatalf("expected PaymentRequiredError, got %v", err)
	}
	if reason != "subscription_past_due" {
		t.Errorf("reason = %q, want %q", reason, "subscription_past_due")
	}
}

// TestDeploy_FreeTierLockdown_Returns402 covers Pre-check 2:
// tenants.disabled_at IS NOT NULL → 402 PAYMENT_REQUIRED,
// reason="free_tier_exceeded". Subscription is active (so pre-check 1
// passes), the free-tier disabled flag is the failing condition.
func TestDeploy_FreeTierLockdown_Returns402(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	disabledAt := time.Now().UTC().Add(-time.Hour)
	q := &mockDeployQuotaRepo{
		verifyUnderCapFn: func(_ context.Context, _ string, _, _ int64) (bool, error) {
			t.Error("VerifyUnderCap must not be called when tenant is disabled (pre-check 2 short-circuits)")
			return false, nil
		},
	}
	b := &mockDeployBillingRepo{
		getSubscriptionStatusFn: func(_ context.Context, _ string) (domain.SubscriptionStatus, error) {
			return domain.SubscriptionActive, nil
		},
	}
	ten := &mockDeployTenantRepo{
		getByIDFn: func(_ context.Context, _ string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: "t_test", Plan: "free", DisabledAt: &disabledAt}, nil
		},
	}
	svc := &DeploymentService{
		db:             db,
		quotaRepo:      q,
		billingRepo:    b,
		tenantRepo:     ten,
		deploymentRepo: &mockDeployDeploymentRepo{},
		artifactStore:  storage.NewFSArtifactStore(t.TempDir()),
	}
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"fra"}, false, 0, nil, nil, "", [32]byte{})
	reason, ok := errIsPaymentRequired(t, err)
	if !ok {
		t.Fatalf("expected PaymentRequiredError, got %v", err)
	}
	if reason != "free_tier_exceeded" {
		t.Errorf("reason = %q, want %q", reason, "free_tier_exceeded")
	}
}

// TestDeploy_OverageGraceSkipsCapCheck covers Pre-check 3:
// tenants.overage_allowed_until > now() → VerifyUnderCap is SKIPPED.
// We assert this via the mockDeployQuotaRepo contract: if
// VerifyUnderCap is called, the test fails. The deployment will fail
// later for an unrelated reason (no DB tx setup), but it should NOT
// have a 402 reason — the grace override means pre-check 4 is
// bypassed.
func TestDeploy_OverageGraceSkipsCapCheck(t *testing.T) {
	_, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	q := &mockDeployQuotaRepo{
		getByTenantIDFn: func(_ context.Context, _ string) (*domain.Quota, error) {
			// With grace active, GetByTenantID is still called (it's
			// the very first thing in Deploy) but quota gets nilled
			// out at pre-check 3.
			return &domain.Quota{MaxDeployments: 100}, nil
		},
		verifyUnderCapFn: func(_ context.Context, _ string, _, _ int64) (bool, error) {
			t.Error("VerifyUnderCap must NOT be called when overage grace is active (pre-check 3 short-circuits pre-check 4)")
			return false, nil
		},
	}
	b := &mockDeployBillingRepo{
		getSubscriptionStatusFn: func(_ context.Context, _ string) (domain.SubscriptionStatus, error) {
			return domain.SubscriptionActive, nil
		},
	}
	grace := time.Now().UTC().Add(time.Hour)
	ten := &mockDeployTenantRepo{
		getByIDFn: func(_ context.Context, _ string) (*domain.Tenant, error) {
			return &domain.Tenant{
				ID:                  "t_test",
				Plan:                "pro",
				OverageAllowedUntil: &grace,
			}, nil
		},
	}
	// We force the count check (pre-check 5) to fail by returning a
	// count error. The exact error doesn't matter — the assertion is
	// that VerifyUnderCap was bypassed (mock), and the returned err
	// is NOT 402.
	dep := &mockDeployDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) {
			return 0, errors.New("forced count failure to short-circuit deploy")
		},
	}
	svc := &DeploymentService{
		// No db → legacy non-tx path: Create() is called instead of
		// db.BeginTx. The test only cares about the pre-check ordering,
		// not the artifact-save path.
		db:             nil,
		quotaRepo:      q,
		billingRepo:    b,
		tenantRepo:     ten,
		deploymentRepo: dep,
		artifactStore:  storage.NewFSArtifactStore(t.TempDir()),
		keyring:        signing.TestKeyring(t),
	}
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"fra"}, false, 0, nil, nil, "", [32]byte{})
	if err == nil {
		t.Fatalf("expected an error from the count check; we should have fallen through to it")
	}
	if errors.Is(err, ErrPaymentRequired) {
		t.Fatalf("got 402, expected a count-quota error after the grace bypassed pre-check 4: %v", err)
	}
	// The exact error is the count-check failure; we just want to be
	// sure we got THAT and not a 402.
	if !strings.Contains(err.Error(), "counting deployments") {
		t.Errorf("err = %v, want a counting-deployments error", err)
	}
}

// TestDeploy_VerifyUnderCap_Returns402 covers Pre-check 4:
// VerifyUnderCap returns false (cap would be exceeded) → 402 with
// reason="quota_will_be_exceeded". Subscription active, no lockdown,
// no grace.
func TestDeploy_VerifyUnderCap_Returns402(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	q := &mockDeployQuotaRepo{
		verifyUnderCapFn: func(_ context.Context, _ string, _, _ int64) (bool, error) {
			return false, nil
		},
	}
	b := &mockDeployBillingRepo{
		getSubscriptionStatusFn: func(_ context.Context, _ string) (domain.SubscriptionStatus, error) {
			return domain.SubscriptionActive, nil
		},
	}
	ten := &mockDeployTenantRepo{
		getByIDFn: func(_ context.Context, _ string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: "t_test", Plan: "pro"}, nil
		},
	}
	dep := &mockDeployDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) {
			t.Error("CountByApp must not be called when VerifyUnderCap fails (pre-check 4 short-circuits)")
			return 0, nil
		},
	}
	svc := &DeploymentService{
		db:             db,
		quotaRepo:      q,
		billingRepo:    b,
		tenantRepo:     ten,
		deploymentRepo: dep,
		artifactStore:  storage.NewFSArtifactStore(t.TempDir()),
	}
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"fra"}, false, 0, nil, nil, "", [32]byte{})
	reason, ok := errIsPaymentRequired(t, err)
	if !ok {
		t.Fatalf("expected PaymentRequiredError, got %v", err)
	}
	if reason != "quota_will_be_exceeded" {
		t.Errorf("reason = %q, want %q", reason, "quota_will_be_exceeded")
	}
}

// TestDeploy_VerifyUnderCap_BoundarySuccess proves the boundary case:
// VerifyUnderCap returns true → deploy proceeds past the gate.
func TestDeploy_VerifyUnderCap_BoundarySuccess(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	q := &mockDeployQuotaRepo{
		verifyUnderCapFn: func(_ context.Context, _ string, projectedReqs, projectedBytes int64) (bool, error) {
			if projectedReqs != 1 || projectedBytes != 0 {
				t.Errorf("default projection = (%d, %d), want (1, 0)", projectedReqs, projectedBytes)
			}
			return true, nil
		},
	}
	b := &mockDeployBillingRepo{
		getSubscriptionStatusFn: func(_ context.Context, _ string) (domain.SubscriptionStatus, error) {
			return domain.SubscriptionActive, nil
		},
	}
	ten := &mockDeployTenantRepo{
		getByIDFn: func(_ context.Context, _ string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: "t_test", Plan: "pro"}, nil
		},
	}
	svc := &DeploymentService{
		db:             db,
		quotaRepo:      q,
		billingRepo:    b,
		tenantRepo:     ten,
		deploymentRepo: &mockDeployDeploymentRepo{},
		artifactStore:  storage.NewFSArtifactStore(t.TempDir()),
	}
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"fra"}, false, 0, nil, nil, "", [32]byte{})
	if err == nil {
		// We expect ErrMaxDeploymentsQuotaExceeded from the deployment
		// count check (default count=0 < MaxDeployments=100, actually
		// no error)... wait we fall through to artifact save which
		// needs DB tx setup. The graceful path test passed the count
		// check; here we let the tx path fail. Either way: we should
		// NOT have a 402.
		t.Log("grace path returned nil; that's fine, but unlikely — leaving as informational")
	}
	if errors.Is(err, ErrPaymentRequired) {
		t.Fatalf("got 402, expected to pass pre-check 4 (VerifyUnderCap returned true): %v", err)
	}
}

// newMinimalDeploymentServiceForRollback builds a DeploymentService
// for the rollback path. Same wiring as the activate helper.
func newMinimalDeploymentServiceForRollback(t *testing.T, db *sqlx.DB) *DeploymentService {
	t.Helper()
	return &DeploymentService{
		db:             db,
		deploymentRepo: repository.NewDeploymentRepository(db),
		activeRepo:     repository.NewActiveDeploymentRepository(db),
		appEnvRepo:     repository.NewAppEnvRepository(db),
		tenantRepo:     repository.NewTenantRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		outboxRepo:     repository.NewOutboxRepository(db),
		defaultRegion:  "us-east",
	}
}

// TestDeploy_MemoryQuota_Rejects402 covers Pre-check 5 (issue #44
// part 2): VerifyMemoryUnderCap returns false (per-app memory push
// the tenant over MaxMemoryMB) → 402 with
// reason="memory_quota_will_be_exceeded". The tenant is enabled,
// subscription active, no overage grace, and VerifyUnderCap (the
// previous gate) succeeds so the memory gate is the one firing.
func TestDeploy_MemoryQuota_Rejects402(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	q := &mockDeployQuotaRepo{
		verifyUnderCapFn: func(_ context.Context, _ string, _, _ int64) (bool, error) {
			return true, nil
		},
		verifyMemoryUnderCapFn: func(_ context.Context, _ string, _ int64) (bool, error) {
			return false, nil
		},
	}
	b := &mockDeployBillingRepo{
		getSubscriptionStatusFn: func(_ context.Context, _ string) (domain.SubscriptionStatus, error) {
			return domain.SubscriptionActive, nil
		},
	}
	ten := &mockDeployTenantRepo{
		getByIDFn: func(_ context.Context, _ string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: "t_test", Plan: "pro"}, nil
		},
	}
	dep := &mockDeployDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) {
			t.Error("CountByApp must not be called when VerifyMemoryUnderCap fails (pre-check 5 short-circuits)")
			return 0, nil
		},
	}
	svc := &DeploymentService{
		db:             db,
		quotaRepo:      q,
		billingRepo:    b,
		tenantRepo:     ten,
		deploymentRepo: dep,
		artifactStore:  storage.NewFSArtifactStore(t.TempDir()),
	}
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"fra"}, false, 0, nil, nil, "", [32]byte{})
	reason, ok := errIsPaymentRequired(t, err)
	if !ok {
		t.Fatalf("expected PaymentRequiredError, got %v", err)
	}
	if reason != "memory_quota_will_be_exceeded" {
		t.Errorf("reason = %q, want %q", reason, "memory_quota_will_be_exceeded")
	}
	// Gate must have been consulted exactly once — if a future
	// pre-check short-circuits above this one, the count will be
	// 0 and this assertion fails.
	if got := q.verifyMemoryCalls.Load(); got != 1 {
		t.Errorf("VerifyMemoryUnderCap call count = %d, want 1 (pre-check 5 must fire)", got)
	}
}

// TestDeploy_MemoryQuota_Accepts proves the boundary case: per-app
// memory fits inside MaxMemoryMB → deploy proceeds past pre-check 5.
// The mock asserts the perAppMemoryMB passed in equals MaxMemoryMB
// (the value perAppMemoryMB(quota) derives when MaxMemoryMB > 0).
func TestDeploy_MemoryQuota_Accepts(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	q := &mockDeployQuotaRepo{
		verifyUnderCapFn: func(_ context.Context, _ string, _, _ int64) (bool, error) {
			return true, nil
		},
		verifyMemoryUnderCapFn: func(_ context.Context, _ string, perApp int64) (bool, error) {
			if perApp != 512 {
				t.Errorf("VerifyMemoryUnderCap perApp = %d, want 512 (MaxMemoryMB)", perApp)
			}
			return true, nil
		},
	}
	b := &mockDeployBillingRepo{
		getSubscriptionStatusFn: func(_ context.Context, _ string) (domain.SubscriptionStatus, error) {
			return domain.SubscriptionActive, nil
		},
	}
	ten := &mockDeployTenantRepo{
		getByIDFn: func(_ context.Context, _ string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: "t_test", Plan: "pro"}, nil
		},
	}
	q.getByTenantIDFn = func(_ context.Context, _ string) (*domain.Quota, error) {
		// Used so pre-check 4 has a non-nil quota. MaxMemoryMB=512 is
		// what the test's VerifyMemoryUnderCap mock expects.
		return &domain.Quota{
			TenantID: "t_test", MaxDeployments: 100, MaxApps: 50,
			MaxWorkers: 10, MaxMemoryMB: 512, MaxOutboundMB: 1024,
			MaxRequestsPerMonth: 100_000,
			QuotaPeriodStart:    time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		}, nil
	}
	dep := &mockDeployDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) {
			// Pre-check 5 passes, so count is consulted. Returning a
			// small number is fine for the purposes of this test —
			// the assertion is on VerifyMemoryUnderCap being called.
			return 1, nil
		},
	}
	svc := &DeploymentService{
		db:             db,
		quotaRepo:      q,
		billingRepo:    b,
		tenantRepo:     ten,
		deploymentRepo: dep,
		artifactStore:  storage.NewFSArtifactStore(t.TempDir()),
	}
	// We don't care if Deploy ultimately fails (artifact save needs a
	// real DB); we only care it didn't fail at pre-check 5.
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"fra"}, false, 0, nil, nil, "", [32]byte{})
	if err != nil {
		var pr *PaymentRequiredError
		if errors.As(err, &pr) && pr.Reason == "memory_quota_will_be_exceeded" {
			t.Fatalf("got unexpected 402 memory_quota_will_be_exceeded: %v", err)
		}
	}
	// Gate must have been consulted exactly once.
	if got := q.verifyMemoryCalls.Load(); got != 1 {
		t.Errorf("VerifyMemoryUnderCap call count = %d, want 1", got)
	}
}

// TestDeploy_MemoryQuotaEnterprisePlan_Bypasses covers the enterprise
// sentinel: max_memory_mb == -1 means unlimited. The repo's
// VerifyMemoryUnderCap implements the short-circuit (via the SQL
// `max_memory_mb = -1 OR ...`), so the service still calls it —
// but it must return true for the unlimited tenant. The fallback
// perAppMemoryMB for MaxMemoryMB=-1 is the 256 default.
func TestDeploy_MemoryQuotaEnterprisePlan_Bypasses(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	q := &mockDeployQuotaRepo{
		verifyUnderCapFn: func(_ context.Context, _ string, _, _ int64) (bool, error) {
			return true, nil
		},
		verifyMemoryUnderCapFn: func(_ context.Context, _ string, perApp int64) (bool, error) {
			if perApp != 256 {
				t.Errorf("perAppMemoryMB with MaxMemoryMB=-1 = %d, want 256 (fallback)", perApp)
			}
			return true, nil
		},
	}
	q.getByTenantIDFn = func(_ context.Context, _ string) (*domain.Quota, error) {
		return &domain.Quota{
			TenantID: "t_test", MaxDeployments: 100, MaxApps: 50,
			MaxWorkers: 10, MaxMemoryMB: -1, MaxOutboundMB: 1024,
			MaxRequestsPerMonth: 100_000,
			QuotaPeriodStart:    time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		}, nil
	}
	b := &mockDeployBillingRepo{
		getSubscriptionStatusFn: func(_ context.Context, _ string) (domain.SubscriptionStatus, error) {
			return domain.SubscriptionActive, nil
		},
	}
	ten := &mockDeployTenantRepo{
		getByIDFn: func(_ context.Context, _ string) (*domain.Tenant, error) {
			return &domain.Tenant{ID: "t_test", Plan: "enterprise"}, nil
		},
	}
	dep := &mockDeployDeploymentRepo{
		countByAppFn: func(_ context.Context, _, _ string) (int, error) {
			return 1, nil
		},
	}
	svc := &DeploymentService{
		db:             db,
		quotaRepo:      q,
		billingRepo:    b,
		tenantRepo:     ten,
		deploymentRepo: dep,
		artifactStore:  storage.NewFSArtifactStore(t.TempDir()),
	}
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"fra"}, false, 0, nil, nil, "", [32]byte{})
	if err != nil {
		var pr *PaymentRequiredError
		if errors.As(err, &pr) && pr.Reason == "memory_quota_will_be_exceeded" {
			t.Fatalf("enterprise plan must bypass memory gate, got 402: %v", err)
		}
	}
	// Enterprise plan must still consult the gate — the short-circuit
	// lives inside VerifyMemoryUnderCap's SQL (max_memory_mb = -1 OR
	// ...). The service calls it; the repo returns true.
	if got := q.verifyMemoryCalls.Load(); got != 1 {
		t.Errorf("VerifyMemoryUnderCap call count = %d, want 1 (service must still call the gate)", got)
	}
}

// TestDeploy_MemoryQuota_OverageGrace_Bypasses mirrors
// TestDeploy_VerifyUnderCap_OverageGrace_Bypasses: when the operator
// has set overage_allowed_until in the future, the deploy pre-flight
// skips the memory check too (quota is set to nil to force the bypass).
func TestDeploy_MemoryQuota_OverageGrace_Bypasses(t *testing.T) {
	db, _, cleanup := newDeploymentMockDB(t)
	defer cleanup()
	q := &mockDeployQuotaRepo{
		verifyMemoryUnderCapFn: func(_ context.Context, _ string, _ int64) (bool, error) {
			t.Error("VerifyMemoryUnderCap must NOT be called when overage grace is active (quota == nil)")
			return false, nil
		},
	}
	b := &mockDeployBillingRepo{
		getSubscriptionStatusFn: func(_ context.Context, _ string) (domain.SubscriptionStatus, error) {
			return domain.SubscriptionActive, nil
		},
	}
	ten := &mockDeployTenantRepo{
		getByIDFn: func(_ context.Context, _ string) (*domain.Tenant, error) {
			future := time.Now().Add(24 * time.Hour)
			return &domain.Tenant{
				ID: "t_test", Plan: "pro",
				OverageAllowedUntil: &future,
			}, nil
		},
	}
	svc := &DeploymentService{
		db:          db,
		quotaRepo:   q,
		billingRepo: b,
		tenantRepo:  ten,
		deploymentRepo: &mockDeployDeploymentRepo{
			countByAppFn: func(_ context.Context, _, _ string) (int, error) {
				// Force a clean exit after the pre-checks so the test
				// doesn't fall through to the artifact-save path (which
				// needs a real DB).
				return 0, errors.New("forced count failure to short-circuit deploy")
			},
		},
		artifactStore: storage.NewFSArtifactStore(t.TempDir()),
		keyring:       signing.TestKeyring(t),
	}
	_, _, err := svc.Deploy(context.Background(), "t_test", "myapp",
		bytes.NewReader(validWasmBytes),
		[]string{"fra"}, false, 0, nil, nil, "", [32]byte{})
	if err == nil {
		t.Fatalf("expected the forced count-check error, got nil")
	}
	if errors.Is(err, ErrPaymentRequired) {
		t.Fatalf("overage grace must bypass memory gate, got 402: %v", err)
	}
	// Overage grace makes quota == nil → the service skips pre-check
	// 4 AND pre-check 5. The gate must NEVER fire while grace is
	// active. If a future refactor lifts the bypass for one gate
	// but not the other, this assertion catches it.
	if got := q.verifyMemoryCalls.Load(); got != 0 {
		t.Errorf("VerifyMemoryUnderCap call count = %d, want 0 (overage grace must skip pre-check 5)", got)
	}
}

// TestActivateDeployment_IncrementsMemoryCounter confirms the
// activate-tx mutation flows through to the quota repo with the
// per-app memory value. The test mirrors TestActivateDeployment_FansOutToAllRegions
// up through the outbox INSERT, then asserts the new in-tx quota
// re-read + AddMemoryMB(+512) calls fire with the right delta and
// that the tx commits.
func TestActivateDeployment_IncrementsMemoryCounter(t *testing.T) {
	pub := newRecordingPublisher()
	svc, drainer, mock, cleanup := activateSvcForTest(t, pub, "global")
	defer cleanup()

	const (
		deploymentID   = "d_mem_inc"
		appName        = "myapp"
		tenantID       = "t_test"
		deploymentHash = "meminc123hash"
	)
	now := time.Now()

	// 1. GetByID
	regionsCol := `{"us-east"}`
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id =`)).
		WithArgs(deploymentID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow(deploymentID, tenantID, appName, domain.StatusDeployed, deploymentHash, regionsCol, now, false, "", "", []byte{}, 0, nil, nil, nil))

	// 2. Begin tx + lockTenantForUpdate (issue #440) +
	// GetForUpdate returns no rows (first activate).
	mock.ExpectBegin()
	expectTenantForUpdateOK(mock, tenantID)
	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs(tenantID, appName).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL WHERE tenant_id = $1 AND app_name = $2`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// 3. In-tx quota read (issue #44 part 2): read once up-front, reused
	// for buildPublishPayload's maxMemoryMB and the in-tx AddMemoryMB
	// UPDATE. env / tenant are then read by buildPublishPayload.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, used_memory_mb, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers",
			"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
			"used_outbound_bytes", "used_request_count", "used_memory_mb",
			"quota_period_start", "quota_lock_grace_until",
		}).AddRow(tenantID, 100, 50, 10, 512, 1024, 100_000, 0, 0, 0, now, nil))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow(tenantID, "T", "pro", `{}`, now))

	// 4. outbox INSERT
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox`)).
		WithArgs(tenantID, appName, "task_update", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// 5. Issue #44 part 2: AddMemoryMB(+512). The quota row was loaded
	// once above (step 3) and reused for both buildPublishPayload and
	// this UPDATE — no second SELECT.
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + $2 WHERE tenant_id = $1`)).
		WithArgs(tenantID, int64(512)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers",
			"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
			"used_outbound_bytes", "used_request_count", "used_memory_mb",
			"quota_period_start", "quota_lock_grace_until",
		}).AddRow(tenantID, 100, 50, 10, 512, 1024, 100_000, 0, 0, 512, now, nil))

	// 6. Commit + drainer flow
	mock.ExpectCommit()
	expectDrainerTickSuccess(t, mock, tenantID, appName, deploymentID,
		[]string{"us-east"}, 512)

	if err := svc.ActivateDeployment(context.Background(), tenantID, appName, deploymentID); err != nil {
		t.Fatalf("ActivateDeployment: %v", err)
	}
	// Drive the drainer so the post-commit publish mocks fire.
	drainer.Tick(context.Background())
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestRollbackDeployment_DecrementsMemoryCounter exercises the
// rollback path: rollback TO d_last_good decrements the rolled-back
// deployment's memory counter and increments the rolled-back-to
// deployment's counter, both inside the tx.
func TestRollbackDeployment_DecrementsMemoryCounter(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	tenantID, appName := "t_test", "myapp"
	now := time.Now()

	// Mock the rollback tx flow:
	// - tx begin → lockTenantForUpdate (issue #440)
	// - getActive → returns active deployment row referencing the new deployment
	// - tx begin → SELECT last_good FROM active_deployments FOR UPDATE
	// - tx read app_env / tenants / quota
	// - outbox INSERT
	// - AddMemoryMB(+256) for last_good
	// - AddMemoryMB(-256) for the current (failed) active
	// - commit

	mock.ExpectBegin()
	expectTenantForUpdateOK(mock, tenantID)

	mock.ExpectQuery(`SELECT.*active_deployments.*FOR UPDATE`).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "app_name", "deployment_id", "last_good_deployment_id",
			"auto_rollback_enabled", "stable_since", "regions_published",
			"regions_failed", "regions_cached", "regions_cache_failed",
			"last_publish_at", "last_publish_attempt_id", "preview_id", "preview_pr_number",
		}).AddRow(tenantID, appName, "d_failed", "d_last_good", true, nil, nil, nil, nil, nil, nil, nil, nil, nil))

	// rollback target re-read: deploymentRepo.GetByID("d_last_good")
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, app_name, status, hash, regions, created_at, auto_rollback_enabled, signature, signing_key_id, build_attestation, desired_replicas, preview_id, preview_pr_number, preview_expires_at FROM deployments WHERE id = $1`)).
		WithArgs("d_last_good").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled", "signature", "signing_key_id", "build_attestation", "desired_replicas", "preview_id", "preview_pr_number", "preview_expires_at"}).
			AddRow("d_last_good", tenantID, appName, domain.StatusDeployed, "lastgoodhash", `{"us-east"}`, now, false, "", "", []byte{}, 0, nil, nil, nil))

	// Quota read for per-app memory capture (rollback line 1580)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, max_deployments, max_apps, max_workers, max_memory_mb, max_outbound_mb, max_requests_per_month, used_outbound_bytes, used_request_count, used_memory_mb, quota_period_start, quota_lock_grace_until FROM quotas WHERE tenant_id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers",
			"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
			"used_outbound_bytes", "used_request_count", "used_memory_mb",
			"quota_period_start", "quota_lock_grace_until",
		}).AddRow(tenantID, 100, 50, 10, 256, 1024, 100_000, 0, 0, 256, now, nil))

	// Set last_good → d_last_good (the rollback target). Production
	// code calls Set then ClearStableSince BEFORE buildPublishPayload.
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO active_deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE active_deployments SET stable_since = NULL`)).
		WithArgs(tenantID, appName).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// In-tx reads for buildPublishPayload: env / tenant. The quota row
	// was loaded once above and reused for buildPublishPayload's
	// maxMemoryMB — no second SELECT (issue #44 part 2 hoisted).
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, app_name, env_key, env_value FROM app_env`)).
		WithArgs(tenantID, appName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, plan, allowlisted_destinations, created_at, disabled_at, overage_allowed_until FROM tenants WHERE id =`)).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "plan", "allowlisted_destinations", "created_at"}).
			AddRow(tenantID, "T", "pro", `{}`, now))

	// outbox INSERT
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO outbox`)).
		WithArgs(tenantID, appName, "task_update", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// memory counter — +256 (last_good), -256 (d_failed)
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + $2 WHERE tenant_id = $1`)).
		WithArgs(tenantID, int64(256)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers",
			"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
			"used_outbound_bytes", "used_request_count", "used_memory_mb",
			"quota_period_start", "quota_lock_grace_until",
		}).AddRow(tenantID, 100, 50, 10, 256, 1024, 100_000, 0, 0, 512, now, nil))
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE quotas SET used_memory_mb = used_memory_mb + $2 WHERE tenant_id = $1`)).
		WithArgs(tenantID, int64(-256)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers",
			"max_memory_mb", "max_outbound_mb", "max_requests_per_month",
			"used_outbound_bytes", "used_request_count", "used_memory_mb",
			"quota_period_start", "quota_lock_grace_until",
		}).AddRow(tenantID, 100, 50, 10, 256, 1024, 100_000, 0, 0, 256, now, nil))

	mock.ExpectCommit()

	svc := newMinimalDeploymentServiceForRollback(t, db)
	if _, err := svc.RollbackDeployment(context.Background(), tenantID, appName); err != nil {
		t.Fatalf("RollbackDeployment: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}
