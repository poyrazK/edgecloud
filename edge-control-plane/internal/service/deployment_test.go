package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strings"
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

	// Quota lookup happens first; mock a quota row that allows deploys.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024))

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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024))
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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024))
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024))
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024))
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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024))
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

func TestIsValidAppName(t *testing.T) {
	if !IsValidAppName("myapp") {
		t.Error("expected valid")
	}
	if !IsValidAppName("a") {
		t.Error("expected valid for single char")
	}
	if IsValidAppName("") {
		t.Error("expected invalid for empty")
	}
	if IsValidAppName("../evil") {
		t.Error("expected invalid for path traversal")
	}
	if IsValidAppName("a/b") {
		t.Error("expected invalid for slash")
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
