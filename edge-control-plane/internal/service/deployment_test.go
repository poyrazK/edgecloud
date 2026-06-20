package service

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/jmoiron/sqlx"
)

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

// TestDeploy_RejectsNonWasmBytes exercises the validateWasm guard added
// in Commit 4. Without the guard, a non-wasm payload would be hashed,
// stored on disk, and shipped to workers — failing only at execution
// time and wasting storage on broken deployments.
//
// The test sets up sqlmock expectations for everything Deploy does up
// to the validateWasm check (quota lookup, deployment count) and then
// asserts that the deployment INSERT is NOT issued: the guard fires
// first, returning a clear error.
func TestDeploy_RejectsNonWasmBytes(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	tmpDir := t.TempDir()

	// Quota lookup happens first; mock a quota row that allows deploys.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id`)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "max_deployments", "max_apps", "max_workers", "max_memory_mb", "max_outbound_mb",
		}).AddRow("t_test", 100, 50, 10, 1024, 1024))

	// CountByApp is the second DB call. The actual query is
	// "SELECT COUNT(*) FROM deployments WHERE tenant_id = $1 AND app_name = $2".
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM deployments`)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	// The deployment INSERT and artifact save MUST NOT be called. If
	// sqlmock sees an unmatched INSERT, ExpectationsWereMet below will
	// fail with a clear message.

	svc := &DeploymentService{
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewArtifactStore(tmpDir),
	}

	bad := bytes.NewReader([]byte("this is not a wasm binary — no magic bytes"))
	_, err := svc.Deploy(context.Background(), "t_test", "myapp", bad, nil)
	if err == nil {
		t.Fatal("expected error for non-wasm bytes, got nil")
	}
	if !strings.Contains(err.Error(), "magic bytes") {
		t.Errorf("error %q should mention 'magic bytes'", err.Error())
	}

	// Allow the quota + count queries to be considered "consumed" — the
	// test cares that the deployment INSERT was NOT issued, not that
	// the quota/count queries were perfectly matched.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestDeploy_AcceptsWasmBytes sanity-checks the happy path: a payload
// that starts with the wasm magic bytes passes the guard and proceeds
// to the deployment INSERT. The test does not assert storage behavior
// beyond the DB call (artifact save writes to t.TempDir()).
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
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deployments`)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	svc := &DeploymentService{
		deploymentRepo: repository.NewDeploymentRepository(db),
		quotaRepo:      repository.NewQuotaRepository(db),
		artifactStore:  storage.NewArtifactStore(tmpDir),
	}

	good := bytes.NewReader(validWasmBytes)
	dep, err := svc.Deploy(context.Background(), "t_test", "myapp", good, nil)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if dep == nil || !strings.HasPrefix(dep.ID, "d_") {
		t.Errorf("deployment.ID = %v, want prefix 'd_'", dep)
	}
	if !time.Now().After(dep.CreatedAt.Add(-1 * time.Minute)) {
		t.Errorf("deployment.CreatedAt = %v, want recent", dep.CreatedAt)
	}
}

// TestValidateWasm_AlreadyCoveredInMigrationTest points readers at the
// existing comprehensive test for the validateWasm function itself
// (see migration_test.go). The test here asserts only the integration:
// the guard is wired into the Deploy hot path.
var _ = domain.Deployment{} // keep domain import alive if future tests remove usage
