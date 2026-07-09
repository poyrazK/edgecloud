package repository

import (
	"context"
	"database/sql"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func newDeploymentMockRepo(t *testing.T) (*DeploymentRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewDeploymentRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

func TestDeploymentRepository_Create(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	now := time.Now()
	d := &domain.Deployment{
		ID:                  "d_1",
		TenantID:            "t_1",
		AppName:             "hello",
		Status:              domain.StatusDeployed,
		Hash:                "abc123",
		Regions:             pq.StringArray{"fra", "sfo"},
		CreatedAt:           now,
		AutoRollbackEnabled: false,
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deployments`)).
		WithArgs(d.ID, d.TenantID, d.AppName, d.Status, d.Hash, pq.Array(d.Regions), d.CreatedAt, d.AutoRollbackEnabled, d.Signature, d.SigningKeyID, d.BuildAttestation, d.DesiredReplicas, d.PreviewID, d.PreviewPRNumber, d.PreviewExpiresAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Create(context.Background(), d); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestDeploymentRepository_Create_NilRegionsUsesEmptyArray(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	now := time.Now()
	d := &domain.Deployment{
		ID:        "d_2",
		TenantID:  "t_1",
		AppName:   "hello",
		Status:    domain.StatusDeployed,
		Hash:      "def456",
		Regions:   nil, // nil slice — repo must convert to empty array
		CreatedAt: now,
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deployments`)).
		WithArgs(d.ID, d.TenantID, d.AppName, d.Status, d.Hash, pq.Array(pq.StringArray{}), d.CreatedAt, d.AutoRollbackEnabled, d.Signature, d.SigningKeyID, d.BuildAttestation, d.DesiredReplicas, d.PreviewID, d.PreviewPRNumber, d.PreviewExpiresAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Create(context.Background(), d); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestDeploymentRepository_GetByID(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled",
		"signature", "signing_key_id", "build_attestation", "desired_replicas",
	}).AddRow("d_1", "t_1", "hello", domain.StatusDeployed, "abc", pq.StringArray{"fra"}, now, true, "", "", []byte{}, 0)

	mock.ExpectQuery(`SELECT id.*FROM deployments WHERE`).
		WithArgs("d_1").
		WillReturnRows(rows)

	got, err := repo.GetByID(context.Background(), "d_1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ID != "d_1" {
		t.Errorf("ID = %q, want d_1", got.ID)
	}
	if len(got.Regions) != 1 || got.Regions[0] != "fra" {
		t.Errorf("Regions = %v", got.Regions)
	}
	if !got.AutoRollbackEnabled {
		t.Error("AutoRollbackEnabled = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestDeploymentRepository_GetByID_NotFound(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT id.*FROM deployments WHERE`).
		WithArgs("d_missing").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.GetByID(context.Background(), "d_missing")
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

func TestDeploymentRepository_ListByApp(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled",
		"signature", "signing_key_id", "build_attestation", "desired_replicas",
	}).AddRow("d_1", "t_1", "hello", "deployed", "hash1", pq.StringArray{"fra"}, time.Now(), false, "", "", []byte{}, 0).
		AddRow("d_2", "t_1", "hello", "active", "hash2", pq.StringArray{"sfo"}, time.Now(), false, "", "", []byte{}, 0)

	mock.ExpectQuery(`SELECT id.*FROM deployments WHERE.*ORDER BY created_at DESC`).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	deployments, err := repo.ListByApp(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("ListByApp: %v", err)
	}
	if len(deployments) != 2 {
		t.Errorf("len = %d, want 2", len(deployments))
	}
}

func TestDeploymentRepository_ListByAppPaginated(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "app_name", "status", "hash", "regions", "created_at", "auto_rollback_enabled",
		"signature", "signing_key_id", "build_attestation", "desired_replicas",
	}).AddRow("d_1", "t_1", "hello", "deployed", "hash1", pq.StringArray{}, time.Now(), false, "", "", []byte{}, 0)

	mock.ExpectQuery(`SELECT id.*FROM deployments WHERE.*ORDER BY created_at DESC LIMIT.*OFFSET`).
		WithArgs("t_1", "hello", 10, 5).
		WillReturnRows(rows)

	deps, err := repo.ListByAppPaginated(context.Background(), "t_1", "hello", 10, 5)
	if err != nil {
		t.Fatalf("ListByAppPaginated: %v", err)
	}
	if len(deps) != 1 {
		t.Errorf("len = %d, want 1", len(deps))
	}
}

func TestDeploymentRepository_CountByApp(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(2)
	mock.ExpectQuery(`SELECT COUNT.*FROM deployments`).
		WithArgs("t_1", "hello").
		WillReturnRows(rows)

	got, err := repo.CountByApp(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("CountByApp: %v", err)
	}
	if got != 2 {
		t.Errorf("CountByApp = %d, want 2", got)
	}
}

func TestDeploymentRepository_UpdateStatus(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE deployments SET status`)).
		WithArgs("d_1", domain.StatusActive).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.UpdateStatus(context.Background(), "d_1", domain.StatusActive); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
}

func TestDeploymentRepository_DeleteByApp(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM deployments WHERE`)).
		WithArgs("t_1", "hello").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.DeleteByApp(context.Background(), "t_1", "hello"); err != nil {
		t.Fatalf("DeleteByApp: %v", err)
	}
}

func TestDeploymentRepository_DeleteByID(t *testing.T) {
	repo, mock, cleanup := newDeploymentMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM deployments WHERE`)).
		WithArgs("d_1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.DeleteByID(context.Background(), "d_1"); err != nil {
		t.Fatalf("DeleteByID: %v", err)
	}
}
