package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// newAutoscaleMockRepo wires a sqlmock-backed sqlx.DB into an
// AutoscaleRepository. Mirrors newWorkerMockRepo so the patterns line
// up across repository tests.
func newAutoscaleMockRepo(t *testing.T) (*AutoscaleRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return &AutoscaleRepository{db: sqlxDB}, mock, func() { _ = mockDB.Close() }
}

// TestAutoscaleRepository_Insert_HappyPath pins the wire shape of a
// scale_up decision: the Insert binds the eight non-id columns and
// returns the new id via RETURNING. A future schema drift (extra
// column, rename) surfaces here as a sqlmock expectation mismatch.
func TestAutoscaleRepository_Insert_HappyPath(t *testing.T) {
	repo, mock, cleanup := newAutoscaleMockRepo(t)
	defer cleanup()

	const wantID int64 = 42
	mock.ExpectQuery(`INSERT INTO autoscale_events`).
		WithArgs(
			"fra",                   // region
			"scale_up",              // action
			1,                       // from_count
			2,                       // to_count
			"free_slots=0 needed=5", // reason
			"noop",                  // provider_kind
			true,                    // succeeded
			nil,                     // error_message
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(wantID))

	e := &domain.AutoscaleEvent{
		Region:       "fra",
		Action:       domain.AutoscaleUp,
		FromCount:    1,
		ToCount:      2,
		Reason:       "free_slots=0 needed=5",
		ProviderKind: "noop",
		Succeeded:    true,
	}
	id, err := repo.Insert(context.Background(), e)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if id != wantID {
		t.Errorf("id = %d, want %d", id, wantID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestAutoscaleRepository_Insert_Failure pins the error path: a failed
// cloud-provider call is recorded with succeeded=false and a non-nil
// error_message. The Insert must surface the error verbatim so the
// autoscaler can log/alert on a high failure rate.
func TestAutoscaleRepository_Insert_Failure(t *testing.T) {
	repo, mock, cleanup := newAutoscaleMockRepo(t)
	defer cleanup()

	errMsg := "hetzner: rate limited"
	mock.ExpectQuery(`INSERT INTO autoscale_events`).
		WithArgs(
			"fra",
			"scale_up",
			1, 2,
			"free_slots=0 needed=5",
			"hetzner",
			false,
			errMsg, // non-nil error message
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(43))

	e := &domain.AutoscaleEvent{
		Region:       "fra",
		Action:       domain.AutoscaleUp,
		FromCount:    1,
		ToCount:      2,
		Reason:       "free_slots=0 needed=5",
		ProviderKind: "hetzner",
		Succeeded:    false,
		ErrorMessage: &errMsg,
	}
	if _, err := repo.Insert(context.Background(), e); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestAutoscaleRepository_ListRecent_ByRegion pins the WHERE clause
// and ORDER BY for the cluster admin endpoint's "events" view. The
// descending (region, created_at) index makes this query O(log n + limit).
func TestAutoscaleRepository_ListRecent_ByRegion(t *testing.T) {
	repo, mock, cleanup := newAutoscaleMockRepo(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "created_at", "region", "action", "from_count", "to_count",
		"reason", "provider_kind", "succeeded", "error_message",
	}).AddRow(
		2, now.Add(-time.Minute), "fra", "scale_up", 1, 2,
		"free_slots=0 needed=5", "noop", true, nil,
	).AddRow(
		1, now.Add(-time.Hour), "fra", "noop", 1, 1,
		"within target", "noop", true, nil,
	)

	mock.ExpectQuery(`SELECT.*FROM autoscale_events.*WHERE region = \$1.*ORDER BY created_at DESC.*LIMIT \$2`).
		WithArgs("fra", 50).
		WillReturnRows(rows)

	got, err := repo.ListRecent(context.Background(), "fra", 50)
	if err != nil {
		t.Fatalf("ListRecent: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Action != domain.AutoscaleUp {
		t.Errorf("[0].Action = %q, want scale_up", got[0].Action)
	}
	if got[1].Action != domain.AutoscaleNoop {
		t.Errorf("[1].Action = %q, want noop", got[1].Action)
	}
	if got[0].ErrorMessage != nil {
		t.Errorf("[0].ErrorMessage = %v, want nil", *got[0].ErrorMessage)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestAutoscaleRepository_ListRecent_AllRegions covers the empty-string
// branch: no WHERE filter, returns across-region events newest-first.
func TestAutoscaleRepository_ListRecent_AllRegions(t *testing.T) {
	repo, mock, cleanup := newAutoscaleMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "created_at", "region", "action", "from_count", "to_count",
		"reason", "provider_kind", "succeeded", "error_message",
	})
	mock.ExpectQuery(`SELECT.*FROM autoscale_events.*ORDER BY created_at DESC.*LIMIT \$1`).
		WithArgs(10).
		WillReturnRows(rows)

	got, err := repo.ListRecent(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("ListRecent: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestAutoscaleRepository_ListRecent_LimitZero pins the limit guard:
// passing 0 returns nil without hitting the DB. Prevents a runaway
// admin request (e.g. `?limit=`) from materializing the full table.
func TestAutoscaleRepository_ListRecent_LimitZero(t *testing.T) {
	repo, _, cleanup := newAutoscaleMockRepo(t)
	defer cleanup()

	got, err := repo.ListRecent(context.Background(), "fra", 0)
	if err != nil {
		t.Fatalf("ListRecent: %v", err)
	}
	if got != nil {
		t.Errorf("got = %v, want nil for limit=0", got)
	}
}

// TestAutoscaleRepository_CountByRegion pins the COUNT(*) helper used
// by autoscaler integration tests.
func TestAutoscaleRepository_CountByRegion(t *testing.T) {
	repo, mock, cleanup := newAutoscaleMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM autoscale_events WHERE region = \$1`).
		WithArgs("fra").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))

	got, err := repo.CountByRegion(context.Background(), "fra")
	if err != nil {
		t.Fatalf("CountByRegion: %v", err)
	}
	if got != 7 {
		t.Errorf("count = %d, want 7", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}
