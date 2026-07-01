package repository

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

// TestListByApps_HappyPath pins the new bulk-env query (PR #166
// follow-up #1): one round trip with `app_name = ANY($2)` returns
// env vars for every requested app. The previous implementation
// called List once per app (N+1); this test pins the single-call
// shape so a regression that reintroduces the per-app loop would
// fail the regex assertion.
func TestListByApps_HappyPath(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = mockDB.Close() }()
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	repo := NewAppEnvRepository(sqlxDB)

	rows := sqlmock.NewRows([]string{"tenant_id", "app_name", "env_key", "env_value"}).
		AddRow("t_a", "app1", "K1", "v1").
		AddRow("t_a", "app2", "K2", "v2")

	mock.ExpectQuery(`SELECT.*app_env.*app_name = ANY`).
		WithArgs("t_a", sqlmock.AnyArg()).
		WillReturnRows(rows)

	got, err := repo.ListByApps(context.Background(), "t_a", []string{"app1", "app2"})
	if err != nil {
		t.Fatalf("ListByApps: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len=%d, want 2", len(got))
	}
	if got[0].EnvKey != "K1" || got[1].EnvKey != "K2" {
		t.Errorf("got=%+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}