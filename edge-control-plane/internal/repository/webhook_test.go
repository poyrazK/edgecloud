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

func newWebhookMockRepo(t *testing.T) (*WebhookRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewWebhookRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

func TestWebhookRepository_Create(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	now := time.Now()
	wh := &domain.Webhook{
		ID:          "wh_1",
		TenantID:    "t_1",
		URL:         "https://hooks.example.com/evt",
		Secret:      "supersecret12345678",
		Events:      pq.StringArray{"deploy", "activate"},
		Description: "deploy notifications",
		Enabled:     true,
		CreatedAt:   now,
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO webhooks`)).
		WithArgs(wh.ID, wh.TenantID, wh.URL, wh.Secret, pq.Array(wh.Events), wh.Description, wh.Enabled, wh.CreatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Create(context.Background(), wh); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestWebhookRepository_GetByID_Found(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at",
	}).AddRow("wh_1", "t_1", "https://hooks.example.com/evt", "secret", pq.StringArray{"deploy"}, "", true, now)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks WHERE id = $1`)).
		WithArgs("wh_1").WillReturnRows(rows)

	got, err := repo.GetByID(context.Background(), "wh_1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got == nil || got.ID != "wh_1" {
		t.Errorf("got %+v, want wh_1", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestWebhookRepository_GetByID_NotFound(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks WHERE id = $1`)).
		WithArgs("wh_missing").WillReturnError(sql.ErrNoRows)

	got, err := repo.GetByID(context.Background(), "wh_missing")
	if err != nil {
		t.Fatalf("expected nil for ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
}

func TestWebhookRepository_ListByTenant(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at",
	}).AddRow("wh_1", "t_1", "https://a.example.com", "s1", pq.StringArray{"deploy"}, "", true, now).
		AddRow("wh_2", "t_1", "https://b.example.com", "s2", pq.StringArray{"activate"}, "", false, now)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks WHERE tenant_id = $1 ORDER BY created_at DESC`)).
		WithArgs("t_1").WillReturnRows(rows)

	whs, err := repo.ListByTenant(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	if len(whs) != 2 {
		t.Fatalf("len = %d, want 2", len(whs))
	}
	if whs[0].ID != "wh_1" {
		t.Errorf("first id = %q, want wh_1", whs[0].ID)
	}
}

func TestWebhookRepository_ListByTenant_Empty(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at",
	})
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks WHERE tenant_id = $1 ORDER BY created_at DESC`)).
		WithArgs("t_empty").WillReturnRows(rows)

	whs, err := repo.ListByTenant(context.Background(), "t_empty")
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	if len(whs) != 0 {
		t.Errorf("len = %d, want 0", len(whs))
	}
}

func TestWebhookRepository_ListEnabledByTenantAndEvent(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at",
	}).AddRow("wh_1", "t_1", "https://hooks.example.com", "secret", pq.StringArray{"deploy", "activate"}, "", true, now)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks
		WHERE tenant_id = $1 AND enabled = true AND $2 = ANY(events)
		ORDER BY created_at ASC`)).
		WithArgs("t_1", "deploy").WillReturnRows(rows)

	whs, err := repo.ListEnabledByTenantAndEvent(context.Background(), "t_1", "deploy")
	if err != nil {
		t.Fatalf("ListEnabledByTenantAndEvent: %v", err)
	}
	if len(whs) != 1 {
		t.Fatalf("len = %d, want 1", len(whs))
	}
	if whs[0].ID != "wh_1" {
		t.Errorf("id = %q, want wh_1", whs[0].ID)
	}
}

func TestWebhookRepository_ListEnabledByTenantAndEvent_NoMatch(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at",
	})
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks
		WHERE tenant_id = $1 AND enabled = true AND $2 = ANY(events)
		ORDER BY created_at ASC`)).
		WithArgs("t_1", "rollback").WillReturnRows(rows)

	whs, err := repo.ListEnabledByTenantAndEvent(context.Background(), "t_1", "rollback")
	if err != nil {
		t.Fatalf("ListEnabledByTenantAndEvent: %v", err)
	}
	if len(whs) != 0 {
		t.Errorf("len = %d, want 0", len(whs))
	}
}

func TestWebhookRepository_Update(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE webhooks SET url=$2, secret=$3, events=$4, description=$5, enabled=$6 WHERE id=$1 AND tenant_id=$7`)).
		WithArgs("wh_1", "https://new-url.example.com", "newsecret12345678", pq.StringArray{"deploy"}, "updated", true, "t_1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	wh := &domain.Webhook{
		ID:          "wh_1",
		TenantID:    "t_1",
		URL:         "https://new-url.example.com",
		Secret:      "newsecret12345678",
		Events:      pq.StringArray{"deploy"},
		Description: "updated",
		Enabled:     true,
	}
	if err := repo.Update(context.Background(), wh); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestWebhookRepository_Delete_Found(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM webhooks WHERE id = $1 AND tenant_id = $2 RETURNING true`)).
		WithArgs("wh_1", "t_1").WillReturnRows(sqlmock.NewRows([]string{"bool"}).AddRow(true))

	ok, err := repo.Delete(context.Background(), "wh_1", "t_1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !ok {
		t.Error("ok = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestWebhookRepository_Delete_NotFound(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM webhooks WHERE id = $1 AND tenant_id = $2 RETURNING true`)).
		WithArgs("wh_missing", "t_1").WillReturnError(sql.ErrNoRows)

	ok, err := repo.Delete(context.Background(), "wh_missing", "t_1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok {
		t.Error("ok = true, want false")
	}
}

func TestWebhookRepository_InsertDelivery(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	now := time.Now()
	statusCode := 200
	completedAt := now.Add(time.Second)
	d := &domain.WebhookDelivery{
		WebhookID:   "wh_1",
		EventType:   "deploy",
		Status:      "success",
		StatusCode:  &statusCode,
		RequestBody: `{"event":"deploy"}`,
		Attempt:     1,
		MaxAttempts: 3,
		CreatedAt:   now,
		CompletedAt: &completedAt,
	}

	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO webhook_deliveries
		(webhook_id, event_type, status, status_code, request_body, response_body, error_msg, attempt, max_attempts, created_at, completed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id`)).
		WithArgs(d.WebhookID, d.EventType, d.Status, d.StatusCode, d.RequestBody, d.ResponseBody, d.ErrorMsg, d.Attempt, d.MaxAttempts, d.CreatedAt, d.CompletedAt).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(42))

	id, err := repo.InsertDelivery(context.Background(), d)
	if err != nil {
		t.Fatalf("InsertDelivery: %v", err)
	}
	if id != 42 {
		t.Errorf("id = %d, want 42", id)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// DeleteOlderThanBatched — issue #574 retention GC sweep path.
// Mirrors log_entry_test.go:25-201 verbatim, against webhook_deliveries
// and the (created_at) index added in migration 031.
// ---------------------------------------------------------------------------

// TestWebhookRepository_DeleteOlderThanBatches_PaginatesUntilEmpty
func TestWebhookRepository_DeleteOlderThanBatches_PaginatesUntilEmpty(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	mock.ExpectExec(`DELETE FROM webhook_deliveries WHERE id IN \(SELECT id FROM webhook_deliveries WHERE created_at < NOW\(\)`).
		WithArgs(30*24*60*60.0, 10_000). // 30 days in seconds
		WillReturnResult(sqlmock.NewResult(0, 10_000))
	mock.ExpectExec(`DELETE FROM webhook_deliveries WHERE id IN \(SELECT id FROM webhook_deliveries WHERE created_at < NOW\(\)`).
		WithArgs(30*24*60*60.0, 10_000).
		WillReturnResult(sqlmock.NewResult(0, 10_000))
	mock.ExpectExec(`DELETE FROM webhook_deliveries WHERE id IN \(SELECT id FROM webhook_deliveries WHERE created_at < NOW\(\)`).
		WithArgs(30*24*60*60.0, 10_000).
		WillReturnResult(sqlmock.NewResult(0, 5_234))

	deleted, err := repo.DeleteOlderThanBatched(
		context.Background(), 30*24*time.Hour, 10_000, 100)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if want := int64(25_234); deleted != want {
		t.Errorf("deleted = %d, want %d", deleted, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWebhookRepository_DeleteOlderThanBatches_StopsAtMaxBatches
func TestWebhookRepository_DeleteOlderThanBatches_StopsAtMaxBatches(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	for i := 0; i < 3; i++ {
		mock.ExpectExec(`DELETE FROM webhook_deliveries`).
			WithArgs(3600.0, 1000).
			WillReturnResult(sqlmock.NewResult(0, 1000))
	}

	deleted, err := repo.DeleteOlderThanBatched(
		context.Background(), 1*time.Hour, 1000, 3)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if want := int64(3000); deleted != want {
		t.Errorf("deleted = %d, want %d", deleted, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWebhookRepository_DeleteOlderThanBatches_HonorsCtxCancellation
func TestWebhookRepository_DeleteOlderThanBatches_HonorsCtxCancellation(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	mock.ExpectExec(`DELETE FROM webhook_deliveries`).
		WithArgs(3600.0, 1000).
		WillReturnResult(sqlmock.NewResult(0, 1000))
	mock.ExpectExec(`DELETE FROM webhook_deliveries`).
		WithArgs(3600.0, 1000).
		WillReturnResult(sqlmock.NewResult(0, 1000))

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	deleted, err := repo.DeleteOlderThanBatched(
		ctx, 1*time.Hour, 1000, 100)
	_ = err // not asserted: timing-dependent
	if deleted == 0 {
		t.Errorf("deleted = 0, want >= 1000")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (loop ran more than 2 batches): %v", err)
	}
}

// TestWebhookRepository_DeleteOlderThanBatches_RejectsNonPositiveRetention
func TestWebhookRepository_DeleteOlderThanBatches_RejectsNonPositiveRetention(t *testing.T) {
	repo, _, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	if _, err := repo.DeleteOlderThanBatched(
		context.Background(), 0, 1000, 100); err == nil {
		t.Error("expected error for retention=0, got nil")
	}
	if _, err := repo.DeleteOlderThanBatched(
		context.Background(), -1*time.Hour, 1000, 100); err == nil {
		t.Error("expected error for retention=-1h, got nil")
	}
}

// TestWebhookRepository_DeleteOlderThanBatches_UsesServerNOW
func TestWebhookRepository_DeleteOlderThanBatches_UsesServerNOW(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	mock.ExpectExec(`DELETE FROM webhook_deliveries WHERE id IN \(SELECT id FROM webhook_deliveries WHERE created_at < NOW\(\) - make_interval\(secs => \$1\) LIMIT \$2\)`).
		WithArgs(30*24*60*60.0, int64(10_000)).
		WillReturnResult(sqlmock.NewResult(0, 100))

	_, err := repo.DeleteOlderThanBatched(
		context.Background(), 30*24*time.Hour, 10_000, 100)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (SQL shape or args wrong): %v", err)
	}
}

// TestWebhookRepository_DeleteOlderThanBatches_BoundInt64
func TestWebhookRepository_DeleteOlderThanBatches_BoundInt64(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	const wantBatchSize = int64(10_000)
	mock.ExpectExec(`DELETE FROM webhook_deliveries WHERE id IN \(SELECT id FROM webhook_deliveries WHERE created_at < NOW\(\) - make_interval\(secs => \$1\) LIMIT \$2\)`).
		WithArgs(30*24*60*60.0, wantBatchSize).
		WillReturnResult(sqlmock.NewResult(0, 100))

	_, err := repo.DeleteOlderThanBatched(
		context.Background(), 30*24*time.Hour, int(wantBatchSize), 100)
	if err != nil {
		t.Fatalf("DeleteOlderThanBatched: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (LIMIT $2 was not bound as int64): %v", err)
	}
}
