package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func newWebhookMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return sqlxDB, mock, func() { _ = mockDB.Close() }
}

func TestWebhookService_Create(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	wh := &domain.Webhook{
		ID:          "wh_1",
		TenantID:    "t_1",
		URL:         "https://hooks.example.com",
		Secret:      "supersecret12345678",
		Events:      pq.StringArray{"deploy"},
		Description: "test",
		Enabled:     true,
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO webhooks`)).
		WithArgs(wh.ID, wh.TenantID, wh.URL, wh.Secret, pq.Array(wh.Events), wh.Description, wh.Enabled, wh.CreatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := svc.Create(context.Background(), wh); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestWebhookService_ListByTenant(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"}).
		AddRow("wh_1", "t_1", "https://a.example.com", "s1", pq.StringArray{"deploy"}, "", true, time.Now())

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks WHERE tenant_id = $1 ORDER BY created_at DESC`)).
		WithArgs("t_1").WillReturnRows(rows)

	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	whs, err := svc.ListByTenant(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("ListByTenant: %v", err)
	}
	if len(whs) != 1 {
		t.Fatalf("len = %d, want 1", len(whs))
	}
}

func TestWebhookService_GetByID(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"}).
		AddRow("wh_1", "t_1", "https://a.example.com", "s1", pq.StringArray{"deploy"}, "", true, time.Now())

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks WHERE id = $1`)).
		WithArgs("wh_1").WillReturnRows(rows)

	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	wh, err := svc.GetByID(context.Background(), "wh_1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if wh == nil || wh.ID != "wh_1" {
		t.Errorf("got %+v, want wh_1", wh)
	}
}

func TestWebhookService_Update(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE webhooks`)).
		WithArgs("wh_1", "https://new.example.com", "newsecret12345678", pq.StringArray{"deploy"}, "updated", true, "t_1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	wh := &domain.Webhook{
		ID:          "wh_1",
		TenantID:    "t_1",
		URL:         "https://new.example.com",
		Secret:      "newsecret12345678",
		Events:      pq.StringArray{"deploy"},
		Description: "updated",
		Enabled:     true,
	}
	if err := svc.Update(context.Background(), wh); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

func TestWebhookService_Delete(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`DELETE FROM webhooks WHERE id = $1 AND tenant_id = $2 RETURNING true`)).
		WithArgs("wh_1", "t_1").WillReturnRows(sqlmock.NewRows([]string{"bool"}).AddRow(true))

	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	ok, err := svc.Delete(context.Background(), "wh_1", "t_1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !ok {
		t.Error("ok = false, want true")
	}
}

// ── PublishEvent / deliver tests ──────────────────────────────────────

func testService(repo *repository.WebhookRepository, client *http.Client, retryMax int) *WebhookService {
	return &WebhookService{
		repo:     repo,
		client:   client,
		retryMax: retryMax,
		interval: time.Millisecond,
	}
}

func TestWebhookService_PublishEvent_HappyPath(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	var received int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&received, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rows := sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"}).
		AddRow("wh_1", "t_1", srv.URL, "supersecret12345678", pq.StringArray{"deploy", "activate"}, "", true, time.Now())
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks
		WHERE tenant_id = $1 AND enabled = true AND $2 = ANY(events)
		ORDER BY created_at ASC`)).
		WithArgs("t_1", "deploy").WillReturnRows(rows)

	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO webhook_deliveries`)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	repo := repository.NewWebhookRepository(db)
	svc := testService(repo, srv.Client(), 1)
	svc.PublishEvent(context.Background(), "t_1", "myapp", "deploy", map[string]string{"key": "val"})

	time.Sleep(100 * time.Millisecond)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
	if n := atomic.LoadInt32(&received); n != 1 {
		t.Errorf("received requests = %d, want 1", n)
	}
}

func TestWebhookService_Deliver_Success(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO webhook_deliveries`)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	repo := repository.NewWebhookRepository(db)
	svc := testService(repo, srv.Client(), 1)

	body, _ := json.Marshal(domain.WebhookEvent{EventType: "deploy", TenantID: "t_1"})
	svc.deliver(context.Background(), domain.Webhook{
		ID: "wh_1", URL: srv.URL, Secret: "supersecret12345678",
	}, body, "deploy")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestWebhookService_Deliver_RetryThenSuccess(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	var attemptCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attemptCount, 1)
		if n == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO webhook_deliveries`)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO webhook_deliveries`)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(2))

	repo := repository.NewWebhookRepository(db)
	svc := testService(repo, srv.Client(), 2)

	body, _ := json.Marshal(domain.WebhookEvent{EventType: "deploy"})
	svc.deliver(context.Background(), domain.Webhook{
		ID: "wh_2", URL: srv.URL, Secret: "supersecret12345678",
	}, body, "deploy")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
	if n := atomic.LoadInt32(&attemptCount); n != 2 {
		t.Errorf("attempts = %d, want 2", n)
	}
}

func TestWebhookService_Deliver_AllRetriesExhausted(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	var attemptCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attemptCount, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	for i := 0; i < 3; i++ {
		mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO webhook_deliveries`)).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(i + 1)))
	}

	repo := repository.NewWebhookRepository(db)
	svc := testService(repo, srv.Client(), 3)

	body, _ := json.Marshal(domain.WebhookEvent{EventType: "deploy"})
	svc.deliver(context.Background(), domain.Webhook{
		ID: "wh_3", URL: srv.URL, Secret: "supersecret12345678",
	}, body, "deploy")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
	if n := atomic.LoadInt32(&attemptCount); n != 3 {
		t.Errorf("attempts = %d, want 3", n)
	}
}

func TestWebhookService_PublishEvent_NoMatchingWebhooks(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"})
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks
		WHERE tenant_id = $1 AND enabled = true AND $2 = ANY(events)
		ORDER BY created_at ASC`)).
		WithArgs("t_1", "deploy").WillReturnRows(rows)

	repo := repository.NewWebhookRepository(db)
	svc := testService(repo, http.DefaultClient, 1)
	svc.PublishEvent(context.Background(), "t_1", "myapp", "deploy", nil)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestWebhookService_PublishEvent_NilService(t *testing.T) {
	var svc *WebhookService
	svc.PublishEvent(context.Background(), "t_1", "myapp", "deploy", nil) // must not panic
}

func TestWebhookService_PublishEvent_RepoError(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks
		WHERE tenant_id = $1 AND enabled = true AND $2 = ANY(events)
		ORDER BY created_at ASC`)).
		WithArgs("t_1", "deploy").WillReturnError(sqlmock.ErrCancelled)

	repo := repository.NewWebhookRepository(db)
	svc := testService(repo, http.DefaultClient, 1)
	svc.PublishEvent(context.Background(), "t_1", "myapp", "deploy", nil) // must not panic

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}
