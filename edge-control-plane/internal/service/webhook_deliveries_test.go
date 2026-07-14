package service

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// deliveryCols is the column list the repository SELECT returns for
// the deliveries endpoint (excludes request_body / response_body per
// domain.WebhookDelivery json:"-" tags). Kept here as a single source
// of truth for every test that builds sqlmock rows.
var deliveryCols = []string{
	"id", "webhook_id", "event_type", "status", "status_code",
	"error_msg", "attempt", "max_attempts", "created_at", "completed_at",
}

// TestWebhookService_ListDeliveriesByWebhook_HappyPath pins the
// success path: ownership check passes, no cursor, default limit 50,
// the repo is called with limit+1=51, the result is trimmed to 50,
// and next_cursor is null when the repo returned fewer than 51 rows.
func TestWebhookService_ListDeliveriesByWebhook_HappyPath(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()
	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	now := time.Now().UTC()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, url, secret, events, description, enabled, created_at FROM webhooks WHERE id = $1`)).
		WithArgs("wh_1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"}).
			AddRow("wh_1", "t_1", "https://a.example.com", "supersecret12345678", "{}", "", true, now))

	// 50 rows < limit+1=51 → hasMore=false → next_cursor=null.
	rows := sqlmock.NewRows(deliveryCols)
	for i := 0; i < 50; i++ {
		rows.AddRow(int64(100-i), "wh_1", "deploy", "success", 200, "", 1, 3, now, now)
	}
	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2`)).
		WithArgs("wh_1", 51).
		WillReturnRows(rows)

	res, err := svc.ListDeliveriesByWebhook(context.Background(), "t_1", "wh_1", 50, "")
	if err != nil {
		t.Fatalf("ListDeliveriesByWebhook: %v", err)
	}
	if res.Limit != 50 {
		t.Errorf("limit = %d, want 50", res.Limit)
	}
	if len(res.Deliveries) != 50 {
		t.Errorf("len(deliveries) = %d, want 50", len(res.Deliveries))
	}
	if res.NextCursor != nil {
		t.Errorf("next_cursor = %v, want nil (page exactly full)", *res.NextCursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWebhookService_ListDeliveriesByWebhook_HasMoreEmitsNextCursor pins
// the limit+1 probe-row contract: repo returns limit+1 rows, service
// trims to limit, encodes next_cursor from the LAST visible row's
// (created_at, id).
func TestWebhookService_ListDeliveriesByWebhook_HasMoreEmitsNextCursor(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()
	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	now := time.Now().UTC()
	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhooks WHERE id = $1`)).
		WithArgs("wh_1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"}).
			AddRow("wh_1", "t_1", "https://a.example.com", "supersecret12345678", "{}", "", true, now))

	// 4 rows for limit=3 (so the 4th is the probe).
	rows := sqlmock.NewRows(deliveryCols).
		AddRow(int64(100), "wh_1", "deploy", "success", 200, "", 1, 3, now, now).
		AddRow(int64(99), "wh_1", "deploy", "success", 200, "", 1, 3, now.Add(-time.Second), now.Add(-time.Second)).
		AddRow(int64(98), "wh_1", "deploy", "success", 200, "", 1, 3, now.Add(-2*time.Second), now.Add(-2*time.Second)).
		AddRow(int64(97), "wh_1", "deploy", "success", 200, "", 1, 3, now.Add(-3*time.Second), now.Add(-3*time.Second))
	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2`)).
		WithArgs("wh_1", 4).
		WillReturnRows(rows)

	res, err := svc.ListDeliveriesByWebhook(context.Background(), "t_1", "wh_1", 3, "")
	if err != nil {
		t.Fatalf("ListDeliveriesByWebhook: %v", err)
	}
	if len(res.Deliveries) != 3 {
		t.Errorf("len(deliveries) = %d, want 3", len(res.Deliveries))
	}
	if res.NextCursor == nil {
		t.Fatalf("next_cursor = nil, want non-nil")
	}
	// Decode the cursor and verify it points at the LAST visible row (id=98).
	ts, id, err := decodeWebhookDeliveryCursor(*res.NextCursor)
	if err != nil {
		t.Fatalf("decode next cursor: %v", err)
	}
	if id != 98 {
		t.Errorf("next_cursor id = %d, want 98 (last visible row)", id)
	}
	if !ts.Equal(now.Add(-2 * time.Second)) {
		t.Errorf("next_cursor ts = %s, want %s", ts, now.Add(-2*time.Second))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWebhookService_ListDeliveriesByWebhook_OwnershipMismatchReturnsNotFound
// pins the tenant-isolation contract: a webhook that exists but belongs
// to a different tenant returns ErrWebhookNotFound AND the repo's
// deliveries query is NEVER called. This is the same shape as
// TestWebhookService_Update's coverage but at the deliveries layer.
func TestWebhookService_ListDeliveriesByWebhook_OwnershipMismatchReturnsNotFound(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()
	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhooks WHERE id = $1`)).
		WithArgs("wh_other").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"}).
			AddRow("wh_other", "t_OTHER", "https://a.example.com", "supersecret12345678", "{}", "", true, time.Now()))

	_, err := svc.ListDeliveriesByWebhook(context.Background(), "t_1", "wh_other", 50, "")
	if !errors.Is(err, ErrWebhookNotFound) {
		t.Fatalf("err = %v, want ErrWebhookNotFound", err)
	}
	// Critical: the deliveries query must NOT have been issued.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (deliveries query should not have run): %v", err)
	}
}

// TestWebhookService_ListDeliveriesByWebhook_RepoGetByIDErrorPropagates
// pins the error-translation contract: any error from the underlying
// GetByID repo call (e.g. transient DB error) is wrapped with context
// and propagated, NOT collapsed to ErrWebhookNotFound. The
// ErrWebhookNotFound surface is reserved for "missing or wrong tenant"
// — a transient DB error is a 500, not a 404.
func TestWebhookService_ListDeliveriesByWebhook_RepoGetByIDErrorPropagates(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()
	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhooks WHERE id = $1`)).
		WithArgs("wh_any").
		WillReturnError(sqlmock.ErrCancelled)

	_, err := svc.ListDeliveriesByWebhook(context.Background(), "t_1", "wh_any", 50, "")
	if errors.Is(err, ErrWebhookNotFound) {
		t.Fatalf("err = ErrWebhookNotFound; should be a wrapped repo error (got %v)", err)
	}
	if err == nil {
		t.Fatalf("expected an error from the repo, got nil")
	}
}

// TestWebhookService_ListDeliveriesByWebhook_MalformedCursorReturnsTypedError
// pins the typed error from the cursor codec — mirrors the
// TestLogService_CursorModeRejectsMalformedCursor pattern from #644.
// The service calls GetByID FIRST (ownership check), then decodes the
// cursor — so we DO need the webhook lookup to succeed; only after
// ownership passes does the malformed cursor trigger the typed error.
func TestWebhookService_ListDeliveriesByWebhook_MalformedCursorReturnsTypedError(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()
	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	now := time.Now().UTC()
	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhooks WHERE id = $1`)).
		WithArgs("wh_1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"}).
			AddRow("wh_1", "t_1", "https://a.example.com", "supersecret12345678", "{}", "", true, now))

	_, err := svc.ListDeliveriesByWebhook(context.Background(), "t_1", "wh_1", 50, "not base64!")
	if !errors.Is(err, ErrInvalidWebhookDeliveryCursor) {
		t.Fatalf("err = %v, want ErrInvalidWebhookDeliveryCursor", err)
	}
	// Repo deliveries query must NOT have been issued.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations (deliveries query should not have run): %v", err)
	}
}

// TestWebhookService_ListDeliveriesByWebhook_CursorDecodesAndPropagates
// pins that a valid cursor is decoded into (ts, id) and passed to the
// repo as the strict-tuple predicate. The exact SQL filter shape is
// owned by the repo test; here we assert the typed-decoding happens
// by checking that the service succeeds end-to-end with a known
// cursor and the repo receives the expected limit+1.
func TestWebhookService_ListDeliveriesByWebhook_CursorDecodesAndPropagates(t *testing.T) {
	db, mock, cleanup := newWebhookMockDB(t)
	defer cleanup()
	repo := repository.NewWebhookRepository(db)
	svc := NewWebhookService(repo)

	now := time.Now().UTC()
	cursorTS := now.Add(-1 * time.Hour)
	cursor, err := encodeWebhookDeliveryCursor(cursorTS, 42)
	if err != nil {
		t.Fatalf("encodeWebhookDeliveryCursor: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhooks WHERE id = $1`)).
		WithArgs("wh_1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"}).
			AddRow("wh_1", "t_1", "https://a.example.com", "supersecret12345678", "{}", "", true, now))

	rows := sqlmock.NewRows(deliveryCols).
		AddRow(int64(10), "wh_1", "deploy", "success", 200, "", 1, 3, now, now)
	mock.ExpectQuery(regexp.QuoteMeta(`WHERE webhook_id = $1 AND (created_at, id) < ($2, $3)`)).
		WithArgs("wh_1", cursorTS, int64(42), 51).
		WillReturnRows(rows)

	res, err := svc.ListDeliveriesByWebhook(context.Background(), "t_1", "wh_1", 50, cursor)
	if err != nil {
		t.Fatalf("ListDeliveriesByWebhook: %v", err)
	}
	if len(res.Deliveries) != 1 {
		t.Errorf("len(deliveries) = %d, want 1", len(res.Deliveries))
	}
	if res.NextCursor != nil {
		t.Errorf("next_cursor = %v, want nil (final page)", *res.NextCursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWebhookService_ListDeliveriesByWebhook_ClampsLimit pins the
// limit-clamp policy:
//
//	<=0  → DefaultWebhookDeliveryLimit (50)
//	>max → MaxWebhookDeliveryLimit (200)
//	else → unchanged
//
// Repo is always called with effective+1.
func TestWebhookService_ListDeliveriesByWebhook_ClampsLimit(t *testing.T) {
	cases := []struct {
		name     string
		input    int
		wantRepo int
	}{
		{"zero_clamps_to_default", 0, 51},
		{"negative_clamps_to_default", -5, 51},
		{"over_max_clamps_to_max", 999, 201},
		{"exactly_max_unchanged", 200, 201},
		{"in_range_unchanged", 10, 11},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, cleanup := newWebhookMockDB(t)
			defer cleanup()
			repo := repository.NewWebhookRepository(db)
			svc := NewWebhookService(repo)

			now := time.Now().UTC()
			mock.ExpectQuery(regexp.QuoteMeta(`FROM webhooks WHERE id = $1`)).
				WithArgs("wh_1").
				WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "url", "secret", "events", "description", "enabled", "created_at"}).
					AddRow("wh_1", "t_1", "https://a.example.com", "supersecret12345678", "{}", "", true, now))

			mock.ExpectQuery(regexp.QuoteMeta(`FROM webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2`)).
				WithArgs("wh_1", tc.wantRepo).
				WillReturnRows(sqlmock.NewRows(deliveryCols))

			res, err := svc.ListDeliveriesByWebhook(context.Background(), "t_1", "wh_1", tc.input, "")
			if err != nil {
				t.Fatalf("ListDeliveriesByWebhook: %v", err)
			}
			expectedLimit := tc.wantRepo - 1
			if res.Limit != expectedLimit {
				t.Errorf("res.Limit = %d, want %d", res.Limit, expectedLimit)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet mock expectations: %v", err)
			}
		})
	}
}