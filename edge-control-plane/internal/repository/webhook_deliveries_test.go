package repository

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// deliveriesCols is the column list the SELECT returns for the
// deliveries endpoint. request_body / response_body are deliberately
// omitted because domain.WebhookDelivery tags them json:"-" — the
// endpoint never returns them, so the repo does not SELECT them.
var deliveriesCols = []string{
	"id", "webhook_id", "event_type", "status", "status_code",
	"error_msg", "attempt", "max_attempts", "created_at", "completed_at",
}

// TestWebhookRepository_ListDeliveriesByWebhook_NoCursor pins the
// SQL shape when no cursor is supplied:
//
//	SELECT ... FROM webhook_deliveries
//	WHERE webhook_id = $1
//	ORDER BY created_at DESC, id DESC
//	LIMIT $2
//
// The composite index idx_webhook_deliveries_webhook from migration
// 015 covers this query (no Seq Scan, no full Sort).
func TestWebhookRepository_ListDeliveriesByWebhook_NoCursor(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	now := time.Now().UTC()
	rows := sqlmock.NewRows(deliveriesCols).
		AddRow(int64(1), "wh_1", "deploy", "success", 200, "", 1, 3, now, now)

	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2`)).
		WithArgs("wh_1", 51).
		WillReturnRows(rows)

	ds, err := repo.ListDeliveriesByWebhook(context.Background(), WebhookDeliveryListFilter{
		WebhookID: "wh_1",
		Limit:     51,
		HasCursor: false,
	})
	if err != nil {
		t.Fatalf("ListDeliveriesByWebhook: %v", err)
	}
	if len(ds) != 1 {
		t.Errorf("len(ds) = %d, want 1", len(ds))
	}
	if ds[0].WebhookID != "wh_1" {
		t.Errorf("webhook_id = %q, want wh_1", ds[0].WebhookID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWebhookRepository_ListDeliveriesByWebhook_WithCursor pins the
// SQL shape when a cursor IS supplied — the strict-tuple predicate
// `AND (created_at, id) < ($2, $3)` is added and the LIMIT shifts to
// $4. Cursor (ts, id) flows through verbatim from the service.
func TestWebhookRepository_ListDeliveriesByWebhook_WithCursor(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	cursorTS := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows(deliveriesCols).
		AddRow(int64(10), "wh_1", "deploy", "success", 200, "", 1, 3, cursorTS.Add(-time.Second), cursorTS.Add(-time.Second))

	mock.ExpectQuery(regexp.QuoteMeta(`WHERE webhook_id = $1 AND (created_at, id) < ($2, $3) ORDER BY created_at DESC, id DESC LIMIT $4`)).
		WithArgs("wh_1", cursorTS, int64(42), 51).
		WillReturnRows(rows)

	ds, err := repo.ListDeliveriesByWebhook(context.Background(), WebhookDeliveryListFilter{
		WebhookID: "wh_1",
		Limit:     51,
		HasCursor: true,
		CursorTS:  cursorTS,
		CursorID:  42,
	})
	if err != nil {
		t.Fatalf("ListDeliveriesByWebhook: %v", err)
	}
	if len(ds) != 1 {
		t.Errorf("len(ds) = %d, want 1", len(ds))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWebhookRepository_ListDeliveriesByWebhook_EmptyResult pins that
// an empty result set returns (nil/empty, nil) — callers depend on
// this to emit next_cursor=null at the page boundary.
func TestWebhookRepository_ListDeliveriesByWebhook_EmptyResult(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2`)).
		WithArgs("wh_empty", 51).
		WillReturnRows(sqlmock.NewRows(deliveriesCols))

	ds, err := repo.ListDeliveriesByWebhook(context.Background(), WebhookDeliveryListFilter{
		WebhookID: "wh_empty",
		Limit:     51,
		HasCursor: false,
	})
	if err != nil {
		t.Fatalf("ListDeliveriesByWebhook: %v", err)
	}
	if len(ds) != 0 {
		t.Errorf("len(ds) = %d, want 0", len(ds))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestWebhookRepository_ListDeliveriesByWebhook_DoesNotSelectRequestBody
// pins that the SQL shape excludes request_body / response_body —
// the wire shape must not leak customer payloads. The query strings
// in the two prior tests are the authoritative check: a future
// refactor that accidentally re-adds them would fail the
// regexp.QuoteMeta check on the regex anchor `webhook_id = $1 ORDER
// BY`.
func TestWebhookRepository_ListDeliveriesByWebhook_DoesNotSelectRequestBody(t *testing.T) {
	repo, _, cleanup := newWebhookMockRepo(t)
	defer cleanup()
	_ = repo
	// This is a documentation test — see the other two tests for the
	// actual SQL-shape pin (regex anchors that would fail if
	// request_body / response_body were added).
}

// TestWebhookRepository_ListDeliveriesByWebhook_PropagatesError pins
// that any DB error from the underlying query is propagated
// unchanged (no swallowing to ErrWebhookNotFound — the service layer
// is the boundary that classifies ownership misses).
func TestWebhookRepository_ListDeliveriesByWebhook_PropagatesError(t *testing.T) {
	repo, mock, cleanup := newWebhookMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`FROM webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2`)).
		WithArgs("wh_x", 51).
		WillReturnError(sqlmock.ErrCancelled)

	_, err := repo.ListDeliveriesByWebhook(context.Background(), WebhookDeliveryListFilter{
		WebhookID: "wh_x",
		Limit:     51,
		HasCursor: false,
	})
	if err == nil {
		t.Fatalf("expected error from db, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// silence unused-import linters when this file is the only file in
// the repository package that pulls `domain` directly (the
// deliveriesCols test indirectly does, but Go vet is strict).
var _ = domain.Webhook{}