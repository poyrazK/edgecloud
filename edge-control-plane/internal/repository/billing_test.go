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
)

// newBillingMockRepo builds a sqlmock-backed BillingRepository. Mirrors
// newWebhookMockRepo at webhook_test.go:16.
func newBillingMockRepo(t *testing.T) (*BillingRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return NewBillingRepository(sqlxDB), mock, func() { _ = mockDB.Close() }
}

// TestBillingRepository_GetByTenant_Found exercises the happy path of
// GetByTenant. The row's column order matches the SELECT in
// repository/billing.go:37-39.
func TestBillingRepository_GetByTenant_Found(t *testing.T) {
	repo, mock, cleanup := newBillingMockRepo(t)
	defer cleanup()

	now := time.Now().UTC()
	periodEnd := now.Add(30 * 24 * time.Hour)
	rows := sqlmock.NewRows([]string{
		"tenant_id", "provider", "provider_customer_id", "provider_subscription_id",
		"plan", "status", "current_period_end", "cancel_at_period_end",
		"created_at", "updated_at",
	}).AddRow("t_1", "stripe", "cus_abc", "sub_xyz", "pro", "active", periodEnd, false, now, now)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id, provider, provider_customer_id, provider_subscription_id,
		                  plan, status, current_period_end, cancel_at_period_end, created_at, updated_at
		             FROM billing_subscriptions WHERE tenant_id = $1`)).
		WithArgs("t_1").
		WillReturnRows(rows)

	got, err := repo.GetByTenant(context.Background(), "t_1")
	if err != nil {
		t.Fatalf("GetByTenant: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want row")
	}
	if got.TenantID != "t_1" || got.Plan != "pro" || got.Status != domain.SubscriptionActive {
		t.Errorf("got %+v, want tenant=t_1 plan=pro status=active", got)
	}
	if got.ProviderSubscriptionID != "sub_xyz" {
		t.Errorf("provider_subscription_id = %q, want sub_xyz", got.ProviderSubscriptionID)
	}
	if got.CurrentPeriodEnd == nil || !got.CurrentPeriodEnd.Equal(periodEnd) {
		t.Errorf("current_period_end = %v, want %v", got.CurrentPeriodEnd, periodEnd)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingRepository_GetByTenant_NotFound confirms the
// (nil, nil) contract when the row doesn't exist. Mirrors
// webhook_test.go:78-92.
func TestBillingRepository_GetByTenant_NotFound(t *testing.T) {
	repo, mock, cleanup := newBillingMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`FROM billing_subscriptions WHERE tenant_id = $1`)).
		WithArgs("t_missing").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.GetByTenant(context.Background(), "t_missing")
	if err != nil {
		t.Fatalf("expected nil error for ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("got %+v, want nil", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingRepository_ListByProviderCustomer_Found exercises the
// webhook-side lookup: customer.subscription.* events don't carry
// tenant, so the service resolves tenant via this index.
func TestBillingRepository_ListByProviderCustomer_Found(t *testing.T) {
	repo, mock, cleanup := newBillingMockRepo(t)
	defer cleanup()

	now := time.Now().UTC()
	rows := sqlmock.NewRows([]string{
		"tenant_id", "provider", "provider_customer_id", "provider_subscription_id",
		"plan", "status", "current_period_end", "cancel_at_period_end",
		"created_at", "updated_at",
	}).AddRow("t_42", "stripe", "cus_xyz", "sub_xyz", "business", "active", nil, false, now, now)

	mock.ExpectQuery(regexp.QuoteMeta(`FROM billing_subscriptions
		            WHERE provider = $1 AND provider_customer_id = $2`)).
		WithArgs(domain.ProviderStripe, "cus_xyz").
		WillReturnRows(rows)

	got, err := repo.ListByProviderCustomer(context.Background(), domain.ProviderStripe, "cus_xyz")
	if err != nil {
		t.Fatalf("ListByProviderCustomer: %v", err)
	}
	if got == nil {
		t.Fatal("got nil, want row")
	}
	if got.TenantID != "t_42" || got.Provider != domain.ProviderStripe {
		t.Errorf("got %+v, want tenant=t_42 provider=stripe", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingRepository_Upsert confirms the INSERT … ON CONFLICT path.
// Args order matches the literal in repository/billing.go:87-89
// (8 user-supplied args; created_at + updated_at are NOW()).
func TestBillingRepository_Upsert(t *testing.T) {
	repo, mock, cleanup := newBillingMockRepo(t)
	defer cleanup()

	periodEnd := time.Now().Add(30 * 24 * time.Hour)
	sub := &domain.BillingSubscription{
		TenantID:               "t_1",
		Provider:               domain.ProviderStripe,
		ProviderCustomerID:     "cus_abc",
		ProviderSubscriptionID: "sub_xyz",
		Plan:                   "pro",
		Status:                 domain.SubscriptionActive,
		CurrentPeriodEnd:       &periodEnd,
		CancelAtPeriodEnd:      false,
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO billing_subscriptions`)).
		WithArgs(
			"t_1",
			string(domain.ProviderStripe),
			"cus_abc",
			"sub_xyz",
			"pro",
			string(domain.SubscriptionActive),
			&periodEnd,
			false,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Upsert(context.Background(), sub); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingRepository_TryRecordEvent_Inserted exercises the
// first-delivery path: ON CONFLICT DO NOTHING returns 1 row affected.
func TestBillingRepository_TryRecordEvent_Inserted(t *testing.T) {
	repo, mock, cleanup := newBillingMockRepo(t)
	defer cleanup()

	tenantID := "t_1"
	evt := &domain.BillingEvent{
		EventID:     "evt_1",
		Provider:    domain.ProviderStripe,
		EventType:   domain.EventCheckoutCompleted,
		TenantID:    &tenantID,
		PayloadHash: "abc123",
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO billing_events`)).
		WithArgs("evt_1", string(domain.ProviderStripe), string(domain.EventCheckoutCompleted), &tenantID, "abc123").
		WillReturnResult(sqlmock.NewResult(1, 1))

	recorded, err := repo.TryRecordEvent(context.Background(), evt)
	if err != nil {
		t.Fatalf("TryRecordEvent: %v", err)
	}
	if !recorded {
		t.Errorf("recorded = false, want true (new row)")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingRepository_TryRecordEvent_AlreadyProcessed exercises
// the idempotent-replay path: ON CONFLICT DO NOTHING returns 0 rows
// affected. The service uses the (false, nil) signal to short-circuit
// the dispatch and return 200 to Stripe.
func TestBillingRepository_TryRecordEvent_AlreadyProcessed(t *testing.T) {
	repo, mock, cleanup := newBillingMockRepo(t)
	defer cleanup()

	tenantID := "t_1"
	evt := &domain.BillingEvent{
		EventID:     "evt_1",
		Provider:    domain.ProviderStripe,
		EventType:   domain.EventCheckoutCompleted,
		TenantID:    &tenantID,
		PayloadHash: "abc123",
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO billing_events`)).
		WithArgs("evt_1", string(domain.ProviderStripe), string(domain.EventCheckoutCompleted), &tenantID, "abc123").
		WillReturnResult(sqlmock.NewResult(0, 0))

	recorded, err := repo.TryRecordEvent(context.Background(), evt)
	if err != nil {
		t.Fatalf("TryRecordEvent: %v", err)
	}
	if recorded {
		t.Errorf("recorded = true, want false (replay)")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingRepository_MarkProcessed exercises the
// processed_at-stamp path. The service calls this at the end of
// HandleWebhook so operators can grep for processed_at IS NULL to
// find stuck events.
func TestBillingRepository_MarkProcessed(t *testing.T) {
	repo, mock, cleanup := newBillingMockRepo(t)
	defer cleanup()

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE billing_events SET processed_at = $2 WHERE event_id = $1`)).
		WithArgs("evt_1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.MarkProcessed(context.Background(), "evt_1"); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingRepository_WithTx confirms WithTx swaps the underlying
// DBTX to the passed-in transaction. The transactional path
// (BillingService.HandleWebhook) is the only caller that relies on
// this; if WithTx silently ignored the tx we'd lose atomicity.
func TestBillingRepository_WithTx(t *testing.T) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "postgres")

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE billing_events SET processed_at = $2 WHERE event_id = $1`)).
		WithArgs("evt_tx", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := sqlxDB.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTxx: %v", err)
	}

	txRepo := NewBillingRepository(sqlxDB).WithTx(tx)
	if err := txRepo.MarkProcessed(context.Background(), "evt_tx"); err != nil {
		t.Fatalf("MarkProcessed via tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingRepository_Upsert_EmptyCustomerID confirms the
// StartCheckout seed path: the row exists from the moment
// StartCheckout is called, BEFORE the provider has filled in the
// real provider_customer_id. The column is now nullable (migration
// 024), so the upsert succeeds with an empty string.
//
// Issue #419 review follow-up.
func TestBillingRepository_Upsert_EmptyCustomerID(t *testing.T) {
	repo, mock, cleanup := newBillingMockRepo(t)
	defer cleanup()

	sub := &domain.BillingSubscription{
		TenantID:           "t_1",
		Provider:           domain.ProviderStripe,
		ProviderCustomerID: "", // seeded before provider has resolved the id
		Plan:               "pro",
		Status:             domain.SubscriptionIncomplete,
		CancelAtPeriodEnd:  false,
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO billing_subscriptions`)).
		WithArgs(
			"t_1",
			string(domain.ProviderStripe),
			"",
			"",
			"pro",
			string(domain.SubscriptionIncomplete),
			nil,
			false,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Upsert(context.Background(), sub); err != nil {
		t.Fatalf("Upsert with empty customer_id: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}
