package billing

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
)

// stubProvider is a hand-rolled BillingProvider used by the service
// tests. Keeps the test surface flat: no real SDK calls, no
// concurrency primitives, and full control over VerifyWebhook.
type stubProvider struct {
	name            domain.BillingProvider
	checkoutSess    CheckoutSession
	checkoutErr     error
	portalSess      PortalSession
	portalErr       error
	subscription    domain.BillingSubscription
	subscriptionErr error
	verifyEvent     domain.NormalizedEvent
	verifyErr       error
	verifyCalls     int
	createCalls     int
	createInputs    []CheckoutInput
}

func (s *stubProvider) Name() domain.BillingProvider { return s.name }

func (s *stubProvider) CreateCheckoutSession(_ context.Context, in CheckoutInput) (CheckoutSession, error) {
	s.createCalls++
	s.createInputs = append(s.createInputs, in)
	return s.checkoutSess, s.checkoutErr
}

func (s *stubProvider) CreatePortalSession(_ context.Context, _ string, _ string) (PortalSession, error) {
	return s.portalSess, s.portalErr
}

func (s *stubProvider) GetSubscription(_ context.Context, _ string) (domain.BillingSubscription, error) {
	return s.subscription, s.subscriptionErr
}

func (s *stubProvider) VerifyWebhook(_ http.Header, _ []byte) (domain.NormalizedEvent, error) {
	s.verifyCalls++
	return s.verifyEvent, s.verifyErr
}

// stubTenantUpdater records UpdateTenantPlan calls. Returns whatever
// planErr the test set.
type stubTenantUpdater struct {
	calls   []string // tenantID, newPlan, applyQuotaDefaults
	planErr error
}

func (s *stubTenantUpdater) UpdateTenantPlan(_ context.Context, tenantID, newPlan string, applyQuotaDefaults bool) error {
	s.calls = append(s.calls, tenantID+"|"+newPlan+"|"+boolStr(applyQuotaDefaults))
	return s.planErr
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// newMockService wires a BillingService against a sqlmock-backed DB
// and the given provider / tenant updater stubs. Returns the service,
// the mock handle, and a cleanup func.
func newMockService(t *testing.T, provider BillingProvider, tenants TenantUpdater) (*BillingService, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	repo := repository.NewBillingRepository(sqlxDB)
	svc := NewService(sqlxDB, repo, provider, tenants, "https://ok", "https://cancel")
	return svc, mock, func() { _ = mockDB.Close() }
}

// TestBillingService_StartCheckout_FirstCall confirms the bootstrap
// path: no existing row → Upsert with empty customer_id, then
// CreateCheckoutSession on the provider.
func TestBillingService_StartCheckout_FirstCall(t *testing.T) {
	provider := &stubProvider{
		name: domain.ProviderNoop,
		checkoutSess: CheckoutSession{
			ID:        "noop_abc",
			URL:       "/api/v1/billing/subscription?dev=noop",
			ExpiresAt: time.Now().Add(30 * time.Minute),
		},
	}
	svc, mock, cleanup := newMockService(t, provider, &stubTenantUpdater{})
	defer cleanup()

	// GetByTenant → not found.
	mock.ExpectQuery(regexp.QuoteMeta(`FROM billing_subscriptions WHERE tenant_id = $1`)).
		WithArgs("t_1").
		WillReturnError(sql.ErrNoRows)
	// Seed upsert.
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO billing_subscriptions`)).
		WithArgs("t_1", string(domain.ProviderNoop), "", "", "pro", string(domain.SubscriptionIncomplete), nil, false).
		WillReturnResult(sqlmock.NewResult(0, 1))

	sess, err := svc.StartCheckout(context.Background(), "t_1", "pro")
	if err != nil {
		t.Fatalf("StartCheckout: %v", err)
	}
	if sess.ID != "noop_abc" {
		t.Errorf("sess.ID = %q, want noop_abc", sess.ID)
	}
	if provider.createCalls != 1 {
		t.Errorf("CreateCheckoutSession calls = %d, want 1", provider.createCalls)
	}
	if got := provider.createInputs[0]; got.TenantID != "t_1" || got.Plan != "pro" {
		t.Errorf("provider input = %+v, want tenant=t_1 plan=pro", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingService_StartCheckout_UnknownPlan rejects an unknown
// plan name without touching the DB or the provider.
func TestBillingService_StartCheckout_UnknownPlan(t *testing.T) {
	provider := &stubProvider{name: domain.ProviderNoop}
	svc, mock, cleanup := newMockService(t, provider, &stubTenantUpdater{})
	defer cleanup()
	// no mock expectations — DB and provider must not be touched

	if _, err := svc.StartCheckout(context.Background(), "t_1", "platinum"); !errors.Is(err, domain.ErrUnknownPlan) {
		t.Fatalf("StartCheckout(platinum) = %v, want ErrUnknownPlan", err)
	}
	if provider.createCalls != 0 {
		t.Errorf("CreateCheckoutSession calls = %d, want 0", provider.createCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingService_OpenPortal_NoRow returns ErrNoSubscription when
// the tenant has no billing_subscriptions row.
func TestBillingService_OpenPortal_NoRow(t *testing.T) {
	provider := &stubProvider{name: domain.ProviderNoop}
	svc, mock, cleanup := newMockService(t, provider, &stubTenantUpdater{})
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`FROM billing_subscriptions WHERE tenant_id = $1`)).
		WithArgs("t_1").
		WillReturnError(sql.ErrNoRows)

	_, err := svc.OpenPortal(context.Background(), "t_1", "https://return")
	if !errors.Is(err, ErrNoSubscription) {
		t.Fatalf("OpenPortal = %v, want ErrNoSubscription", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingService_OpenPortal_HappyPath resolves the row and asks
// the provider to mint a portal session.
func TestBillingService_OpenPortal_HappyPath(t *testing.T) {
	provider := &stubProvider{
		name:       domain.ProviderStripe,
		portalSess: PortalSession{URL: "https://billing.stripe.com/session/abc"},
	}
	svc, mock, cleanup := newMockService(t, provider, &stubTenantUpdater{})
	defer cleanup()

	now := time.Now().UTC()
	rows := sqlmock.NewRows([]string{
		"tenant_id", "provider", "provider_customer_id", "provider_subscription_id",
		"plan", "status", "current_period_end", "cancel_at_period_end",
		"created_at", "updated_at",
	}).AddRow("t_1", "stripe", "cus_abc", "sub_xyz", "pro", "active", nil, false, now, now)

	mock.ExpectQuery(regexp.QuoteMeta(`FROM billing_subscriptions WHERE tenant_id = $1`)).
		WithArgs("t_1").
		WillReturnRows(rows)

	ps, err := svc.OpenPortal(context.Background(), "t_1", "https://return")
	if err != nil {
		t.Fatalf("OpenPortal: %v", err)
	}
	if ps.URL != "https://billing.stripe.com/session/abc" {
		t.Errorf("ps.URL = %q, want stripe portal", ps.URL)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingService_GetSubscription_NoRow returns ErrNoSubscription.
func TestBillingService_GetSubscription_NoRow(t *testing.T) {
	provider := &stubProvider{name: domain.ProviderNoop}
	svc, mock, cleanup := newMockService(t, provider, &stubTenantUpdater{})
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`FROM billing_subscriptions WHERE tenant_id = $1`)).
		WithArgs("t_1").
		WillReturnError(sql.ErrNoRows)

	_, err := svc.GetSubscription(context.Background(), "t_1")
	if !errors.Is(err, ErrNoSubscription) {
		t.Fatalf("GetSubscription = %v, want ErrNoSubscription", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingService_HandleWebhook_SignatureFailure surfaces the
// provider's error verbatim; the handler maps to 400.
func TestBillingService_HandleWebhook_SignatureFailure(t *testing.T) {
	provider := &stubProvider{
		name:      domain.ProviderStripe,
		verifyErr: errors.New("stripe: bad sig"),
	}
	svc, mock, cleanup := newMockService(t, provider, &stubTenantUpdater{})
	defer cleanup()
	// no DB expectations — sig failure must short-circuit before any tx

	err := svc.HandleWebhook(context.Background(), http.Header{}, []byte(`{"id":"evt_x"}`))
	if err == nil {
		t.Fatal("HandleWebhook = nil, want signature error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// fixedEvent is a NormalizedEvent used by the dispatch tests. The
// provider/tenant/plan/status fields are set by each test.
type fixedEvent struct {
	id       string
	provider domain.BillingProvider
	typ      domain.NormalizedEventType
	tenant   string
	plan     string
	status   string
}

func (e *fixedEvent) EventID() string                       { return e.id }
func (e *fixedEvent) Provider() domain.BillingProvider      { return e.provider }
func (e *fixedEvent) EventType() domain.NormalizedEventType { return e.typ }
func (e *fixedEvent) TenantID() string                      { return e.tenant }
func (e *fixedEvent) Plan() string                          { return e.plan }
func (e *fixedEvent) Status() string                        { return e.status }

// expectBillingTx is a small helper for the HandleWebhook tests that
// walk the full happy path. It registers a Begin + the
// TryRecordEvent / MarkProcessed / dispatch SELECT + upsert + COMMIT
// in the order the service issues them. The plan push
// (UpdateTenantPlan) is stubbed and doesn't touch the DB, so no
// ExpectExec for it is needed.
//
// eventType is the NormalizedEventType the test's event carries —
// it determines what gets stamped on the billing_events row.
func expectBillingTx(
	t *testing.T,
	mock sqlmock.Sqlmock,
	tenantID, plan, status string,
	eventType domain.NormalizedEventType,
) {
	t.Helper()
	now := time.Now().UTC()
	mock.ExpectBegin()
	// TryRecordEvent INSERT.
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO billing_events`)).
		WithArgs(
			"evt_1",
			string(domain.ProviderStripe),
			string(eventType),
			&tenantID,
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// dispatch: GetByTenant.
	rows := sqlmock.NewRows([]string{
		"tenant_id", "provider", "provider_customer_id", "provider_subscription_id",
		"plan", "status", "current_period_end", "cancel_at_period_end",
		"created_at", "updated_at",
	}).AddRow(tenantID, "stripe", "cus_abc", "sub_xyz", plan, status, nil, false, now, now)
	mock.ExpectQuery(regexp.QuoteMeta(`FROM billing_subscriptions WHERE tenant_id = $1`)).
		WithArgs(tenantID).
		WillReturnRows(rows)
	// dispatch: Upsert.
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO billing_subscriptions`)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// MarkProcessed.
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE billing_events SET processed_at`)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

// TestBillingService_HandleWebhook_Updated_Active exercises the
// subscription.updated → tenants.plan push path. The event carries
// plan=business + status=active; the stub tenant updater must record
// the call.
func TestBillingService_HandleWebhook_Updated_Active(t *testing.T) {
	tenants := &stubTenantUpdater{}
	provider := &stubProvider{
		name: domain.ProviderStripe,
		verifyEvent: &fixedEvent{
			id:       "evt_1",
			provider: domain.ProviderStripe,
			typ:      domain.EventSubscriptionUpdated,
			tenant:   "t_1",
			plan:     "business",
			status:   "active",
		},
	}
	svc, mock, cleanup := newMockService(t, provider, tenants)
	defer cleanup()

	expectBillingTx(t, mock, "t_1", "pro", "active", domain.EventSubscriptionUpdated)

	if err := svc.HandleWebhook(context.Background(), http.Header{}, []byte(`{"id":"evt_1"}`)); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(tenants.calls) != 1 {
		t.Fatalf("UpdateTenantPlan calls = %d, want 1", len(tenants.calls))
	}
	got := tenants.calls[0]
	if got != "t_1|business|false" {
		t.Errorf("UpdateTenantPlan call = %q, want t_1|business|false", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingService_HandleWebhook_Deleted_DowngradesToFree verifies
// that subscription.deleted pushes "free" with
// applyQuotaDefaults=true so the tenant's quota row shrinks to the
// free tier immediately.
func TestBillingService_HandleWebhook_Deleted_DowngradesToFree(t *testing.T) {
	tenants := &stubTenantUpdater{}
	provider := &stubProvider{
		name: domain.ProviderStripe,
		verifyEvent: &fixedEvent{
			id:       "evt_1",
			provider: domain.ProviderStripe,
			typ:      domain.EventSubscriptionDeleted,
			tenant:   "t_1",
			status:   "canceled",
		},
	}
	svc, mock, cleanup := newMockService(t, provider, tenants)
	defer cleanup()

	// The dispatch Upsert will write the canceled status onto the
	// existing row. Plan in the existing row is "pro" but the event
	// carries no plan → the service leaves it alone.
	expectBillingTx(t, mock, "t_1", "pro", "active", domain.EventSubscriptionDeleted)

	if err := svc.HandleWebhook(context.Background(), http.Header{}, []byte(`{"id":"evt_1"}`)); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(tenants.calls) != 1 {
		t.Fatalf("UpdateTenantPlan calls = %d, want 1", len(tenants.calls))
	}
	got := tenants.calls[0]
	if got != "t_1|free|true" {
		t.Errorf("UpdateTenantPlan call = %q, want t_1|free|true", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingService_HandleWebhook_PaymentFailed_NoTenantPush
// confirms payment.failed does NOT push a plan change — only the
// status flips to past_due.
func TestBillingService_HandleWebhook_PaymentFailed_NoTenantPush(t *testing.T) {
	tenants := &stubTenantUpdater{}
	provider := &stubProvider{
		name: domain.ProviderStripe,
		verifyEvent: &fixedEvent{
			id:       "evt_1",
			provider: domain.ProviderStripe,
			typ:      domain.EventPaymentFailed,
			tenant:   "t_1",
			status:   "past_due",
		},
	}
	svc, mock, cleanup := newMockService(t, provider, tenants)
	defer cleanup()

	expectBillingTx(t, mock, "t_1", "pro", "active", domain.EventPaymentFailed)

	if err := svc.HandleWebhook(context.Background(), http.Header{}, []byte(`{"id":"evt_1"}`)); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(tenants.calls) != 0 {
		t.Errorf("UpdateTenantPlan calls = %d, want 0 (no plan change on payment.failed)", len(tenants.calls))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// TestBillingService_HandleWebhook_Replay covers the idempotency
// path: TryRecordEvent returns 0 rows affected, the service
// short-circuits and returns nil (handler maps to 200).
func TestBillingService_HandleWebhook_Replay(t *testing.T) {
	tenants := &stubTenantUpdater{}
	provider := &stubProvider{
		name: domain.ProviderStripe,
		verifyEvent: &fixedEvent{
			id:       "evt_1",
			provider: domain.ProviderStripe,
			typ:      domain.EventSubscriptionUpdated,
			tenant:   "t_1",
			plan:     "business",
			status:   "active",
		},
	}
	svc, mock, cleanup := newMockService(t, provider, tenants)
	defer cleanup()

	mock.ExpectBegin()
	tenantID := "t_1"
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO billing_events`)).
		WithArgs(
			"evt_1",
			string(domain.ProviderStripe),
			string(domain.EventSubscriptionUpdated),
			&tenantID,
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows = replay
	mock.ExpectCommit()

	if err := svc.HandleWebhook(context.Background(), http.Header{}, []byte(`{"id":"evt_1"}`)); err != nil {
		t.Fatalf("HandleWebhook: %v", err)
	}
	if len(tenants.calls) != 0 {
		t.Errorf("UpdateTenantPlan calls = %d, want 0 on replay", len(tenants.calls))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

// Ensure database/sql is referenced; ErrNoRows is used as the
// "not found" sentinel in repo queries.
var _ = sql.ErrNoRows
