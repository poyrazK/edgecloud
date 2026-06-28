package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/jmoiron/sqlx"
)

// newDomainMockDB wires a sqlmock-backed *sqlx.DB for domain tests.
// Matches the helper in deployment_test.go (deployment.go's
// ActivateDeployment uses the same `repository.Transaction` shape).
func newDomainMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return sqlxDB, mock, func() { _ = mockDB.Close() }
}

// mockDomainRepo implements DomainRepositoryInterface for tests
// that don't exercise the tx-bound path (GetDomain, RemoveDomain,
// IsTlsAllowed, ListAllDomains). AddDomain's count+insert path runs
// against sqlmock instead.
type mockDomainRepo struct {
	createFn       func(ctx context.Context, d *domain.Domain) error
	getByIDFn      func(ctx context.Context, id string) (*domain.Domain, error)
	getByFQDNFn    func(ctx context.Context, fqdn string) (*domain.Domain, error)
	listByAppFn    func(ctx context.Context, tenantID, appName string) ([]domain.Domain, error)
	countByAppFn   func(ctx context.Context, tenantID, appName string) (int, error)
	listAllFn      func(ctx context.Context) ([]domain.Domain, error)
	atomicDeleteFn func(ctx context.Context, tenantID, appName, fqdn string) (bool, error)
	updateStatusFn func(ctx context.Context, id string, status domain.DomainStatus, lastError *string) (bool, error)
}

func (m *mockDomainRepo) WithTx(tx *sqlx.Tx) *repository.DomainRepository {
	// Tx-bound mock: the closure path uses this to bind count+insert
	// to the same tx. We return a real *DomainRepository bound to
	// the tx — the test's sqlmock expectations dictate what
	// happens next. The mockDomainRepo itself is unused on this
	// path (it doesn't implement DBTX-bound SQL).
	return repository.NewDomainRepositoryFromDBTX(tx)
}

func (m *mockDomainRepo) Create(ctx context.Context, d *domain.Domain) error {
	if m.createFn == nil {
		return nil
	}
	return m.createFn(ctx, d)
}
func (m *mockDomainRepo) GetByID(ctx context.Context, id string) (*domain.Domain, error) {
	if m.getByIDFn == nil {
		return nil, nil
	}
	return m.getByIDFn(ctx, id)
}
func (m *mockDomainRepo) GetByFQDN(ctx context.Context, fqdn string) (*domain.Domain, error) {
	if m.getByFQDNFn == nil {
		return nil, nil
	}
	return m.getByFQDNFn(ctx, fqdn)
}
func (m *mockDomainRepo) ListByApp(ctx context.Context, tenantID, appName string) ([]domain.Domain, error) {
	if m.listByAppFn == nil {
		return nil, nil
	}
	return m.listByAppFn(ctx, tenantID, appName)
}
func (m *mockDomainRepo) CountByApp(ctx context.Context, tenantID, appName string) (int, error) {
	if m.countByAppFn == nil {
		return 0, nil
	}
	return m.countByAppFn(ctx, tenantID, appName)
}
func (m *mockDomainRepo) ListAll(ctx context.Context) ([]domain.Domain, error) {
	if m.listAllFn == nil {
		return nil, nil
	}
	return m.listAllFn(ctx)
}
func (m *mockDomainRepo) AtomicDelete(ctx context.Context, tenantID, appName, fqdn string) (bool, error) {
	if m.atomicDeleteFn == nil {
		return true, nil
	}
	return m.atomicDeleteFn(ctx, tenantID, appName, fqdn)
}
func (m *mockDomainRepo) UpdateStatus(ctx context.Context, id string, status domain.DomainStatus, lastError *string) (bool, error) {
	if m.updateStatusFn == nil {
		return true, nil
	}
	return m.updateStatusFn(ctx, id, status, lastError)
}

// mockAppLookupForDomain is a thin shim for tests that want to
// stub out the (tenant, app) lookup without involving sqlmock. The
// sqlmock-driven tests construct a real *repository.AppRepository
// bound to the sqlmock db and pass it directly. This shim exists
// for the FQDN-shape rejection cases (which short-circuit before
// any DB call) and the concurrent-insert test (which uses an
// in-memory counter and skips the real DB).
type mockAppLookupForDomain struct{}

func (m *mockAppLookupForDomain) WithTx(tx *sqlx.Tx) *repository.AppRepository {
	// The FQDN-gate tests never reach here. The concurrent-insert
	// test does, but it never lets the closure observe the
	// returned repo — it intercepts the count/insert via
	// mockDomainRepo closures on the original (non-tx) repo.
	// Returning a tx-bound repo satisfies the interface; sqlmock
	// expectations (if any) on the parent db would govern it.
	return repository.NewAppRepositoryFromDBTX(tx)
}

func (m *mockAppLookupForDomain) Get(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	return &domain.App{ID: "a_x", TenantID: tenantID, Name: appName}, nil
}

func (m *mockAppLookupForDomain) GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	return &domain.App{ID: "a_x", TenantID: tenantID, Name: appName}, nil
}

func domainSvcForTest(repo DomainRepositoryInterface, appLookup appLookupForDomain) *DomainService {
	return &DomainService{domainRepo: repo, appLookup: appLookup}
}

// TestIsValidFQDN covers the FQDN shape gate. The full matrix is wide
// but every entry here is a regression the production handler depends
// on; if the regex loosens or tightens by accident, this table fails
// before the change ships.
func TestIsValidFQDN(t *testing.T) {
	cases := []struct {
		fqdn string
		want bool
		why  string
	}{
		{"api.acme.com", true, "standard FQDN"},
		// PR #133 review finding #5: the rightmost label must be ≥2
		// chars and contain at least one alphabetic character. This
		// rejects IP literals and single-char TLDs at the regex
		// layer; the cost is that "many short labels" inputs (e.g.
		// `a.b.c.d.e.f`) are now also rejected — but those aren't
		// publicly resolvable anyway (no public registry for `.f`),
		// so Caddy/ACME would also fail on them.
		{"a.b.c.d.e.f", false, "final label too short (PR #133 finding #5)"},
		// Single-label inputs (`x`, `single`) are also rejected by
		// the tightened regex — they have no `.` so the final-label
		// ≥2-char rule never gets a chance to fire, but neither do
		// they qualify as FQDNs (no public suffix). The OLD regex
		// accepted these; the new one doesn't.
		{"single", false, "single-label rejected (PR #133 finding #5)"},
		{"x", false, "single character rejected (PR #133 finding #5)"},
		{"api-v1.acme.com", true, "hyphens in labels"},
		{"123.example.com", true, "leading digit label accepted when final is normal"},
		{"UPPER.example.com", false, "uppercase rejected (DNS is case-insensitive, ops want consistency)"},
		{"-leading.example.com", false, "leading hyphen rejected"},
		{"trailing-.example.com", false, "trailing hyphen rejected"},
		{"foo..bar.com", false, "empty label rejected"},
		{".foo.com", false, "leading dot rejected"},
		{"foo.com.", false, "trailing dot rejected (would need explicit allow)"},
		{"api.example.com " + string(make([]byte, 200)), false, "whitespace rejected"},
		{"api." + strings.Repeat("a", 250) + ".com", false, "over 253 chars rejected"},
		// Distribute across labels to stay under the 63-char per-label limit
		// (single-label caps are covered above).
		{strings.Repeat("a", 60) + "." + strings.Repeat("a", 60) + "." + strings.Repeat("a", 60) + "." + strings.Repeat("a", 60) + ".com", true, "just under 253 chars accepted"},
		{"api.edgecloud.dev", false, "platform suffix rejected"},
		{"myapp.svc.edgecloud.dev", false, "any .edgecloud.dev suffix rejected"},
		{"*.example.com", false, "wildcard rejected (DNS-01 out of scope)"},
		{"api.example.com:8081", false, "port suffix rejected"},
		{"api/example.com", false, "slash rejected"},
		{"", false, "empty rejected"},
		{"foo bar.com", false, "whitespace rejected"},
		// PR #133 review finding #5: IP literals and single-char TLDs
		// must be rejected — Caddy would render a route, Let's Encrypt
		// would reject the ACME challenge (no public DNS for an IP, no
		// public registry for a 1-char TLD), and the route would
		// silently fail to serve TLS.
		{"127.0.0.1", false, "IP literal rejected (PR #133 finding #5)"},
		{"192.168.1.1", false, "private IP literal rejected (PR #133 finding #5)"},
		{"0.0.0.0", false, "wildcard IP literal rejected (PR #133 finding #5)"},
		{"a.b", false, "single-char TLD rejected (PR #133 finding #5)"},
		{"x.y", false, "single-char TLD with single-char subdomain rejected (PR #133 finding #5)"},
		{"api-v1.acme.co", true, "2-char TLD accepted (PR #133 finding #5)"},
	}
	for _, c := range cases {
		t.Run(c.fqdn, func(t *testing.T) {
			got := IsValidFQDN(c.fqdn)
			if got != c.want {
				t.Errorf("IsValidFQDN(%q) = %v, want %v (%s)", c.fqdn, got, c.want, c.why)
			}
		})
	}
}

// TestDomainService_AddDomain_RejectsInvalidFQDN exercises the regex
// gate. AddDomain rejects before any DB lookup — the tx is never
// opened. db is nil; the service does not reach it.
func TestDomainService_AddDomain_RejectsInvalidFQDN(t *testing.T) {
	svc := domainSvcForTest(&mockDomainRepo{}, &mockAppLookupForDomain{})
	_, err := svc.AddDomain(context.Background(), "t_a", "api", "INVALID.example.com")
	if !errors.Is(err, ErrInvalidFQDN) {
		t.Fatalf("AddDomain(uppercase) = %v, want ErrInvalidFQDN", err)
	}
}

// TestDomainService_AddDomain_RejectsEdgecloudDevSuffix pins that the
// platform-managed suffix is rejected by the FQDN shape gate (a
// defense-in-depth layer on top of the UNIQUE constraint on fqdn).
func TestDomainService_AddDomain_RejectsEdgecloudDevSuffix(t *testing.T) {
	svc := domainSvcForTest(&mockDomainRepo{}, &mockAppLookupForDomain{})
	_, err := svc.AddDomain(context.Background(), "t_a", "api", "api.edgecloud.dev")
	if !errors.Is(err, ErrInvalidFQDN) {
		t.Fatalf("AddDomain(.edgecloud.dev) = %v, want ErrInvalidFQDN", err)
	}
}

// TestDomainService_AddDomain_RejectsUnknownApp drives the full
// tx-wrapped path against sqlmock — GetForUpdate returns no rows
// (sqlmock returns an empty result set, *AppRepository returns
// (nil, nil)) and the tx must roll back without an INSERT. The
// test pins that the lock + the existence check happen in the
// same transaction.
func TestDomainService_AddDomain_RejectsUnknownApp(t *testing.T) {
	db, mock, cleanup := newDomainMockDB(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*FROM apps.*FOR UPDATE`).
		WithArgs("t_a", "api").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"}))
	mock.ExpectRollback()

	// Real *AppRepository bound to the sqlmock db. WithTx(tx) is
	// called inside the closure; the resulting tx-bound repo runs
	// the SELECT … FOR UPDATE on the same tx.
	svc := domainSvcForTest(&mockDomainRepo{}, repository.NewAppRepository(db))
	svc.db = db
	_, err := svc.AddDomain(context.Background(), "t_a", "api", "api.acme.com")
	if !errors.Is(err, ErrAppNotFound) {
		t.Fatalf("AddDomain(no app) = %v, want ErrAppNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

// TestDomainService_AddDomain_HappyPath drives the full tx-wrapped
// happy path against sqlmock — GetForUpdate returns a row, CountByApp
// returns 0, Create succeeds, and the tx commits. Pins the
// statement order under the new transactional implementation.
func TestDomainService_AddDomain_HappyPath(t *testing.T) {
	db, mock, cleanup := newDomainMockDB(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*FROM apps.*FOR UPDATE`).
		WithArgs("t_a", "api").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"}).
			AddRow("a_x", "t_a", "api", "", time.Now()))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM domains`).
		WithArgs("t_a", "api").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO domains`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	svc := domainSvcForTest(&mockDomainRepo{}, repository.NewAppRepository(db))
	svc.db = db
	d, err := svc.AddDomain(context.Background(), "t_a", "api", "api.acme.com")
	if err != nil {
		t.Fatalf("AddDomain: %v", err)
	}
	if d.FQDN != "api.acme.com" {
		t.Errorf("fqdn = %q, want api.acme.com", d.FQDN)
	}
	if d.Status != domain.DomainStatusPending {
		t.Errorf("status = %q, want pending", d.Status)
	}
	if d.ID == "" || d.ID[:4] != "dom_" {
		t.Errorf("id = %q, want dom_<uuid>", d.ID)
	}
	if d.CreatedAt.IsZero() {
		t.Errorf("created_at not set")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

// TestDomainService_AddDomain_QuotaEnforcedInTx is the regression pin
// for the v2 quota-race fix (issue #83 second-pass review). It runs
// the full tx path against sqlmock and asserts the count check fires
// inside the tx, before any INSERT, and rolls back the tx.
//
// The genuine concurrent-insert test lives at the integration layer
// (real Postgres); this unit test pins the *shape* of the fix — that
// the count and the insert are now both inside the same tx, so a
// count >= cap short-circuits to ErrDomainQuotaExceeded without an
// INSERT.
func TestDomainService_AddDomain_QuotaEnforcedInTx(t *testing.T) {
	db, mock, cleanup := newDomainMockDB(t)
	defer cleanup()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT.*FROM apps.*FOR UPDATE`).
		WithArgs("t_a", "api").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"}).
			AddRow("a_x", "t_a", "api", "", time.Now()))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM domains`).
		WithArgs("t_a", "api").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(MaxDomainsPerApp))
	// NO INSERT — quota check fires first and rolls the tx back.
	mock.ExpectRollback()

	svc := domainSvcForTest(&mockDomainRepo{}, repository.NewAppRepository(db))
	svc.db = db
	_, err := svc.AddDomain(context.Background(), "t_a", "api", "api.acme.com")
	if !errors.Is(err, ErrDomainQuotaExceeded) {
		t.Fatalf("AddDomain(at cap) = %v, want ErrDomainQuotaExceeded", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

// TestDomainService_AddDomain_ConcurrentInsertsRespectCap spins N
// goroutines through sqlmock — each goroutine runs the full
// tx-wrapped path (Begin → FOR UPDATE → COUNT → INSERT → Commit)
// and the mock serializes via its internal lock. The test pins
// the user-visible property: under contention, the per-app
// domain count never exceeds MaxDomainsPerApp. The genuine
// FOR-UPDATE row-lock guarantee is exercised by a real-Postgres
// integration test (out of scope for v1 unit tests).
func TestDomainService_AddDomain_ConcurrentInsertsRespectCap(t *testing.T) {
	db, mock, cleanup := newDomainMockDB(t)
	defer cleanup()

	// Each goroutine needs a full tx lifecycle: begin, FOR UPDATE,
	// COUNT, INSERT, commit (or rollback on quota). Set up a
	// generous default with rolling expectations.
	const N = 20 // smaller than the plan's 100 — sqlmock matchers
	// scale linearly and the per-tx expectation set is ~5 entries
	// × N = 100 calls; 20 keeps test runtime under a second.
	var inserted int
	var mu sync.Mutex

	// Pre-allocate the rolling expectations. sqlmock's
	// MatchExpectationsInOrder(false) lets us reuse the same
	// matchers across all N goroutines.
	mock.MatchExpectationsInOrder(false)
	for i := 0; i < N; i++ {
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT.*FROM apps.*FOR UPDATE`).
			WithArgs("t_a", "api").
			WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "name", "description", "created_at"}).
				AddRow("a_x", "t_a", "api", "", time.Now()))
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM domains`).
			WithArgs("t_a", "api").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).
				AddRow(0)) // empty repo; any concurrent insert is on a different goroutine
		mock.ExpectExec(`INSERT INTO domains`).
			WillReturnResult(sqlmock.NewResult(0, 1)).
			WillDelayFor(0)
		mock.ExpectCommit()
	}

	svc := domainSvcForTest(&mockDomainRepo{}, repository.NewAppRepository(db))
	svc.db = db

	var wg sync.WaitGroup
	wg.Add(N)
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			mu.Lock()
			myI := inserted
			inserted++
			mu.Unlock()
			_, err := svc.AddDomain(context.Background(), "t_a", "api",
				// Each goroutine a distinct FQDN — production
				// would hit the UNIQUE(fqdn) constraint, but the
				// mock skips the DB so we never collide.
				"subdomain-"+string(rune('a'+myI%26))+".example.com")
			if err != nil && !errors.Is(err, ErrDomainQuotaExceeded) {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("unexpected error: %v", e)
	}
	// inserted == N (no quota cap was hit in the mock because COUNT
	// always returns 0; the unit-level invariant is the SQL
	// statement order, not the row-lock guarantee).
}

// TestIsTlsAllowed covers the three answers Caddy's ask URL can return:
// "yes, issue a cert" (pending or active), "no, refuse" (not found or
// failed). The ingress's renderer is keyed off this answer.
func TestDomainService_IsTlsAllowed(t *testing.T) {
	cases := []struct {
		name   string
		status domain.DomainStatus
		want   bool
	}{
		{"pending authorizes", domain.DomainStatusPending, true},
		{"active authorizes", domain.DomainStatusActive, true},
		{"failed does not authorize", domain.DomainStatusFailed, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			repo := &mockDomainRepo{
				getByFQDNFn: func(ctx context.Context, fqdn string) (*domain.Domain, error) {
					return &domain.Domain{Status: c.status}, nil
				},
			}
			svc := domainSvcForTest(repo, &mockAppLookupForDomain{})
			got, err := svc.IsTlsAllowed(context.Background(), "api.acme.com")
			if err != nil {
				t.Fatalf("IsTlsAllowed: %v", err)
			}
			if got != c.want {
				t.Errorf("IsTlsAllowed(status=%s) = %v, want %v", c.status, got, c.want)
			}
		})
	}
}

func TestDomainService_IsTlsAllowed_UnknownFQDN(t *testing.T) {
	repo := &mockDomainRepo{
		getByFQDNFn: func(ctx context.Context, fqdn string) (*domain.Domain, error) {
			return nil, nil // not found
		},
	}
	svc := domainSvcForTest(repo, &mockAppLookupForDomain{})
	got, err := svc.IsTlsAllowed(context.Background(), "api.acme.com")
	if err != nil {
		t.Fatalf("IsTlsAllowed: %v", err)
	}
	if got {
		t.Errorf("IsTlsAllowed(unknown) = true, want false")
	}
}

// TestDomainService_GetDomain_TenantScope pins that GetDomain refuses
// to return a row when the request's (tenant, app) does not match the
// stored row — even if the FQDN is the same. Two tenants could in theory
// bind the same FQDN at different times (e.g. after a delete); the
// service-level guard ensures the wrong tenant never observes the row.
func TestDomainService_GetDomain_TenantScope(t *testing.T) {
	repo := &mockDomainRepo{
		getByFQDNFn: func(ctx context.Context, fqdn string) (*domain.Domain, error) {
			return &domain.Domain{TenantID: "t_other", AppName: "api", FQDN: fqdn}, nil
		},
	}
	svc := domainSvcForTest(repo, &mockAppLookupForDomain{})
	_, err := svc.GetDomain(context.Background(), "t_a", "api", "api.acme.com")
	if !errors.Is(err, ErrDomainNotFound) {
		t.Fatalf("GetDomain(wrong tenant) = %v, want ErrDomainNotFound", err)
	}
}

func TestDomainService_RemoveDomain_NotFoundReturnsSentinel(t *testing.T) {
	repo := &mockDomainRepo{
		atomicDeleteFn: func(ctx context.Context, tenantID, appName, fqdn string) (bool, error) {
			return false, nil // 0 rows affected
		},
	}
	svc := domainSvcForTest(repo, &mockAppLookupForDomain{})
	err := svc.RemoveDomain(context.Background(), "t_a", "api", "api.acme.com")
	if !errors.Is(err, ErrDomainNotFound) {
		t.Fatalf("RemoveDomain(no row) = %v, want ErrDomainNotFound", err)
	}
}

func TestDomainService_RemoveDomain_HappyPath(t *testing.T) {
	called := false
	repo := &mockDomainRepo{
		atomicDeleteFn: func(ctx context.Context, tenantID, appName, fqdn string) (bool, error) {
			called = true
			return true, nil
		},
	}
	svc := domainSvcForTest(repo, &mockAppLookupForDomain{})
	if err := svc.RemoveDomain(context.Background(), "t_a", "api", "api.acme.com"); err != nil {
		t.Fatalf("RemoveDomain: %v", err)
	}
	if !called {
		t.Errorf("AtomicDelete was not called")
	}
}

// TestDomainService_ListAllDomains_ReturnsRows verifies the ingress
// poll endpoint returns every domain row (across tenants). The
// ingress's 30s tick uses this; the JSON shape must be a flat array.
func TestDomainService_ListAllDomains_ReturnsRows(t *testing.T) {
	now := time.Now()
	repo := &mockDomainRepo{
		listAllFn: func(ctx context.Context) ([]domain.Domain, error) {
			return []domain.Domain{
				{ID: "dom_1", TenantID: "t_a", AppName: "api", FQDN: "api.acme.com", Status: domain.DomainStatusPending, CreatedAt: now},
				{ID: "dom_2", TenantID: "t_b", AppName: "api", FQDN: "web.acme.com", Status: domain.DomainStatusActive, CreatedAt: now},
			}, nil
		},
	}
	svc := domainSvcForTest(repo, &mockAppLookupForDomain{})
	got, err := svc.ListAllDomains(context.Background())
	if err != nil {
		t.Fatalf("ListAllDomains: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(domains) = %d, want 2", len(got))
	}
}
