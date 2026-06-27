package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// mockInternalDomainSvc implements handler.InternalDomainServiceInterface
// for testing. We test the handler wire shape (status codes, JSON
// encoding, route parameters) here; the service-layer logic is covered
// in service/domain_test.go.
type mockInternalDomainSvc struct {
	listAllDomainsFn func(ctx context.Context) ([]domain.Domain, error)
	isTlsAllowedFn   func(ctx context.Context, fqdn string) (bool, error)
	updateStatusFn   func(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error
}

func (m *mockInternalDomainSvc) ListAllDomains(ctx context.Context) ([]domain.Domain, error) {
	if m.listAllDomainsFn == nil {
		return nil, nil
	}
	return m.listAllDomainsFn(ctx)
}
func (m *mockInternalDomainSvc) IsTlsAllowed(ctx context.Context, fqdn string) (bool, error) {
	if m.isTlsAllowedFn == nil {
		return false, nil
	}
	return m.isTlsAllowedFn(ctx, fqdn)
}
func (m *mockInternalDomainSvc) UpdateStatus(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error {
	if m.updateStatusFn == nil {
		return nil
	}
	return m.updateStatusFn(ctx, id, status, lastError)
}

// newInternalHandler builds an InternalHandler whose only meaningful
// field is `domainSvc` — the deployment and worker services are nil,
// so the routes that need them are NOT exercised. The custom-domain
// routes under test here don't touch them.
func newInternalHandler(svc handler.InternalDomainServiceInterface) *handler.InternalHandler {
	// The production constructor panics if any service is nil. Tests
	// that exercise ONLY the custom-domain routes need a way to inject
	// only the domain service; the cleanest way is to use the typed
	// constructor and let the deployment/worker service be the zero
	// value (which the routes we test never touch).
	//
	// We rely on the InternalHandler struct's `domainSvc` being the
	// first thing the custom-domain routes read; the deployment
	// service is only read by Download, which is not in this test set.
	return handler.NewInternalHandler(nil, nil, svc)
}

// TestInternal_ListDomains_HappyPath pins the array-shape contract that
// the ingress poller depends on. A future refactor that wraps the
// array in `{"domains": [...]}` would silently break the poller
// (each entry decodes as a map, not a Domain).
func TestInternal_ListDomains_HappyPath(t *testing.T) {
	svc := &mockInternalDomainSvc{
		listAllDomainsFn: func(ctx context.Context) ([]domain.Domain, error) {
			return []domain.Domain{
				{ID: "dom_1", TenantID: "t_a", AppName: "api", FQDN: "api.acme.com"},
				{ID: "dom_2", TenantID: "t_b", AppName: "web", FQDN: "web.acme.com"},
			}, nil
		},
	}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/domains", nil)
	rec := httptest.NewRecorder()
	h.ListDomains(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got []domain.Domain
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len = %d, want 2", len(got))
	}
}

// TestInternal_TlsAllowed_HappyPath: the FQDN is registered and
// active, Caddy is asking "may I issue a cert?" — 200.
func TestInternal_TlsAllowed_HappyPath(t *testing.T) {
	svc := &mockInternalDomainSvc{
		isTlsAllowedFn: func(ctx context.Context, fqdn string) (bool, error) {
			if fqdn != "api.acme.com" {
				t.Errorf("fqdn = %q, want api.acme.com", fqdn)
			}
			return true, nil
		},
	}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/tls-allowed?fqdn=api.acme.com", nil)
	rec := httptest.NewRecorder()
	h.TlsAllowed(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

// TestInternal_TlsAllowed_NotFound: the FQDN is NOT registered — 404.
// This is the Caddy ask-URL's most common response for a fresh tenant
// who hasn't yet added the domain.
func TestInternal_TlsAllowed_NotFound(t *testing.T) {
	svc := &mockInternalDomainSvc{
		isTlsAllowedFn: func(ctx context.Context, fqdn string) (bool, error) {
			return false, nil
		},
	}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/tls-allowed?fqdn=api.acme.com", nil)
	rec := httptest.NewRecorder()
	h.TlsAllowed(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// TestInternal_TlsAllowed_AppDeletionCascadesToDomainRow pins the
// post-fix behaviour (PR #133 review finding #4, migration
// 011_domains_cascade.up.sql). Previously an orphaned domain row
// whose underlying (tenant, app) was deleted returned 200 from
// TlsAllowed, because there was no FK cascade from `apps` to
// `domains` — letting Caddy issue a cert for a hostname whose app
// no longer existed.
//
// The cascade now removes the domain row in the same transaction
// as the app deletion. `IsTlsAllowed` therefore sees no row, returns
// false, and the handler must answer 404. The mock here simulates
// the post-cascade state (no row → false); the migration is the
// real-world trigger.
func TestInternal_TlsAllowed_AppDeletionCascadesToDomainRow(t *testing.T) {
	svc := &mockInternalDomainSvc{
		isTlsAllowedFn: func(ctx context.Context, fqdn string) (bool, error) {
			// App deleted → cascade removed the domains row → no
			// match in `GetByFQDN` → IsTlsAllowed returns false.
			return false, nil
		},
	}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/tls-allowed?fqdn=api.acme.com", nil)
	rec := httptest.NewRecorder()
	h.TlsAllowed(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (app deletion must cascade to the domain row)", rec.Code)
	}
}

// TestInternal_TlsAllowed_MissingFQDN: empty query string — 400.
func TestInternal_TlsAllowed_MissingFQDN(t *testing.T) {
	svc := &mockInternalDomainSvc{}
	h := newInternalHandler(svc)
	req := httptest.NewRequest("GET", "/api/internal/tls-allowed", nil)
	rec := httptest.NewRecorder()
	h.TlsAllowed(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// TestInternal_UpdateDomainStatus_NotFound_Returns404 is the regression
// pin for the v2 webhook path: a stale id (e.g. the domain was
// deleted between Caddy's first request and the post-issuance
// callback) must NOT silently look like success. The 204 the v1 code
// returned was a contract bug — the operator's "rows in failed
// state" alerts would never fire for these cases.
func TestInternal_UpdateDomainStatus_NotFound_Returns404(t *testing.T) {
	svc := &mockInternalDomainSvc{
		updateStatusFn: func(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error {
			return service.ErrDomainNotFound
		},
	}
	h := newInternalHandler(svc)
	body := `{"status":"active"}`
	req := httptest.NewRequest("POST", "/api/internal/domains/dom_missing/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "dom_missing")
	rec := httptest.NewRecorder()
	h.UpdateDomainStatus(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// TestInternal_UpdateDomainStatus_InvalidStatus_Returns400: only
// active|failed accepted; the v1 code already pins this.
func TestInternal_UpdateDomainStatus_InvalidStatus_Returns400(t *testing.T) {
	svc := &mockInternalDomainSvc{}
	h := newInternalHandler(svc)
	body := `{"status":"weird"}`
	req := httptest.NewRequest("POST", "/api/internal/domains/dom_x/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "dom_x")
	rec := httptest.NewRecorder()
	h.UpdateDomainStatus(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// TestInternal_UpdateDomainStatus_HappyPath: 204 on success.
func TestInternal_UpdateDomainStatus_HappyPath(t *testing.T) {
	called := false
	svc := &mockInternalDomainSvc{
		updateStatusFn: func(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error {
			called = true
			if id != "dom_x" {
				t.Errorf("id = %q, want dom_x", id)
			}
			if status != domain.DomainStatusActive {
				t.Errorf("status = %q, want active", status)
			}
			return nil
		},
	}
	h := newInternalHandler(svc)
	body := `{"status":"active"}`
	req := httptest.NewRequest("POST", "/api/internal/domains/dom_x/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "dom_x")
	rec := httptest.NewRecorder()
	h.UpdateDomainStatus(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
	if !called {
		t.Errorf("UpdateStatus was not called")
	}
}

// TestInternal_UpdateDomainStatus_InternalError_Returns500: any
// non-sentinel error must surface as 500, never as 4xx.
func TestInternal_UpdateDomainStatus_InternalError_Returns500(t *testing.T) {
	svc := &mockInternalDomainSvc{
		updateStatusFn: func(ctx context.Context, id string, status domain.DomainStatus, lastError *string) error {
			return errors.New("db boom")
		},
	}
	h := newInternalHandler(svc)
	body := `{"status":"active"}`
	req := httptest.NewRequest("POST", "/api/internal/domains/dom_x/status", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "dom_x")
	rec := httptest.NewRecorder()
	h.UpdateDomainStatus(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}
