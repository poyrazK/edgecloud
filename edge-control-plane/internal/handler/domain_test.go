package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// mockDomainSvc implements handler.DomainServiceInterface for testing.
// We test the handler wire shape (status codes, JSON encoding, route
// parameters) here; the service-layer logic (FQDN validation, quota,
// tenant scoping) is covered exhaustively in service/domain_test.go.
type mockDomainSvc struct {
	addFn    func(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error)
	listFn   func(ctx context.Context, tenantID, appName string) ([]domain.Domain, error)
	getFn    func(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error)
	removeFn func(ctx context.Context, tenantID, appName, fqdn string) error
}

func (m *mockDomainSvc) AddDomain(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
	return m.addFn(ctx, tenantID, appName, fqdn)
}
func (m *mockDomainSvc) ListDomains(ctx context.Context, tenantID, appName string) ([]domain.Domain, error) {
	return m.listFn(ctx, tenantID, appName)
}
func (m *mockDomainSvc) GetDomain(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
	return m.getFn(ctx, tenantID, appName, fqdn)
}
func (m *mockDomainSvc) RemoveDomain(ctx context.Context, tenantID, appName, fqdn string) error {
	return m.removeFn(ctx, tenantID, appName, fqdn)
}

// tenantCtx seeds the request context with a tenant ID, mirroring the
// production middleware (`middleware.Authenticate`). The context-key
// type is `middleware.TenantIDKey` — we reach into the package via the
// exported `WithTenantID` helper if present, otherwise we set the
// private key via a small bridge in this test file.
func tenantCtx(tenantID string) context.Context {
	return withTenantID(context.Background(), tenantID)
}

func newReq(method, target, body string) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	return r
}

func TestDomainHandler_Add_HappyPath(t *testing.T) {
	svc := &mockDomainSvc{
		addFn: func(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
			return &domain.Domain{
				ID: "dom_x", TenantID: tenantID, AppName: appName, FQDN: fqdn,
				Status: domain.DomainStatusPending, CreatedAt: time.Now(),
			}, nil
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)

	req := newReq("POST", "/api/apps/api/domains", `{"fqdn":"api.acme.com"}`)
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	rec := httptest.NewRecorder()
	h.Add(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", rec.Code)
	}
	var got domain.Domain
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.FQDN != "api.acme.com" {
		t.Errorf("fqdn = %q, want api.acme.com", got.FQDN)
	}
	if got.Status != domain.DomainStatusPending {
		t.Errorf("status = %q, want pending", got.Status)
	}
}

// TestDomainHandler_Add_InvalidFQDN_Returns400 pins the sentinel-to-status
// mapping. Without this, a future refactor that drops the errors.Is
// check would silently 500 instead of 400 on bad input.
func TestDomainHandler_Add_InvalidFQDN_Returns400(t *testing.T) {
	svc := &mockDomainSvc{
		addFn: func(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
			return nil, service.ErrInvalidFQDN
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("POST", "/api/apps/api/domains", `{"fqdn":"INVALID.example.com"}`)
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	rec := httptest.NewRecorder()
	h.Add(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestDomainHandler_Add_QuotaExceeded_Returns429(t *testing.T) {
	svc := &mockDomainSvc{
		addFn: func(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
			return nil, service.ErrDomainQuotaExceeded
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("POST", "/api/apps/api/domains", `{"fqdn":"api.acme.com"}`)
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	rec := httptest.NewRecorder()
	h.Add(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
}

func TestDomainHandler_Add_AppNotFound_Returns404(t *testing.T) {
	svc := &mockDomainSvc{
		addFn: func(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
			return nil, service.ErrAppNotFound
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("POST", "/api/apps/api/domains", `{"fqdn":"api.acme.com"}`)
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	rec := httptest.NewRecorder()
	h.Add(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// TestDomainHandler_Add_InvalidBody_Returns400 catches a JSON decode
// error before the service layer is touched. Pins that a malformed body
// doesn't accidentally produce a 500.
func TestDomainHandler_Add_InvalidBody_Returns400(t *testing.T) {
	svc := &mockDomainSvc{}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("POST", "/api/apps/api/domains", `not json`)
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	rec := httptest.NewRecorder()
	h.Add(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// TestDomainHandler_Add_MissingFQDN_Returns400 catches the
// service-layer branch where the body is valid JSON but `fqdn` is
// empty. The handler short-circuits without calling the service.
func TestDomainHandler_Add_MissingFQDN_Returns400(t *testing.T) {
	called := false
	svc := &mockDomainSvc{
		addFn: func(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
			called = true
			return nil, nil
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("POST", "/api/apps/api/domains", `{"fqdn":""}`)
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	rec := httptest.NewRecorder()
	h.Add(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if called {
		t.Errorf("service should not be called when fqdn is empty")
	}
}

// TestDomainHandler_List_EmptyArray pins the JSON shape for the empty
// case. The CLI's tabular output depends on the array literal, not
// `null`.
func TestDomainHandler_List_EmptyArray(t *testing.T) {
	svc := &mockDomainSvc{
		listFn: func(ctx context.Context, tenantID, appName string) ([]domain.Domain, error) {
			return nil, nil
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("GET", "/api/apps/api/domains", "")
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	rec := httptest.NewRecorder()
	h.List(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"domains":[]`) {
		t.Errorf("body = %q, want contains '\"domains\":[]'", rec.Body.String())
	}
}

func TestDomainHandler_List_ReturnsRows(t *testing.T) {
	now := time.Now()
	svc := &mockDomainSvc{
		listFn: func(ctx context.Context, tenantID, appName string) ([]domain.Domain, error) {
			return []domain.Domain{
				{ID: "dom_1", TenantID: tenantID, AppName: appName, FQDN: "api.acme.com", Status: domain.DomainStatusPending, CreatedAt: now},
				{ID: "dom_2", TenantID: tenantID, AppName: appName, FQDN: "web.acme.com", Status: domain.DomainStatusActive, CreatedAt: now},
			}, nil
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("GET", "/api/apps/api/domains", "")
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	rec := httptest.NewRecorder()
	h.List(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"fqdn":"api.acme.com"`) {
		t.Errorf("body missing api.acme.com: %s", body)
	}
	if !strings.Contains(body, `"fqdn":"web.acme.com"`) {
		t.Errorf("body missing web.acme.com: %s", body)
	}
}

func TestDomainHandler_Get_NotFound_Returns404(t *testing.T) {
	svc := &mockDomainSvc{
		getFn: func(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
			return nil, service.ErrDomainNotFound
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("GET", "/api/apps/api/domains/api.acme.com", "")
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	req.SetPathValue("fqdn", "api.acme.com")
	rec := httptest.NewRecorder()
	h.Get(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestDomainHandler_Get_HappyPath(t *testing.T) {
	now := time.Now()
	svc := &mockDomainSvc{
		getFn: func(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error) {
			return &domain.Domain{
				ID: "dom_x", TenantID: tenantID, AppName: appName, FQDN: fqdn,
				Status: domain.DomainStatusPending, CreatedAt: now,
			}, nil
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("GET", "/api/apps/api/domains/api.acme.com", "")
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	req.SetPathValue("fqdn", "api.acme.com")
	rec := httptest.NewRecorder()
	h.Get(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	var got domain.Domain
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.FQDN != "api.acme.com" {
		t.Errorf("fqdn = %q, want api.acme.com", got.FQDN)
	}
}

func TestDomainHandler_Remove_HappyPath(t *testing.T) {
	called := false
	svc := &mockDomainSvc{
		removeFn: func(ctx context.Context, tenantID, appName, fqdn string) error {
			called = true
			if fqdn != "api.acme.com" {
				t.Errorf("fqdn = %q, want api.acme.com", fqdn)
			}
			return nil
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("DELETE", "/api/apps/api/domains/api.acme.com", "")
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	req.SetPathValue("fqdn", "api.acme.com")
	rec := httptest.NewRecorder()
	h.Remove(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
	if !called {
		t.Errorf("RemoveDomain was not called")
	}
}

func TestDomainHandler_Remove_NotFound_Returns404(t *testing.T) {
	svc := &mockDomainSvc{
		removeFn: func(ctx context.Context, tenantID, appName, fqdn string) error {
			return service.ErrDomainNotFound
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("DELETE", "/api/apps/api/domains/api.acme.com", "")
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	req.SetPathValue("fqdn", "api.acme.com")
	rec := httptest.NewRecorder()
	h.Remove(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// TestDomainHandler_Remove_InternalError_Returns500 pins the default
// 5xx path. A non-sentinel error must NOT be mapped to 4xx.
func TestDomainHandler_Remove_InternalError_Returns500(t *testing.T) {
	svc := &mockDomainSvc{
		removeFn: func(ctx context.Context, tenantID, appName, fqdn string) error {
			return errors.New("boom")
		},
	}
	h := handler.NewDomainHandlerFromMock(svc)
	req := newReq("DELETE", "/api/apps/api/domains/api.acme.com", "")
	req = req.WithContext(tenantCtx("t_a"))
	req.SetPathValue("appName", "api")
	req.SetPathValue("fqdn", "api.acme.com")
	rec := httptest.NewRecorder()
	h.Remove(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}
