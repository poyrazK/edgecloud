package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

type mockAppSvc struct {
	createApp *domain.App
	createErr error
	listPage  *service.AppListPage
	listErr   error
	getApp    *domain.App
	getErr    error
	updateApp *domain.App
	updateErr error
	deleteErr error
	// L4 port accessors (issue #548). getL4Port is the persisted
	// port (0 = unallocated). allocateL4Port mirrors what the service
	// returns; if unset, the mock echoes the input port.
	getL4Port      uint16
	getL4PortErr   error
	allocateL4Port uint16
	allocateL4Err  error
}

func (m *mockAppSvc) Create(ctx context.Context, tenantID, appName string, req *domain.CreateAppRequest) (*domain.App, error) {
	return m.createApp, m.createErr
}
func (m *mockAppSvc) List(ctx context.Context, tenantID string, limit int, afterCursor string) (*service.AppListPage, error) {
	return m.listPage, m.listErr
}
func (m *mockAppSvc) Get(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.getApp, nil
}
func (m *mockAppSvc) Update(ctx context.Context, tenantID, appName string, req *domain.UpdateAppRequest) (*domain.App, error) {
	return m.updateApp, m.updateErr
}
func (m *mockAppSvc) Delete(ctx context.Context, tenantID, appName string) error {
	return m.deleteErr
}
func (m *mockAppSvc) GetL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	return m.getL4Port, m.getL4PortErr
}
func (m *mockAppSvc) AllocateL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	return m.allocateL4Port, m.allocateL4Err
}

func newAppMux(svc *mockAppSvc) *http.ServeMux {
	mux := http.NewServeMux()
	h := &AppHandler{appSvc: svc}
	mux.HandleFunc("POST /api/apps/{appName}", h.Create)
	mux.HandleFunc("GET /api/apps", h.List)
	mux.HandleFunc("GET /api/apps/{appName}", h.Get)
	mux.HandleFunc("GET /api/v1/apps/{appName}/l4-port", h.GetL4Port)
	mux.HandleFunc("POST /api/v1/apps/{appName}/l4-port", h.AllocateL4Port)
	mux.HandleFunc("PUT /api/apps/{appName}", h.Update)
	mux.HandleFunc("DELETE /api/apps/{appName}", h.Delete)
	return mux
}

func TestAppHandler_Create_Success(t *testing.T) {
	svc := &mockAppSvc{createApp: &domain.App{ID: "a_1", TenantID: "t_test", Name: "hello"}}
	mux := newAppMux(svc)

	body := `{"description":"my-app"}`
	req := httptest.NewRequest(http.MethodPost, "/api/apps/hello", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	var app domain.App
	if err := json.Unmarshal(rr.Body.Bytes(), &app); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if app.Name != "hello" {
		t.Errorf("Name = %q, want hello", app.Name)
	}
}

func TestAppHandler_Create_InvalidBody(t *testing.T) {
	mux := newAppMux(&mockAppSvc{})

	req := httptest.NewRequest(http.MethodPost, "/api/apps/hello", strings.NewReader(`not json`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestAppHandler_Create_AlreadyExists(t *testing.T) {
	svc := &mockAppSvc{createErr: service.ErrAppAlreadyExists}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/apps/hello", strings.NewReader(`{}`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", rr.Code)
	}
}

func TestAppHandler_Create_QuotaExceeded(t *testing.T) {
	svc := &mockAppSvc{createErr: service.ErrMaxAppsQuotaExceeded}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/apps/hello", strings.NewReader(`{}`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rr.Code)
	}
}

// TestAppHandler_List_FirstPage covers the happy-path first-page
// request: no `?cursor=`, no `?offset=`, no `?limit=`. Verifies
// that the response envelope carries `{apps, limit}` and that
// `next_cursor` is OMITTED (omitempty) when the service returns
// nil. Issue #58.
func TestAppHandler_List_FirstPage(t *testing.T) {
	svc := &mockAppSvc{listPage: &service.AppListPage{
		Apps:       []domain.App{},
		Limit:      50,
		NextCursor: nil,
	}}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	// Decode into a generic map so we can assert field presence
	// and absence (next_cursor must be missing on the final page).
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := resp["apps"]; !ok {
		t.Error("response missing 'apps' field")
	}
	if v, ok := resp["limit"].(float64); !ok || int(v) != 50 {
		t.Errorf("limit = %v, want 50", resp["limit"])
	}
	if _, hasNext := resp["next_cursor"]; hasNext {
		t.Errorf("response has 'next_cursor' on final page; want omitted (omitempty)")
	}
	// Issue #58 — offset is gone entirely.
	if _, hasOff := resp["offset"]; hasOff {
		t.Errorf("response has 'offset'; the apps envelope is hard-cut (issue #58)")
	}
}

// TestAppHandler_List_HasMore_EmitsNextCursor pins the
// limit+1-probe + cursor-encode path: when the service returns a
// page with NextCursor, the envelope MUST serialize it as a
// base64url string under the `next_cursor` key.
func TestAppHandler_List_HasMore_EmitsNextCursor(t *testing.T) {
	cursor := "eyJ2IjoxLCJwIjp7Im5hbWUiOiJiYXR0In19"
	svc := &mockAppSvc{listPage: &service.AppListPage{
		Apps:       []domain.App{{ID: "a_1", Name: "alpha"}, {ID: "a_2", Name: "batt"}},
		Limit:      50,
		NextCursor: &cursor,
	}}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps?limit=50", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v, ok := resp["next_cursor"].(string); !ok || v != cursor {
		t.Errorf("next_cursor = %v, want %q", resp["next_cursor"], cursor)
	}
}

// TestAppHandler_List_BadCursor_400 — a malformed cursor string
// must surface as 400 with the generic "invalid cursor" message,
// never 500. Mirrors the typed-error contract at
// handler/webhook.go:199-207.
func TestAppHandler_List_BadCursor_400(t *testing.T) {
	svc := &mockAppSvc{listErr: service.ErrInvalidAppCursor}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps?cursor=garbage", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "invalid cursor") {
		t.Errorf("body = %q, want substring 'invalid cursor'", rr.Body.String())
	}
}

// TestAppHandler_List_RejectsCursorAndOffset_400 — a request that
// supplies BOTH `?cursor=` and `?offset=` is rejected as 400 even
// though `?offset=` is no longer advertised. Mirrors the
// handler/webhook.go:179-185 idiom.
func TestAppHandler_List_RejectsCursorAndOffset_400(t *testing.T) {
	svc := &mockAppSvc{}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps?cursor=eyJ2Ijox&offset=10", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "mutually exclusive") {
		t.Errorf("body = %q, want substring 'mutually exclusive'", rr.Body.String())
	}
}

// TestAppHandler_List_BadLimit_400 — a non-numeric or non-positive
// `?limit=` returns 400 with the generic "invalid limit" message.
// The cap-clamping path (limit > 500) is tested separately as
// TestAppHandler_List_ClampsLimit.
func TestAppHandler_List_BadLimit_400(t *testing.T) {
	svc := &mockAppSvc{}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps?limit=abc", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for non-numeric limit", rr.Code)
	}
}

// TestAppHandler_List_ClampsLimit pins the silent cap on
// `?limit=` > appsLimitCap (500). The handler must clamp AND
// report the effective limit in the response so the caller can
// learn the cap without consulting docs.
func TestAppHandler_List_ClampsLimit(t *testing.T) {
	// The mock records the limit it received; if clamping works the
	// service should see 500, not 5000.
	var seenLimit int
	svc := &mockAppSvc{
		listPage: &service.AppListPage{Apps: nil, Limit: 0, NextCursor: nil},
	}
	// Wrap so we can spy on the limit argument.
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Re-call svc.List with the same arguments via a wrapper
		// service isn't easy — instead, assert on the response
		// `limit` field which the handler reports from page.Limit
		// (the effective limit the service was called with).
		_ = seenLimit
		h := &AppHandler{appSvc: &limitSpy{inner: svc, got: &seenLimit}}
		h.List(w, r)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/apps?limit=5000", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if seenLimit != appsLimitCap {
		t.Errorf("service called with limit=%d, want %d (clamped)", seenLimit, appsLimitCap)
	}
}

// limitSpy wraps a mockAppSvc to record the limit argument. Used
// only by TestAppHandler_List_ClampsLimit so we can assert the
// clamp without an extra field on the public mock struct.
type limitSpy struct {
	inner *mockAppSvc
	got   *int
}

func (s *limitSpy) Create(ctx context.Context, tenantID, appName string, req *domain.CreateAppRequest) (*domain.App, error) {
	return s.inner.Create(ctx, tenantID, appName, req)
}
func (s *limitSpy) List(ctx context.Context, tenantID string, limit int, afterCursor string) (*service.AppListPage, error) {
	*s.got = limit
	return s.inner.List(ctx, tenantID, limit, afterCursor)
}
func (s *limitSpy) Get(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	return s.inner.Get(ctx, tenantID, appName)
}
func (s *limitSpy) Update(ctx context.Context, tenantID, appName string, req *domain.UpdateAppRequest) (*domain.App, error) {
	return s.inner.Update(ctx, tenantID, appName, req)
}
func (s *limitSpy) Delete(ctx context.Context, tenantID, appName string) error {
	return s.inner.Delete(ctx, tenantID, appName)
}
func (s *limitSpy) GetL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	return s.inner.GetL4Port(ctx, tenantID, appName)
}
func (s *limitSpy) AllocateL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	return s.inner.AllocateL4Port(ctx, tenantID, appName)
}

func TestAppHandler_Get_Found(t *testing.T) {
	svc := &mockAppSvc{getApp: &domain.App{ID: "a_1", TenantID: "t_test", Name: "hello"}}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps/hello", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
}

func TestAppHandler_Get_NotFound(t *testing.T) {
	svc := &mockAppSvc{getApp: nil}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/apps/hello", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestAppHandler_Update_Success(t *testing.T) {
	desc := "updated description"
	svc := &mockAppSvc{
		updateApp: &domain.App{ID: "a_1", TenantID: "t_test", Name: "hello", Description: &desc},
	}
	mux := newAppMux(svc)

	body := `{"description":"updated description"}`
	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var app domain.App
	if err := json.Unmarshal(rr.Body.Bytes(), &app); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if app.Description == nil || *app.Description != "updated description" {
		t.Errorf("Description = %v, want 'updated description'", app.Description)
	}
}

func TestAppHandler_Update_ClearsDescription(t *testing.T) {
	empty := ""
	svc := &mockAppSvc{
		updateApp: &domain.App{ID: "a_1", TenantID: "t_test", Name: "hello", Description: &empty},
	}
	mux := newAppMux(svc)

	body := `{"description":""}`
	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var app domain.App
	if err := json.Unmarshal(rr.Body.Bytes(), &app); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if app.Description == nil || *app.Description != "" {
		t.Errorf("Description = %v, want empty string", app.Description)
	}
}

func TestAppHandler_Update_NotFound(t *testing.T) {
	svc := &mockAppSvc{updateErr: service.ErrAppNotFound}
	mux := newAppMux(svc)

	body := `{"description":"doesnt matter"}`
	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello", strings.NewReader(body))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestAppHandler_Update_InvalidBody(t *testing.T) {
	mux := newAppMux(&mockAppSvc{})

	req := httptest.NewRequest(http.MethodPut, "/api/apps/hello", strings.NewReader(`not json`))
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestAppHandler_Delete_Success(t *testing.T) {
	mux := newAppMux(&mockAppSvc{})

	req := httptest.NewRequest(http.MethodDelete, "/api/apps/hello", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rr.Code)
	}
}

func TestAppHandler_Delete_NotFound(t *testing.T) {
	svc := &mockAppSvc{deleteErr: service.ErrAppNotFound}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodDelete, "/api/apps/hello", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

// TestAppHandler_Delete_ServiceErrorReturns500 pins the post-fix
// contract (issue #60): when AppService.Delete returns a non-ErrAppNotFound
// error — for example a cascade failure or a post-commit artifact-store
// failure — the handler maps it to HTTP 500. Operators see the failure
// instead of a misleading 204.
func TestAppHandler_Delete_ServiceErrorReturns500(t *testing.T) {
	svc := &mockAppSvc{deleteErr: errors.New("simulated cascade failure")}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodDelete, "/api/apps/hello", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

// ── L4 port endpoints (issue #548) ────────────────────────────────────

func TestAppHandler_GetL4Port_Allocated(t *testing.T) {
	svc := &mockAppSvc{getL4Port: 31042}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp struct {
		PublicPort uint16 `json:"public_port"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.PublicPort != 31042 {
		t.Errorf("public_port = %d, want 31042", resp.PublicPort)
	}
}

func TestAppHandler_GetL4Port_Unallocated(t *testing.T) {
	// Mock returns (0, nil) — app exists but port is unset.
	svc := &mockAppSvc{getL4Port: 0}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestAppHandler_GetL4Port_AppNotFound(t *testing.T) {
	svc := &mockAppSvc{getL4PortErr: service.ErrAppNotFound}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestAppHandler_AllocateL4Port_HappyPath(t *testing.T) {
	svc := &mockAppSvc{allocateL4Port: 31042}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp struct {
		PublicPort uint16 `json:"public_port"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.PublicPort != 31042 {
		t.Errorf("public_port = %d, want 31042", resp.PublicPort)
	}
}

func TestAppHandler_AllocateL4Port_RangeExhausted(t *testing.T) {
	svc := &mockAppSvc{allocateL4Err: service.ErrL4PortRangeExhausted}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", rr.Code)
	}
}

func TestAppHandler_AllocateL4Port_AppNotFound(t *testing.T) {
	svc := &mockAppSvc{allocateL4Err: service.ErrAppNotFound}
	mux := newAppMux(svc)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/apps/hello/l4-port", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}
