package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

type mockWebhookSvc struct {
	createErr    error
	list         []domain.Webhook
	listErr      error
	getByID      *domain.Webhook
	getByIDErr   error
	updateErr    error
	deleteOK     bool
	deleteErr    error
	publishCalls int

	// ListDeliveriesByWebhook (issue #659) fields.
	listDeliveries       []domain.WebhookDelivery
	listDeliveriesResult *service.WebhookDeliveriesResult
	listDeliveriesErr    error
	listDeliveriesCalls  int
	lastDeliveriesLimit  int
	lastDeliveriesCursor string
}

func (m *mockWebhookSvc) Create(_ context.Context, wh *domain.Webhook) error {
	wh.ID = "wh_" + wh.URL // simulate ID generation for assertion
	return m.createErr
}
func (m *mockWebhookSvc) ListByTenant(_ context.Context, _ string) ([]domain.Webhook, error) {
	return m.list, m.listErr
}
func (m *mockWebhookSvc) GetByID(_ context.Context, _ string) (*domain.Webhook, error) {
	return m.getByID, m.getByIDErr
}
func (m *mockWebhookSvc) Update(_ context.Context, _ *domain.Webhook) error {
	return m.updateErr
}
func (m *mockWebhookSvc) Delete(_ context.Context, _, _ string) (bool, error) {
	return m.deleteOK, m.deleteErr
}
func (m *mockWebhookSvc) PublishEvent(_ context.Context, _, _, _ string, _ interface{}) {
	m.publishCalls++
}
func (m *mockWebhookSvc) ListDeliveriesByWebhook(_ context.Context, _, _ string, limit int, cursor string) (*service.WebhookDeliveriesResult, error) {
	m.listDeliveriesCalls++
	m.lastDeliveriesLimit = limit
	m.lastDeliveriesCursor = cursor
	if m.listDeliveriesErr != nil {
		return nil, m.listDeliveriesErr
	}
	if m.listDeliveriesResult != nil {
		return m.listDeliveriesResult, nil
	}
	return &service.WebhookDeliveriesResult{
		Deliveries: m.listDeliveries,
		Limit:      limit,
	}, nil
}

func newWebhookMux(svc *mockWebhookSvc) *http.ServeMux {
	mux := http.NewServeMux()
	h := NewWebhookHandler(svc)
	mux.HandleFunc("POST /api/v1/webhooks", h.Create)
	mux.HandleFunc("GET /api/v1/webhooks", h.List)
	mux.HandleFunc("PUT /api/v1/webhooks/{webhookID}", h.Update)
	mux.HandleFunc("DELETE /api/v1/webhooks/{webhookID}", h.Delete)
	return mux
}

func withTenant(ctx context.Context, tenantID string) context.Context {
	ctx = middleware.WithTenantID(ctx, tenantID)
	ctx = middleware.WithAPIKeyID(ctx, "ak_test")
	ctx = middleware.WithRole(ctx, "owner")
	return ctx
}

func TestWebhookHandler_Create_Success(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{})

	body := `{"url":"https://hooks.example.com/evt","secret":"supersecret12345678","events":["deploy"],"description":"my webhook"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	var resp domain.Webhook
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.URL != "https://hooks.example.com/evt" {
		t.Errorf("url = %q", resp.URL)
	}
	if resp.Enabled != true {
		t.Error("enabled = false, want true")
	}
}

func TestWebhookHandler_Create_InvalidBody(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks", strings.NewReader(`bad`))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestWebhookHandler_Create_MissingURL(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{})

	body := `{"url":"","secret":"supersecret12345678","events":["deploy"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestWebhookHandler_Create_NonHTTPS(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{})

	body := `{"url":"http://hooks.example.com/evt","secret":"supersecret12345678","events":["deploy"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestWebhookHandler_Create_ShortSecret(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{})

	body := `{"url":"https://hooks.example.com","secret":"short","events":["deploy"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestWebhookHandler_Create_EmptyEvents(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{})

	body := `{"url":"https://hooks.example.com","secret":"supersecret12345678","events":[]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestWebhookHandler_Create_InvalidEvent(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{})

	body := `{"url":"https://hooks.example.com","secret":"supersecret12345678","events":["invalid_event"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestWebhookHandler_Create_ServiceError(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{createErr: context.DeadlineExceeded})

	body := `{"url":"https://hooks.example.com","secret":"supersecret12345678","events":["deploy"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rr.Code)
	}
}

func TestWebhookHandler_List_Success(t *testing.T) {
	svc := &mockWebhookSvc{
		list: []domain.Webhook{
			{ID: "wh_1", TenantID: "t_1", URL: "https://hooks.example.com", Events: []string{"deploy"}, Enabled: true},
		},
	}
	mux := newWebhookMux(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/webhooks", nil)
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	whs, ok := resp["webhooks"].([]interface{})
	if !ok {
		t.Fatalf("webhooks not an array, got %T", resp["webhooks"])
	}
	if len(whs) != 1 {
		t.Errorf("len(webhooks) = %d, want 1", len(whs))
	}
}

func TestWebhookHandler_Update_Success(t *testing.T) {
	svc := &mockWebhookSvc{
		getByID: &domain.Webhook{
			ID: "wh_1", TenantID: "t_1", URL: "https://old.example.com",
			Secret: "supersecret12345678", Events: []string{"deploy"}, Enabled: true,
		},
	}
	mux := newWebhookMux(svc)

	body := `{"url":"https://new.example.com","description":"updated webhook"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/webhooks/wh_1", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var resp domain.Webhook
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.URL != "https://new.example.com" {
		t.Errorf("url = %q, want https://new.example.com", resp.URL)
	}
}

func TestWebhookHandler_Update_NotFound(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{})

	body := `{"url":"https://new.example.com"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/webhooks/wh_missing", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestWebhookHandler_Update_WrongTenant(t *testing.T) {
	svc := &mockWebhookSvc{
		getByID: &domain.Webhook{
			ID: "wh_1", TenantID: "t_other", URL: "https://example.com",
			Secret: "supersecret12345678", Events: []string{"deploy"}, Enabled: true,
		},
	}
	mux := newWebhookMux(svc)

	body := `{"url":"https://new.example.com"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/webhooks/wh_1", strings.NewReader(body))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestWebhookHandler_Update_InvalidBody(t *testing.T) {
	svc := &mockWebhookSvc{
		getByID: &domain.Webhook{
			ID: "wh_1", TenantID: "t_1", URL: "https://old.example.com",
			Secret: "supersecret12345678", Events: []string{"deploy"}, Enabled: true,
		},
	}
	mux := newWebhookMux(svc)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/webhooks/wh_1", strings.NewReader(`bad`))
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestWebhookHandler_Delete_Success(t *testing.T) {
	svc := &mockWebhookSvc{deleteOK: true}
	mux := newWebhookMux(svc)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/webhooks/wh_1", nil)
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rr.Code)
	}
}

func TestWebhookHandler_Delete_NotFound(t *testing.T) {
	mux := newWebhookMux(&mockWebhookSvc{})

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/webhooks/wh_missing", nil)
	req = req.WithContext(withTenant(req.Context(), "t_1"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
}

func TestValidateWebhookRequest(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		secret  string
		events  []string
		wantErr bool
	}{
		{"valid", "https://hooks.example.com", "supersecret12345678", []string{"deploy"}, false},
		{"empty url", "", "supersecret12345678", []string{"deploy"}, true},
		{"invalid url", "not-a-url", "supersecret12345678", []string{"deploy"}, true},
		{"http scheme", "http://hooks.example.com", "supersecret12345678", []string{"deploy"}, true},
		{"short secret", "https://hooks.example.com", "short", []string{"deploy"}, true},
		{"empty events", "https://hooks.example.com", "supersecret12345678", nil, true},
		{"invalid event", "https://hooks.example.com", "supersecret12345678", []string{"nope"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWebhookRequest(tt.url, tt.secret, tt.events)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateWebhookRequest() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
