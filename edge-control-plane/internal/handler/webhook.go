package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
	"github.com/google/uuid"
)

type WebhookHandler struct {
	webhookSvc service.WebhookServiceInterface
}

type createWebhookRequest struct {
	URL         string   `json:"url"`
	Secret      string   `json:"secret"`
	Events      []string `json:"events"`
	Description string   `json:"description"`
}

type updateWebhookRequest struct {
	URL         *string  `json:"url"`
	Secret      *string  `json:"secret"`
	Events      []string `json:"events"`
	Description *string  `json:"description"`
	Enabled     *bool    `json:"enabled"`
}

func NewWebhookHandler(webhookSvc service.WebhookServiceInterface) *WebhookHandler {
	return &WebhookHandler{webhookSvc: webhookSvc}
}

func (h *WebhookHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	var req createWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if err := validateWebhookRequest(req.URL, req.Secret, req.Events); err != nil {
		httperror.BadRequestCtx(w, r, err.Error())
		return
	}

	wh := &domain.Webhook{
		ID:          "wh_" + uuid.New().String(),
		TenantID:    tenantID,
		URL:         req.URL,
		Secret:      req.Secret,
		Events:      req.Events,
		Description: req.Description,
		Enabled:     true,
	}

	if err := h.webhookSvc.Create(r.Context(), wh); err != nil {
		log.Printf("webhook create: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(wh)
}

func (h *WebhookHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	whs, err := h.webhookSvc.ListByTenant(r.Context(), tenantID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"webhooks": whs})
}

func (h *WebhookHandler) Update(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	webhookID := r.PathValue("webhookID")

	var req updateWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}

	wh, err := h.webhookSvc.GetByID(r.Context(), webhookID)
	if err != nil || wh == nil || wh.TenantID != tenantID {
		httperror.NotFoundCtx(w, r, "webhook not found")
		return
	}

	if req.URL != nil {
		wh.URL = *req.URL
	}
	if req.Secret != nil {
		wh.Secret = *req.Secret
	}
	if req.Events != nil {
		wh.Events = req.Events
	}
	if req.Description != nil {
		wh.Description = *req.Description
	}
	if req.Enabled != nil {
		wh.Enabled = *req.Enabled
	}

	if err := validateWebhookRequest(wh.URL, wh.Secret, []string(wh.Events)); err != nil {
		httperror.BadRequestCtx(w, r, err.Error())
		return
	}

	if err := h.webhookSvc.Update(r.Context(), wh); err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(wh)
}

func (h *WebhookHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	webhookID := r.PathValue("webhookID")

	ok, err := h.webhookSvc.Delete(r.Context(), webhookID, tenantID)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	if !ok {
		httperror.NotFoundCtx(w, r, "webhook not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// webhookDeliveriesResponse is the JSON envelope returned by
// GET /api/v1/webhooks/{webhookID}/deliveries. Mirrors the
// {"deliveries": [...]} shape used by List ("webhooks": [...]) so
// future envelope fields (filters, totals) can be added without
// breaking the wire.
type webhookDeliveriesResponse struct {
	Deliveries []domain.WebhookDelivery `json:"deliveries"`
	Limit      int                     `json:"limit"`
	NextCursor *string                  `json:"next_cursor"`
}

// ListDeliveries handles GET /api/v1/webhooks/{webhookID}/deliveries.
// Cursor pagination only (no offset — there is no legacy offset for
// this endpoint to deprecate). Mirrors handler/logs.go::List:
//   - parse + validate → service → encode
//   - typed cursor errors mapped to 400 with structured log.Printf
//   - ownership mismatch (webhook belongs to another tenant or doesn't
//     exist) maps to a 404 — collapsing both cases prevents enumeration
//     of webhook IDs across tenants.
//
// Status codes:
//
//	200  envelope {deliveries, limit, next_cursor}
//	400  invalid cursor / invalid limit
//	404  webhook not found OR not owned by the calling tenant
//	500  unexpected
func (h *WebhookHandler) ListDeliveries(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	webhookID := r.PathValue("webhookID")

	q := r.URL.Query()
	if q.Has("cursor") && q.Has("offset") {
		// Defensive: the endpoint does not advertise `offset`, but
		// reject any request that supplies one alongside a cursor so
		// a confused client cannot trigger ambiguous SQL branches.
		httperror.BadRequestCtx(w, r, "cursor and offset are mutually exclusive")
		return
	}

	limit, err := parseWebhookDeliveriesLimitParam(q.Get("limit"))
	if err != nil {
		httperror.BadRequestCtx(w, r, "invalid limit: "+err.Error())
		return
	}

	result, err := h.webhookSvc.ListDeliveriesByWebhook(r.Context(), tenantID, webhookID, limit, q.Get("cursor"))
	if err != nil {
		if errors.Is(err, service.ErrWebhookNotFound) {
			httperror.NotFoundCtx(w, r, "webhook not found")
			return
		}
		if errors.Is(err, service.ErrInvalidWebhookDeliveryCursor) || errors.Is(err, service.ErrUnsupportedWebhookDeliveryCursorVersion) {
			// Structured operator log so "malformed cursor" can be
			// distinguished from "unsupported cursor version" in
			// production without enabling debug logging. Mirrors
			// handler/logs.go:112-122 (issue #644).
			log.Printf("invalid webhook delivery cursor (tenant=%s webhook=%s): %v", tenantID, webhookID, err)
			httperror.BadRequestCtx(w, r, "invalid cursor")
			return
		}
		log.Printf("internal error listing deliveries (tenant=%s webhook=%s): %v", tenantID, webhookID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	resp := webhookDeliveriesResponse{
		Deliveries: result.Deliveries,
		Limit:      result.Limit,
		NextCursor: result.NextCursor,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// parseWebhookDeliveriesLimitParam returns the user-supplied limit, or 0
// when absent. The service substitutes a default (50) when <=0 and
// clamps to MaxWebhookDeliveryLimit (200) — the handler only validates
// that the string parses as a non-negative integer.
func parseWebhookDeliveriesLimitParam(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("expected integer")
	}
	if n < 0 {
		return 0, errors.New("must be non-negative")
	}
	return n, nil
}

func validateWebhookRequest(rawURL, secret string, events []string) error {
	if rawURL == "" {
		return errors.New("url is required")
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return errors.New("url must be a valid URL")
	}
	if u.Scheme != "https" {
		return errors.New("url must use https scheme")
	}
	if len(secret) < 16 {
		return errors.New("secret must be at least 16 characters")
	}
	if len(events) == 0 {
		return errors.New("at least one event type is required")
	}
	for _, e := range events {
		if !domain.IsValidWebhookEvent(e) {
			return errors.New("invalid event: " + e + " (valid: " + strings.Join(domain.ValidWebhookEvents, ", ") + ")")
		}
	}
	return nil
}
