package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// BillingHandler exposes the merchant-agnostic billing HTTP surface
// (issue #419). Four endpoints:
//
//	POST /api/v1/billing/checkout    — auth required
//	POST /api/v1/billing/portal      — auth required
//	GET  /api/v1/billing/subscription — auth required
//	POST /api/v1/billing/webhook     — NO auth (signature-verified inline)
//
// The webhook is mounted on a separate mux (no auth middleware) at
// app composition time. The first three are mounted on the
// authenticated mux.
type BillingHandler struct {
	billingSvc billing.BillingServiceInterface
}

func NewBillingHandler(billingSvc billing.BillingServiceInterface) *BillingHandler {
	return &BillingHandler{billingSvc: billingSvc}
}

// CheckoutRequest is the body for POST /api/v1/billing/checkout.
// SuccessURL / CancelURL are optional — when empty the service
// falls back to the operator-configured defaults wired at startup.
type CheckoutRequest struct {
	Plan       string `json:"plan"`
	SuccessURL string `json:"success_url,omitempty"`
	CancelURL  string `json:"cancel_url,omitempty"`
}

// CheckoutResponse is what we send back to the tenant's browser
// handler (or a CLI). The frontend redirects the user to URL.
type CheckoutResponse struct {
	CheckoutURL string `json:"checkout_url"`
	SessionID   string `json:"session_id"`
	ExpiresAt   string `json:"expires_at,omitempty"`
}

// PortalRequest is the body for POST /api/v1/billing/portal.
type PortalRequest struct {
	ReturnURL string `json:"return_url"`
}

// PortalResponse is the URL to the self-service portal.
type PortalResponse struct {
	PortalURL string `json:"portal_url"`
}

// StartCheckout handles POST /api/v1/billing/checkout. Auth
// required. Plan must be a known paid tier; "free" is rejected here
// because the only way to land on free is via subscription.deleted
// (a webhook) or admin override.
func (h *BillingHandler) StartCheckout(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	var req CheckoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.Plan == "" {
		httperror.BadRequestCtx(w, r, "plan is required")
		return
	}
	if !domain.IsValidPlan(req.Plan) {
		httperror.BadRequestCtx(w, r, "plan: must be one of free, pro, business, enterprise")
		return
	}
	if req.Plan == "free" {
		httperror.BadRequestCtx(w, r, "plan: free tier does not require checkout")
		return
	}

	sess, err := h.billingSvc.StartCheckout(r.Context(), tenantID, req.Plan)
	if err != nil {
		log.Printf("StartCheckout(%s, %s): %v", tenantID, req.Plan, err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(CheckoutResponse{
		CheckoutURL: sess.URL,
		SessionID:   sess.ID,
		ExpiresAt:   sess.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z"),
	})
}

// OpenPortal handles POST /api/v1/billing/portal. Auth required.
// The tenant must have a billing_subscriptions row; if not, the
// service returns ErrNoSubscription and the handler surfaces 404.
func (h *BillingHandler) OpenPortal(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	var req PortalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.ReturnURL == "" {
		httperror.BadRequestCtx(w, r, "return_url is required")
		return
	}

	ps, err := h.billingSvc.OpenPortal(r.Context(), tenantID, req.ReturnURL)
	if err != nil {
		switch {
		case errors.Is(err, billing.ErrNoSubscription):
			httperror.NotFoundCtx(w, r, "no subscription for tenant — start a checkout first")
		default:
			log.Printf("OpenPortal(%s): %v", tenantID, err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(PortalResponse{PortalURL: ps.URL})
}

// GetSubscription handles GET /api/v1/billing/subscription. Auth
// required. Returns the local billing_subscriptions row (canonical
// mirror updated by webhooks).
func (h *BillingHandler) GetSubscription(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	sub, err := h.billingSvc.GetSubscription(r.Context(), tenantID)
	if err != nil {
		switch {
		case errors.Is(err, billing.ErrNoSubscription):
			httperror.NotFoundCtx(w, r, "no subscription for tenant")
		default:
			log.Printf("GetSubscription(%s): %v", tenantID, err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(sub)
}

// StripeWebhook handles POST /api/v1/billing/webhook. NO auth
// middleware — the provider's VerifyWebhook checks the signature
// inline. Returns:
//
//	200 — successful dispatch, idempotent replay, OR unhandled event
//	      type (we explicitly ignore event classes we don't dispatch on;
//	      otherwise Stripe would 5xx-retry forever and burn the 3-day
//	      retry window)
//	400 — signature verification failed (provider-specific sentinel)
//	422 — event references a tenant we don't know about
//	500 — DB error; Stripe will retry
//
// Reads the body raw via io.ReadAll because the signature is
// computed over the exact bytes; a Decode through json would
// normalize whitespace and break verification.
func (h *BillingHandler) StripeWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httperror.BadRequestCtx(w, r, "could not read request body")
		return
	}
	if err := h.billingSvc.HandleWebhook(r.Context(), r.Header, body); err != nil {
		switch {
		case isSignatureFailure(err):
			// 400 so the merchant retries.
			httperror.BadRequestCtx(w, r, "invalid signature")
		case errors.Is(err, billing.ErrUnknownEvent):
			// 200 — the merchant sent something we intentionally
			// ignore. NOT a 4xx (no merchant action) and NOT a 5xx
			// (we don't want Stripe to retry forever).
			log.Printf("HandleWebhook: ignoring unknown event: %v", err)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ignored"})
		case errors.Is(err, billing.ErrTenantUnresolved):
			// 422 — the event landed but we can't attribute it.
			httperror.BadRequestCtx(w, r, "tenant unresolved for event")
		default:
			log.Printf("HandleWebhook: %v", err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// isSignatureFailure is a string-based sniff for the provider's
// ErrInvalidSignature sentinels. Stripe's is
// "stripe: invalid webhook signature"; noop's is "noop provider
// rejects all webhooks (dev/CI/test only)". Both contain the word
// "signature" so we check for that — the handler has no other
// reason to be string-matching on errors. A future provider that
// wants a stronger contract can expose a typed error.
func isSignatureFailure(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "signature") ||
		strings.Contains(err.Error(), "noop provider rejects all webhooks")
}
