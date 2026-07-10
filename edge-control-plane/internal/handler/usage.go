package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// UsageServiceInterface is the subset of service.UsageService used by
// UsageHandler. Mirrors QuotaServiceInterface at handler/quota.go:14-17
// so tests inject a hand-rolled mock without depending on the full
// service.
type UsageServiceInterface interface {
	GetUsage(ctx context.Context, tenantID string, from, to time.Time, limit int) (*domain.TenantUsage, error)
}

// UsageHandler handles GET /api/v1/usage — the tenant-facing usage
// dashboard endpoint (issue #421). Composes current-period counters
// (from quotas), a subscription-event timeline (from billing_events),
// and per-tenant upgrade options + billing portal URL.
//
// The handler is read-only, idempotent, and side-effect-free. The
// service layer handles caching (10s SWR), so this layer is just
// param parsing + envelope shaping.
type UsageHandler struct {
	svc UsageServiceInterface
}

func NewUsageHandler(svc UsageServiceInterface) *UsageHandler {
	return &UsageHandler{svc: svc}
}

// defaultUsageWindow is the [from, to] span when the client omits both
// query params. 30 days matches the Stripe dashboard convention so the
// tenant sees a meaningful subscription-history window without
// specifying one.
const defaultUsageWindow = 30 * 24 * time.Hour

// defaultUsageLimit / maxUsageLimit bound the events[] slice length.
// The service clamps internally as well; we clamp here so a bad
// client gets 400 instead of a silently-truncated response.
const (
	defaultUsageLimit = 50
	maxUsageLimit     = 200
)

// GetUsage handles GET /api/v1/usage. Tenant is extracted from the
// auth context (same pattern as QuotaHandler.GetQuota at handler/quota.go:39-57).
//
// Query params:
//
//	from  RFC3339, default now-30d
//	to    RFC3339, default now
//	limit int 1..200, default 50
//
// Errors:
//
//	400  malformed from/to/limit
//	404  tenant has no quota row (GetUsage returned nil, nil)
//	500  any other error from the service
func (h *UsageHandler) GetUsage(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	from, to, limit, err := parseUsageParams(r)
	if err != nil {
		http.Error(w, `{"error": "`+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	usage, err := h.svc.GetUsage(r.Context(), tenantID, from, to, limit)
	if err != nil {
		log.Printf("UsageHandler.GetUsage(%s): %v", tenantID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if usage == nil {
		http.Error(w, `{"error": "tenant not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(usage); err != nil {
		log.Printf("UsageHandler.GetUsage(%s): encode: %v", tenantID, err)
	}
}

// parseUsageParams pulls from/to/limit out of the URL. Returns 400-able
// errors with messages that surface directly to the dashboard developer.
//
// Note: the service applies its own defaults (defaultLimit / defaultWindow)
// when limit or [from,to] are zero — but parseUsageParams fills in
// those defaults here so the response's From/To echo the resolved
// window, not the user's input. Otherwise the dashboard would render
// "showing 1970-01-01 to 1970-01-01" for clients that omit the params.
func parseUsageParams(r *http.Request) (time.Time, time.Time, int, error) {
	q := r.URL.Query()

	to := time.Now().UTC()
	if v := q.Get("to"); v != "" {
		parsed, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, time.Time{}, 0, errors.New("invalid 'to': must be RFC3339")
		}
		to = parsed.UTC()
	}

	from := to.Add(-defaultUsageWindow)
	if v := q.Get("from"); v != "" {
		parsed, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, time.Time{}, 0, errors.New("invalid 'from': must be RFC3339")
		}
		from = parsed.UTC()
	}

	if from.After(to) {
		return time.Time{}, time.Time{}, 0, errors.New("'from' must be <= 'to'")
	}

	limit := defaultUsageLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return time.Time{}, time.Time{}, 0, errors.New("invalid 'limit': must be a positive integer")
		}
		if n > maxUsageLimit {
			n = maxUsageLimit
		}
		limit = n
	}

	return from, to, limit, nil
}