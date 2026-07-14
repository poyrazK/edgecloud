package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// LogHandler serves GET /api/v1/apps/{appName}/logs — the read path for
// issue #77. Tenants ask for the most recent N entries of one of their
// apps, optionally filtered by minimum level and time window.
//
// The handler is intentionally thin: parse + validate → service → encode.
// All defaults, clamping, and canonicalization lives in the service layer
// (and ultimately the repository) so other callers (e.g. a future operator
// UI) can reuse the same query semantics.
type LogHandler struct {
	logSvc *service.LogService
}

func NewLogHandler(logSvc *service.LogService) *LogHandler {
	return &LogHandler{logSvc: logSvc}
}

// LogListResponse is the JSON envelope returned by GET .../logs. Cursor
// pagination is preferred; next_offset remains for one compatibility release.
type LogListResponse struct {
	Items      []domain.LogEntry `json:"items"`
	Limit      int               `json:"limit"`
	Since      string            `json:"since"`
	NextOffset *int              `json:"next_offset"`
	NextCursor *string           `json:"next_cursor"`
}

// List handles GET /api/v1/apps/{appName}/logs.
//
// Query params (all optional):
//
//	since   RFC3339 timestamp; missing → service default (5m).
//	level   trace|debug|info|warn|error; filter ≥ level. Missing → no filter.
//	limit   1..1000; missing/0 → service default (100).
//
// Status codes:
//
//	200  envelope {items, limit, since}
//	400  invalid appName, malformed since/level/limit
//	500  anything else
func (h *LogHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	// Path-traversal guard first: validateAppName writes the 400 itself
	// (mirrors every other handler in this package).
	if !validateAppName(w, appName) {
		return
	}

	q := r.URL.Query()
	if q.Has("cursor") && q.Has("offset") {
		httperror.BadRequestCtx(w, r, "cursor and offset are mutually exclusive")
		return
	}

	now := time.Now()
	since, sinceAt, err := parseSinceParam(q.Get("since"), now)
	if err != nil {
		httperror.BadRequestCtx(w, r, "invalid since: "+err.Error())
		return
	}
	until, err := parseUntilParam(q.Get("until"), now)
	if err != nil {
		httperror.BadRequestCtx(w, r, "invalid until: "+err.Error())
		return
	}
	if !sinceAt.IsZero() && !until.IsZero() && until.Before(sinceAt) {
		httperror.BadRequestCtx(w, r, "until must not be before since")
		return
	}
	minLvl := q.Get("level")
	if minLvl != "" && service.LogLevelOrdinal(minLvl) < 0 {
		httperror.BadRequestCtx(w, r, "invalid level: "+minLvl)
		return
	}
	limit, err := parseLimitParam(q.Get("limit"))
	if err != nil {
		httperror.BadRequestCtx(w, r, "invalid limit: "+err.Error())
		return
	}
	offset, err := parseOffsetParam(q.Get("offset"))
	if err != nil {
		httperror.BadRequestCtx(w, r, "invalid offset: "+err.Error())
		return
	}

	result, err := h.logSvc.ListByTenantApp(r.Context(), tenantID, appName, service.LogQuery{
		Since:  since,
		Until:  until,
		MinLvl: minLvl,
		Limit:  limit,
		Offset: offset,
		Cursor: q.Get("cursor"),
	})
	if err != nil {
		if errors.Is(err, service.ErrInvalidLogCursor) || errors.Is(err, service.ErrUnsupportedLogCursorVersion) {
			// Log the typed reason at info level so an operator
			// can distinguish "malformed base64 / JSON" from
			// "unsupported cursor version" without enabling
			// debug logging. The wire response collapses both
			// to a generic "invalid cursor" so we don't leak
			// decoder internals to clients.
			log.Printf("invalid cursor (tenant=%s app=%s): %v", tenantID, appName, err)
			httperror.BadRequestCtx(w, r, "invalid cursor")
			return
		}
		// service.ErrInvalidLevel is gated above; any other error is unexpected.
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	resp := LogListResponse{
		Items:      result.Entries,
		Limit:      result.Limit,
		Since:      effectiveSinceRFC3339(result.Since, now),
		NextOffset: result.NextOffset,
		NextCursor: result.NextCursor,
	}
	w.Header().Set("Content-Type", "application/json")
	// json.NewEncoder never returns a meaningful error here — the
	// httptest.Recorder swallows it on the test side, and in production
	// the connection drop is the real failure mode, not the encode.
	_ = json.NewEncoder(w).Encode(resp)
}

// parseSinceParam converts an optional RFC3339 lower bound into the positive
// lookback duration consumed by the repository's DB-clock-relative predicate.
func parseSinceParam(raw string, now time.Time) (time.Duration, time.Time, error) {
	if raw == "" {
		return 0, time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, time.Time{}, errors.New("expected RFC3339 timestamp")
	}
	if t.After(now) {
		return 0, time.Time{}, errors.New("since must not be in the future")
	}
	return now.Sub(t), t.UTC(), nil
}

func parseUntilParam(raw string, now time.Time) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, errors.New("expected RFC3339 timestamp")
	}
	if t.After(now) {
		return time.Time{}, errors.New("until must not be in the future")
	}
	return t.UTC(), nil
}

// parseLimitParam returns the user-supplied limit, or 0 when absent.
// The service is responsible for substituting a default and clamping
// the upper bound — the handler only validates that the string parses
// as a non-negative integer (the service treats ≤0 as "use default").
func parseLimitParam(raw string) (int, error) {
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

// parseOffsetParam returns the user-supplied offset, or 0 when absent.
// Accepts non-negative integers only; negative values are rejected with
// a 400 (the service doesn't silently clamp).
func parseOffsetParam(raw string) (int, error) {
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

// effectiveSinceRFC3339 converts the service-bound Duration back to the
// absolute timestamp the server used as the lower bound. Empty when no
// `since` was supplied AND the service applied its default — in that
// case the client should not pin follow-on requests to a specific ts
// because the cutoff can drift on each poll.
func effectiveSinceRFC3339(d time.Duration, now time.Time) string {
	if d <= 0 {
		return ""
	}
	return now.Add(-d).UTC().Format(time.RFC3339Nano)
}
