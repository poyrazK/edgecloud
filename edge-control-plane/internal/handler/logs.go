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

// LogListResponse is the JSON envelope returned by GET .../logs.
//
// "since" echoes the cutoff the server actually applied. The client may
// have sent `?since=<RFC3339>` (absolute) or nothing; either way the
// response tells them the effective lower bound as RFC3339 so a
// follow-on --follow can pin `since = <this> + 1ms` without parsing
// the input again.
type LogListResponse struct {
	Items []domain.LogEntry `json:"items"`
	Limit int               `json:"limit"`
	Since string            `json:"since"` // RFC3339; "" if unbounded
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
	since, err := parseSinceParam(q.Get("since"))
	if err != nil {
		httperror.BadRequestCtx(w, r, "invalid since: "+err.Error())
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

	entries, effectiveLimit, err := h.logSvc.ListByTenantApp(r.Context(), tenantID, appName, service.LogQuery{
		Since:  since,
		MinLvl: minLvl,
		Limit:  limit,
	})
	if err != nil {
		// service.ErrInvalidLevel is gated above; any error reaching
		// here is unexpected — log + 500 to keep the contract tight.
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	resp := LogListResponse{
		Items: entries,
		Limit: effectiveLimit,
		Since: effectiveSinceRFC3339(since),
	}
	w.Header().Set("Content-Type", "application/json")
	// json.NewEncoder never returns a meaningful error here — the
	// httptest.Recorder swallows it on the test side, and in production
	// the connection drop is the real failure mode, not the encode.
	_ = json.NewEncoder(w).Encode(resp)
}

// parseSinceParam converts the wire-level RFC3339 string into the
// time.Duration the service expects.
//
// Returns (0, nil) when the param is absent — the service substitutes
// its own default in that case. Returns an error for both malformed
// timestamps AND future-dated values: silently clamping a `since` in
// the future to "now" would let a client with a clock slightly ahead
// of the server request "everything from now" and get the default
// window without ever knowing their bound was ignored. A 400 surfaces
// the mistake early.
//
// Why RFC3339 (absolute) on the wire rather than a duration string:
//
//   - The CLI's `--follow` mode advances `since` by the timestamp it
//     saw last (plus 1ms). That's an absolute timestamp; sending
//     durations back and forth would require the server to know the
//     client's current time, which it doesn't.
//
//   - Timezones. "5m" is unambiguous (relative), but if a future client
//     wants `since=2026-06-24T12:00:00-07:00`, RFC3339 carries the
//     offset; a Go duration string would not.
func parseSinceParam(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, errors.New("expected RFC3339 timestamp")
	}
	// Check "is in the future" BEFORE computing time.Until. The latter
	// returns a time.Duration (int64 nanoseconds), which saturates at
	// math.MaxInt64 / 1e9 ≈ 292 years; a `since=` value a thousand
	// years out would otherwise look indistinguishable from one
	// a few seconds out, since both yield a positive Duration.
	if t.After(time.Now()) {
		return 0, errors.New("since must not be in the future")
	}
	return time.Until(t), nil
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

// effectiveSinceRFC3339 converts the service-bound Duration back to the
// absolute timestamp the server used as the lower bound. Empty when no
// `since` was supplied AND the service applied its default — in that
// case the client should not pin follow-on requests to a specific ts
// because the cutoff can drift on each poll.
func effectiveSinceRFC3339(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return time.Now().Add(-d).UTC().Format(time.RFC3339Nano)
}
