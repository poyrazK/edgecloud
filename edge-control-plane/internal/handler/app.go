package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// AppServiceInterface is the subset of *service.AppService used by AppHandler.
type AppServiceInterface interface {
	Create(ctx context.Context, tenantID, appName string, req *domain.CreateAppRequest) (*domain.App, error)
	// List is the keyset-paginated apps endpoint (issue #58). The
	// afterCursor is the opaque value previously returned as
	// NextCursor; empty means "first page". Returns an AppListPage
	// with the page slice, the effective limit, and NextCursor
	// (nil on the final page).
	List(ctx context.Context, tenantID string, limit int, afterCursor string) (*service.AppListPage, error)
	Get(ctx context.Context, tenantID, appName string) (*domain.App, error)
	Update(ctx context.Context, tenantID, appName string, req *domain.UpdateAppRequest) (*domain.App, error)
	Delete(ctx context.Context, tenantID, appName string) error
	// L4 port accessors (issue #548). AllocateL4Port is the
	// "assign a port if one isn't yet set" entry point used by the
	// ingress on first TCP heartbeat; GetL4Port is the read-only
	// fetch used by `GET /api/v1/apps/{appName}/l4-port`. The
	// handler treats both as tenant-scoped (they take tenantID from
	// the auth middleware).
	GetL4Port(ctx context.Context, tenantID, appName string) (uint16, error)
	AllocateL4Port(ctx context.Context, tenantID, appName string) (uint16, error)
}

// AppHandler handles app HTTP requests.
type AppHandler struct {
	appSvc AppServiceInterface
}

func NewAppHandler(appSvc AppServiceInterface) *AppHandler {
	return &AppHandler{appSvc: appSvc}
}

// Create handles POST /api/apps/{appName} — create a new app.
func (h *AppHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	if appName == "" {
		httperror.BadRequestCtx(w, r, "app name required")
		return
	}

	var req domain.CreateAppRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}

	app, err := h.appSvc.Create(r.Context(), tenantID, appName, &req)
	if err != nil {
		if errors.Is(err, service.ErrAppAlreadyExists) {
			httperror.ConflictCtx(w, r, "app already exists")
			return
		}
		if errors.Is(err, service.ErrMaxAppsQuotaExceeded) {
			httperror.QuotaExceededCtx(w, r, "max apps quota exceeded")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(app); err != nil {
		log.Printf("Create app: failed to encode response: %v", err)
	}
	auditRecord(r, "create", "app", appName, "app "+appName+" created", "success")
}

// appListResponse is the JSON envelope for GET /api/v1/apps.
// Issue #58 — hard-cut to cursor pagination: no `offset` and no
// `total`. `next_cursor` is omitted on the final page (the field's
// `omitempty` tag handles that). Mirrors the cursor-only shape at
// internal/handler/webhook.go::webhookDeliveriesResponse.
type appListResponse struct {
	Apps       []domain.App `json:"apps"`
	Limit      int          `json:"limit"`
	NextCursor *string      `json:"next_cursor,omitempty"`
}

// appsLimitCap is the maximum `?limit=` value the handler accepts
// (issue #58). Mirrors the handler/webhook.go precedent (200 there,
// 500 here) — apps surface is broader so a higher cap is reasonable.
// A request that asks for more is silently clamped, NOT rejected —
// the response `limit` field reports the effective value.
const appsLimitCap = 500

// appsDefaultLimit is the implicit page size when the client does
// not supply `?limit=`. Matches the previous default (50).
const appsDefaultLimit = 50

// List handles GET /api/v1/apps — list apps for the tenant with
// cursor pagination. Issue #58.
//
// Query parameters:
//
//	?limit=<int>   page size; default 50, max 500. Clamped silently
//	               (the response `limit` field reports the effective value).
//	?cursor=<str>  opaque keyset cursor; pass back the previous page's
//	               next_cursor to fetch the next page. Absent/empty = first page.
//
// The legacy `?offset=` parameter is intentionally NOT accepted:
// issue #58 hard-cuts apps to cursor pagination, so any request
// that supplies both `?cursor=` and `?offset=` returns 400 (the
// "mutually exclusive" gate below). A request that supplies ONLY
// `?offset=` silently ignores it — that's a deliberate compat
// choice for clients that haven't migrated yet (no error, but they
// always get page 1).
//
// Status codes:
//
//	200  envelope {apps, limit, next_cursor?}
//	400  invalid cursor / unsupported cursor version / cursor+offset supplied
//	500  unexpected
func (h *AppHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	q := r.URL.Query()
	// Defensive: the endpoint does not advertise `offset`, but reject
	// any request that supplies one alongside a cursor so a confused
	// client cannot trigger ambiguous SQL branches. Mirrors the
	// handler/webhook.go:179-185 idiom.
	if q.Has("cursor") && q.Has("offset") {
		httperror.BadRequestCtx(w, r, "cursor and offset are mutually exclusive")
		return
	}

	limit := appsDefaultLimit
	if l := q.Get("limit"); l != "" {
		v, err := strconv.Atoi(l)
		if err != nil || v <= 0 {
			httperror.BadRequestCtx(w, r, "invalid limit")
			return
		}
		limit = min(v, appsLimitCap)
	}

	page, err := h.appSvc.List(r.Context(), tenantID, limit, q.Get("cursor"))
	if err != nil {
		// Typed cursor errors → 400 with a generic message; a
		// structured log.Printf preserves the operator signal.
		// Mirrors handler/webhook.go:199-207.
		if errors.Is(err, service.ErrInvalidAppCursor) || errors.Is(err, service.ErrUnsupportedAppCursorVersion) {
			log.Printf("invalid app cursor (tenant=%s): %v", tenantID, err)
			httperror.BadRequestCtx(w, r, "invalid cursor")
			return
		}
		if errors.Is(err, service.ErrInvalidLimit) {
			httperror.BadRequestCtx(w, r, "invalid limit")
			return
		}
		log.Printf("internal error listing apps (tenant=%s): %v", tenantID, err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	resp := appListResponse{
		Apps:       page.Apps,
		Limit:      page.Limit,
		NextCursor: page.NextCursor,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("List apps: failed to encode response: %v", err)
	}
}

// Get handles GET /api/apps/{appName} — get a specific app.
func (h *AppHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	if appName == "" {
		http.Error(w, `{"error": "app name required"}`, http.StatusBadRequest)
		return
	}

	app, err := h.appSvc.Get(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if app == nil {
		httperror.NotFoundCtx(w, r, "app not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(app); err != nil {
		log.Printf("Get app: failed to encode response: %v", err)
	}
}

// Update handles PUT /api/v1/apps/{appName} — update mutable fields of an app.
func (h *AppHandler) Update(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	if appName == "" {
		httperror.BadRequestCtx(w, r, "app name required")
		return
	}

	var req domain.UpdateAppRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}

	app, err := h.appSvc.Update(r.Context(), tenantID, appName, &req)
	if err != nil {
		if errors.Is(err, service.ErrAppNotFound) {
			httperror.NotFoundCtx(w, r, "app not found")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(app); err != nil {
		log.Printf("Update app: failed to encode response: %v", err)
	}
	auditRecord(r, "update", "app", appName, "app "+appName+" updated", "success")
}

// Delete handles DELETE /api/apps/{appName} — delete an app and all its data.
func (h *AppHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	if appName == "" {
		httperror.BadRequestCtx(w, r, "app name required")
		return
	}

	err := h.appSvc.Delete(r.Context(), tenantID, appName)
	if err != nil {
		if errors.Is(err, service.ErrAppNotFound) {
			httperror.NotFoundCtx(w, r, "app not found")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	auditRecord(r, "delete", "app", appName, "app "+appName+" deleted", "success")
}

// l4PortResponse is the wire shape for GET /api/v1/apps/{appName}/l4-port
// (issue #548). Typed (vs anonymous map) so the contract is explicit and
// the test asserts against a struct. `public_port` is the ingress-side
// port the tenant should connect to for raw-TCP traffic to the named
// app; `0` is not a valid public port, so any non-error response
// carries a non-zero value.
type l4PortResponse struct {
	PublicPort uint16 `json:"public_port"`
}

// GetL4Port handles GET /api/v1/apps/{appName}/l4-port — returns the
// allocated L4/TCP public port for the named app (issue #548).
//
// On a fresh TCP-app heartbeat the ingress calls
// `AllocateL4Port` to atomically assign a port from the
// configured L4 range and persist it on the apps row. Subsequent
// heartbeats (and any external observer, including a tenant who
// wants to know where to point their client) read it back via
// this endpoint.
//
// Status codes:
//   - 200: `{ "public_port": <uint16> }`.
//   - 404: app not found, OR app exists but has no allocated L4
//     port (e.g. it's an HTTP-only app, or the L4 port was never
//     allocated because no TCP heartbeat has been processed yet).
//     Both surface as 404 because the caller has nothing useful
//     to do in either case.
//   - 500: anything else.
//
// Tenant-authenticated (mirrors the rest of /api/v1/apps/*).
func (h *AppHandler) GetL4Port(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	if appName == "" {
		httperror.BadRequestCtx(w, r, "app name required")
		return
	}

	port, err := h.appSvc.GetL4Port(r.Context(), tenantID, appName)
	if err != nil {
		if errors.Is(err, service.ErrAppNotFound) {
			httperror.NotFoundCtx(w, r, "app not found")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if port == 0 {
		// App exists but no port is allocated yet (HTTP-only app
		// or pre-allocation). 404 because the caller has nothing
		// to do with an unallocated port — they'd just retry.
		httperror.NotFoundCtx(w, r, "no L4 public port allocated for this app")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(l4PortResponse{PublicPort: port}); err != nil {
		log.Printf("GetL4Port: failed to encode response: %v", err)
	}
}

// AllocateL4Port handles POST /api/v1/apps/{appName}/l4-port —
// atomically allocates an L4/TCP public port for the named app
// if one is not already allocated (issue #548). Idempotent: calling
// twice returns the same port without allocating a second one.
//
// This endpoint exists primarily so the ingress can request a
// port allocation server-side via the authenticated API rather
// than triggering the implicit allocation path on first TCP
// heartbeat. The CLI also uses it as a discovery helper
// (`edge tcp-info hello-tcp` — a follow-up).
//
// Status codes:
//   - 200: `{ "public_port": <uint16> }`.
//   - 404: app not found.
//   - 409: port range exhausted (L4_PORT_RANGE_START..END fully
//     allocated); caller should retry after widening the range.
//   - 500: anything else.
//
// Tenant-authenticated.
func (h *AppHandler) AllocateL4Port(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	if appName == "" {
		httperror.BadRequestCtx(w, r, "app name required")
		return
	}

	port, err := h.appSvc.AllocateL4Port(r.Context(), tenantID, appName)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrAppNotFound):
			httperror.NotFoundCtx(w, r, "app not found")
		case errors.Is(err, service.ErrL4PortRangeExhausted):
			httperror.ConflictCtx(w, r, "L4 public port range exhausted; widen L4_PORT_RANGE_END or reduce concurrent L4 apps")
		default:
			log.Printf("internal error: %v", err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(l4PortResponse{PublicPort: port}); err != nil {
		log.Printf("AllocateL4Port: failed to encode response: %v", err)
	}
}

// GetL4PortInternal handles GET /api/v1/internal/l4-port/{tenantID}/{appName} —
// the ingress-to-CP read endpoint for L4 public-port assignments
// (issue #548). Mounted under InternalAuth (shared-secret header,
// same as the other /api/v1/internal/* endpoints the ingress
// polls), this is what the ingress `L4PortCache` (see
// edge-ingress/src/l4_cache.rs, follow-up) refreshes from every
// QUOTA_FETCH_INTERVAL (~30s).
//
// Unlike the tenant-authenticated GET /api/v1/apps/{appName}/l4-port,
// the tenant comes from the URL path — the ingress is a
// service-to-service caller and does not carry an API key.
//
// Status codes:
//   - 200: `{ "public_port": <uint16> }`.
//   - 400: invalid app_name or tenant_id.
//   - 404: app not found, OR app exists but has no allocated L4
//     port (HTTP-only app, or no TCP heartbeat yet). The ingress
//     treats both as a cache miss and falls back to the
//     ingress-local L4PortPool.
//   - 500: anything else.
func (h *AppHandler) GetL4PortInternal(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	appName := r.PathValue("appName")
	if !validateAppName(w, appName) {
		return
	}
	if tenantID == "" || containsPathTraversal(tenantID) {
		http.Error(w, `{"error": "invalid tenant id"}`, http.StatusBadRequest)
		return
	}

	port, err := h.appSvc.GetL4Port(r.Context(), tenantID, appName)
	if err != nil {
		if errors.Is(err, service.ErrAppNotFound) {
			httperror.NotFoundCtx(w, r, "app not found")
			return
		}
		log.Printf("GetL4PortInternal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if port == 0 {
		// App exists but no port allocated. 404 lets the ingress
		// fall back to its local L4PortPool + write-back (so a
		// future TCP heartbeat will pick up the assignment).
		httperror.NotFoundCtx(w, r, "no L4 public port allocated for this app")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(l4PortResponse{PublicPort: port}); err != nil {
		log.Printf("GetL4PortInternal: failed to encode response: %v", err)
	}
}
