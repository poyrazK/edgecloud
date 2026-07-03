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
	List(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error)
	Get(ctx context.Context, tenantID, appName string) (*domain.App, error)
	Update(ctx context.Context, tenantID, appName string, req *domain.UpdateAppRequest) (*domain.App, error)
	Delete(ctx context.Context, tenantID, appName string) error
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

// List handles GET /api/apps — list all apps for the tenant with pagination.
func (h *AppHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = min(v, 500)
		}
	}
	offset := 0
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}

	apps, err := h.appSvc.List(r.Context(), tenantID, limit, offset)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{"apps": apps, "limit": limit, "offset": offset}); err != nil {
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
