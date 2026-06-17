package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// AppHandler handles app HTTP requests.
type AppHandler struct {
	appSvc *service.AppService
}

func NewAppHandler(appSvc *service.AppService) *AppHandler {
	return &AppHandler{appSvc: appSvc}
}

// Create handles POST /api/apps/{appName} — create a new app.
func (h *AppHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	if appName == "" {
		http.Error(w, `{"error": "app name required"}`, http.StatusBadRequest)
		return
	}

	var req domain.CreateAppRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		return
	}

	app, err := h.appSvc.Create(r.Context(), tenantID, appName, &req)
	if err != nil {
		if errors.Is(err, service.ErrAppAlreadyExists) {
			http.Error(w, `{"error": "app already exists"}`, http.StatusConflict)
			return
		}
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(app)
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
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"apps": apps, "limit": limit, "offset": offset})
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
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}
	if app == nil {
		http.Error(w, `{"error": "app not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(app)
}

// Delete handles DELETE /api/apps/{appName} — delete an app and all its data.
func (h *AppHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	if appName == "" {
		http.Error(w, `{"error": "app name required"}`, http.StatusBadRequest)
		return
	}

	err := h.appSvc.Delete(r.Context(), tenantID, appName)
	if err != nil {
		if errors.Is(err, service.ErrAppNotFound) {
			http.Error(w, `{"error": "app not found"}`, http.StatusNotFound)
			return
		}
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
