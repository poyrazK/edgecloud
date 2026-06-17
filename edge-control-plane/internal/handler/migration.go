package handler

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// MigrationHandler handles migration requests.
type MigrationHandler struct {
	migrationSvc *service.MigrationService
}

// NewMigrationHandler creates a MigrationHandler.
func NewMigrationHandler(migrationSvc *service.MigrationService) *MigrationHandler {
	return &MigrationHandler{migrationSvc: migrationSvc}
}

// Migrate handles POST /api/migrate — accepts a C source file, transforms it,
// and returns a MigrationReport.
func (h *MigrationHandler) Migrate(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	if tenantID == "" {
		http.Error(w, `{"error":"missing tenant ID"}`, http.StatusUnauthorized)
		return
	}

	if err := r.ParseMultipartForm(50 << 20); err != nil {
		http.Error(w, `{"error":"failed to parse multipart form"}`, http.StatusBadRequest)
		return
	}

	filename := r.MultipartForm.Value["filename"]
	if len(filename) == 0 || filename[0] == "" {
		http.Error(w, `{"error":"missing filename field"}`, http.StatusBadRequest)
		return
	}

	language := r.MultipartForm.Value["language"]
	if len(language) == 0 || language[0] != "c" {
		http.Error(w, `{"error":"only C language is supported"}`, http.StatusBadRequest)
		return
	}

	fileParts := r.MultipartForm.File["file"]
	if len(fileParts) == 0 {
		http.Error(w, `{"error":"missing file field"}`, http.StatusBadRequest)
		return
	}

	srcFile, err := fileParts[0].Open()
	if err != nil {
		http.Error(w, `{"error": "failed to open file"}`, http.StatusInternalServerError)
		return
	}
	defer srcFile.Close()

	source, err := io.ReadAll(srcFile)
	if err != nil {
		http.Error(w, `{"error": "failed to read file"}`, http.StatusInternalServerError)
		return
	}

	report, err := h.migrationSvc.Migrate(r.Context(), tenantID, filename[0], language[0], string(source))
	if err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
	}
}
