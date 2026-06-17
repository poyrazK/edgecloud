package handler

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
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
		// Distinguish sentinel errors (clang failure, edge-migrate failure) from
		// infrastructure errors. Sentinel errors carry a partial report with useful
		// analysis data — return it to the client rather than discarding it.
		switch err {
		case service.ErrEdgeMigrateFailed, service.ErrClangFailed:
			// edge-migrate or clang failed — report contains pattern analysis.
			// Return the report; status is already set in the report.
			if report != nil {
				log.Printf("migration %s: %v", err, report)
				w.Header().Set("Content-Type", "application/json")
				if report.Status == domain.MigrationStatusFailed {
					w.WriteHeader(http.StatusUnprocessableEntity) // 422 — source has untransformable patterns
				} else {
					w.WriteHeader(http.StatusOK) // partial — analysis is useful even if compilation failed
				}
				json.NewEncoder(w).Encode(report)
			} else {
				// Report is nil — infrastructure error (should not happen for sentinel errors)
				log.Printf("internal error: %v", err)
				http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
			}
		default:
			// Infrastructure error (DB, artifact store) — do not leak details.
			log.Printf("internal error: %v", err)
			http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(report); err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
	}
}

// Analyze handles GET /api/migrate/analyze — runs pattern analysis on C source
// and returns the MigrationReport without compiling or storing anything.
func (h *MigrationHandler) Analyze(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	if tenantID == "" {
		http.Error(w, `{"error":"missing tenant ID"}`, http.StatusUnauthorized)
		return
	}

	var filename, source string

	// Support multipart form upload (same as Migrate).
	if err := r.ParseMultipartForm(10 << 20); err == nil {
		nameParts := r.MultipartForm.Value["filename"]
		if len(nameParts) > 0 && nameParts[0] != "" {
			filename = nameParts[0]
		}
		fileParts := r.MultipartForm.File["file"]
		if len(fileParts) == 0 {
			http.Error(w, `{"error":"missing file field"}`, http.StatusBadRequest)
			return
		}
		f, err := fileParts[0].Open()
		if err != nil {
			http.Error(w, `{"error": "failed to open file"}`, http.StatusInternalServerError)
			return
		}
		defer f.Close()
		body, err := io.ReadAll(f)
		if err != nil {
			http.Error(w, `{"error": "failed to read file"}`, http.StatusInternalServerError)
			return
		}
		source = string(body)
	} else {
		// Fall back to query param: GET /api/migrate/analyze?source=...
		source = r.URL.Query().Get("source")
		if source == "" {
			http.Error(w, `{"error":"source query param or multipart file required"}`, http.StatusBadRequest)
			return
		}
		filename = "source.c"
	}

	report, err := h.migrationSvc.Analyze(r.Context(), tenantID, filename, source)
	if err != nil {
		switch err {
		case service.ErrEdgeMigrateFailed:
			// edge-migrate failed — return the partial report if available.
			if report != nil {
				w.Header().Set("Content-Type", "application/json")
				if report.Status == domain.MigrationStatusFailed {
					w.WriteHeader(http.StatusUnprocessableEntity)
				} else {
					w.WriteHeader(http.StatusOK)
				}
				json.NewEncoder(w).Encode(report)
			} else {
				log.Printf("internal error: %v", err)
				http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
			}
		default:
			log.Printf("internal error: %v", err)
			http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(report); err != nil {
		log.Printf("internal error: %v", err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
	}
}
