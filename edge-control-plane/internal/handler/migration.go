package handler

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// Migrate upload limits (shared by single-file /api/migrate and
// multi-file /api/migrate-tree).
const (
	// maxMigrateBodyBytes is the hard cap on the request body for
	// both POST /api/migrate (single-file) and POST /api/migrate-tree
	// (tree uploads). Larger bodies are rejected mid-stream by
	// http.MaxBytesReader.
	maxMigrateBodyBytes int64 = 50 << 20 // 50 MiB
	// parseMultipartMaxMemory is the per-part threshold above which
	// ParseMultipartForm spills a part to a temp file instead of
	// pinning it in RAM. It MUST be strictly less than
	// maxMigrateBodyBytes: if it equals the body cap, every accepted
	// upload fits in RAM and the hint is a no-op. 10 MiB is a
	// conservative default — single-file mode's `file` part is
	// already capped at MaxArtifactSize (100 MiB) downstream, and
	// tree mode's per-part cap is maxPartBytes (5 MiB), so spilling
	// anything above 10 MiB to a temp file is always correct.
	parseMultipartMaxMemory int64 = 10 << 20 // 10 MiB
	// maxTreeFiles is the cap on the number of files in a single tree
	// upload. Larger trees are rejected with 400.
	maxTreeFiles = 256
	// maxPartBytes is the per-file cap inside a tree upload. A
	// single 49 MiB file could otherwise consume the entire body
	// budget (and the server's memory) before the per-file manifest
	// mismatch check at line ~202 ever ran. Generous default
	// (median .c file is ~10 KiB; even large generated code is well
	// under 5 MiB) but small enough to prevent memory abuse.
	maxPartBytes int64 = 5 << 20 // 5 MiB
)

// treeUploadExts is the set of file extensions accepted in a tree
// upload. C: `.c`/`.h` (M2). Rust: `.rs` (M3). Other extensions are
// silently skipped — neither accepted nor rejected — so a tarball
// with a `Makefile` or `Cargo.toml` still works.
var treeUploadExts = map[string]bool{
	".c":  true,
	".h":  true,
	".rs": true,
}

// isClientMigrationError reports whether `err` from the migration
// service is a request-level failure (the request was syntactically
// valid but the source didn't transform / compile / fit). The
// handler maps these to HTTP 422 and emits the structured report
// body so the caller can read the per-pattern error detail. All
// other errors are server-level (DB outage, IO, etc.) and map to 500.
func isClientMigrationError(err error) bool {
	return errors.Is(err, service.ErrMigrateTreeFailed) ||
		errors.Is(err, service.ErrMigrationFailed) ||
		errors.Is(err, service.ErrEdgeMigrateFailed) ||
		errors.Is(err, service.ErrClangFailed) ||
		errors.Is(err, service.ErrRustcFailed) ||
		errors.Is(err, service.ErrWasmToolsFailed) ||
		errors.Is(err, service.ErrCargoBuildFailed)
}

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
		httperror.UnauthorizedCtx(w, r, "missing tenant ID")
		return
	}

	// Cap the request body up front so a malicious caller can't pin
	// a multi-GiB upload mid-stream. The MigrateTree handler applies
	// the same guard with the same 50 MiB limit (see below); the
	// single-file Migrate path previously only relied on
	// ParseMultipartForm's "max memory" hint, which doesn't cap the
	// underlying body read.
	r.Body = http.MaxBytesReader(w, r.Body, maxMigrateBodyBytes)

	if err := r.ParseMultipartForm(parseMultipartMaxMemory); err != nil {
		if httperror.MaxBodyBytes(w, err, http.StatusRequestEntityTooLarge, "request body too large") {
			return
		}
		httperror.BadRequestCtx(w, r, "failed to parse multipart form")
		return
	}

	filename := r.MultipartForm.Value["filename"]
	if len(filename) == 0 || filename[0] == "" {
		httperror.BadRequestCtx(w, r, "missing filename field")
		return
	}
	// Reject path-traversal early — derived app_name is what actually gets
	// written to the DB and used in the registry path. The service has a
	// defense-in-depth check; this one gives a clear 400 to the client.
	if containsPathTraversal(strings.TrimSuffix(filename[0], ".c")) {
		httperror.BadRequestCtx(w, r, "filename must not contain path-traversal characters")
		return
	}

	language := r.MultipartForm.Value["language"]
	if len(language) == 0 || (language[0] != "c" && language[0] != "rust") {
		http.Error(w, `{"error":"only c and rust are supported"}`, http.StatusBadRequest)
		return
	}

	fileParts := r.MultipartForm.File["file"]
	if len(fileParts) == 0 {
		httperror.BadRequestCtx(w, r, "missing file field")
		return
	}

	srcFile, err := fileParts[0].Open()
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}
	defer func() {
		if err := srcFile.Close(); err != nil {
			log.Printf("MigrateFile: failed to close uploaded file: %v", err)
		}
	}()

	source, err := io.ReadAll(srcFile)
	if err != nil {
		httperror.InternalErrorCtx(w, r)
		return
	}

	report, err := h.migrationSvc.Migrate(r.Context(), tenantID, filename[0], language[0], string(source))
	if err != nil {
		if isClientMigrationError(err) {
			// Source-level failure (didn't transform, didn't compile,
			// etc.). Surface as 422 with the structured report body
			// so the caller can read the per-pattern error detail.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			if encErr := json.NewEncoder(w).Encode(report); encErr != nil {
				log.Printf("Migrate encode error after 422: %v", encErr)
			}
			return
		}
		log.Printf("Migrate internal error: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		httperror.InternalErrorCtx(w, r)
	}
}

// MigrateTree handles POST /api/migrate-tree. Accepts a multi-file
// source tree (`language: c` or `language: rust`) in two wire formats:
//
//   - Variant A — multipart parts: one `file` per source file, plus
//     a `tree` JSON manifest `{"files": [...]}`. Each `file` part's
//     filename must match an entry in the manifest.
//
//   - Variant B — zip archive: a single `tree` part with
//     `Content-Type: application/zip`. Zip entries are the source of
//     truth; the `tree` JSON manifest is not used.
//
// Both variants produce a `domain.TreeMigrationReport` JSON response.
// See `edge-migrate/docs/design.md` §6.1.2.
func (h *MigrationHandler) MigrateTree(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	if tenantID == "" {
		http.Error(w, `{"error":"missing tenant ID"}`, http.StatusUnauthorized)
		return
	}

	// Cap the request body up front so a malicious caller can't pin
	// a 10 GB upload mid-stream.
	r.Body = http.MaxBytesReader(w, r.Body, maxMigrateBodyBytes)

	if err := r.ParseMultipartForm(parseMultipartMaxMemory); err != nil {
		if httperror.MaxBodyBytes(w, err, http.StatusRequestEntityTooLarge, "request body too large") {
			return
		}
		http.Error(w, `{"error":"failed to parse multipart form"}`, http.StatusBadRequest)
		return
	}

	// app_name is required for both variants.
	appName := r.MultipartForm.Value["app_name"]
	if len(appName) == 0 || appName[0] == "" {
		http.Error(w, `{"error":"missing app_name field"}`, http.StatusBadRequest)
		return
	}
	if !service.IsValidDeploymentAppName(appName[0]) {
		http.Error(w, `{"error":"invalid app_name"}`, http.StatusBadRequest)
		return
	}

	// Language gate. M2 accepted only C; M3 widens to c + rust,
	// but tree mode is C-only (issue #415): the cargo-based Rust
	// pipeline builds a single-file Cargo project, while a tree
	// submission would need a synthesized Cargo.toml + multi-file
	// src/lib.rs wrapper. Single-file Rust is routed through
	// POST /api/v1/migrate instead.
	language := r.MultipartForm.Value["language"]
	if len(language) == 0 || (language[0] != "c" && language[0] != "rust") {
		http.Error(w, `{"error":"only c and rust are supported"}`, http.StatusBadRequest)
		return
	}
	if language[0] == "rust" {
		http.Error(w, `{"error":"rust tree-mode migration is not supported; submit a single-file project via POST /api/v1/migrate"}`, http.StatusBadRequest)
		return
	}

	// Detect variant: if `tree` is a file part, it's variant B (zip).
	// Otherwise it's variant A (multipart parts + JSON manifest).
	treeFiles := r.MultipartForm.File["tree"]
	var entries []domain.FileEntry
	if len(treeFiles) > 0 {
		// Variant B: zip.
		e, err := readZipEntries(treeFiles[0], maxTreeFiles, maxMigrateBodyBytes)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
			return
		}
		entries = e
	} else {
		// Variant A: multipart parts + JSON manifest.
		treeManifest := r.MultipartForm.Value["tree"]
		if len(treeManifest) == 0 {
			http.Error(w, `{"error":"missing tree manifest or zip"}`, http.StatusBadRequest)
			return
		}
		var manifest struct {
			Files []string `json:"files"`
		}
		if err := json.Unmarshal([]byte(treeManifest[0]), &manifest); err != nil {
			http.Error(w, `{"error":"invalid tree manifest JSON"}`, http.StatusBadRequest)
			return
		}
		fileParts := r.MultipartForm.File["file"]
		if len(fileParts) == 0 {
			http.Error(w, `{"error":"missing file parts"}`, http.StatusBadRequest)
			return
		}
		if len(manifest.Files) != len(fileParts) {
			http.Error(w, `{"error":"manifest mismatch: file count differs"}`, http.StatusBadRequest)
			return
		}
		if len(manifest.Files) > maxTreeFiles {
			http.Error(w, fmt.Sprintf(`{"error":"too many files: max %d"}`, maxTreeFiles), http.StatusBadRequest)
			return
		}
		// Build path→part map, reject duplicates and bad paths.
		partByName := make(map[string]*multipart.FileHeader, len(fileParts))
		for _, fp := range fileParts {
			if !isSafeFilePath(fp.Filename) {
				http.Error(w, fmt.Sprintf(`{"error":"unsafe file path: %q"}`, fp.Filename), http.StatusBadRequest)
				return
			}
			// Filter by language-appropriate extension. Mirrors the
			// zip variant's `treeUploadExts` check at line 291. Without
			// this, a user uploading `language: rust` with a `foo.c`
			// part would have those bytes passed to `rustc` and fail
			// opaquely at compile time.
			ext := strings.ToLower(filepath.Ext(fp.Filename))
			if !treeUploadExts[ext] {
				http.Error(w, fmt.Sprintf(
					`{"error":"unsupported file extension in multipart part: %q (allowed: .c .h .rs)"}`,
					fp.Filename,
				), http.StatusBadRequest)
				return
			}
			partByName[normalizeFileName(fp.Filename)] = fp
		}
		for _, p := range manifest.Files {
			if !isSafeFilePath(p) {
				http.Error(w, fmt.Sprintf(`{"error":"unsafe manifest path: %q"}`, p), http.StatusBadRequest)
				return
			}
			// Same extension filter on the manifest side. A
			// `language: rust` upload with a `foo.c` entry in the
			// manifest must be rejected here, not at compile time.
			ext := strings.ToLower(filepath.Ext(p))
			if !treeUploadExts[ext] {
				http.Error(w, fmt.Sprintf(
					`{"error":"unsupported file extension in manifest: %q (allowed: .c .h .rs)"}`,
					p,
				), http.StatusBadRequest)
				return
			}
			part := partByName[normalizeFileName(p)]
			if part == nil {
				http.Error(w, fmt.Sprintf(`{"error":"manifest mismatch: missing file for %q"}`, p), http.StatusBadRequest)
				return
			}
			src, err := part.Open()
			if err != nil {
				http.Error(w, `{"error":"failed to open file part"}`, http.StatusInternalServerError)
				return
			}
			// LimitReader at maxPartBytes+1 lets us detect "exactly
			// the cap" vs "exceeded the cap" without reading the
			// whole oversized blob into memory.
			body, err := io.ReadAll(io.LimitReader(src, maxPartBytes+1))
			if closeErr := src.Close(); closeErr != nil {
				log.Printf("MigrateTree: failed to close file part: %v", closeErr)
			}
			if err != nil {
				http.Error(w, `{"error":"failed to read file part"}`, http.StatusInternalServerError)
				return
			}
			if int64(len(body)) > maxPartBytes {
				http.Error(w, fmt.Sprintf(
					`{"error":"file part exceeds %d bytes"}`, maxPartBytes,
				), http.StatusRequestEntityTooLarge)
				return
			}
			entries = append(entries, domain.FileEntry{Path: p, Source: string(body)})
		}
	}

	report, err := h.migrationSvc.MigrateTree(r.Context(), tenantID, appName[0], language[0], entries)
	if err != nil {
		if isClientMigrationError(err) {
			// Source-level failure (a file didn't transform, the
			// final compile failed, the artifact is oversized, etc.).
			// Surface as 422 with the structured report body so the
			// caller can read the per-file / tree-level error detail.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			if encErr := json.NewEncoder(w).Encode(report); encErr != nil {
				log.Printf("MigrateTree encode error after 422: %v", encErr)
			}
			return
		}
		log.Printf("MigrateTree internal error: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		log.Printf("MigrateTree encode error: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
	}
}

// readZipEntries opens the uploaded zip, validates each entry name
// (zip-slip protection), and returns the supported source files as
// FileEntry slices. The accepted extensions live in `treeUploadExts`
// (C: `.c`/`.h`, Rust: `.rs`). Caps the number of files, per-entry
// size, and total decompressed size.
func readZipEntries(header *multipart.FileHeader, maxFiles int, maxBody int64) ([]domain.FileEntry, error) {
	src, err := header.Open()
	if err != nil {
		return nil, fmt.Errorf("opening zip part: %w", err)
	}
	defer func() {
		if closeErr := src.Close(); closeErr != nil {
			log.Printf("readZipEntries: failed to close file: %v", closeErr)
		}
	}()

	zr, err := zip.NewReader(src, header.Size)
	if err != nil {
		return nil, fmt.Errorf("reading zip: %w", err)
	}
	var entries []domain.FileEntry
	var total int64
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		// Reject any name that's not a relative, safe source-tree path.
		name := strings.ReplaceAll(f.Name, "\\", "/")
		if !isSafeFilePath(name) {
			return nil, fmt.Errorf("unsafe zip entry: %q", f.Name)
		}
		ext := strings.ToLower(filepath.Ext(name))
		if !treeUploadExts[ext] {
			continue
		}
		if len(entries) >= maxFiles {
			return nil, fmt.Errorf("too many files: max %d", maxFiles)
		}
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("opening zip entry %q: %w", f.Name, err)
		}
		// Per-entry cap. The total cap is enforced via the running
		// `total` accumulator below; the per-entry cap protects
		// against a single zip-bomb entry consuming the whole
		// budget before we ever reach the total check.
		body, err := io.ReadAll(io.LimitReader(rc, maxPartBytes+1))
		if closeErr := rc.Close(); closeErr != nil {
			return nil, fmt.Errorf("closing zip entry %q: %w", f.Name, closeErr)
		}
		if err != nil {
			return nil, fmt.Errorf("reading zip entry %q: %w", f.Name, err)
		}
		if int64(len(body)) > maxPartBytes {
			return nil, fmt.Errorf("zip entry %q exceeds %d bytes", f.Name, maxPartBytes)
		}
		total += int64(len(body))
		if total > maxBody {
			return nil, fmt.Errorf("decompressed zip too large")
		}
		entries = append(entries, domain.FileEntry{Path: name, Source: string(body)})
	}
	return entries, nil
}

// isSafeFilePath rejects absolute paths, parent-directory escapes,
// backslashes, and Windows drive letters — used by both the
// multipart manifest path validator and the zip entry name validator.
func isSafeFilePath(p string) bool {
	if p == "" {
		return false
	}
	if strings.HasPrefix(p, "/") || strings.HasPrefix(p, "\\") {
		return false
	}
	if strings.Contains(p, "\\") {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(p))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return false
	}
	// Windows drive letter: "C:" or "C:foo"
	if len(clean) >= 2 && clean[1] == ':' {
		return false
	}
	return true
}

// normalizeFileName normalizes a multipart part filename by
// stripping the directory (e.g. "src/main.c" → "main.c"). The
// comparison then becomes name-based. The handler matches the full
// path on the manifest side; the part filename is just the basename
// per HTTP convention.
func normalizeFileName(s string) string {
	s = strings.ReplaceAll(s, "\\", "/")
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}
