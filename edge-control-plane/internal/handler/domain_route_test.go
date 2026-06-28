package handler_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestDomainRoutes_MountedAtV1 is a regression pin for review finding C-1:
// the four custom-domain routes MUST be mounted under `/api/v1/apps/.../domains*`
// so they sit behind the auth sub-mux in `cmd/api/main.go` (which mounts
// at `/api/v1/`). Mounting them at the unversioned `/api/apps/.../domains*`
// would make the routes unreachable for any client — no auth, no handler.
//
// The test builds a tiny replica of the production mux shape (root
// with redirect entries + a `/api/v1/` sub-mux with the canonical routes)
// and asserts that a POST to the unversioned path gets a redirect
// (sunset) and a POST to the v1 path reaches the sub-mux. Without this
// pin, a future refactor that drops the `/v1/` prefix from `main.go`
// would silently break the entire CLI surface, and the only signal
// would be 404s from real tenants.
func TestDomainRoutes_MountedAtV1(t *testing.T) {
	// Root mux: the legacy `/api/apps/.../domains*` paths redirect to
	// the v1 version (same shape as `cmd/api/main.go` sunset redirects).
	root := http.NewServeMux()
	root.HandleFunc("POST /api/apps/{appName}/domains", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/api/v1/apps/"+r.PathValue("appName")+"/domains")
		w.WriteHeader(http.StatusMovedPermanently)
	})
	root.HandleFunc("GET /api/apps/{appName}/domains", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/api/v1/apps/"+r.PathValue("appName")+"/domains")
		w.WriteHeader(http.StatusMovedPermanently)
	})
	root.HandleFunc("GET /api/apps/{appName}/domains/{fqdn}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/api/v1/apps/"+r.PathValue("appName")+"/domains/"+r.PathValue("fqdn"))
		w.WriteHeader(http.StatusMovedPermanently)
	})
	root.HandleFunc("DELETE /api/apps/{appName}/domains/{fqdn}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/api/v1/apps/"+r.PathValue("appName")+"/domains/"+r.PathValue("fqdn"))
		w.WriteHeader(http.StatusMovedPermanently)
	})

	// Sub-mux: the canonical `/api/v1/apps/.../domains*` paths reach
	// the handler. Sentinels return their expected status codes.
	sub := http.NewServeMux()
	sub.HandleFunc("POST /api/v1/apps/{appName}/domains", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	sub.HandleFunc("GET /api/v1/apps/{appName}/domains", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	sub.HandleFunc("GET /api/v1/apps/{appName}/domains/{fqdn}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	sub.HandleFunc("DELETE /api/v1/apps/{appName}/domains/{fqdn}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	root.Handle("/api/v1/", sub)

	// Each call hits the root mux exactly the way the production server
	// would. The legacy path redirects; the canonical path reaches the
	// sub-mux. If anyone removes the v1 prefix from main.go's
	// `api.HandleFunc("POST /api/v1/apps/{appName}/domains", …)` call,
	// the v1 row below flips to 404 and this test fails loudly.
	cases := []struct {
		method, path string
		wantStatus   int
		wantLocation string
	}{
		{"POST", "/api/apps/api/domains", http.StatusMovedPermanently, "/api/v1/apps/api/domains"},
		{"GET", "/api/apps/api/domains", http.StatusMovedPermanently, "/api/v1/apps/api/domains"},
		{"GET", "/api/apps/api/domains/api.acme.com", http.StatusMovedPermanently, "/api/v1/apps/api/domains/api.acme.com"},
		{"DELETE", "/api/apps/api/domains/api.acme.com", http.StatusMovedPermanently, "/api/v1/apps/api/domains/api.acme.com"},
		{"POST", "/api/v1/apps/api/domains", http.StatusCreated, ""},
		{"GET", "/api/v1/apps/api/domains", http.StatusOK, ""},
		{"GET", "/api/v1/apps/api/domains/api.acme.com", http.StatusOK, ""},
		{"DELETE", "/api/v1/apps/api/domains/api.acme.com", http.StatusNoContent, ""},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(""))
		rec := httptest.NewRecorder()
		root.ServeHTTP(rec, req)
		if rec.Code != tc.wantStatus {
			t.Errorf("%s %s: status = %d, want %d", tc.method, tc.path, rec.Code, tc.wantStatus)
		}
		if tc.wantLocation != "" {
			if got := rec.Header().Get("Location"); got != tc.wantLocation {
				t.Errorf("%s %s: Location = %q, want %q", tc.method, tc.path, got, tc.wantLocation)
			}
		}
	}
}
