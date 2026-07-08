package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// mockAppTargetLookup is the minimum service.AppTargetLookup implementation
// needed by AppIngress. Kept narrow so adding a method to the real
// service doesn't ripple into this test.
type mockAppTargetLookup struct {
	target *domain.AppTarget
	err    error
	// lastTenant / lastApp record the arguments the handler passed to
	// GetAppTarget so tests can assert the tenant filter is applied
	// correctly.
	lastTenant string
	lastApp    string
}

func (m *mockAppTargetLookup) GetAppTarget(_ context.Context, tenantID, appName string) (*domain.AppTarget, error) {
	m.lastTenant = tenantID
	m.lastApp = appName
	return m.target, m.err
}

// newAppIngressMux wires a single route through a real *http.ServeMux so
// r.PathValue("appName") is populated the same way it is in production.
// The deploymentSvc is nil because AppIngress never calls into it —
// the test exists to lock the AppIngress contract, not the deployment
// service contract.
func newAppIngressMux(lookup *mockAppTargetLookup) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/apps/{appName}/ingress", NewDeploymentHandler(nil, lookup, nil, nil, "").AppIngress)
	return mux
}

// ---------------------------------------------------------------------------
// AppIngress — 200 (found)
// ---------------------------------------------------------------------------

func TestAppIngress_Found_Returns200AndFullTarget(t *testing.T) {
	want := &domain.AppTarget{
		AppName:    "myapp",
		TenantID:   "t_test",
		WorkerID:   "w_fra_abc",
		Region:     "fra",
		WorkerAddr: "203.0.113.10",
		Port:       8081,
	}
	lookup := &mockAppTargetLookup{target: want}
	mux := newAppIngressMux(lookup)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/ingress", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["ready"] != true {
		t.Errorf("ready = %v, want true", got["ready"])
	}
	if got["app_name"] != "myapp" {
		t.Errorf("app_name = %v, want myapp", got["app_name"])
	}
	if got["tenant_id"] != "t_test" {
		t.Errorf("tenant_id = %v, want t_test", got["tenant_id"])
	}
	if got["worker_addr"] != "203.0.113.10" {
		t.Errorf("worker_addr = %v, want 203.0.113.10", got["worker_addr"])
	}
	// JSON numbers decode to float64.
	if port, _ := got["port"].(float64); int(port) != 8081 {
		t.Errorf("port = %v, want 8081", got["port"])
	}
	// The handler must propagate the tenant id from the auth context, not
	// from the URL — this is what keeps cross-tenant lookup from working.
	if lookup.lastTenant != "t_test" {
		t.Errorf("GetAppTarget called with tenant %q, want t_test", lookup.lastTenant)
	}
	if lookup.lastApp != "myapp" {
		t.Errorf("GetAppTarget called with app %q, want myapp", lookup.lastApp)
	}
}

// ---------------------------------------------------------------------------
// AppIngress — 404 (no running target for this tenant)
// ---------------------------------------------------------------------------

func TestAppIngress_NotFound_Returns404AndStructuredBody(t *testing.T) {
	lookup := &mockAppTargetLookup{target: nil}
	mux := newAppIngressMux(lookup)

	req := httptest.NewRequest("GET", "/api/v1/apps/missing/ingress", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["ready"] != false {
		t.Errorf("ready = %v, want false", got["ready"])
	}
	if got["app_name"] != "missing" {
		t.Errorf("app_name = %v, want missing", got["app_name"])
	}
	if _, ok := got["reason"]; !ok {
		t.Errorf("reason field missing from 404 body: %s", rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// AppIngress — 500 (service error)
// ---------------------------------------------------------------------------

func TestAppIngress_ServiceError_Returns500(t *testing.T) {
	lookup := &mockAppTargetLookup{err: errors.New("db unreachable")}
	mux := newAppIngressMux(lookup)

	req := httptest.NewRequest("GET", "/api/v1/apps/myapp/ingress", nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body: %s", rr.Code, rr.Body.String())
	}
}

// ---------------------------------------------------------------------------
// AppIngress — 400 (path traversal in appName)
// ---------------------------------------------------------------------------

// TestAppIngress_PathTraversal_Returns400 exercises the 400 path the
// handler can actually trigger. Note that Go's `http.ServeMux` does
// its own path cleaning BEFORE the handler is called:
//   - A request whose `URL.RawPath` is empty (i.e. the decoded path
//     equals the raw path) and that contains a literal `..` segment
//     gets collapsed by the mux cleaner into a 307 redirect to the
//     normalized URL — the handler never sees it.
//   - A `/` inside what would be the `{appName}` segment makes the
//     pattern not match at all (mux returns 404 — the handler never
//     sees it).
//   - An empty `{appName}` is unreachable: `GET /api/apps//ingress`
//     redirects to `/api/apps/ingress/` via the mux's trailing-slash
//     cleaner, so `r.PathValue("appName")` is never empty in practice.
//
// What survives mux cleaning and reaches the handler:
//   - Literal backslashes (POSIX URL parsers pass them through).
//   - Percent-encoded forms (`%2E%2E`, `%2F`) — these survive because
//     when `URL.RawPath` is set and differs from `URL.Path`, the mux
//     cleaner uses `RawPath`, where `%2E%2E` is a literal 6-char token
//     with no `..` to collapse. The decoded `..` only materializes
//     when `r.PathValue` decodes the matched segment.
func TestAppIngress_PathTraversal_Returns400(t *testing.T) {
	tests := []struct {
		name    string
		appName string // path segment as it appears in the URL
	}{
		// Backslash is a path-separator on Windows; POSIX URL parsers
		// pass it through verbatim. The handler's containsPathTraversal
		// explicitly rejects it.
		{"backslash", `foo\bar`},
		// %2E%2E — see block comment above for why the mux cleaner
		// leaves this alone and the handler sees the decoded "..".
		{"percent-encoded dots", "%2E%2E"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookup := &mockAppTargetLookup{}
			mux := newAppIngressMux(lookup)
			url := "/api/v1/apps/" + tt.appName + "/ingress"
			req := httptest.NewRequest("GET", url, nil)
			req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
			if lookup.lastApp != "" {
				t.Errorf("GetAppTarget should not have been called for traversal appName, got %q", lookup.lastApp)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseRegions — pure-function unit tests for the `?regions=` query parser.
// Pulled out from the deploy handler so the parsing contract is testable
// without standing up a DeploymentService. The handler-level 400 tests
// below exercise the parser indirectly through the real HTTP path.
// ---------------------------------------------------------------------------

func TestParseRegions(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		// Empty / missing → nil, no error. The service layer
		// treats nil as "use the control plane's default region".
		{"empty string", "", nil, false},
		{"whitespace only", "   ", nil, false},
		{"commas only", ",,,", nil, false},

		// Single value, happy paths.
		{"single region", "us-east", []string{"us-east"}, false},
		{"single region with surrounding spaces", "  eu-west  ", []string{"eu-west"}, false},

		// Multiple values, order preserved.
		{"two regions", "us-east,eu-west", []string{"us-east", "eu-west"}, false},
		{"three regions with spaces", "us-east, eu-west , ap-south", []string{"us-east", "eu-west", "ap-south"}, false},

		// Dedup (first-seen wins).
		{"dupe collapsed", "us-east,us-east,eu-west", []string{"us-east", "eu-west"}, false},
		{"dupe with spaces", "us-east, us-east ,eu-west", []string{"us-east", "eu-west"}, false},

		// Empty entries are dropped, not surfaced as `""`.
		{"leading empty", ",us-east,eu-west", []string{"us-east", "eu-west"}, false},
		{"trailing empty", "us-east,eu-west,", []string{"us-east", "eu-west"}, false},
		{"interleaved empties", "us-east,,eu-west", []string{"us-east", "eu-west"}, false},

		// Invalid: anything outside `[a-z0-9-]{1,64}`. Each entry is
		// validated at the handler boundary so a malformed value
		// never reaches the service layer.
		{"uppercase rejected", "US-EAST", nil, true},
		{"underscore rejected", "us_east", nil, true},
		{"dot rejected", "us.east", nil, true},
		{"slash rejected", "us/east", nil, true},
		{"space inside rejected", "us east", nil, true},
		// Empty entries (consecutive commas) are dropped, not rejected —
		// the parser trims first, then validates. A list of only
		// empties (`",,,"`) collapses to nil.
		{"empties dropped, rest kept", "us-east,,eu-west", []string{"us-east", "eu-west"}, false},
		// 65 chars = over the 64-char cap.
		{"too long rejected", "a2345678901234567890123456789012345678901234567890123456789012345", nil, true},

		// Cap (MaxRegionsPerDeployment = 16). The cap is enforced AFTER
		// dedupe, so duplicates must not count toward the limit.
		{"at cap (16 unique)", "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p",
			[]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"}, false},
		{"over cap (17 unique)", "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q", nil, true},
		{"dupes not counted (17 copies of a)", strings.Repeat("a,", 16) + "a", []string{"a"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRegions(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseRegions(%q) err = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !equalSlices(got, tt.want) {
				t.Errorf("parseRegions(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Deploy handler — 400 paths (invalid regions). The 200 path is covered
// by service-level tests in service/deployment_regions_test.go, which
// exercise the full Deploy → ActivateDeployment chain end-to-end with a
// real service and a recording mock publisher.
// ---------------------------------------------------------------------------

// newDeployMux wires a single route through a real *http.ServeMux. The
// deploymentSvc is nil because these tests only cover 400 paths that
// short-circuit before the service is called; a happy-path test would
// panic on the nil pointer deref.
func newDeployMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/deploy/{appName}", NewDeploymentHandler(nil, nil, nil, nil, "").Deploy)
	return mux
}

func TestDeploy_InvalidRegions_Returns400(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		appName string
	}{
		{"uppercase region", "regions=US-EAST", "myapp"},
		{"dot in region", "regions=us.east", "myapp"},
		{"slash in region", "regions=us/east", "myapp"},
		{"one good one bad", "regions=us-east,US-EAST", "myapp"},
		// Path-traversal in appName is checked BEFORE the regions
		// query, so a backslash in appName + valid regions still
		// 400s on app name, not regions — confirming the layering.
		{"path traversal in appName", "regions=us-east", "foo\\bar"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newDeployMux()
			url := "/api/deploy/" + tt.appName + "?" + tt.query
			req := httptest.NewRequest("POST", url, nil)
			req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

// TestDeploy_TooManyRegions_Returns400 verifies the cap enforcement
// at the handler boundary. parseRegions runs BEFORE the service is
// called, so 17 valid regions get a 400 without ever reaching the
// service. The service-layer rejection (defense-in-depth) is tested
// separately in service/deployment_test.go.
func TestDeploy_TooManyRegions_Returns400(t *testing.T) {
	mux := newDeployMux()
	// 17 unique valid regions → over the cap of 16.
	query := "regions=" + strings.Join([]string{
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
	}, ",")
	req := httptest.NewRequest("POST", "/api/deploy/myapp?"+query, nil)
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "too many regions") {
		t.Errorf("body = %q, want it to mention 'too many regions'", rr.Body.String())
	}
}

// TestDeploy_OversizedBody_Returns413 verifies that the handler
// caps the request body at service.MaxArtifactSize via
// http.MaxBytesReader. A body that exceeds the cap returns 413
// (Request Entity Too Large) with a JSON error body, before the
// deployment service is ever called.
//
// PR2 switched the wire format to multipart/form-data, so this
// test now wraps the oversized payload in a multipart envelope so
// the request reaches the size check (otherwise the handler
// returns 415 for non-multipart Content-Type — the deliberate
// wire format break is covered by
// TestDeploy_NonMultipartContentType_Returns415).
//
// Pre-fix this returned 500 (or hung the handler on a multi-GiB
// allocation) because io.ReadAll on an unbounded r.Body consumed
// the full payload before the service layer's io.LimitReader ran.
func TestDeploy_OversizedBody_Returns413(t *testing.T) {
	mux := newDeployMux()
	body, ctype := oversizedMultipartBody(t, service.MaxArtifactSize+1)
	req := httptest.NewRequest("POST", "/api/deploy/myapp", body)
	req.Header.Set("Content-Type", ctype)
	req.ContentLength = service.MaxArtifactSize + 1
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "artifact exceeds maximum size") {
		t.Errorf("body = %q, want it to mention 'artifact exceeds maximum size'",
			rr.Body.String())
	}
}

// zeroReader emits a stream of zero bytes — used to construct
// an arbitrarily large body without actually allocating it.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// oversizedMultipartBody wraps the zeroReader stream in a real
// multipart envelope whose total advertised size equals
// `wantContentLength`. Used by TestDeploy_OversizedBody_Returns413
// to drive the size-cap path on the post-PR2 multipart wire.
//
// The total Content-Length (set by the test on the resulting
// http.Request) is `wantContentLength` so the handler's pre-check
// (Content-Length > MaxArtifactSize) fires before any I/O.
//
// We don't actually allocate wantContentLength-1 bytes; the
// multipart writer streams. The MaxBytesReader fires mid-stream
// and maps to 413.
func oversizedMultipartBody(t *testing.T, wantContentLength int64) (*io.PipeReader, string) {
	t.Helper()
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		defer pw.Close()
		if err := mw.WriteField("build_metadata", "{}"); err != nil {
			return
		}
		h := make(textproto.MIMEHeader)
		h.Set("Content-Disposition", `form-data; name="file"; filename="app.wasm"`)
		h.Set("Content-Type", "application/wasm")
		fp, err := mw.CreatePart(h)
		if err != nil {
			return
		}
		// Emit wantContentLength-1 bytes (one less than the cap)
		// so MaxBytesReader fires during the actual io.Copy and
		// the assertion path matches the streaming rejection, not
		// a pre-check rejection.
		_, _ = io.CopyN(fp, zeroReader{}, wantContentLength-1)
		_ = mw.Close()
	}()
	return pr, mw.FormDataContentType()
}

// TestDeploy_NonMultipartContentType_Returns415 verifies the PR2
// wire-format break: deploy requests that don't arrive as
// multipart/form-data are rejected with 415 (Unsupported Media
// Type). The CLI ships alongside the CP, so the wire break is
// acceptable per the PR2 release notes.
func TestDeploy_NonMultipartContentType_Returns415(t *testing.T) {
	mux := newDeployMux()
	// Raw octet-stream — the pre-PR2 wire shape. Should be rejected
	// with 415 and a message pointing the operator at the CLI
	// upgrade.
	req := httptest.NewRequest("POST", "/api/deploy/myapp",
		io.NopCloser(strings.NewReader("not really a wasm")))
	req.Header.Set("Content-Type", "application/octet-stream")
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "multipart/form-data") {
		t.Errorf("body = %q, want it to mention 'multipart/form-data'", rr.Body.String())
	}
}

// TestDeploy_MultipartMissingFile_Returns400 verifies that a
// multipart request that doesn't carry the required `file` part
// returns 400 with a precise error, instead of being silently
// accepted as a no-op.
func TestDeploy_MultipartMissingFile_Returns400(t *testing.T) {
	mux := newDeployMux()
	var buf strings.Builder
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("build_metadata", "{}")
	_ = mw.Close()

	req := httptest.NewRequest("POST", "/api/deploy/myapp",
		io.NopCloser(strings.NewReader(buf.String())))
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "'file' part") {
		t.Errorf("body = %q, want it to mention the missing 'file' part", rr.Body.String())
	}
}
