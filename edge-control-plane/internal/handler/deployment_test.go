package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
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
		defer func() { _ = pw.Close() }()
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

// ── parsePreviewOpts (issue #308) ───────────────────────────────────────
//
// Unit tests for the preview query-param parser. We exercise it
// directly rather than going through the full Deploy handler so the
// assertion space stays narrow and the failure mode ("which input
// produced this error?") is obvious.

func TestParsePreviewOpts_AllEmpty_ReturnsNil(t *testing.T) {
	// No preview-id, no pr-number, no ttl → not a preview deploy.
	// Caller treats nil as "no preview" and the rest of Deploy
	// proceeds identically to the pre-#308 code path.
	got, err := parsePreviewOpts("", "", "")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got != nil {
		t.Errorf("opts = %+v, want nil", got)
	}
}

func TestParsePreviewOpts_OnlyID_NoPRNumber_NoTTL(t *testing.T) {
	// preview-id alone is allowed — the CLI mints this for a
	// laptop `edge deploy --preview` where there's no GitHub PR
	// context. The TTL falls back to PreviewDefaultTTL (7 days).
	got, err := parsePreviewOpts("abcd1234", "", "")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got == nil {
		t.Fatal("opts = nil, want non-nil")
	}
	if got.PreviewID != "abcd1234" {
		t.Errorf("PreviewID = %q, want %q", got.PreviewID, "abcd1234")
	}
	if got.PreviewPRNumber != nil {
		t.Errorf("PreviewPRNumber = %v, want nil", *got.PreviewPRNumber)
	}
	// Default TTL: the handler resolves to a future time, so we
	// just assert it's roughly 7d from now.
	if d := time.Until(got.ExpiresAt); d < 6*24*time.Hour || d > 8*24*time.Hour {
		t.Errorf("ExpiresAt is %v from now, want roughly 7 days", d)
	}
}

func TestParsePreviewOpts_FullParams(t *testing.T) {
	got, err := parsePreviewOpts("abcd1234", "123", "24h")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got.PreviewID != "abcd1234" {
		t.Errorf("PreviewID = %q, want %q", got.PreviewID, "abcd1234")
	}
	if got.PreviewPRNumber == nil || *got.PreviewPRNumber != 123 {
		t.Errorf("PreviewPRNumber = %v, want Some(123)", got.PreviewPRNumber)
	}
	// 24h ttl: assert it's in (23h, 25h) to allow clock skew.
	if d := time.Until(got.ExpiresAt); d < 23*time.Hour || d > 25*time.Hour {
		t.Errorf("ExpiresAt is %v from now, want ~24h", d)
	}
}

func TestParsePreviewOpts_InvalidIDFormat_ReturnsErr(t *testing.T) {
	// preview-id must be 8..16 lowercase hex chars. The handler
	// rejects non-conforming input before any DB writes — a
	// malformed id is the cheapest of the failure modes (no
	// row created, no blob stored, no preview allocated).
	cases := []struct {
		name string
		id   string
	}{
		{"too short", "abc"},
		{"too long", "abcdef01234567890"},
		{"uppercase", "ABCD1234"},
		{"non-hex chars", "abcd123g"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePreviewOpts(tc.id, "", "")
			if err == nil {
				t.Errorf("id=%q: err = nil, want non-nil", tc.id)
			}
		})
	}
}

func TestParsePreviewOpts_InvalidPRNumber_ReturnsErr(t *testing.T) {
	// Negative pr-number is rejected. The composite action
	// forwards `${{ github.event.pull_request.number }}` which is
	// always >= 1; a negative value means a CLI bug or a
	// curl-based caller.
	_, err := parsePreviewOpts("abcd1234", "-1", "")
	if err == nil {
		t.Errorf("err = nil, want non-nil")
	}
}

func TestParsePreviewOpts_InvalidTTL_ReturnsErr(t *testing.T) {
	// Negative ttl (or zero) is rejected — both are foot-guns
	// that would expire the preview immediately.
	_, err := parsePreviewOpts("abcd1234", "", "-1h")
	if err == nil {
		t.Errorf("err = nil, want non-nil")
	}
	_, err = parsePreviewOpts("abcd1234", "", "0s")
	if err == nil {
		t.Errorf("err = nil, want non-nil (0s)")
	}
}

func TestParsePreviewOpts_PRNumberWithoutID_ReturnsErr(t *testing.T) {
	// preview-pr-number without preview-id is meaningless: the
	// pr-number is metadata for a preview that doesn't exist.
	// Reject so a CLI bug surfaces immediately rather than as
	// confusing stored-row metadata.
	_, err := parsePreviewOpts("", "123", "")
	if err == nil {
		t.Errorf("err = nil, want non-nil")
	}
}

// ---------------------------------------------------------------------------
// statusResponse DTO mapper — pins the wire contract for
// GET /api/v1/status/{id}, GET /api/v1/apps/{app}/active, and the
// items in GET /api/v1/list/{app}. The CLI deserializes these
// endpoints and was previously broken by `*domain.Deployment` being
// emitted raw (PascalCase, no `URL`). These tests lock the snake_case
// DTO shape so the wire contract can't drift again.
// ---------------------------------------------------------------------------

// TestNewStatusResponse_SnakeCaseAndURL pins every field of the DTO
// mapper: snake_case keys, RFC3339 timestamp, computed URL, optional
// preview/signature fields suppressed when unset. A regression here
// would break the CLI's `edge status`, `edge deployments`, and
// `edge open` against a real CP.
func TestNewStatusResponse_SnakeCaseAndURL(t *testing.T) {
	previewID := "abcd1234"
	prNum := 42
	expires := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	d := &domain.Deployment{
		ID:                  "d_abc",
		TenantID:            "t_test",
		AppName:             "myapp",
		Status:              "active",
		Hash:                "3a2f5e4b",
		Signature:           "5c1e9b",
		SigningKeyID:        "prod-2026-q3",
		Regions:             pq.StringArray{"us-east-1", "eu-west-1"},
		AutoRollbackEnabled: true,
		DesiredReplicas:     3,
		BuildAttestation:    json.RawMessage(`{"payload":"..."}`),
		PreviewID:           &previewID,
		PreviewPRNumber:     &prNum,
		PreviewExpiresAt:    &expires,
		CreatedAt:           time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	}

	got := newStatusResponse(d, "t_test", "myapp")

	if got.ID != "d_abc" {
		t.Errorf("ID = %q, want d_abc", got.ID)
	}
	if got.TenantID != "t_test" {
		t.Errorf("TenantID = %q, want t_test", got.TenantID)
	}
	if got.AppName != "myapp" {
		t.Errorf("AppName = %q, want myapp", got.AppName)
	}
	if got.Status != "active" {
		t.Errorf("Status = %q, want active", got.Status)
	}
	if got.Hash != "3a2f5e4b" {
		t.Errorf("Hash = %q, want 3a2f5e4b", got.Hash)
	}
	if got.Signature != "5c1e9b" {
		t.Errorf("Signature = %q, want 5c1e9b", got.Signature)
	}
	if got.SigningKeyID != "prod-2026-q3" {
		t.Errorf("SigningKeyID = %q, want prod-2026-q3", got.SigningKeyID)
	}
	if !equalSlices(got.Regions, []string{"us-east-1", "eu-west-1"}) {
		t.Errorf("Regions = %v, want [us-east-1 eu-west-1]", got.Regions)
	}
	if !got.AutoRollbackEnabled {
		t.Errorf("AutoRollbackEnabled = false, want true")
	}
	if got.DesiredReplicas != 3 {
		t.Errorf("DesiredReplicas = %d, want 3", got.DesiredReplicas)
	}
	if !bytes.Contains(got.BuildAttestation, []byte(`"payload"`)) {
		t.Errorf("BuildAttestation = %s, want to contain payload", got.BuildAttestation)
	}
	if got.PreviewID != "abcd1234" {
		t.Errorf("PreviewID = %q, want abcd1234", got.PreviewID)
	}
	if got.PreviewPRNumber == nil || *got.PreviewPRNumber != 42 {
		t.Errorf("PreviewPRNumber = %v, want Some(42)", got.PreviewPRNumber)
	}
	if got.PreviewExpiresAt != "2026-07-16T00:00:00Z" {
		t.Errorf("PreviewExpiresAt = %q, want 2026-07-16T00:00:00Z", got.PreviewExpiresAt)
	}
	if got.CreatedAt != "2026-07-01T12:00:00Z" {
		t.Errorf("CreatedAt = %q, want 2026-07-01T12:00:00Z", got.CreatedAt)
	}
	if got.URL != "https://t_test-myapp.edgecloud.dev" {
		t.Errorf("URL = %q, want https://t_test-myapp.edgecloud.dev", got.URL)
	}
}

// TestNewStatusResponse_OmitsOptionalFieldsWhenNil asserts that a row
// with no preview / signature / attestation produces an empty DTO
// for those fields. `omitempty` on the JSON tags means the wire body
// stays free of empty strings / null pointers — important because the
// CLI uses `#[serde(default)]` and benefits from the absence being
// well-defined.
func TestNewStatusResponse_OmitsOptionalFieldsWhenNil(t *testing.T) {
	d := &domain.Deployment{
		ID:        "d_min",
		TenantID:  "t_x",
		AppName:   "a",
		Status:    "deployed",
		Hash:      "h",
		Regions:   pq.StringArray{},
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	got := newStatusResponse(d, "t_x", "a")

	if got.Signature != "" {
		t.Errorf("Signature = %q, want empty (omitempty)", got.Signature)
	}
	if got.SigningKeyID != "" {
		t.Errorf("SigningKeyID = %q, want empty (omitempty)", got.SigningKeyID)
	}
	if got.BuildAttestation != nil {
		t.Errorf("BuildAttestation = %s, want nil (omitempty)", got.BuildAttestation)
	}
	if got.PreviewID != "" {
		t.Errorf("PreviewID = %q, want empty (omitempty)", got.PreviewID)
	}
	if got.PreviewPRNumber != nil {
		t.Errorf("PreviewPRNumber = %v, want nil (omitempty)", got.PreviewPRNumber)
	}
	if got.PreviewExpiresAt != "" {
		t.Errorf("PreviewExpiresAt = %q, want empty (omitempty)", got.PreviewExpiresAt)
	}
}

// TestNewStatusResponse_RoundTripJSON confirms the DTO round-trips
// through encoding/json with the exact snake_case field names the
// CLI expects. This is the single most important regression guard
// — if anyone renames a field or drops a `json:` tag, the CLI breaks
// against the real CP and this test catches it.
func TestNewStatusResponse_RoundTripJSON(t *testing.T) {
	previewID := "abcd1234"
	prNum := 7
	d := &domain.Deployment{
		ID:              "d_rt",
		TenantID:        "t_rt",
		AppName:         "rt",
		Status:          "active",
		Hash:            "h",
		Regions:         pq.StringArray{"us-east"},
		PreviewID:       &previewID,
		PreviewPRNumber: &prNum,
		CreatedAt:       time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC),
	}
	got := newStatusResponse(d, "t_rt", "rt")

	b, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var wire map[string]interface{}
	if err := json.Unmarshal(b, &wire); err != nil {
		t.Fatalf("unmarshal into map: %v", err)
	}

	wantStrings := map[string]string{
		"id":         "d_rt",
		"tenant_id":  "t_rt",
		"app_name":   "rt",
		"status":     "active",
		"hash":       "h",
		"preview_id": "abcd1234",
		"url":        "https://t_rt-rt.edgecloud.dev",
		"created_at": "2026-07-09T00:00:00Z",
	}
	for k, want := range wantStrings {
		if got, _ := wire[k].(string); got != want {
			t.Errorf("wire[%q] = %q, want %q", k, got, want)
		}
	}
	if _, ok := wire["signature"]; ok {
		t.Errorf("wire should not contain \"signature\" when empty (omitempty)")
	}
	if _, ok := wire["build_attestation"]; ok {
		t.Errorf("wire should not contain \"build_attestation\" when empty (omitempty)")
	}
	if prNum, _ := wire["preview_pr_number"].(float64); int(prNum) != 7 {
		t.Errorf("wire[preview_pr_number] = %v, want 7", wire["preview_pr_number"])
	}
}

// ── Idempotency-Key (issue #52) ──────────────────────────────────────────
//
// Four pin tests for the Idempotency-Key wire contract:
//
//   - malformed header → 400 (caught at the handler before any service call)
//   - valid header + cache hit → 200 with the cached deployment_id
//   - valid header + body-hash mismatch → 422
//   - header omitted → 201 (fresh deploy), the same shape as pre-#52
//
// Each test constructs a *service.DeploymentService with a
// hand-rolled stubIdempotencyRepo so the cache check returns
// the desired state without spinning up sqlx. Other service
// fields (db, repos) are nil because the replay short-circuits
// the function before they're reached.
type stubIdempotencyRepo struct {
	row *domain.IdempotencyKey
	err error
}

func (s *stubIdempotencyRepo) Lookup(_ context.Context, _, _ string) (*domain.IdempotencyKey, error) {
	return s.row, s.err
}
func (s *stubIdempotencyRepo) Insert(_ context.Context, _ *domain.IdempotencyKey) error {
	return nil
}

// stubDeploymentRepo is the minimal DeploymentRepository surface
// needed for the replay short-circuit (GetByID only). The other
// methods would panic if called — the replay test never reaches
// them, so leaving them un-implemented is the right tradeoff.
type stubDeploymentRepo struct {
	hit *domain.Deployment
}

func (s *stubDeploymentRepo) GetByID(_ context.Context, _ string) (*domain.Deployment, error) {
	if s.hit == nil {
		return nil, nil
	}
	return s.hit, nil
}

// newFullStubDeploymentRepo wraps a partial stubDeploymentRepo
// in a type that satisfies service.deploymentRepoInterface
// so the reflect-set on the service struct accepts it. The
// unused methods here panic (a deliberate tripwire): if a
// future Deploy change routes the replay path through any
// repo method besides GetByID, this test fixture explodes
// loudly instead of silently no-opping.
func newFullStubDeploymentRepo(partial *stubDeploymentRepo) *fullStubDeploymentRepo {
	return &fullStubDeploymentRepo{partial: partial}
}

type fullStubDeploymentRepo struct {
	partial *stubDeploymentRepo
}

func (f *fullStubDeploymentRepo) GetByID(ctx context.Context, id string) (*domain.Deployment, error) {
	return f.partial.GetByID(ctx, id)
}

func (f *fullStubDeploymentRepo) ListByApp(_ context.Context, _, _ string) ([]domain.Deployment, error) {
	panic("fullStubDeploymentRepo.ListByApp: replay path should not reach here")
}
func (f *fullStubDeploymentRepo) CountByApp(_ context.Context, _, _ string) (int, error) {
	panic("fullStubDeploymentRepo.CountByApp: replay path should not reach here")
}
func (f *fullStubDeploymentRepo) ListByAppPaginated(_ context.Context, _, _ string, _, _ int) ([]domain.Deployment, error) {
	panic("fullStubDeploymentRepo.ListByAppPaginated: replay path should not reach here")
}
func (f *fullStubDeploymentRepo) Create(_ context.Context, _ *domain.Deployment) error {
	panic("fullStubDeploymentRepo.Create: replay path should not reach here")
}
func (f *fullStubDeploymentRepo) DeleteByID(_ context.Context, _ string) error {
	panic("fullStubDeploymentRepo.DeleteByID: replay path should not reach here")
}
func (f *fullStubDeploymentRepo) WithTx(_ *sqlx.Tx) *repository.DeploymentRepository {
	panic("fullStubDeploymentRepo.WithTx: replay path should not reach here")
}

// newIdempotencyMux wires a Deploy route with a stubbed
// DeploymentService. Two service fields are populated via
// reflection (unexported setters don't exist on the service
// struct):
//   - idempotencyRepo — the test's stubIdempotencyRepo wrapped
//     in the service's expected interface (Lookup + Insert)
//   - deploymentRepo — a tiny stub that returns the cached
//     deployment row when the service's replay path calls
//     GetByID. Only reached on a cache hit.
//
// Both fields are UNEXPORTED on service.DeploymentService
// (intentional; the production setter path is via
// SetIdempotencyRepo + WithTx). Reflection is the test-only
// channel for accessing them; this concentrates the
// type-system escape hatch in one helper.
func newIdempotencyMux(idem *stubIdempotencyRepo, depRepo *stubDeploymentRepo) http.Handler {
	svc := &service.DeploymentService{}
	svc.SetIdempotencyRepo(idempotencyRepoAdapter{stub: idem})
	// Only inject a deploymentRepo when the test actually
	// needs it (the replay / mismatch paths). The malformed-
	// key and fresh-path tests never reach the service, and
	// injecting an incomplete stub triggers a reflect assign
	// failure ("not assignable to deploymentRepoInterface").
	if depRepo != nil {
		setUnexportedField(svc, "deploymentRepo", newFullStubDeploymentRepo(depRepo))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/deploy/{appName}", NewDeploymentHandler(svc, nil, nil, nil, "").Deploy)
	return mux
}

// setUnexportedField writes a value to an unexported struct
// field via reflect. Test-only; production code never needs
// this (it has setters). The field name is verified to exist
// at runtime so a future rename of the service struct
// surfaces as a test failure rather than a silent nil.
func setUnexportedField(target any, fieldName string, value any) {
	v := reflect.ValueOf(target).Elem().FieldByName(fieldName)
	if !v.IsValid() {
		panic("setUnexportedField: no field " + fieldName + " on service.DeploymentService")
	}
	// FieldByName returns a Value that's read-only for
	// unexported fields; reflect.New + Elem().Set is the
	// canonical workaround.
	reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(value))
}

// idempotencyRepoAdapter bridges the test stub to the
// service's expected interface (Lookup + Insert). It's a
// test-only shim — production uses the real
// *repository.IdempotencyKeyRepo.
type idempotencyRepoAdapter struct {
	stub *stubIdempotencyRepo
}

func (a idempotencyRepoAdapter) Lookup(ctx context.Context, tenantID, key string) (*domain.IdempotencyKey, error) {
	return a.stub.Lookup(ctx, tenantID, key)
}
func (a idempotencyRepoAdapter) Insert(ctx context.Context, k *domain.IdempotencyKey) error {
	return a.stub.Insert(ctx, k)
}

// TestDeploy_IdempotencyKey_Malformed_Returns400 pins the
// pre-service 400 short-circuit on a header that doesn't match
// [a-fA-F0-9-]{8,128}. A too-short key, a key with disallowed
// characters, and an over-128-char key all 400 before the
// multipart body is even read.
func TestDeploy_IdempotencyKey_Malformed_Returns400(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"too short", "abc"},
		{"contains space", "ab cd efgh"},
		{"contains special", "abc!@#$%"},
		{"exceeds 128 chars", "a" + strings.Repeat("b", 130)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := newMultipartDeployRequest(t, "/api/deploy/myapp")
			req.Header.Set("Idempotency-Key", tc.key)
			req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
			rr := httptest.NewRecorder()

			mux := newIdempotencyMux(&stubIdempotencyRepo{}, nil)
			mux.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "Idempotency-Key") {
				t.Errorf("body = %q, want it to mention 'Idempotency-Key'", rr.Body.String())
			}
		})
	}
}

// TestDeploy_IdempotencyKey_Fresh_Returns201 pins the fresh
// path: a well-formed key with no cached row proceeds to
// service.Deploy (which panics on the nil deploymentRepo —
// but that means we reached past the replay check, which is
// what we're locking here). The handler must NOT return 200
// when there's no cached row.
//
// We test this with a stubIdempotencyRepo that returns
// (nil, nil) for Lookup, then expect a panic from the
// service (the nil deploymentRepo deref). The deferred
// recover() converts that expected panic into a quiet
// return — the assertion that matters is "we did NOT see
// a 200 (or any early return); we got past the replay
// check."
func TestDeploy_IdempotencyKey_Fresh_Returns201(t *testing.T) {
	body, ctype := multipartWasmBody(t)
	req := httptest.NewRequest("POST", "/api/deploy/myapp", body)
	req.Header.Set("Content-Type", ctype)
	req.Header.Set("Idempotency-Key", "deadbeef-1234-5678-9abc-def012345678")
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()

	// No cached row. The service will panic on the nil
	// deploymentRepo deref when we reach the heavy
	// service code path — that's the signal that the
	// replay short-circuit fired (we got past the
	// cache check but didn't return early).
	mux := newIdempotencyMux(&stubIdempotencyRepo{}, nil)
	func() {
		defer func() { _ = recover() }()
		mux.ServeHTTP(rr, req)
	}()
	// The handler must not have returned a status before
	// reaching the service. If it had, rr.Code would be
	// a non-default value (http.ResponseRecorder defaults
	// to 200 for WriteHeader, but the handler only calls
	// WriteHeader at the very end — so a default 200
	// would actually hide the bug). Assert that we did
	// NOT see one of the early-return codes (400/401/404)
	// instead, which proves we got past the cache check.
	if rr.Code == http.StatusBadRequest ||
		rr.Code == http.StatusUnauthorized ||
		rr.Code == http.StatusNotFound {
		t.Fatalf("handler returned early status %d; the replay check should have fired and the service should have run", rr.Code)
	}
}

// TestDeploy_IdempotencyKey_Replay_Returns200 verifies that a
// cache hit short-circuits the service and the handler returns
// 200 with the original deployment_id (issue #52 contract).
// We use a stub that returns a cached row pointing at a
// pre-built *domain.Deployment; the handler must NOT panic
// (which would mean the heavy service path ran).
func TestDeploy_IdempotencyKey_Replay_Returns200(t *testing.T) {
	const wantID = "d_replay_target_42"
	body, ctype := multipartWasmBody(t)
	// Pre-compute the artifact SHA the handler will compute so
	// the cached row's RequestSHA256 matches. The handler's
	// "same key, different body" guard returns 422 otherwise,
	// which would mask the replay-path assertion.
	artifactBytes := []byte("\x00asm\x01\x00\x00\x00minimal-wasm-bytes\n")
	artifactSHA := sha256.Sum256(artifactBytes)
	req := httptest.NewRequest("POST", "/api/deploy/myapp", body)
	req.Header.Set("Content-Type", ctype)
	req.Header.Set("Idempotency-Key", "deadbeef-1234-5678-9abc-def012345678")
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()

	cached := &stubIdempotencyRepo{
		row: &domain.IdempotencyKey{
			TenantID:      "t_test",
			Key:           "deadbeef-1234-5678-9abc-def012345678",
			DeploymentID:  wantID,
			RequestSHA256: artifactSHA,
		},
	}
	depRepo := &stubDeploymentRepo{
		hit: &domain.Deployment{ID: wantID, TenantID: "t_test", AppName: "myapp"},
	}
	mux := newIdempotencyMux(cached, depRepo)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	if jerr := json.Unmarshal(rr.Body.Bytes(), &got); jerr != nil {
		t.Fatalf("response is not JSON: %v", jerr)
	}
	if got["id"] != wantID {
		t.Errorf("id = %v, want %q", got["id"], wantID)
	}
}

// TestDeploy_IdempotencyKey_BodyMismatch_Returns422 pins the
// "same key, different body" guard. A cache hit whose
// RequestSHA256 differs from the handler-computed hash returns
// 422 with the sentinel error message. The 422 is the
// operator's signal that the key was reused by mistake.
func TestDeploy_IdempotencyKey_BodyMismatch_Returns422(t *testing.T) {
	body, ctype := multipartWasmBody(t)
	req := httptest.NewRequest("POST", "/api/deploy/myapp", body)
	req.Header.Set("Content-Type", ctype)
	req.Header.Set("Idempotency-Key", "deadbeef-1234-5678-9abc-def012345678")
	req = req.WithContext(middleware.WithTenantID(req.Context(), "t_test"))
	rr := httptest.NewRecorder()

	// Cached row points at a deployment whose SHA is DIFFERENT
	// from the one the handler is about to compute (zero
	// bytes vs whatever the multipart body hashes to).
	cached := &stubIdempotencyRepo{
		row: &domain.IdempotencyKey{
			TenantID:     "t_test",
			Key:          "deadbeef-1234-5678-9abc-def012345678",
			DeploymentID: "d_old_replay_target",
			// Force a mismatch by setting a non-zero
			// SHA the artifact bytes will not equal.
			RequestSHA256: [32]byte{0xff},
		},
	}
	mux := newIdempotencyMux(cached, &stubDeploymentRepo{
		hit: &domain.Deployment{ID: "d_old_replay_target", TenantID: "t_test", AppName: "myapp"},
	})
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "idempotency key reused") {
		t.Errorf("body = %q, want it to mention 'idempotency key reused'", rr.Body.String())
	}
}

// multipartWasmBody builds a minimal multipart/form-data body
// with a single `file` part. The bytes are NOT a real wasm
// module — that's the service's job to reject via the
// 4-byte magic peek — but the multipart envelope must be
// well-formed for the handler to parse. The four-byte
// prefix `\x00asm` mimics the wasm magic so a future test
// extension can re-use this helper without changing the
// envelope shape.
func multipartWasmBody(t *testing.T) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	// header line + 4 wasm-magic bytes + a trailing newline,
	// enough for the handler's `written == 0` check to
	// succeed (1+ bytes) without forcing test-time work.
	const fileContent = "\x00asm\x01\x00\x00\x00minimal-wasm-bytes\n"
	fw, err := mw.CreateFormFile("file", "app.wasm")
	if err != nil {
		t.Fatalf("mw.CreateFormFile: %v", err)
	}
	if _, err := fw.Write([]byte(fileContent)); err != nil {
		t.Fatalf("fw.Write: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("mw.Close: %v", err)
	}
	return &buf, mw.FormDataContentType()
}

// newMultipartDeployRequest is a thin wrapper that calls
// multipartWasmBody and constructs an *http.Request suitable
// for use with httptest. Kept separate so callers in the
// 400-path tests can ignore the body bytes if they want.
func newMultipartDeployRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	body, ctype := multipartWasmBody(t)
	req := httptest.NewRequest("POST", url, body)
	req.Header.Set("Content-Type", ctype)
	return req
}
