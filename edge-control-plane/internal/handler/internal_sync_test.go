package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/nats"
)

// fakeSyncBuilder is the test double for syncPayloadBuilder. Returns
// whatever the test injects; default is an empty map and nil error.
type fakeSyncBuilder struct {
	apps map[string]nats.AppConfig
	err  error
}

func (f *fakeSyncBuilder) BuildFullSync(_ context.Context, _, _ string) (map[string]nats.AppConfig, error) {
	return f.apps, f.err
}

// syncHandler builds a minimal InternalHandler with just the
// dependencies the Sync endpoint touches. Other fields stay nil — the
// handler must not call them on this code path.
//
// `worker` and `workerErr` are accepted only for legacy call-site
// compatibility; since PR #166 follow-up #6 the Sync endpoint no
// longer calls workerSvc.Get (it derives tenant/region from the
// JWT). Tests that want to assert "Get was NOT called" pass a
// non-nil worker pointer (which would be read if Get fired) — see
// TestSync_JWTWorkerIDMatchesURL_SkipsWorkerSvcGet.
//
// `builder` is the syncPayloadBuilder interface (not *fakeSyncBuilder)
// so tests that need to record BuildFullSync arguments can substitute
// their own type — see recordingSyncBuilder.
func syncHandler(worker *domain.Worker, workerErr error, builder syncPayloadBuilder) *InternalHandler {
	workerSvc := &fakeWorkerSvc{worker: worker, getErr: workerErr}
	// Pass an untyped nil when builder is nil. A typed nil boxed
	// into the syncPayloadBuilder interface is NOT == nil — Go's
	// classic interface-nil gotcha — so the handler's nil check
	// would falsely see a non-nil builder and skip the 501
	// short-circuit.
	var b syncPayloadBuilder
	if builder != nil {
		b = builder
	}
	return NewInternalHandler(nil, workerSvc, nil, nil, nil, b, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil, nil)
}

// withSyncJWT attaches the same context values middleware.WorkerAuth
// would after validating a real worker JWT: worker_id, tenant_id, and
// region. The Sync endpoint derives all three from the JWT (not from
// the workerSvc DB lookup, which PR #166 follow-up #6 removed). Tests
// that drive the handler past the worker_id-mismatch check need all
// three populated; tests that 4xx/5xx before that check
// (nil-builder, missing-workerID) don't.
func withSyncJWT(r *http.Request, workerID, tenantID, region string) *http.Request {
	ctx := r.Context()
	ctx = context.WithValue(ctx, middleware.WorkerIDKey, workerID)
	ctx = context.WithValue(ctx, middleware.WorkerTenantIDKey, tenantID)
	ctx = context.WithValue(ctx, middleware.WorkerRegionKey, region)
	return r.WithContext(ctx)
}

// --- Sync ---------------------------------------------------------

func TestSync_NilBuilder_Returns501(t *testing.T) {
	h := syncHandler(nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_1/sync", nil)
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusNotImplemented {
		t.Errorf("status=%d, want 501 (nil builder = endpoint disabled)", rr.Code)
	}
}

func TestSync_MissingWorkerID_Returns400(t *testing.T) {
	// Path param missing — handled by the mux normally, but the
	// handler must also defend against empty input.
	h := syncHandler(nil, nil, &fakeSyncBuilder{})
	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers//sync", nil)
	// Simulate empty path value.
	req.SetPathValue("workerID", "")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400 (missing worker_id)", rr.Code)
	}
}

// TestSync_URLWorkerIDMismatchWithJWT_Returns404 pins the new
// cross-tenant defense (PR #166 follow-up #6, replacing the prior
// workerSvc.Get DB lookup): a worker whose JWT has worker_id="w_A"
// must NOT be able to fetch /sync for "w_B" — even if "w_B" is a real
// registered worker. Without this check, a compromised worker JWT
// could enumerate other workerIDs and pull their full app set.
//
// Returns 404 (not 403) with the same body as the previous "worker
// not found" branch so an attacker can't enumerate workerIDs by
// comparing differential responses.
func TestSync_URLWorkerIDMismatchWithJWT_Returns404(t *testing.T) {
	// fakeWorkerSvc is configured to panic if Get is called — the new
	// handler must never invoke it on this code path.
	h := syncHandler(&domain.Worker{ /* unused; panic below if read */ }, nil, &fakeSyncBuilder{})

	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_B/sync", nil)
	req.SetPathValue("workerID", "w_B")
	// JWT says worker_id="w_A"; URL says "w_B". Mismatch → 404.
	req = withSyncJWT(req, "w_A", "t_A", "global")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status=%d, want 404 (mismatched URL/JWT worker_id)", rr.Code)
	}
	expected := `{"error": "worker not found"}`
	if got := strings.TrimSpace(rr.Body.String()); got != expected {
		t.Errorf("body=%q, want %q", got, expected)
	}
}

// TestSync_JWTWithoutWorkerID_Returns404 covers the malformed-JWT
// defense: if the JWT has no worker_id claim (older tokens, or a
// misconfigured issuer), the handler must treat it identically to a
// mismatch — same 404 body — so an attacker can't probe by sending a
// degenerate JWT.
func TestSync_JWTWithoutWorkerID_Returns404(t *testing.T) {
	h := syncHandler(nil, nil, &fakeSyncBuilder{})

	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_1/sync", nil)
	req.SetPathValue("workerID", "w_1")
	// JWT has tenant_id but no worker_id (withSyncJWT(""=workerID, ...))
	req = withSyncJWT(req, "", "t_1", "global")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status=%d, want 404 (no worker_id claim)", rr.Code)
	}
	expected := `{"error": "worker not found"}`
	if got := strings.TrimSpace(rr.Body.String()); got != expected {
		t.Errorf("body=%q, want %q", got, expected)
	}
}

// TestSync_JWTWithoutTenantID_Returns404 covers the malformed-JWT
// defense for the tenant_id claim: if the JWT carries a valid
// worker_id (matches the URL) but is missing tenant_id, the handler
// must 404 with the same body as a URL mismatch — preventing the
// asymmetric outcome where one missing claim yields 404 and another
// yields 500 (info-leak about which claims were parsed by the JWT
// middleware vs which are read from the request).
func TestSync_JWTWithoutTenantID_Returns404(t *testing.T) {
	h := syncHandler(nil, nil, &fakeSyncBuilder{err: errors.New("builder reached")})

	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_1/sync", nil)
	req.SetPathValue("workerID", "w_1")
	// worker_id matches URL, but tenant_id is empty.
	req = withSyncJWT(req, "w_1", "", "global")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status=%d, want 404 (no tenant_id claim)", rr.Code)
	}
	expected := `{"error": "worker not found"}`
	if got := strings.TrimSpace(rr.Body.String()); got != expected {
		t.Errorf("body=%q, want %q", got, expected)
	}
}

// TestSync_JWTWithoutRegion_Returns404: same shape as the
// tenant_id case — a JWT with a valid worker_id + tenant_id but no
// region claim must 404 with the same body, not reach BuildFullSync
// with region="".
func TestSync_JWTWithoutRegion_Returns404(t *testing.T) {
	h := syncHandler(nil, nil, &fakeSyncBuilder{err: errors.New("builder reached")})

	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_1/sync", nil)
	req.SetPathValue("workerID", "w_1")
	req = withSyncJWT(req, "w_1", "t_1", "")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status=%d, want 404 (no region claim)", rr.Code)
	}
	expected := `{"error": "worker not found"}`
	if got := strings.TrimSpace(rr.Body.String()); got != expected {
		t.Errorf("body=%q, want %q", got, expected)
	}
}

// TestSync_JWTWorkerIDMatchesURL_SkipsWorkerSvcGet pins the new
// positive path: when jwt.worker_id == url.workerID, the handler must
// derive (tenant, region) from the JWT and call BuildFullSync WITHOUT
// touching workerSvc.Get. The fakeSyncBuilder's BuildFullSync returns
// an error, so the handler will surface it as 500 — proving the
// handler reached the builder (i.e., did NOT short-circuit on
// workerSvc.Get returning nil-not-found). If a future regression
// re-introduces a workerSvc.Get call that returns nil, the test
// would short-circuit at the 404 branch instead and the builder
// error would NOT surface — making the test detect the regression.
func TestSync_JWTWorkerIDMatchesURL_SkipsWorkerSvcGet(t *testing.T) {
	h := syncHandler(nil, nil, &fakeSyncBuilder{err: errors.New("builder reached = good")})

	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_1/sync", nil)
	req.SetPathValue("workerID", "w_1")
	req = withSyncJWT(req, "w_1", "t_1", "us-east")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	// 500 means the handler reached BuildFullSync (and surfaced the
	// injected error). If a regression brings back workerSvc.Get and
	// it returns (nil, nil), the handler would 404 instead and this
	// assertion would fail.
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want 500 (builder should have been reached, proving workerSvc.Get was skipped); body=%s",
			rr.Code, rr.Body.String())
	}
}

// TestSync_BuilderError_Returns500: BuildFullSync error path is
// unchanged — once past the JWT-match check, the handler still maps
// a builder error to 500.
func TestSync_BuilderError_Returns500(t *testing.T) {
	h := syncHandler(nil, nil, &fakeSyncBuilder{err: errors.New("repo gone")})
	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_1/sync", nil)
	req.SetPathValue("workerID", "w_1")
	req = withSyncJWT(req, "w_1", "t_1", "global")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("status=%d, want 500 (builder error)", rr.Code)
	}
}

// TestSync_EmptyApps_ReturnsFullSyncEnvelopeWithEmptyMap: a worker
// with no active deployments must still get a valid response — empty
// apps map, NOT null. The worker's deserializer would crash on
// `"apps": null` because the Rust HashMap doesn't represent null.
// The handler explicitly normalizes nil to {}.
func TestSync_EmptyApps_ReturnsFullSyncEnvelopeWithEmptyMap(t *testing.T) {
	h := syncHandler(nil, nil, &fakeSyncBuilder{apps: nil})
	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_1/sync", nil)
	req.SetPathValue("workerID", "w_1")
	req = withSyncJWT(req, "w_1", "t_1", "global")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rr.Code)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v; body=%s", err, rr.Body.String())
	}
	if got["type"] != "full_sync" {
		t.Errorf("type=%v, want full_sync", got["type"])
	}
	if got["tenant_id"] != "t_1" {
		t.Errorf("tenant_id=%v, want t_1", got["tenant_id"])
	}
	apps, ok := got["apps"].(map[string]interface{})
	if !ok {
		t.Fatalf("apps=%v (%T), want JSON object", got["apps"], got["apps"])
	}
	if len(apps) != 0 {
		t.Errorf("apps len=%d, want 0 (normalized from nil)", len(apps))
	}
}

// TestSync_PopulatedApps_ReturnsPayload locks the wire shape: a
// future refactor must not change field names or omit the type
// field. The tenant_id in the response must come from the JWT (not
// from a DB lookup), so this test also implicitly pins that.
func TestSync_PopulatedApps_ReturnsPayload(t *testing.T) {
	want := map[string]nats.AppConfig{
		"myapp": {
			DeploymentID:   "d_1",
			DeploymentHash: "abc",
			Env:            map[string]string{"K": "v"},
			Allowlist:      []string{"api.example.com"},
			MaxMemoryMB:    256,
		},
	}
	h := syncHandler(nil, nil, &fakeSyncBuilder{apps: want})
	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_1/sync", nil)
	req.SetPathValue("workerID", "w_1")
	req = withSyncJWT(req, "w_1", "t_1", "us-east")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	var got struct {
		Type     string                     `json:"type"`
		TenantID string                     `json:"tenant_id"`
		Apps     map[string]json.RawMessage `json:"apps"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Type != "full_sync" {
		t.Errorf("type=%q, want full_sync", got.Type)
	}
	if got.TenantID != "t_1" {
		t.Errorf("tenant_id=%q, want t_1", got.TenantID)
	}
	cfg, ok := got.Apps["myapp"]
	if !ok {
		t.Fatalf("apps=%v, want myapp", got.Apps)
	}
	var parsed nats.AppConfig
	if err := json.Unmarshal(cfg, &parsed); err != nil {
		t.Fatalf("myapp decode: %v", err)
	}
	if parsed.DeploymentID != "d_1" || parsed.DeploymentHash != "abc" {
		t.Errorf("parsed=%+v", parsed)
	}
}

// TestSync_JWTRegionDrivesBuildFullSync pins that the region passed
// to BuildFullSync is the JWT's region claim, not anything derived
// from a workerSvc lookup. Catches a future refactor that adds back
// a region lookup.
func TestSync_JWTRegionDrivesBuildFullSync(t *testing.T) {
	var gotRegion string
	recording := &recordingSyncBuilder{recordRegion: &gotRegion}
	h := syncHandler(nil, nil, recording)
	req := httptest.NewRequest(http.MethodGet, "/api/internal/workers/w_1/sync", nil)
	req.SetPathValue("workerID", "w_1")
	req = withSyncJWT(req, "w_1", "t_1", "eu-central")
	rr := httptest.NewRecorder()

	h.Sync(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if gotRegion != "eu-central" {
		t.Errorf("BuildFullSync region=%q, want eu-central (from JWT)", gotRegion)
	}
}

// recordingSyncBuilder captures the region argument so tests can
// assert it's derived from the JWT, not a DB lookup.
type recordingSyncBuilder struct {
	recordRegion *string
}

func (r *recordingSyncBuilder) BuildFullSync(_ context.Context, _ string, region string) (map[string]nats.AppConfig, error) {
	*r.recordRegion = region
	return map[string]nats.AppConfig{}, nil
}
