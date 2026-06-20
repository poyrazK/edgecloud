package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/golang-jwt/jwt/v5"
)

// -----------------------------------------------------------------------
// Mock repo — exercises the handler without a live DB. The interface is
// declared in internal_logs.go and mirrors the methods we call.
// -----------------------------------------------------------------------

type mockLogEntryRepo struct {
	insertBatchFunc func(ctx context.Context, entries []domain.LogEntry) error
	lastEntries     []domain.LogEntry
	calls           int
}

func (m *mockLogEntryRepo) InsertBatch(ctx context.Context, entries []domain.LogEntry) error {
	m.calls++
	m.lastEntries = entries
	if m.insertBatchFunc != nil {
		return m.insertBatchFunc(ctx, entries)
	}
	return nil
}

// -----------------------------------------------------------------------
// Test wiring helpers
// -----------------------------------------------------------------------

const (
	testJWTSecret = "test-secret"
	testJWTIssuer = "edgecloud"
)

// validToken mints a Worker JWT with the given tenant/worker IDs and a
// 24-hour TTL (matching production).
func validToken(t *testing.T, tenantID, workerID string) string {
	t.Helper()
	return validTokenWithRegion(t, tenantID, workerID, "fra")
}

// validTokenWithRegion is the variant used by TestIngestLogs_RegionFromJWT —
// it lets the test pin a specific region on the JWT.
func validTokenWithRegion(t *testing.T, tenantID, workerID, region string) string {
	t.Helper()
	claims := &middleware.WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    testJWTIssuer,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: workerID,
		TenantID: tenantID,
		Region:   region,
		Apps:     []string{},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(testJWTSecret))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return signed
}

// newIngestLogsServer builds a handler chain with WorkerAuth + IngestLogs,
// exactly as main.go wires it. Tests send requests through this server to
// exercise auth + handler together.
func newIngestLogsServer(repo *mockLogEntryRepo) http.Handler {
	h := &InternalHandler{logEntryRepo: repo}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/internal/logs", h.IngestLogs)
	return middleware.WorkerAuth(middleware.WorkerJWTConfig{
		Secret: testJWTSecret,
		Issuer: testJWTIssuer,
	})(mux)
}

// postLogs is a small helper that serializes entries as JSON and sends the
// request through the test server. `overrideTenant`, `overrideWorker`, and
// `overrideRegion` let us forge an entry with a different identity from the
// JWT — used to test the tenant/worker/region overwrite behavior.
func postLogs(t *testing.T, server http.Handler, token string, entries []domain.LogEntry, overrideTenant, overrideWorker, overrideRegion string) *httptest.ResponseRecorder {
	t.Helper()
	for i := range entries {
		if overrideTenant != "" {
			entries[i].TenantID = overrideTenant
		}
		if overrideWorker != "" {
			entries[i].WorkerID = overrideWorker
		}
		if overrideRegion != "" {
			entries[i].Region = overrideRegion
		}
	}
	body, err := json.Marshal(IngestLogsRequest{Entries: entries})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest("POST", "/api/internal/logs", bytes.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	return rec
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

func TestIngestLogs_HappyPath(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_abc123")

	entries := []domain.LogEntry{
		{DeploymentID: "d_1", AppName: "app1", Level: "info", Message: "hello", Labels: json.RawMessage(`{"k":"v"}`)},
		{DeploymentID: "d_1", AppName: "app1", Level: "warn", Message: "uh oh"},
		{DeploymentID: "d_2", AppName: "app2", Level: "error", Message: "boom"},
	}

	rec := postLogs(t, server, token, entries, "", "", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rec.Code, rec.Body.String())
	}
	if repo.calls != 1 {
		t.Fatalf("repo called %d times, want 1", repo.calls)
	}
	if got := len(repo.lastEntries); got != 3 {
		t.Fatalf("repo got %d entries, want 3", got)
	}
	// Auth fields overwritten from JWT.
	for i, e := range repo.lastEntries {
		if e.TenantID != "t_real" {
			t.Errorf("entry[%d].TenantID = %q, want t_real", i, e.TenantID)
		}
		if e.WorkerID != "w_fra_abc123" {
			t.Errorf("entry[%d].WorkerID = %q, want w_fra_abc123", i, e.WorkerID)
		}
	}
}

func TestIngestLogs_EmptyEntries(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_abc123")

	rec := postLogs(t, server, token, []domain.LogEntry{}, "", "", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if repo.calls != 0 {
		t.Errorf("repo should not be called on empty entries, got %d calls", repo.calls)
	}
}

func TestIngestLogs_EmptyBody(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_abc123")

	req := httptest.NewRequest("POST", "/api/internal/logs", bytes.NewReader(nil))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if repo.calls != 0 {
		t.Errorf("repo should not be called on empty body, got %d calls", repo.calls)
	}
}

func TestIngestLogs_AuthMissing(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)

	// No Authorization header — WorkerAuth should reject before the handler runs.
	rec := postLogs(t, server, "", []domain.LogEntry{{AppName: "x"}}, "", "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if repo.calls != 0 {
		t.Errorf("repo should not be called when unauthenticated, got %d calls", repo.calls)
	}
}

func TestIngestLogs_AuthInvalid(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)

	// Sign with a different secret than the server expects.
	bad := validTokenWithSecret(t, "t_real", "w_fra_abc123", "wrong-secret")
	rec := postLogs(t, server, bad, []domain.LogEntry{{AppName: "x"}}, "", "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if repo.calls != 0 {
		t.Errorf("repo should not be called with bad JWT, got %d calls", repo.calls)
	}
}

func TestIngestLogs_BatchTooLarge(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_abc123")

	// Build a JSON body whose total size exceeds MaxLogBatchSize. The JSON
	// prefix is valid so the decoder actually tries to read past the cap;
	// if we sent raw garbage the test would hit a JSON syntax error instead
	// and never reach the size check.
	prefix := []byte(`{"entries":[{"message":"`)
	padding := bytes.Repeat([]byte("x"), MaxLogBatchSize+1)
	suffix := []byte(`"}]}`)
	body := append(append(prefix, padding...), suffix...)

	req := httptest.NewRequest("POST", "/api/internal/logs", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "batch too large") {
		t.Errorf("expected error to mention 'batch too large', got: %s", rec.Body.String())
	}
	if repo.calls != 0 {
		t.Errorf("repo should not be called on oversize batch, got %d calls", repo.calls)
	}
}

// TestIngestLogs_RegionFromJWT confirms the handler overwrites the body-
// supplied region with the JWT's region. A worker that lies about region
// in the body must be ignored — the JWT is the source of truth.
func TestIngestLogs_RegionFromJWT(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validTokenWithRegion(t, "t_real", "w_fra_real", "fra")

	// Body claims region "us-east-1"; JWT carries "fra". Handler must use "fra".
	entries := []domain.LogEntry{{AppName: "x", Level: "info", Message: "m", Region: "us-east-1"}}
	rec := postLogs(t, server, token, entries, "", "", "us-east-1")

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rec.Code, rec.Body.String())
	}
	if repo.calls != 1 {
		t.Fatalf("repo called %d times, want 1", repo.calls)
	}
	if got := repo.lastEntries[0].Region; got != "fra" {
		t.Errorf("Region = %q, want %q (body value us-east-1 must be overwritten)", got, "fra")
	}
}

// TestIngestLogs_UnknownFieldAccepted verifies the lenient-decode behavior:
// a struct drift on the worker side (a new field the control plane doesn't
// know) is now accepted (was rejected with 400 under DisallowUnknownFields).
// Syntactically broken bodies still 400 — see TestIngestLogs_BadJSON.
func TestIngestLogs_UnknownFieldAccepted(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_real")

	body := []byte(`{"entries":[{"deployment_id":"d_1","app_name":"x","level":"info","message":"m","future_field":"new"}]}`)
	req := httptest.NewRequest("POST", "/api/internal/logs", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rec.Code, rec.Body.String())
	}
	if repo.calls != 1 {
		t.Errorf("repo should be called once for the valid entry, got %d", repo.calls)
	}
}

// TestIngestLogs_BadJSON verifies that syntactically broken JSON still 400s
// — the lenient-decode change accepts unknown fields but not garbage.
func TestIngestLogs_BadJSON(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_real")

	req := httptest.NewRequest("POST", "/api/internal/logs", bytes.NewReader([]byte(`{"entries": [garbage`)))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if repo.calls != 0 {
		t.Errorf("repo should not be called on bad JSON, got %d calls", repo.calls)
	}
}

// TestIngestLogs_TooManyEntries caps the row count per batch — a worker that
// submits many tiny entries in a 1 MiB body cannot blow past MaxEntries.
func TestIngestLogs_TooManyEntries(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_real")

	entries := make([]domain.LogEntry, MaxEntries+1)
	for i := range entries {
		entries[i] = domain.LogEntry{
			DeploymentID: "d_1",
			AppName:      "x",
			Level:        "info",
			Message:      "m",
		}
	}
	rec := postLogs(t, server, token, entries, "", "", "")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "too many entries") {
		t.Errorf("expected error to mention 'too many entries', got: %s", rec.Body.String())
	}
	if repo.calls != 0 {
		t.Errorf("repo should not be called when entry count exceeds cap, got %d calls", repo.calls)
	}
}

func TestIngestLogs_RepoError(t *testing.T) {
	repo := &mockLogEntryRepo{
		insertBatchFunc: func(ctx context.Context, entries []domain.LogEntry) error {
			return context.DeadlineExceeded
		},
	}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_abc123")

	rec := postLogs(t, server, token, []domain.LogEntry{{AppName: "x", Level: "info"}}, "", "", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal error") {
		t.Errorf("expected error to mention 'internal error', got: %s", rec.Body.String())
	}
}

// This is the security-critical test: a worker lies about tenant_id in the
// body. The handler MUST overwrite it with the JWT's tenant_id before
// inserting. If this test ever fails, the security boundary is broken.
func TestIngestLogs_TenantIDAndWorkerOverwritten(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_real")

	// Body tries to attribute the log to a different tenant and a different
	// worker. The handler must refuse these and use the JWT identity.
	entries := []domain.LogEntry{{AppName: "x", Level: "info", Message: "leak attempt"}}
	rec := postLogs(t, server, token, entries, "t_evil", "w_fra_evil", "us-east-1")

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rec.Code, rec.Body.String())
	}
	if repo.calls != 1 {
		t.Fatalf("repo called %d times, want 1", repo.calls)
	}
	got := repo.lastEntries[0]
	if got.TenantID != "t_real" {
		t.Errorf("TenantID = %q, want t_real (body value t_evil must be overwritten)", got.TenantID)
	}
	if got.WorkerID != "w_fra_real" {
		t.Errorf("WorkerID = %q, want w_fra_real (body value w_fra_evil must be overwritten)", got.WorkerID)
	}
}

// TestIngestLogs_RejectsInvalidLevel pins the level allow-list. Body-
// supplied levels outside the canonical set are rejected with 400 before
// any row is written. Without this guard, a guest that emits "critical"
// or "fatal" would land a row the query endpoint cannot filter on.
func TestIngestLogs_RejectsInvalidLevel(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_real")

	entries := []domain.LogEntry{
		{AppName: "x", Level: "info", Message: "ok"},
		{AppName: "x", Level: "critical", Message: "not canonical"},
	}
	rec := postLogs(t, server, token, entries, "", "", "")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid level") {
		t.Errorf("expected error to mention 'invalid level', got: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "critical") {
		t.Errorf("expected error to echo the offending level, got: %s", rec.Body.String())
	}
	if repo.calls != 0 {
		t.Errorf("repo must not be called when any entry has an invalid level, got %d calls", repo.calls)
	}
}

// TestIngestLogs_AcceptsAllCanonicalLevels verifies the allow-list is
// inclusive for every canonical level. If a future change accidentally
// narrows the set (e.g. dropping "trace"), this test catches it.
func TestIngestLogs_AcceptsAllCanonicalLevels(t *testing.T) {
	repo := &mockLogEntryRepo{}
	server := newIngestLogsServer(repo)
	token := validToken(t, "t_real", "w_fra_real")

	for _, lvl := range []string{"debug", "info", "warn", "error", "trace"} {
		entries := []domain.LogEntry{{AppName: "x", Level: lvl, Message: "m"}}
		rec := postLogs(t, server, token, entries, "", "", "")
		if rec.Code != http.StatusNoContent {
			t.Errorf("level=%q: status = %d, want 204; body=%s", lvl, rec.Code, rec.Body.String())
		}
	}
	if got := repo.calls; got != 5 {
		t.Errorf("repo called %d times, want 5 (one per canonical level)", got)
	}
}

// validTokenWithSecret is a variant that lets a test sign with a non-default
// secret (for the AuthInvalid test).
func validTokenWithSecret(t *testing.T, tenantID, workerID, secret string) string {
	t.Helper()
	claims := &middleware.WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    testJWTIssuer,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
		WorkerID: workerID,
		TenantID: tenantID,
		Region:   "fra",
		Apps:     []string{},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return signed
}
