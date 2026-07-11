package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// fakeSyncRequester is the test double for the syncRequester
// interface. It records every RequestSync call so the RegisterWorker
// tests can assert (tenantID, region) without standing up the full
// ReconcileService (DB + NATS + publisher + repos).
type fakeSyncRequester struct {
	mu    sync.Mutex
	calls []syncCall
}

type syncCall struct {
	tenantID string
	region   string
}

func (f *fakeSyncRequester) RequestSync(_ context.Context, tenantID, region string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, syncCall{tenantID: tenantID, region: region})
}

func (f *fakeSyncRequester) callsCopy() []syncCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]syncCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// fakeWorkerSvc is the test double for the workerRegisterer
// interface. By default Register returns nil (success); individual
// tests can swap in a stub that returns an error. Also covers the
// Sync endpoint's worker lookup — see worker/getErr fields.
//
// RegisterWorker tests leave worker/getErr at their zero values
// (worker=nil, getErr=nil), and the RegisterWorker code path only
// reads Register; the unused fields are harmless on that path.
// Sync tests inject worker + getErr to drive the cross-tenant check.
type fakeWorkerSvc struct {
	registerErr error
	worker      *domain.Worker // returned by Get; nil means (nil, nil)
	getErr      error
}

func (f *fakeWorkerSvc) Register(_ context.Context, _ string, _ *domain.RegisterWorkerRequest) error {
	return f.registerErr
}

func (f *fakeWorkerSvc) ListByTenant(_ context.Context, _ string) ([]domain.Worker, error) {
	return nil, nil
}

func (f *fakeWorkerSvc) Get(_ context.Context, _ string) (*domain.Worker, error) {
	return f.worker, f.getErr
}

// withWorkerCtx returns a copy of the request with the worker tenant
// id attached via the same context key the middleware uses. The
// handler trusts this value (the same as it would after
// middleware.WorkerAuth validates a real JWT).
func withWorkerCtx(r *http.Request, tenantID string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), middleware.WorkerTenantIDKey, tenantID))
}

func TestRegisterWorker_TriggersRequestSync(t *testing.T) {
	// On-register hook (issue #53): after a successful upsert, the
	// handler must call reconcileSvc.RequestSync with the request's
	// tenantID and region. The call is fire-and-forget so we wait
	// briefly for the goroutine to land.
	syncer := &fakeSyncRequester{}
	h := NewInternalHandler(nil, &fakeWorkerSvc{}, nil, nil, syncer, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)

	body, _ := json.Marshal(domain.RegisterWorkerRequest{
		WorkerID: "w_us-east_1",
		Region:   "us-east",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/internal/workers", bytes.NewReader(body))
	req = withWorkerCtx(req, "t_test")
	rr := httptest.NewRecorder()

	h.RegisterWorker(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status=%d, want 201; body=%s", rr.Code, rr.Body.String())
	}
	// Goroutine is fire-and-forget; poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(syncer.callsCopy()) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	calls := syncer.callsCopy()
	if len(calls) != 1 {
		t.Fatalf("RequestSync calls=%d, want 1", len(calls))
	}
	if calls[0].tenantID != "t_test" {
		t.Errorf("tenantID=%q, want t_test", calls[0].tenantID)
	}
	if calls[0].region != "us-east" {
		t.Errorf("region=%q, want us-east", calls[0].region)
	}
}

func TestRegisterWorker_NilSyncer_DoesNotPanic(t *testing.T) {
	// When reconcileSvc is nil (e.g. test stubs that don't wire the
	// sync hook) the handler must still return 201 and skip the sync.
	// The periodic timer in cmd/api/main.go is the durable safety net.
	h := NewInternalHandler(nil, &fakeWorkerSvc{}, nil, nil, nil, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)

	body, _ := json.Marshal(domain.RegisterWorkerRequest{
		WorkerID: "w_us-east_1",
		Region:   "us-east",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/internal/workers", bytes.NewReader(body))
	req = withWorkerCtx(req, "t_test")
	rr := httptest.NewRecorder()

	// Must not panic on nil syncer.
	h.RegisterWorker(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("status=%d, want 201 (nil syncer should not affect response)", rr.Code)
	}
}

func TestRegisterWorker_FailedRegister_DoesNotTriggerSync(t *testing.T) {
	// If the upsert itself fails (validation, quota, etc.) the
	// response is the error and NO RequestSync call should fire —
	// publishing a full_sync for a worker we didn't register would
	// race with the upsert and produce a stale payload.
	syncer := &fakeSyncRequester{}
	// Exercise the missing-field validation path, which is the most
	// common "register fails" scenario in production.
	h := NewInternalHandler(nil, &fakeWorkerSvc{}, nil, nil, syncer, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)

	body, _ := json.Marshal(domain.RegisterWorkerRequest{
		WorkerID: "w_us-east_1",
		// Region intentionally omitted — handler should reject.
	})
	req := httptest.NewRequest(http.MethodPost, "/api/internal/workers", bytes.NewReader(body))
	req = withWorkerCtx(req, "t_test")
	rr := httptest.NewRecorder()

	h.RegisterWorker(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400 (missing region)", rr.Code)
	}
	if got := len(syncer.callsCopy()); got != 0 {
		t.Errorf("RequestSync calls=%d, want 0 (failed register must not publish)", got)
	}
}

func TestRegisterWorker_InvalidBody_DoesNotTriggerSync(t *testing.T) {
	syncer := &fakeSyncRequester{}
	h := NewInternalHandler(nil, &fakeWorkerSvc{}, nil, nil, syncer, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/internal/workers", bytes.NewReader([]byte("{not json")))
	req = withWorkerCtx(req, "t_test")
	rr := httptest.NewRecorder()

	h.RegisterWorker(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status=%d, want 400", rr.Code)
	}
	if got := len(syncer.callsCopy()); got != 0 {
		t.Errorf("RequestSync calls=%d, want 0 (bad body must not publish)", got)
	}
}

func TestRegisterWorker_RegisterError_DoesNotTriggerSync(t *testing.T) {
	// When Register returns a non-validation error (e.g. quota
	// exceeded, region mismatch, db failure) the handler must respond
	// with the mapped status code and skip the sync hook — same
	// invariant as the validation path.
	syncer := &fakeSyncRequester{}
	worker := &fakeWorkerSvc{registerErr: service.ErrQuotaExceeded}
	h := NewInternalHandler(nil, worker, nil, nil, syncer, nil, "", "", "", middleware.WorkerJWTConfig{}, 0, "", "", nil, nil, nil)

	body, _ := json.Marshal(domain.RegisterWorkerRequest{
		WorkerID: "w_us-east_1",
		Region:   "us-east",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/internal/workers", bytes.NewReader(body))
	req = withWorkerCtx(req, "t_test")
	rr := httptest.NewRecorder()

	h.RegisterWorker(rr, req)

	// 429 QuotaExceeded is the documented mapping in the handler.
	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("status=%d, want 429 (quota exceeded)", rr.Code)
	}
	if got := len(syncer.callsCopy()); got != 0 {
		t.Errorf("RequestSync calls=%d, want 0 (register error must not publish)", got)
	}
}
