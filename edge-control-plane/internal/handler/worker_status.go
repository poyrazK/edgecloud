package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// AppWorkerStatusLookup is the narrow contract the handler needs to
// answer "what is the worker-reported status of (tenant, app)?". Kept
// separate from the full *service.WorkerService so handler tests can
// stub the one method without standing up a NATS connection, a worker
// repo, a quota repo, and an active repo. The production wiring
// (cmd/api/main.go) satisfies this interface implicitly via the real
// *service.WorkerService.
type AppWorkerStatusLookup interface {
	GetAppStatus(ctx context.Context, tenantID, appName string) (*domain.AppWorkerStatus, error)
}

// WorkerStatusHandler serves GET /api/v1/apps/{appName}/status. The
// response is the AppWorkerStatus struct populated by
// WorkerService.GetAppStatus; a "no data" condition surfaces as 200
// with `{ "status": "unknown" }` (never 404), so a probing tenant
// cannot distinguish "no such app" from "exists but is not yours".
//
// Staleness: there is no server-side TTL on worker_status.last_report
// (see service/worker.go handleHeartbeat). A dead worker leaves its
// last-known status behind indefinitely. The CLI applies a
// client-side staleness check (5 minutes) before deciding whether to
// surface the "crashed → rollback" hint, but the endpoint itself
// returns the raw row so a future consumer (e.g. a dashboard) can
// make its own policy decision.
type WorkerStatusHandler struct {
	workerSvc AppWorkerStatusLookup
}

func NewWorkerStatusHandler(workerSvc AppWorkerStatusLookup) *WorkerStatusHandler {
	return &WorkerStatusHandler{workerSvc: workerSvc}
}

// Get handles GET /api/v1/apps/{appName}/status.
//
// Status codes:
//
//	200  envelope { app_name, status, last_heartbeat, region, worker_id, exit_code? }
//	400  invalid appName (path traversal or empty)
//	500  unexpected service error
func (h *WorkerStatusHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")

	// Path-traversal guard first: same `validateAppName` every other
	// per-app handler uses. Mirrors deployment.go:361-367.
	if !validateAppName(w, appName) {
		return
	}

	status, err := h.workerSvc.GetAppStatus(r.Context(), tenantID, appName)
	if err != nil {
		// Service errors here are unexpected — the repo only returns
		// (nil, nil) for cross-tenant or no-data. A real DB error is
		// the only thing that should land here, and it's a 500.
		log.Printf("worker status: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	// json.NewEncoder never returns a meaningful error here — the
	// httptest.Recorder swallows it on the test side, and in
	// production the connection drop is the real failure mode, not
	// the encode. The pattern matches every other handler in this
	// package (see LogHandler.List).
	_ = json.NewEncoder(w).Encode(status)
}
