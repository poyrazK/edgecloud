package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// MaxLogBatchSize caps the size of a single POST /api/internal/logs body.
// 1 MiB is well above any plausible batch (the worker batcher flushes every
// 1s/100 entries, and each entry is typically <1 KiB). The cap bounds per-
// request memory and per-DB-row-count; oversize requests are rejected before
// we touch the DB.
const MaxLogBatchSize = 1 << 20

// MaxEntries caps the number of log entries in a single batch. Matches the
// worker's `max_buffer_len * HARD_CAP_MULT = 1000` ceiling so any worker
// batch always fits. A runaway worker cannot submit unbounded rows in one
// POST even with tiny messages.
const MaxEntries = 1000

// validLevels is the canonical set of log-level strings the runtime emits.
// The WIT `edge:observe/log-level` enum is typed on the runtime side, but
// the ingest wire is still stringly-typed — body-supplied levels are stored
// verbatim, so a guest that emits "critical" or "fatal" today gets a row
// the query endpoint cannot index by. We reject anything outside this set at
// the ingest boundary so non-canonical values are a clean 400 instead of a
// silently-orphaned record.
//
// This is the interim fix; the full fix is a WIT change (typed emit_log
// enum) tracked separately.
var validLevels = map[string]struct{}{
	"debug": {},
	"info":  {},
	"warn":  {},
	"error": {},
	"trace": {},
}

// logEntryRepo is the subset of *repository.LogEntryRepository used here.
// Defining it locally keeps tests mockable without a live DB.
type logEntryRepo interface {
	InsertBatch(ctx context.Context, entries []domain.LogEntry) error
}

// IngestLogsRequest is the JSON body the worker sends to /api/internal/logs.
//
// The JSON shape is intentionally lenient: unknown fields are accepted (a
// future worker struct drift becomes a no-op instead of a 400 that drops
// the entire batch). Syntactically broken bodies still 400.
type IngestLogsRequest struct {
	Entries []domain.LogEntry `json:"entries"`
}

// IngestLogs handles POST /api/internal/logs — tenant log ingest from workers.
//
// Auth: WorkerAuth middleware (HMAC-SHA256 JWT). The handler:
//   - rejects empty JWT identity claims (tenant_id/worker_id/region) up
//     front so a buggy admin tool or test fixture that mints a token with
//     a blank claim cannot insert orphan rows the query endpoint cannot
//     filter on. Returns 400 (not 401) because auth already passed; the
//     failure is in claim shape, not token validity.
//   - caps request body at MaxLogBatchSize
//   - caps the number of entries per batch at MaxEntries
//   - validates each entry's level against the canonical set (debug/info/
//     warn/error/trace) so a non-canonical value doesn't land an orphan
//     row. Runs BEFORE the JWT overwrite loop so a rejected batch cannot
//     attribute a (still-stamped) row to a real tenant.
//   - overwrites each entry's TenantID, WorkerID, and Region with the JWT's
//     claims (the worker can lie in the body, but the JWT is the truth —
//     this is the security boundary that prevents a compromised worker
//     from attributing its logs to another tenant, worker, or region)
//   - trusts DeploymentID, AppName, Level, Message, Labels from the body
//
// Response: 204 No Content on success. 400 on malformed body, oversize
// batch, too many entries, empty JWT identity, or non-canonical level.
// 401 is handled by WorkerAuth before the handler runs. 500 on DB failure.
func (h *InternalHandler) IngestLogs(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetWorkerTenantID(r.Context())
	workerID := middleware.GetWorkerID(r.Context())
	region := middleware.GetWorkerRegion(r.Context())

	// Reject empty JWT identity up front. A token that passes
	// WorkerAuth (HMAC + exp + iss) but carries a blank tenant_id,
	// worker_id, or region claim would land orphan rows that
	// aggregation / billing queries can't distinguish from auth bugs.
	// 400 (not 401): auth passed; the failure is in claim shape.
	if tenantID == "" || workerID == "" || region == "" {
		log.Printf("ingest logs: missing identity (tenant=%q worker=%q region=%q)", tenantID, workerID, region)
		http.Error(w, `{"error": "invalid worker identity"}`, http.StatusBadRequest)
		return
	}

	// Cap request body before decoding. MaxBytesReader returns a
	// *http.MaxBytesError when the (N+1)-th read past the cap is attempted.
	r.Body = http.MaxBytesReader(w, r.Body, MaxLogBatchSize)
	defer func() {
		if err := r.Body.Close(); err != nil {
			log.Printf("IngestLogs: failed to close request body: %v", err)
		}
	}()

	var req IngestLogsRequest
	// Lenient decode: unknown fields are accepted so a future worker struct
	// drift doesn't drop the whole batch. Syntactically broken JSON still
	// surfaces as a decode error below.
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		// MaxBytesReader returns a *http.MaxBytesError when the body exceeds
		// the cap; io.EOF for empty body; everything else is shape/parse.
		// Surface all as 400 with a generic message so we don't leak parser
		// internals to the worker.
		var maxErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxErr):
			http.Error(w, `{"error": "batch too large"}`, http.StatusBadRequest)
		case errors.Is(err, io.EOF):
			http.Error(w, `{"error": "empty body"}`, http.StatusBadRequest)
		default:
			http.Error(w, `{"error": "invalid request body"}`, http.StatusBadRequest)
		}
		return
	}

	// Reject empty batches early — saves a roundtrip and matches the
	// repository's no-op behavior.
	if len(req.Entries) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Cap the entry count so a worker that submits many tiny entries in a
	// 1 MiB body cannot blow past any per-batch row budget.
	if len(req.Entries) > MaxEntries {
		http.Error(w, `{"error": "too many entries"}`, http.StatusBadRequest)
		return
	}

	// Validate each entry's level against the canonical set. Body-supplied
	// levels are stored verbatim, so a non-canonical value ("critical",
	// "fatal", etc.) would land in the logs table and break the level
	// filter on the query side. Reject the whole batch with 400 so the
	// guest sees a clean failure instead of an orphaned record.
	for i, e := range req.Entries {
		if _, ok := validLevels[e.Level]; !ok {
			http.Error(w,
				fmt.Sprintf(`{"error": "invalid level: %q"}`, e.Level),
				http.StatusBadRequest)
			log.Printf("ingest logs: invalid level %q at entry[%d] (tenant=%s)", e.Level, i, tenantID)
			return
		}
	}

	// Overwrite authoritative fields from the JWT. We trust the JWT, not
	// the body, for tenant/worker/region identity. This is the security
	// boundary: a worker that lies about TenantID/WorkerID/Region in the
	// body gets its logs filed under the JWT's values.
	for i := range req.Entries {
		req.Entries[i].TenantID = tenantID
		req.Entries[i].WorkerID = workerID
		req.Entries[i].Region = region
	}

	if err := h.logEntryRepo.InsertBatch(r.Context(), req.Entries); err != nil {
		log.Printf("ingest logs: insert failed (tenant=%s, count=%d): %v", tenantID, len(req.Entries), err)
		http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
