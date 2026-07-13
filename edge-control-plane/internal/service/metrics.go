package service

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// MetricsAggregator collects per-app metric samples pushed via heartbeats
// and renders them as Prometheus text-format output.
//
// Data is held in memory only — no DB persistence. Each heartbeat replaces
// the previous batch for a given (tenantID, appName) pair, matching the
// worker's subtract_delta model where counters reflect the delta since the
// last heartbeat rather than a cumulative total.
//
// Background-GC metrics (issue #581) live alongside the per-app metrics
// under the same a.mu lock. The Record* closures take a.mu.Lock(); readers
// (RenderAll / RenderTenant) take a.mu.RLock(). Readers see either the
// pre-update or post-update state — never a half-updated counter.
type MetricsAggregator struct {
	mu sync.RWMutex
	// tenantID → appName → []sample
	data map[string]map[string]appMetrics
	// Fleet-wide, label-free metrics stamped by every heartbeat
	// (issue #641). Single value across all workers — see
	// `workerMetrics` for the field list.
	worker workerMetrics

	// log_gc (4 families)
	logGcTick int64
	logGcRow  int64
	logGcErr  int64
	logGcTime int64

	// preview_gc (7 families)
	previewGcTick     int64
	previewGcBlob     int64
	previewGcRow      int64
	previewGcBatch    int64
	previewGcErr      int64
	previewGcBlobFail int64
	previewGcTime     int64

	// cache_retry_sweep (9 families)
	cacheRetrySweepTick          int64
	cacheRetrySweepBatch         int64
	cacheRetrySweepRow           int64
	cacheRetrySweepPushedOK      int64
	cacheRetrySweepStillFailing  int64
	cacheRetrySweepConfigMissing int64
	cacheRetrySweepGivenUp       int64
	cacheRetrySweepErr           int64
	cacheRetrySweepTime          int64

	// audit_gc (4 families, issue #574 retention GC trio)
	auditGcTick int64
	auditGcRow  int64
	auditGcErr  int64
	auditGcTime int64

	// webhook_delivery_gc (4 families, issue #574 retention GC trio)
	webhookDeliveryGcTick int64
	webhookDeliveryGcRow  int64
	webhookDeliveryGcErr  int64
	webhookDeliveryGcTime int64

	// autoscale_event_gc (4 families, issue #574 retention GC trio)
	autoscaleEventGcTick int64
	autoscaleEventGcRow  int64
	autoscaleEventGcErr  int64
	autoscaleEventGcTime int64

	// worker_enroll (3 families, issue #430 per-worker enrollment
	// observability). Counts every outcome of POST
	// /api/internal/worker-bootstrap/enroll so an operator can detect
	// "fleet enrollment failure spike" before any single worker 401s
	// long enough to alert.
	workerEnrollTotal int64
	workerEnrollErrs  int64
	workerEnrollTime  int64

	// migrate_preflight (issue #622 commit 6) — a bounded-cardinality
	// counter keyed by (language, reason). Labels are sourced from
	// the closed `migratePreflightReasons` slice + the
	// preflightLanguage set ({"c","rust"}); anything outside the
	// closed slice is coalesced onto a default bucket so a future
	// reason that ships without updating the slice still surfaces
	// in the metric.
	migratePreflight map[[2]string]int64
}

type appMetrics struct {
	requestCount  uint64
	outboundBytes uint64
	samples       []domain.MetricSample
}

// workerMetrics holds label-free, fleet-wide metrics stamped by every
// heartbeat. Today there is just the port-pool exhaustion counter
// (issue #641); future PRs can add `cpu_pct`, `mem_pct`, etc. without
// touching the per-app path. Mutated under a.mu.
type workerMetrics struct {
	// portPoolExhausted is the cumulative count of `PortPool::acquire() → None`
	// events across all workers since process boot. Reset on CP restart
	// (matches the worker's per-process-boot semantics). Surfaced as
	// `edge_worker_port_pool_exhausted_total` — alert on a non-zero rate.
	portPoolExhausted uint64
}

// NewMetricsAggregator returns a ready-to-use aggregator.
func NewMetricsAggregator() *MetricsAggregator {
	return &MetricsAggregator{
		data:             make(map[string]map[string]appMetrics),
		migratePreflight: make(map[[2]string]int64),
	}
}

// Ingest records the metric samples for one (tenantID, appName) pair reported
// in a heartbeat. It also ingests the built-in request_count and
// outbound_bytes so all per-app metrics are served from one place.
func (a *MetricsAggregator) Ingest(tenantID, appName string, requestCount, outboundBytes uint64, samples []domain.MetricSample) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.data[tenantID]; !ok {
		a.data[tenantID] = make(map[string]appMetrics)
	}
	a.data[tenantID][appName] = appMetrics{
		requestCount:  requestCount,
		outboundBytes: outboundBytes,
		samples:       samples,
	}
}

// IngestWorker records the worker-level (label-free) metrics from a
// single heartbeat. Today's only field is the port-pool exhaustion
// counter (issue #641); future worker-level metrics land here without
// touching the per-app path. `portPoolExhaustedCount` is treated as
// the LATEST worker-stamped value (not a delta) — workers emit their
// own per-process-boot cumulative, so the CP just renders the most
// recent value across the fleet.
func (a *MetricsAggregator) IngestWorker(portPoolExhaustedCount uint64) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.worker.portPoolExhausted = portPoolExhaustedCount
}

// ---------------------------------------------------------------------------
// Background-GC metric sinks (issue #581).
//
// Each background GC (LogGC, PreviewGC, CacheRetrySweep) calls into a
// typed closure after every sweep tick. The closure bumps the relevant
// counters/gauges on the aggregator; emit() reads them at render time.
// Concurrency: every closure takes a.mu.Lock() so a scrape in flight
// (RLock) either sees the pre-update state or the post-update state
// — never a half-updated counter. nil-receiver guards return no-op
// closures so unit tests can pass `nil` to the GC constructors.
// ---------------------------------------------------------------------------

// LogGCSink records one LogGCService tick outcome.
type LogGCSink func(rowsDeleted int64, hadError bool)

// PreviewGCSink records one PreviewGCService tick outcome. The per-blob
// failure counter is recorded separately via PreviewBlobFailureRecorder
// (kept distinct so the per-tick signature stays at 4 args and the call
// site in PreviewGCService.runOnce is unambiguous).
type PreviewGCSink func(blobsDeleted, rowsDeleted, batchesSwept int, hadError bool)

// PreviewBlobFailureRecorder bumps the per-blob failure counter. Called
// from the inner loop when a single blob Delete returns an error.
type PreviewBlobFailureRecorder func()

// CacheRetrySweepSink records one CacheRetrySweepService tick outcome.
// The four middle ints are the per-region partition totals from the
// sweep (success / stillFailing / configMissing / giveUp).
type CacheRetrySweepSink func(rowsTouched, pushedOK, stillFailing, configMissing, givenUp, batchesSwept int, hadError bool)

// AuditGCSink records one AuditGCService tick outcome. Mirrors
// LogGCSink (issue #574 retention GC trio).
type AuditGCSink func(rowsDeleted int64, hadError bool)

// WebhookDeliveryGCSink records one WebhookDeliveryGCService tick
// outcome. Mirrors LogGCSink (issue #574 retention GC trio).
type WebhookDeliveryGCSink func(rowsDeleted int64, hadError bool)

// AutoscaleEventGCSink records one AutoscaleEventGCService tick
// outcome. Mirrors LogGCSink (issue #574 retention GC trio).
type AutoscaleEventGCSink func(rowsDeleted int64, hadError bool)

// NewLogGCSink returns a sink that bumps the log_gc families. Passing
// the returned closure to LogGCService records one tick.
//
// The timestamp is captured inside the closure body (not at factory
// call time) so a long-lived sink — wired once at boot by app.New
// and called per tick — actually reflects the time of the most
// recent tick. The closure is the documented surface for the
// "alert on staleness" rule in CLAUDE.md.
func (a *MetricsAggregator) NewLogGCSink() LogGCSink {
	if a == nil {
		return func(int64, bool) {}
	}
	return func(rowsDeleted int64, hadError bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.logGcTick++
		a.logGcRow += rowsDeleted
		if hadError {
			a.logGcErr++
		}
		a.logGcTime = time.Now().Unix()
	}
}

// NewPreviewGCSink returns a sink that bumps the preview_gc families
// (per-tick counters). The per-blob failure counter is recorded
// separately via NewPreviewBlobFailureRecorder.
//
// Timestamp is captured inside the closure body so a long-lived sink
// wired once at boot reflects the actual time of the most recent tick.
func (a *MetricsAggregator) NewPreviewGCSink() PreviewGCSink {
	if a == nil {
		return func(int, int, int, bool) {}
	}
	return func(blobsDeleted, rowsDeleted, batchesSwept int, hadError bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.previewGcTick++
		a.previewGcBlob += int64(blobsDeleted)
		a.previewGcRow += int64(rowsDeleted)
		a.previewGcBatch += int64(batchesSwept)
		if hadError {
			a.previewGcErr++
		}
		a.previewGcTime = time.Now().Unix()
	}
}

// NewPreviewBlobFailureRecorder returns a closure that bumps the
// per-blob failure counter. Called from the inner per-blob loop in
// PreviewGCService.runOnce.
func (a *MetricsAggregator) NewPreviewBlobFailureRecorder() PreviewBlobFailureRecorder {
	if a == nil {
		return func() {}
	}
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.previewGcBlobFail++
	}
}

// NewCacheRetrySweepSink returns a sink that bumps the cache_retry_sweep
// families.
//
// Timestamp is captured inside the closure body so a long-lived sink
// wired once at boot reflects the actual time of the most recent tick.
func (a *MetricsAggregator) NewCacheRetrySweepSink() CacheRetrySweepSink {
	if a == nil {
		return func(int, int, int, int, int, int, bool) {}
	}
	return func(rowsTouched, pushedOK, stillFailing, configMissing, givenUp, batchesSwept int, hadError bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.cacheRetrySweepTick++
		a.cacheRetrySweepBatch += int64(batchesSwept)
		a.cacheRetrySweepRow += int64(rowsTouched)
		a.cacheRetrySweepPushedOK += int64(pushedOK)
		a.cacheRetrySweepStillFailing += int64(stillFailing)
		a.cacheRetrySweepConfigMissing += int64(configMissing)
		a.cacheRetrySweepGivenUp += int64(givenUp)
		if hadError {
			a.cacheRetrySweepErr++
		}
		a.cacheRetrySweepTime = time.Now().Unix()
	}
}

// NewAuditGCSink returns a sink that bumps the audit_gc families.
// Mirrors NewLogGCSink (issue #574 retention GC trio).
//
// Timestamp is captured inside the closure body so a long-lived sink
// wired once at boot reflects the actual time of the most recent tick.
func (a *MetricsAggregator) NewAuditGCSink() AuditGCSink {
	if a == nil {
		return func(int64, bool) {}
	}
	return func(rowsDeleted int64, hadError bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.auditGcTick++
		a.auditGcRow += rowsDeleted
		if hadError {
			a.auditGcErr++
		}
		a.auditGcTime = time.Now().Unix()
	}
}

// NewWebhookDeliveryGCSink returns a sink that bumps the
// webhook_delivery_gc families. Mirrors NewLogGCSink (issue #574
// retention GC trio).
//
// Timestamp is captured inside the closure body so a long-lived sink
// wired once at boot reflects the actual time of the most recent tick.
func (a *MetricsAggregator) NewWebhookDeliveryGCSink() WebhookDeliveryGCSink {
	if a == nil {
		return func(int64, bool) {}
	}
	return func(rowsDeleted int64, hadError bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.webhookDeliveryGcTick++
		a.webhookDeliveryGcRow += rowsDeleted
		if hadError {
			a.webhookDeliveryGcErr++
		}
		a.webhookDeliveryGcTime = time.Now().Unix()
	}
}

// NewAutoscaleEventGCSink returns a sink that bumps the
// autoscale_event_gc families. Mirrors NewLogGCSink (issue #574
// retention GC trio).
//
// Timestamp is captured inside the closure body so a long-lived sink
// wired once at boot reflects the actual time of the most recent tick.
func (a *MetricsAggregator) NewAutoscaleEventGCSink() AutoscaleEventGCSink {
	if a == nil {
		return func(int64, bool) {}
	}
	return func(rowsDeleted int64, hadError bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.autoscaleEventGcTick++
		a.autoscaleEventGcRow += rowsDeleted
		if hadError {
			a.autoscaleEventGcErr++
		}
		a.autoscaleEventGcTime = time.Now().Unix()
	}
}

// WorkerEnrollSink records one outcome of the per-worker enrollment
// endpoint (POST /api/internal/worker-bootstrap/enroll, issue #430).
// Pass `hadError=true` for any non-2xx response (challenge replay,
// signature mismatch, persistence failure, etc.); the success path
// is implicit (errors_total stays still, total increments).
//
// Three families:
//   - edge_worker_enroll_total (counter)
//   - edge_worker_enroll_errors_total (counter)
//   - edge_worker_enroll_last_enroll_timestamp_seconds (gauge)
//
// Operators alert on a sustained errors/total ratio > 5% over a 5m
// window — that's the "fleet can't enroll after a JWT_SECRET
// rotation" early signal.
type WorkerEnrollSink func(hadError bool)

// NewWorkerEnrollSink returns a sink that bumps the worker_enroll
// families. Mirrors the GC sink pattern (issue #581).
//
// Timestamp is captured inside the closure body so a long-lived sink
// wired once at boot reflects the actual time of the most recent
// enrollment attempt.
func (a *MetricsAggregator) NewWorkerEnrollSink() WorkerEnrollSink {
	if a == nil {
		return func(bool) {}
	}
	return func(hadError bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.workerEnrollTotal++
		if hadError {
			a.workerEnrollErrs++
		}
		a.workerEnrollTime = time.Now().Unix()
	}
}

// ---------------------------------------------------------------------------
// Migration preflight metric (issue #622 commit 6).
//
// Distinct from the GC sinks because the L1 preflight rejection
// happens at the HTTP boundary and carries two labels (language,
// reason). Bounded-cardinality: the reason slice is closed (any
// future reason must add a row here AND update the
// TestMigratePreflightSink_AllReasonsCovered test), and language
// is filtered to {"c","rust"} — anything else falls into a default
// bucket so a misbehaving caller can't blow up Prometheus
// cardinality.
//
// Counter semantics: cumulative since process boot (each upload
// that the preflight rejects bumps the counter by len(matches)).
// An operator watching a dashboard sees the rejection rate via
// Prometheus `rate()` over a window; the raw counter is for
// debugging single-event spikes.
// ---------------------------------------------------------------------------

// migratePreflightReasons is the closed set of preflight reason
// labels shipped to Prometheus. Must stay in sync with the
// `preflightReason*` constants in
// internal/handler/migration_preflight.go. Drift between the two
// surfaces is caught by TestMigratePreflightSink_AllReasonsCovered.
//
// Adding a new reason is a deliberate security decision: a new
// pattern class is being denied at the HTTP boundary. Update the
// preflight scanner, the reason constant, this slice, the handler
// test, AND the runbook — keep them all in lockstep.
var migratePreflightReasons = []string{
	"include_bytes",     // include_bytes!(...)
	"include_str",       // include_str!(...)
	"include_macro",     // include!(...)
	"env_macro",         // env!(...)
	"option_env",        // option_env!(...)
	"compile_error",     // compile_error!(...)
	"path_attr",         // #[path = "..."]
	"include_attr",      // #[include = "..."]
	"absolute_include",  // #include "/etc/..." / "<...:>"
	"traversal_include", // #include "../..." or "../..."
	"embed",             // #embed "..."
}

// MigratePreflightSink is the sink signature the preflight
// handler calls for each rejected upload. The handler iterates
// the match slice and invokes the sink once per (language,
// reason) pair — a single multi-pattern upload bumps multiple
// counters.
type MigratePreflightSink func(language, reason string)

// NewMigratePreflightSink returns a sink that bumps the
// edge_migrate_preflight_rejected_total counter on every preflight
// rejection. nil-receiver safe — returns a no-op closure so tests
// can pass `nil`.
//
// Reason values outside the closed migratePreflightReasons slice
// are coalesced onto "unknown" so a future scanner drift can't
// blow up Prometheus cardinality by emitting arbitrary string
// labels. Language values outside {"c","rust"} are coerced the
// same way. See TestMigratePreflightSink_UnknownReasonCoalesced
// for the regression-guard invariant.
func (a *MetricsAggregator) NewMigratePreflightSink() MigratePreflightSink {
	if a == nil {
		return func(string, string) {}
	}
	return func(language, reason string) {
		language = normalizePreflightLanguage(language)
		reason = normalizePreflightReason(reason)
		a.mu.Lock()
		defer a.mu.Unlock()
		a.migratePreflight[[2]string{language, reason}]++
	}
}

// normalizePreflightLanguage whitelists the language label to the
// bounded {"c","rust"} set. Anything else becomes "unknown" so
// label cardinality stays bounded regardless of caller input.
func normalizePreflightLanguage(s string) string {
	switch s {
	case "c", "rust":
		return s
	default:
		return "unknown"
	}
}

// normalizePreflightReason whitelists the reason label to the
// closed migratePreflightReasons slice. Anything else becomes
// "unknown".
func normalizePreflightReason(s string) string {
	for _, r := range migratePreflightReasons {
		if r == s {
			return s
		}
	}
	return "unknown"
}

// RenderTenant returns a Prometheus text-format string containing only the
// metrics for the given tenant. Returns an empty string when no data has
// been ingested for that tenant yet.
//
// Global background-GC families (edge_log_gc_*, edge_preview_gc_*,
// edge_cache_retry_sweep_*) are included on every per-tenant render —
// operators want tenants to see fleet-wide GC health on their own
// /api/v1/metrics page (per user decision on issue #581).
func (a *MetricsAggregator) RenderTenant(tenantID string) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var b strings.Builder
	if apps, ok := a.data[tenantID]; ok {
		var fl familyLines
		collectFamilyLines(&fl, tenantID, apps)
		fl.emit(&b)
	}
	// GC families are emitted separately from familyLines.emit —
	// keep the two paths in sync if you add a new GC family.
	emitGCFamilies(&b, a)
	// Worker-level families (issue #641) — same fleet-wide visibility
	// on every per-tenant render as the GC families, so tenants can
	// see whether the cluster is currently under port-pool pressure
	// from their own /api/v1/metrics page.
	emitWorkerFamilies(&b, a)
	return b.String()
}

// RenderAll returns a Prometheus text-format string containing metrics for
// all tenants. Used by the unauthenticated GET /metrics operator endpoint.
//
// All tenants are collected into shared per-family line slices before emitting
// so each `# TYPE` declaration appears exactly once across the full output —
// Prometheus parsers reject a metric family name whose TYPE line appears more
// than once in a single scrape response.
func (a *MetricsAggregator) RenderAll() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var fl familyLines
	for tenantID, apps := range a.data {
		collectFamilyLines(&fl, tenantID, apps)
	}
	var b strings.Builder
	fl.emit(&b)
	// GC families are emitted separately from familyLines.emit —
	// keep the two paths in sync if you add a new GC family.
	emitGCFamilies(&b, a)
	// Worker-level families (issue #641).
	emitWorkerFamilies(&b, a)
	return b.String()
}

// familyLines holds accumulated series lines for the per-tenant/per-app
// Prometheus metric families. Collecting across all tenants/apps before
// emitting ensures each `# TYPE` appears exactly once in the final output.
type familyLines struct {
	reqCount  []string
	outBytes  []string
	counters  []string
	gauges    []string
	histogram []string
}

func (fl *familyLines) emit(b *strings.Builder) {
	emitFamily(b, "edge_request_count", "gauge", fl.reqCount)
	emitFamily(b, "edge_outbound_bytes", "gauge", fl.outBytes)
	emitFamily(b, "edge_counter", "counter", fl.counters)
	emitFamily(b, "edge_gauge", "gauge", fl.gauges)
	emitFamily(b, "edge_histogram_sample", "untyped", fl.histogram)
}

// emitGCFamilies writes the 20 background-GC families (issue #581) using
// the current aggregator counter values. All label-free, one series per
// family globally — no cardinality concerns. Caller must hold a.mu
// (read or write) so the counter reads are coherent.
func emitGCFamilies(b *strings.Builder, a *MetricsAggregator) {
	fmt.Fprintf(b, "# TYPE edge_log_gc_ticks_total counter\n")
	fmt.Fprintf(b, "edge_log_gc_ticks_total %d\n", a.logGcTick)
	fmt.Fprintf(b, "# TYPE edge_log_gc_rows_deleted_total counter\n")
	fmt.Fprintf(b, "edge_log_gc_rows_deleted_total %d\n", a.logGcRow)
	fmt.Fprintf(b, "# TYPE edge_log_gc_errors_total counter\n")
	fmt.Fprintf(b, "edge_log_gc_errors_total %d\n", a.logGcErr)
	fmt.Fprintf(b, "# TYPE edge_log_gc_last_tick_timestamp_seconds gauge\n")
	fmt.Fprintf(b, "edge_log_gc_last_tick_timestamp_seconds %d\n", a.logGcTime)

	fmt.Fprintf(b, "# TYPE edge_preview_gc_ticks_total counter\n")
	fmt.Fprintf(b, "edge_preview_gc_ticks_total %d\n", a.previewGcTick)
	fmt.Fprintf(b, "# TYPE edge_preview_gc_blobs_deleted_total counter\n")
	fmt.Fprintf(b, "edge_preview_gc_blobs_deleted_total %d\n", a.previewGcBlob)
	fmt.Fprintf(b, "# TYPE edge_preview_gc_rows_deleted_total counter\n")
	fmt.Fprintf(b, "edge_preview_gc_rows_deleted_total %d\n", a.previewGcRow)
	fmt.Fprintf(b, "# TYPE edge_preview_gc_batches_swept_total counter\n")
	fmt.Fprintf(b, "edge_preview_gc_batches_swept_total %d\n", a.previewGcBatch)
	fmt.Fprintf(b, "# TYPE edge_preview_gc_errors_total counter\n")
	fmt.Fprintf(b, "edge_preview_gc_errors_total %d\n", a.previewGcErr)
	fmt.Fprintf(b, "# TYPE edge_preview_gc_blob_delete_failures_total counter\n")
	fmt.Fprintf(b, "edge_preview_gc_blob_delete_failures_total %d\n", a.previewGcBlobFail)
	fmt.Fprintf(b, "# TYPE edge_preview_gc_last_tick_timestamp_seconds gauge\n")
	fmt.Fprintf(b, "edge_preview_gc_last_tick_timestamp_seconds %d\n", a.previewGcTime)

	fmt.Fprintf(b, "# TYPE edge_cache_retry_sweep_ticks_total counter\n")
	fmt.Fprintf(b, "edge_cache_retry_sweep_ticks_total %d\n", a.cacheRetrySweepTick)
	fmt.Fprintf(b, "# TYPE edge_cache_retry_sweep_batches_swept_total counter\n")
	fmt.Fprintf(b, "edge_cache_retry_sweep_batches_swept_total %d\n", a.cacheRetrySweepBatch)
	fmt.Fprintf(b, "# TYPE edge_cache_retry_sweep_rows_touched_total counter\n")
	fmt.Fprintf(b, "edge_cache_retry_sweep_rows_touched_total %d\n", a.cacheRetrySweepRow)
	fmt.Fprintf(b, "# TYPE edge_cache_retry_sweep_pushed_ok_total counter\n")
	fmt.Fprintf(b, "edge_cache_retry_sweep_pushed_ok_total %d\n", a.cacheRetrySweepPushedOK)
	fmt.Fprintf(b, "# TYPE edge_cache_retry_sweep_still_failing_total counter\n")
	fmt.Fprintf(b, "edge_cache_retry_sweep_still_failing_total %d\n", a.cacheRetrySweepStillFailing)
	fmt.Fprintf(b, "# TYPE edge_cache_retry_sweep_config_missing_total counter\n")
	fmt.Fprintf(b, "edge_cache_retry_sweep_config_missing_total %d\n", a.cacheRetrySweepConfigMissing)
	fmt.Fprintf(b, "# TYPE edge_cache_retry_sweep_given_up_total counter\n")
	fmt.Fprintf(b, "edge_cache_retry_sweep_given_up_total %d\n", a.cacheRetrySweepGivenUp)
	fmt.Fprintf(b, "# TYPE edge_cache_retry_sweep_errors_total counter\n")
	fmt.Fprintf(b, "edge_cache_retry_sweep_errors_total %d\n", a.cacheRetrySweepErr)
	fmt.Fprintf(b, "# TYPE edge_cache_retry_sweep_last_tick_timestamp_seconds gauge\n")
	fmt.Fprintf(b, "edge_cache_retry_sweep_last_tick_timestamp_seconds %d\n", a.cacheRetrySweepTime)

	// audit_gc — issue #574 retention GC trio.
	fmt.Fprintf(b, "# TYPE edge_audit_log_gc_ticks_total counter\n")
	fmt.Fprintf(b, "edge_audit_log_gc_ticks_total %d\n", a.auditGcTick)
	fmt.Fprintf(b, "# TYPE edge_audit_log_gc_rows_deleted_total counter\n")
	fmt.Fprintf(b, "edge_audit_log_gc_rows_deleted_total %d\n", a.auditGcRow)
	fmt.Fprintf(b, "# TYPE edge_audit_log_gc_errors_total counter\n")
	fmt.Fprintf(b, "edge_audit_log_gc_errors_total %d\n", a.auditGcErr)
	fmt.Fprintf(b, "# TYPE edge_audit_log_gc_last_tick_timestamp_seconds gauge\n")
	fmt.Fprintf(b, "edge_audit_log_gc_last_tick_timestamp_seconds %d\n", a.auditGcTime)

	// webhook_delivery_gc — issue #574 retention GC trio.
	fmt.Fprintf(b, "# TYPE edge_webhook_delivery_gc_ticks_total counter\n")
	fmt.Fprintf(b, "edge_webhook_delivery_gc_ticks_total %d\n", a.webhookDeliveryGcTick)
	fmt.Fprintf(b, "# TYPE edge_webhook_delivery_gc_rows_deleted_total counter\n")
	fmt.Fprintf(b, "edge_webhook_delivery_gc_rows_deleted_total %d\n", a.webhookDeliveryGcRow)
	fmt.Fprintf(b, "# TYPE edge_webhook_delivery_gc_errors_total counter\n")
	fmt.Fprintf(b, "edge_webhook_delivery_gc_errors_total %d\n", a.webhookDeliveryGcErr)
	fmt.Fprintf(b, "# TYPE edge_webhook_delivery_gc_last_tick_timestamp_seconds gauge\n")
	fmt.Fprintf(b, "edge_webhook_delivery_gc_last_tick_timestamp_seconds %d\n", a.webhookDeliveryGcTime)

	// autoscale_event_gc — issue #574 retention GC trio.
	fmt.Fprintf(b, "# TYPE edge_autoscale_event_gc_ticks_total counter\n")
	fmt.Fprintf(b, "edge_autoscale_event_gc_ticks_total %d\n", a.autoscaleEventGcTick)
	fmt.Fprintf(b, "# TYPE edge_autoscale_event_gc_rows_deleted_total counter\n")
	fmt.Fprintf(b, "edge_autoscale_event_gc_rows_deleted_total %d\n", a.autoscaleEventGcRow)
	fmt.Fprintf(b, "# TYPE edge_autoscale_event_gc_errors_total counter\n")
	fmt.Fprintf(b, "edge_autoscale_event_gc_errors_total %d\n", a.autoscaleEventGcErr)
	fmt.Fprintf(b, "# TYPE edge_autoscale_event_gc_last_tick_timestamp_seconds gauge\n")
	fmt.Fprintf(b, "edge_autoscale_event_gc_last_tick_timestamp_seconds %d\n", a.autoscaleEventGcTime)

	// worker_enroll — issue #430 per-worker enrollment observability.
	// Operators alert on errors_total > 0 sustained over a 5m window,
	// or total = 0 over the same window after a JWT_SECRET rotation
	// (no worker enrolled yet = rotation didn't land).
	fmt.Fprintf(b, "# TYPE edge_worker_enroll_total counter\n")
	fmt.Fprintf(b, "edge_worker_enroll_total %d\n", a.workerEnrollTotal)
	fmt.Fprintf(b, "# TYPE edge_worker_enroll_errors_total counter\n")
	fmt.Fprintf(b, "edge_worker_enroll_errors_total %d\n", a.workerEnrollErrs)
	fmt.Fprintf(b, "# TYPE edge_worker_enroll_last_enroll_timestamp_seconds gauge\n")
	fmt.Fprintf(b, "edge_worker_enroll_last_enroll_timestamp_seconds %d\n", a.workerEnrollTime)

	// migrate_preflight_rejected_total — issue #622 commit 6.
	// Labeled counter. TYPE line is emitted ONLY when at least one
	// series has a non-zero value, so the scrape response stays
	// empty for environments that have never seen a rejection
	// (the operator's dashboard then fails closed on
	// `absent() == true` rather than reading a misleading `0`).
	if len(a.migratePreflight) > 0 {
		fmt.Fprintf(b, "# TYPE edge_migrate_preflight_rejected_total counter\n")
		// Iterate in declared order so the scrape output is
		// deterministic (Prometheus doesn't require ordering,
		// but stable ordering makes test assertions simpler).
		// The "unknown" reason bucket sorts last per language
		// so the closed set is enumerated first.
		for _, lang := range []string{"c", "rust", "unknown"} {
			for _, reason := range migratePreflightReasons {
				if v := a.migratePreflight[[2]string{lang, reason}]; v > 0 {
					fmt.Fprintf(b, "edge_migrate_preflight_rejected_total{language=%q,reason=%q} %d\n", lang, reason, v)
				}
			}
			if v := a.migratePreflight[[2]string{lang, "unknown"}]; v > 0 {
				fmt.Fprintf(b, "edge_migrate_preflight_rejected_total{language=%q,reason=%q} %d\n", lang, "unknown", v)
			}
		}
	}
}

// emitWorkerFamilies writes the worker-level (label-free) metric
// families stamped by every heartbeat (issue #641). Today there is
// just the port-pool exhaustion counter; future PRs add to this
// function. Caller must hold a.mu (read or write) so the counter
// reads are coherent.
//
// `edge_worker_port_pool_exhausted_total` reflects the latest
// worker-stamped value across the fleet. Workers emit their own
// per-process-boot cumulative; the CP just renders the most recent
// value seen. The metric TYPE is `counter` because that is the
// Prometheus-correct semantic for "count of exhaustion events" and
// aligns with how operators will alert on it
// (`rate(edge_worker_port_pool_exhausted_total[5m]) > 0`).
func emitWorkerFamilies(b *strings.Builder, a *MetricsAggregator) {
	fmt.Fprintf(b, "# TYPE edge_worker_port_pool_exhausted_total counter\n")
	fmt.Fprintf(b, "edge_worker_port_pool_exhausted_total %d\n", a.worker.portPoolExhausted)
}

// collectFamilyLines appends series strings for every app in one tenant into
// fl. Callers that need to merge multiple tenants call this once per tenant
// before a single fl.emit — guaranteeing one TYPE line per family.
func collectFamilyLines(fl *familyLines, tenantID string, apps map[string]appMetrics) {
	for appName, m := range apps {
		baseLabels := "tenant_id=" + promQuoteLabelValue(tenantID) + ",app=" + promQuoteLabelValue(appName)
		fl.reqCount = append(fl.reqCount, fmt.Sprintf("edge_request_count{%s} %d", baseLabels, m.requestCount))
		fl.outBytes = append(fl.outBytes, fmt.Sprintf("edge_outbound_bytes{%s} %d", baseLabels, m.outboundBytes))

		for _, s := range m.samples {
			labelStr := buildLabelStr(baseLabels, s.Labels)
			metricLabel := "metric=" + promQuoteLabelValue(s.Name)
			switch s.Kind {
			case domain.MetricKindCounter:
				fl.counters = append(fl.counters, fmt.Sprintf("edge_counter{%s,%s} %g", labelStr, metricLabel, s.Value))
			case domain.MetricKindGauge:
				fl.gauges = append(fl.gauges, fmt.Sprintf("edge_gauge{%s,%s} %g", labelStr, metricLabel, s.Value))
			case domain.MetricKindHistogramSample:
				fl.histogram = append(fl.histogram, fmt.Sprintf("edge_histogram_sample{%s,%s} %g", labelStr, metricLabel, s.Value))
			}
		}
	}
}

// emitFamily writes a Prometheus text-format metric family: one TYPE line
// followed by all series lines. Skipped entirely if lines is empty.
func emitFamily(b *strings.Builder, name, typeName string, lines []string) {
	if len(lines) == 0 {
		return
	}
	fmt.Fprintf(b, "# TYPE %s %s\n", name, typeName)
	for _, line := range lines {
		fmt.Fprintf(b, "%s\n", line)
	}
}

// reservedLabelNames is the set of label names already present in the base
// label set plus the `metric` label appended after buildLabelStr. A guest
// label whose sanitized key collides with one of these would produce a
// duplicate label name, which Prometheus rejects for the entire scrape sample.
var reservedLabelNames = map[string]bool{
	"tenant_id": true,
	"app":       true,
	"metric":    true,
}

// buildLabelStr appends extra guest-supplied labels to the base label set.
// Label keys are sanitized to [a-zA-Z_][a-zA-Z0-9_]* and checked against
// reserved names; colliding keys are prefixed with "user_" to avoid duplicate
// label names in the Prometheus output. Label values are quoted with
// promQuoteLabelValue, which uses only the escape sequences Prometheus accepts.
func buildLabelStr(base string, extra [][2]string) string {
	if len(extra) == 0 {
		return base
	}
	var parts []string
	for _, kv := range extra {
		k := sanitizeLabelName(kv[0])
		if reservedLabelNames[k] {
			k = "user_" + k
		}
		parts = append(parts, k+"="+promQuoteLabelValue(kv[1]))
	}
	return base + "," + strings.Join(parts, ",")
}

// sanitizeLabelName replaces every character that is not [a-zA-Z0-9_] with
// an underscore, and prepends an underscore when the first character is a
// digit, producing a valid Prometheus label name.
func sanitizeLabelName(s string) string {
	if s == "" {
		return "_"
	}
	var buf strings.Builder
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			buf.WriteRune(r)
		case r >= '0' && r <= '9':
			if i == 0 {
				buf.WriteRune('_')
			}
			buf.WriteRune(r)
		default:
			buf.WriteRune('_')
		}
	}
	return buf.String()
}

// promQuoteLabelValue returns a Prometheus text-format quoted label value.
// The Prometheus exposition format only allows three escape sequences inside
// double-quoted label values: \" \\ \n. Go's %q emits additional escapes
// (\t, \a, \b, \r, \f, \v, \uXXXX) that Prometheus parsers reject.
func promQuoteLabelValue(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
