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
}

type appMetrics struct {
	requestCount  uint64
	outboundBytes uint64
	samples       []domain.MetricSample
}

// NewMetricsAggregator returns a ready-to-use aggregator.
func NewMetricsAggregator() *MetricsAggregator {
	return &MetricsAggregator{
		data: make(map[string]map[string]appMetrics),
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

// NewLogGCSink returns a sink that bumps the log_gc families. Passing
// the returned closure to LogGCService records one tick.
func (a *MetricsAggregator) NewLogGCSink() LogGCSink {
	if a == nil {
		return func(int64, bool) {}
	}
	now := time.Now().Unix()
	return func(rowsDeleted int64, hadError bool) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.logGcTick++
		a.logGcRow += rowsDeleted
		if hadError {
			a.logGcErr++
		}
		a.logGcTime = now
	}
}

// NewPreviewGCSink returns a sink that bumps the preview_gc families
// (per-tick counters). The per-blob failure counter is recorded
// separately via NewPreviewBlobFailureRecorder.
func (a *MetricsAggregator) NewPreviewGCSink() PreviewGCSink {
	if a == nil {
		return func(int, int, int, bool) {}
	}
	now := time.Now().Unix()
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
		a.previewGcTime = now
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
func (a *MetricsAggregator) NewCacheRetrySweepSink() CacheRetrySweepSink {
	if a == nil {
		return func(int, int, int, int, int, int, bool) {}
	}
	now := time.Now().Unix()
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
		a.cacheRetrySweepTime = now
	}
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
	emitGCFamilies(&b, a)
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
	emitGCFamilies(&b, a)
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