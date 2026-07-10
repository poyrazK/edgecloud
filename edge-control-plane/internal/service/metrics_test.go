package service

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// ---------------------------------------------------------------------------
// sanitizeLabelName
// ---------------------------------------------------------------------------

func TestSanitizeLabelName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"valid_name", "valid_name"},
		{"CamelCase", "CamelCase"},
		{"3starts_with_digit", "_3starts_with_digit"},
		{"has-hyphen", "has_hyphen"},
		{"has space", "has_space"},
		{"", "_"},
		{"metric", "metric"},
		{"_under", "_under"},
	}
	for _, c := range cases {
		got := sanitizeLabelName(c.in)
		if got != c.want {
			t.Errorf("sanitizeLabelName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// promQuoteLabelValue
// ---------------------------------------------------------------------------

func TestPromQuoteLabelValue(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"simple", `"simple"`},
		{`has"quote`, `"has\"quote"`},
		{`has\backslash`, `"has\\backslash"`},
		{"has\nnewline", `"has\nnewline"`},
		// Tab must NOT be escaped as \t — Prometheus only accepts \n \\ \"
		{"has\ttab", "\"has\ttab\""},
		// Non-ASCII must pass through unescaped (not \uXXXX)
		{"café", `"café"`},
	}
	for _, c := range cases {
		got := promQuoteLabelValue(c.in)
		if got != c.want {
			t.Errorf("promQuoteLabelValue(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// MetricsAggregator — Ingest + RenderTenant
// ---------------------------------------------------------------------------

func TestMetricsAggregator_IngestAndRenderTenant(t *testing.T) {
	agg := NewMetricsAggregator()
	agg.Ingest("t_abc", "my-app", 5, 1024, []domain.MetricSample{
		{Name: "hits", Kind: domain.MetricKindCounter, Value: 42, Labels: [][2]string{{"route", "/api"}}},
		{Name: "mem", Kind: domain.MetricKindGauge, Value: 512, Labels: [][2]string{}},
		{Name: "latency", Kind: domain.MetricKindHistogramSample, Value: 12.5, Labels: [][2]string{}},
	})

	out := agg.RenderTenant("t_abc")

	// Per-tenant families present (5) plus the global background-GC
	// families (issue #581 — exposed on both /metrics and /api/v1/metrics).
	for _, want := range []string{
		"# TYPE edge_request_count gauge",
		"# TYPE edge_outbound_bytes gauge",
		"# TYPE edge_counter counter",
		"# TYPE edge_gauge gauge",
		"# TYPE edge_histogram_sample untyped",
		`edge_request_count{tenant_id="t_abc",app="my-app"} 5`,
		`edge_outbound_bytes{tenant_id="t_abc",app="my-app"} 1024`,
		`edge_counter{tenant_id="t_abc",app="my-app",route="`,
		`metric="hits"} 42`,
		`edge_gauge{`,
		`metric="mem"} 512`,
		`edge_histogram_sample{`,
		`metric="latency"} 12.5`,
		// GC families — zero-valued because no Record* has been called yet.
		"edge_log_gc_ticks_total 0",
		"edge_preview_gc_ticks_total 0",
		"edge_cache_retry_sweep_ticks_total 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderTenant output missing %q\ngot:\n%s", want, out)
		}
	}

	// Unknown tenant still gets the global background-GC families
	// (issue #581 decision: expose on both /metrics AND /api/v1/metrics)
	// but no per-tenant series. We assert the GC families are present
	// and the per-tenant series are absent.
	outUnknown := agg.RenderTenant("t_unknown")
	if strings.Contains(outUnknown, `tenant_id="t_unknown"`) {
		t.Error("RenderTenant for unknown tenant must not include any per-tenant series")
	}
	for _, want := range []string{
		"# TYPE edge_log_gc_ticks_total counter",
		"edge_log_gc_ticks_total 0",
		"# TYPE edge_preview_gc_ticks_total counter",
		"edge_preview_gc_ticks_total 0",
		"# TYPE edge_cache_retry_sweep_ticks_total counter",
		"edge_cache_retry_sweep_ticks_total 0",
	} {
		if !strings.Contains(outUnknown, want) {
			t.Errorf("RenderTenant (unknown) missing %q\ngot:\n%s", want, outUnknown)
		}
	}
}

// TestMetricsAggregator_RenderAll checks the operator-scoped render covers all
// tenants and emits each TYPE line exactly once across the full output.
func TestMetricsAggregator_RenderAll(t *testing.T) {
	agg := NewMetricsAggregator()
	agg.Ingest("t_1", "app-a", 1, 0, nil)
	agg.Ingest("t_2", "app-b", 2, 0, nil)

	out := agg.RenderAll()
	if !strings.Contains(out, `tenant_id="t_1"`) {
		t.Error("RenderAll must include tenant t_1")
	}
	if !strings.Contains(out, `tenant_id="t_2"`) {
		t.Error("RenderAll must include tenant t_2")
	}

	// Each TYPE line must appear exactly once — Prometheus parsers reject
	// a metric family name whose TYPE line is repeated in a single scrape.
	for _, typeLine := range []string{
		"# TYPE edge_request_count gauge",
		"# TYPE edge_outbound_bytes gauge",
	} {
		count := strings.Count(out, typeLine)
		if count != 1 {
			t.Errorf("RenderAll emitted %q %d times (want 1); both tenants share one TYPE declaration\ngot:\n%s", typeLine, count, out)
		}
	}
}

// ---------------------------------------------------------------------------
// buildLabelStr — reserved-name collision avoidance
// ---------------------------------------------------------------------------

// TestBuildLabelStr_ReservedNamePrefixed verifies that a guest label key that
// sanitizes to a reserved name (tenant_id, app, metric) is prefixed with
// "user_" rather than silently producing a duplicate Prometheus label name that
// would cause the scrape to fail.
func TestBuildLabelStr_ReservedNamePrefixed(t *testing.T) {
	base := `tenant_id="t_abc",app="my-app"`

	// "metric" is a reserved label (appended separately in renderApps).
	out := buildLabelStr(base, [][2]string{{"metric", "my_counter"}})
	if strings.Contains(out, `,metric="my_counter"`) {
		t.Errorf("buildLabelStr must not emit bare 'metric' key for guest label; got %q", out)
	}
	if !strings.Contains(out, `user_metric="my_counter"`) {
		t.Errorf("buildLabelStr must prefix reserved key with user_; got %q", out)
	}

	// "tenant_id" is also reserved.
	out2 := buildLabelStr(base, [][2]string{{"tenant_id", "spoofed"}})
	if strings.Contains(out2, `,tenant_id="spoofed"`) {
		t.Errorf("buildLabelStr must not emit bare 'tenant_id' key for guest label; got %q", out2)
	}
	if !strings.Contains(out2, `user_tenant_id="spoofed"`) {
		t.Errorf("buildLabelStr must prefix reserved key with user_; got %q", out2)
	}
}

// TestBuildLabelStr_NoExtraLabels checks that absent guest labels return the
// base label string unchanged.
func TestBuildLabelStr_NoExtraLabels(t *testing.T) {
	base := `tenant_id="t_abc",app="my-app"`
	if got := buildLabelStr(base, nil); got != base {
		t.Errorf("buildLabelStr with nil extra = %q, want %q", got, base)
	}
	if got := buildLabelStr(base, [][2]string{}); got != base {
		t.Errorf("buildLabelStr with empty extra = %q, want %q", got, base)
	}
}

// TestPromQuoteLabelValue_TabPassesThrough confirms that \t is NOT backslash-
// escaped in the output (Prometheus text format doesn't support \t; the literal
// tab character is the correct representation).
func TestPromQuoteLabelValue_ControlCharsPassThrough(t *testing.T) {
	got := promQuoteLabelValue("a\tb")
	// Should contain a literal tab, not the two-char sequence \t.
	if strings.Contains(got, `\t`) {
		t.Errorf("promQuoteLabelValue must not emit \\t escape; got %q", got)
	}
	if !strings.Contains(got, "\t") {
		t.Errorf("promQuoteLabelValue must preserve literal tab; got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Background-GC metrics (issue #581).
//
// The aggregator's emitGCFamilies writes 20 families at every render,
// reading counter state directly off the aggregator struct. Each
// *Sink closure bumps the relevant fields under a.mu.Lock(). nil-
// receiver guards make the closures safe to call on a nil aggregator
// so unit tests can wire `nil` into the GC constructors without
// panicking.
// ---------------------------------------------------------------------------

// TestMetricsAggregator_RenderAll_IncludesGCFamilies: a fresh aggregator
// (no Record* calls) emits all 20 GC families with zero values, both on
// RenderAll and RenderTenant. Operators can alert on a family that never
// moves (e.g. last_tick_timestamp_seconds older than 2× interval).
func TestMetricsAggregator_RenderAll_IncludesGCFamilies(t *testing.T) {
	agg := NewMetricsAggregator()

	all := agg.RenderAll()
	for _, want := range []string{
		"# TYPE edge_log_gc_ticks_total counter",
		"edge_log_gc_ticks_total 0",
		"# TYPE edge_log_gc_rows_deleted_total counter",
		"edge_log_gc_rows_deleted_total 0",
		"# TYPE edge_log_gc_errors_total counter",
		"edge_log_gc_errors_total 0",
		"# TYPE edge_log_gc_last_tick_timestamp_seconds gauge",
		"edge_log_gc_last_tick_timestamp_seconds 0",
		"# TYPE edge_preview_gc_ticks_total counter",
		"edge_preview_gc_ticks_total 0",
		"# TYPE edge_preview_gc_blobs_deleted_total counter",
		"edge_preview_gc_blobs_deleted_total 0",
		"# TYPE edge_preview_gc_rows_deleted_total counter",
		"edge_preview_gc_rows_deleted_total 0",
		"# TYPE edge_preview_gc_batches_swept_total counter",
		"edge_preview_gc_batches_swept_total 0",
		"# TYPE edge_preview_gc_errors_total counter",
		"edge_preview_gc_errors_total 0",
		"# TYPE edge_preview_gc_blob_delete_failures_total counter",
		"edge_preview_gc_blob_delete_failures_total 0",
		"# TYPE edge_preview_gc_last_tick_timestamp_seconds gauge",
		"edge_preview_gc_last_tick_timestamp_seconds 0",
		"# TYPE edge_cache_retry_sweep_ticks_total counter",
		"edge_cache_retry_sweep_ticks_total 0",
		"# TYPE edge_cache_retry_sweep_batches_swept_total counter",
		"edge_cache_retry_sweep_batches_swept_total 0",
		"# TYPE edge_cache_retry_sweep_rows_touched_total counter",
		"edge_cache_retry_sweep_rows_touched_total 0",
		"# TYPE edge_cache_retry_sweep_pushed_ok_total counter",
		"edge_cache_retry_sweep_pushed_ok_total 0",
		"# TYPE edge_cache_retry_sweep_still_failing_total counter",
		"edge_cache_retry_sweep_still_failing_total 0",
		"# TYPE edge_cache_retry_sweep_config_missing_total counter",
		"edge_cache_retry_sweep_config_missing_total 0",
		"# TYPE edge_cache_retry_sweep_given_up_total counter",
		"edge_cache_retry_sweep_given_up_total 0",
		"# TYPE edge_cache_retry_sweep_errors_total counter",
		"edge_cache_retry_sweep_errors_total 0",
		"# TYPE edge_cache_retry_sweep_last_tick_timestamp_seconds gauge",
		"edge_cache_retry_sweep_last_tick_timestamp_seconds 0",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("RenderAll missing %q\ngot:\n%s", want, all)
		}
	}
}

// TestMetricsAggregator_RecordLogGC: a sequence of sink calls is reflected
// in the rendered output. Three ticks: (5 deleted, ok), (0 deleted, error),
// (3 deleted, ok). ticks_total=3, rows_deleted_total=8, errors_total=1.
func TestMetricsAggregator_RecordLogGC(t *testing.T) {
	agg := NewMetricsAggregator()
	sink := agg.NewLogGCSink()
	sink(5, false)
	sink(0, true)
	sink(3, false)

	out := agg.RenderAll()
	for _, want := range []string{
		"edge_log_gc_ticks_total 3",
		"edge_log_gc_rows_deleted_total 8",
		"edge_log_gc_errors_total 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderAll missing %q after sink calls\ngot:\n%s", want, out)
		}
	}
	// last_tick_timestamp_seconds must be non-zero (we just called it).
	if strings.Contains(out, "edge_log_gc_last_tick_timestamp_seconds 0") {
		t.Error("edge_log_gc_last_tick_timestamp_seconds must be > 0 after a sink call")
	}
}

// TestMetricsAggregator_RecordPreviewGC: a sequence of sink calls accumulates
// per-tick counters (blobs/rows/batches/errors) correctly.
func TestMetricsAggregator_RecordPreviewGC(t *testing.T) {
	agg := NewMetricsAggregator()
	sink := agg.NewPreviewGCSink()
	sink(4, 4, 1, false)
	sink(2, 2, 1, true)

	out := agg.RenderAll()
	for _, want := range []string{
		"edge_preview_gc_ticks_total 2",
		"edge_preview_gc_blobs_deleted_total 6",
		"edge_preview_gc_rows_deleted_total 6",
		"edge_preview_gc_batches_swept_total 2",
		"edge_preview_gc_errors_total 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderAll missing %q\ngot:\n%s", want, out)
		}
	}
}

// TestMetricsAggregator_RecordPreviewGC_BlobFailures: the per-blob
// failure recorder is independent from the per-tick sink. Three calls
// bumps the blob_delete_failures_total counter, and the per-tick
// counters are not touched.
func TestMetricsAggregator_RecordPreviewGC_BlobFailures(t *testing.T) {
	agg := NewMetricsAggregator()
	rec := agg.NewPreviewBlobFailureRecorder()
	rec()
	rec()
	rec()

	out := agg.RenderAll()
	if !strings.Contains(out, "edge_preview_gc_blob_delete_failures_total 3") {
		t.Errorf("RenderAll missing edge_preview_gc_blob_delete_failures_total 3\ngot:\n%s", out)
	}
	// Per-tick counters must stay zero (no per-tick sink was called).
	if !strings.Contains(out, "edge_preview_gc_ticks_total 0") {
		t.Error("blob-failure recorder must not bump ticks_total")
	}
}

// TestMetricsAggregator_RecordCacheRetrySweep: the most complex sink —
// six int counters plus an error flag plus a timestamp.
func TestMetricsAggregator_RecordCacheRetrySweep(t *testing.T) {
	agg := NewMetricsAggregator()
	sink := agg.NewCacheRetrySweepSink()
	// rowsTouched, pushedOK, stillFailing, configMissing, givenUp, batchesSwept
	sink(10, 5, 3, 1, 1, 2, false)
	sink(0, 0, 0, 0, 0, 0, true)

	out := agg.RenderAll()
	for _, want := range []string{
		"edge_cache_retry_sweep_ticks_total 2",
		"edge_cache_retry_sweep_rows_touched_total 10",
		"edge_cache_retry_sweep_pushed_ok_total 5",
		"edge_cache_retry_sweep_still_failing_total 3",
		"edge_cache_retry_sweep_config_missing_total 1",
		"edge_cache_retry_sweep_given_up_total 1",
		"edge_cache_retry_sweep_batches_swept_total 2",
		"edge_cache_retry_sweep_errors_total 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderAll missing %q\ngot:\n%s", want, out)
		}
	}
}

// TestMetricsAggregator_NilSinkIsNoop: nil-aggregator sinks must not panic.
// This is the contract that lets unit tests pass `nil` as the sink arg
// to the GC constructors.
func TestMetricsAggregator_NilSinkIsNoop(t *testing.T) {
	var nilAgg *MetricsAggregator

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("nil-aggregator sink panicked: %v", r)
		}
	}()

	nilAgg.NewLogGCSink()(5, false)
	nilAgg.NewPreviewGCSink()(1, 2, 3, false)
	nilAgg.NewPreviewBlobFailureRecorder()()
	nilAgg.NewCacheRetrySweepSink()(1, 2, 3, 4, 5, 6, false)
}

// TestMetricsAggregator_RenderTenant_IncludesGCFamilies: per the user
// decision on issue #581, the GC families appear on /api/v1/metrics
// (per-tenant) as well as on /metrics (operator). Verify by ingesting
// a tenant then asserting the rendered output contains both the
// per-tenant series and the global GC families.
func TestMetricsAggregator_RenderTenant_IncludesGCFamilies(t *testing.T) {
	agg := NewMetricsAggregator()
	agg.Ingest("t_1", "app-a", 1, 0, nil)
	sink := agg.NewLogGCSink()
	sink(7, false)

	out := agg.RenderTenant("t_1")
	// Per-tenant series still present.
	if !strings.Contains(out, `edge_request_count{tenant_id="t_1",app="app-a"} 1`) {
		t.Errorf("per-tenant series missing\ngot:\n%s", out)
	}
	// GC families included too.
	if !strings.Contains(out, "edge_log_gc_ticks_total 1") {
		t.Errorf("GC families missing on per-tenant render\ngot:\n%s", out)
	}
	if !strings.Contains(out, "edge_log_gc_rows_deleted_total 7") {
		t.Errorf("GC families missing on per-tenant render\ngot:\n%s", out)
	}
}

// TestMetricsAggregator_TimestampUpdatesPerTick: the
// last_tick_timestamp_seconds gauge must reflect the time of the most
// recent sink call, NOT the time the sink was constructed. This is the
// documented "alert on staleness" surface in CLAUDE.md; the bug it
// guards was the review-required fix on PR #588 (capture-at-construction
// inside New*Sink factories froze the gauge at process start).
//
// We sleep 1.1s between two sink calls so the Unix-second boundary
// is guaranteed to advance — a sub-second gap would be racy on hosts
// where the test happens to land on the same Unix second twice.
func TestMetricsAggregator_TimestampUpdatesPerTick(t *testing.T) {
	agg := NewMetricsAggregator()
	sink := agg.NewLogGCSink()

	before := time.Now().Unix()
	sink(1, false)
	firstTickAt := time.Now().Unix()
	out1 := agg.RenderAll()

	firstOK := strings.Contains(out1, fmt.Sprintf("edge_log_gc_last_tick_timestamp_seconds %d", firstTickAt)) ||
		strings.Contains(out1, fmt.Sprintf("edge_log_gc_last_tick_timestamp_seconds %d", before))
	if !firstOK {
		t.Errorf("first sink: expected timestamp in [%d,%d], got:\n%s", before, firstTickAt, out1)
	}

	time.Sleep(1100 * time.Millisecond)
	sink(1, false)
	after := time.Now().Unix()
	out2 := agg.RenderAll()

	// Parse the second-tick timestamp back out and assert it advanced.
	var got int64
	for _, line := range strings.Split(out2, "\n") {
		if strings.HasPrefix(line, "edge_log_gc_last_tick_timestamp_seconds ") {
			_, _ = fmt.Sscanf(line, "edge_log_gc_last_tick_timestamp_seconds %d", &got)
		}
	}
	if got <= firstTickAt {
		t.Errorf("second sink: timestamp %d did not advance past %d (capture-at-construction bug regressed)\ngot:\n%s", got, firstTickAt, out2)
	}
	if got > after {
		t.Errorf("second sink: timestamp %d is in the future (after=%d)", got, after)
	}
}
