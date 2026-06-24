package service

import (
	"strings"
	"testing"

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

	// All five families present (request_count, outbound_bytes, counter, gauge, histogram).
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
	} {
		if !strings.Contains(out, want) {
			t.Errorf("RenderTenant output missing %q\ngot:\n%s", want, out)
		}
	}

	// Unknown tenant returns empty.
	if agg.RenderTenant("t_unknown") != "" {
		t.Error("unknown tenant should return empty string")
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
