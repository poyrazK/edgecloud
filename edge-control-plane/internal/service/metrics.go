package service

import (
	"fmt"
	"strings"
	"sync"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// MetricsAggregator collects per-app metric samples pushed via heartbeats
// and renders them as Prometheus text-format output.
//
// Data is held in memory only — no DB persistence. Each heartbeat replaces
// the previous batch for a given (tenantID, appName) pair, matching the
// worker's subtract_delta model where counters reflect the delta since the
// last heartbeat rather than a cumulative total.
type MetricsAggregator struct {
	mu sync.RWMutex
	// tenantID → appName → []sample
	data map[string]map[string]appMetrics
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

// RenderTenant returns a Prometheus text-format string containing only the
// metrics for the given tenant. Returns an empty string when no data has
// been ingested for that tenant yet.
func (a *MetricsAggregator) RenderTenant(tenantID string) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	apps, ok := a.data[tenantID]
	if !ok {
		return ""
	}
	var b strings.Builder
	renderApps(&b, tenantID, apps)
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
	return b.String()
}

// familyLines holds accumulated series lines for each Prometheus metric family.
// Collecting across all tenants/apps before emitting ensures each `# TYPE`
// appears exactly once in the final output.
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

// renderApps writes Prometheus text-format output for every app belonging to
// one tenant. Each metric family gets exactly one `# TYPE` declaration (before
// all of its series), satisfying the Prometheus exposition format spec.
// Built-in delta counters (request_count, outbound_bytes) are emitted as
// `gauge` because the worker resets them after each heartbeat — they are
// per-interval deltas, not monotonically increasing totals, and Prometheus
// `counter` type requires monotonic values.
func renderApps(b *strings.Builder, tenantID string, apps map[string]appMetrics) {
	var fl familyLines
	collectFamilyLines(&fl, tenantID, apps)
	fl.emit(b)
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
