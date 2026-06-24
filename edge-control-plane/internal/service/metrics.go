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
func (a *MetricsAggregator) RenderAll() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var b strings.Builder
	for tenantID, apps := range a.data {
		renderApps(&b, tenantID, apps)
	}
	return b.String()
}

// renderApps writes Prometheus text-format output for every app belonging to
// one tenant. Each metric family gets exactly one `# TYPE` declaration (before
// all of its series), satisfying the Prometheus exposition format spec.
// Built-in delta counters (request_count, outbound_bytes) are emitted as
// `gauge` because the worker resets them after each heartbeat — they are
// per-interval deltas, not monotonically increasing totals, and Prometheus
// `counter` type requires monotonic values.
func renderApps(b *strings.Builder, tenantID string, apps map[string]appMetrics) {
	// Collect series per family across all apps in one pass, then emit
	// families in a second pass so each `# TYPE` appears exactly once.
	var (
		reqCountLines  []string
		outBytesLines  []string
		counterLines   []string
		gaugeLines     []string
		histogramLines []string
	)

	for appName, m := range apps {
		baseLabels := fmt.Sprintf(`tenant_id=%q,app=%q`, tenantID, appName)
		reqCountLines = append(reqCountLines, fmt.Sprintf("edge_request_count{%s} %d", baseLabels, m.requestCount))
		outBytesLines = append(outBytesLines, fmt.Sprintf("edge_outbound_bytes{%s} %d", baseLabels, m.outboundBytes))

		for _, s := range m.samples {
			labelStr := buildLabelStr(baseLabels, s.Labels)
			switch s.Kind {
			case domain.MetricKindCounter:
				counterLines = append(counterLines, fmt.Sprintf("edge_counter{%s,metric=%q} %g", labelStr, s.Name, s.Value))
			case domain.MetricKindGauge:
				gaugeLines = append(gaugeLines, fmt.Sprintf("edge_gauge{%s,metric=%q} %g", labelStr, s.Name, s.Value))
			case domain.MetricKindHistogramSample:
				histogramLines = append(histogramLines, fmt.Sprintf("edge_histogram_sample{%s,metric=%q} %g", labelStr, s.Name, s.Value))
			}
		}
	}

	// Emit each family: one TYPE declaration followed by all series.
	// Built-in metrics are delta values (reset each heartbeat) → gauge type.
	// Guest edge_counter is cumulative across app lifetime → counter type.
	emitFamily(b, "edge_request_count", "gauge", reqCountLines)
	emitFamily(b, "edge_outbound_bytes", "gauge", outBytesLines)
	emitFamily(b, "edge_counter", "counter", counterLines)
	emitFamily(b, "edge_gauge", "gauge", gaugeLines)
	emitFamily(b, "edge_histogram_sample", "untyped", histogramLines)
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

// buildLabelStr appends extra guest-supplied labels to the base label set.
// Label keys from guest code are sanitized to [a-zA-Z_][a-zA-Z0-9_]* (the
// Prometheus label name grammar) to prevent injection through the unquoted
// key position in the exposition format. Label values are %q-quoted by the
// caller and are safe without further escaping.
func buildLabelStr(base string, extra [][2]string) string {
	if len(extra) == 0 {
		return base
	}
	var parts []string
	for _, kv := range extra {
		parts = append(parts, fmt.Sprintf("%s=%q", sanitizeLabelName(kv[0]), kv[1]))
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
