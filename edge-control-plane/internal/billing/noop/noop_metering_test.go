package noop

import (
	"context"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/billing"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

// TestRecordUsage_always_succeeds is the contract test: the noop
// provider never errors, never panics, and ignores all fields except
// it doesn't crash on a zero-value input. The drainer relies on this —
// a noop metering path should never write a MarkFailed row, which
// would inflate the retry/giveup counters in dashboards.
func TestRecordUsage_always_succeeds(t *testing.T) {
	p := NewMetering()
	cases := []struct {
		name string
		in   billing.MeterUsage
	}{
		{"zero_value", billing.MeterUsage{}},
		{"resident_seconds_60", billing.MeterUsage{
			TenantID:       "t_demo",
			Kind:           domain.MeterKindResidentSeconds,
			Quantity:       60,
			IdempotencyKey: "t_demo:resident_seconds:abc123",
			RecordedAt:     time.Now(),
		}},
		{"huge_quantity", billing.MeterUsage{
			TenantID:       "t_demo",
			Kind:           domain.MeterKindOutboundBytes,
			Quantity:       1<<63 - 1,
			IdempotencyKey: "t_demo:outbound_bytes:xyz789",
			RecordedAt:     time.Now(),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := p.RecordUsage(context.Background(), tc.in); err != nil {
				t.Fatalf("RecordUsage returned %v, want nil (noop never errors)", err)
			}
		})
	}
}

// TestName_returns_provider_noop is the contract test for the row
// stamp: the noop MeteringProvider's Name() must match the noop
// BillingProvider's Name() so a single tenant config can route both
// surfaces through noop with a consistent provider column on the row.
func TestName_returns_provider_noop(t *testing.T) {
	if got, want := NewMetering().Name(), domain.ProviderNoop; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}
}
