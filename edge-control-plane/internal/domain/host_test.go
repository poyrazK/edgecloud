package domain

import "testing"

// TestIngressHost locks the public hostname format. Drift from the Rust
// ingress's `INGRESS_HOST_SUFFIX` (edge-ingress/src/config.rs) is the
// failure mode this test exists to catch — both sides must agree on the
// suffix and on the `<tenant>-<app>` ordering.
func TestIngressHost(t *testing.T) {
	tests := []struct {
		tenantID string
		appName  string
		want     string
	}{
		{"t_acme", "api", "t_acme-api.edgecloud.dev"},
		{"t_globex", "web", "t_globex-web.edgecloud.dev"},
		{"t_a", "b", "t_a-b.edgecloud.dev"},
	}
	for _, tt := range tests {
		t.Run(tt.tenantID+"/"+tt.appName, func(t *testing.T) {
			got := IngressHost(tt.tenantID, tt.appName)
			if got != tt.want {
				t.Errorf("IngressHost(%q, %q) = %q, want %q", tt.tenantID, tt.appName, got, tt.want)
			}
		})
	}
}

// TestIngressHostSuffix guards against accidental re-branding — every
// public URL the control plane advertises depends on this constant.
func TestIngressHostSuffix(t *testing.T) {
	if IngressHostSuffix != "edgecloud.dev" {
		t.Errorf("IngressHostSuffix = %q, want %q", IngressHostSuffix, "edgecloud.dev")
	}
}
