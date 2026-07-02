package autoscale

import (
	"context"
	"testing"
)

func TestNoopCloudProvider_ProvisionReturnsEmptyString(t *testing.T) {
	n := NewNoopCloudProvider(discardLogger())
	wid, err := n.Provision(context.Background(), "fra")
	if wid != "" {
		t.Errorf("Provision workerID = %q, want \"\"", wid)
	}
	if err != nil {
		t.Errorf("Provision err = %v, want nil", err)
	}
}

func TestNoopCloudProvider_DeprovisionReturnsNil(t *testing.T) {
	n := NewNoopCloudProvider(discardLogger())
	if err := n.Deprovision(context.Background(), "fra", "w_fra_abc"); err != nil {
		t.Errorf("Deprovision err = %v, want nil", err)
	}
}
