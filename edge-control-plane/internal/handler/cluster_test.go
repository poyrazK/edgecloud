package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// mockClusterSvc implements service.ClusterServiceInterface for testing.
type mockClusterSvc struct {
	listResp *service.ClusterView
	listErr  error
}

func (m *mockClusterSvc) List(_ context.Context) (*service.ClusterView, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.listResp, nil
}

func TestCluster_Get_HappyPath(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	ip := "10.0.0.5"
	want := &service.ClusterView{
		GeneratedAt: now,
		Regions: map[string]service.RegionView{
			"us-east": {
				Workers: []service.WorkerView{
					{WorkerID: "w_us-east_a", Region: "us-east", IP: ip, AppCount: 2, MemoryMB: 4096, LastSeen: now},
					{WorkerID: "w_us-east_b", Region: "us-east", AppCount: 0, MemoryMB: 2048, LastSeen: now},
				},
				AppsPerWorker: 1, // (2 + 0) / 2 = 1
			},
			"eu-west": {
				Workers: []service.WorkerView{
					{WorkerID: "w_eu-west_a", Region: "eu-west", AppCount: 3, MemoryMB: 4096, LastSeen: now},
				},
				AppsPerWorker: 3,
			},
		},
	}
	svc := &mockClusterSvc{listResp: want}
	h := handler.NewClusterHandler(svc)

	req := httptest.NewRequest("GET", "/api/admin/cluster", nil)
	rr := httptest.NewRecorder()
	h.Get(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var got service.ClusterView
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(got.Regions) != 2 {
		t.Fatalf("len(Regions) = %d, want 2", len(got.Regions))
	}
	usEast, ok := got.Regions["us-east"]
	if !ok {
		t.Fatalf("missing us-east region in response")
	}
	if len(usEast.Workers) != 2 {
		t.Errorf("us-east Workers len = %d, want 2", len(usEast.Workers))
	}
	if usEast.AppsPerWorker != 1 {
		t.Errorf("us-east AppsPerWorker = %d, want 1", usEast.AppsPerWorker)
	}
	euWest, ok := got.Regions["eu-west"]
	if !ok {
		t.Fatalf("missing eu-west region in response")
	}
	if euWest.AppsPerWorker != 3 {
		t.Errorf("eu-west AppsPerWorker = %d, want 3", euWest.AppsPerWorker)
	}
}

func TestCluster_Get_EmptyCluster(t *testing.T) {
	// No workers registered yet — the view should still return successfully
	// with an empty regions map (not a 404).
	svc := &mockClusterSvc{listResp: &service.ClusterView{
		GeneratedAt: time.Now().UTC(),
		Regions:     map[string]service.RegionView{},
	}}
	h := handler.NewClusterHandler(svc)

	req := httptest.NewRequest("GET", "/api/admin/cluster", nil)
	rr := httptest.NewRecorder()
	h.Get(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var got service.ClusterView
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(got.Regions) != 0 {
		t.Errorf("len(Regions) = %d, want 0", len(got.Regions))
	}
}

func TestCluster_Get_ServiceError(t *testing.T) {
	svc := &mockClusterSvc{listErr: errors.New("db connection refused")}
	h := handler.NewClusterHandler(svc)

	req := httptest.NewRequest("GET", "/api/admin/cluster", nil)
	rr := httptest.NewRecorder()
	h.Get(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
	// Body should be a structured error envelope, not the raw error string.
	// http.Error appends a trailing newline — trim before comparing.
	if got := strings.TrimSpace(rr.Body.String()); got != `{"error":"internal error"}` {
		t.Errorf("body = %q, want structured envelope", got)
	}
}
