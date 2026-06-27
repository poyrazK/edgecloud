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

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// mockClusterSvc implements service.ClusterServiceInterface for testing.
type mockClusterSvc struct {
	listResp     *service.ClusterView
	listErr      error
	eventsResp   *service.AutoscaleEventList
	eventsErr    error
	eventsRegion string
	eventsLimit  int
}

func (m *mockClusterSvc) List(_ context.Context) (*service.ClusterView, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.listResp, nil
}

func (m *mockClusterSvc) RecentEvents(_ context.Context, region string, limit int) (*service.AutoscaleEventList, error) {
	m.eventsRegion = region
	m.eventsLimit = limit
	if m.eventsErr != nil {
		return nil, m.eventsErr
	}
	return m.eventsResp, nil
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
	if got := strings.TrimSpace(rr.Body.String()); got != `{"error":{"code":"INTERNAL_ERROR","message":"internal error"}}` {
		t.Errorf("body = %q, want structured envelope", got)
	}
}

// TestCluster_Events_HappyPath pins the wire shape of
// GET /api/v1/admin/cluster/events — the envelope MUST echo back the
// applied limit + region filter so a CLI paginating across calls can
// verify what was actually applied.
func TestCluster_Events_HappyPath(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	region := "fra"
	svc := &mockClusterSvc{eventsResp: &service.AutoscaleEventList{
		Items: []domain.AutoscaleEvent{
			{
				ID:           7,
				CreatedAt:    now,
				Region:       "fra",
				Action:       domain.AutoscaleUp,
				FromCount:    1,
				ToCount:      2,
				Reason:       "free_slots=0 needed=5",
				ProviderKind: "noop",
				Succeeded:    true,
			},
		},
		Limit:  50,
		Region: &region,
	}}
	h := handler.NewClusterHandler(svc)

	req := httptest.NewRequest("GET", "/api/v1/admin/cluster/events?region=fra&limit=50", nil)
	rr := httptest.NewRecorder()
	h.Events(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got service.AutoscaleEventList
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(got.Items) != 1 {
		t.Fatalf("len(Items) = %d, want 1", len(got.Items))
	}
	if got.Items[0].Action != domain.AutoscaleUp {
		t.Errorf("Items[0].Action = %q, want scale_up", got.Items[0].Action)
	}
	if got.Limit != 50 {
		t.Errorf("Limit = %d, want 50", got.Limit)
	}
	if got.Region == nil || *got.Region != "fra" {
		t.Errorf("Region = %v, want \"fra\"", got.Region)
	}
	if svc.eventsRegion != "fra" {
		t.Errorf("service region = %q, want fra", svc.eventsRegion)
	}
	if svc.eventsLimit != 50 {
		t.Errorf("service limit = %d, want 50", svc.eventsLimit)
	}
}

// TestCluster_Events_DefaultLimit pins that an absent `?limit=`
// falls back to the handler's default (50). Operators who forget to
// set the query param still get a sane page size.
func TestCluster_Events_DefaultLimit(t *testing.T) {
	svc := &mockClusterSvc{eventsResp: &service.AutoscaleEventList{
		Items: []domain.AutoscaleEvent{},
		Limit: 50,
	}}
	h := handler.NewClusterHandler(svc)

	req := httptest.NewRequest("GET", "/api/v1/admin/cluster/events", nil)
	rr := httptest.NewRecorder()
	h.Events(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if svc.eventsLimit != 50 {
		t.Errorf("service limit = %d, want default 50", svc.eventsLimit)
	}
}

// TestCluster_Events_BadLimitFallsBack pins that a malformed
// `?limit=foo` falls back to the default rather than 400ing — the
// endpoint is for operator dashboards, where a typo in a URL bar
// should not break the page.
func TestCluster_Events_BadLimitFallsBack(t *testing.T) {
	svc := &mockClusterSvc{eventsResp: &service.AutoscaleEventList{Items: nil, Limit: 50}}
	h := handler.NewClusterHandler(svc)

	req := httptest.NewRequest("GET", "/api/v1/admin/cluster/events?limit=foo", nil)
	rr := httptest.NewRecorder()
	h.Events(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if svc.eventsLimit != 50 {
		t.Errorf("service limit = %d, want default 50", svc.eventsLimit)
	}
}

// TestCluster_Events_AllRegions pins that an absent `?region=`
// queries across all regions — the mock records the empty string so
// we can verify the handler does NOT inject a default region.
func TestCluster_Events_AllRegions(t *testing.T) {
	svc := &mockClusterSvc{eventsResp: &service.AutoscaleEventList{Items: nil, Limit: 50, Region: nil}}
	h := handler.NewClusterHandler(svc)

	req := httptest.NewRequest("GET", "/api/v1/admin/cluster/events", nil)
	rr := httptest.NewRecorder()
	h.Events(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if svc.eventsRegion != "" {
		t.Errorf("service region = %q, want empty (all regions)", svc.eventsRegion)
	}
	// Decode the envelope to confirm Region is nil (not the empty string).
	var got service.AutoscaleEventList
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Region != nil {
		t.Errorf("Region = %v, want nil for all-regions query", got.Region)
	}
}

// TestCluster_Events_ServiceError pins the failure path: a DB error
// returns 500 with the structured envelope rather than the raw error.
func TestCluster_Events_ServiceError(t *testing.T) {
	svc := &mockClusterSvc{eventsErr: errors.New("db connection refused")}
	h := handler.NewClusterHandler(svc)

	req := httptest.NewRequest("GET", "/api/v1/admin/cluster/events", nil)
	rr := httptest.NewRecorder()
	h.Events(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr.Code)
	}
	if got := strings.TrimSpace(rr.Body.String()); got != `{"error":{"code":"INTERNAL_ERROR","message":"internal error"}}` {
		t.Errorf("body = %q, want structured envelope", got)
	}
}
