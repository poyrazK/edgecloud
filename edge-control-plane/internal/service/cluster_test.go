package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

type mockClusterWorkerRepo struct {
	listFn              func(ctx context.Context) ([]domain.Worker, error)
	getLatestStatusesFn func(ctx context.Context, ids []string) (map[string]domain.WorkerStatus, error)
}

func (m *mockClusterWorkerRepo) List(ctx context.Context) ([]domain.Worker, error) {
	return m.listFn(ctx)
}
func (m *mockClusterWorkerRepo) GetLatestStatuses(ctx context.Context, ids []string) (map[string]domain.WorkerStatus, error) {
	if m.getLatestStatusesFn == nil {
		return map[string]domain.WorkerStatus{}, nil
	}
	return m.getLatestStatusesFn(ctx, ids)
}

func TestClusterService_List_EmptyWorkers(t *testing.T) {
	svc := NewClusterService(&mockClusterWorkerRepo{
		listFn: func(ctx context.Context) ([]domain.Worker, error) { return nil, nil },
	}, nil)
	view, err := svc.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if view.Regions == nil {
		t.Fatal("Regions map is nil, want empty map")
	}
	if len(view.Regions) != 0 {
		t.Errorf("Regions length = %d, want 0", len(view.Regions))
	}
}

func TestClusterService_List_WorkerWithNoStatus(t *testing.T) {
	now := time.Now()
	svc := NewClusterService(&mockClusterWorkerRepo{
		listFn: func(ctx context.Context) ([]domain.Worker, error) {
			return []domain.Worker{{ID: "w_1", Region: "fra", LastSeen: now, MemoryMB: 4096}}, nil
		},
		getLatestStatusesFn: func(ctx context.Context, ids []string) (map[string]domain.WorkerStatus, error) {
			return map[string]domain.WorkerStatus{}, nil
		},
	}, nil)
	view, err := svc.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(view.Regions) != 1 {
		t.Fatalf("Regions length = %d, want 1", len(view.Regions))
	}
	fra := view.Regions["fra"]
	if len(fra.Workers) != 1 {
		t.Fatalf("fra Workers length = %d, want 1", len(fra.Workers))
	}
	if fra.Workers[0].AppCount != 0 {
		t.Errorf("AppCount = %d, want 0 (no status row)", fra.Workers[0].AppCount)
	}
}

func TestClusterService_List_SingleWorkerRunningApps(t *testing.T) {
	now := time.Now()
	apps := map[string]domain.AppStatus{
		"app-a": {Status: "running"},
		"app-b": {Status: "running"},
		"app-c": {Status: "crashed"},
	}
	appsJSON, _ := json.Marshal(apps)

	svc := NewClusterService(&mockClusterWorkerRepo{
		listFn: func(ctx context.Context) ([]domain.Worker, error) {
			return []domain.Worker{{ID: "w_1", Region: "fra", LastSeen: now, MemoryMB: 4096}}, nil
		},
		getLatestStatusesFn: func(ctx context.Context, ids []string) (map[string]domain.WorkerStatus, error) {
			return map[string]domain.WorkerStatus{
				"w_1": {Apps: appsJSON},
			}, nil
		},
	}, nil)
	view, err := svc.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	fra := view.Regions["fra"]
	if fra.Workers[0].AppCount != 3 {
		t.Errorf("AppCount = %d, want 3", fra.Workers[0].AppCount)
	}
	if fra.AppsPerWorker != 3 {
		t.Errorf("AppsPerWorker = %d, want 3", fra.AppsPerWorker)
	}
}

func TestClusterService_List_MultipleRegions(t *testing.T) {
	now := time.Now()
	apps := map[string]domain.AppStatus{"app-a": {Status: "running"}}
	appsJSON, _ := json.Marshal(apps)

	svc := NewClusterService(&mockClusterWorkerRepo{
		listFn: func(ctx context.Context) ([]domain.Worker, error) {
			return []domain.Worker{
				{ID: "w_fra", Region: "fra", LastSeen: now, MemoryMB: 4096},
				{ID: "w_sfo", Region: "sfo", LastSeen: now, MemoryMB: 4096},
			}, nil
		},
		getLatestStatusesFn: func(ctx context.Context, ids []string) (map[string]domain.WorkerStatus, error) {
			return map[string]domain.WorkerStatus{
				"w_fra": {Apps: appsJSON},
				"w_sfo": {Apps: appsJSON},
			}, nil
		},
	}, nil)
	view, err := svc.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(view.Regions) != 2 {
		t.Errorf("Regions length = %d, want 2", len(view.Regions))
	}
	for _, region := range []string{"fra", "sfo"} {
		if rv, ok := view.Regions[region]; !ok {
			t.Errorf("missing region %q", region)
		} else if len(rv.Workers) != 1 {
			t.Errorf("region %q worker count = %d, want 1", region, len(rv.Workers))
		}
	}
}

func TestClusterService_List_InvalidStatusJSON(t *testing.T) {
	now := time.Now()
	svc := NewClusterService(&mockClusterWorkerRepo{
		listFn: func(ctx context.Context) ([]domain.Worker, error) {
			return []domain.Worker{{ID: "w_1", Region: "fra", LastSeen: now, MemoryMB: 4096}}, nil
		},
		getLatestStatusesFn: func(ctx context.Context, ids []string) (map[string]domain.WorkerStatus, error) {
			return map[string]domain.WorkerStatus{
				"w_1": {Apps: json.RawMessage(`{bad}`)},
			}, nil
		},
	}, nil)
	view, err := svc.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if view.Regions["fra"].Workers[0].AppCount != 0 {
		t.Errorf("AppCount = %d, want 0 (invalid JSON swallowed)", view.Regions["fra"].Workers[0].AppCount)
	}
}

func TestClusterService_List_NilIP(t *testing.T) {
	now := time.Now()
	svc := NewClusterService(&mockClusterWorkerRepo{
		listFn: func(ctx context.Context) ([]domain.Worker, error) {
			return []domain.Worker{{ID: "w_1", Region: "fra", LastSeen: now, MemoryMB: 4096, IP: nil}}, nil
		},
		getLatestStatusesFn: func(ctx context.Context, ids []string) (map[string]domain.WorkerStatus, error) {
			return map[string]domain.WorkerStatus{}, nil
		},
	}, nil)
	view, err := svc.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if view.Regions["fra"].Workers[0].IP != "" {
		t.Errorf("IP = %q, want '' for nil IP", view.Regions["fra"].Workers[0].IP)
	}
}

func TestClusterService_List_IntegerDivisionRounding(t *testing.T) {
	now := time.Now()
	apps := map[string]domain.AppStatus{
		"a": {}, "b": {}, "c": {}, "d": {}, "e": {},
	}
	appsJSON, _ := json.Marshal(apps)

	svc := NewClusterService(&mockClusterWorkerRepo{
		listFn: func(ctx context.Context) ([]domain.Worker, error) {
			return []domain.Worker{
				{ID: "w_1", Region: "fra", LastSeen: now, MemoryMB: 4096},
				{ID: "w_2", Region: "fra", LastSeen: now, MemoryMB: 4096},
			}, nil
		},
		getLatestStatusesFn: func(ctx context.Context, ids []string) (map[string]domain.WorkerStatus, error) {
			return map[string]domain.WorkerStatus{
				"w_1": {Apps: appsJSON}, // 5 apps
			}, nil
		},
	}, nil)
	view, err := svc.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// 5 apps total, 2 workers => 5/2 = 2
	if view.Regions["fra"].AppsPerWorker != 2 {
		t.Errorf("AppsPerWorker = %d, want 2 (5/2 integer division)", view.Regions["fra"].AppsPerWorker)
	}
}
