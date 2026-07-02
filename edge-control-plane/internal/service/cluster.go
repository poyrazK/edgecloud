package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// ClusterView is the operator-facing snapshot of every region + worker
// in the platform. Returned by GET /api/admin/cluster.
type ClusterView struct {
	GeneratedAt time.Time             `json:"generated_at"`
	Regions     map[string]RegionView `json:"regions"`
}

// RegionView groups workers by region and reports the average app
// count per worker — useful to spot skew after app-pinning (#86).
type RegionView struct {
	Workers       []WorkerView `json:"workers"`
	AppsPerWorker int          `json:"apps_per_worker_avg"`
}

// WorkerView is the per-worker projection of the cluster view.
type WorkerView struct {
	WorkerID string    `json:"worker_id"`
	Region   string    `json:"region"`
	IP       string    `json:"ip,omitempty"`
	LastSeen time.Time `json:"last_seen"`
	AppCount int       `json:"app_count"`
	MemoryMB int       `json:"memory_mb"`
}

// AutoscaleEventList is the envelope returned by
// GET /api/v1/admin/cluster/events. The `region` field echoes back
// the filter (or `nil` when listing all regions) so a CLI paginating
// across calls can verify its filter without re-parsing query state.
type AutoscaleEventList struct {
	Items  []domain.AutoscaleEvent `json:"items"`
	Limit  int                     `json:"limit"`
	Region *string                 `json:"region"`
}

// ClusterServiceInterface allows handler tests to substitute a mock.
type ClusterServiceInterface interface {
	List(ctx context.Context) (*ClusterView, error)
	RecentEvents(ctx context.Context, region string, limit int) (*AutoscaleEventList, error)
}

// ClusterWorkerRepoInterface is the subset of *repository.WorkerRepository used by ClusterService.
type ClusterWorkerRepoInterface interface {
	List(ctx context.Context) ([]domain.Worker, error)
	GetLatestStatuses(ctx context.Context, ids []string) (map[string]domain.WorkerStatus, error)
}

// ClusterService builds the cluster view from the worker + worker_status
// repositories. Both queries are best-effort: a worker with no status
// row simply gets AppCount=0 (heartbeat hasn't arrived yet).
//
// `autoscaleEvents` (issue #85) powers RecentEvents. The two endpoints
// serve different operator questions ("what does the fleet look like
// RIGHT NOW?" vs. "why did the fleet size change?") so they intentionally
// share no state.
type ClusterService struct {
	workerRepo      ClusterWorkerRepoInterface
	autoscaleEvents *repository.AutoscaleRepository
}

// NewClusterService constructs a ClusterService. `workerRepo` is the
// worker + worker_status accessor; `autoscaleEvents` is the autoscaler
// event log (issue #85).
func NewClusterService(
	workerRepo ClusterWorkerRepoInterface,
	autoscaleEvents *repository.AutoscaleRepository,
) *ClusterService {
	return &ClusterService{workerRepo: workerRepo, autoscaleEvents: autoscaleEvents}
}

// regionAccum collects per-region aggregates during the worker loop so we
// don't have to keep three parallel maps (workers, app totals, worker
// counts) in lockstep. Adding a new per-region aggregate only requires
// adding a field here.
type regionAccum struct {
	workers     []WorkerView
	appTotal    int
	workerCount int
}

// List returns the current cluster view.
//
// Cost: exactly 2 SQL queries regardless of cluster size (one to list
// workers, one batched DISTINCT ON to load the latest worker_status row
// per worker). The previous N+1 implementation called GetStatus() once
// per worker.
func (s *ClusterService) List(ctx context.Context) (*ClusterView, error) {
	workers, err := s.workerRepo.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing workers: %w", err)
	}

	// Pre-load every worker's latest status in one query.
	ids := make([]string, len(workers))
	for i, w := range workers {
		ids[i] = w.ID
	}
	statuses, err := s.workerRepo.GetLatestStatuses(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("loading worker statuses: %w", err)
	}

	view := &ClusterView{
		GeneratedAt: time.Now().UTC(),
		Regions:     make(map[string]RegionView),
	}
	acc := make(map[string]*regionAccum)

	for _, w := range workers {
		appCount := 0
		if status, ok := statuses[w.ID]; ok {
			var apps map[string]domain.AppStatus
			if jsonErr := json.Unmarshal(status.Apps, &apps); jsonErr == nil {
				appCount = len(apps)
			}
		}

		wv := WorkerView{
			WorkerID: w.ID,
			Region:   w.Region,
			LastSeen: w.LastSeen,
			AppCount: appCount,
			MemoryMB: w.MemoryMB,
		}
		if w.IP != nil {
			wv.IP = *w.IP
		}

		a, ok := acc[w.Region]
		if !ok {
			a = &regionAccum{}
			acc[w.Region] = a
		}
		a.workers = append(a.workers, wv)
		a.appTotal += appCount
		a.workerCount++
	}

	for region, a := range acc {
		avg := 0
		if a.workerCount > 0 {
			avg = a.appTotal / a.workerCount
		}
		view.Regions[region] = RegionView{
			Workers:       a.workers,
			AppsPerWorker: avg,
		}
	}
	return view, nil
}

// RecentEvents returns the most-recent `autoscale_events` rows,
// newest first. Powers GET /api/v1/admin/cluster/events (issue #85).
//
// `region` is optional: empty string matches all regions; a non-empty
// value scopes the result to one region. `limit` is clamped to [1, 500]
// before the DB query so a typo'd query string can't materialize the
// whole table.
//
// `RecentEvents` was deliberately not folded into `List` — the two
// endpoints serve different operator questions ("what does the fleet
// look like RIGHT NOW?" vs. "why did the fleet size change?"). Keeping
// them separate also means a hot `cluster status` loop (e.g., a
// dashboard polling every 5s) doesn't drag the autoscale_events
// table along with it.
func (s *ClusterService) RecentEvents(ctx context.Context, region string, limit int) (*AutoscaleEventList, error) {
	const maxLimit = 500
	if limit <= 0 {
		limit = 50
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	items, err := s.autoscaleEvents.ListRecent(ctx, region, limit)
	if err != nil {
		return nil, fmt.Errorf("listing autoscale events: %w", err)
	}
	out := &AutoscaleEventList{
		Items: items,
		Limit: limit,
	}
	if region != "" {
		r := region
		out.Region = &r
	}
	return out, nil
}
