package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/nats-io/nats.go"
)

// Sentinel errors for WorkerService Register.
var (
	ErrInvalidWorkerID = errors.New("invalid worker_id format: must be w_<region>_<uuid>")
	ErrRegionMismatch  = errors.New("region mismatch between worker_id and request region")
	ErrQuotaExceeded   = errors.New("max workers reached for tenant")
)

// workerRepoInterface defines the repository methods used by WorkerService.
type workerRepoInterface interface {
	Upsert(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) (bool, error)
	CountByTenant(ctx context.Context, tenantID string) (int, error)
	Delete(ctx context.Context, id string) error
	ListByTenant(ctx context.Context, tenantID string) ([]domain.Worker, error)
	UpdateLastSeen(ctx context.Context, id string) error
	UpdateAddr(ctx context.Context, id, addr string) error
	UpsertStatus(ctx context.Context, ws *domain.WorkerStatus) error
	ListRunningAppTarget(ctx context.Context, tenantID, appName string) ([]domain.AppTarget, error)
}

// quotaRepoInterface defines the repository methods used by WorkerService.
type quotaRepoInterface interface {
	GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error)
}

// WorkerService handles worker lifecycle business logic.
type WorkerService struct {
	workerRepo workerRepoInterface
	quotaRepo  quotaRepoInterface
	nc         *nats.Conn
}

// NewWorkerService creates a new WorkerService.
func NewWorkerService(workerRepo *repository.WorkerRepository, quotaRepo *repository.QuotaRepository, nc *nats.Conn) *WorkerService {
	return &WorkerService{
		workerRepo: workerRepo,
		quotaRepo:  quotaRepo,
		nc:         nc,
	}
}

// AppTargetLookup is the narrow contract the deployment handler needs to
// answer "is this tenant's app currently routable?". Kept separate from
// the full WorkerService so handler tests can mock just the one method
// without standing up a NATS connection, a worker repo, and a quota repo.
type AppTargetLookup interface {
	GetAppTarget(ctx context.Context, tenantID, appName string) (*domain.AppTarget, error)
}

// Register creates or updates a worker record for a tenant.
// It is idempotent — if the worker already exists, it just updates last_seen.
func (s *WorkerService) Register(ctx context.Context, tenantID string, req *domain.RegisterWorkerRequest) error {
	// 1. Validate worker_id format: w_<region>_<uuid>
	if !domain.IsValidWorkerID(req.WorkerID) {
		return ErrInvalidWorkerID
	}

	// 2. Validate region in worker_id matches request region
	parts := strings.SplitN(req.WorkerID[2:], "_", 2)
	if len(parts) != 2 || parts[0] != req.Region {
		return ErrRegionMismatch
	}

	// 3. Atomic upsert — handles both new registrations and re-registrations.
	// Uses ON CONFLICT DO NOTHING so concurrent inserts for the same worker_id
	// are safely deduplicated; the RETURNING clause tells us if a row was inserted.
	wasCreated, err := s.workerRepo.Upsert(ctx, tenantID, req)
	if err != nil {
		return fmt.Errorf("upserting worker: %w", err)
	}

	// 4. If this was a re-registration (worker already existed), we're done.
	if !wasCreated {
		return nil
	}

	// 5. New worker: enforce MaxWorkers quota.
	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		// Rollback the newly inserted worker row.
		_ = s.workerRepo.Delete(ctx, req.WorkerID)
		return fmt.Errorf("getting quota: %w", err)
	}
	count, err := s.workerRepo.CountByTenant(ctx, tenantID)
	if err != nil {
		_ = s.workerRepo.Delete(ctx, req.WorkerID)
		return fmt.Errorf("counting workers: %w", err)
	}
	if count > quota.MaxWorkers {
		_ = s.workerRepo.Delete(ctx, req.WorkerID)
		return ErrQuotaExceeded
	}
	return nil
}

// ListByTenant returns all workers for a tenant.
func (s *WorkerService) ListByTenant(ctx context.Context, tenantID string) ([]domain.Worker, error) {
	return s.workerRepo.ListByTenant(ctx, tenantID)
}

// SubscribeHeartbeats starts a background NATS subscription to edgecloud.heartbeats.*
// and upserts worker status on each message.
func (s *WorkerService) SubscribeHeartbeats(ctx context.Context) error {
	if s.nc == nil {
		// No NATS connection — skip subscription (e.g., in tests)
		return nil
	}
	ch := make(chan *nats.Msg, 100)
	sub, err := s.nc.ChanSubscribe("edgecloud.heartbeats.>", ch)
	if err != nil {
		return fmt.Errorf("subscribing to heartbeats: %w", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				close(ch)
				return
			case msg := <-ch:
				s.handleHeartbeat(ctx, msg)
			}
		}
	}()
	return nil
}

func (s *WorkerService) handleHeartbeat(ctx context.Context, msg *nats.Msg) {
	var hb struct {
		Type       string          `json:"type"`
		Timestamp  time.Time       `json:"timestamp"`
		WorkerID   string          `json:"worker_id"`
		Region     string          `json:"region"`
		WorkerAddr string          `json:"worker_addr"`
		Apps       json.RawMessage `json:"apps"`
	}
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		return
	}
	if err := s.workerRepo.UpdateLastSeen(ctx, hb.WorkerID); err != nil {
		log.Printf("heartbeat: failed to update last_seen for %s: %v", hb.WorkerID, err)
	}
	// Only update the worker's public IP when the heartbeat actually carries
	// one — a heartbeat from a legacy worker (no EDGE_WORKER_ADDR) must not
	// clobber a previously-known good value.
	if hb.WorkerAddr != "" {
		if err := s.workerRepo.UpdateAddr(ctx, hb.WorkerID, hb.WorkerAddr); err != nil {
			log.Printf("heartbeat: failed to update ip for %s: %v", hb.WorkerID, err)
		}
	}

	ws := &domain.WorkerStatus{
		WorkerID:   hb.WorkerID,
		Apps:       hb.Apps,
		LastReport: hb.Timestamp,
	}
	if err := s.workerRepo.UpsertStatus(ctx, ws); err != nil {
		log.Printf("heartbeat: failed to upsert status for %s: %v", hb.WorkerID, err)
	}

	// Decode app statuses to sum outbound bytes and enforce the per-tenant
	// max_outbound_mb quota. Old workers omit outbound_bytes (defaults to 0),
	// which we treat as "no data" — we log but do not act on a 0-byte total
	// so a single old worker cannot cause a false quota violation.
	s.checkOutboundQuota(ctx, hb.Apps)
}

// checkOutboundQuota sums outbound_bytes across all apps in a heartbeat and
// logs a violation when a tenant's total for this interval exceeds their quota.
// Phase 1: log-only. Phase 2 (tracked in issue #120 follow-up): evict apps.
func (s *WorkerService) checkOutboundQuota(ctx context.Context, appsRaw json.RawMessage) {
	if len(appsRaw) == 0 {
		return
	}
	var apps map[string]domain.AppStatus
	if err := json.Unmarshal(appsRaw, &apps); err != nil {
		log.Printf("heartbeat: could not decode apps for quota check: %v", err)
		return
	}

	// Group outbound bytes by tenant across all apps in this heartbeat.
	byTenant := make(map[string]uint64)
	for _, app := range apps {
		if app.TenantID != "" {
			byTenant[app.TenantID] += app.OutboundBytes
		}
	}

	for tenantID, totalBytes := range byTenant {
		if totalBytes == 0 {
			// Old worker or no traffic — skip; don't act on missing data.
			continue
		}
		quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
		if err != nil || quota == nil {
			continue
		}
		if quota.MaxOutboundMB <= 0 {
			// Unlimited or unconfigured — nothing to enforce.
			continue
		}
		limitBytes := uint64(quota.MaxOutboundMB) * 1024 * 1024
		if totalBytes > limitBytes {
			log.Printf(
				"quota: tenant %s outbound bytes %d exceeds interval limit %d (%d MB) — enforcement pending",
				tenantID, totalBytes, limitBytes, quota.MaxOutboundMB,
			)
		}
	}
}

// GetAppTarget returns the running target for a single
// `(tenant_id, app_name)` pair, or `nil` when no running target is
// found. The CLI's `edge status` uses this to validate that a
// `live_url` is actually live. The query is scoped to the calling
// tenant at the SQL level so cross-tenant information is not loaded
// into Go memory.
func (s *WorkerService) GetAppTarget(ctx context.Context, tenantID, appName string) (*domain.AppTarget, error) {
	targets, err := s.workerRepo.ListRunningAppTarget(ctx, tenantID, appName)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, nil
	}
	return &targets[0], nil
}
