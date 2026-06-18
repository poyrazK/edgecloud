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
	ListRunningAppTargets(ctx context.Context) ([]domain.AppTarget, error)
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
}

// ListRunningAppTargets returns every app currently routable on a worker.
// The public ingress can use this to cold-start its routing table before
// the first heartbeat lands; the CLI uses it to validate that a
// `live_url` is actually live.
func (s *WorkerService) ListRunningAppTargets(ctx context.Context) ([]domain.AppTarget, error) {
	return s.workerRepo.ListRunningAppTargets(ctx)
}
