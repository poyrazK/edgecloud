package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/nats-io/nats.go"
)

// WorkerService handles worker lifecycle business logic.
type WorkerService struct {
	workerRepo *repository.WorkerRepository
	quotaRepo  *repository.QuotaRepository
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
		return errors.New("invalid worker_id format: must be w_<region>_<uuid>")
	}

	// 2. Validate region in worker_id matches request region
	parts := strings.SplitN(req.WorkerID[2:], "_", 2)
	if len(parts) != 2 || parts[0] != req.Region {
		return fmt.Errorf("region mismatch: worker_id region %q does not match request region %q", parts[0], req.Region)
	}

	// 3. Check if worker already exists
	existing, err := s.workerRepo.GetByID(ctx, req.WorkerID)
	if err != nil {
		return fmt.Errorf("checking existing worker: %w", err)
	}
	if existing != nil {
		// Idempotent: just update last_seen
		return s.workerRepo.UpdateLastSeen(ctx, req.WorkerID)
	}

	// 4. Enforce MaxWorkers quota
	quota, err := s.quotaRepo.GetByTenantID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("getting quota: %w", err)
	}
	count, err := s.workerRepo.CountByTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("counting workers: %w", err)
	}
	if count >= quota.MaxWorkers {
		return fmt.Errorf("max workers (%d) reached for tenant", quota.MaxWorkers)
	}

	// 5. Create new worker
	memoryMB := req.MemoryMB
	if memoryMB == 0 {
		memoryMB = 4096
	}
	var ip *string
	if req.IP != "" {
		ip = &req.IP
	}
	worker := &domain.Worker{
		ID:        req.WorkerID,
		TenantID:  tenantID,
		Region:    req.Region,
		IP:        ip,
		MemoryMB:  memoryMB,
		LastSeen:  time.Now(),
		CreatedAt: time.Now(),
	}
	return s.workerRepo.Create(ctx, worker)
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
		Type      string    `json:"type"`
		Timestamp time.Time `json:"timestamp"`
		WorkerID  string    `json:"worker_id"`
		Region    string    `json:"region"`
		Apps      json.RawMessage `json:"apps"`
	}
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		return
	}
	// Update last_seen (idempotent — worker must already be registered)
	_ = s.workerRepo.UpdateLastSeen(ctx, hb.WorkerID)

	// Upsert per-app status
	ws := &domain.WorkerStatus{
		WorkerID:   hb.WorkerID,
		Apps:       hb.Apps,
		LastReport: hb.Timestamp,
	}
	_ = s.workerRepo.UpsertStatus(ctx, ws)
}
