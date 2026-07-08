package service

import (
	"log"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// Auditor records audit events for state-changing API calls.
// Insertions are best-effort — failures are logged but never returned to
// the caller. Auditor is safe to use without initializing the repo
// (Record becomes a no-op).
type Auditor struct {
	repo *repository.AuditRepository
}

// AuditInfo carries the caller-provided metadata for an audit event.
// Handlers extract auth context from the request and build this struct
// before calling Record, avoiding a service→middleware import cycle.
type AuditInfo struct {
	TenantID   string
	APIKeyID   string
	Role       string
	Action     string
	Resource   string
	ResourceID string
	Details    string
	Outcome    string
	ErrorMsg   string
	RequestIP  string
}

func NewAuditor(repo *repository.AuditRepository) *Auditor {
	return &Auditor{repo: repo}
}

// Record inserts an audit row. Best-effort: logs errors but never panics.
func (a *Auditor) Record(info AuditInfo) {
	if a == nil || a.repo == nil {
		return
	}
	ev := &domain.AuditEvent{
		TenantID:   info.TenantID,
		APIKeyID:   info.APIKeyID,
		Role:       info.Role,
		Action:     info.Action,
		Resource:   info.Resource,
		ResourceID: info.ResourceID,
		Details:    info.Details,
		Outcome:    info.Outcome,
		ErrorMsg:   info.ErrorMsg,
		RequestIP:  info.RequestIP,
		CreatedAt:  time.Now().UTC(),
	}
	if _, err := a.repo.Insert(noopCtx, ev); err != nil {
		log.Printf("audit: failed to record %s/%s: %v", info.Resource, info.Action, err)
	}
}

// noopCtx is a placeholder context for repository calls that don't
// carry cancellation or deadlines (audit writes are best-effort).
var noopCtx = &auditCtx{}

type auditCtx struct{}

func (*auditCtx) Deadline() (deadline time.Time, ok bool) { return }
func (*auditCtx) Done() <-chan struct{}                   { return nil }
func (*auditCtx) Err() error                              { return nil }
func (*auditCtx) Value(any) any                           { return nil }

// stripPort removes the port portion from a "host:port" address.
func StripPort(addr string) string {
	for i := 0; i < len(addr); i++ {
		if addr[i] == ':' {
			return addr[:i]
		}
	}
	return addr
}
