package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// WebhookService dispatches webhook HTTP POSTs on deployment lifecycle events.
// Operations are best-effort — failures are logged but never returned.
type WebhookService struct {
	repo     *repository.WebhookRepository
	client   *http.Client
	retryMax int
	interval time.Duration
}

// Limits for the deliveries read endpoint (issue #659). Same shape as
// logs.ResolveLimit — single source of truth for the policy, exported so
// the handler can echo the post-clamp value in the response envelope
// without re-implementing the policy.
const (
	DefaultWebhookDeliveryLimit = 50
	MaxWebhookDeliveryLimit     = 200
)

// WebhookDeliveriesResult is the service-bound result for
// ListDeliveriesByWebhook. Mirrors the logs envelope shape (see
// LogListResult) so the wire can be reasoned about with one rule.
type WebhookDeliveriesResult struct {
	Deliveries []domain.WebhookDelivery
	Limit      int
	NextCursor *string
}

// ErrWebhookNotFound is returned by ListDeliveriesByWebhook when the
// webhook does not exist OR belongs to a different tenant. Collapsing
// both cases into one typed error prevents enumeration of webhook IDs
// across tenants.
var ErrWebhookNotFound = errors.New("webhook not found")

// WebhookServiceInterface abstracts webhook operations for the handler.
type WebhookServiceInterface interface {
	Create(ctx context.Context, wh *domain.Webhook) error
	ListByTenant(ctx context.Context, tenantID string) ([]domain.Webhook, error)
	GetByID(ctx context.Context, id string) (*domain.Webhook, error)
	Update(ctx context.Context, wh *domain.Webhook) error
	Delete(ctx context.Context, id, tenantID string) (bool, error)
	PublishEvent(ctx context.Context, tenantID, appName, eventType string, payload interface{})
	ListDeliveriesByWebhook(ctx context.Context, tenantID, webhookID string, limit int, cursor string) (*WebhookDeliveriesResult, error)
}

func NewWebhookService(repo *repository.WebhookRepository) *WebhookService {
	return &WebhookService{
		repo:     repo,
		client:   &http.Client{Timeout: 10 * time.Second},
		retryMax: 3,
		interval: 5 * time.Second,
	}
}

func (s *WebhookService) Create(ctx context.Context, wh *domain.Webhook) error {
	return s.repo.Create(ctx, wh)
}
func (s *WebhookService) ListByTenant(ctx context.Context, tenantID string) ([]domain.Webhook, error) {
	return s.repo.ListByTenant(ctx, tenantID)
}
func (s *WebhookService) GetByID(ctx context.Context, id string) (*domain.Webhook, error) {
	return s.repo.GetByID(ctx, id)
}
func (s *WebhookService) Update(ctx context.Context, wh *domain.Webhook) error {
	return s.repo.Update(ctx, wh)
}
func (s *WebhookService) Delete(ctx context.Context, id, tenantID string) (bool, error) {
	return s.repo.Delete(ctx, id, tenantID)
}

func (s *WebhookService) PublishEvent(ctx context.Context, tenantID, appName, eventType string, payload interface{}) {
	if s == nil || s.repo == nil {
		return
	}

	webhooks, err := s.repo.ListEnabledByTenantAndEvent(ctx, tenantID, eventType)
	if err != nil {
		log.Printf("webhook: listing subscriptions for %s/%s: %v", tenantID, eventType, err)
		return
	}
	if len(webhooks) == 0 {
		return
	}

	event := &domain.WebhookEvent{
		EventType: eventType,
		TenantID:  tenantID,
		AppName:   appName,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}

	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("webhook: marshal event for %s/%s: %v", tenantID, eventType, err)
		return
	}

	for _, wh := range webhooks {
		wh := wh
		go s.deliver(ctx, wh, body, eventType)
	}
}

func (s *WebhookService) deliver(ctx context.Context, wh domain.Webhook, body []byte, eventType string) {
	mac := hmac.New(sha256.New, []byte(wh.Secret))
	mac.Write(body)
	sig := hex.EncodeToString(mac.Sum(nil))

	now := time.Now().UTC()
	delivery := &domain.WebhookDelivery{
		WebhookID:   wh.ID,
		EventType:   eventType,
		Status:      "retrying",
		RequestBody: string(body),
		Attempt:     1,
		MaxAttempts: s.retryMax,
		CreatedAt:   now,
	}

	for attempt := 1; attempt <= s.retryMax; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, wh.URL, bytes.NewReader(body))
		if err != nil {
			delivery.ErrorMsg = fmt.Sprintf("create request: %v", err)
			delivery.Attempt = attempt
			s.logDelivery(ctx, delivery)
			time.Sleep(s.interval)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Webhook-Signature", "sha256="+sig)
		req.Header.Set("User-Agent", "edgecloud-webhook/1.0")

		resp, err := s.client.Do(req)
		completed := time.Now().UTC()
		delivery.CompletedAt = &completed
		delivery.Attempt = attempt

		if err != nil {
			delivery.Status = "failed"
			delivery.ErrorMsg = fmt.Sprintf("attempt %d: %v", attempt, err)
			s.logDelivery(ctx, delivery)
			if attempt < s.retryMax {
				time.Sleep(s.interval)
			}
			continue
		}

		delivery.StatusCode = &resp.StatusCode
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			delivery.Status = "success"
			delivery.ErrorMsg = ""
			s.logDelivery(ctx, delivery)
			return
		}

		delivery.Status = "failed"
		delivery.ErrorMsg = fmt.Sprintf("HTTP %d", resp.StatusCode)
		s.logDelivery(ctx, delivery)
		if attempt < s.retryMax {
			time.Sleep(s.interval)
		}
	}
}

func (s *WebhookService) logDelivery(ctx context.Context, d *domain.WebhookDelivery) {
	if _, err := s.repo.InsertDelivery(ctx, d); err != nil {
		log.Printf("webhook: log delivery %s/%s: %v", d.WebhookID, d.EventType, err)
	}
}

// ListDeliveriesByWebhook serves GET /api/v1/webhooks/{id}/deliveries.
// Mirrors the LogService.ListByTenantApp pattern (issue #644, PR #681):
//
//  1. Ownership check via GetByID + tenant_id compare. Missing webhook
//     AND wrong-tenant webhook both surface as ErrWebhookNotFound so the
//     wire response cannot be used to enumerate webhook IDs across tenants.
//  2. Decode the opaque cursor (typed errors → 400 in the handler).
//  3. Clamp limit via the same `<=0 → default / >max → max` rule used by
//     ResolveLimit (logs).
//  4. Request limit+1 rows from the repository, trim, derive hasMore.
//  5. Encode next_cursor from the last visible row's (created_at, id)
//     when hasMore is true.
//
// Returns ErrWebhookNotFound, ErrInvalidWebhookDeliveryCursor, or
// ErrUnsupportedWebhookDeliveryCursorVersion as typed errors — the
// handler maps these to 404 and 400 respectively (with structured
// log.Printf on the cursor path).
func (s *WebhookService) ListDeliveriesByWebhook(
	ctx context.Context, tenantID, webhookID string, limit int, cursor string,
) (*WebhookDeliveriesResult, error) {
	wh, err := s.repo.GetByID(ctx, webhookID)
	if err != nil {
		return nil, fmt.Errorf("get webhook %s: %w", webhookID, err)
	}
	if wh == nil || wh.TenantID != tenantID {
		return nil, ErrWebhookNotFound
	}

	effectiveLimit := limit
	switch {
	case effectiveLimit <= 0:
		effectiveLimit = DefaultWebhookDeliveryLimit
	case effectiveLimit > MaxWebhookDeliveryLimit:
		effectiveLimit = MaxWebhookDeliveryLimit
	}

	var cursorTS time.Time
	var cursorID int64
	hasCursor := cursor != ""
	if hasCursor {
		cursorTS, cursorID, err = decodeWebhookDeliveryCursor(cursor)
		if err != nil {
			return nil, err
		}
	}

	deliveries, err := s.repo.ListDeliveriesByWebhook(ctx, repository.WebhookDeliveryListFilter{
		WebhookID: webhookID,
		Limit:     effectiveLimit + 1,
		CursorTS:  cursorTS,
		CursorID:  cursorID,
		HasCursor: hasCursor,
	})
	if err != nil {
		return nil, err
	}

	hasMore := len(deliveries) > effectiveLimit
	if hasMore {
		deliveries = deliveries[:effectiveLimit]
	}

	var nextCursor *string
	if hasMore && len(deliveries) > 0 {
		last := deliveries[len(deliveries)-1]
		encoded, err := encodeWebhookDeliveryCursor(last.CreatedAt, last.ID)
		if err != nil {
			return nil, fmt.Errorf("encode next cursor: %w", err)
		}
		nextCursor = &encoded
	}

	return &WebhookDeliveriesResult{
		Deliveries: deliveries,
		Limit:      effectiveLimit,
		NextCursor: nextCursor,
	}, nil
}
