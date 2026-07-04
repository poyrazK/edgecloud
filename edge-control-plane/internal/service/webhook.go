package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

// WebhookServiceInterface abstracts webhook operations for the handler.
type WebhookServiceInterface interface {
	Create(ctx context.Context, wh *domain.Webhook) error
	ListByTenant(ctx context.Context, tenantID string) ([]domain.Webhook, error)
	GetByID(ctx context.Context, id string) (*domain.Webhook, error)
	Update(ctx context.Context, wh *domain.Webhook) error
	Delete(ctx context.Context, id, tenantID string) (bool, error)
	PublishEvent(ctx context.Context, tenantID, appName, eventType string, payload interface{})
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
