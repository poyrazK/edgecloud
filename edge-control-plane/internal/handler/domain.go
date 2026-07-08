package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// DomainServiceInterface is the narrow contract DomainHandler needs
// from DomainService. Mirrors the `appRepoInterface` / `workerRepoInterface`
// pattern in the service package — kept as an unexported interface so
// the concrete *service.DomainService satisfies it implicitly. The
// exported `NewDomainHandler` accepts the interface so tests can inject
// mocks; production wiring passes the concrete service.
type DomainServiceInterface interface {
	AddDomain(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error)
	ListDomains(ctx context.Context, tenantID, appName string) ([]domain.Domain, error)
	GetDomain(ctx context.Context, tenantID, appName, fqdn string) (*domain.Domain, error)
	RemoveDomain(ctx context.Context, tenantID, appName, fqdn string) error
}

// DomainHandler handles custom-domain HTTP requests for tenants.
type DomainHandler struct {
	domainSvc DomainServiceInterface
}

func NewDomainHandler(domainSvc *service.DomainService) *DomainHandler {
	return &DomainHandler{domainSvc: domainSvc}
}

// NewDomainHandlerFromMock is a constructor that accepts the
// `DomainServiceInterface` directly. Used by handler tests to inject a
// mock without standing up a real *service.DomainService (and thus a
// DB + appSvc). Production code MUST use `NewDomainHandler` so the
// concrete service is wired correctly.
func NewDomainHandlerFromMock(svc DomainServiceInterface) *DomainHandler {
	return &DomainHandler{domainSvc: svc}
}

// addDomainRequest is the POST body for `POST /api/v1/apps/{app}/domains`.
// Only `fqdn` is required; everything else is server-derived.
type addDomainRequest struct {
	FQDN string `json:"fqdn"`
}

// Add handles POST /api/v1/apps/{appName}/domains — bind a custom FQDN to an app.
//
// Returns 201 + the created row on success. 400 if the FQDN is malformed
// or the FQDN ends in `.edgecloud.dev`. 404 if the app does not exist.
// 429 if `MaxDomainsPerApp` is exceeded.
func (h *DomainHandler) Add(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	if appName == "" {
		httperror.BadRequestCtx(w, r, "app name required")
		return
	}

	var req addDomainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httperror.BadRequestCtx(w, r, "invalid request body")
		return
	}
	if req.FQDN == "" {
		httperror.BadRequestCtx(w, r, "fqdn required")
		return
	}

	d, err := h.domainSvc.AddDomain(r.Context(), tenantID, appName, req.FQDN)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidFQDN):
			httperror.BadRequestCtx(w, r, err.Error())
		case errors.Is(err, service.ErrDomainQuotaExceeded):
			httperror.QuotaExceededCtx(w, r, err.Error())
		case errors.Is(err, service.ErrAppNotFound):
			httperror.NotFoundCtx(w, r, err.Error())
		default:
			log.Printf("internal error: %v", err)
			httperror.InternalErrorCtx(w, r)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(d)
	auditRecord(r, "create", "domain", req.FQDN, "domain "+req.FQDN+" added to app "+appName, "success")
}

// List handles GET /api/v1/apps/{appName}/domains — list all custom domains
// for an app. Returns an empty array (not 404) when the app has no
// domains; matches `DeploymentRepository.ListByApp`.
func (h *DomainHandler) List(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	if appName == "" {
		httperror.BadRequestCtx(w, r, "app name required")
		return
	}

	domains, err := h.domainSvc.ListDomains(r.Context(), tenantID, appName)
	if err != nil {
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	if domains == nil {
		// Guarantee a JSON `[]` instead of `null` for empty lists. The CLI
		// relies on this shape for tabular output.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"domains":[]}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"domains": domains})
}

// Get handles GET /api/v1/apps/{appName}/domains/{fqdn} — fetch one domain.
//
// The FQDN may contain dots; `r.PathValue("fqdn")` returns the decoded
// segment as Go's `net/http` URL-decodes path values automatically. This
// means a literal `api.acme.com` round-trips cleanly.
func (h *DomainHandler) Get(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	fqdn := r.PathValue("fqdn")
	if appName == "" || fqdn == "" {
		httperror.BadRequestCtx(w, r, "app name and fqdn required")
		return
	}

	d, err := h.domainSvc.GetDomain(r.Context(), tenantID, appName, fqdn)
	if err != nil {
		if errors.Is(err, service.ErrDomainNotFound) {
			httperror.NotFoundCtx(w, r, "domain not found")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(d)
}

// Remove handles DELETE /api/v1/apps/{appName}/domains/{fqdn} — drop a
// custom domain. The 30s ingress poller will pick up the deletion on
// its next tick.
func (h *DomainHandler) Remove(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	appName := r.PathValue("appName")
	fqdn := r.PathValue("fqdn")
	if appName == "" || fqdn == "" {
		httperror.BadRequestCtx(w, r, "app name and fqdn required")
		return
	}

	err := h.domainSvc.RemoveDomain(r.Context(), tenantID, appName, fqdn)
	if err != nil {
		if errors.Is(err, service.ErrDomainNotFound) {
			httperror.NotFoundCtx(w, r, "domain not found")
			return
		}
		log.Printf("internal error: %v", err)
		httperror.InternalErrorCtx(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	auditRecord(r, "delete", "domain", fqdn, "domain "+fqdn+" removed from app "+appName, "success")
}
