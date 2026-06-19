package middleware

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// stubAPIKeyRepo is a service.APIKeyRepo stub for the middleware tests.
// Each test wires only the methods it cares about; the rest return safe
// defaults so the middleware path can be exercised in isolation.
type stubAPIKeyRepo struct {
	getByLookupHashFn func(ctx context.Context, lookupHash string) (*domain.APIKey, error)
}

func (s *stubAPIKeyRepo) Create(ctx context.Context, k *domain.APIKey) error { return nil }
func (s *stubAPIKeyRepo) GetByLookupHash(ctx context.Context, lookupHash string) (*domain.APIKey, error) {
	if s.getByLookupHashFn != nil {
		return s.getByLookupHashFn(ctx, lookupHash)
	}
	return nil, nil
}
func (s *stubAPIKeyRepo) ListByTenant(ctx context.Context, tenantID string) ([]domain.APIKey, error) {
	return nil, nil
}
func (s *stubAPIKeyRepo) Delete(ctx context.Context, id string) error { return nil }
func (s *stubAPIKeyRepo) UpdateLastUsed(ctx context.Context, id string) error {
	return nil
}
func (s *stubAPIKeyRepo) UpdateHashIfAlgorithm(ctx context.Context, id, currentAlgo, newHash, newAlgo string) (int64, error) {
	return 0, nil
}

// newAuthSvc wires a real *service.APIKeyService with a stub repo so the
// middleware path actually runs through the service-layer verify. The raw
// key is whatever the test wants AuthenticateRawKey to receive; the stub
// can choose to ignore it (e.g. for "no such key" cases) or hash it
// (for happy paths).
func newAuthSvc(getFn func(ctx context.Context, lookupHash string) (*domain.APIKey, error)) *service.APIKeyService {
	svc := service.NewAPIKeyService(nil)
	return svc.SetAPIKeyRepo(&stubAPIKeyRepo{getByLookupHashFn: getFn})
}

// newAuthSvcWithHash wires a service that hashes the supplied raw key with
// argon2id and stores it on the returned APIKey. The stub returns this row
// for any GetByLookupHash call. Use this for happy-path tests so the
// service-layer verify actually runs.
func newAuthSvcWithHash(t *testing.T, raw string, mutate func(*domain.APIKey)) *service.APIKeyService {
	t.Helper()
	hash, err := service.HashAPIKey(raw)
	if err != nil {
		t.Fatalf("HashAPIKey: %v", err)
	}
	k := &domain.APIKey{
		ID:            "k_test",
		TenantID:      "t_test",
		KeyHash:       hash,
		LookupHash:    "lookup-" + raw,
		HashAlgorithm: domain.HashAlgorithmArgon2ID,
		Role:          domain.RoleDeveloper,
	}
	if mutate != nil {
		mutate(k)
	}
	return newAuthSvc(func(ctx context.Context, lookupHash string) (*domain.APIKey, error) {
		return k, nil
	})
}

// runMiddleware drives a request through AuthMiddleware + a no-op handler
// and returns the recorder. The handler echoes the role from context so
// tests can assert the middleware injected it.
func runMiddleware(svc *service.APIKeyService, req *http.Request) *httptest.ResponseRecorder {
	mw := NewAuthMiddleware(svc)
	handler := mw.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if role, ok := r.Context().Value(RoleKey).(string); ok {
			_, _ = io.WriteString(w, "role="+role)
		}
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestAuthenticate_RejectsMissingHeader(t *testing.T) {
	svc := newAuthSvc(nil)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := runMiddleware(svc, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if !strings.Contains(rec.Body.String(), "missing authorization") {
		t.Errorf("body %q should mention 'missing authorization'", rec.Body.String())
	}
}

func TestAuthenticate_RejectsMalformedHeader(t *testing.T) {
	cases := []struct {
		name, header string
	}{
		{"no scheme", "just-a-key"},
		{"wrong scheme", "Basic dXNlcjpwYXNz"},
		{"empty scheme", " key"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc := newAuthSvc(nil)
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", c.header)
			rec := runMiddleware(svc, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d (body=%q)", rec.Code, http.StatusUnauthorized, rec.Body.String())
			}
		})
	}
}

func TestAuthenticate_RejectsEmptyBearer(t *testing.T) {
	svc := newAuthSvc(nil)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer   ") // spaces only
	rec := runMiddleware(svc, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (body=%q)", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

func TestAuthenticate_RejectsInvalidKey(t *testing.T) {
	// Lookup returns nil (no row) → service returns ErrInvalidAPIKey.
	svc := newAuthSvc(func(ctx context.Context, lookupHash string) (*domain.APIKey, error) {
		return nil, nil
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer does-not-exist")
	rec := runMiddleware(svc, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthenticate_RejectsWrongKey(t *testing.T) {
	// Row exists with one hash, but client presents a different raw key.
	// Service must call VerifyAPIKey and reject with ErrInvalidAPIKey.
	svc := newAuthSvcWithHash(t, "correct-key", nil)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := runMiddleware(svc, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (body=%q)", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

func TestAuthenticate_RejectsExpiredKey(t *testing.T) {
	svc := newAuthSvcWithHash(t, "expired-key", func(k *domain.APIKey) {
		past := time.Now().Add(-1 * time.Hour)
		k.ExpiresAt = &past
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer expired-key")
	rec := runMiddleware(svc, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (body=%q)", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

func TestAuthenticate_AcceptsValidKey(t *testing.T) {
	svc := newAuthSvcWithHash(t, "valid-key", nil)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := runMiddleware(svc, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body=%q)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "role="+domain.RoleDeveloper) {
		t.Errorf("body %q should contain injected role", rec.Body.String())
	}
}

func TestAuthenticate_InternalErrorOnDBFailure(t *testing.T) {
	// Repo returns a non-ErrInvalidAPIKey error → middleware surfaces 500.
	svc := newAuthSvc(func(ctx context.Context, lookupHash string) (*domain.APIKey, error) {
		return nil, errors.New("connection refused")
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer any")
	rec := runMiddleware(svc, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d (body=%q)", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestAuthenticate_BearerSchemeIsCaseInsensitive(t *testing.T) {
	svc := newAuthSvcWithHash(t, "mixed-case-bearer", nil)
	for _, scheme := range []string{"bearer", "Bearer", "BEARER", "BeArEr"} {
		t.Run(scheme, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", scheme+" mixed-case-bearer")
			rec := runMiddleware(svc, req)
			if rec.Code != http.StatusOK {
				t.Errorf("scheme %q: status = %d, want %d", scheme, rec.Code, http.StatusOK)
			}
		})
	}
}
