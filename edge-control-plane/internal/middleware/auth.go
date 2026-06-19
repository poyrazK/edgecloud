package middleware

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// AuthMiddleware authenticates incoming requests by Bearer-token API key.
// It delegates to APIKeyService.AuthenticateRawKey, which dispatches to
// the algorithm-specific verifier (argon2id for new keys; lazy upgrade
// from legacy SHA-256) and enforces ExpiresAt.
//
// Earlier versions of this middleware called the repository directly and
// treated the lookup_hash match as proof of validity, which silently
// bypassed argon2id verification and ignored expiry. That bypass is the
// headline reason this file was rewritten.
type AuthMiddleware struct {
	apiKeySvc *service.APIKeyService
}

func NewAuthMiddleware(apiKeySvc *service.APIKeyService) *AuthMiddleware {
	return &AuthMiddleware{apiKeySvc: apiKeySvc}
}

// contextKey is a custom type for context keys to avoid collisions.
type contextKey string

const TenantIDKey contextKey = "tenant_id"
const APIKeyIDKey contextKey = "api_key_id"
const RoleKey contextKey = "role"

// Authenticate extracts the Bearer token and validates it through the
// APIKeyService. On success the tenant_id, api_key_id, and role are
// stored in the request context for downstream handlers.
func (m *AuthMiddleware) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, `{"error": "missing authorization header"}`, http.StatusUnauthorized)
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
			http.Error(w, `{"error": "invalid authorization format"}`, http.StatusUnauthorized)
			return
		}

		// Trim trailing whitespace from the token. A header like
		// "Bearer <key> " (trailing space) would otherwise hash to a
		// different value than "Bearer <key>" and reject a valid key.
		rawKey := strings.TrimSpace(parts[1])
		if rawKey == "" {
			http.Error(w, `{"error": "invalid api key"}`, http.StatusUnauthorized)
			return
		}

		apiKey, err := m.apiKeySvc.AuthenticateRawKey(r.Context(), rawKey)
		if err != nil {
			// service.ErrInvalidAPIKey is the public "401 Unauthorized"
			// signal; any other error is an infrastructure failure and
			// must surface as 500.
			if errors.Is(err, service.ErrInvalidAPIKey) {
				http.Error(w, `{"error": "invalid api key"}`, http.StatusUnauthorized)
				return
			}
			http.Error(w, `{"error": "internal error"}`, http.StatusInternalServerError)
			return
		}

		ctx := context.WithValue(r.Context(), TenantIDKey, apiKey.TenantID)
		ctx = context.WithValue(ctx, APIKeyIDKey, apiKey.ID)
		ctx = context.WithValue(ctx, RoleKey, apiKey.Role)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireRole checks that the authenticated user has one of the allowed roles.
func RequireRole(allowed ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			role, ok := r.Context().Value(RoleKey).(string)
			if !ok {
				http.Error(w, `{"error": "unauthorized"}`, http.StatusUnauthorized)
				return
			}

			for _, a := range allowed {
				if role == a {
					next.ServeHTTP(w, r)
					return
				}
			}

			http.Error(w, `{"error": "forbidden"}`, http.StatusForbidden)
		})
	}
}

// GetTenantID extracts tenant ID from context.
func GetTenantID(ctx context.Context) string {
	if id, ok := ctx.Value(TenantIDKey).(string); ok {
		return id
	}
	return ""
}

// WithTenantID returns a new context with tenant ID set. Used for testing.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, TenantIDKey, tenantID)
}
