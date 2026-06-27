package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/golang-jwt/jwt/v5"
)

// WorkerClaims are the JWT claims issued to workers.
type WorkerClaims struct {
	jwt.RegisteredClaims
	WorkerID string   `json:"worker_id"`
	TenantID string   `json:"tenant_id"`
	Region   string   `json:"region"`
	Apps     []string `json:"apps"`
	// Role distinguishes the bearer: per-worker tokens carry
	// RoleWorker (or empty, for backward compatibility with tokens
	// minted before this field existed); the long-lived ingress
	// service token carries RoleIngest so the control plane can
	// gate cross-tenant reads (ListDomains, TlsAllowed,
	// UpdateDomainStatus) to the poller only — a per-worker JWT
	// would otherwise see every tenant's domain mapping.
	Role string `json:"role,omitempty"`
}

// Role constants for the Role claim.
const (
	// RoleWorker is the default for any worker-issued JWT. May run
	// per-worker endpoints (e.g. /api/internal/download/*,
	// /api/internal/workers/*) but NOT the cross-tenant domain
	// endpoints.
	RoleWorker = "worker"
	// RoleIngest is for the long-lived ingress service token
	// (cmd/api/mint.go). Allows access to the cross-tenant domain
	// endpoints used by the FQDN poller and the v2 Caddy event
	// hook (issue #83).
	RoleIngest = "ingest"
)

// WorkerJWTConfig holds the HMAC secret and expected issuer.
type WorkerJWTConfig struct {
	Secret string
	Issuer string
}

const (
	WorkerIDKey       contextKey = "worker_id"
	WorkerTenantIDKey contextKey = "worker_tenant_id"
	WorkerRegionKey   contextKey = "worker_region"
	WorkerAppsKey     contextKey = "worker_apps"
	WorkerRoleKey     contextKey = "worker_role"
)

// VerifyWorkerJWT parses and validates a HMAC-SHA256 JWT.
//
// `exp` is required (jwt.WithExpirationRequired) so a token with no
// expiration cannot be replayed indefinitely after JWT_SECRET rotation.
// `iss` is enforced via jwt.WithIssuer(cfg.Issuer); the library skips
// the iss check when the supplied issuer is empty, so an empty
// cfg.Issuer effectively disables iss enforcement. Production callers
// must set cfg.Issuer (the control-plane config defaults it to
// "edgecloud"); only test setups with no issuer constraint may
// leave it empty.
func VerifyWorkerJWT(tokenString string, cfg WorkerJWTConfig) (*WorkerClaims, error) {
	opts := []jwt.ParserOption{jwt.WithExpirationRequired()}
	// Always call WithIssuer. The library short-circuits the iss check
	// when the supplied issuer is empty (c.iss == ""), so the behavior
	// is identical to the previous `if cfg.Issuer != ""` guard. The
	// explicit call makes the intent visible and removes a layer of
	// conditional indirection that the library handles internally.
	opts = append(opts, jwt.WithIssuer(cfg.Issuer))
	token, err := jwt.ParseWithClaims(tokenString, &WorkerClaims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(cfg.Secret), nil
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}
	claims, ok := token.Claims.(*WorkerClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid claims")
	}
	return claims, nil
}

// WorkerAuth returns a middleware that verifies Bearer JWT on the request.
func WorkerAuth(cfg WorkerJWTConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Header-only transport: a token in the URL would leak into
			// access logs, browser history, and reverse-proxy error pages.
			// (Previously `r.URL.Query().Get("jwt")` was a fallback; it was
			// removed because any leak of the URL — which is much more
			// likely than a leak of the header — would expose a 24h bearer.)
			token := r.Header.Get("Authorization")
			if token == "" {
				httperror.UnauthorizedCtx(w, r, "missing worker token")
				return
			}
			token = strings.TrimPrefix(token, "Bearer ")
			claims, err := VerifyWorkerJWT(token, cfg)
			if err != nil {
				httperror.UnauthorizedCtx(w, r, "invalid worker token")
				return
			}
			ctx := context.WithValue(r.Context(), WorkerIDKey, claims.WorkerID)
			ctx = context.WithValue(ctx, WorkerTenantIDKey, claims.TenantID)
			ctx = context.WithValue(ctx, WorkerRegionKey, claims.Region)
			ctx = context.WithValue(ctx, WorkerAppsKey, claims.Apps)
			ctx = context.WithValue(ctx, WorkerRoleKey, claims.Role)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GetWorkerTenantID extracts the worker tenant ID from context.
func GetWorkerTenantID(ctx context.Context) string {
	if id, ok := ctx.Value(WorkerTenantIDKey).(string); ok {
		return id
	}
	return ""
}

// GetWorkerID extracts the worker ID from context.
func GetWorkerID(ctx context.Context) string {
	if id, ok := ctx.Value(WorkerIDKey).(string); ok {
		return id
	}
	return ""
}

// GetWorkerRegion extracts the worker region from context.
func GetWorkerRegion(ctx context.Context) string {
	if r, ok := ctx.Value(WorkerRegionKey).(string); ok {
		return r
	}
	return ""
}

// GetWorkerRole extracts the role claim from context. Returns "" if
// the JWT predates the role field (backward compatibility — such
// tokens are treated as RoleWorker by HasRole).
func GetWorkerRole(ctx context.Context) string {
	if r, ok := ctx.Value(WorkerRoleKey).(string); ok {
		return r
	}
	return ""
}

// HasRole reports whether the request's JWT carries the named role.
// Empty role (legacy tokens without the claim) matches RoleWorker so
// existing per-worker endpoints keep working.
func HasRole(ctx context.Context, role string) bool {
	got := GetWorkerRole(ctx)
	if got == "" {
		got = RoleWorker
	}
	return got == role
}

// RequireWorkerRole returns a middleware that 403s any request whose
// JWT does not carry the named role. Used to gate cross-tenant
// internal endpoints (issue #83's domain poller feeds) to the
// long-lived ingress token rather than per-worker JWTs.
func RequireWorkerRole(role string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !HasRole(r.Context(), role) {
				httperror.ForbiddenCtx(w, r, "insufficient role")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
