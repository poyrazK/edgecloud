package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/workerclaims"
	"github.com/golang-jwt/jwt/v5"
)

// WorkerClaims is a type alias for `workerclaims.WorkerClaims` so all
// existing call sites (`claims := middleware.WorkerClaims{...}`)
// keep compiling unchanged. PR #200 review finding H2: this alias
// makes the verifier and the minter reference the same underlying
// struct, so adding a new claim in `workerclaims` automatically
// threads through both halves — eliminating the silent-drift risk of
// two separately-defined structs.
type WorkerClaims = workerclaims.WorkerClaims

// Role constants are re-exported from the workerclaims package so
// existing references (`middleware.RoleIngest`) keep working.
const (
	RoleWorker = workerclaims.RoleWorker
	RoleIngest = workerclaims.RoleIngest
)

// WorkerJWTConfig holds the HMAC secret, expected issuer, and
// expected audience. PR #200 review finding H8: the `aud` claim is
// a defense-in-depth check that distinguishes worker-issued JWTs
// from any other JWT type minted with the same secret in the
// future (e.g. an admin or tenant-facing token). An empty
// `Audience` disables the check, mirroring the existing `Issuer`
// behavior — production callers must set both.
type WorkerJWTConfig struct {
	Secret   string
	Issuer   string
	Audience string
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
	// PR #200 review finding H8: gate on the `aud` claim so worker
	// JWTs are distinguishable from any other token type minted with
	// the same secret in the future. The library's `WithAudience`
	// does NOT short-circuit on an empty string (unlike `WithIssuer`,
	// which treats empty as "no issuer check"); it actively requires
	// the claim to be present. Skip the option entirely when
	// `cfg.Audience == ""` so existing call sites (which predated
	// the field) keep working unchanged. Production callers must set
	// `WorkerJWTConfig.Audience = "edge-internal"`.
	if cfg.Audience != "" {
		opts = append(opts, jwt.WithAudience(cfg.Audience))
	}
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
