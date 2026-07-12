package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/signing"
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

// WorkerJWTConfig holds the HMAC secrets and expected issuer for
// verifying worker JWTs. Supports two modes:
//
//  1. Legacy: a single Secret (no kid header in tokens).
//  2. Keyring: ActiveKID + Keys map. Tokens with a kid header are
//     verified against the matching key in Keys. Tokens without kid
//     fall back to Secret (if set) or the active key (if only Keys).
type WorkerJWTConfig struct {
	// Secret is a single JWT signing secret.
	// DEPRECATED: use Keys + ActiveKID instead.
	Secret string
	// Issuer is the expected `iss` claim. Empty = skip issuer check.
	Issuer string
	// ActiveKID is the key ID used for signing new tokens.
	// Only needed by callers that sign tokens (mintIngressToken).
	// Verification uses whichever kid the token carries.
	ActiveKID string
	// Keys maps kid → secret for verification. When set, tokens with
	// a kid header are verified against the matching key; tokens
	// without kid fall back to Secret.
	Keys map[string]string
	// WorkerKeyCache (issue #430) is the public-key cache that backs
	// the wkr_ verification branch. When nil, wkr_-kid tokens are
	// refused outright (fail-closed) — see resolveKey for the
	// rationale. Production wires this from app.New using
	// repository.WorkerRepository.GetPublicKey as the loader.
	WorkerKeyCache *WorkerKeyCache
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
// Key selection (in priority order):
//  1. If the token's `kid` header matches an entry in `cfg.Keys`, use that key.
//  2. If `kid` is present but not in `cfg.Keys`, fall back to `cfg.Secret`.
//  3. If `kid` is absent and `cfg.Secret` is set, use it (legacy compat).
//  4. If `kid` is absent and only `cfg.Keys` is configured, try the active key.
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
	opts = append(opts, jwt.WithIssuer(cfg.Issuer))
	token, err := jwt.ParseWithClaims(tokenString, &WorkerClaims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return cfg.resolveKey(token)
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

// resolveKey selects the verification key for an incoming JWT.
// Separate from VerifyWorkerJWT so ResolveSigningKey and this can
// share the lookup logic without circular calls.
//
// Key selection (in priority order):
//  1. kid header in wkr_ namespace (issue #430) — look up the
//     worker's public_key via the key cache and re-derive the
//     per-worker HS256 secret via HKDF.
//  2. kid header present and Keys configured.
//  3. Legacy Secret fallback.
//  4. Keyring configured, no Secret. Use the active key as default.
//
// The wkr_ branch takes priority over the legacy keyring so a
// token signed with a per-worker derived secret can never be
// accepted by a stale cluster secret — the bug that motivated
// issue #430 was exactly that the legacy key accepted a token
// signed by a leaked cluster secret.
func (cfg *WorkerJWTConfig) resolveKey(token *jwt.Token) ([]byte, error) {
	kid, _ := token.Header["kid"].(string)

	// 1. Per-worker derived secret (issue #430). Requires the
	// key cache to be wired — if it isn't, the wkr_ branch
	// refuses, which is the correct fail-closed behavior: an
	// unconfigured cache means the operator hasn't deployed the
	// schema migration or the loader, and silently falling back
	// to the legacy key would re-open the original defect.
	if signing.IsWorkerKID(kid) {
		if cfg.WorkerKeyCache == nil {
			return nil, errors.New("per-worker kid presented but WorkerKeyCache is not configured")
		}
		claims, ok := token.Claims.(*WorkerClaims)
		if !ok {
			return nil, errors.New("per-worker kid requires WorkerClaims")
		}
		if claims.WorkerID == "" {
			return nil, errors.New("per-worker kid requires worker_id claim")
		}
		// Use Background ctx for the cache lookup — the inbound
		// request ctx is fine in principle (the loader is a
		// single round-trip) but Background decouples the cache
		// lifetime from any client-side cancellation that might
		// fire mid-resolve. The middleware should never abandon
		// a verification it already started.
		pubkey, err := cfg.WorkerKeyCache.GetOrLoad(context.Background(), claims.WorkerID)
		if err != nil {
			return nil, fmt.Errorf("per-worker key lookup for %s: %w", claims.WorkerID, err)
		}
		if pubkey == "" {
			return nil, fmt.Errorf("worker %s has no enrolled public_key", claims.WorkerID)
		}
		// Sanity-check the kid matches the claimed pubkey. Without
		// this, a token minted for kid=wkr_abcdef but signed with
		// a secret derived from a different pubkey would verify —
		// exactly the cross-worker forgery we're closing.
		if kid != signing.WorkerKID(pubkey) {
			return nil, fmt.Errorf("kid %q does not match worker %s's enrolled pubkey", kid, claims.WorkerID)
		}
		derived, err := signing.DeriveWorkerSecret(
			[]byte(cfg.Secret), claims.WorkerID, claims.TenantID, claims.Region, pubkey)
		if err != nil {
			return nil, fmt.Errorf("derive worker secret for %s: %w", claims.WorkerID, err)
		}
		return derived, nil
	}

	// 2. kid header present and Keys configured.
	if kid != "" && len(cfg.Keys) > 0 {
		if secret, ok := cfg.Keys[kid]; ok {
			return []byte(secret), nil
		}
		// kid is in the token but not in our keyring. Fall through to
		// Secret fallback so a token signed with a recently-removed key
		// is still accepted during the transition window.
	}

	// 3. Legacy Secret fallback.
	if cfg.Secret != "" {
		return []byte(cfg.Secret), nil
	}

	// 4. Keyring configured, no Secret. Use the active key as default.
	if len(cfg.Keys) > 0 && cfg.ActiveKID != "" {
		if secret, ok := cfg.Keys[cfg.ActiveKID]; ok {
			return []byte(secret), nil
		}
	}

	return nil, errors.New("no matching signing key found")
}

// ResolveSigningKey returns the signing key bytes for the active key.
// Used by mintIngressToken to choose which key signs new tokens.
// Priority: cfg.Keys[cfg.ActiveKID] → []byte(cfg.Secret).
func (cfg WorkerJWTConfig) ResolveSigningKey() ([]byte, error) {
	// Prefer keyring with active KID.
	if len(cfg.Keys) > 0 && cfg.ActiveKID != "" {
		if secret, ok := cfg.Keys[cfg.ActiveKID]; ok {
			return []byte(secret), nil
		}
		return nil, fmt.Errorf("active_kid %q not found in keys", cfg.ActiveKID)
	}
	// Fallback to legacy Secret.
	if cfg.Secret != "" {
		return []byte(cfg.Secret), nil
	}
	return nil, errors.New("no signing key configured: set jwt.secret or jwt.keys")
}

// ResolveSigningKeyForWorker returns the HS256 signing key bound
// to a specific worker (issue #430). The caller (typically
// MintWorkerToken) supplies the worker's workerID + tenantID +
// region; the function looks up the worker's enrolled public_key
// and returns the HKDF-derived secret.
//
// Returns the same shape as ResolveSigningKey so callers can
// substitute one for the other in `token.SignedString(...)`.
//
// Requires cfg.WorkerKeyCache (the same cache WorkerAuth uses
// for verify). If the worker has not enrolled (no public_key
// stored), returns an error — MintWorkerToken must surface 500
// to the operator, because the only way a worker can reach this
// code path without an enrolled key is if the cluster was
// configured with WorkerKeyCache=nil (the wkr_ branch is
// fail-closed) or the worker hasn't run the bootstrap handshake.
func (cfg WorkerJWTConfig) ResolveSigningKeyForWorker(ctx context.Context, workerID, tenantID, region string) ([]byte, error) {
	if cfg.WorkerKeyCache == nil {
		return nil, errors.New("WorkerKeyCache not configured: cannot derive per-worker signing key")
	}
	if workerID == "" {
		return nil, errors.New("workerID required to derive per-worker signing key")
	}
	pubkey, err := cfg.WorkerKeyCache.GetOrLoad(ctx, workerID)
	if err != nil {
		return nil, fmt.Errorf("worker pubkey lookup for %s: %w", workerID, err)
	}
	if pubkey == "" {
		return nil, fmt.Errorf("worker %s has no enrolled public_key (must complete bootstrap handshake first)", workerID)
	}
	return signing.DeriveWorkerSecret([]byte(cfg.Secret), workerID, tenantID, region, pubkey)
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

// IsSharedWorker returns true if the context represents a multi-tenant/shared worker.
// A worker is shared if its tenant ID is wildcard ("*") or empty string.
func IsSharedWorker(ctx context.Context) bool {
	tID := GetWorkerTenantID(ctx)
	return tID == "*" || tID == ""
}
