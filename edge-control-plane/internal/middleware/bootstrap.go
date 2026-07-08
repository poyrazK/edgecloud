package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
	"github.com/golang-jwt/jwt/v5"
)

// BootstrapClaims are the JWT claims issued to workers during the
// bootstrap handshake. These are short-lived (5 minutes) and only
// authorize access to GET /api/internal/worker-secret.
type BootstrapClaims struct {
	jwt.RegisteredClaims
	WorkerID string `json:"worker_id"`
	TenantID string `json:"tenant_id"`
	Region   string `json:"region"`
}

// BootstrapJWTConfig holds the bootstrap secret for signing and
// verifying bootstrap JWTs.
type BootstrapJWTConfig struct {
	// BootstrapSecret is the shared HMAC secret used to sign the
	// bootstrap JWT and verify bootstrap request signatures.
	// Must match BOOTSTRAP_SECRET on the worker side.
	BootstrapSecret string

	// BootstrapJWTSecret is the key used to sign bootstrap JWTs
	// themselves (which the worker uses to fetch the real secret).
	// Defaults to BootstrapSecret when empty.
	BootstrapJWTSecret string

	// Issuer for bootstrap JWTs. Defaults to "edgecloud-bootstrap".
	Issuer string
}

// BootstrapRequest is the JSON body for a bootstrap request.
type BootstrapRequest struct {
	WorkerID  string `json:"worker_id"`
	Region    string `json:"region"`
	TenantID  string `json:"tenant_id"`
	Timestamp string `json:"timestamp"` // RFC3339, used for replay protection
	Nonce     string `json:"nonce"`     // random value for replay protection
	Signature string `json:"signature"` // HMAC-SHA256 of worker_id+region+tenant_id+timestamp+nonce
}

// ValidateAndVerifyBootstrapRequest verifies the bootstrap request's
// HMAC-SHA256 signature. The signature is computed over:
// worker_id + ":" + region + ":" + tenant_id + ":" + timestamp + ":" + nonce
func ValidateAndVerifyBootstrapRequest(req *BootstrapRequest, secret []byte) error {
	if req.WorkerID == "" || req.Region == "" || req.TenantID == "" {
		return errors.New("worker_id, region, and tenant_id are required")
	}
	if req.Timestamp == "" || req.Nonce == "" || req.Signature == "" {
		return errors.New("timestamp, nonce, and signature are required")
	}

	// Reject timestamps older than 5 minutes (replay protection).
	ts, err := time.Parse(time.RFC3339, req.Timestamp)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}
	if time.Since(ts) > 5*time.Minute || time.Since(ts) < -1*time.Minute {
		return errors.New("timestamp is too old or in the future")
	}

	// Reconstruct the signed payload.
	payload := fmt.Sprintf("%s:%s:%s:%s:%s",
		req.WorkerID, req.Region, req.TenantID, req.Timestamp, req.Nonce)

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	expectedSig := hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(req.Signature), []byte(expectedSig)) {
		return errors.New("invalid signature")
	}
	return nil
}

// IssueBootstrapJWT creates a short-lived JWT from the bootstrap secret.
// The token is valid for 5 minutes and contains the worker's identity.
func IssueBootstrapJWT(cfg BootstrapJWTConfig, workerID, tenantID, region string) (string, error) {
	now := time.Now()
	secret := cfg.BootstrapJWTSecret
	if secret == "" {
		secret = cfg.BootstrapSecret
	}
	if secret == "" {
		return "", errors.New("bootstrap secret is not configured")
	}

	issuer := cfg.Issuer
	if issuer == "" {
		issuer = "edgecloud-bootstrap"
	}

	claims := BootstrapClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now.Add(-30 * time.Second)),
			ID:        fmt.Sprintf("bs-%d", now.UnixNano()),
		},
		WorkerID: workerID,
		TenantID: tenantID,
		Region:   region,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("signing bootstrap JWT: %w", err)
	}
	return tokenString, nil
}

// VerifyBootstrapJWT parses and validates a bootstrap JWT.
// Returns the claims on success.
func VerifyBootstrapJWT(tokenString string, cfg BootstrapJWTConfig) (*BootstrapClaims, error) {
	secret := cfg.BootstrapJWTSecret
	if secret == "" {
		secret = cfg.BootstrapSecret
	}
	if secret == "" {
		return nil, errors.New("bootstrap secret is not configured")
	}

	issuer := cfg.Issuer
	if issuer == "" {
		issuer = "edgecloud-bootstrap"
	}

	token, err := jwt.ParseWithClaims(tokenString, &BootstrapClaims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secret), nil
	}, jwt.WithExpirationRequired(), jwt.WithIssuer(issuer))
	if err != nil {
		return nil, fmt.Errorf("invalid bootstrap token: %w", err)
	}
	claims, ok := token.Claims.(*BootstrapClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid bootstrap claims")
	}
	return claims, nil
}

// BootstrapAuth returns a middleware that verifies a bootstrap JWT on
// the request — separate from the regular WorkerAuth middleware because
// bootstrap JWTs use a different key and a shorter expiry.
func BootstrapAuth(cfg BootstrapJWTConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := r.Header.Get("Authorization")
			if token == "" {
				httperror.UnauthorizedCtx(w, r, "missing bootstrap token")
				return
			}
			token = strings.TrimPrefix(token, "Bearer ")
			claims, err := VerifyBootstrapJWT(token, cfg)
			if err != nil {
				httperror.UnauthorizedCtx(w, r, "invalid bootstrap token")
				return
			}
			ctx := context.WithValue(r.Context(), WorkerIDKey, claims.WorkerID)
			ctx = context.WithValue(ctx, WorkerTenantIDKey, claims.TenantID)
			ctx = context.WithValue(ctx, WorkerRegionKey, claims.Region)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
