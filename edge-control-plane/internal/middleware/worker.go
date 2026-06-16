package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// WorkerClaims are the JWT claims issued to workers.
type WorkerClaims struct {
	jwt.RegisteredClaims
	WorkerID string   `json:"worker_id"`
	TenantID string   `json:"tenant_id"`
	Apps     []string `json:"apps"`
}

// WorkerJWTConfig holds the HMAC secret and expected issuer.
type WorkerJWTConfig struct {
	Secret string
	Issuer string
}

const (
	WorkerIDKey       contextKey = "worker_id"
	WorkerTenantIDKey contextKey = "worker_tenant_id"
	WorkerAppsKey     contextKey = "worker_apps"
)

// VerifyWorkerJWT parses and validates a HMAC-SHA256 JWT.
func VerifyWorkerJWT(tokenString string, cfg WorkerJWTConfig) (*WorkerClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &WorkerClaims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(cfg.Secret), nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}
	claims, ok := token.Claims.(*WorkerClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid claims")
	}
	if cfg.Issuer != "" && claims.Issuer != cfg.Issuer {
		return nil, fmt.Errorf("invalid issuer: %s", claims.Issuer)
	}
	return claims, nil
}

// WorkerAuth returns a middleware that verifies Bearer JWT on the request.
func WorkerAuth(cfg WorkerJWTConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := r.Header.Get("Authorization")
			if token == "" {
				token = r.URL.Query().Get("jwt")
			}
			if token == "" {
				http.Error(w, `{"error": "missing worker token"}`, http.StatusUnauthorized)
				return
			}
			token = strings.TrimPrefix(token, "Bearer ")
			claims, err := VerifyWorkerJWT(token, cfg)
			if err != nil {
				http.Error(w, `{"error": "invalid worker token"}`, http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), WorkerIDKey, claims.WorkerID)
			ctx = context.WithValue(ctx, WorkerTenantIDKey, claims.TenantID)
			ctx = context.WithValue(ctx, WorkerAppsKey, claims.Apps)
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
