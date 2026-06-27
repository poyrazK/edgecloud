package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
	"github.com/golang-jwt/jwt/v5"
)

// mintIngressToken builds a long-lived (1y TTL) HMAC-SHA256 JWT for the
// ingress binary. The token carries `role: "ingest"` so the
// `WorkerAuth` middleware can distinguish it from per-worker tokens
// (which carry `role: "worker"`, or no `role` for backward compat).
//
// The token is NOT a per-process secret. It's the same on every restart
// unless the operator rotates `JWT_SECRET`. Operators SHOULD rotate
// JWT_SECRET periodically; rotating invalidates both the new token
// written here AND every existing worker JWT, so it requires a
// coordinated control-plane + worker + ingress redeploy. This is
// intentional — the ingress and workers are managed together.
func mintIngressToken(secret, issuer, region string) (string, error) {
	if secret == "" {
		return "", fmt.Errorf("JWT secret is empty")
	}
	if region == "" {
		region = "global"
	}
	now := time.Now()
	subject := "ingress-" + region
	claims := middleware.WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   subject,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(365 * 24 * time.Hour)),
			NotBefore: jwt.NewNumericDate(now.Add(-1 * time.Minute)),
		},
		WorkerID: subject,
		// TenantID is intentionally empty — the ingress is a global
		// service, not bound to a single tenant. Internal endpoints
		// (`ListDomains`, `TlsAllowed`) are tenant-agnostic.
		Role: middleware.RoleIngest,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("signing ingress token: %w", err)
	}
	return signed, nil
}

// writeIngressTokenFile writes the long-lived ingress token to a 0600
// file under `dir` and returns the path. The filename is
// `ingest-token.<region>.<unix-ts>` so a restart produces a NEW file
// (operators can see the rotation timestamp in the file name and
// `ls -l`) while the previous file remains on disk for recovery.
//
// We deliberately do NOT overwrite the previous file — overwriting
// would mean a half-copied file could end up as the active
// INGRESS_SERVICE_TOKEN on a misconfigured operator host.
//
// The file is created with 0600 perms; if the dir does not exist
// (e.g. the operator hasn't run `init`), we make it best-effort.
// The token file path is logged (not the token) so it lands in
// the operator's log aggregator without leaking the secret.
func writeIngressTokenFile(dir, region, token string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("token dir is empty")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", fmt.Errorf("creating token dir %s: %w", dir, err)
	}
	name := fmt.Sprintf("ingest-token.%s.%d", region, time.Now().Unix())
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(token), 0600); err != nil {
		return "", fmt.Errorf("writing token file %s: %w", path, err)
	}
	return path, nil
}
