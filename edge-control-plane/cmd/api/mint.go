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
// `WorkerAuth` middleware can distinguish it from per-worker tokens.
//
// When the WorkerJWTConfig has an ActiveKID and Keys map, the token's
// `kid` header is set so key rotation can proceed without invalidating
// this token mid-lifecycle. The signing key is resolved via
// cfg.ResolveSigningKey().
func mintIngressToken(cfg middleware.WorkerJWTConfig, region string) (string, error) {
	signingKey, err := cfg.ResolveSigningKey()
	if err != nil {
		return "", fmt.Errorf("resolving signing key: %w", err)
	}
	if region == "" {
		region = "global"
	}
	issuer := cfg.Issuer
	if issuer == "" {
		issuer = "edgecloud"
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
		Role:     middleware.RoleIngest,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	if cfg.ActiveKID != "" {
		token.Header["kid"] = cfg.ActiveKID
	}
	signed, err := token.SignedString(signingKey)
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
