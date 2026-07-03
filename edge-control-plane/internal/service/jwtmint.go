package service

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/workerclaims"
	"github.com/golang-jwt/jwt/v5"
)

// WorkerJWTMinter mints HS256 JWTs for workers. Single source of
// truth for the worker-token wire format — every call site that
// needs a token (the bootstrap handler, future programmatic flows,
// integration test helpers that want to skip WorkerAuth) goes
// through `Mint` rather than constructing the JWT inline.
//
// The minter is constructed once at startup from the JWT config
// (secret + issuer + audience + TTL) and shared by every handler.
// There's no mutable state to speak of, so concurrent Mint calls
// are safe and cheap.
type WorkerJWTMinter struct {
	secret   []byte
	issuer   string
	audience string
	ttl      time.Duration
}

// NewWorkerJWTMinter constructs a minter from the configured JWT
// settings. Pulls the secret / issuer / audience / TTL out of
// `config.JWTConfig` (the same fields that drive
// `middleware.WorkerAuth`) so a token minted here is identical to
// one the operator could mint by hand with `JWT_SECRET`. The TTL
// is converted from hours to a Duration.
func NewWorkerJWTMinter(cfg config.JWTConfig) *WorkerJWTMinter {
	ttl := time.Duration(cfg.TTL) * time.Hour
	if ttl <= 0 {
		// Defensive: config.go already defaults TTL to 24 when zero,
		// but a future config schema change could break that default.
		// Falling back to 24h here is safer than a zero-duration token
		// (which would 401 immediately on every WorkerAuth call).
		ttl = 24 * time.Hour
	}
	audience := cfg.Audience
	if audience == "" {
		// PR #200 review H8: default the audience to "edge-internal" so
		// deployments that haven't set JWT_AUDIENCE still get the
		// defense-in-depth gate. Matches the default the worker-side
		// signer uses (edge-worker/src/config.rs).
		audience = "edge-internal"
	}
	return &WorkerJWTMinter{
		secret:   []byte(cfg.Secret),
		issuer:   cfg.Issuer,
		audience: audience,
		ttl:      ttl,
	}
}

// Mint produces a worker JWT carrying the supplied claims. The
// returned `time.Time` is the token's `exp` — the worker uses it to
// decide when to refresh (5 minutes before exp per the worker's
// REFRESH_LEAD constant). Returning the exp lets callers persist it
// without re-parsing the token.
//
// `tenantID` is the tenant claim the worker is authorized for.
// Currently the worker is single-tenant (whitepaper §9.3 calls for
// tenant-agnostic workers as a follow-up); when that lands the
// minter will accept a slice here and mint a per-tenant token.
//
// `apps` is informational on the bootstrap token (empty) — the
// worker fills it in on heartbeats as it learns which apps it's
// hosting. Kept in the signature so a future per-app JWT doesn't
// need a new constructor.
func (m *WorkerJWTMinter) Mint(workerID, tenantID, region string) (string, time.Time, error) {
	now := time.Now()
	exp := now.Add(m.ttl)
	claims := &workerclaims.WorkerClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    m.issuer,
			Audience:  jwt.ClaimStrings{m.audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
			ID:        newJTI(),
		},
		WorkerID: workerID,
		TenantID: tenantID,
		Region:   region,
		Apps:     []string{},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(m.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign worker JWT: %w", err)
	}
	return signed, exp, nil
}

// newJTI returns a random per-token identifier. Replay protection +
// guarantees that each Mint produces a unique token even within the
// same second. Uses crypto/rand via the same package the
// golang-jwt library recommends.
func newJTI() string {
	// 128 bits is the same size golang-jwt uses internally for its
	// default claim IDs — collision-free for any plausible call
	// volume and short enough to stay readable in logs.
	var b [16]byte
	// crypto/rand.Read never returns an error in the stdlib on any
	// supported platform; if it does, fall back to time-based entropy
	// so the token still gets a (weaker but functional) jti rather
	// than failing Mint.
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("jti-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
