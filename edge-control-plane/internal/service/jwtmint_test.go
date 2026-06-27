package service

import (
	"testing"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/workerclaims"
	"github.com/golang-jwt/jwt/v5"
)

func newTestMinter(t *testing.T) *WorkerJWTMinter {
	t.Helper()
	cfg := config.JWTConfig{
		Secret: "test-secret-32-bytes-minimum-please",
		Issuer: "edgecloud",
		TTL:    24,
	}
	return NewWorkerJWTMinter(cfg)
}

func TestMint_ProducesParseableToken(t *testing.T) {
	m := newTestMinter(t)
	tokenStr, exp, err := m.Mint("w_fra_abc", "t_tenant1", "fra")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if tokenStr == "" {
		t.Fatal("empty token")
	}
	if exp.IsZero() {
		t.Fatal("exp is zero")
	}
	// Round-trip: parse the token back with the same secret and
	// confirm it decodes to the expected claims.
	claims := &workerclaims.WorkerClaims{}
	parsed, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			t.Fatalf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte("test-secret-32-bytes-minimum-please"), nil
	})
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	if !parsed.Valid {
		t.Fatal("token not valid")
	}
}

func TestMint_CarriesExpectedClaims(t *testing.T) {
	m := newTestMinter(t)
	tokenStr, _, err := m.Mint("w_fra_abc", "t_tenant1", "fra")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	claims := &workerclaims.WorkerClaims{}
	_, err = jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		return []byte("test-secret-32-bytes-minimum-please"), nil
	})
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	if claims.WorkerID != "w_fra_abc" {
		t.Errorf("WorkerID = %q, want w_fra_abc", claims.WorkerID)
	}
	if claims.TenantID != "t_tenant1" {
		t.Errorf("TenantID = %q, want t_tenant1", claims.TenantID)
	}
	if claims.Region != "fra" {
		t.Errorf("Region = %q, want fra", claims.Region)
	}
	if claims.Issuer != "edgecloud" {
		t.Errorf("Issuer = %q, want edgecloud", claims.Issuer)
	}
	if claims.ID == "" {
		t.Error("jti must be non-empty")
	}
}

func TestMint_SetsExpToNowPlusTTL(t *testing.T) {
	m := newTestMinter(t)
	before := time.Now()
	tokenStr, exp, err := m.Mint("w_fra_abc", "t_tenant1", "fra")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	after := time.Now()
	// exp should be 24h from the Mint call, within a small skew.
	wantMin := before.Add(24 * time.Hour).Add(-time.Second)
	wantMax := after.Add(24 * time.Hour).Add(time.Second)
	if exp.Before(wantMin) || exp.After(wantMax) {
		t.Errorf("exp = %v, want in [%v, %v]", exp, wantMin, wantMax)
	}
	// And the parsed exp matches.
	claims := &workerclaims.WorkerClaims{}
	_, err = jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		return []byte("test-secret-32-bytes-minimum-please"), nil
	})
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	if claims.ExpiresAt == nil {
		t.Fatal("ExpiresAt is nil")
	}
	// 1-second tolerance for sub-second clock skew between the two
	// time.Now() calls.
	delta := claims.ExpiresAt.Time.Sub(exp)
	if delta < -time.Second || delta > time.Second {
		t.Errorf("parsed exp mismatch: %v vs %v (delta %v)", claims.ExpiresAt.Time, exp, delta)
	}
}

func TestMint_HonoursConfiguredTTL(t *testing.T) {
	cfg := config.JWTConfig{
		Secret: "test-secret-32-bytes-minimum-please",
		Issuer: "edgecloud",
		TTL:    1, // 1 hour
	}
	m := NewWorkerJWTMinter(cfg)
	before := time.Now()
	_, exp, err := m.Mint("w", "t", "r")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	wantMin := before.Add(time.Hour).Add(-time.Second)
	if exp.Before(wantMin) {
		t.Errorf("exp = %v, want >= %v (1h TTL)", exp, wantMin)
	}
}

func TestMint_HonoursConfiguredIssuer(t *testing.T) {
	cfg := config.JWTConfig{
		Secret: "test-secret-32-bytes-minimum-please",
		Issuer: "custom-issuer",
		TTL:    24,
	}
	m := NewWorkerJWTMinter(cfg)
	tokenStr, _, err := m.Mint("w", "t", "r")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	claims := &workerclaims.WorkerClaims{}
	_, err = jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		return []byte("test-secret-32-bytes-minimum-please"), nil
	}, jwt.WithIssuer("custom-issuer"))
	if err != nil {
		t.Errorf("token should validate against custom-issuer, got: %v", err)
	}
}

func TestMint_FallsBackTo24hOnZeroTTL(t *testing.T) {
	cfg := config.JWTConfig{
		Secret: "test-secret-32-bytes-minimum-please",
		Issuer: "edgecloud",
		TTL:    0, // unset — defensive fallback
	}
	m := NewWorkerJWTMinter(cfg)
	before := time.Now()
	_, exp, err := m.Mint("w", "t", "r")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	// 24h default with 1s tolerance.
	wantMin := before.Add(24 * time.Hour).Add(-time.Second)
	if exp.Before(wantMin) {
		t.Errorf("exp = %v, want >= %v (24h default)", exp, wantMin)
	}
}
