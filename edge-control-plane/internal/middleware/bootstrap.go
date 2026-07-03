package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/handler/httperror"
)

// BootstrapAuthConfig holds the pre-shared keys workers use to prove
// their identity during enrollment. Separate from WorkerJWTConfig
// because the bootstrap path is the chicken-and-egg predecessor of
// the JWT path — same server, different proof mechanism.
//
// PR #200 review finding H1: per-tenant PSK binding. The PSKs map is
// keyed by `tenant_id`; a worker bootstrapping for tenant T must use
// the PSK configured under `PSKs[T]`. A compromised PSK for tenant A
// therefore cannot mint a JWT for tenant B (the HMAC verification
// fails because the keyed lookup returns no entry for tenant B).
//
// Empty map disables the endpoint entirely (every request is rejected
// with 503 — the route exists but cannot succeed until configured).
type BootstrapAuthConfig struct {
	// PSKs is the per-tenant pre-shared key map. Tenant IDs follow
	// the same `^t_[a-z0-9_]+$` regex the worker sends in the body's
	// `tenant_id` field. The HMAC computation uses the value
	// `PSKs[tenantID]` as the key.
	PSKs map[string][]byte
}

// PSKFor returns the per-tenant PSK for the given tenant ID, or nil
// if no PSK is configured for that tenant. Callers should treat nil as
// "tenant unknown" and reject the request with the same generic 401
// used for signature mismatches (don't reveal whether the tenant was
// unknown vs. whether the signature was wrong — avoids an oracle).
func (c BootstrapAuthConfig) PSKFor(tenantID string) []byte {
	if c.PSKs == nil {
		return nil
	}
	return c.PSKs[tenantID]
}

const (
	// BootstrapWorkerIDKey / BootstrapRegionKey / BootstrapTenantIDKey
	// carry the validated identity from PSKAuth into the handler via
	// context. Named distinctly from WorkerIDKey / WorkerRegionKey so a
	// handler that wants WorkerAuth claims (post-bootstrap) doesn't
	// accidentally read a bootstrap-time identity that hasn't been
	// promoted to a full JWT yet.
	BootstrapWorkerIDKey contextKey = "bootstrap_worker_id"
	BootstrapRegionKey   contextKey = "bootstrap_region"
	BootstrapTenantIDKey contextKey = "bootstrap_tenant_id"
)

// Identity character-set rules. Mirrored on the worker side in
// `config.rs::MIN_*_BYTES` (length) and `bootstrap::sign_with_psk` (no
// colons allowed because they're the canonical separator). Used by
// `validateIdentity` to reject forged or malformed identity strings
// before signature verification so the JWT's `worker_id` / `region` /
// `tenant_id` claims can be trusted by downstream consumers.
//
// Character-set (finding A3):
//   - worker_id:  ^w_[a-z0-9_]+$        length 1..=64
//   - region:     ^[a-z]{3,16}$
//   - tenant_id:  ^t_[a-z0-9_]+$        length 1..=64
var (
	workerIDPattern = regexp.MustCompile(`^w_[a-z0-9_]{1,64}$`)
	regionPattern   = regexp.MustCompile(`^[a-z]{3,16}$`)
	tenantIDPattern = regexp.MustCompile(`^t_[a-z0-9_]{1,64}$`)
)

// validateIdentity checks that the supplied identity strings match the
// documented format. Returns nil if all three are well-formed; an
// error otherwise. The caller returns 400 (NOT 401) on failure — the
// signature may be valid; the inputs just don't match the format.
//
// A colon or uppercase letter in `worker_id` would let an attacker
// craft two different workers whose HMAC payloads collide (the
// canonical form is `{worker_id}:{region}:{tenant_id}` with colons as
// separators). Without this check, a downstream JWT consumer that
// trusts the `worker_id` claim would accept a forged identity.
func validateIdentity(workerID, region, tenantID string) error {
	if !workerIDPattern.MatchString(workerID) {
		return errors.New("worker_id must match ^w_[a-z0-9_]+$ (length 1..=64)")
	}
	if !regionPattern.MatchString(region) {
		return errors.New("region must match ^[a-z]{3,16}$")
	}
	if !tenantIDPattern.MatchString(tenantID) {
		return errors.New("tenant_id must match ^t_[a-z0-9_]+$ (length 1..=64)")
	}
	return nil
}

// VerifyPSKSignature checks that
// hex(HMAC-SHA256(psk, "{worker_id}:{region}:{tenant_id}")) matches the
// supplied `signatureHex`. Returns nil on success and an error
// describing the failure otherwise.
//
// `signatureHex` must be a 64-char lowercase hex digest (the same
// shape the worker produces via `bootstrap::sign_with_psk`). Mismatched
// length, non-hex chars, or wrong digest all return errors without
// distinguishing which condition failed — an attacker probing the
// endpoint shouldn't learn whether their guess had a valid format.
//
// `hmac.Equal` (constant-time) is used for the final byte comparison
// so an attacker can't time-side-channel the signature. The early
// returns on length / hex errors are not constant-time, but they leak
// at most "your input shape was wrong" which is already public
// information — the worker always sends a 64-char hex string.
//
// **Tenant binding (finding A1):** the canonical payload includes
// `tenantID` so an attacker who captures a valid signature for tenant
// A cannot replay it to mint a JWT for tenant B.
func VerifyPSKSignature(psk []byte, workerID, region, tenantID, signatureHex string) error {
	if len(signatureHex) != 64 {
		return errors.New("signature must be 64-char hex")
	}
	if _, err := hex.DecodeString(signatureHex); err != nil {
		return errors.New("signature must be valid hex")
	}
	mac := hmac.New(sha256.New, psk)
	mac.Write([]byte(workerID))
	mac.Write([]byte(":"))
	mac.Write([]byte(region))
	mac.Write([]byte(":"))
	mac.Write([]byte(tenantID))
	expected := mac.Sum(nil)
	got, err := hex.DecodeString(signatureHex)
	if err != nil {
		// Already checked above, but be defensive — hex.DecodeString
		// on a 64-char lowercase hex string can't fail here.
		return errors.New("signature decode failed")
	}
	if !hmac.Equal(expected, got) {
		return errors.New("signature mismatch")
	}
	return nil
}

// PSKAuth returns a middleware that validates the X-Bootstrap-Signature
// header against the configured PSK. On success, worker_id, region,
// and tenant_id are placed into the request context for the handler
// to read.
//
// **Request flow:** the middleware reads the body BEFORE signature
// verification so the canonical HMAC payload can include the body's
// `tenant_id` (finding A1 — without binding tenant_id into the signed
// payload, an attacker who captured a valid signature for tenant A
// could replay it against tenant B). The body is read into memory
// (small JSON, ~80 bytes); it's restored to `r.Body` so the handler
// can re-read it.
//
// **Route precedence:** this middleware is intended for the OUTER mux
// (not inside the WorkerAuth subtree). Go 1.22+'s ServeMux matches
// the most specific pattern first, so registering
// `POST /api/internal/auth/token` on the outer mux wins over
// `/api/internal/` despite living on the same mux. See cmd/api/main.go
// for the exact wiring.
//
// On failure, returns:
//   - 503 (PSK not configured on the server — operator error)
//   - 401 (invalid signature or mismatched identity — generic message
//     to avoid leaking which condition failed)
//   - 400 (missing/malformed headers, identity format violation, or
//     missing/malformed body)
func PSKAuth(cfg BootstrapAuthConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if len(cfg.PSKs) == 0 {
				// 503 (not 401) because the server itself is
				// misconfigured — not the client's fault. Operators
				// see this in their own logs and know to set
				// BOOTSTRAP_PSKS.
				httperror.WriteCtx(w, r, http.StatusServiceUnavailable, "bootstrap disabled: BOOTSTRAP_PSKS not configured")
				return
			}
			workerID := strings.TrimSpace(r.Header.Get("X-Worker-Id"))
			region := strings.TrimSpace(r.Header.Get("X-Worker-Region"))
			signature := r.Header.Get("X-Bootstrap-Signature")
			if workerID == "" || region == "" || signature == "" {
				httperror.BadRequestCtx(w, r, "missing X-Worker-Id, X-Worker-Region, or X-Bootstrap-Signature header")
				return
			}
			// Read the body BEFORE signature verification so the
			// canonical payload can include `tenant_id` (finding A1).
			// The body is small (~80 bytes); we restore it for the
			// downstream handler.
			var body struct {
				WorkerID string `json:"worker_id"`
				Region   string `json:"region"`
				TenantID string `json:"tenant_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				httperror.BadRequestCtx(w, r, "invalid JSON body")
				return
			}
			if body.WorkerID == "" || body.Region == "" || body.TenantID == "" {
				httperror.BadRequestCtx(w, r, "worker_id, region, and tenant_id are required")
				return
			}
			// Identity character-set validation (finding A3) BEFORE
			// signature verification so a malformed identity gets a
			// clear 400 (not the generic 401). The signature is
			// computed over the body's tenant_id, so format-checking
			// the body tenant_id is what matters here.
			if err := validateIdentity(workerID, region, body.TenantID); err != nil {
				httperror.BadRequestCtx(w, r, err.Error())
				return
			}
			// H1: per-tenant PSK binding. Look up the PSK for this
			// tenant before signature verification. An empty result
			// means "this tenant has no PSK configured"; we return
			// the same generic 401 as a signature mismatch so an
			// attacker can't enumerate which tenants have PSKs
			// configured.
			psk := cfg.PSKFor(body.TenantID)
			if psk == nil {
				httperror.UnauthorizedCtx(w, r, "invalid signature")
				return
			}
			if err := VerifyPSKSignature(psk, workerID, region, body.TenantID, signature); err != nil {
				httperror.UnauthorizedCtx(w, r, "invalid signature")
				return
			}
			// Body / header agreement is implicit — if the signature
			// verified with `body.TenantID`, the body's tenant_id was
			// the signed value. Headers carry worker_id + region (the
			// signed values); if the body disagreed, the signature
			// wouldn't have matched. Defense-in-depth: still assert
			// body matches headers to catch a future refactor that
			// drops a field from the signed payload.
			if body.WorkerID != workerID || body.Region != region {
				httperror.BadRequestCtx(w, r, "body worker_id or region does not match signed headers")
				return
			}
			ctx := context.WithValue(r.Context(), BootstrapWorkerIDKey, workerID)
			ctx = context.WithValue(ctx, BootstrapRegionKey, region)
			ctx = context.WithValue(ctx, BootstrapTenantIDKey, body.TenantID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GetBootstrapWorkerID extracts the bootstrap-time worker ID from
// context. Returns "" if the request wasn't PSKAuth-authenticated.
func GetBootstrapWorkerID(ctx context.Context) string {
	if id, ok := ctx.Value(BootstrapWorkerIDKey).(string); ok {
		return id
	}
	return ""
}

// GetBootstrapRegion extracts the bootstrap-time region from context.
func GetBootstrapRegion(ctx context.Context) string {
	if r, ok := ctx.Value(BootstrapRegionKey).(string); ok {
		return r
	}
	return ""
}

// GetBootstrapTenantID extracts the bootstrap-time tenant ID from
// context. Returns "" if the request wasn't PSKAuth-authenticated.
func GetBootstrapTenantID(ctx context.Context) string {
	if t, ok := ctx.Value(BootstrapTenantIDKey).(string); ok {
		return t
	}
	return ""
}
