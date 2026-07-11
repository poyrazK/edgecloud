// Per-worker HKDF key derivation (issue #430).
//
// Background: before issue #430, /api/internal/worker-secret returned
// the cluster-wide cfg.JWT.Secret to anyone holding a valid bootstrap
// JWT. A compromised worker could exfiltrate the master secret and
// forge JWTs for every other worker in the cluster. The fix replaces
// the symmetric cluster secret with **per-worker HS256 secrets**
// derived deterministically from:
//
//	HKDF-SHA256(
//	    ikm  = cfg.JWT.Secret,                  // master (cluster-wide)
//	    salt = public_key_hex,                  // per-worker
//	    info = "worker-v1|" + workerID + "|" + tenantID + "|" + region,
//	    L    = 32                               // 256 bits, HS256 key size
//	)
//
// The public_key is the worker's Ed25519 public key (hex-encoded, 64
// ASCII chars). The same public key is presented during the
// /worker-bootstrap/enroll handshake where the worker proves
// possession of the matching private key via a challenge signature;
// the CP then persists public_key on the workers row (see migration
// 032). At verify time, the WorkerAuth middleware (commit 4) re-
// derives the secret from the stored public_key and verifies HS256
// against it.
//
// Why HKDF and not a simple HMAC or a per-worker random key:
//   - Pure function of (master, public_key, claims): no DB lookup or
//     per-worker state needed at verify time, beyond the cached
//     public_key. The worker_key_cache middleware is the only state.
//   - Binds the derived secret to the worker's public key (compromise
//     of one worker's signing key does not let the attacker derive
//     other workers' secrets — they would still need each worker's
//     private key to enroll a forged identity).
//   - The "worker-v1|" prefix is a domain-separation tag. If the
//     cluster master secret is ever reused as IKM for a different
//     derivation (e.g. token-mint signing), the resulting bytes will
//     never collide with a worker secret because the info strings
//     differ.
//   - HKDF is FIPS-acceptable; no new dependency on a third-party KDF.
//
// WorkerKID returns the JWT `kid` header value: "wkr_" + the first
// 8 hex chars of sha256(public_key). The same KID is stamped on
// minted per-tenant tokens so WorkerAuth can route them through the
// per-worker verification path.
package signing

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"golang.org/x/crypto/hkdf"
)

// workerHKDFInfoVersion is the domain-separation tag prefix for HKDF
// info. Bump (and add a sister migration that re-keys all derived
// secrets) only if the derivation inputs change — operators do not
// want a silent re-key just because someone reordered the info fields.
const workerHKDFInfoVersion = "worker-v1|"

// workerDerivedKeyLen is the HKDF output length in bytes. 32 bytes is
// the standard HS256 key size (HS256 uses HMAC-SHA-256, which takes a
// key of any length but truncates/hashes to 32 bytes internally).
const workerDerivedKeyLen = 32

// kidPrefixWorker is the namespace marker that tells the CP that a JWT
// `kid` header is a per-worker KID (not a keyring kid from cfg.JWT.Keys).
// WorkerAuth's resolveKey branches on this prefix.
const kidPrefixWorker = "wkr_"

// kidFingerprintHexLen is how many hex chars of the pubkey-hash to
// embed in the KID. 8 hex chars = 32 bits, which is enough entropy to
// make collisions astronomically unlikely while keeping the KID
// human-greppable in logs.
const kidFingerprintHexLen = 8

// DeriveWorkerSecret computes the per-worker HS256 signing secret.
// Inputs:
//   - master:        the cluster-wide HKDF input keying material (today:
//                    cfg.JWT.Secret; tomorrow: a separate
//                    WORKER_DERIVATION_KEY if we want to split
//                    concerns). MUST be at least 32 bytes; callers
//                    should validate at startup, not here.
//   - workerID:      the worker's identity string (e.g. "w_fra_abc")
//   - tenantID:      the tenant the worker is bootstrapping for
//                    (may be "*" for the wildcard case)
//   - region:        the worker's region
//   - publicKeyHex:  the worker's Ed25519 public key, hex-encoded
//                    (64 lowercase ASCII chars)
//
// Returns 32 bytes. The output is deterministic: same inputs always
// produce the same secret, and changing ANY of (workerID, tenantID,
// region, publicKeyHex) produces a completely different secret (HKDF
// is a PRF). This is the property that lets the CP re-derive the
// verification key at JWT-verify time without persisting the secret
// itself.
//
// Errors only if HKDF extraction itself fails (which can only happen
// if the master is empty — empty IKM produces an HKDF extract error).
// Callers should pre-validate master length at startup; this returns
// the error defensively.
func DeriveWorkerSecret(master []byte, workerID, tenantID, region, publicKeyHex string) ([]byte, error) {
	if len(master) == 0 {
		return nil, fmt.Errorf("signing: DeriveWorkerSecret: master key material is empty")
	}
	// Salt binds the derivation to the worker's public key. The CP uses
	// the stored public_key at verify time, so the salt is exactly the
	// same bytes on both sides.
	salt := []byte(publicKeyHex)

	// Info binds the derivation to (worker_id, tenant_id, region). Same
	// on both sides because all three come from the verified JWT
	// claims (which the worker supplied during enrollment, and the CP
	// now trusts because enrollment required a valid Ed25519 signature
	// over the challenge).
	info := []byte(workerHKDFInfoVersion + workerID + "|" + tenantID + "|" + region)

	// hkdf.New(hash, ikm, salt, info) returns a Reader; Read fills the
	// output buffer. HKDF.Extract followed by Expand is exactly the
	// "extract-and-expand" two-step pattern RFC 5869 describes.
	r := hkdf.New(sha256.New, master, salt, info)
	out := make([]byte, workerDerivedKeyLen)
	if _, err := r.Read(out); err != nil {
		return nil, fmt.Errorf("signing: DeriveWorkerSecret: hkdf.Read: %w", err)
	}
	return out, nil
}

// WorkerKID returns the JWT `kid` header value for a given worker
// public key. Format: "wkr_" + first 8 hex chars of sha256(publicKey).
//
// The KID is stable for the lifetime of the keypair — re-deriving
// KID for the same public_key always returns the same string — and
// it changes immediately when the worker re-enrolls with a new
// keypair (because the hash changes). This is the property that lets
// the CP detect re-enrollment and invalidate any cached derived
// secret via WorkerAuth's kid branch.
//
// The 8-hex-char fingerprint is a convenience for log-grepping ("which
// kid is this worker using?") — the CP always has the full pubkey
// stored, so the fingerprint does not need to be collision-resistant.
func WorkerKID(publicKeyHex string) string {
	sum := sha256.Sum256([]byte(publicKeyHex))
	return kidPrefixWorker + hex.EncodeToString(sum[:])[:kidFingerprintHexLen]
}

// IsWorkerKID reports whether a JWT `kid` header is in the per-worker
// namespace. WorkerAuth.resolveKey branches on this: wkr_ kids go
// through the public_key + HKDF path; everything else goes through
// the existing cfg.Keys[cfg.ActiveKID] path (used by mintIngressToken
// and any pre-rotation token).
func IsWorkerKID(kid string) bool {
	return len(kid) > len(kidPrefixWorker) && kid[:len(kidPrefixWorker)] == kidPrefixWorker
}
