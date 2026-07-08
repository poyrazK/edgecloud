// Multi-key signing keyring for issue #307 follow-up PR1.
//
// The single-key `Signer` (signer.go) is the per-key cryptographic
// primitive: it holds one Ed25519 private key, signs and verifies.
// `Keyring` is the per-process collection of `Signer`s indexed by
// their operator-chosen key id (`kid`). At sign time, the active kid
// is selected from the env var `EDGE_SIGNING_KEY_ID`; at verify time
// (CP-side, for tests and tooling only — workers verify with their
// own keyring on the other side of the wire), the kid is taken from
// the deployment row.
//
// File format (`EDGE_SIGNING_KEYRING_PATH`):
//
//	# comments and blank lines are ignored
//	k1 = 9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60
//	k2 = 5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d
//
// Each line is `<kid> = <32-byte seed in lowercase hex>` (64 chars).
// This mirrors the worker's pubkey keyring file format
// (`edge-worker/src/verifier.rs::from_inline`) so operators only
// learn one shape. 64-byte (full RFC 8032 §5.1.2) private keys are
// also accepted (the first 32 bytes are the seed, matching what
// `ed25519.NewKeyFromSeed` expects — see parsePrivateKey in
// signer.go).
//
// Env-var resolution order (LoadFromEnv):
//
//  1. EDGE_SIGNING_KEYRING_PATH — path to a keyring file
//  2. EDGE_SIGNING_KEYRING      — inline keyring payload
//  3. EDGE_SIGNING_KEY_PATH     — path to a single 32-byte or 64-byte key
//     (legacy single-key shim; wraps in a 1-entry keyring whose
//     kid is "default"; logs a deprecation warning)
//  4. EDGE_SIGNING_KEY          — inline single key (legacy)
//
// If none are set, LoadFromEnv returns ErrInvalidKey — fail-fast
// at startup, same shape as before.
//
// The "active kid" for signing is `EDGE_SIGNING_KEY_ID`. If unset,
// the keyring must contain a `default` entry. If set but not
// present in the keyring, startup fails (no silent fallback to a
// different key — the operator asked for a specific kid, and using
// a different one would silently invalidate the rotation contract).
package signing

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

// DefaultKeyID is the implicit fallback kid. Artifacts that arrive
// without an explicit `signing_key_id` (legacy deployments, or a
// CP started without `EDGE_SIGNING_KEY_ID`) are signed by the
// keyring entry whose kid is this string. The same string is used
// on the worker side as the keyring's implicit fallback
// (`verifier::DEFAULT_KID`).
const DefaultKeyID = "default"

// Keyring is the control-plane-side multi-key signing collection.
// One instance per CP process; constructed at startup from
// `LoadFromEnv`. Read-only after construction (the active-kid
// pointer is fixed); hot-reload is a future-PR concern.
//
// The map is `kid → *Signer`. Each `Signer` is the existing
// per-key primitive (signer.go). Map access is guarded by Go's
// concurrent-map-read-safe semantics; writes only happen at
// construction so no external lock is needed.
type Keyring struct {
	keys map[string]*Signer
	// active is the kid Sign() uses. Cached at construction so the
	// hot path doesn't re-read the env var per call.
	active string
}

// LoadFromEnv constructs a Keyring from one of the EDGE_SIGNING_*
// env vars described in the package doc. Returns ErrInvalidKey (or
// an os error) if no key material is set.
//
// Deprecation: the legacy `EDGE_SIGNING_KEY[_PATH]` env vars are
// accepted as a single-key shim (one entry, kid = "default") for
// one release. A deprecation warning is logged so operators notice
// they should migrate. Drop in a follow-up PR after one release.
func LoadFromEnv() (*Keyring, error) {
	active := os.Getenv("EDGE_SIGNING_KEY_ID")

	if path := os.Getenv("EDGE_SIGNING_KEYRING_PATH"); path != "" {
		return LoadKeyringFromFile(path, active)
	}
	if inline := os.Getenv("EDGE_SIGNING_KEYRING"); inline != "" {
		return LoadKeyringFromInline(inline, active)
	}

	// Legacy single-key fallback (deprecated).
	if path := os.Getenv("EDGE_SIGNING_KEY_PATH"); path != "" {
		log.Printf("signing: EDGE_SIGNING_KEY_PATH is deprecated; use EDGE_SIGNING_KEYRING_PATH (issue #307 PR1)")
		return loadLegacySingleKeyFromFile(path, active)
	}
	if inline := os.Getenv("EDGE_SIGNING_KEY"); inline != "" {
		log.Printf("signing: EDGE_SIGNING_KEY is deprecated; use EDGE_SIGNING_KEYRING (issue #307 PR1)")
		return loadLegacySingleKeyFromInline(inline, active)
	}

	return nil, fmt.Errorf("%w: set EDGE_SIGNING_KEYRING_PATH or EDGE_SIGNING_KEYRING", ErrInvalidKey)
}

// LoadKeyringFromFile reads a keyring file in the format described
// in the package doc. `active` is the kid to use for Sign(); pass
// "" to default to DefaultKeyID. Returns an error if the file is
// missing, empty, malformed, or `active` doesn't resolve to a kid
// in the loaded keyring.
func LoadKeyringFromFile(path, active string) (*Keyring, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading keyring %q: %w", path, err)
	}
	return LoadKeyringFromInline(string(data), active)
}

// LoadKeyringFromInline parses an inline keyring payload (same
// format as the file). Used both by LoadKeyringFromFile and by
// LoadFromEnv when `EDGE_SIGNING_KEYRING` is set without a backing
// file. Empty lines and `# comment` lines are skipped. Each
// non-comment line must be `<kid> = <hex>`. Duplicate kids, blank
// kids, or malformed lines produce errors.
func LoadKeyringFromInline(raw, active string) (*Keyring, error) {
	keys, err := parseKeyringLines(raw)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: keyring is empty; expected at least one `<kid> = <hex>` line", ErrInvalidKey)
	}

	if active == "" {
		active = DefaultKeyID
	}
	if _, ok := keys[active]; !ok {
		return nil, fmt.Errorf("%w: active kid %q not present in keyring (loaded kids: %s)",
			ErrInvalidKey, active, sortedKids(keys))
	}
	return &Keyring{keys: keys, active: active}, nil
}

// parseKeyringLines is the shared parser used by file + inline
// paths. Returns a `kid → *Signer` map keyed by insertion order is
// NOT preserved (Go map); callers that need a stable iteration
// order should sort via sortedKids.
func parseKeyringLines(raw string) (map[string]*Signer, error) {
	out := map[string]*Signer{}
	sc := bufio.NewScanner(strings.NewReader(raw))
	lineno := 0
	for sc.Scan() {
		lineno++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kid, hexStr, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("keyring line %d: expected `<kid> = <hex>`, got %q", lineno, line)
		}
		kid = strings.TrimSpace(kid)
		hexStr = strings.TrimSpace(hexStr)
		if kid == "" {
			return nil, fmt.Errorf("keyring line %d: kid must be non-empty", lineno)
		}
		if _, dup := out[kid]; dup {
			return nil, fmt.Errorf("keyring line %d: duplicate kid %q", lineno, kid)
		}
		// parsePrivateKey accepts 32 / 64 raw bytes or 64 / 128 hex
		// chars (signer.go). We feed it the trimmed line so any
		// surrounding whitespace from a sloppy paste is removed.
		priv, err := parsePrivateKey([]byte(hexStr))
		if err != nil {
			return nil, fmt.Errorf("keyring line %d (kid=%q): %w", lineno, kid, err)
		}
		out[kid] = newSigner(priv, kid)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading keyring: %w", err)
	}
	return out, nil
}

// KeyringFromSigner wraps an existing single-key Signer in a
// 1-entry Keyring. Used by the legacy env-var fallback path in
// `app.loadKeyring` so the legacy code path can stay call-compatible
// with the new `Keyring` field type on the service constructors.
// `kid` overrides the wrapped Signer's kid; pass "" to inherit.
func KeyringFromSigner(s *Signer, kid string) *Keyring {
	if kid == "" {
		kid = s.keyID
	}
	if kid == "" {
		kid = DefaultKeyID
	}
	return &Keyring{
		keys:   map[string]*Signer{kid: s},
		active: kid,
	}
}

// loadLegacySingleKeyFromFile is the deprecated one-key shim: reads
// a single key from `path`, wraps it in a 1-entry Keyring with kid
// = "default". If `active` is non-empty and != DefaultKeyID, returns
// an error (the legacy file cannot supply a non-default kid).
func loadLegacySingleKeyFromFile(path, active string) (*Keyring, error) {
	if active != "" && active != DefaultKeyID {
		return nil, fmt.Errorf("%w: legacy single-key file cannot satisfy EDGE_SIGNING_KEY_ID=%q; use EDGE_SIGNING_KEYRING_PATH",
			ErrInvalidKey, active)
	}
	s, err := LoadFromFile(path, DefaultKeyID)
	if err != nil {
		return nil, err
	}
	return &Keyring{keys: map[string]*Signer{DefaultKeyID: s}, active: DefaultKeyID}, nil
}

// loadLegacySingleKeyFromInline mirrors loadLegacySingleKeyFromFile
// for the inline form (EDGE_SIGNING_KEY).
func loadLegacySingleKeyFromInline(inline, active string) (*Keyring, error) {
	if active != "" && active != DefaultKeyID {
		return nil, fmt.Errorf("%w: legacy single-key cannot satisfy EDGE_SIGNING_KEY_ID=%q; use EDGE_SIGNING_KEYRING",
			ErrInvalidKey, active)
	}
	s, err := LoadFromRaw([]byte(inline), DefaultKeyID)
	if err != nil {
		return nil, err
	}
	return &Keyring{keys: map[string]*Signer{DefaultKeyID: s}, active: DefaultKeyID}, nil
}

// Sign signs `(sha256(artifact_bytes) || deployment_id)` with the
// active key and returns `(base64url-no-pad signature, kid, error)`.
// The deployment_id is stamped into the signed payload so the same
// hash on a different deployment_id produces a different signature
// (issue #307: replay protection).
//
// The returned kid is the key used (== `Keyring.ActiveKeyID()`).
// Callers should stamp it onto the deployment row alongside the
// signature so workers can pick the right pubkey from their
// keyring at verify time.
func (k *Keyring) Sign(hashHex, deploymentID string) (sig string, kid string, err error) {
	s, ok := k.keys[k.active]
	if !ok {
		// Defensive: LoadFromEnv / LoadKeyringFromInline reject an
		// unresolvable active kid, so this branch should be
		// unreachable in practice. Treat as ErrInvalidKey so the
		// error type stays consistent with the rest of the package.
		return "", "", fmt.Errorf("%w: active kid %q not in keyring", ErrInvalidKey, k.active)
	}
	sig, err = s.Sign(hashHex, deploymentID)
	if err != nil {
		return "", "", err
	}
	return sig, k.active, nil
}

// SignBytes signs an arbitrary payload with the active key and
// returns `(base64url-no-pad signature, kid, error)`. Same shape
// as Sign but without the deployment-id binding — used by issue
// #307 PR2 to sign in-toto Statement envelopes.
//
// The active kid is returned so the caller can stamp it into the
// DSSE wrapper's `keyid` field. Like Sign, the kid is always
// Keyring.ActiveKeyID() — there's no per-call kid override because
// the active-kid pointer is the keyring's single source of truth
// for "what is currently producing signatures".
func (k *Keyring) SignBytes(msg []byte) (sig string, kid string, err error) {
	s, ok := k.keys[k.active]
	if !ok {
		return "", "", fmt.Errorf("%w: active kid %q not in keyring", ErrInvalidKey, k.active)
	}
	sig, err = s.SignBytes(msg)
	if err != nil {
		return "", "", err
	}
	return sig, k.active, nil
}

// VerifyBytes verifies a signature produced by SignBytes. Resolves
// the key by `kid` (same way Verify does); empty kid falls back to
// DefaultKeyID so a verifier that doesn't care about kid rotation
// can still verify a default-kid envelope.
func (k *Keyring) VerifyBytes(msg []byte, signatureB64, kid string) (bool, error) {
	if kid == "" {
		kid = DefaultKeyID
	}
	s, ok := k.keys[kid]
	if !ok {
		return false, fmt.Errorf("%w: kid %q not in keyring (loaded: %s)",
			ErrInvalidKey, kid, sortedKids(k.keys))
	}
	return s.VerifyBytes(msg, signatureB64)
}

// Verify is the CP-side verification helper used by tests and
// operator tooling. Resolves the key by `kid` (the same way the
// worker does on the other side of the wire); empty `kid` falls
// back to DefaultKeyID so a legacy deployment row with an empty
// `signing_key_id` column can be verified locally.
//
// Returns `ErrInvalidKey` if `kid` does not resolve.
func (k *Keyring) Verify(hashHex, deploymentID, signatureB64, kid string) (bool, error) {
	if kid == "" {
		kid = DefaultKeyID
	}
	s, ok := k.keys[kid]
	if !ok {
		return false, fmt.Errorf("%w: kid %q not in keyring (loaded: %s)",
			ErrInvalidKey, kid, sortedKids(k.keys))
	}
	return s.Verify(hashHex, deploymentID, signatureB64)
}

// ActiveKeyID returns the kid Sign() will use. Empty only if the
// Keyring was constructed via a path that bypassed the active-kid
// resolution (none of the public constructors do).
func (k *Keyring) ActiveKeyID() string { return k.active }

// Kids returns a sorted list of all kids in the keyring. Useful
// for diagnostics and for tests.
func (k *Keyring) Kids() []string { return sortedKids(k.keys) }

// PublicKeyHex returns the 64-lowercase-hex form of the public key
// for `kid` (matches the worker-side keyring format). Returns
// ErrInvalidKey if `kid` is not in the keyring.
func (k *Keyring) PublicKeyHex(kid string) (string, error) {
	if kid == "" {
		kid = DefaultKeyID
	}
	s, ok := k.keys[kid]
	if !ok {
		return "", fmt.Errorf("%w: kid %q not in keyring", ErrInvalidKey, kid)
	}
	return s.PublicKeyHex(), nil
}

// sortedKids returns the kids of `m` in sorted order. Stable for
// tests and log lines; not part of the hot path.
func sortedKids(m map[string]*Signer) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// In-place sort; cheap for small maps.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
