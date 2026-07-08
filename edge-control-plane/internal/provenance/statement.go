// Package provenance produces and verifies SLSA Level 1 provenance
// attestations for edgecloud deployments (issue #307 PR2).
//
// The output is an in-toto Statement v0.1 envelope (see
// https://github.com/in-toto/docs/blob/master/in-toto-spec.md) with
// a SLSA provenance v1 predicate, signed via DSSE-style wrapping
// and persisted on the `deployments.build_attestation JSONB` column.
//
// Wire shape (one-shot, returned to the CLI in the deploy response
// and stored verbatim in the DB):
//
//	{
//	  "payloadType": "application/vnd.in-toto+json",
//	  "payload":    "<base64url-no-pad of canonical-JSON Statement>",
//	  "signatures": [{"keyid": "<kid>", "sig": "<base64url-no-pad>"}]
//	}
//
// Verifiers reconstruct the Statement by base64url-decoding
// `payload` and re-canonicalizing it the same way the signer did
// (Canonicalize), then verify the signature with the public key
// looked up by `keyid`. The same canonicalization function is
// used on both sides, so a signature is portable across processes.
//
// No new top-level deps: this package is hand-rolled on purpose.
// Reference impls (`github.com/in-toto/in-toto-golang`) pull DSSE,
// protobuf, and ~30 transitive deps for what's a ~150-LOC envelope
// with stable shape. The Statement v0.1 schema is small enough
// (https://github.com/in-toto/attestation/blob/main/spec/v0.1/field_types.md)
// that hand-rolling keeps the dependency surface flat.
package provenance

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// In-toto Statement v0.1 type URI. Pinned as a const so a typo
// doesn't silently produce a non-conforming envelope.
const (
	StatementTypeURI        = "https://in-toto.io/Statement/v0.1"
	PredicateSLSAProvenance = "https://slsa.dev/provenance/v1"
	BuildTypeURI            = "https://edgecloud.dev/provenance/v1"
	BuilderID               = "https://github.com/poyrazK/edgecloud"
	PayloadTypeInTotoJSON   = "application/vnd.in-toto+json"
)

// maxMaterials caps the per-file material entries in the
// `predicate.materials` array. Tree-mode migrations can touch
// thousands of files; cap protects the persisted JSONB size and
// keeps the on-wire envelope under a few KiB. The full list is
// always computable by re-walking the source, so a truncated
// envelope is still self-consistent — we just set
// `materials_truncated: true` in `predicate.metadata` to flag it.
const maxMaterials = 1000

// Digest is a {algorithm: hex} pair as it appears in in-toto
// subject and material entries. SHA-256 only for now; the SLSA
// spec allows a list but pinning one keeps the wire shape tight.
type Digest struct {
	SHA256 string `json:"sha256"`
}

// Subject is one entry in `subject[]`. We always emit exactly one
// (the artifact being attested); the array shape is per spec.
type Subject struct {
	Name   string `json:"name"`
	Digest Digest `json:"digest"`
}

// Material is one entry in `predicate.materials[]`. One per source
// file for the migrate path; absent for the deploy path (which has
// no per-file materials visible to the server).
type Material struct {
	URI    string `json:"uri"`
	Digest Digest `json:"digest"`
}

// ToolEntry is one entry in `predicate.buildTools[]`. Version
// string is operator-supplied (e.g. "rustc 1.82.0 (...)"); we
// store it verbatim without parsing.
type ToolEntry struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Invocation captures how the build was invoked. ConfigSource
// references the project root (a git+file URI is the SLSA-blessed
// form for a local checkout); Parameters captures the build-time
// knobs (target triple, profile).
type Invocation struct {
	ConfigSource ConfigSource `json:"configSource"`
	Parameters   Parameters   `json:"parameters"`
}

// ConfigSource is the URI + digest of the build's source of truth.
// For edgecloud today the source is the local project root (no
// remote git fetch in the build path), so `uri` is `git+file://...`
// and `digest` is `sha256(<source contents>)` if the CLI supplied
// one, or `nil` if not (legacy CLI without build metadata).
type ConfigSource struct {
	URI    string `json:"uri"`
	Digest Digest `json:"digest"`
}

// Parameters holds the build-time knobs. Today this is the target
// triple + profile; future knobs (e.g. optimization level) slot in
// without a wire break.
type Parameters struct {
	Target  string `json:"target"`
	Profile string `json:"profile"`
}

// Completeness declares what the envelope covers. We always set
// both fields true (the build environment is fully captured in the
// metadata; the materials array is the complete source-file list
// modulo the truncation cap).
type Completeness struct {
	Environment bool `json:"environment"`
	Materials   bool `json:"materials"`
}

// Metadata is the build-time bookkeeping. BuildStartedOn /
// BuildFinishedOn are RFC3339; Reproducible is false today (the
// build is not bit-reproducible across hosts).
type Metadata struct {
	BuildStartedOn     string       `json:"buildStartedOn"`
	BuildFinishedOn    string       `json:"buildFinishedOn"`
	Completeness       Completeness `json:"completeness"`
	Reproducible       bool         `json:"reproducible"`
	MaterialsTruncated bool         `json:"materials_truncated,omitempty"`
}

// Predicate is the SLSA provenance v1 predicate body. The buildType
// is the edgecloud-specific build script; everything else is
// per-spec.
type Predicate struct {
	BuildType  string      `json:"buildType"`
	Builder    Builder     `json:"builder"`
	Invocation Invocation  `json:"invocation"`
	BuildTools []ToolEntry `json:"buildTools"`
	Metadata   Metadata    `json:"metadata"`
	Materials  []Material  `json:"materials"`
}

// Builder identifies the build platform. `id` is a URI; we don't
// embed per-key public-key material here because the DSSE wrapper
// already carries the signing key id, and downstream verifiers can
// resolve the public key out-of-band. Keeping the predicate slim
// makes it forward-compatible with future SLSA shape changes.
type Builder struct {
	ID string `json:"id"`
}

// Statement is the in-toto Statement v0.1 envelope before
// canonicalization. Marshaled via CanonicalStatement for signing.
type Statement struct {
	Type          string    `json:"_type"`
	PredicateType string    `json:"predicateType"`
	Subject       []Subject `json:"subject"`
	Predicate     Predicate `json:"predicate"`
}

// CLISideMetadata is what the CLI uploads alongside the artifact
// (multipart field `build_metadata`). It's the CLI's contribution
// to the envelope: toolchain versions, target triple, source
// digest. Optional — an absent CLI metadata (operator ran `cargo
// build` manually) just means the envelope uses "unknown" toolchain
// fields and no source digest.
type CLISideMetadata struct {
	ToolchainRustc   string `json:"toolchain_rustc,omitempty"`
	ToolchainCargo   string `json:"toolchain_cargo,omitempty"`
	ToolchainClang   string `json:"toolchain_clang,omitempty"`
	ToolchainRustcUP string `json:"toolchain_rustup,omitempty"`
	Target           string `json:"target,omitempty"`
	Profile          string `json:"profile,omitempty"`
	SourceDigest     string `json:"source_digest,omitempty"`
	BuildStartedOn   string `json:"build_started_on,omitempty"`
}

// BuildOptions configures statement construction. Required:
// ArtifactSHA256 (subject digest), ArtifactPath (subject name).
// Optional: everything else. Server-side callers populate from the
// MigrationReport + CLI metadata; CLI-side callers can't populate
// the server-only fields and should leave them empty.
type BuildOptions struct {
	ArtifactSHA256 string
	ArtifactPath   string

	// Build invocation (always populated server-side).
	Target          string // e.g. "wasm32-wasip2"
	Profile         string // e.g. "release"
	BuildStartedOn  time.Time
	BuildFinishedOn time.Time

	// Tooling (one entry per tool used). Order is preserved in the
	// canonical output to keep signatures deterministic across runs.
	Tools []ToolEntry

	// Materials: one entry per source file (migrate path) or empty
	// (deploy path). If the slice exceeds maxMaterials, only the
	// first maxMaterials entries are kept and
	// `predicate.metadata.materials_truncated` is set true.
	Materials []Material

	// Optional CLI metadata. Nil-safe: every field is optional.
	CLI *CLISideMetadata
}

// NewStatement constructs a Statement from BuildOptions. Returns
// the struct (callers should pass it through Canonicalize +
// SignStatement for a wire envelope).
func NewStatement(opts BuildOptions) (Statement, error) {
	if opts.ArtifactSHA256 == "" {
		return Statement{}, fmt.Errorf("provenance: ArtifactSHA256 is required")
	}
	if opts.ArtifactPath == "" {
		return Statement{}, fmt.Errorf("provenance: ArtifactPath is required")
	}
	if !validHex64(opts.ArtifactSHA256) {
		return Statement{}, fmt.Errorf("provenance: ArtifactSHA256 must be 64 lowercase hex chars, got %q", opts.ArtifactSHA256)
	}
	if opts.BuildStartedOn.IsZero() {
		opts.BuildStartedOn = time.Now().UTC()
	}
	if opts.BuildFinishedOn.IsZero() {
		opts.BuildFinishedOn = time.Now().UTC()
	}

	truncated := false
	mats := opts.Materials
	if len(mats) > maxMaterials {
		mats = mats[:maxMaterials]
		truncated = true
	}

	stmt := Statement{
		Type:          StatementTypeURI,
		PredicateType: PredicateSLSAProvenance,
		Subject: []Subject{{
			Name:   opts.ArtifactPath,
			Digest: Digest{SHA256: opts.ArtifactSHA256},
		}},
		Predicate: Predicate{
			BuildType: BuildTypeURI,
			Builder:   Builder{ID: BuilderID},
			Invocation: Invocation{
				ConfigSource: ConfigSource{
					URI:    "git+file://" + opts.ArtifactPath,
					Digest: Digest{SHA256: cliSourceDigest(opts.CLI)},
				},
				Parameters: Parameters{
					Target:  opts.Target,
					Profile: opts.Profile,
				},
			},
			BuildTools: opts.Tools,
			Metadata: Metadata{
				BuildStartedOn:     opts.BuildStartedOn.UTC().Format(time.RFC3339),
				BuildFinishedOn:    opts.BuildFinishedOn.UTC().Format(time.RFC3339),
				Completeness:       Completeness{Environment: true, Materials: true},
				Reproducible:       false,
				MaterialsTruncated: truncated,
			},
			Materials: mats,
		},
	}
	return stmt, nil
}

// cliSourceDigest returns the CLI-supplied source digest, or "" if
// not supplied. Empty digest renders as `"sha256":""` in JSON —
// that's intentional: in-toto allows an empty digest and a
// downstream verifier treats it as "unknown", which is the honest
// answer when the CLI didn't supply one.
func cliSourceDigest(cli *CLISideMetadata) string {
	if cli == nil {
		return ""
	}
	return cli.SourceDigest
}

// Canonicalize returns the canonical byte representation of s —
// the unique byte sequence the verifier MUST reconstruct before
// checking the signature. Two properties:
//
//  1. Object keys are sorted lexicographically at every depth
//     (json.Marshal sorts map keys automatically; structs are
//     already ordered by field declaration but we re-marshal via
//     map[string]any for the predicate subtree to get nested
//     sorts too).
//  2. No insignificant whitespace. We re-marshal through
//     `json.Marshal` (which already omits insignificant spaces in
//     Go) and then sort keys via an intermediate map round-trip.
//
// The output is stable: the same Statement value always produces
// the same bytes, so a signature over one canonicalization is
// valid against any other canonicalization of the same value.
func Canonicalize(s Statement) ([]byte, error) {
	// Round-trip through map[string]any forces key sorting at
	// every depth; Go's json.Marshal sorts map keys but not struct
	// fields, so a direct marshal would emit struct order which
	// could drift if a future refactor reorders fields.
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("provenance: marshal Statement: %w", err)
	}
	var anyMap map[string]any
	if err := json.Unmarshal(raw, &anyMap); err != nil {
		return nil, fmt.Errorf("provenance: reparse Statement: %w", err)
	}
	out, err := json.Marshal(anyMap)
	if err != nil {
		return nil, fmt.Errorf("provenance: re-marshal canonical: %w", err)
	}
	return out, nil
}

// DSSESignature is one entry in `signatures[]`. `Keyid` and `Sig`
// are both base64url-safe-bytes-via-alphabet but the wire form is
// plain strings (no encoding).
type DSSESignature struct {
	KeyID string `json:"keyid"`
	Sig   string `json:"sig"`
}

// DSSEEnvelope is the on-the-wire / on-disk shape returned from
// SignStatement and persisted as `build_attestation`. Field order
// matches the DSSE spec: payloadType, payload, signatures.
type DSSEEnvelope struct {
	PayloadType string          `json:"payloadType"`
	Payload     string          `json:"payload"`
	Signatures  []DSSESignature `json:"signatures"`
}

// SignStatement canonicalizes stmt, signs it with the keyring's
// active key, and wraps the result in a DSSE envelope. Returns
// the envelope (ready to persist / return to the CLI) plus the
// raw canonical bytes (handy for tests asserting canonicalization
// stability).
//
// The keyring's active kid is stamped into the signature entry's
// `keyid` so a downstream verifier can resolve the public key
// from its own keyring.
func SignStatement(stmt Statement, keyring interface {
	SignBytes(msg []byte) (sig, kid string, err error)
}) (DSSEEnvelope, []byte, error) {
	canonical, err := Canonicalize(stmt)
	if err != nil {
		return DSSEEnvelope{}, nil, err
	}
	sig, kid, err := keyring.SignBytes(canonical)
	if err != nil {
		return DSSEEnvelope{}, nil, fmt.Errorf("provenance: keyring.SignBytes: %w", err)
	}
	env := DSSEEnvelope{
		PayloadType: PayloadTypeInTotoJSON,
		Payload:     base64.RawURLEncoding.EncodeToString(canonical),
		Signatures: []DSSESignature{{
			KeyID: kid,
			Sig:   sig,
		}},
	}
	return env, canonical, nil
}

// VerifyEnvelope checks that env's signature verifies under
// `keyring` for the payload it carries. Convenience helper for
// tests and downstream tooling; production verification of the
// deployment's primary artifact signature still goes through
// Keyring.Verify (different payload).
func VerifyEnvelope(env DSSEEnvelope, keyring interface {
	VerifyBytes(msg []byte, signatureB64, kid string) (bool, error)
}) (bool, error) {
	if len(env.Signatures) != 1 {
		return false, fmt.Errorf("provenance: expected exactly 1 signature, got %d", len(env.Signatures))
	}
	canonical, err := base64.RawURLEncoding.DecodeString(env.Payload)
	if err != nil {
		return false, fmt.Errorf("provenance: payload base64url decode: %w", err)
	}
	sig := env.Signatures[0]
	return keyring.VerifyBytes(canonical, sig.Sig, sig.KeyID)
}

// HashArtifactBytes is a tiny helper used by callers that already
// have the artifact in memory (e.g. the CP's SaveAndHash). It
// returns the lowercase hex SHA-256 of the bytes — the shape
// Statement.Subject[0].Digest.SHA256 expects.
func HashArtifactBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// validHex64 returns true if s is exactly 64 lowercase hex chars.
// Mirrors signing.decodeHashHex but is internal — provenance
// statements don't pass through the signing keyring's
// deployment-id-binding validation, so we re-implement the shape
// check locally.
func validHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}
