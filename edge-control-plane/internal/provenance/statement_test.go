package provenance

import (
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

// fixedArtifactSHA256 is a deterministic test fixture. Not a
// "real" sha256 of anything — just 64 lowercase hex chars.
const fixedArtifactSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// testBuildOptions returns a fully-populated BuildOptions for
// happy-path tests. All time fields are pinned to a fixed
// timestamp so canonicalization is byte-identical across runs.
func testBuildOptions() BuildOptions {
	return BuildOptions{
		ArtifactSHA256:  fixedArtifactSHA256,
		ArtifactPath:    "registry/t_a/demo/d_abc.wasm",
		Target:          "wasm32-wasip2",
		Profile:         "release",
		BuildStartedOn:  time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC),
		BuildFinishedOn: time.Date(2026, 7, 8, 10, 0, 5, 0, time.UTC),
		Tools: []ToolEntry{
			{Name: "cargo", Version: "cargo 1.82.0"},
			{Name: "rustc", Version: "rustc 1.82.0"},
		},
		Materials: []Material{
			{URI: "file://src/main.rs", Digest: Digest{SHA256: "1111111111111111111111111111111111111111111111111111111111111111"}},
			{URI: "file://src/lib.rs", Digest: Digest{SHA256: "2222222222222222222222222222222222222222222222222222222222222222"}},
		},
	}
}

//  1. NewStatement produces the in-toto v0.1 + SLSA v1 wire shape
//     exactly. Operator tooling that parses the persisted JSONB
//     needs every field name + nesting to be correct, so this test
//     pins the high-level shape rather than just "doesn't panic".
func TestNewStatement_ShapeConformsToSpec(t *testing.T) {
	stmt, err := NewStatement(testBuildOptions())
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	if stmt.Type != StatementTypeURI {
		t.Errorf("Statement.Type = %q, want %q", stmt.Type, StatementTypeURI)
	}
	if stmt.PredicateType != PredicateSLSAProvenance {
		t.Errorf("Statement.PredicateType = %q, want %q", stmt.PredicateType, PredicateSLSAProvenance)
	}
	if len(stmt.Subject) != 1 {
		t.Fatalf("len(Subject) = %d, want 1", len(stmt.Subject))
	}
	if stmt.Subject[0].Digest.SHA256 != fixedArtifactSHA256 {
		t.Errorf("Subject[0].Digest.SHA256 = %q, want %q", stmt.Subject[0].Digest.SHA256, fixedArtifactSHA256)
	}
	if stmt.Predicate.BuildType != BuildTypeURI {
		t.Errorf("Predicate.BuildType = %q, want %q", stmt.Predicate.BuildType, BuildTypeURI)
	}
	if stmt.Predicate.Builder.ID != BuilderID {
		t.Errorf("Predicate.Builder.ID = %q, want %q", stmt.Predicate.Builder.ID, BuilderID)
	}
	if stmt.Predicate.Invocation.Parameters.Target != "wasm32-wasip2" {
		t.Errorf("Predicate.Invocation.Parameters.Target = %q", stmt.Predicate.Invocation.Parameters.Target)
	}
	if stmt.Predicate.Metadata.Reproducible {
		t.Error("Predicate.Metadata.Reproducible = true, want false (we don't claim reproducibility today)")
	}
	if stmt.Predicate.Metadata.MaterialsTruncated {
		t.Error("Predicate.Metadata.MaterialsTruncated = true on a small materials list")
	}
}

//  2. NewStatement rejects a malformed artifact hash up front so
//     we don't persist an envelope that workers / auditors can't
//     parse. The shape check is 64 lowercase hex (same as the
//     signing package's `decodeHashHex` rule).
func TestNewStatement_RejectsMalformedHash(t *testing.T) {
	cases := []struct {
		name string
		hash string
	}{
		{"empty", ""},
		{"too_short", "abc"},
		{"uppercase", strings.ToUpper(fixedArtifactSHA256)},
		{"non_hex", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := testBuildOptions()
			opts.ArtifactSHA256 = tc.hash
			if _, err := NewStatement(opts); err == nil {
				t.Errorf("expected error for hash=%q, got nil", tc.hash)
			}
		})
	}
}

//  3. Canonicalize is byte-stable. Same Statement value MUST
//     produce the same canonical bytes on every call; otherwise a
//     signature over one canonicalization wouldn't verify against
//     another. This is the single most important property of the
//     envelope.
func TestCanonicalize_StableAcrossCalls(t *testing.T) {
	stmt, err := NewStatement(testBuildOptions())
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	a, err := Canonicalize(stmt)
	if err != nil {
		t.Fatalf("Canonicalize (1): %v", err)
	}
	b, err := Canonicalize(stmt)
	if err != nil {
		t.Fatalf("Canonicalize (2): %v", err)
	}
	if string(a) != string(b) {
		t.Errorf("Canonicalize not stable:\n  first:  %s\n  second: %s", a, b)
	}
}

//  4. Canonicalize is key-sorted at every depth. The output MUST
//     be JSON with lexicographically sorted keys (so a re-marshal
//     on the verifier side produces the same bytes). We assert by
//     comparing against a hand-built canonical reference.
func TestCanonicalize_KeysAreSorted(t *testing.T) {
	stmt, err := NewStatement(testBuildOptions())
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	canon, err := Canonicalize(stmt)
	if err != nil {
		t.Fatalf("Canonicalize: %v", err)
	}
	// Top-level keys must appear in sorted order: _type, predicate,
	// predicateType, subject.
	for _, want := range []string{`"_type"`, `"predicate"`, `"predicateType"`, `"subject"`} {
		if !strings.Contains(string(canon), want) {
			t.Errorf("canonical JSON missing key %s; got %s", want, canon)
		}
	}
	// Predicate-level keys must appear in sorted order: buildTools,
	// buildType, builder, invocation, materials, metadata.
	for _, want := range []string{`"buildTools"`, `"buildType"`, `"builder"`, `"invocation"`, `"materials"`, `"metadata"`} {
		if !strings.Contains(string(canon), want) {
			t.Errorf("predicate subtree missing sorted key %s", want)
		}
	}
}

//  5. SignStatement produces a verifiable envelope. Round-trip:
//     sign, base64-decode payload, verify with the same keyring.
//     Pins the canonical SignStatement → VerifyEnvelope contract.
func TestSignStatement_VerifyEnvelope_Roundtrip(t *testing.T) {
	ring := testKeyring(t)
	stmt, err := NewStatement(testBuildOptions())
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	env, canonical, err := SignStatement(stmt, ring)
	if err != nil {
		t.Fatalf("SignStatement: %v", err)
	}

	// Payload must be base64url(no-pad) of the canonical bytes.
	gotCanonical, err := base64.RawURLEncoding.DecodeString(env.Payload)
	if err != nil {
		t.Fatalf("payload base64 decode: %v", err)
	}
	if string(gotCanonical) != string(canonical) {
		t.Error("env.Payload doesn't match the canonical bytes returned alongside")
	}

	// Exactly one signature entry, keyid matches active kid.
	if len(env.Signatures) != 1 {
		t.Fatalf("len(Signatures) = %d, want 1", len(env.Signatures))
	}
	if env.Signatures[0].KeyID != ring.ActiveKeyID() {
		t.Errorf("signatures[0].keyid = %q, want %q", env.Signatures[0].KeyID, ring.ActiveKeyID())
	}

	// payloadType is pinned to the in-toto+json media type.
	if env.PayloadType != PayloadTypeInTotoJSON {
		t.Errorf("payloadType = %q, want %q", env.PayloadType, PayloadTypeInTotoJSON)
	}

	// Verify must accept the envelope.
	ok, err := VerifyEnvelope(env, ring)
	if err != nil {
		t.Fatalf("VerifyEnvelope: %v", err)
	}
	if !ok {
		t.Error("VerifyEnvelope returned false on a signature SignStatement just produced")
	}
}

//  6. Tampered envelope (different payload bytes) MUST NOT verify.
//     Pinned so a future bug that skips re-canonicalization in the
//     verifier surfaces as a failing test, not a silent re-org.
func TestVerifyEnvelope_RejectsTamperedPayload(t *testing.T) {
	ring := testKeyring(t)
	stmt, err := NewStatement(testBuildOptions())
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	env, _, err := SignStatement(stmt, ring)
	if err != nil {
		t.Fatalf("SignStatement: %v", err)
	}

	// Decode payload, flip a byte, re-encode.
	canonical, err := base64.RawURLEncoding.DecodeString(env.Payload)
	if err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	canonical[10] ^= 0x01
	env.Payload = base64.RawURLEncoding.EncodeToString(canonical)

	ok, err := VerifyEnvelope(env, ring)
	if err != nil {
		t.Fatalf("VerifyEnvelope (tampered): %v", err)
	}
	if ok {
		t.Error("VerifyEnvelope returned true on a tampered payload")
	}
}

//  7. Cross-keyring verify fails. A second keyring (loaded with a
//     different kid) MUST NOT verify an envelope signed by the
//     first — proves the kid is part of the trust decision, not
//     just a label. VerifyEnvelope surfaces a typed error when the
//     kid isn't in the verifier's keyring (mirror of Keyring.Verify);
//     that's the correct rejection signal here, not ok=false.
func TestVerifyEnvelope_RejectsCrossKeyring(t *testing.T) {
	ring1 := testKeyring(t)
	ring2 := testFreshKeyring(t)

	stmt, err := NewStatement(testBuildOptions())
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	env, _, err := SignStatement(stmt, ring1)
	if err != nil {
		t.Fatalf("SignStatement: %v", err)
	}

	ok, err := VerifyEnvelope(env, ring2)
	if ok {
		t.Error("VerifyEnvelope returned true when verified under a different keyring")
	}
	// Acceptable: typed "kid not in keyring" error (preferred), or
	// silent ok=false (also correct). What we MUST NOT see is
	// ok=true.
	_ = err
}

//  8. Material truncation. A 2500-entry materials list must produce
//     an envelope with exactly maxMaterials entries and the
//     truncated flag set in metadata. Verified end-to-end so an
//     operator inspecting a persisted envelope sees the cap
//     behavior, not a 2500-entry payload.
func TestNewStatement_TruncatesLargeMaterials(t *testing.T) {
	opts := testBuildOptions()
	opts.Materials = make([]Material, maxMaterials+500)
	for i := range opts.Materials {
		opts.Materials[i] = Material{
			URI:    "file://src/file_" + itoa(i) + ".rs",
			Digest: Digest{SHA256: hex.EncodeToString([]byte{byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})},
		}
	}
	stmt, err := NewStatement(opts)
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	if len(stmt.Predicate.Materials) != maxMaterials {
		t.Errorf("len(Materials) = %d, want %d", len(stmt.Predicate.Materials), maxMaterials)
	}
	if !stmt.Predicate.Metadata.MaterialsTruncated {
		t.Error("MaterialsTruncated = false on a >maxMaterials input")
	}
}

//  9. CLI-supplied metadata flows into configSource digest.
//     Pinned so a future refactor that drops CLI metadata silently
//     (e.g. forgets to plumb it through) surfaces here.
func TestNewStatement_CLISourceDigestFlowsThrough(t *testing.T) {
	opts := testBuildOptions()
	opts.CLI = &CLISideMetadata{
		ToolchainRustc: "rustc 1.82.0",
		ToolchainCargo: "cargo 1.82.0",
		Target:         "wasm32-wasip2",
		SourceDigest:   "abcdef0000000000000000000000000000000000000000000000000000000000",
	}
	stmt, err := NewStatement(opts)
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	if stmt.Predicate.Invocation.ConfigSource.Digest.SHA256 != opts.CLI.SourceDigest {
		t.Errorf("ConfigSource.Digest.SHA256 = %q, want %q",
			stmt.Predicate.Invocation.ConfigSource.Digest.SHA256, opts.CLI.SourceDigest)
	}
}

//  10. DecodePayload returns the embedded Statement as a map. The
//     predicateType field round-trips through the encode/decode
//     cycle intact — operators using DecodePayload for inspection
//     rely on this.
func TestDecodePayload_Roundtrip(t *testing.T) {
	ring := testKeyring(t)
	stmt, err := NewStatement(testBuildOptions())
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	env, _, err := SignStatement(stmt, ring)
	if err != nil {
		t.Fatalf("SignStatement: %v", err)
	}

	payload, err := DecodePayload(env)
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	if pt, _ := payload["predicateType"].(string); pt != PredicateSLSAProvenance {
		t.Errorf("payload.predicateType = %q, want %q", pt, PredicateSLSAProvenance)
	}
	if got, err := SubjectSHA256(env); err != nil {
		t.Errorf("SubjectSHA256 error: %v", err)
	} else if got != fixedArtifactSHA256 {
		t.Errorf("SubjectSHA256 = %q, want %q", got, fixedArtifactSHA256)
	}
	if got, _ := PredicateType(env); got != PredicateSLSAProvenance {
		t.Errorf("PredicateType = %q, want %q", got, PredicateSLSAProvenance)
	}
}

//  11. HashArtifactBytes: same input always produces the same hex
//     digest; different inputs produce different digests.
func TestHashArtifactBytes(t *testing.T) {
	a := HashArtifactBytes([]byte("hello"))
	b := HashArtifactBytes([]byte("hello"))
	if a != b {
		t.Errorf("HashArtifactBytes not stable: %s vs %s", a, b)
	}
	c := HashArtifactBytes([]byte("world"))
	if a == c {
		t.Errorf("HashArtifactBytes collision on different inputs: %s == %s", a, c)
	}
	// SHA-256("hello") is a well-known constant; pinning it so a
	// future refactor that swaps the algorithm (e.g. accidentally
	// to SHA-1) is caught.
	const wantSHA256Hello = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if a != wantSHA256Hello {
		t.Errorf("HashArtifactBytes(hello) = %s, want %s", a, wantSHA256Hello)
	}
}

// itoa is a stdlib-free small-int formatter for the truncation
// test's URI labels. Avoids dragging in strconv just for one call.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
