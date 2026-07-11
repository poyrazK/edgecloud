package signing

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

// fixedMaster is the deterministic cluster master used across the
// HKDF tests. Mirrors the test fixtures in keyring_test.go (issue
// #307) so the file conventions stay consistent.
var fixedMaster = []byte("test-cluster-master-secret-that-is-long-enough-32-bytes!")

// TestDeriveWorkerSecret_Deterministic pins the core HKDF contract:
// same inputs always produce the same output. This is the property
// that lets the CP re-derive at verify time without storing the
// secret — if it ever drifted, the worker would 401 on every
// outbound request and the worker_key_cache middleware would loop
// forever.
func TestDeriveWorkerSecret_Deterministic(t *testing.T) {
	got1, err := DeriveWorkerSecret(fixedMaster, "w_fra_abc", "t_acme", "fra",
		"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
	if err != nil {
		t.Fatalf("DeriveWorkerSecret call 1: %v", err)
	}
	got2, err := DeriveWorkerSecret(fixedMaster, "w_fra_abc", "t_acme", "fra",
		"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
	if err != nil {
		t.Fatalf("DeriveWorkerSecret call 2: %v", err)
	}
	if !bytes.Equal(got1, got2) {
		t.Errorf("HKDF not deterministic: call1 != call2\ncall1=%x\ncall2=%x", got1, got2)
	}
	if len(got1) != 32 {
		t.Errorf("len(got) = %d, want 32 (HS256 key size)", len(got1))
	}
}

// TestDeriveWorkerSecret_DistinctInputs pins the cross-input
// collision resistance: changing ANY of (workerID, tenantID, region,
// publicKeyHex) must produce a completely different secret. This is
// the property that makes per-worker isolation hold — a worker
// with public_key_A cannot derive the secret of public_key_B even
// knowing the master, and a worker enrolled for tenant_X cannot
// derive the secret for tenant_Y. The output divergence is strong
// (every byte differs on average); the test just asserts no prefix
// match.
func TestDeriveWorkerSecret_DistinctInputs(t *testing.T) {
	base := func() []byte {
		got, err := DeriveWorkerSecret(fixedMaster, "w_fra_abc", "t_acme", "fra",
			"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
		if err != nil {
			t.Fatalf("DeriveWorkerSecret base: %v", err)
		}
		return got
	}

	cases := []struct {
		name       string
		workerID   string
		tenantID   string
		region     string
		publicKey  string
	}{
		{"different worker_id", "w_fra_xyz", "t_acme", "fra",
			"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"},
		{"different tenant_id", "w_fra_abc", "t_other", "fra",
			"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"},
		{"different region", "w_fra_abc", "t_acme", "ams",
			"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"},
		{"different public_key", "w_fra_abc", "t_acme", "fra",
			"0000000000000000000000000000000000000000000000000000000000000000"},
	}
	baseOut := base()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DeriveWorkerSecret(fixedMaster, tc.workerID, tc.tenantID, tc.region, tc.publicKey)
			if err != nil {
				t.Fatalf("DeriveWorkerSecret: %v", err)
			}
			if bytes.Equal(got, baseOut) {
				t.Errorf("HKDF collision: %s produced the same secret as base", tc.name)
			}
		})
	}
}

// TestDeriveWorkerSecret_LengthAlways32 pins the HS256 key-length
// contract. The HS256 signer truncates or hashes keys longer than
// 32 bytes, but a 32-byte output is the canonical match. A regression
// here would silently produce a key that HS256 truncates to a
// deterministic prefix — breaking the per-worker isolation.
func TestDeriveWorkerSecret_LengthAlways32(t *testing.T) {
	masters := [][]byte{
		[]byte("a-32-byte-master-secret-here!1"),                          // 32 bytes
		[]byte("a-64-byte-master-secret-here-for-good-measure-padding!!!"), // 64 bytes
		bytes.Repeat([]byte{0xab}, 128),                                   // 128 bytes
	}
	for _, m := range masters {
		got, err := DeriveWorkerSecret(m, "w_fra_abc", "t_acme", "fra",
			"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
		if err != nil {
			t.Fatalf("DeriveWorkerSecret: %v", err)
		}
		if len(got) != 32 {
			t.Errorf("len(got) = %d, want 32 for master len %d", len(got), len(m))
		}
	}
}

// TestDeriveWorkerSecret_EmptyMaster pins the defensive error: an
// empty master MUST be rejected. This is the only way DeriveWorkerSecret
// can return an error — HKDF extraction requires non-empty IKM.
// Operators see a startup failure rather than a silent zero-key.
func TestDeriveWorkerSecret_EmptyMaster(t *testing.T) {
	_, err := DeriveWorkerSecret(nil, "w_fra_abc", "t_acme", "fra",
		"aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
	if err == nil {
		t.Error("got nil error for empty master, want non-nil")
	}
	if !strings.Contains(err.Error(), "master") {
		t.Errorf("error %q does not mention 'master'", err)
	}
}

// TestWorkerKID_Stable pins the KID stability property: same pubkey
// always produces the same KID. WorkerAuth caches the derived secret
// per kid — if KID were unstable the cache would churn and the
// worker would 401 every restart.
func TestWorkerKID_Stable(t *testing.T) {
	pubkey := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	want := WorkerKID(pubkey)
	if want != WorkerKID(pubkey) {
		t.Errorf("WorkerKID unstable: %s != %s", want, WorkerKID(pubkey))
	}
	if !strings.HasPrefix(want, "wkr_") {
		t.Errorf("WorkerKID = %q, want wkr_ prefix", want)
	}
	// 4 (prefix) + 8 (hex fingerprint) = 12 chars total.
	if len(want) != 4+8 {
		t.Errorf("WorkerKID len = %d, want 12 (wkr_ + 8 hex)", len(want))
	}
}

// TestWorkerKID_DifferentPubkeys ensures the KID changes when the
// pubkey changes (the property that triggers per-worker rotation on
// re-enrollment). We don't assert on the exact hex values (those
// depend on sha256 internals) — just that two distinct pubkeys map
// to distinct KIDs.
func TestWorkerKID_DifferentPubkeys(t *testing.T) {
	a := WorkerKID("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
	b := WorkerKID("0000000000000000000000000000000000000000000000000000000000000000")
	if a == b {
		t.Errorf("WorkerKID collision: %q == %q for distinct pubkeys", a, b)
	}
}

// TestIsWorkerKID pins the namespace-routing predicate. WorkerAuth
// uses this to decide whether to route through the HKDF path or
// the legacy cfg.Keys path. A regression that flipped the prefix
// check would silently break per-worker JWT verification.
func TestIsWorkerKID(t *testing.T) {
	cases := []struct {
		kid  string
		want bool
	}{
		{"wkr_aabbccdd", true},
		{"wkr_", false}, // just the prefix; no fingerprint
		{"aabbccdd", false},
		{"", false},
		{"WKR_aabbccdd", false}, // case-sensitive
		{"wkx_aabbccdd", false}, // off-by-one prefix
	}
	for _, tc := range cases {
		if got := IsWorkerKID(tc.kid); got != tc.want {
			t.Errorf("IsWorkerKID(%q) = %v, want %v", tc.kid, got, tc.want)
		}
	}
}

// TestDeriveWorkerSecret_KnownVector pins an external-checkable test
// vector. The vector was generated by:
//
//	python3 -c "
//	import hashlib, hmac
//	def hkdf(salt, ikm, info, length):
//	    prk = hmac.new(salt, ikm, hashlib.sha256).digest()
//	    t, okm, i = b'', b'', 0
//	    while len(okm) < length:
//	        i += 1
//	        t = hmac.new(prk, t + info + bytes([i]), hashlib.sha256).digest()
//	        okm += t
//	    return okm[:length]
//	print(hkdf(b'pubkey', b'master-secret', b'worker-v1|w_1|t_1|fra', 32).hex())
//	"
//
// If HKDF or the info construction ever changes shape, this test
// will catch it before any worker goes into production.
func TestDeriveWorkerSecret_KnownVector(t *testing.T) {
	master := []byte("master-secret")
	pubkey := "pubkey"
	got, err := DeriveWorkerSecret(master, "w_1", "t_1", "fra", pubkey)
	if err != nil {
		t.Fatalf("DeriveWorkerSecret: %v", err)
	}
	const wantHex = "609b0af91e0b69c28abfbfdca1d5849a3b266df5c82d711b39a249d4506c0d83"
	if hex.EncodeToString(got) != wantHex {
		t.Errorf("known-vector mismatch:\n got  %s\n want %s",
			hex.EncodeToString(got), wantHex)
	}
}
