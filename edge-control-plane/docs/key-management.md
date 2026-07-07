# Artifact signing key management

Issue #307 closes the gap where an attacker with **both** the artifact
store and the `deployments.hash` DB column could substitute a malicious
artifact with a matching SHA-256. The control plane now signs every
new deployment's artifact with an Ed25519 private key; workers
verify the signature against the configured public key before
instantiation. This doc covers how operators generate, deploy, and
rotate that key.

## Threat model

- **In scope:** an attacker who has read-write access to both
  `/registry/` (or S3) **and** the `deployments` table can swap a
  wasm binary AND update its hash to match — SHA-256 alone does not
  detect this. With Ed25519 signing, a swap is detected because
  the artifact's new bytes do not verify against the CP's signing
  key, which the attacker does not possess.
- **Out of scope:** supply-chain attacks against the build
  pipeline, key compromise of the CP itself, or compromise of the
  worker's signing-pubkey configuration. Those require their own
  mitigations (SLSA provenance, HSM-backed keys, signed
  worker-pubkey manifests).

## Key generation

Generate a fresh Ed25519 keypair and write the **private** key to a
file only the CP can read:

```bash
# 32-byte seed → 64-byte private key (Go's crypto/ed25519 will
# auto-expand either shape; we accept both. See
# internal/signing/signer.go::parsePrivateKey for the loader.)
openssl rand -hex 32 > /etc/edge/signing.key
chmod 0600 /etc/edge/signing.key
chown edge-control-plane:edge /etc/edge/signing.key
```

Generate the **public** key (in the format the worker consumes):

```bash
# Derive the Ed25519 public key from the 32-byte seed. Ed25519 pubkey
# derivation per RFC 8032 §5.1.2 is NOT a simple SHA-512 truncation —
# it's a clamping + scalar-multiply of the seed against the Ed25519
# base point. Use a real Ed25519 library.
#
# Option A: Go (always available alongside the control plane binary):
go run ./cmd/printpub -key /etc/edge/signing.key
#
# Option B: Python with the `cryptography` package:
python3 -c "
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
seed = bytes.fromhex(open('/etc/edge/signing.key').read().strip())
sk = Ed25519PrivateKey.from_private_bytes(seed)
print(sk.public_key().public_bytes_raw().hex())
"
```

The resulting 64-character hex string is what workers pass as
`EDGE_SIGNING_PUBKEY`. (`cmd/printpub` is a small `main.go` in the
control-plane repo — see `cmd/printpub/main.go`. It loads the seed
via `signing.LoadFromFile` and prints `PublicKeyHex()`. Operators
without Go access can use Option B with PyNaCl as a one-off:
`pip install pynacl && python3 -c "from nacl.signing import SigningKey; print(SigningKey(bytes.fromhex(open('/etc/edge/signing.key').read().strip())).verify_key.encode().hex())"`.)

> Note: a previously-shipped version of this doc had a wrong recipe
> that derived the pubkey as `SHA-512(seed)[32:]`. That is NOT the
> Ed25519 derivation; signatures verified under such a "pubkey" will
> never match. If you followed that recipe, re-derive with one of the
> options above before deploying the worker.

## Control plane configuration

The CP refuses to start without a signing key (issue #307 fail-fast).

```yaml
signing:
  key_path: /etc/edge/signing.key
  key_id: k1   # logical key id, stamped onto deployments.signing_key_id
```

or via env vars (recommended for containers):

```bash
export EDGE_SIGNING_KEY_PATH=/etc/edge/signing.key
export EDGE_SIGNING_KEY_ID=k1
# Inline hex variant (development only):
export EDGE_SIGNING_KEY=$(cat /etc/edge/signing.key)
```

`key_id` is optional in v1 — operators that don't care about
rotation can leave it empty (a startup warning logs that rotation
semantics will be ambiguous). The string is purely diagnostic —
distinct key ids let operators reason about "is this artifact signed
by the current key?" without a code change today, and forms the
basis of the rotation plumbing in a follow-up PR.

## Worker configuration

Workers verify signatures against the public key. The default value
of `EDGE_REQUIRE_SIGNATURE` is **`true`** (secure-by-default); an
empty signature is treated as "unsigned legacy artifact" and
rejected.

```bash
export EDGE_REQUIRE_SIGNATURE=true
export EDGE_SIGNING_PUBKEY=<64-hex-of-public-key>
# (or)
export EDGE_SIGNING_PUBKEY_PATH=/etc/edge/signing.pub.hex
```

The two env-var shapes mirror the CP config (inline vs file path).
The file-path shape is the production recommendation: it lets
operators rotate the key via a ConfigMap mount without restarting
the process on a key change (the worker reads the file once at
boot).

## Rotation

The rotation story is being finalized in a follow-up PR. The shape
today:

1. Generate a new keypair (k2).
2. Update the CP's `signing.key_id` to `k2` and rotate the file at
   `EDGE_SIGNING_KEY_PATH` (or set `EDGE_SIGNING_KEY_ID=k2` +
   rotate file).
3. **CP behavior:** every artifact uploaded after the rotation
   carries `signing_key_id=k2`. Existing `signing_key_id=k1` rows
   are unchanged (no re-sign), but workers running the old
   `k1`-era public key can no longer verify `k2` artifacts.
4. **Worker behavior:** today the worker holds a single public key
   via `EDGE_SIGNING_PUBKEY`. To accept both `k1` (in-flight) and
   `k2` (new) signatures, the worker must accept a keyring until
   the k1 artifacts age out. The follow-up PR adds
   `EDGE_SIGNING_PUBKEY_<KID>=...` env-var support for this
   transition.

**Operational guidance for now:** roll the worker with the new
public key after re-deploying every active app. The "fail closed"
default of `EDGE_REQUIRE_SIGNATURE=true` ensures a stale public key
rejects in-flight unverified builds rather than silently accepting
an unsigned or wrong-key artifact.

## File format reference

| Path                              | Format                  | Size     |
| --------------------------------- | ----------------------- | -------- |
| `EDGE_SIGNING_KEY_PATH`           | raw bytes or hex        | 32 / 64 / 64-hex / 128-hex |
| `EDGE_SIGNING_KEY` (inline)       | raw bytes or hex        | same     |
| `EDGE_SIGNING_PUBKEY`             | hex (32 bytes)          | 64 chars |
| `EDGE_SIGNING_PUBKEY_PATH`        | hex file                | 64 chars on disk |

The CP's loader (`internal/signing/signer.go::parsePrivateKey`)
auto-detects size and content. Wrong sizes, non-ASCII, or non-hex
content return a typed `ErrInvalidKey` error with a size hint; the
server fails to start with a clear message instead of running with
the default placeholder.
