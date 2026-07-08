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
file only the CP can read. Operators building a keyring generate
one such file per kid and combine them into a single keyring
file (see [Rotation](#rotation-zero-downtime) below):

```bash
# 32-byte seed → 64-byte private key (Go's crypto/ed25519 will
# auto-expand either shape; we accept both. See
# internal/signing/signer.go::parsePrivateKey for the loader.)
openssl rand -hex 32 > /etc/edge/signing.k1.key
chmod 0600 /etc/edge/signing.k1.key
chown edge-control-plane:edge /etc/edge/signing.k1.key
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
As of PR #307 follow-up PR1, the CP boots from a **keyring** (a
named map of `kid → private key`). The legacy single-key form is
still accepted as a one-release deprecation fallback and logs a
warning at startup.

### Keyring form (recommended)

The keyring is one `<kid> = <32-byte-seed-hex>` line per key:

```text
# /etc/edge/signing.keyring
# Lines starting with `#` and blank lines are ignored.
# The kid is operator-chosen; conventionally lowercase short
# identifiers (e.g. "k1", "k2"). The value is the 32-byte Ed25519
# seed as 64 lowercase hex characters.

k1 = 9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60
k2 = 5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d
```

`EDGE_SIGNING_KEY_ID` selects which key is active for new
deployments. Set it to a kid that's present in the keyring — a
typo or stale value fails the CP at startup with `ErrInvalidKey`.

```bash
# Recommended (containers):
export EDGE_SIGNING_KEYRING_PATH=/etc/edge/signing.keyring
export EDGE_SIGNING_KEY_ID=k2      # current active signing key
# Inline form (dev only):
export EDGE_SIGNING_KEYRING='k1 = <hex>\nk2 = <hex>'
export EDGE_SIGNING_KEY_ID=k2
```

Or via YAML config:

```yaml
signing:
  keyring_path: /etc/edge/signing.keyring
  key_id: k2
```

### Legacy single-key form (deprecated)

The pre-PR1 single-key env vars (`EDGE_SIGNING_KEY_PATH` /
`EDGE_SIGNING_KEY`) still work; they wrap the key in a 1-entry
keyring with kid `"default"` and log a deprecation warning at
startup. Migrate by generating a keyring file and switching the
env vars. The deprecation will be removed in a follow-up release.

```bash
export EDGE_SIGNING_KEY_PATH=/etc/edge/signing.key    # deprecated
export EDGE_SIGNING_KEY_ID=k1                         # must be empty or "default"
```

## Worker configuration

Workers verify signatures against the public key. The default value
of `EDGE_REQUIRE_SIGNATURE` is **`true`** (secure-by-default); an
empty signature is treated as "unsigned legacy artifact" and
rejected.

```bash
export EDGE_REQUIRE_SIGNATURE=true
export EDGE_SIGNING_KEYRING_PATH=/etc/edge/signing.pub.keyring
# (or, for the single-key legacy form)
export EDGE_SIGNING_PUBKEY=<64-hex-of-public-key>
export EDGE_SIGNING_PUBKEY_PATH=/etc/edge/signing.pub.hex
```

The keyring file format mirrors the CP side, but each line is
`<kid> = <32-byte-pubkey-hex>` (the public counterpart):

```text
# /etc/edge/signing.pub.keyring
k1 = d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a
k2 = 5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d2e7f3c1a5b6e6c4e1a8f4b9d
```

**Keyring resolution order (worker startup):**

1. `EDGE_SIGNING_KEYRING_PATH` — multi-key keyring file. Wins if set.
2. `EDGE_SIGNING_KEYRING` — inline keyring payload.
3. `EDGE_SIGNING_PUBKEY_PATH` — legacy single-key file (deprecation
   fallback; wraps in a 1-entry keyring with kid `default`).
4. `EDGE_SIGNING_PUBKEY` — legacy inline single-key.
5. None of the above + `EDGE_REQUIRE_SIGNATURE=false` → no keyring,
   unsigned artifacts accepted (rollout escape hatch).
6. None of the above + `EDGE_REQUIRE_SIGNATURE=true` → worker
   refuses to start with a clear error naming all six env vars.

**Signed message layout** (for operators debugging a failing verify
or running a non-Go verifier out-of-band): the worker reconstructs
the signed payload as `sha256_raw_32_bytes || deployment_id_bytes`
— the raw 32-byte hash, **not** the hex form. Any non-Go verifier
must hash the artifact, take the raw 32 bytes, append the raw
`deployment_id` bytes, and verify with the public key.

## Rotation (zero-downtime)

The keyring makes signing-key rotation a no-restart-for-the-worker
operation. Sequence:

1. **Generate** a fresh keypair (k2). Keep k1 around — in-flight
   artifacts are signed by k1.

   ```bash
   openssl rand -hex 32 > /etc/edge/signing.k2.key
   chmod 0600 /etc/edge/signing.k2.key
   ```

2. **Derive k2's public key** (using one of the recipes above).

3. **Update the worker keyring** to include both k1 and k2:

   ```text
   # /etc/edge/signing.pub.keyring (mounted into the worker)
   k1 = <hex-of-current-pubkey>
   k2 = <hex-of-new-pubkey>
   ```

   No worker restart needed if the keyring is mounted from a
   ConfigMap the operator can hot-swap (the worker reads the file
   once at boot today; in-process hot-reload is a future-PR
   concern — until then, **restart the worker** to pick up the new
   keyring).

4. **Update the CP keyring** to include both keys and rotate the
   active kid:

   ```text
   # /etc/edge/signing.keyring (mounted into the CP)
   k1 = <hex-of-current-seed>
   k2 = <hex-of-new-seed>
   ```

   ```bash
   export EDGE_SIGNING_KEY_ID=k2      # CP signs new artifacts with k2
   ```

   Restart the CP. New artifacts carry `signing_key_id=k2`; old
   `k1` artifacts on disk still verify because the worker keyring
   still has k1.

5. **Audit retirement.** Operators can list deployments signed by
   the retired key with:

   ```sql
   SELECT id, app_name, created_at FROM deployments
   WHERE signing_key_id = 'k1'
   ORDER BY created_at DESC;
   ```

   Once that list is empty (all k1-signed artifacts aged out via
   re-deploy or natural eviction), remove k1 from both keyring
   files.

6. **Worker keyring reload.** Until hot-reload lands, repeat step
   3's worker restart as part of the rotation cutover. The total
   window is small (~1 CP restart + 1 worker restart, no
   in-flight artifact failures).

The `signing_key_id` column on `deployments` is already populated
by every signing path since PR #355, so the operator audit query
in step 5 works without a new migration.

## File format reference

| Path                              | Format                          | Size         |
| --------------------------------- | ------------------------------- | ------------ |
| `EDGE_SIGNING_KEYRING_PATH`       | keyring file (CP, private keys) | one line per kid |
| `EDGE_SIGNING_KEYRING`            | inline keyring (CP)             | same         |
| `EDGE_SIGNING_KEY_PATH`           | raw bytes or hex (CP, single)   | 32 / 64 / 64-hex / 128-hex |
| `EDGE_SIGNING_KEY`                | inline (CP, single)              | same         |
| `EDGE_SIGNING_KEYRING_PATH` (worker) | keyring file (pubkeys)        | one line per kid |
| `EDGE_SIGNING_PUBKEY`             | hex (32 bytes, single)          | 64 chars |
| `EDGE_SIGNING_PUBKEY_PATH`        | hex file (single)               | 64 chars on disk |

The CP's loader (`internal/signing/keyring.go::parseKeyringLines`
for the keyring form, `internal/signing/signer.go::parsePrivateKey`
for the legacy single-key form) auto-detects size and content.
Wrong sizes, non-ASCII, non-hex, malformed `<kid> = <hex>` lines,
or duplicate kids return a typed `ErrInvalidKey` error with a
specific message; the server fails to start with that message
instead of running with a default placeholder.
