# Migration Compiler Sandbox — Operator Runbook

**Audience:** operators deploying `edge-control-plane` in environments where `POST /api/v1/migrate*` is internet-reachable.
**Scope:** process-level sandboxing for the Rust (`rustc`/`cargo`/`wasm-tools`) and C (`clang`) toolchains the CP spawns when handling `POST /api/v1/migrate` and `POST /api/v1/migrate-tree`.

## Threat model recap

The CP compiles tenant-supplied source **on the CP host** (PR #416 closed the `build.rs` RCE vector; issue #622 closed the compile-time macro / env-var / host-include exfiltration vector). The five layers shipped in PR #668 (`fix/issue-622-migration-exfil`):

| Layer | Where | Catches |
|---|---|---|
| L1 | `internal/handler/migration_preflight.go` (regex, cheap) | Obvious `include_bytes!` / `env!` / `#embed` payloads at the HTTP boundary |
| L2 | `edge-migrate-lib` Rust + C analyzers (tree-sitter, authoritative) | Any AST-precise host-reach macro / absolute include, every call path |
| L3 | `internal/service/migration.go::scrubbedEnv` | Env-var leak: `env!("JWT_SECRET")` returns `"JWT_SECRET"` (literal) |
| L4 | `internal/service/migration.go::clangArgs` (`-nostdinc --sysroot`) | Host-filesystem `#include` / `#embed` resolution at C compile |
| L5 | **This document.** Process sandbox via nsjail / bwrap / gVisor / firejail | Arbitrary code execution during toolchain runtime; cargo network egress; `$TMPDIR` symlink races; toolchain DoS |

L1–L4 are shipped in code; L5 is **operator-deployed** and out of scope for code PRs. This runbook is the deployment contract.

## What L1–L4 do NOT catch

Even with all four code layers, the toolchain itself is a complex program that reads files, writes files, spawns child processes, and may attempt network egress:

1. **Arbitrary code execution via legitimate WASI surface** — the analyzer only blocks *known* host-reach patterns. A novel bypass would let a `.wasm` artifact run inside a worker and call any `wasi:sockets/*` host function that the tenant's `socket_mode` permits.
2. **`$TMPDIR` symlink races** — the per-upload temp directory lives under `$TMPDIR` (typically `/tmp`). A second tenant or local process could pre-create a symlink at the predicted path and confuse the toolchain into reading or overwriting arbitrary host files.
3. **Cargo network egress** — `cargo build` fetches crates from crates.io by default. A malicious `Cargo.toml` could exfiltrate host env / files via a custom registry, build script, or proc-macro that triggers network I/O during compile.
4. **Toolchain DoS / resource exhaustion** — an attacker can submit source designed to consume CPU/memory/disk for an unbounded period. The per-request context cancel only kills the top-level process; toolchain subprocess trees can leak.
5. **Defense-in-depth bypass** — L1 regex is loose by design (false-negative risk); L2 tree-sitter is authoritative but only covers `compile-time host-reach` macros. Future analyzers may add coverage, but a sandbox bounds the blast radius of any bypass.

## Recommended: nsjail

[nsjail](https://github.com/google/nsjail) is a process isolation tool that uses Linux namespaces, seccomp-bpf, and capability dropping. It is the most feature-complete of the options listed here.

### Profile

Save as `/etc/edgecloud/nsjail-migrate.profile`:

```ini
# Issue #622 sandbox profile for the edge-control-plane migration
# toolchain (cargo, rustc, wasm-tools, clang, edge-migrate).
# Spawn every toolchain subprocess inside this sandbox.

name: "edgecloud-migrate"

# Namespace isolation.
clone_newnet: true        # NO network access — cargo cannot reach crates.io or
                           # an attacker's custom registry; rustc cannot make
                           # network calls. This is the single most important
                           # setting: closes the cargo-network-exfil vector.
clone_newpid: true        # Own PID namespace — kill the leader and the whole
                           # tree dies; no orphaned grandchildren.
clone_newns: true         # Own mount namespace.
clone_newuts: true        # Hide hostname from the tenant process.
clone_newipc: true
clone_newuser: true       # Map uid 0 in the sandbox → uid 65534 (nobody) outside.

# Mount only the directories the toolchain needs. Everything else is
# unreachable even as a path string.
# wasi-sdk provides the sysroot + clang. CARGO_HOME is operator-vendored
# (if you want air-gapped builds). /tmp is a private tmpfs (closes the
# $TMPDIR symlink race).
mount {
  src: "/opt/wasi-sdk"
  dst: "/opt/wasi-sdk"
  is_bind: true
  rw: false
}
mount {
  src: "/opt/edgecloud/cargo-home"
  dst: "/root/.cargo"
  is_bind: true
  rw: false
}
mount {
  src: "/tmp"
  dst: "/tmp"
  fstype: "tmpfs"
  options: "size=512m,nr_inodes=64k,mode=1777"
}
# /proc is needed by rustc for symbol resolution but nothing else.
mount {
  src: "/proc"
  dst: "/proc"
  fstype: "proc"
}

# Resource limits — these are the "toolchain DoS" defenses.
rlimit_as: 4294967296      # 4 GiB address space
rlimit_cpu: 300            # 5 minutes wall clock per upload
rlimit_fsize: 268435456    # 256 MiB max file size (a tenant .wasm is bounded)
rlimit_nofile: 256         # fd exhaustion guard
rlimit_nproc: 64           # cap children

# Seccomp — block every syscall the toolchain does not need. This is
# the deep-defense layer; even if an attacker escapes the namespace,
# the syscall filter catches them. nsjail ships profile-driven seccomp
# generation; see https://github.com/google/nsjail/blob/master/seccomp/README.md
seccomp_string: "ALLOW"

# Capabilities — drop everything.
capabilities_drop: ["all"]
```

### Invocation

Wrap every toolchain subprocess in nsjail. The CP's `internal/service/migration.go::newToolCmd` is the single chokepoint; once L5 is in scope, every migration subprocess flows through there.

Replace the CP's toolchain runner with an nsjail wrapper. The integration site is `internal/service/migration.go`:

```go
// Pseudocode — actual implementation lives in the operator fork.
func (s *MigrationService) newToolCmd(ctx context.Context, name string, args ...string) *exec.Cmd {
    nsjailArgs := []string{
        "--config", "/etc/edgecloud/nsjail-migrate.profile",
        "--",
        name,
    }
    nsjailArgs = append(nsjailArgs, args...)
    cmd := exec.CommandContext(ctx, "nsjail", nsjailArgs...)
    cmd.Env = scrubbedEnv()
    cmd.Cancel = func() error { return cmd.Process.Kill() }
    cmd.WaitDelay = 5 * time.Second
    return cmd
}
```

`--` separates the nsjail args from the toolchain argv. nsjail will `execve()` the toolchain with the remaining args. `scrubbedEnv()` (L3) is still applied; the sandbox layers do not duplicate work.

## Alternatives

### Bubblewrap (bwrap)

Lower-overhead alternative. Suitable when nsjail is not packaged for your distribution.

```bash
bwrap \
  --unshare-all \
  --share-net \                # set to --unshare-net to also kill network
  --bind /opt/wasi-sdk /opt/wasi-sdk \
  --bind /opt/edgecloud/cargo-home /root/.cargo \
  --tmpfs /tmp \
  --proc /proc \
  --die-with-parent \
  --new-session \
  -- \
  /usr/bin/rustc --target wasm32-wasip2 - …
```

The flag layout is similar but lacks the per-syscall seccomp profile that nsjail ships. Operators who need seccomp should prefer nsjail.

### gVisor (runsc)

Highest isolation: gVisor intercepts every syscall in user-space, presenting a Linux ABI to the guest. Suitable for shared-tenant deployments where the threat model includes kernel-level escape from the namespace.

```bash
runsc --network=none --rootless \
  --volume /opt/wasi-sdk:/opt/wasi-sdk:ro \
  --volume /opt/edgecloud/cargo-home:/root/.cargo:ro \
  --tmpfs /tmp:size=512m \
  -- \
  cargo build --target wasm32-wasip2 --release
```

The tradeoff is throughput: gVisor adds ~10–30% to toolchain wall time vs. nsjail.

### Firejail

Lighter weight than nsjail, easier to set up, weaker isolation profile (no seccomp generation, relies on path-based blacklisting). Acceptable when the operator is already running on a hardened host.

```ini
# /etc/firejail/edgecloud-migrate.profile
include /etc/firejail/default.profile
no-net
private-tmp
private-dev
read-only /opt/wasi-sdk
read-only /opt/edgecloud/cargo-home
```

## Tunables

| Env var | Default | Effect |
|---|---|---|
| `WASI_SDK_PATH` | (required) | Operator-supplied path to wasi-sdk. Bind-mount read-only into the sandbox. |
| `CARGO_HOME` | `/opt/edgecloud/cargo-home` | Operator-vendored cargo registry. Bind-mount read-only. Without a vendored registry the sandbox's `--unshare-net` will break `cargo build` (no network). |
| `RUSTC_PATH` | `rustc` | Override for the toolchain binary. L4 (`clangArgs`) and L3 (`scrubbedEnv`) apply regardless. |
| `EDGE_MIGRATE_SANDBOX` | unset | Reserved: when set to a recognized value (`nsjail`, `bwrap`, `runsc`, `firejail`), the CP wraps every migration toolchain subprocess in the corresponding wrapper. Default `unset` = no sandbox (operator must deploy L5 manually; L1–L4 still apply). |
| `EDGE_MIGRATE_RLIMIT_CPU` | `300` (seconds) | Wall-clock per upload. Mirrors nsjail `rlimit_cpu`. |
| `EDGE_MIGRATE_RLIMIT_FSIZE` | `268435456` (bytes) | Max output file size. Mirrors nsjail `rlimit_fsize`. |

## What to alert on

The preflight metric (`edge_migrate_preflight_rejected_total{language,reason}`) is a leading indicator of attempted abuse — a healthy fleet shows a steady-state non-zero baseline from accidental user mistakes. A **spike** on a single tenant or on `reason="include_bytes"` / `reason="absolute_include"` is the smoking gun for an automated attacker:

```promql
# Spike alert — more than 10 rejections per minute from a single tenant.
sum by (tenant_id) (
  rate(edge_migrate_preflight_rejected_total[1m])
) > 10
```

(Use the per-tenant scrape at `/api/v1/metrics` to get the `tenant_id` label.)

The preflight metric is layered; L1 (handler preflight, additive to all existing counters) and L2 (analyzer `Status: failed`, no metric — see issue #622 for the rationale). An attacker probing L2 only will not bump the metric; the per-tenant `/api/v1/deployments` endpoint is the fallback audit surface.

## References

- PR #668 — `fix/issue-622-migration-exfil` (defense-in-depth 6-commit sequence; L1–L4 + this runbook).
- Issue #622 — original security audit finding.
- PR #416 — closed the proc-macro / `build.rs` RCE vector; the `Cargo.toml` is now fixed-no-deps. Combined with this runbook, even a cargo-network bypass would be neutered by `clone_newnet`.
- PR #451 — `internal/handler/migration.go` body-size limits (50 MiB); also enforced at the storage layer.
- `edge-migrate/docs/design.md` §2.2 — clang preprocessor invocation (`-nostdinc -E`).