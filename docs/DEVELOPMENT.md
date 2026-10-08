# Development — BOOT-001

## Scope

Exactly two workspace members: `crates/domain` (library) and `apps/radar`
(binary). Both use only the standard library; there are no third-party or
path dependencies. `domain` intentionally exports no proposed trading API.
SPEC-001 owns those contracts. No exchange, market data, levels, signals,
recording, replay, account, or order execution is implemented.

The binary prints one status line and exits. `shadow` is just an explicit
safe label in this skeleton: it does not connect to anything or produce signals.
No configuration file, environment variable, feature, or accepted argument can
enable live execution. No runtime/test code performs network requests.

## Toolchain and environment

`rust-toolchain.toml` pins Rust **1.98.1**, profile `minimal`, with `rustfmt` and
`clippy`. This is a deliberately fixed stable release, not a floating `stable`
or a claim to use the newest release. Its official release announcement, checked
on 2026-10-05, is https://blog.rust-lang.org/2026/09/03/Rust-1.98.1/ .
It includes the 1.98.0 vtable miscompilation fix. Real installation and execution
were then verified in CI; an announcement alone is not execution evidence.

Worker environment: Debian GNU/Linux 13.3, Linux x86_64, Bash 5.2.37, Git 2.47.3.
Rust/Cargo/rustup/rustfmt/clippy are absent. GitHub DNS fails from the worker
container, so no local checkout or Rust execution is claimed. Changes are made
through the GitHub connector; local source staging is not a repository checkout.

CI is configured for the GitHub-hosted `ubuntu-24.04` Linux runner. Each job logs
`/etc/os-release`, architecture, Git, the exact checked-out SHA, active toolchain,
Rust/Cargo/rustfmt/Clippy versions, and the Cargo executable path. Use those logs
for the actual runner image and tool versions; the label alone is not proof.

Observed in CI run 37299372900 (2026-10-05): Ubuntu 24.04.5 LTS, x86_64,
runner image `20260927.320.1`, Git 2.55.0; Rustup installed and selected
`1.98.1-x86_64-unknown-linux-gnu`; rustc `1.98.1 (48a229cea 2026-09-01)`,
Cargo `1.98.1 (797e8a9bc 2026-08-05)`, rustfmt `1.9.0-stable`,
Clippy `0.1.98`, LLVM `22.1.8`. Actual setup and generated lockfile log:
https://github.com/al-gri/pro-sclpng/actions/runs/37299372900/job/111728177941 .
This first run was not an acceptance PASS; see the handoff for its failures.

All three checks subsequently passed on source SHA
`b2bf5470bbd2d15d96f4e08c881fd81f85f68214` in run 37299608362, including
15 real-binary integration tests and unchanged tracked files:
https://github.com/al-gri/pro-sclpng/actions/runs/37299608362 .
The final head after the documentation commit and its fresh check results are
recorded in PR #9; the earlier run is not substituted for that final-head check.

## Prepare a real development checkout

From a separate clean checkout of this repository at the SHA being tested, with
Git, Rustup, and the platform linker installed:

```sh
git rev-parse HEAD
git status --porcelain
rustup toolchain install 1.98.1 --profile minimal --component rustfmt --component clippy
export RUSTUP_TOOLCHAIN=1.98.1
rustup show active-toolchain
rustc --version --verbose
cargo --version
```

Require `rustc` release `1.98.1`. The explicit environment setting prevents a
pre-existing directory override from selecting another toolchain. Check it,
rather than inferring the active compiler from the TOML file:
https://rust-lang.github.io/rustup/overrides.html .
CI reads the pin from TOML and checks the real compiler before running checks.

## CLI contract

```sh
cargo run --locked -p radar --
cargo run --locked -p radar -- --mode offline
cargo run --locked -p radar -- --mode shadow
```

Successful output is exactly `mode=offline execution=disabled` or
`mode=shadow execution=disabled`, followed by a newline; stderr is empty and the
binary exits 0. The default is offline. `--mode live`, any other mode, missing
mode value, `--live`, unknown/positional arguments, `--mode=value`, and any repeat
of `--mode` exit 2, write an error plus usage to stderr, and leave stdout empty.
There is no fallback from invalid input to a safe-looking successful launch.
Even non-Unicode OS arguments are rejected without a panic. Output I/O failure
exits 1, rather than panicking.

After building, invoke `./target/debug/radar` directly to inspect the binary's
exit status without a Cargo wrapper. The integration tests use
`CARGO_BIN_EXE_radar`, execute the real binary, and assert status/stdout/stderr.
These tests cover the bootstrap CLI and INV-01, not trading strategy correctness.

## Acceptance checks

Run from the workspace root with the verified toolchain:

```sh
export CARGO_NET_OFFLINE=true
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
git diff --exit-code
git diff --cached --exit-code
git status --porcelain
```

Require an empty final status and record each command's exit code and tested SHA.
A missing, skipped, cancelled, or unexecuted check is not PASS. See
`handoffs/BOOT-001.md` and the PR for actual evidence.

`Cargo.lock` must come from the pinned Cargo, not a hand-written approximation:

```sh
cargo generate-lockfile --offline
git diff --exit-code -- Cargo.lock
```

The initial CI bootstrap run 37299372900 generated and printed the lockfile using
Cargo 1.98.1 at source SHA `10c42763c25539c990cf21df3367a745a533fba1`.
Its tracked-lockfile gate failed because the generated file was not committed yet.
The exact generated bytes were then copied from that log into `Cargo.lock`.
Every subsequent run regenerates it and compares against the tracked file before
acceptance build/tests; no hand-written dependency resolution is substituted.
Cargo documentation: https://doc.rust-lang.org/cargo/commands/cargo-generate-lockfile.html .

## CI and security boundaries

`.github/workflows/ci.yml` has three independent checks: `rust-fmt`,
`rust-clippy`, `rust-tests`. The last runs both build and tests. Events are
`pull_request` targeting `main` and `push` to `main`. Every job fetches only
`https://github.com/al-gri/pro-sclpng.git`, checks out the PR's
`github.event.pull_request.head.sha` (or push SHA), and asserts the actual HEAD.
The PR run's event/synthetic merge SHA is not reported as the tested source SHA.

The workflow uses shell `run` steps only, no external Actions, secrets, checkout
credentials, deployment, services, `pull_request_target`, or write permissions.
Repository visibility must remain public for this unauthenticated checkout;
if it becomes private, checkout fails rather than introducing credentials.
Repository settings and required checks are not changed by this work.

Source fetching and Rust toolchain installation need network during CI setup.
Cargo is configured offline afterwards; that alone is not a network sandbox.
Absence of runtime/test network calls also requires source/dependency review.
No binary or integration test accesses the network. A green build makes no
performance, profitability, market-data, or live-trading claim.

Official workflow references:
- https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax
- https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows

## REC-001F-1 offline diagnostic replay

This section describes the bounded replay addition; the BOOT-001 notes above
record the earlier bootstrap state. The `radar` package now also builds the
`radar-replay` binary with local `domain`, `market-data` and `recording`
dependencies. `default-run = "radar"` preserves existing `cargo run -p radar`
behavior. No workspace member or third-party dependency is added.

```sh
cargo run --locked -p radar --bin radar-replay -- \
  --wal tests/fixtures/replay/synthetic-v1.wal --profile synthetic-rec001f1-v1
```

The executable reads one offline, single-segment WAL using the accepted reader,
decoder and DataHealth reducer. Receive order, lexical payloads, recorded
monotonic samples, Timer fields and supported controls are preserved. Structural
records receive explicit Noop steps. The initial synthetic definitions and
configuration must exactly match the pinned profile; the optional inactive
specification version 2 is only a diagnostic descriptor and is not activated.
Archive admission is capped at 8 MiB, 256 records and two declared streams;
the diagnostic profile supports its one exact stream.

`--help` exits 0. Malformed CLI, unknown/repeated/missing arguments, live options
and unknown profiles exit 2 with stderr. Physical incomplete/corrupt/unsupported
inputs, profile mismatch, cap exhaustion and unsupported semantic controls
return nonzero with explicit diagnostics. The existing reader does not skip,
repair or rejoin corrupt suffixes. Semantic blocking may leave a separately
scanned physical suffix, which is not described as semantic replay.

Later configuration, SpecActivate and Verification/Warmup/Freshness proof
application block dependent projection before that record. There is no
production artifact verifier. Initial descriptor applicability remains
`BLOCKED_UNVERIFIED`/unresolved; only the matched pinned synthetic diagnostic
profile may exit 0 with these unresolved initial descriptors. No proof is
fabricated, pending frames do not become usable, and physical Complete does not
prove a canonical book. Observed Recording Failed remains terminal after a
later Healthy observation; parsed evidence is not a trusted StorageFence or
publication permit. Timer fields do not reconstruct scheduler authorization or
physical Ping/Close delivery. U-09/U-10 and regular snapshot-zero semantics
remain unresolved, and no quantity normalization or level mutation occurs.

### Rebuild and compare the fixture

Run from the repository root on Unix with the verified pinned toolchain and an
Integrator-composed lockfile. `mktemp` supplies a fresh directory; the builder
uses create-new and refuses an existing destination. Its explicit seals precede
`WalWriter::finish`. The existing finish implementation synchronizes the file
and parent directory on Unix. Windows parent-directory synchronization is
unsupported and the builder returns an error; Windows is not a substitute for
a successful Unix builder run. Offline replay itself uses no live-time or
network reads.

```sh
replay_tmp="$(mktemp -d)"
cargo run --locked -p radar --example build_replay_fixture -- \
  --output "$replay_tmp/synthetic-v1.wal"
cmp tests/fixtures/replay/synthetic-v1.wal "$replay_tmp/synthetic-v1.wal"
sha256sum -c tests/fixtures/replay/SHA256SUMS

cargo run --locked -p radar --bin radar-replay -- \
  --wal tests/fixtures/replay/synthetic-v1.wal \
  --profile synthetic-rec001f1-v1 > "$replay_tmp/replay-1.txt"
cargo run --locked -p radar --bin radar-replay -- \
  --wal tests/fixtures/replay/synthetic-v1.wal \
  --profile synthetic-rec001f1-v1 > "$replay_tmp/replay-2.txt"
cmp "$replay_tmp/replay-1.txt" "$replay_tmp/replay-2.txt"
cmp tests/fixtures/replay/synthetic-v1.expected.txt "$replay_tmp/replay-1.txt"
sha256sum "$replay_tmp/replay-1.txt" "$replay_tmp/replay-2.txt"

cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test -p radar --test replay --locked
```

These are verification commands, not a claim that a particular SHA passed them.
The real-binary tests exercise deterministic output, CLI and profile guards,
recorded health transitions and physical corruption/recovery boundaries. The
fixture README and manifest pin synthetic provenance, exact raw-source hashes,
engineering policy and byte offsets. Preserve accepted raw Git blob bytes,
including LF line endings, when rebuilding; checkout newline conversion would
change the embedded payloads and derivative archive hash.

### Lock integration and review

Root `Cargo.lock` belongs to the Integrator. The worker prepares the permitted
code/fixtures and a separate exact Cargo-generated proposed lock delta, then
opens one Draft PR with status `DRAFT_FOR_LOCK_INTEGRATION`. Checks on a separate
composed candidate apply to that candidate only. The Integrator commits the
lockfile sequentially in the same branch. Final locked checks and
`READY_FOR_INDEPENDENT_QA` require the actual final head after that lock commit;
fresh CI and independent QA must inspect that exact SHA. Draft readiness does
not claim merge approval, canonical readiness, full REC-001F completion or M1
completion. Exact executed commands, outcomes and limitations belong in the
task handoff.
