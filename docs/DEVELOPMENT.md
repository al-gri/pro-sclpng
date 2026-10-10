# Development — BOOT-001

Active role/lifecycle authority is [WORKFLOW](WORKFLOW.md): Integrator orchestration, one ephemeral Worker, exact FINAL_SHA/TREE SOURCE_FROZEN, then separate independent QA. The dated implementation/baseline commands and historical readiness labels below retain their original scoped meaning; they do not replace the current freeze/QA lifecycle or transfer PASS to another executor/head.

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

## REC-001F-2 owner-bound scripted capture

This separate synthetic example composes the existing capture owner and public
supervisor with a synchronous scripted fake transport. Its output is a real
filesystem WAL read back by the existing `WalReader`. The example adds no live
socket, TLS, DNS, live clock, signal handler, async runtime or production verifier.
The accepted F-1 replay profile, fixtures and commands above remain applicable to
F-1. F-2 uses a separate synthetic profile with supervisor venue token `bitget`;
F-1 pins `Bitget` and does not support the raw subscription ACK. F-2 promises
reader round-trip diagnostics, not full F-1 replay compatibility.

The worker checked committed code head `fa4c3ab5122dd7916a4f7a3ac1c0896f69d270b1`
on Linux: app debug/release17, workspace577 (566 runtime+11 compile-fail), accepted
F-1 replay24 and focused Timer/QA42 filters passed. The Handoff retains those
actual code-head checks. The final docs-containing head, repeated checks, fresh
CI and accessible evidence identities are recorded in Issue #37 / its Draft PR
after all commits; no prior-head result is silently transferred.

Build and run from the repository root on Linux with verified Rust/Cargo 1.98.1.
Use a Linux filesystem for Durable file and parent-directory synchronization.
The worker executor is the `rec-001f-2-worker` Docker Linux container using the
existing `rust:1.98.1-bookworm` image and an overlayfs filesystem. Rust 1.98.1
and its rustfmt/Clippy components are installed and verified there. Worker builds use
`CARGO_TARGET_DIR=/tmp/rec-f2-target`; generated WAL and evidence belong in fresh
directories under `/tmp`, outside Git. Exact environment values and image identity
belong in the evidence receipt.

Use a clean Linux checkout with `core.autocrlf=false`. Accepted F-1 fixture JSON
must retain exact Git blob/LF bytes: Windows newline conversion changes embedded
raw bytes and breaks its golden comparison without a Git source delta. The
worker retained that failed attempt and reran the same head in canonical Linux
source; no excluded fixture was edited.

```sh
export RUSTUP_TOOLCHAIN=1.98.1
rustc --version --verbose
cargo --version
rustup show active-toolchain
cargo build --locked -p radar --example capture_scripted
capture_binary="${CARGO_TARGET_DIR:-target}/debug/examples/capture_scripted"
capture_check_dir="$(mktemp -d /tmp/rec-001f-2.XXXXXXXX)"
"$capture_binary" --scenario nominal \
  --output "$capture_check_dir/nominal.wal"
```

The CLI accepts exactly one scenario and one fresh output path:

```text
capture_scripted --scenario nominal|heartbeat-pong|heartbeat-timeout|transport-error|overflow|write-error|shutdown-refused --output <fresh-path.wal>
```

| Scenario / input | Purpose | Actual code-head exit |
| --- | --- | --- |
| `nominal` | Connect, subscription ACK, exact original books bytes in receive order, healthy finalization | `0` |
| `heartbeat-pong` | Library Ping/Pong scheduling using controlled original stamps | `0` |
| `heartbeat-timeout` | Library Timeout -> Down -> the same mandatory Close; diagnostic terminal outcome | `2` |
| `transport-error` | Preserve callback attempts, ambiguous effect and original Close recovery | `2` |
| `overflow` | Terminal item/work exhaustion with truthful failure and unresolved ownership | `2` |
| `write-error` | Public BeforeWrite fault through the real bound sink, terminal StorageStopped | `2` |
| `shutdown-refused` | Previously admitted Raw fails in Closing; finalization is invalidated | `2` |
| Invalid arguments / unknown scenario / non-Unicode values / bounded path | Reject input without panic or created output | `3` |
| Existing output path | Create-new refusal; existing bytes stay unchanged | `3` |

The nominal direct process must leave stderr empty. Negative scenarios must
return their documented nonzero status with truthful diagnostics. A wrapper must
capture each actual exit code and check it against the recorded expectation;
`|| true` is not a validation result. Do not assume `CARGO_BIN_EXE_*` exists for
an example; invoke the example executable produced by the explicit build.

All metadata, IDs, inputs, configuration and stamps are synthetic/unverified.
The F-2 profile uses an explicitly synthetic repeated `f2` digest token; it is
local engineering identity, not authenticated Bitget provenance.
Every report retains `synthetic=true`, `canonical_status=NotEvaluated`,
`canonical_applicability=BLOCKED_UNVERIFIED` and `usable_data=false`. A physically
Complete healthy archive has owner input quality Unknown. Missing evidence does
not become VALID. U09/U10/delete remain BLOCKED, U20 REST healing is FORBIDDEN,
C01 is BLOCKED and C03 is UNKNOWN; #22, #5 and full M1 remain open.

The application must use the existing owner registration, genuine affine leases,
bound sink and dispatch path. The library owns Timer identity, generation and
deadlines under SupervisorV2 revision 2 (30,000,000,000 ns Ping interval and
15,000,000,000 ns Pong timeout). Original recorded stamps determine deadlines;
dispatch time never moves them. A local stop uses same-authority mandatory Close
and lawful reclaim/dispatch. It does not invent a received Disconnected input.
Timer-only progress cannot make an active Timeout Close ready. Drop is not a
successful receipt or settlement, and an ambiguous Close retry does not promise
exactly-once physical effects. StorageStopped denies write retries and final
seals; diagnostic closure preserves unresolved owners truthfully.

Demo limits are at most 64 observations, 64 effect entries, 4096 bytes per
payload, 64 KiB total scripted raw bytes, 256 drain/quiescence/reclaim steps and
two effect attempts per Close identity. Arithmetic and exhaustion must be checked.
Separate tests distinguish bounded coalesced raw/frame/byte/message loss from
terminal item/work exhaustion: every overflow does not imply failed latch/Close.

### Repeatable direct-process capture and negative checks

After the example build, use two distinct fresh output paths in separate direct
processes. Canonical JSON excludes paths, PID, randomness, live time, durations
and OS error strings; environment details belong in the evidence archive.

```sh
"$capture_binary" --scenario nominal \
  --output "$capture_check_dir/one.wal" > "$capture_check_dir/one.json" \
  2> "$capture_check_dir/one.stderr"
"$capture_binary" --scenario nominal \
  --output "$capture_check_dir/two.wal" > "$capture_check_dir/two.json" \
  2> "$capture_check_dir/two.stderr"
cmp "$capture_check_dir/one.wal" "$capture_check_dir/two.wal"
cmp "$capture_check_dir/one.json" "$capture_check_dir/two.json"
test ! -s "$capture_check_dir/one.stderr"
test ! -s "$capture_check_dir/two.stderr"
sha256sum "$capture_check_dir/one.wal" "$capture_check_dir/two.wal" \
  "$capture_check_dir/one.json" "$capture_check_dir/two.json"
"$capture_binary" --scenario heartbeat-pong \
  --output "$capture_check_dir/pong.wal"
"$capture_binary" --scenario heartbeat-timeout \
  --output "$capture_check_dir/timeout.wal"
```

Run each negative scenario separately with a fresh path and preserve stdout,
stderr and the actual exit status. Repeat negative scripts to compare normalized
outcomes. For the existing-path check, save its size/hash before the refusal and
require byte/hash equality afterward. Unknown-scenario and bounds checks must
reject without panic. Corruption probes mutate separate copies of a genuinely
owner-created WAL: middle CRC, torn header/payload, removed ArchiveSeal and bytes
after ArchiveSeal. Compare existing reader status/error/good prefix; retain the
original archive and never skip, repair or rejoin a corrupt suffix.

### Final-head checks and evidence

Run every command on the actual committed implementation head, with a clean
checkout and the verified pin. Focused filters must select real tests.

```sh
git rev-parse HEAD
git rev-parse 'HEAD^{tree}'
git status --porcelain
rustc --version --verbose
cargo --version
rustup show active-toolchain
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --workspace --locked
cargo test --workspace --locked
cargo test --locked -p radar --test capture_driver
cargo test --locked --release -p radar --test capture_driver
cargo test --locked -p radar --test replay
cargo test --locked -p market-data --test ws_supervisor timer_a_
cargo test --locked -p recording --test capture_session timer_a_
cargo test --locked -p recording --test capture_session independent_qa_42_
cargo build --locked -p radar --example capture_scripted
git diff --check 8b251e1ef09354a1a205fa152ce7726ea5f5749b HEAD
git diff --name-only 8b251e1ef09354a1a205fa152ce7726ea5f5749b HEAD
git diff --exit-code
git diff --cached --exit-code
git status --porcelain
```

Verify lock equality separately: materialize a clean verification copy of that
final head, run `cargo generate-lockfile --offline` there and require
`git diff --exit-code -- Cargo.lock`. Do not regenerate or commit excluded Cargo
files in the worker checkout. Any necessary dependency/manifest delta requires
the Integrator's explicitly agreed sequential integration and new final-head
checks.

The evidence ZIP must be accessible to an independent QA worker and include raw
command logs, fresh WAL/stdout/stderr, negative outcomes, actual HEAD/tree/lock
hash, source inventory and a manifest/receipt with sizes and SHA256. Verify ZIP
and manifest integrity. A local Windows path is not an accessible evidence
artifact. Publish the final receipt in the existing PR/Issue after all commits,
then verify the three fresh CI jobs and their actual logs on the same head.
Independent QA and owner acceptance are separate gates.
