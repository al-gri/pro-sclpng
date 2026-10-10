# Executor capabilities — 2026-10-10

This is an executor-specific snapshot, not a shared capability promise. Repository scope remains only `al-gri/pro-sclpng`. The owner's Windows local Codex executor and the Linux executor of this chat are separate environments. A worker must name its actual executor, recheck tools/access and record exact source SHA before claiming a test result.

Windows facts below come from the owner's 2026-10-10 report. Its raw commands/logs were not independently replayed by this chat; PASS in that column means **owner-reported PASS**. The Linux column comes from current local probes and GitHub actions actually performed by Integrator in this chat. Historical NOT_RUN statements describe their original sessions and remain unchanged.

| Capability / evidence | Owner's Windows local Codex executor — reported | Current chat Linux executor — observed |
|---|---|---|
| Shell | PowerShell 7.6.5 available. Probe executables with `Get-Command`, not `which`. | Bash 5.2.21 available; `pwsh` ABSENT. |
| Git / GitHub CLI read | Git and `gh` available; GitHub read PASS. | Git 2.51.1 available; `gh` ABSENT. Shell Git remote read of the allowed repository PASS. |
| GitHub permissions / actual write | Reported `pull=true`, `push=true`; actual write NOT_TESTED in that executor. Permissions do not prove a successful write. | Connector read/write exercised: Issues #40–#42 and Draft PR #43 created by Integrator. Shell push/Windows `gh` write are not implied. |
| Native Rust / Cargo | Rust/Cargo 1.99.0 available, plus pinned 1.98.1 with rustfmt/Clippy. Supplied executable path is exactly `C:\Users\user.cargo\bin`; this snapshot preserves the owner's spelling rather than silently correcting it. | `rustc`, `cargo`, `rustup` ABSENT in PATH; no local Rust build/fmt/Clippy/test/release run. |
| Docker / Linux Rust runner | Docker Desktop engine 29.3.1; Linux `rust:1.98.1-bookworm` runner PASS. The reported run required permitted exit from its sandbox. | `docker` ABSENT; `/var/run/docker.sock` ABSENT. Access to the Windows Docker daemon or runner is NOT_PROVEN. |
| Exact-base disposable Docker verification | At `8baf610b4ac15122dfcdbfe07730bc30c6558d73`, owner reports offline fmt/Clippy/build/test exit 0, 577 passing tests including 11 expected compile-fail, and clean checkout PASS. This is separate from CI and is not a run of a future PR head. | Rust/Docker checks NOT_RUN. This chat has not independently reproduced the reported Docker commands/logs. |
| Live public endpoint / real capture | No live-capture PASS established by this capability report; task-specific live checks remain NOT_RUN until executed. | Live socket/TLS/capture checks NOT_RUN; neither connector access nor Git remote read establishes Bitget access. |

Current Linux probes used executable discovery, `bash --version`, `git --version` and Docker socket existence. `gh`, `rustc`, `cargo`, `rustup`, `docker`, `pwsh` and `/var/run/docker.sock` were absent. Integrator's connector mutations and remote Git read are separate checks; no credentials are recorded here.

## Reported exact-base Docker commands

The owner reports these actual runs in one disposable Linux container at `8baf610b4ac15122dfcdbfe07730bc30c6558d73`, with Rust/Cargo 1.98.1 and `CARGO_NET_OFFLINE=true`. Raw logs were not attached to this report; this chat does not invent command output or reattribute the runs to itself.

| Command / check | Owner-reported result |
|---|---|
| `cargo fmt --all -- --check` | PASS, exit 0. |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | PASS, exit 0. |
| `cargo build --workspace --locked` | PASS, exit 0. |
| `cargo test --workspace --locked` | PASS, exit 0; 577 tests including 11 expected compile-fail doctests. |
| Clean checkout after execution | PASS. |

The ordinary Windows sandbox restricted Docker and shell-network access; execution succeeded after a permitted sandbox exit. The repository was unchanged; no merge or auto-merge was performed in that reported run. This does not grant the current chat Docker access or permission to change sandbox policy.

## Rules for the next task packet

- Select and name the executor before implementation or QA. Record shell/OS/architecture, exact base/head, available binaries, active pinned Rust/Cargo versions, dependency/cache preparation and commands with exit codes. Do not inherit the Windows Docker PASS into this Linux chat or transfer existing-base results to a changed head.
- On Windows, use `Get-Command` for discovery and verify the supplied Rust path on that runner. Explicitly select the repository's pinned 1.98.1; native 1.99 availability does not select it automatically. If Docker is selected, use a disposable clean exact-head checkout and verify the actual Linux image/toolchain and mounts.
- New network dependencies need a separate preparation stage. Integrator verifies versions/features/provenance and generates root Cargo.lock sequentially. On the chosen Windows/native or Docker runner, fetch locked dependencies with online preparation permitted (for example `CARGO_NET_OFFLINE=false cargo fetch --locked` in the runner's appropriate shell), or use independently verified vendoring. Enable offline mode only after the required locked registry/git dependencies are prepared in the cache mounted into that exact runner. A Docker image alone does not prove such cache preparation.
- The existing CI has no `cargo fetch` and sets offline mode. Add explicit clean-runner dependency preparation before offline generated-lock equality, locked build, fmt, all-target Clippy, tests and clean checks when external dependencies are introduced. Do not treat a warm cache as reproducible CI evidence.
- Preserve historical handoffs/receipts, including their valid NOT_RUN statements. Record newer executor capability or exact-head runtime results in a new dated receipt; do not rewrite old evidence. Independent QA still verifies its own concrete final head, and absent live access remains NOT_RUN.

This snapshot changes no permissions, settings, toolchain, code, CI workflow, accepted contracts or milestone status. Full M1 and data usability are not accepted by tool availability or the reported baseline run.

## Separate orchestration observation — GOV-ORCH-001, 2026-10-10

The current ChatGPT Work executor exposes real subagent orchestration. Integrator actually spawned a separate read-only process reviewer for #48; independent frozen-head QA is a later sequential role. AUTOSPAWN_CAPABILITY=AVAILABLE_IN_CURRENT_ENVIRONMENT. This observation does not transfer any historical Windows/Linux runtime PASS, establish Rust/Cargo/Docker/cache/network/live access, or change the pinned toolchain/technical gates. Worker and QA each record their actual environment and exact refs. Normal implementation goes to one ephemeral Worker; explicitly scoped governance/shared integration exceptions remain Integrator-owned under [WORKFLOW](WORKFLOW.md). Unavailable orchestration in another executor requires the honest saved full-prompt fallback; available orchestration must be used.


## Active policy note — balanced revision, 2026-10-10

The observations above retain their original executor/date and do not describe every later chat. Current role and activation authority is [WORKFLOW](WORKFLOW.md). Name START/HEAD/TREE/TARGET, actual executor, clean checkout and relevant environment; full preflight repeats when executor/toolchain/access changes. STANDARD uses a fresh independent targeted reviewer, STRICT keeps required independent QA. Current #48/PR49 itself still needs independent documentary QA and existing CI.

Fresh context and filesystem isolation must be verified separately. The currently available spawn_agent mechanism requires explicit `fork_turns="none"` to avoid inherited transcript; another environment may use a different mechanism. Subagents can share a workspace: review/reproduction uses a pinned isolated copy and does not change the delivery branch. Shared/lock/CI ownership is assigned to one executor in a bounded packet, respecting existing claims until explicit transfer; historical ownership statements above remain scoped to their original tasks. Orchestration availability does not prove Rust/network/cache/live capability. Goal/scheduled continuation and user-owned chat creation/messaging are separate capabilities/permissions, not implied by spawn.
