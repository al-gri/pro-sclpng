# Handoff: SPEC-001 — implementation in progress

Status: **PARTIAL / NOT_READY_FOR_QA**. All new contracts and ADR remain **PROPOSED**.
Repository: al-gri/pro-sclpng only. Same worker, Issue3, branch and Draft PR10.
[Packet](https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5994665559) · [Existing claim](https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5995282901) · [PR10](https://github.com/al-gri/pro-sclpng/pull/10).
Base/main: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Branch: `feat/SPEC-001-domain-contracts`.
[Implementation approval5418412674](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5418412674) applies to design `272f6ec50cd0df3630f37ef99cb8b3bb54a967d7`.
Input implementation head of this continuation: `c393439dc90ec381479889303f415d8725da68d8`.
The containing/final head and its actual CI evidence are recorded POST-COMMIT in PR10, not self-referentially in this file.

## Current implementation, not a design-only checkpoint

Five implementation commits are preserved after design approval: numeric4a7ab227, qualified/identity a57752f6, envelope/artifact/policy4478454a, DataHealth e99ab818, accounting/publication/shared tests c393439d. No rollback or history rewriting.

Public `crates/domain/src/` modules: numeric, identity, qualified, artifact, event, policy and record DTOs. Exact arithmetic and unit/reference constructors; source/application/control IDs and causal guards; UNKNOWN; artifact-ref grammar and identity/dependency metadata; policy tags; recorded-input shapes. No byte codec or production storage/hash/verifier in the library.

`crates/domain/tests/support/` contains the explicitly synthetic resolver, caller-owned immutable prefix, bounded pending DataHealth reference transitions, barriers, proof dedup/reorder, original-sample freshness, warm-up cap/freeze, local loss accounting and candidate/fence guards. These test-local relations do not establish real Bitget or physical storage facts.

At input c393, executable source inventory is 95 domain test functions: numeric18, identity9, contracts68. They all actually ran in the verified input-head test job below. Counting functions is NOT a complete vector-coverage audit. No ignored tests are used.

## Evidence correction — historical error remains visible

The preceding [PARTIAL report6001682147](https://github.com/al-gri/pro-sclpng/pull/10#issuecomment-6001682147) and PR description incorrectly cited run37363552027. A fresh GET returned HTTP404. This was an error in my report; the historical comment is retained, not silently rewritten.
[Correction6001945991](https://github.com/al-gri/pro-sclpng/pull/10#issuecomment-6001945991) records the independent recheck.

The actual exact-c393 run is [37363320459](https://github.com/al-gri/pro-sclpng/actions/runs/37363320459), completed/failure:

| Job | Actual result | Evidence limit |
|---|---|---|
| rust-tests111942752010 | completed/success | Full decoded log read; source checkout c393 verified |
| rust-clippy111942752173 | cancelled; no steps | NOT_RUN/no lint verdict from this job; cancellation cause not established |
| rust-fmt111942752247 | cancelled; no steps | NOT_RUN/no formatting verdict from this job; cancellation cause not established |

The [rust-tests log](https://github.com/al-gri/pro-sclpng/actions/runs/37363320459/job/111942752010) confirms Expected SHA=Checked out SHA=`c393439dc90ec381479889303f415d8725da68d8`, Ubuntu24.04.5 LTS/Linux x86_64, Rust/Cargo1.98.1. `cargo build --workspace --locked` and `cargo test --workspace --locked` succeeded (exit0); contracts68+identity9+numeric18=95 domain passed, separately15 CLI passed, zero failed/ignored. Cargo-generated lockfile comparison and initial/final clean checkout succeeded. Domain unit/doc binaries contain0 tests; integration binaries contain the actual contract tests.

Earlier e99 run37360972282 had real fmt/Clippy failures; that must NOT be transferred as a lint verdict to c393. Conversely successful c393 tests must NOT be used as final-head or lint evidence.

## Fresh local preflight

AGENTS, full approval, last PARTIAL comment and current main/head/discussion were read; main/head matched expected and no newer branch work was found. Shell/Git checked afresh: Linux x86_64, Git2.47.3, Python3.13.5; rustc/cargo/rustfmt/clippy-driver/rustup absent. `git ls-remote` on the permitted repository failed DNS github.com, exit128. No local checkout. API authoring/staging is not one.

All local Rust checks are **NOT_RUN**. Existing CI can execute pinned checks; standalone `cargo test -p domain --locked` is not in its workflow and remains a separate **verification blocker / NOT_RUN** until an authorized environment runs it. Windows11/PowerShell5.1 is **NOT_RUN**. Linux success is not Windows success. No empty commits, lint weakening or workflow changes are authorized.

## Current vector mapping and concrete gaps

All paths below are relative to crates/domain/tests. These groups describe inspected input-head coverage; final per-vector audit is still required.

| Group | Test files | Input-head execution / remaining gap |
|---|---|---|
| N01–N21 and bounded grids | numeric.rs; identity.rs | PASS on c393; full scalar/unit/reference assertions |
| E identities/order/UNKNOWN/causality | identity.rs; cases/events.rs | PASS on c393; audit every normative subcase before claiming full coverage |
| R4 parsing/metadata/dependency guards | cases/artifacts.rs; cases/events.rs | PASS on c393; binary descriptors/commitments and AF fixtures still incomplete |
| R1 delayed/current/old/duplicate proofs; R3 time/quiet; C2 | cases/health.rs | PASS on c393; not production feed verification |
| R5 accounting | cases/accounting.rs | PASS on c393; must also exercise through WAL byte recovery |
| R2 mode/receipt/candidate/fence | cases/policy.rs; cases/publication.rs | PASS on c393; completion explicitly synthetic |
| R6 shared transport | cases/shared.rs | PASS on c393; expanded recorded setup; exact trace mapping retained in PR |
| W00–W20, full WAL/rotation/recovery | tests/support codec and binary test cases not complete | **NOT_IMPLEMENTED / NOT_RUN** for missing byte-level parts, not fulfilled by DTOs |
| V2 Gap/order/reversed/policy binary vectors | typed-policy tests exist; complete frame/descriptor tests missing | **PARTIAL**; correct negative CRC/digests and exact offsets still required |

## Remaining implementation work / exact continuation

1. Finish test-local memory-only encoding/decoding for all agreed records; caps/checked offsets, canonical primitive tags and exact EOF. Compose reference/order/definition/GAP/seal checks without filesystem I/O.
2. Freeze independent multi-frame and multi-segment raw/control/GAP/seal goldens outside the tested encoder. Assert every byte cut, corrupted middle, removed whole frame/seal/final segment and trailing data. Round-trip alone is insufficient.
3. Complete PSAD/PSAM/PSCO memory helpers and literal AF assertions. No SHA implementation/dependency: synthetic digest observations remain explicitly test evidence, ParsedRef remains unverified without supplied byte/applicability facts.
4. Add actual positive/reversed Gap bytes, own valid negative CRC, exact scope error at56, policy tag sets/mirrored mismatch and no ordinal cast tests.
5. Finish a committed vector ID -> file -> function -> checked outcome -> result/gap inventory. Do not invent coverage for unimplemented vectors. Run real final-head checks and record diagnostics; only repair demonstrated fmt/Clippy issues, without suppression or changing expected semantics.

No new A1–A3 design review is needed within the approved scope. A real contradiction needs a minimal counterexample to Integrator and blocks only the dependent part.

## Final verification requirements

On the containing final implementation head: `cargo build --workspace --locked`; `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings`; `cargo test --workspace --locked`; separately `cargo test -p domain --locked`.
Record exact checkout SHA, OS/target/toolchain, command exit codes, each binary count, run/job URLs, required steps, lockfile/index cleanliness. No final PASS is claimed in this in-progress handoff. Implementation acceptance still requires independent QA and owner acceptance.

## PowerShell5.1 owner commands — NOT_RUN

Use a separate clean checkout of the exact final SHA recorded in PR, with Rust1.98.1/linker/rustfmt/Clippy installed. These commands have NOT been run on Windows.

```powershell
$ErrorActionPreference = 'Stop'
$env:RUSTUP_TOOLCHAIN = '1.98.1'
$env:CARGO_NET_OFFLINE = 'true'
$ExpectedHead = 'COPY_FINAL_HEAD_FROM_PR'
if ($ExpectedHead -notmatch '^[0-9a-f]{40}$') { throw 'Set exact PR SHA' }
$actual = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw 'git rev-parse failed' }
if ($actual -ne $ExpectedHead) { throw 'Wrong SHA' }
$status = git status --porcelain
if ($LASTEXITCODE -ne 0) { throw 'git status failed' }
if ($status) { throw 'Dirty checkout' }
$active = rustup show active-toolchain
if ($LASTEXITCODE -ne 0) { throw 'rustup failed' }
Write-Output $active
if ($active -notmatch '^1\.98\.1-') { throw 'Wrong toolchain' }
rustc --version --verbose
if ($LASTEXITCODE -ne 0) { throw 'rustc failed' }
cargo --version
if ($LASTEXITCODE -ne 0) { throw 'cargo version failed' }
cargo build --workspace --locked
if ($LASTEXITCODE -ne 0) { throw 'build failed' }
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { throw 'fmt failed' }
cargo clippy --workspace --all-targets --locked -- -D warnings
if ($LASTEXITCODE -ne 0) { throw 'clippy failed' }
cargo test --workspace --locked
if ($LASTEXITCODE -ne 0) { throw 'workspace tests failed' }
cargo test -p domain --locked
if ($LASTEXITCODE -ne 0) { throw 'domain tests failed' }
git diff --exit-code -- Cargo.lock
if ($LASTEXITCODE -ne 0) { throw 'Lockfile changed' }
git diff --exit-code
if ($LASTEXITCODE -ne 0) { throw 'Tracked files changed' }
git diff --cached --exit-code
if ($LASTEXITCODE -ne 0) { throw 'Index changed' }
$status = git status --porcelain
if ($LASTEXITCODE -ne 0) { throw 'Final git status failed' }
if ($status) { throw 'Final checkout dirty' }
```

## Boundaries and transfer

Allowed paths and std-only/Rust1.98.1 retained; no new dependencies/crates/features/build scripts. Root Cargo.toml/lock/toolchain/CI/apps/radar/PROJECT_STATE/specs registry/accepted ADR-0001/other handoffs unchanged. No production connector/book/queues/recorder/replay/loader/hash/verifier/real fence/publisher/strategy/TradePlan/Telegram/execution.
Real feed facts remain UNKNOWN/BLOCKED_BY_MD_001; physical durability and external delivery remain NOT_RUN/OUT_OF_SCOPE. These do not block independent synthetic tests. Issues3/6 remain open. No merge/auto-merge/force-push/settings changes or self-acceptance. This file supersedes the obsolete docs-only handoff; earlier history and the reporting error remain auditable in Git/PR.
