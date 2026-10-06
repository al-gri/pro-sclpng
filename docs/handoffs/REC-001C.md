# REC-001C handoff

## Task

- Task ID: **REC-001C**
- Issue: #18 — DataHealth + continuity runtime reducer without network/book mutation
- Parent epic: #5 REC-001
- Exact base SHA: `24fb4872150232af2665d3fc45bcdc63d6b355e9`
- Branch: `feat/REC-001C-data-health`
- PR: #32
- Pre-handoff implementation head: `cff099706f09d26189fec43ef46bb9940aeb71f3`
- Final head SHA: intentionally not embedded in this committed file because a Git commit cannot reliably contain its own SHA without self-reference. Following the REC-001A/REC-001B repository precedent, the exact post-handoff final head and exact-final-head CI run are recorded after the last commit in mutable PR #32 / Issue #18 metadata.

## Recovery provenance

This handoff completes the existing REC-001C workstream rather than creating a second worker:

- original worker claim: Issue #18 comment `6019475093`;
- integrator recovery authorization: Issue #18 comment `6020450404`;
- recovery status: `RESUME_EXISTING_CLAIM / CONTINUE_PR_32`;
- original accepted base remains `24fb4872150232af2665d3fc45bcdc63d6b355e9`;
- recovery-observed implementation head was `cff099706f09d26189fec43ef46bb9940aeb71f3`;
- no second claim, branch, or PR was created.

## Delivered scope

REC-001C adds a pure deterministic production-facing DataHealth / continuity reducer inside `crates/market-data`.

The reducer:

1. consumes explicitly recorded, ordered observations with a supplied monotonic clock sample;
2. reuses accepted `domain` identities, epochs, policy and record/evidence types;
3. preserves independent transport, freshness and book-validity owners;
4. keys transport by `(ConnectionId, ConnectionEpoch)`;
5. tracks per-stream full epoch tags, invalidation barriers, bounded pending evidence, anchor, capped warm-up progress, frozen witness, last valid original sample and applied raw frontier;
6. reuses REC-001A `ContinuityClassifier` for source-visible regular `books50` sequence classification;
7. performs no socket ownership, no live wall-clock read, no WAL filesystem I/O and no canonical level/quantity mutation.

No accepted spec, ADR or `crates/domain/**` contract was changed.

## Accepted contracts reused

The implementation is bounded to the accepted contracts already in main:

- `specs/market-data/data-health-v1.md`;
- `specs/market-data/events-v1.md`;
- `specs/domain/review-vectors-v2.md`;
- `specs/domain/test-matrix-v1.md`;
- `docs/adr/0002-domain-event-wal-contracts.md`;
- accepted domain identities/epoch/policy/evidence types;
- accepted REC-001A decoder and `ContinuityClassifier`;
- accepted Bitget public-feed constraints and bounded synthetic fixtures.

The executable `crates/domain/tests/support/health/**` model was used as a behavioral oracle/test support, not copied as a production API.

## Key DataHealth invariants delivered

### Recorded order and time

- `RecordNo` is authoritative processing order.
- Records must be dense after the reducer's first accepted record.
- Exchange/source timestamps never reorder inputs.
- Evaluation is `max(previous, recorded monotonic sample)` within one accepted `ClockScope`.
- The reducer never calls `Instant::now`, `SystemTime::now` or another live clock.
- Expiry/freshness processing occurs before the current observation transition.

### Transport ownership

- Transport is owned by `(ConnectionId, ConnectionEpoch)`.
- Registering a second stream on an already-Up shared connection preserves that transport and the existing stream state.
- Current `Down` invalidates only current dependents of that connection.
- Later `Up` or duplicate Up/heartbeat does not restore book validity or clear the barrier.
- Connection epoch advance creates the new generation as `Transport::Unknown`, resets only its dependents to `FUnknown / NoSnapshot`, and leaves other connections unchanged.
- Subscription and Book epoch advances reset only their selected owner/dependent stream while preserving shared connection transport.

### Book continuity and barriers

- Registration initializes a book stream as `NoSnapshot` with barrier at the registration record.
- Current gap, REC-001A continuity gap/reset/mismatch/needs-snapshot/unexpected-snapshot, current proof conflict, pending overflow, pending timeout and deadline overflow fail closed.
- Fail-closed invalidation advances the barrier to the current recorded input and clears pending/anchor/progress/witness/last-valid-sample continuity state.
- Historical applied frontier is retained for audit/dedup exactly like the accepted reference model; the new barrier prevents old inputs from becoming current again.
- Pre-barrier and obsolete-tag observations are diagnostics/no-ops for the recovered generation.
- A snapshot is only a continuity `AnchorCandidate` until matching current post-verifier evidence releases it.
- Released snapshot enters `Warming`, never `Usable` automatically.
- Delta release without an anchor fails closed as `NeedsSnapshot`.
- A truthful explicit current warm-up witness is required before `Usable`.

### REC-001A continuity reuse

The reducer does not duplicate Bitget sequence rules. It consumes the existing REC-001A classifier outcomes:

- snapshot -> first update: accepted interval rule `current.pseq <= snapshot.seq <= current.seq`;
- update -> update: accepted `previous.seq == current.pseq`;
- source-visible gap/reset/discontinuity/mismatch outcomes invalidate the current continuity scope;
- exact duplicate observation is a diagnostic/no-op.

### Deduplication / proof boundary

`VerifiedFrame` and `VerifiedWarmup` are explicitly post-verifier semantic observations. Parsed artifact references alone must not be converted into them.

Within this reducer boundary:

- equivalent already-applied frame proof -> no new effect/sample/progress;
- equivalent already-ready proof -> no new effect;
- explicit contradictory current-scope verifier result -> `ProofConflict` and fail-closed invalidation;
- old/pre-barrier conflict cannot poison the recovered generation;
- duplicate source frame creates no new logical progress.

Artifact loading, digest/body verification and complete proof-descriptor verification remain outside REC-001C as required by the bounded task.

### Bounded pending state

Per-stream pending state is bounded by the accepted policy fields:

- frame count;
- total raw bytes;
- total candidate output count;
- wait deadline.

Overflow never evicts the oldest frame, silently skips data or coalesces loss. The triggering observation yields a typed `PendingOverflow` / timeout/deadline-overflow invalidation and advances the barrier.

### Freshness independence

- Freshness is derived only from applied original recorded samples and versioned policy.
- Delayed proof does not refresh with proof receipt time.
- Equal/decreasing source timestamps do not reorder inputs.
- A recorded timer/silence sample can move freshness to Stale/Unknown while transport remains Up.
- `usable_data` requires current Up transport, Fresh/QuietVerified freshness, `BookValidity::Usable`, a current post-barrier anchor and its frozen witness.

REC-001C does not productionize the full accepted quiet-proof/artifact model; publication candidate/permit and StorageFence logic are also intentionally outside this bounded reducer task.

## Changed files

Exact PR scope after this handoff commit:

- `Cargo.lock`
- `crates/market-data/Cargo.toml`
- `crates/market-data/src/data_health.rs`
- `crates/market-data/src/lib.rs`
- `crates/market-data/tests/architecture.rs`
- `crates/market-data/tests/data_health.rs`
- `docs/handoffs/REC-001C.md`

No `crates/domain/**`, `crates/recording/**`, `specs/**`, `docs/adr/**`, `docs/PROJECT_STATE.md`, workflow, app or Bitget fixture/provenance file is changed.

## Dependency change

`crates/market-data/Cargo.toml` adds only the accepted local workspace dependency:

```toml
domain = { path = "../domain" }
```

`Cargo.lock` changes only to reflect the `market-data -> domain` workspace dependency.

No third-party dependency was added.

## Regression tests

`crates/market-data/tests/data_health.rs` covers the required deterministic REC-001C scenarios:

1. gap invalidation and new barrier;
2. reset/discontinuity invalidation;
3. typed pending overflow without silent eviction;
4. duplicate/idempotent source observation as no-op plus current-scope proof conflict fail-closed;
5. pre-barrier and old-epoch observations cannot restore state;
6. Transport Down fan-out only to its connection dependents;
7. Transport Up / duplicate Up heartbeat does not restore the book;
8. connection epoch advance resets only dependents and creates new `Transport::Unknown` generation;
9. subscription/book epoch owner isolation;
10. repeated snapshot behavior is conservative and never implicitly ready;
11. snapshot -> first update uses REC-001A interval continuity;
12. update -> update uses REC-001A `previous.seq == pseq` continuity;
13. snapshot/continuous updates remain non-usable without required explicit warm-up witness;
14. recorded silence/timer changes freshness while transport stays Up;
15. reversed/equal source timestamps do not change recorded processing order;
16. shared-connection registration preserves the first stream;
17. multi-connection faults preserve unrelated connection state;
18. identical bounded traces produce identical step results and final snapshots.

The tests reuse accepted bounded synthetic Bitget fixtures and do not add raw live-market archives.

`crates/market-data/tests/architecture.rs` continues to prohibit:

- runtime network/socket libraries and ownership;
- REST/HTTP clients;
- filesystem/WAL file I/O;
- live wall clocks;
- canonical `BookUpdate` / `SetLevel` / `DeleteLevel` mutation;
- private API/execution/strategy surfaces;
- unexpected third-party runtime dependencies.

## Self-review / scope audit

A recovery self-review was performed against exact base
`24fb4872150232af2665d3fc45bcdc63d6b355e9` and the full PR #32 diff.

Result: **PASS (worker self-review; not independent QA).**

Checked specifically:

- accepted T/F/B owner separation;
- current epoch/barrier transitions;
- fail-closed gap/reset/continuity/proof/overflow behavior;
- old/pre-barrier no-restoration behavior;
- shared transport fan-out/isolation matching V-R6-SHARED and V-R6-DOWN-FANOUT;
- snapshot and warm-up proof gating;
- duplicate/idempotency semantics;
- pending bounds and no silent eviction;
- original recorded-sample freshness;
- deterministic recorded order independent of exchange timestamps;
- REC-001A continuity reuse rather than a second sequence implementation;
- no hidden U-09/U-10/U-20/C-01/C-03 resolution;
- no scope creep into networking, WAL filesystem code, canonical local-book mutation, strategy or execution.

No code defect requiring another implementation commit was identified during this self-review. This does not replace independent QA.

## Verification evidence

### Local worker shell

This recovery execution surface uses the GitHub connector and has no local Rust/shell checkout evidence. Therefore:

- `cargo fmt --all -- --check` — **NOT_RUN locally**
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — **NOT_RUN locally**
- `cargo test --workspace --locked` — **NOT_RUN locally**

No CI result is described as a local run.

### Historical exact-head CI before this handoff

GitHub Actions Rust CI run **37491536358** ran on exact implementation head
`cff099706f09d26189fec43ef46bb9940aeb71f3` and completed **SUCCESS**:

- `rust-fmt` / formatting check — **PASS**;
- `rust-clippy` / workspace all targets with `-D warnings` — **PASS**;
- `rust-tests` — **PASS**;
- Cargo-generated `Cargo.lock` verification — **PASS**;
- workspace build — **PASS**;
- clean checkout verification — **PASS**.

This run is historical after the handoff commit and is not transferred to the final PR head.

### Final post-handoff exact-head CI

At authorship time the commit containing this file does not yet have a SHA, so its exact-head CI cannot yet exist.

After this handoff commit:

1. obtain the new exact PR #32 head SHA;
2. require a new GitHub Actions run for that exact SHA;
3. require `rust-fmt`, `rust-clippy`, `rust-tests`, lockfile verification, workspace build and clean-checkout verification all to PASS;
4. record the exact final SHA and run ID in PR #32 and Issue #18;
5. do not mark REC-001C `READY_FOR_INDEPENDENT_QA` until those checks pass.

## Preserved UNKNOWN / BLOCKED / FORBIDDEN boundaries

- **U-09 UNKNOWN / BLOCKED** — regular `books50` quantity asset/unit remains unknown. No canonical quantity unit is inferred.
- **U-10 UNKNOWN / BLOCKED** — zero quantity is not interpreted as `DeleteLevel`.
- **U-20 NOT_PROVEN / FORBIDDEN** — no REST<->WS causal bridge, stitching or healing is implemented.
- **C-01 CONTRACT_CONFLICT / BLOCKED** — RPI two-quantity depth is not normalized into the one-quantity canonical Book.
- **C-03 DOC_CONFLICT / UNKNOWN** — no instruments-route alias/deprecation relationship is asserted.

The reducer does not parse lexical book quantity into canonical quantity and does not construct canonical `SetLevel` / `DeleteLevel`.

## Explicitly OUT_OF_SCOPE

Not implemented by REC-001C:

- live WebSocket/client ownership or reconnect supervisor;
- REST networking/healing;
- DNS/TLS/runtime socket ownership;
- WAL filesystem writer/reader integration;
- recorder application wiring;
- replay application integration;
- production artifact loader/verifier;
- publication candidate/permit;
- StorageFence producer or physical-storage verification;
- canonical local-book quantity/level mutation;
- RPI normalization;
- Parquet/export;
- strategy/FSM setup;
- signals;
- TradePlan;
- Telegram/external delivery;
- private API / API keys / order execution;
- live trading;
- performance/profitability claims.

## Known limitations / downstream boundary

- The reducer accepts already-qualified recorded semantic observations; live supervisor admission and socket lifecycle belong to REC-001D.
- Complete artifact/proof descriptor verification remains outside this reducer boundary. Callers must not treat a parsed proof reference as `VerifiedFrame` or `VerifiedWarmup`.
- The full accepted quiet FreshnessEvidence model is not productionized here; ordinary recorded-sample freshness/timer behavior is implemented.
- Recording-health/publication/storage-fence semantics remain downstream bounded work.
- Canonical book application remains blocked by MD-002 / U-09 / U-10 and belongs to REC-001E after accepted evidence.

## Readiness

Worker readiness is conditional only on the fresh exact-post-handoff CI described above.

After that run succeeds, update PR #32 metadata, convert Draft -> Ready for review if permitted, post the durable Issue #18 completion record with status `READY_FOR_INDEPENDENT_QA`, and stop without merge or auto-merge.


## Independent QA CHANGES_REQUIRED and repair

Independent QA reviewed exact head
`6ad75f9862521f4cae6043bf8e5412a9142624e0` against exact base
`24fb4872150232af2665d3fc45bcdc63d6b355e9` and returned
**CHANGES_REQUIRED**. Integrator return-to-worker record: Issue #18 comment
`6021958200`.

The repair continued the existing claim/branch/PR #32. No new claim, branch or PR
was created. The branch lineage is a fast-forward from the QA-reviewed head.

### F1 — HIGH: ordered pending proof expiry

Disposition: **FIXED**.

Production no longer stores frame verification as an irreversible boolean.
`VerifiedFrameProof` is an explicit REC-001C post-verifier projection carrying
the accepted `VerificationEvidence` plus `valid_until_ns: Option<u64>`.
Pending state stores the corresponding accepted local projection.

Proof expiry uses checked recorded evaluation semantics:

- at first verified-proof receipt, `evaluation_ns >= valid_until_ns` fails closed;
- immediately before each ready frame is actually released from the ordered pending prefix,
  the same exclusive-bound check is repeated;
- expiry yields typed `BookInvalidReason::ProofExpired`;
- release-time expiry is a semantic invalidation, not a transactional Rust `Err`, so
  whole frames already completed earlier in that same release step remain historical;
- the expired frame emits no release effect and the current scope is invalidated at the
  current recorded input, clearing pending/anchor/progress/witness/last-valid continuity state.

This is the bounded production analogue of accepted `V-R1-EXPIRED-PROOF`.
Artifact loading/body verification remains outside REC-001C; a parsed
`ArtifactRef` alone is still not a verified frame proof.

New regression coverage includes:

- later delta proof becomes ready behind an unverified snapshot;
- equality expiry at actual ordered release;
- earlier snapshot release remains in the step result;
- expired delta release is absent;
- final book state is `Invalid(ProofExpired)`, freshness Unknown, barrier=current record,
  pending/anchor/progress/witness/last-valid cleared and `usable_data == false`;
- a proof already expired at its initial verification receipt fails closed without release.

### F2 — MEDIUM: freshness deadline overflow diagnostic

Disposition: **FIXED**.

Production ordinary freshness now returns both the resulting `Freshness` and
whether checked `sample + deadline` overflowed. Overflow preserves
`Freshness::Unknown` and emits typed
`HealthDiagnostic::FreshnessDeadlineOverflow { stream }` even when freshness
was already Unknown, so lack of a state transition cannot hide the diagnostic.
No saturating or wrapping arithmetic is used.

The production regression for accepted `V-R3-OVERFLOW` uses an applied original
sample `u64::MAX - 5` with deadline 10 and asserts Unknown freshness plus the
typed diagnostic while transport stays Up and book continuity is not reset.

### F3 — MEDIUM: architecture regression gates

Disposition: **FIXED**.

The manifest gate is now an exact allow-list for normal runtime dependencies:
the only accepted `[dependencies]` entry is exactly

`domain = { path = "../domain" }`.

The deterministic no-dependency parser also rejects:

- any additional normal runtime dependency regardless of package name;
- `[dependencies.<name>]` tables;
- any `[build-dependencies]` / `[build-dependencies.<name>]`;
- target-specific production dependency/build-dependency sections.

No parser dependency was added.

Source scanning remains explicitly a defense-in-depth regression guard rather than
a formal architecture proof. It now catches `std::fs`, `std::net`,
`std::process`, direct and grouped imports, absolute/std aliases and obvious
`extern crate std as ...` alias attempts, in addition to the existing socket,
HTTP/REST, filesystem open, live-clock, WAL-path, canonical mutation and
private/execution/strategy tokens.

Synthetic regressions prove that arbitrary manifest dependencies and representative
`use std::fs; fs::read(...)`, `use std::net; ...`, grouped import and std-alias
bypasses are rejected.

### Additional QA-requested production regressions

Added explicit tests for:

- pending raw-byte bound overflow;
- pending candidate-output-count overflow;
- pending deadline equality (`evaluation == deadline` => `PendingTimeout`);
- pending deadline checked-add overflow => `PendingDeadlineOverflow` with no wrap;
- attempted second writer for the same accepted `BookRef` rejected transactionally
  as `IdentityError::WriterRebindRequiresNewArchive`, with the prior reducer state
  and `last_record` unchanged.

All pre-existing REC-001C regression scenarios remain in the same test suite.

### Repair changed files relative to rejected QA head

The implementation repair from
`6ad75f9862521f4cae6043bf8e5412a9142624e0` changes only:

- `crates/market-data/src/data_health.rs`;
- `crates/market-data/src/lib.rs`;
- `crates/market-data/tests/architecture.rs`;
- `crates/market-data/tests/data_health.rs`;
- this handoff file is updated by the final documentation commit.

There is no dependency-graph change in the repair; `Cargo.lock` is unchanged
relative to the rejected QA head. No `crates/domain/**`, `crates/recording/**`,
accepted spec/ADR, `docs/PROJECT_STATE.md`, workflow or app change is part of
the repair.

### Verification after QA repair

Local environment capability check in this worker continuation:

- shell/Git: available;
- Rust/Cargo/rustfmt: unavailable in the local execution environment.

Therefore the mandatory Rust commands are truthfully:

- `cargo fmt --all -- --check` — **NOT_RUN locally**;
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — **NOT_RUN locally**;
- `cargo test --workspace --locked` — **NOT_RUN locally**.

GitHub Actions run `37507031250` on intermediate repair head
`281633e987dbe9a53d3a4131a822e05bfc98ccf9` is historical **FAIL** because
`rust-fmt` failed on one alias-regression layout line. On that same SHA,
`rust-clippy` and `rust-tests` (including lockfile verification, workspace
build and clean-checkout verification) passed. The rustfmt log gave the exact
canonical replacement; that formatting-only correction produced pre-handoff repair
head `b2ad1ba2533ed3716ea5af942d06840dbdea2e91`.

No PASS from either the rejected QA head or an intermediate repair head is
transferred to the final post-handoff head. The exact final SHA and fresh
exact-head CI run are recorded in mutable PR #32 / Issue #18 metadata after this
committed handoff update.

### Preserved boundaries after repair

Unchanged:

- **U-09 UNKNOWN / BLOCKED** — regular books50 quantity unit;
- **U-10 UNKNOWN / BLOCKED** — zero/delete semantics;
- **U-20 NOT_PROVEN / FORBIDDEN** — REST<->WS healing/stitching;
- **C-01 CONTRACT_CONFLICT / BLOCKED** — RPI canonical normalization;
- **C-03 DOC_CONFLICT / UNKNOWN** — instruments route relationship.

No canonical `SetLevel`/`DeleteLevel`, quantity mapping, REST healing, RPI
normalization, socket ownership, WAL filesystem I/O, recorder/replay app,
publication/StorageFence, strategy, private API or execution was introduced.

### Re-QA gate

After this handoff update commit:

1. obtain the exact new PR #32 head;
2. require a fresh exact-head GitHub Actions **SUCCESS** with `rust-fmt`,
   `rust-clippy`, `rust-tests`, Cargo.lock verification, workspace build and
   clean checkout all PASS;
3. update PR #32 and Issue #18 with the exact final SHA/run and status
   `READY_FOR_RE_QA`;
4. perform full independent re-QA of that exact SHA.

The worker does not merge or enable auto-merge.


## Independent re-QA CHANGES_REQUIRED: F4–F7 repair

Independent full re-QA reviewed exact head
`53012b195ca0d33a701981657000e867b57eee22` against the unchanged exact base
`24fb4872150232af2665d3fc45bcdc63d6b355e9` and returned
**CHANGES_REQUIRED**.

Historical F1/F2/F3 remain resolved. The new acceptance blockers were:

- **F4 HIGH** — public API could fabricate post-verifier truth from public DTO fields;
- **F5 HIGH** — a repeated current same-epoch `Transport::Down` did not advance the barrier;
- **F6 HIGH** — accepted `require_two_sided_snapshot` policy was not enforced;
- **F7 MEDIUM** — an unannounced future `ConnectionEpoch` was misclassified as obsolete.

The second repair continues the same Issue #18 / branch / PR #32 lineage. No reset,
new claim, branch or PR was created.

### F4 — opaque post-verifier capabilities

Disposition: **FIXED**.

Production no longer exposes constructible positive verifier assertions.

`HealthObservation` still has explicit typed post-verifier variants, but their
payloads are opaque capability types:

- `VerifiedFrameProof`;
- `VerifiedWarmupProof`;
- `VerifiedProofConflict`.

Their semantic fields are private. There is no production constructor, `Default`,
or conversion from `ArtifactRef` / public domain evidence. Test-only minting exists
only under `#[cfg(test)]` inside the `data_health` module and is absent from
normal library builds.

Consequences:

- an ordinary external caller may parse or construct public
  `domain::VerificationEvidence` / `WarmupEvidence`, but cannot convert that DTO
  into a production verified capability;
- without a future in-crate artifact verifier, public callers can admit raw frames
  but cannot release them or install a warm-up witness, so they cannot drive
  `BookValidity::Usable` by self-asserting proof truth;
- verifier conflict is equally sealed and cannot be fabricated as a trusted
  post-verifier outcome by ordinary callers;
- no artifact loader/resolver was productionized in REC-001C.

The former black-box integration state-machine tests were moved to
`crates/market-data/src/data_health_tests.rs`, a unit child-module of
`data_health`. This lets accepted positive proof traces use private test-only
capability minting without widening the production API.

Three `compile_fail` doctests exercise the external boundary and prove that
struct-literal construction of all three capabilities is rejected by Rust privacy.
Code-only CI run `37518018540` confirmed all three doctests PASS.

This preserves ADR-0002 A3: ParsedRef is not Verified. Future production verifier
work must add the internal production minting boundary; until then the positive
verified path is deliberately unavailable to ordinary callers.

### F5 — repeated current Down is a new invalidation record

Disposition: **FIXED**.

Transport handling now first resolves the current epoch for the `ConnectionId`.
For a current same-epoch observation whose previous T value is already `Down`,
the reducer may still emit `DuplicateTransport` as a value-level diagnostic, but
it does **not** early-return. Every current `Down` re-runs fail-closed
invalidation for all current dependents at the current `RecordNo`.

Regression:

`Up -> Down(barrier3) -> post-barrier snapshot raw4 pending -> Down(barrier5)
-> Up -> proof(raw4)`

asserts:

- second Down advances barrier to r5;
- pending raw4 is cleared;
- B remains `Invalid(TransportDown)`;
- later proof of raw4 is `PreBarrier` against barrier5 and cannot restore state.

Unrelated connections and the existing shared-connection fan-out semantics remain
unchanged.

### F6 — require_two_sided_snapshot

Disposition: **FIXED**.

`BookInvalidReason::SnapshotNotTwoSided` was added.

Raw admission stores the minimal structural projection needed at release:
whether the snapshot has at least one ask and at least one bid. This does not parse,
normalize or interpret price/quantity semantics.

Immediately before a verified snapshot is released:

- if `policy.fields.require_two_sided_snapshot == true`;
- and the stored snapshot projection is not two-sided;

the reducer fails closed at the current proof/release record as
`SnapshotNotTwoSided`. It emits no `SnapshotReleased`, installs no anchor, clears
dependent continuity state and cannot become Usable.

The new regression uses a structurally decodable snapshot with an empty ask side.
No U-09/U-10 assumption is made.

### F7 — old vs future ConnectionEpoch

Disposition: **FIXED**.

Transport observations now resolve the connection owner's current epoch before
classification:

- `epoch < current` -> `ObsoleteConnection` diagnostic/no-op;
- `epoch == current` -> apply the transport observation;
- `epoch > current` -> transactional
  `HealthError::Identity(IdentityError::EpochMismatch)`.

The future-epoch error occurs inside the existing clone-and-commit `step()`
transaction, so the rejected record does not advance `last_record`, evaluation or
state. An explicit accepted `EpochAdvance` remains the only route to a newer
connection generation.

The regression asserts both the future mismatch rollback and that the previous
epoch becomes obsolete after an explicit advance.

### Regression suite after F4–F7

The DataHealth tests now execute as an in-crate unit module so they can access the
sealed test-only verifier capability constructors. Code-only exact-head CI run
`37518018540` on
`51b7fce672657376f6d50cc9bab478698e3024b9` completed **SUCCESS** and reported
19 DataHealth tests PASS, including the new:

- `repeated_current_down_advances_barrier_and_blocks_intervening_pending_frame`;
- `two_sided_snapshot_policy_fails_closed_without_quantity_semantics`;
- `future_unannounced_connection_epoch_is_transactional_mismatch`.

The existing F1/F2/F3 and bounded-state regressions remain in the same module and
continue to pass.

Market-data doctests on that SHA:

- opaque `VerifiedFrameProof` compile-fail boundary — PASS;
- opaque `VerifiedWarmupProof` compile-fail boundary — PASS;
- opaque `VerifiedProofConflict` compile-fail boundary — PASS.

### Second repair changed paths relative to re-QA-rejected head

From
`53012b195ca0d33a701981657000e867b57eee22`
to pre-handoff code head
`51b7fce672657376f6d50cc9bab478698e3024b9`,
the semantic/test repair is limited to market-data:

- `crates/market-data/src/data_health.rs`;
- `crates/market-data/src/lib.rs`;
- `crates/market-data/tests/data_health.rs` is moved/renamed to
  `crates/market-data/src/data_health_tests.rs` so positive verified traces can
  use private test-only minting.

This handoff file is the only documentation path changed by the final handoff commit.

No `Cargo.toml` or `Cargo.lock` change is required by F4–F7. No
`crates/domain/**`, `crates/recording/**`, accepted spec/ADR,
`docs/PROJECT_STATE.md`, workflow or app change is part of this repair.

### Verification for the second repair

Worker-local Rust execution remains unavailable, therefore:

- `cargo fmt --all -- --check` — **NOT_RUN locally**;
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — **NOT_RUN locally**;
- `cargo test --workspace --locked` — **NOT_RUN locally**.

CI is not represented as local execution.

Historical intermediate run `37517779202` on
`26303b6bca856f32fb524f94053e9a1b8bfbb357` had clippy/tests PASS but rustfmt
FAIL. Historical run `37517933704` on `2397d467...` similarly exposed only the
remaining rustfmt import-order diff. Neither is acceptance evidence.

Code-only pre-handoff exact-head run `37518018540` on
`51b7fce672657376f6d50cc9bab478698e3024b9` completed **SUCCESS**:

- rust-fmt — PASS;
- rust-clippy — PASS;
- rust-tests — PASS;
- Cargo.lock verification — PASS;
- workspace build — PASS;
- clean checkout verification — PASS;
- DataHealth unit tests — 19/19 PASS;
- market-data opaque-capability compile-fail doctests — 3/3 PASS.

The commit containing this handoff section necessarily has a different SHA.
Therefore `37518018540` becomes historical after this handoff commit and is not
transferred as final acceptance evidence. The exact post-handoff final SHA and
fresh exact-head CI run must be recorded in mutable PR #32 / Issue #18 metadata.

### Preserved hard boundaries after F4–F7

Unchanged:

- **U-09 UNKNOWN / BLOCKED** — regular books50 quantity unit;
- **U-10 UNKNOWN / BLOCKED** — zero/delete semantics;
- **U-20 NOT_PROVEN / FORBIDDEN** — REST<->WS healing/stitching;
- **C-01 CONTRACT_CONFLICT / BLOCKED** — RPI canonical normalization;
- **C-03 DOC_CONFLICT / UNKNOWN** — instruments route relationship.

No canonical `SetLevel` / `DeleteLevel`, quantity mapping, REST healing,
RPI normalization, WebSocket ownership, WAL filesystem I/O, recorder/replay app,
publication/StorageFence, strategy, private API or execution was added.

### Next gate after second repair

After this handoff commit:

1. obtain the exact new PR #32 head;
2. require fresh exact-head CI SUCCESS for fmt/clippy/tests, lockfile verification,
   workspace build and clean checkout;
3. update PR #32 and Issue #18 with the exact SHA/run and status
   `READY_FOR_RE_QA`;
4. run a **full independent re-QA** of that exact SHA, including public verifier
   sealing and the complete DataHealth state machine.

No merge or auto-merge by the worker.
