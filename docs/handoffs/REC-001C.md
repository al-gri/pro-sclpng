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
