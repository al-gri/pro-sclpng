# REC-001D handoff

## Task identity

- Task ID: **REC-001D**
- Issue: #20 — Public WS supervisor + bounded raw capture to WAL
- Parent epic: #5 REC-001
- Existing claim comment: `6024304772`
- Recovery/ownership continuation comment: `6025249885`
- Branch: `feat/REC-001D-ws-supervisor`
- PR: #34
- Original exact claim/base SHA: `c9c23f707299ab35c051783b75f645e592213822`
- Current/final-main reference integrated into this branch: `39ff0dba797eb010586238ef06fb80e996340401`
- Pre-recovery implementation head with historical exact-head CI: `2447a0ad316bf7b6631f514e56436d60bedb3433`
- Main-integration merge head immediately before this handoff commit: `773cf278007c771ec6b896dd6db636ce138cd3c8`
- Exact final branch head containing this handoff: recorded after this file is committed in mutable PR #34 / Issue #20 metadata. A committed file cannot truthfully contain its own final commit SHA without self-reference; this follows the existing REC-001A/B/C handoff precedent.
- Worker stop target after fresh exact-head CI: **READY_FOR_INDEPENDENT_QA**.

No replacement claim, branch, Issue, or PR was created during recovery.

## Delivered scope

REC-001D adds a bounded Bitget public-WebSocket protocol/supervision slice in
`market-data`, feeding accepted raw/control `RecordFrame` values through a
persistence gate before supervisor state/effects are exposed.

Implemented behavior:

1. accepted production public unauthenticated UTA v3 endpoint only:
   `wss://ws.bitget.com/v3/ws/public`;
2. deliberately small configured regular `usdt-futures/books50` profile,
   capped by versioned engineering policy at four streams;
3. one unique connection owner and one unique regular-book owner per configured stream;
4. subscribe messages only for configured native symbols;
5. deterministic connect -> subscribe -> acknowledgement -> snapshot/capture state;
6. text `ping` / `pong` heartbeat supervision with recorded timer/transport controls;
7. heartbeat/pong updates transport liveness only and does not create market freshness;
8. disconnect/heartbeat timeout advances connection, subscription and book epochs;
9. deterministic bounded reconnect backoff with deterministic per-stream jitter;
10. bounded global FIFO plus per-stream raw-frame/raw-byte/message limits;
11. no raw-frame eviction: rejected current-epoch raw admission becomes an explicit
    `Gap(QueueOverflow)` with the lost `CaptureAttemptNo`, loss range/count, and
    stream degradation;
12. overflow/degradation of one stream does not mutate or silently freeze a neighbor stream;
13. raw bytes are persisted exactly before decoding/classification, with local receive
    wall-clock/monotonic timestamps and capture-attempt order preserved;
14. global admission/drain order is FIFO, preserving the caller-observed receive order;
15. old-generation raw input may be recorded diagnostically but cannot revive the current generation;
16. source continuity failures become explicit recorded source GAPs and degraded state;
17. no REST healing/stitching path exists;
18. persistence receipt must reach the configured `RecordingGate` before corresponding
    supervisor state/effects are exposed; weak/mismatched persistence receipts halt fail-closed;
19. offline integration regression writes accepted bootstrap definitions plus supervisor
    transport/raw frames through `recording::WalWriter` and reads them back in order.

`publicTrade` subscription is intentionally **not** added in this bounded slice; it was optional
in Issue #20 and omitting it avoids widening the ownership/overflow surface.

## Runtime / dependency boundary

Concrete DNS/TCP/TLS/WebSocket socket I/O is **not implemented in PR #34**.

The production crate exposes deterministic `TransportCommand` outputs and bounded admission
methods for an external driver, but owns no async runtime, socket, TLS library, live clock source,
filesystem or REST client. This is an explicit limitation, not a claim of live end-to-end capture.

Reason: the accepted workspace has no approved WebSocket/TLS runtime dependency and CI is designed
to run offline. Adding a networking stack only to make the PR look "live" would be an architecture/
dependency decision outside the existing accepted contracts.

Likewise, archive/config/instrument/stream definition records and the concrete `WalWriter`
adapter remain composition-root responsibilities. The offline accepted-path integration regression
demonstrates the required ordering: definitions first, then supervisor control/raw records. REC-001D
does not invent missing provenance artifacts or change the accepted WAL/domain contracts.

Therefore the live public WS smoke status is **NOT_RUN**, and this handoff does not represent the
external socket driver as implemented or tested.

## Engineering-policy provenance

The following are versioned conservative engineering policy, **not Bitget guarantees**:

- `SUPERVISOR_POLICY_VERSION = 1`;
- `MAX_CONFIGURED_STREAMS = 4`;
- per-stream/global queue caps;
- reconnect base/max and deterministic jitter policy;
- pong timeout policy.

Unknown exchange operational limits remain UNKNOWN unless separately proven by accepted official
source evidence. The implementation does not promote engineering constants to exchange guarantees.

## Changed files relative to current main

Expected final PR #34 task diff after this handoff:

- `Cargo.lock`
- `crates/market-data/Cargo.toml`
- `crates/market-data/src/lib.rs`
- `crates/market-data/src/ws_supervisor.rs`
- `crates/market-data/tests/architecture.rs`
- `crates/market-data/tests/ws_supervisor.rs`
- `docs/handoffs/REC-001D.md`

The accepted MD-002 files were merged from current `main` into the same branch and are therefore
not REC-001D changes relative to current main.

No `crates/domain/**`, `crates/recording/**`, accepted spec/ADR, workflow,
`docs/PROJECT_STATE.md`, app, fixture, strategy or execution file is modified by REC-001D.

## Required regression coverage

The current test suite includes deterministic/offline regressions for:

- connect -> subscribe -> persisted accepted transport/subscription state;
- heartbeat/pong does not create market freshness;
- disconnect -> reconnect -> new connection/subscription/book epochs;
- bounded deterministic reconnect/backoff;
- subscription/control failure -> raw then explicit GAP/degraded path;
- critical bounded queue overflow -> explicit `QueueOverflow` GAP;
- no silent eviction of already-admitted raw input;
- overflow of one stream does not freeze a neighbor stream;
- exact raw bytes, receive timestamps and global receive order reach the recording boundary;
- zero quantity remains exact source bytes and is not canonical deletion;
- old-epoch input cannot revive the current generation;
- source continuity gap records GAP and has no REST-healing path;
- heartbeat timeout records timer/down controls and advances generation;
- insufficient persistence gate halts before state/effect publication;
- accepted `WalWriter/WalReader` offline path preserves definition/control/raw order;
- architecture gates reject runtime network/REST/private API/canonical-book mutation dependencies.

CI tests require no Internet.

## Self-review against Issue #20 / accepted contracts

Worker self-review result: **PASS with explicit runtime limitation above**; this is not independent QA.

Confirmed:

- bounded queues and explicit overflow handling;
- no silent critical raw eviction;
- receive FIFO/order and exact raw bytes preserved to recording;
- persistence gate checked before publication/state exposure;
- heartbeat transport liveness remains separate from market freshness;
- reconnect/backoff is bounded and versioned;
- reconnect advances connection/subscription/book generations;
- `Transport::Up` alone does not restore a market stream to captured/fresh state;
- old-epoch data cannot revive current state;
- per-stream failure/overflow is explicit and neighboring streams remain independently serviceable;
- no REST snapshot healing;
- no RPI subscription/normalization;
- no private/authenticated API or login message;
- no canonical `SetLevel` / `DeleteLevel` or quantity-unit normalization;
- no Internet requirement in unit/integration CI.

No worker-side contract change request was required for the implementation as it stands. Independent
QA must decide whether the explicit external socket-driver/composition limitation is acceptable for
Issue #20; this worker does not hide or relabel that limitation as live coverage.

## Preserved UNKNOWN / BLOCKED / FORBIDDEN boundaries

Accepted MD-002 is integrated from current main and preserves:

- **U-09 UNKNOWN / BLOCKED** — regular JSON `books50` quantity unit is not inferred;
- **U-10 UNKNOWN / BLOCKED** — regular JSON zero/deletion semantics are not inferred;
- regular snapshot zero-quantity possibility remains UNKNOWN;
- regular JSON / RPI JSON / SBE profile separation remains enforced.

Other inherited boundaries remain:

- **U-20 NOT_PROVEN / FORBIDDEN** — no REST<->WS causal healing/stitching;
- **C-01 BLOCKED** — no RPI canonical normalization;
- **C-03 UNKNOWN** — no undocumented instruments-route alias/deprecation relationship.

Raw lexical quantity, including `"0"`, may be transported and recorded but is never mapped to a
canonical quantity unit or `DeleteLevel`.

## Explicitly out of scope / not implemented

REC-001D does **not** implement:

- REC-001E canonical local-book reducer;
- REC-001F recorder/replay deterministic application integration;
- canonical `SetLevel` / `DeleteLevel`;
- quantity-unit inference;
- REST snapshot healing/bootstrap;
- RPI normalization;
- historical bootstrap;
- market structure / strategy / FSM / TrueBreakout / FalseBreakout;
- TradePlan, alerts or UI;
- private API, API keys, orders or execution;
- Parquet replacement for WAL;
- Python live/replay implementation;
- HFT/latency/profitability claims.

## Verification evidence

### Worker-local environment

This recovery surface uses the GitHub connector and has no local Rust/shell execution evidence.

- `cargo fmt --all -- --check` — **NOT_RUN locally**
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — **NOT_RUN locally**
- `cargo test --workspace --locked` — **NOT_RUN locally**
- live public WebSocket smoke — **NOT_RUN**

### Historical exact-head implementation CI

GitHub Actions run **37526161316** on exact head
`2447a0ad316bf7b6631f514e56436d60bedb3433` completed **SUCCESS**:

- rust-fmt — **PASS**
- rust-clippy — **PASS**
- rust-tests — **PASS**
- Cargo.lock verification — **PASS**
- workspace build — **PASS**
- clean checkout verification — **PASS**

This run is historical after the main-integration/handoff commits and is not transferred to the
final head.

### Final exact-head CI

At authorship time, the commit containing this handoff does not yet exist, so its SHA and exact-head
workflow run cannot yet exist. After this commit:

1. obtain the exact new PR #34 head SHA;
2. require a fresh GitHub Actions run for that exact SHA;
3. require rust-fmt, rust-clippy, rust-tests, Cargo.lock verification, workspace build and clean
   checkout verification all to PASS;
4. record the exact final SHA and run ID in mutable PR #34 and Issue #20 metadata;
5. only then convert PR #34 Draft -> Ready and set worker status
   `READY_FOR_INDEPENDENT_QA`.

### PASS / FAIL / NOT_RUN summary

- implementation self-review against Issue #20 boundaries — **PASS**
- historical exact-head CI `37526161316` on `2447a0ad...` — **PASS**
- accepted MD-002 main integration — **PASS**
- final exact-head CI — **NOT_RUN at file authorship; required after commit**
- live public WS smoke — **NOT_RUN**
- worker-local Rust commands — **NOT_RUN**
- known failing acceptance check at handoff — **none recorded**

## Stop rule

After fresh exact-final-head CI succeeds, update PR #34 and Issue #20 with the exact SHA/run,
mark the existing PR Ready for review, post the completion record, and stop as:

**READY_FOR_INDEPENDENT_QA**

Worker must not merge, enable auto-merge, start #21/#22, or repair future independent-QA findings
without a concrete QA verdict.

## Independent QA CHANGES_REQUIRED remediation

Independent QA reviewed exact immutable head
`bc14ad634c17c82f2a47c9a5b4ac0fc89f87b8b8` and returned
**CHANGES_REQUIRED**. This repair continues the same Issue #20 claim,
`feat/REC-001D-ws-supervisor` branch and PR #34. No replacement Issue,
claim, branch or PR was created, and no history rewrite/force-push was used.

The earlier readiness/completion statements above are historical for the rejected
head. This section supersedes them for the remediation lineage.

### F1 — HIGH: previous-generation raw bypassed raw bounds

Disposition: **FIXED**.

All non-pong raw input, including a known previous connection generation, now
passes the same per-stream frame/byte/message admission limits and the bounded
global raw-item admission policy before its payload can enter the queue.

A previous-generation payload that cannot be admitted no longer queues or records
the full `Vec<u8>`. Instead the supervisor queues a small
`RejectedStaleRaw` provenance marker. At drain it records an empty
`RawInput` carrying the original stream/tag/`CaptureAttemptNo` followed by a
non-loss-scoped diagnostic GAP. Empty raw is an accepted WAL diagnostic-rejection
representation; this avoids inventing a new wire/domain record while accounting
the attempt without retaining an oversized payload.

The regression
`previous_epoch_raw_exceeding_wal_capacity_is_bounded_as_empty_diagnostic`
uses a **1,100,000-byte** previous-epoch payload. That exceeds both the
supervisor hard raw-message ceiling (1,000,000 bytes) and WAL v1
`MAX_PAYLOAD = 1,048,576`. It asserts zero queued raw-frame/raw-byte charge,
only bounded empty-raw diagnostic persistence, preserved old tag/attempt, and no
global halt.

No source quantity semantics are inspected or inferred by this path.

### F2 — HIGH: sustained overflow caused global halt / undrainable queue

Disposition: **FIXED**.

Queue-capacity pressure no longer sets the fatal `halted` flag. Fatal halt is
reserved for persistence/gate failures and other fail-closed conditions for which
publication cannot continue safely. A full ingress queue returns bounded
`QueueExhausted` without preventing already admitted records from draining to
the configured RecordingGate.

Current-generation rejected raw attempts are represented by explicit
`QueueOverflow` loss observations. Consecutive rejected attempts at the queue
tail for the same stream/tag are coalesced into one bounded known range with
checked `first_attempt`, `last_attempt`, and `loss_count`; one rejected
frame therefore does not require an unbounded one-record-per-frame loss queue.

Raw and ordinary control admission preserve bounded loss-reserve slots
(`LOSS_RESERVE_PER_STREAM`) so already bounded work cannot consume the whole
queue and silently prevent the first required loss observation. The global raw
count is tracked independently from control/loss queue entries, while each
stream retains its raw-frame/raw-byte bounds.

The regression
`sustained_overflow_coalesces_loss_and_does_not_halt_neighbor_stream` uses
two streams with one raw frame allowed per stream, drives 32 consecutive rejected
attempts on stream A, verifies one persisted `QueueOverflow` range
`2..=33` / count 32, admits and drains stream B, and verifies no global halt.

### F3 — HIGH: binding did not match the hard-coded wire profile

Disposition: **FIXED**.

`PublicWsSupervisor::new` now requires the accepted REC-001D identity/profile
projection before emitting the hard-coded Bitget subscription:

- `Channel::BookNormal`;
- project venue token exactly `bitget`;
- project product namespace exactly `usdt-futures`;
- futures `MarketKind::Perpetual | MarketKind::DatedFuture`;
- book id and book epoch present.

This is a project identity validation for the already selected wire profile; it
does not claim a new Bitget exchange guarantee or alter the accepted identity
schema.

Negative regressions reject:

- wrong venue;
- `coin-futures`;
- `usdc-futures`;
- another non-target futures namespace;
- Spot market under the target namespace.

A DatedFuture target-profile binding remains accepted because the accepted
identity contract allows futures kind to be delivery or perpetual based on
metadata.

### F4 — HIGH: canonical single-writer ownership was not enforced

Disposition: **FIXED**.

Supervisor construction now invokes the accepted
`StreamBinding::validate_registration(&previous)` semantics for every
registration before local maps are committed. This preserves the canonical
`BookRef` single-writer rule instead of creating a REC-001D-specific ownership
contract.

The regression
`supervisor_rejects_second_writer_for_same_canonical_normal_book` uses distinct
StreamId/ConnectionId/BookId values for two Normal bindings of the same canonical
instrument and requires
`IdentityError::WriterRebindRequiresNewArchive`.

### F5 — MEDIUM: queued QueueOverflow crossed an epoch boundary

Disposition: **FIXED without changing accepted WAL loss semantics**.

The accepted WAL v1 accounting contract rejects a current local
`QueueOverflow` target after its tag is no longer current
(`GapScopeTransition`). REC-001D therefore does not weaken recovery validation
or silently reinterpret the record.

On disconnect/heartbeat timeout the supervisor now:

1. persists `Transport::Down` immediately and marks the stream degraded;
2. records a bounded pending-disconnect marker;
3. drains/persists already admitted inputs for that same connection generation,
   including their original-tag QueueOverflow provenance;
4. only after no such ingress remains, persists the connection, subscription and
   book epoch advances and emits the bounded reconnect command.

Thus a loss observation queued behind a disconnect cannot become an invalid
old-tag QueueOverflow before persistence, cannot revive the stream, is not lost,
and does not trigger a configuration/global halt. The generation advance is
preserved after the accepted old-generation inputs reach the recording boundary.

The FIFO regression
`queued_overflow_before_disconnect_drains_before_epoch_advance` covers:
admitted raw -> queued disconnect -> queued overflow -> drain. It asserts
Down/degraded while the old tag is still the recording scope, persistence of the
old-tag loss, subsequent connection/subscription/book epoch advance to generation
2, Backoff/Unknown state, and no halt.

This ordering is deliberately chosen over changing the accepted domain/WAL
contract to allow old-tag local QueueOverflow records.

### F6 — LOW: architecture anti-bypass regression weakened

Disposition: **FIXED**.

The source guard again detects representative runtime-I/O namespace bypasses,
including direct `std::fs/net/process`, `use std as ...`,
`use ::std as ...`, `extern crate std as ...`, and grouped
`std::{net,...}` / `std::{self as ...}` forms.

`source_gate_rejects_namespace_import_and_alias_bypasses` provides synthetic
negative regressions. This is still a defense-in-depth source regression, not a
claim of formal static-analysis completeness.

### Remediation changed paths

Relative to the QA-reviewed `bc14ad...` head, the semantic/test remediation is
limited to:

- `crates/market-data/src/ws_supervisor.rs`;
- `crates/market-data/tests/ws_supervisor.rs`;
- `crates/market-data/tests/architecture.rs`;
- this handoff file in the final documentation commit.

No `crates/domain/**`, `crates/recording/**`, accepted spec/ADR, workflow,
application, `docs/PROJECT_STATE.md`, networking dependency, canonical-book,
strategy or execution change is part of the repair.

### Verification during remediation

Worker-local Rust/shell execution remains unavailable on this connector surface:

- `cargo fmt --all -- --check` — **NOT_RUN locally**;
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — **NOT_RUN locally**;
- `cargo test --workspace --locked` — **NOT_RUN locally**;
- live public WebSocket smoke — **NOT_RUN**.

Historical remediation CI:

- run `37537612802` on `f6a346719106a8f4e74ba42f0428e746096a0244`
  — **FAIL**: rust-tests PASS; rust-fmt FAIL and rust-clippy FAIL. Clippy reported
  only the new QueueGap handler argument-count lint; formatter reported concrete
  layout diffs.
- run `37538060739` on `cc473e46dab7d2fe0aa5004ad90a97a7ed9c36ab`
  — **FAIL**: rust-tests PASS, rust-clippy PASS, rust-fmt FAIL on one remaining
  layout diff.
- code-only remediation run **37538172160** on exact head
  `b270da05f5627e1d262aba91476ca8a11657975f` — **SUCCESS**:
  rust-fmt PASS, rust-clippy PASS, rust-tests PASS, Cargo.lock verification
  PASS, workspace build PASS and clean-checkout verification PASS.

The code-only PASS is historical once this handoff update is committed and is
not transferred to the final documentation-containing head.

### Preserved boundaries after remediation

Unchanged:

- **U-09 UNKNOWN / BLOCKED** — regular books50 quantity unit;
- **U-10 UNKNOWN / BLOCKED** — zero/delete semantics;
- **U-20 NOT_PROVEN / FORBIDDEN** — REST<->WS healing/stitching;
- **C-01 BLOCKED** — RPI canonical normalization;
- **C-03 UNKNOWN** — instruments-route relationship.

No canonical `SetLevel`/`DeleteLevel`, quantity conversion, REST healing,
RPI normalization, private/authenticated API, strategy or execution was added.
The concrete DNS/TCP/TLS/WebSocket driver remains the previously documented
external-driver limitation and is not changed by this remediation.
REC-001E and REC-001F remain unimplemented.

### Re-QA gate

After the commit containing this section:

1. obtain the exact new PR #34 head;
2. require a **fresh exact-head** GitHub Actions SUCCESS;
3. require rust-fmt, rust-clippy, rust-tests, Cargo.lock verification, workspace
   build and clean checkout all PASS on that exact SHA;
4. record the exact final SHA/run in PR #34 and Issue #20 mutable metadata;
5. convert this existing PR from Draft back to Ready;
6. request a **full repeated independent QA of the new immutable SHA**, with
   explicit retest of stale oversized raw, sustained overflow/isolation, exact
   binding profile, canonical single-writer registration, queued
   disconnect/QueueOverflow ordering, and the architecture anti-bypass guard.

The old QA verdict and old PASS run `37530660765` are not transferred to the
repaired head.

Target worker stop state after the fresh containing-head CI succeeds:
**READY_FOR_INDEPENDENT_QA**. It is not READY_FOR_OWNER_REVIEW.

## Second independent-QA CHANGES_REQUIRED remediation

The second full independent QA reviewed exact immutable head
`cd02f7c0e3aaa70b31546ad548ab48f46f268a9c` and returned
**CHANGES_REQUIRED** for one remaining liveness defect in the
`pending_disconnect` state machine.

QA explicitly confirmed the prior remediation findings as fixed:

- F1 stale/previous-generation raw bounds — **FIXED**;
- F2 sustained overflow/global halt — **FIXED**;
- F3 exact Bitget profile binding — **FIXED**;
- F4 canonical single writer — **FIXED**;
- original F5 QueueOverflow-before-epoch-advance ordering — **FIXED**;
- F6 architecture namespace/alias anti-bypass guard — **FIXED**.

This second repair preserves those code paths and their regressions. It continues
the same Issue #20 / claim / branch / PR #34 lineage with fast-forward commits
only. No replacement claim, branch, Issue, PR, force-push, merge or auto-merge
was created/performed.

### Blocking finding — repeated same-generation disconnect liveness

Disposition: **FIXED**.

Previously, once the first same-epoch disconnect had been drained,
`pending_disconnect = Some(epoch)`; a second already-queued same-epoch
disconnect called `begin_disconnect()` and returned
`InvalidConfiguration("disconnect already pending")`. Because the ingress had
already been popped, normal post-handler completion was skipped and the pending
transition could remain permanently stranded.

The repaired semantics are:

1. the first loss observation for the current connection epoch records
   `Transport::Down`, moves the stream to Down/Degraded, installs exactly one
   bounded `PendingDisconnect`, and emits one Close command;
2. a repeated loss observation for that same still-current pending epoch is
   **idempotent for transition ownership** but is not silently discarded:
   another existing-contract `Transport::Down` control record is persisted
   with its own receive timestamp;
3. the duplicate observation does **not** install a second pending transition,
   does not emit a second Close, does not create a second generation advance,
   and does not schedule a second reconnect;
4. after every successfully handled ingress, `finish_ready_disconnects()`
   checks whether any already-admitted ingress for the pending connection epoch
   still remains;
5. after the last such ingress reaches the configured RecordingGate, the
   connection/subscription/book epochs advance exactly once and one
   `ReconnectAfter` is emitted;
6. the empty-queue branch of `drain_one()` now also runs
   `finish_ready_disconnects()` before returning `None`, so a ready pending
   transition cannot require a future unrelated packet/tick to make progress;
7. persistence/gate failures still return through the existing fail-closed path
   and keep the supervisor halted rather than publishing unpersisted progress.

The original pending transition timestamp remains the causal stamp for the
single epoch-advance/reconnect transition. Repeated Down observations are
recorded as observations, not reinterpreted as additional transitions.

No Record/WAL/domain contract was added or weakened. In particular, accepted
QueueOverflow/GAP scope and `GapScopeTransition` semantics remain unchanged.

### Targeted regression A — repeated same-epoch disconnect

`repeated_same_epoch_disconnect_records_duplicate_down_and_finishes_once`

Queues two `Disconnected(epoch=1)` observations before the first drain and
proves:

- the first drain records Down and leaves the transition pending while the
  second epoch-1 ingress remains;
- the second Down is persisted without a fatal configuration error;
- exactly two Down observations are recorded;
- exactly three epoch-advance control records exist (connection/subscription/book,
  one transition only);
- exactly one `ReconnectAfter` is emitted across both drains;
- connection/subscription/book reach generation 2;
- final state is `Transport::Unknown / SubscriptionState::Backoff`;
- no market state is revived;
- the queue is empty and a further drain returns `None`.

### Targeted regression B — heartbeat timeout + queued disconnect

`heartbeat_timeout_then_same_epoch_disconnect_finishes_one_transition`

Queues the accepted heartbeat `PongTimeout(epoch=1)`, then queues a normal
`Disconnected(epoch=1)` before draining the timeout.

It proves:

- timer controls are recorded;
- timeout Down is recorded;
- the subsequent same-epoch disconnect records another Down instead of failing;
- no epoch advance occurs while that second old-generation ingress remains;
- exactly one connection/subscription/book generation advance occurs;
- exactly one reconnect schedule is emitted;
- final state is generation 2 Backoff/Unknown with no stranded pending state.

### Targeted regression C — duplicate disconnect with old-generation raw queued

`repeated_disconnect_waits_for_queued_old_generation_raw_before_advancing`

Queues:

`Disconnected(epoch=1) -> admissible raw(epoch=1) -> Disconnected(epoch=1)`.

It proves:

- the first Down does not advance the generation while old-generation ingress is
  still admitted;
- the raw bytes are persisted with the original epoch before the connection
  epoch-advance record;
- draining the raw still does not advance while the second disconnect remains;
- the final duplicate Down is non-fatal and completion then advances to
  generation 2;
- no global halt or permanent Down state remains.

### Targeted regression D — completion when the last blocker empties the queue

`pending_disconnect_finishes_when_last_blocker_drain_empties_queue`

Queues a disconnect followed by one admitted old-generation raw frame. The first
drain installs the pending transition; the second drain consumes the last
generation-1 blocker and simultaneously returns the epoch advance/reconnect
completion. The queue is empty afterward and no future unrelated ingress is
required. A subsequent empty drain is `None`.

The production empty-queue branch also attempts ready pending completion as a
defense-in-depth liveness guard.

### Targeted regression E — stale post-transition disconnect

`stale_disconnect_after_epoch_advance_cannot_start_another_transition`

After a normal generation-1 -> generation-2 disconnect transition, a new
`queue_disconnected(epoch=1)` is rejected by the existing
`UnknownConnectionEpoch` contract. It does not enqueue work, create another
epoch advance, or mutate generation 2.

### Previously fixed regressions retained

The second remediation leaves intact and re-runs the prior independent-QA
coverage, including:

- 1.1 MB previous-generation oversized payload bounded diagnostic;
- sustained 32-overflow coalescing to attempt range `2..=33` / count 32;
- neighbor-stream serviceability;
- exact project identity gate for `bitget/usdt-futures/books50`;
- canonical same-`BookRef` second-writer rejection through
  `validate_registration()`;
- original QueueOverflow-before-epoch-advance FIFO regression;
- architecture grouped/alias namespace anti-bypass regressions.

### Second-remediation changed paths

Relative to second-QA-rejected head
`cd02f7c0e3aaa70b31546ad548ab48f46f268a9c`, the code/test repair before
this documentation commit changes only:

- `crates/market-data/src/ws_supervisor.rs`;
- `crates/market-data/tests/ws_supervisor.rs`.

This handoff file is the only additional path in the final documentation commit.

No `crates/domain/**`, `crates/recording/**`, accepted spec/ADR,
`tests/architecture.rs`, workflow, application composition, networking stack,
`docs/PROJECT_STATE.md`, canonical book, strategy or execution path is changed
by this second remediation.

### Verification during second remediation

Worker-local Rust/shell execution remains unavailable on this connector surface:

- `cargo fmt --all -- --check` — **NOT_RUN locally**;
- `cargo clippy --workspace --all-targets --locked -- -D warnings` —
  **NOT_RUN locally**;
- `cargo test --workspace --locked` — **NOT_RUN locally**;
- live public WebSocket smoke — **NOT_RUN**.

Historical intermediate exact-head run:

- `37540989334` on
  `1a4418ffb6b512d652340c99f976dd6c81cf0d03` — **FAIL** only because
  rust-fmt reported layout changes; on that same SHA rust-clippy and rust-tests
  completed **PASS**.

Code-only post-format exact-head run:

- **37541101768** on
  `c36b8c11aabe5e1b45012e0cbcdbe0a876f904e6` — **SUCCESS**:
  rust-fmt PASS, rust-clippy PASS, rust-tests PASS, Cargo.lock verification
  PASS, workspace build PASS, and clean-checkout verification PASS.

The code-only PASS becomes historical after this handoff commit and is not
transferred to the final documentation-containing SHA.

### Preserved boundaries after second remediation

Unchanged:

- **U-09 UNKNOWN / BLOCKED** — regular books50 quantity unit;
- **U-10 UNKNOWN / BLOCKED** — zero/delete semantics;
- **U-20 NOT_PROVEN / FORBIDDEN** — REST<->WS healing/stitching;
- **C-01 BLOCKED** — RPI canonical normalization;
- **C-03 UNKNOWN** — instruments-route relationship.

No quantity interpretation, `qty=0 -> DeleteLevel`, REST healing, RPI
normalization, private/authenticated API, strategy, execution, REC-001E or
REC-001F was introduced. The concrete DNS/TCP/TLS/WebSocket driver remains the
documented external-driver limitation and is not a finding/remediation here.

### Second re-QA gate

After the commit containing this section:

1. obtain the new immutable PR #34 head;
2. require fresh exact-head GitHub Actions **SUCCESS** for rust-fmt,
   rust-clippy and rust-tests;
3. require Cargo.lock verification, workspace build and clean checkout all PASS;
4. record the final SHA/run in PR #34 and Issue #20 mutable metadata;
5. return the existing PR from Draft to Ready;
6. perform a **full independent QA of the complete new immutable SHA**, not only
   this patch, with explicit retest of repeated disconnect, timeout+disconnect,
   old-generation ingress barrier, empty-queue completion, stale post-transition
   disconnect, and all previously fixed HIGH findings.

The historical PASS `37538436961` belongs to the second rejected head and is
not transferred.

Target worker stop state after the containing-head CI succeeds:
**READY_FOR_INDEPENDENT_QA**. The worker does not declare
READY_FOR_OWNER_REVIEW.


## Third independent-QA CHANGES_REQUIRED remediation

The third full independent QA reviewed exact immutable head
`ac6f0b74776ad597ce523a5825bd9fa3c4fa1352` and returned
**CHANGES_REQUIRED** for three transition-safety findings:

- **N1 HIGH** — reconnect/control completion was not atomic with respect to late deterministic time arithmetic failures;
- **N2 HIGH** — already queued same-generation control ingress could revive a generation after terminal Down;
- **N3 MEDIUM** — epoch/RecordNo exhaustion could leave a non-halted or endlessly retryable incomplete transition.

The same QA explicitly confirmed the earlier remediation set as fixed: F1 stale
raw boundedness, F2 sustained overflow/no global halt, F3 exact target identity,
F4 canonical single writer, original QueueOverflow-before-advance ordering, F6
architecture anti-bypass, repeated disconnect ownership, timeout+disconnect
ownership, empty-queue completion, and stale post-transition disconnect rejection.

This remediation continues the existing Issue #20 / claim / branch / PR #34
lineage with fast-forward commits only. No replacement claim, Issue, branch or
PR was created. No force-push, merge or auto-merge was performed.

### N1 — transition atomicity / derived-time failures

Disposition: **FIXED**.

Transition-critical deterministic calculations are now performed before the
irreversible part of their transition.

For ordinary live controls:

- connected path computes `receive + HEARTBEAT_INTERVAL_NS` before persisting
  `Transport::Up`;
- pong path computes the next heartbeat deadline before persisting a new Up;
- ping-timer path computes `receive + PONG_TIMEOUT_NS_V1` before persisting the
  timer or emitting `SendText("ping")`;
- overflow in any of those calculations sets the supervisor to explicit
  fail-closed `halted` and returns `TimeOverflow` before the corresponding
  new control record/state/live command.

For disconnect/reconnect:

`preflight_disconnect()` constructs a complete bounded pending plan before the
first terminal Down record for a new transition. The plan contains:

- the expected connection epoch;
- BookId;
- checked next connection epoch;
- checked next subscription epoch;
- checked next book epoch;
- checked increment of the reconnect-failure counter;
- deterministic reconnect delay;
- checked reconnect-not-before monotonic time.

It also preflights the required RecordNo capacity for the Down plus all three
epoch-advance records (or the smaller duplicate-Down case).

`finish_disconnect()` then:

1. validates the stored plan against still-current identity before persistence;
2. preflights capacity for all three epoch records;
3. persists connection/subscription/book epoch advances without mutating runtime
   identity between those persistence calls;
4. only after all three RecordingGate receipts succeed mutates the runtime to the
   new generation, clears pending ownership and exposes the single
   `ReconnectAfter` command.

There is no reconnect deadline/counter/epoch arithmetic after the epoch records
or runtime generation have been committed.

If a storage persistence/gate operation itself fails during the multi-record
completion, the existing persistence failure path halts fail-closed; in-memory
runtime is not partially advanced. Any already persisted WAL prefix remains an
honest incomplete prefix rather than being hidden or retried as if unrecorded.

Fourth-QA correction to this historical statement: halting and atomic runtime
tags did not prove delivery of the required Close. On `32820de...`, a completion
error could discard the same call's Down/Close result. The fourth remediation
below separates those outcomes and defines the remaining partial-prefix limits;
the third-remediation statement must not be read as full storage-error/API closure.

Timer admission was also tightened: TimerId is checked before queue mutation,
and timer frontier/queued flags are committed only after bounded ingress
admission succeeds.

### N1 targeted regressions

Added and executed:

- `disconnect_at_max_monotonic_halts_before_down_or_generation_commit`;
- `disconnect_near_reconnect_deadline_overflow_halts_before_down`;
- `connected_heartbeat_deadline_overflow_halts_before_up_or_subscribe`;
- `pong_heartbeat_deadline_overflow_halts_before_new_up_record`;
- `ping_timer_pong_deadline_overflow_halts_before_timer_or_ping`.

They cover exact `u64::MAX`, near reconnect-deadline overflow, connected,
pong and ping-timer deadline overflow. Each requires either no irreversible
transition record/state at all or explicit fail-closed halt; none leaves a
non-halted stranded generation or loses a required command after a late
deterministic calculation.

This claim covered the tested deterministic calculations only. Fourth QA found
required-command loss after a later storage error, addressed separately below.

### N2 — terminal-generation control rule

Disposition: **FIXED**.

After the first accepted terminal Down for connection epoch N, already queued
ingress for epoch N is separated into diagnostic persistence from operational
effects.

- queued `Connected(N)` no longer persists a new `Transport::Up`, no longer
  sets runtime Up/AwaitingAck, and never emits a stale subscribe;
- queued `Pong(N)` no longer persists a new Up, changes heartbeat state, or
  emits a live effect;
- both are consumed explicitly as
  `SupervisorEvent::ObsoleteControl { stream, epoch }`;
- queued `PingTimer(N)` remains truthfully recordable through the accepted
  `Control::Timer`, but does not emit ping and does not install a pong deadline;
- a queued PongTimeout remains recordable as Timer plus the existing duplicate
  terminal Down semantics and cannot own a second transition;
- raw input admitted for the pending terminal generation is still persisted with
  exact raw provenance, but is returned as `ObsoleteRawRecorded` and is not
  decoded into subscription/market-state progression;
- QueueOverflow provenance remains recorded/degraded under the previously fixed
  ordering and still acts as a persistence barrier.

The accepted WAL/domain contract has no no-effect "obsolete connected/pong"
transport record. Persisting `Transport::Up` before epoch advance would replay
as an operational Up and violate the terminal-generation rule. REC-001D therefore
does **not** invent a new WAL/domain record or falsely encode the observation as
Up/Down/Gap. It surfaces the consumed control explicitly at the supervisor API
as `ObsoleteControl`, with no WAL Up and no live command. Timer observations,
which do have a truthful existing no-revive record, continue to be persisted.

This is an explicit runtime diagnostic boundary, not a claim that an obsolete
Connected/Pong has a replayable WAL record kind.

### N2 targeted regressions

Added and executed:

- `terminal_down_suppresses_queued_connected_without_subscribe_or_up_record`;
- `terminal_timeout_suppresses_queued_pong_without_up_or_heartbeat_revival`;
- `terminal_down_records_queued_ping_timer_without_sending_ping`;
- `terminal_control_barrier_between_duplicate_disconnects_finishes_once`;
- `terminal_control_of_one_stream_does_not_block_neighbor_operational_control`.

The tests cover the requested
`Disconnected -> Connected -> Disconnected`,
`PongTimeout -> Pong`,
`Disconnected -> PingTimer`,
control-as-barrier ordering, and multi-stream isolation. They prove one logical
generation advance/reconnect, terminal Down preservation, no stale subscribe or
ping, and independent neighbor control.

The prior raw barrier regression remains in the same suite and now also benefits
from terminal raw diagnostic treatment.

### N3 — epoch / RecordNo / transition-critical counter exhaustion

Disposition: **FIXED**.

Counter exhaustion is now explicit terminal safety behavior.

Epoch exhaustion:

- ConnectionEpoch, SubscriptionEpoch and BookEpoch next values are checked in
  disconnect preflight before terminal Down persistence;
- any exhaustion sets `halted=true` and returns the accepted
  `IdentityError::CounterExhausted` without wrapping/resetting the epoch and
  without starting a partial disconnect transaction.

Reconnect attempt exhaustion:

- `reconnect_failures` no longer uses saturation for transition ownership;
- the next reconnect attempt uses checked `u32::checked_add`;
- failure is fail-closed `CounterExhausted("ReconnectAttempt")` before Down.

RecordNo exhaustion:

- supervisor persistence requires a checked successor RecordNo before writing the
  current frame; it never wraps and does not silently turn the frontier into
  `None` after a successful max record;
- `ensure_record_capacity(count)` preflights multi-record disconnect work;
- insufficient capacity for Down + the complete three-record epoch transition
  halts before Down;
- insufficient capacity at a later completion boundary halts before any epoch
  record from that completion;
- after an exhaustion error, `ensure_running` makes repeated drain attempts
  return `Halted`, so there is no non-halted infinite retry loop and no duplicate
  transition records/live commands.

This is a conservative supervisor safety policy; it does not wrap/reset counters,
start a new archive, or change the accepted WAL/domain counter contract.

### N3 targeted regressions

Added and executed:

- `max_connection_epoch_disconnect_halts_before_down`;
- `max_subscription_epoch_disconnect_halts_before_down`;
- `max_book_epoch_disconnect_halts_before_down`;
- `max_record_frontier_terminal_disconnect_halts_without_partial_down`;
- `insufficient_record_capacity_for_full_disconnect_halts_before_down`.

The RecordNo tests also assert repeated drain after exhaustion returns terminal
Halted behavior without adding records.

### Transition atomicity audit result

The post-repair supervisor was audited specifically for:

- `persist_record(...)?` followed by transition-critical checked time/counter arithmetic;
- runtime identity mutation between the three epoch persistence operations;
- pending ownership cleared before the last fallible transition operation;
- live commands accumulated before a later deterministic calculation could fail;
- timer/counter mutation before queue admission.

The remaining checked arithmetic after this repair is either pre-persistence
transition validation or bounded ingress/accounting logic that has not committed
a reconnect/epoch transition. Reconnect deadline/counter/epoch computations no
longer occur after the durable transition records or generation mutation.

### Previously fixed coverage retained

The same exact-head test run continues to execute the earlier independently
confirmed regressions, including:

- 1.1 MB previous-generation oversized payload bounded diagnostic;
- sustained 32-overflow coalescing to `2..=33` / count 32;
- neighbor-stream serviceability;
- exact `bitget/usdt-futures/books50` identity negatives;
- canonical same-BookRef second-writer rejection;
- original QueueOverflow-before-epoch-advance regression;
- repeated same-epoch disconnect ownership;
- timeout + duplicate disconnect ownership;
- raw old-generation barrier;
- empty-queue pending completion;
- stale post-transition disconnect rejection;
- architecture namespace/alias/grouped-import anti-bypass tests;
- accepted `WalWriter/WalReader` definition/control/raw ordering regression.

### Third-remediation changed paths

Relative to third-QA-rejected head
`ac6f0b74776ad597ce523a5825bd9fa3c4fa1352`, the code/test remediation before
this documentation commit changes only:

- `crates/market-data/src/ws_supervisor.rs`;
- `crates/market-data/tests/ws_supervisor.rs`.

This handoff file is the only additional path changed by the final documentation
commit.

No `crates/domain/**`, `crates/recording/**`, accepted spec/ADR,
architecture test, workflow, application composition, networking dependency,
`docs/PROJECT_STATE.md`, canonical-book, strategy or execution path is changed
by the third remediation.

### Verification during third remediation

Worker-local Rust/shell execution remains unavailable on this connector surface:

- `cargo fmt --all -- --check` — **NOT_RUN locally**;
- `cargo clippy --workspace --all-targets --locked -- -D warnings` —
  **NOT_RUN locally**;
- `cargo test --workspace --locked` — **NOT_RUN locally**;
- live public WebSocket smoke — **NOT_RUN**.

Historical third-remediation runs:

- `37582711865` on
  `70a72ea4386ad734d20456b8d81cb4fbd75e5ffc` — **FAIL** only because
  rust-fmt reported layout changes; rust-clippy and rust-tests were **PASS**;
- `37582833822` on
  `91e40db756d985ab9e5e3fdbbfc2a661f2ce44f5` — **FAIL** only because
  rust-fmt reported one remaining layout change; rust-clippy and rust-tests were
  **PASS**;
- code-only exact-head run **37582919739** on
  `5a1d3f851d68f829586010c55c8fe90b0d2c70d7` — **SUCCESS**:
  rust-fmt PASS, rust-clippy PASS, rust-tests PASS, Cargo.lock verification
  PASS, workspace build PASS and clean-checkout verification PASS.

The code-only PASS becomes historical after this handoff commit and is not
transferred to the final documentation-containing SHA.

The earlier run `37541263829` remains historical PASS evidence only for the
third QA-rejected `ac6f0b...` head.

### Preserved boundaries after third remediation

Unchanged:

- **U-09 UNKNOWN / BLOCKED** — regular books50 quantity unit;
- **U-10 UNKNOWN / BLOCKED** — zero/delete semantics;
- **U-20 NOT_PROVEN / FORBIDDEN** — REST<->WS healing/stitching;
- **C-01 BLOCKED** — RPI canonical normalization;
- **C-03 UNKNOWN** — instruments-route relationship.

No quantity interpretation, `qty=0 -> DeleteLevel`, REST healing, RPI
normalization, private/authenticated API, strategy, execution, REC-001E or
REC-001F was introduced. Concrete DNS/TCP/TLS/WebSocket ownership remains the
documented external-driver limitation and is not a finding/remediation here.

### Third re-QA gate

After the commit containing this section:

1. obtain the new immutable PR #34 head;
2. require fresh exact-head GitHub Actions **SUCCESS** for rust-fmt,
   rust-clippy and rust-tests;
3. require Cargo.lock verification, workspace build and clean checkout all PASS;
4. record the exact final SHA/run in PR #34 and Issue #20 mutable metadata;
5. return the existing PR from Draft to Ready;
6. run a **full independent QA of the complete new immutable SHA**, not patch-only,
   with mandatory retest of N1/N2/N3 and all previously fixed findings.

Target worker stop state after containing-head CI succeeds:
**READY_FOR_INDEPENDENT_QA**. The worker does not declare
READY_FOR_OWNER_REVIEW.

## Fourth independent-QA CHANGES_REQUIRED remediation

Fourth independent QA rejected immutable head
`32820de289ccc4c8f1966cbb9a5726d4594f14da` with H1 HIGH and H2 MEDIUM.
Run `37583133012` is historical PASS only for that rejected head; it is
not acceptance evidence for this repair.

Preflight verified actual main
`39ff0dba797eb010586238ef06fb80e996340401`, PR #34 head and branch ref
`32820de289ccc4c8f1966cbb9a5726d4594f14da`; comparison found zero later
commits. Existing claim `6024304772`, recovery `6025249885` and
third-remediation completion `6032549018` remain the same lineage.
No replacement claim, Issue, branch or PR, force-push, merge or auto-merge.

### H1 — required Close lost on completion persistence error

Disposition: **FIXED**, subject to fresh containing-head CI and independent QA.

The public signature remains
`drain_one(&mut self, &mut impl RecordSink) -> Result<Option<DrainResult>, SupervisorError>`.
No new supervisor DTO, setter, effect queue, Record, WAL or domain contract is
introduced. The observable call boundary is deliberately corrected:

1. First terminal Down must receive an exact receipt covering the configured gate.
2. That ingress call returns `Ok(Some(...))` with Down and its one required
   `TransportCommand::Close`. No completion persistence follows that result
   construction inside the call.
3. Already-admitted same-generation raw/loss/control barriers drain separately.
   Their results also return without a subsequent completion attempt.
4. The next drain checks pending transitions before popping ingress, even when
   ingress is empty. It completes at most one ready stream per call.
5. Completion persists connection, subscription and book EpochAdvance records.
   Only three validated gate receipts allow the runtime tag to change, pending
   ownership to clear, and one ReconnectAfter to return.
6. A completion error returns the original typed persistence/receipt/gate error
   and sets halt. Previously returned Close and neighbor commands remain owned
   by the caller; an error cannot retract them. No reconnect/subscribe/ping is
   returned on failure, and repeated calls return Halted without persistence.

The driver must continue the existing drain loop until None or error, including
a further call after the last ingress empties the queue. This is documented on
the production method. An ingress Down result does not imply completed epoch
advance. Returning a Close command proves caller visibility, not physical socket
closure or successful external delivery; the external driver remains outside
this PR. No promise that a driver closes upon arbitrary SupervisorError is used.

Duplicate Down still persists as a separate external observation but owns no
second Close/transition. Timeout persists Timer then Down and returns its Close
before completion. Obsolete Connected/Pong and terminal PingTimer cannot revive
the pending generation. QueueOverflow reaches the old scope before advance;
accepted GapScopeTransition semantics are unchanged.

### H1 fault-injection matrix and prefix interpretation

A controlled test sink distinguishes every attempted frame from frames whose
exact receipt reaches Durable. It never labels an error, mismatched receipt or
weak gate as a confirmed record, including when bytes may have been received.

All nine combinations are covered for immediate Disconnected and independently
for PongTimeout (18 completion-fault cases):

| EpochAdvance failure point | persist error | receipt mismatch | insufficient gate |
|---|---|---|---|
| connection | injected original Persistence error | explicit expected/actual mismatch | required Durable / achieved Written |
| subscription | same, after confirmed connection prefix | same, after confirmed connection prefix | same, after confirmed connection prefix |
| book | same, after confirmed connection/subscription prefix | same, after confirmed connection/subscription prefix | same, after confirmed connection/subscription prefix |

Every case checks confirmed terminal Down, caller-visible Close in a prior
successful production drain, the exact original error, halt, unchanged complete
runtime tag, no reconnect/live commands, and no retry/persistence on subsequent
drains. Successful earlier epoch writes remain an honest incomplete prefix.
The failing attempt is not reported as confirmed.

The API does not return a partial DrainResult with Err and does not promise a
supervisor-only exact partial-prefix accessor. The caller's RecordSink and
storage recovery retain evidence of successful writes and possible ambiguous
physical bytes. Err does not prove the failing frame absent, roll back earlier
records, or certify a complete archive. No automatic retry, epoch repair, archive
reset or fabricated loss record fills that incomplete prefix.

Additional coverage exercises Raw/QueueOverflow/control barriers, duplicate
disconnect ownership, success with one Close/three advances/one reconnect and
completion on empty ingress. Neighbor tests check both already-returned B
subscribe/ping commands before A failure and B ingress still queued when ready A
fails: completion does not pop/persist B and erase a command. A global terminal
storage failure does prevent subsequent operation; this is distinct from loss of
a command already returned to the caller.

Integration regressions:
- `disconnected_close_survives_every_epoch_completion_storage_failure` (9 cases);
- `pong_timeout_close_survives_every_epoch_completion_storage_failure` (9 cases);
- `close_survives_epoch_completion_faults_after_terminal_ingress_barriers`
  (36 cases across Raw, QueueOverflow, Timer and duplicate Down barriers);
- `empty_queue_deferred_completion_emits_one_reconnect_after_duplicate_and_barriers`;
- `neighbor_commands_are_returned_before_failing_other_stream_completion` (9 cases).

Together these run 63 fault-injection cases plus the successful mixed-barrier
transition. They are test-sink boundary evidence, not physical crash/sync tests.

Existing tests that assumed same-call completion now explicitly drain completion
separately and retain old-tag/Down/barrier/no-revive assertions.

### H2 — CaptureAttemptNo exhaustion

Disposition: **FIXED**, subject to fresh containing-head CI and independent QA.

The archive-long capture frontier uses checked addition. Exhaustion now calls
`halt_with(CounterExhausted("CaptureAttemptNo"))` before frontier, queue,
raw-byte/frame/item accounting or loss provenance changes. It does not wrap,
reset, invent an attempt/GAP, change archive or restart at generation advance.

Private unit setup reaches the impractical max boundary without a production
setter. Tests cover current and supported previous generation with an admitted
raw still queued, confirm all accounting/frontiers unchanged, and repeat
raw/pong/Connected/Disconnected/tick/start/drain calls: Halted, no new records
or commands, no retry loop. The previously confirmed sink prefix stays unchanged.

Near-max coverage confirms MAX-2, MAX-1 and MAX raw attempts across ordinary
generation advance, including previous-generation diagnostic raw. The frontier
is archive-long and MAX remains a valid last attempt; only its successor halts.

Tests:
- `capture_attempt_exhaustion_halts_current_and_previous_generation_without_mutation`;
- `near_max_capture_attempt_progression_is_archive_long_across_generation_change`.

### Audit of related error paths

This is a bounded reachability audit, not a claim that all storage/driver error
boundaries or physical-delivery guarantees are proven.

| Path | Reachable outcome / disposition |
|---|---|
| Down/Close followed by tail completion | H1 removed: result returns before completion; next drain owns any completion error. |
| Connected and PingTimer commands | Checked time precedes persistence; commands follow validated receipt; no subsequent completion can erase them. |
| Successful epoch completion | Three receipts precede tag commit/ReconnectAfter; one completion per call, no later fallible operation. |
| start_commands | No fallible operation follows Connect construction. |
| PongTimeout Timer then Down | A Down error can leave confirmed Timer; no confirmed Down/required Close yet. Original error and halt, no retry. |
| rejected stale diagnostic Raw then GAP | A GAP error can leave confirmed empty Raw. No mandatory command existed; prefix remains incomplete. |
| rejected/current/subscription-failed Raw then GAP | A GAP error can leave confirmed Raw. Original error and halt; no mandatory command existed. |
| continuity Raw then SourceGap | GAP error can leave confirmed Raw and changed internal classifier; halt blocks use. No runtime rollback claim. |
| EpochAdvance x3 | Zero/one/two validated epoch records may precede error. Complete runtime tag stays old; possible physical failed-frame bytes are ambiguous. |
| post-persist missing-stream lookups | Unreachable under private fixed registration maps; admitted ingress refers to registered streams, handlers remove no entries, sink cannot reenter mutable supervisor. |
| CaptureAttemptNo | Reachable exhaustion now terminal before admission/accounting mutation. |
| TimerId/RecordNo/epochs/reconnect attempt | Existing checked terminal paths retained. |
| second raw-byte checked addition | Same successful precheck, no intervening mutation; unreachable overflow under hard queue caps. |
| QueueGap loss_count checked addition | Count cannot exceed allocated archive attempts; allocating a successor at max fails first at CaptureAttemptNo. Not a reachable overflow in valid production state. |
| queue-reserve arithmetic | Constructor-fixed at <=4 streams and <=256 items; invariant failures rather than reachable external exhaustion. |

No adjacent contract change or blocker was required. This section supersedes
any broader interpretation of the third-remediation fail-closed storage claim;
the historical reasoning and rejected SHAs remain preserved above.

### Scope, verification and next gate

Fourth remediation changes only:
- `crates/market-data/src/ws_supervisor.rs`;
- `crates/market-data/tests/ws_supervisor.rs`;
- `docs/handoffs/REC-001D.md`.

F1-F6, repeated-loss liveness, N1-N3 and real WalWriter/WalReader regressions
remain in the full suite. Architecture tests, domain, recording, accepted
specs/ADR, workflow, application composition and dependencies are unchanged.

Shell/read-only diff checks are available in this continuation. Rust/Cargo is
absent: attempted mandatory commands each returned shell exit 127
(`cargo: command not found`), so:
- cargo fmt — **NOT_RUN locally**;
- cargo clippy warnings-as-errors — **NOT_RUN locally**;
- cargo test/workspace build — **NOT_RUN locally**;
- live public WebSocket smoke — **NOT_RUN**.

At file authorship, the containing SHA and its CI do not yet exist. After the
final handoff commit, obtain that immutable SHA and require fresh exact-head
CI SUCCESS: fmt, clippy -D warnings, Cargo-generated lockfile verification,
workspace build, workspace tests/real CLI and clean checkout. Exact final SHA
and run ID are recorded in mutable PR #34 and Issue #20 metadata, following the
accepted no-self-referential-SHA handoff policy. Historical PASS is not transferred.

Preserved without new assumptions:
- U-09 = **UNKNOWN / BLOCKED**;
- U-10 = **UNKNOWN / BLOCKED**;
- U-20 = **NOT_PROVEN / FORBIDDEN**;
- C-01 = **BLOCKED**;
- C-03 = **UNKNOWN**.

No REST healing, RPI normalization, quantity/delete inference, canonical book
mutation, private API, strategy/execution or REC-001E/F. This PR remains a
deterministic supervisor with an external socket-driver boundary, not a runnable
live connector.

After fresh final-head CI: update existing PR #34 metadata, return Draft to
Ready, post Issue #20 fourth-remediation completion, then stop as
**READY_FOR_INDEPENDENT_QA**. Full independent QA must review the entire new
immutable head, with H1/H2 fault injection and every previous fix retested.
The worker does not declare READY_FOR_OWNER_REVIEW or authorize merge.
