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

Fifth-QA correction: the preceding first-loss claim was too broad. The reserve
constrains Raw and ordinary control admission, but `push_loss_ingress` itself
can consume every slot with incompatible stale diagnostics. Q1 below remains
BLOCKED; the historical F2 consecutive-overflow/neighbor regression does not
prove arbitrary diagnostic saturation safe.

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

Intermediate fourth-remediation run `37586813003` on
`6d5d06e68ce3d557b2acc64d4af151de5dce55d6` was **FAIL** only in rust-fmt.
On that head, rust-clippy and workspace tests/real CLI, Cargo.lock verification,
workspace build and clean checkout were **PASS**. The exact rustfmt differences
were applied before this containing commit; that intermediate evidence is not
transferred to the final head.

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

## Fifth independent-QA remediation — Q2 repair / Q1 contract blocker

This is the existing Issue #20 / parent #5 lineage: claim `6024304772`,
recovery `6025249885`, branch `feat/REC-001D-ws-supervisor`, PR #34 and
fourth-remediation completion `6033106016`. The attached fifth-QA task packet
rejects immutable `82742c2e2cbf960ca16dca7ed6b26921903d3f66` for Q1 HIGH
and Q2 MEDIUM. No separate fifth-QA report was attached or posted in the
current PR/Issue threads; the supplied detailed findings/reproduction packet
is the available fifth-QA evidence. No unseen report content is claimed.

Preflight independently verified actual main
`39ff0dba797eb010586238ef06fb80e996340401`, PR head and branch ref
`82742c2e2cbf960ca16dca7ed6b26921903d3f66`, with zero later commits.
Local policy/source/test/handoff Git blob hashes matched that immutable head.
The existing PR was returned to Draft. No new claim, branch, Issue or PR,
force-push, merge, auto-merge or REC-001E/F work was performed.

### Q1 — diagnostics exhaust first current-loss capacity

Disposition: **BLOCKED / NOT_FIXED**. Required behavior remains **FAIL**.
The production loss/admission paths are unchanged in this cycle. Q1 must not
be treated as fixed because Q2 or the containing-head CI passes.

`push_loss_ingress` can fill `max_total_items`; a later current raw then loses
its received payload at `queue_gap_loss` error, leaves the candidate attempt
uncommitted, and can reuse that attempt in a resumed current suffix without
a QueueOverflow barrier. Existing typed QueueExhausted alone is insufficient.
Already admitted work remains drainable, which does not make this loss safe.

The relevant accepted constraints are:
- WAL v1 section 3/RawInput requires each diagnostic Raw's original receive
  Context; empty Raw is representable, but invented per-attempt samples are not.
- WAL v1 section 4.1 accounts current and old-tag attempts for the whole archive;
  only QueueOverflow covers a local attempt hole, and known ranges must start
  exactly at the next frontier. Current local loss for a mismatching tag fails
  GapScopeTransition. Unknown/source gaps cannot cover missing local attempts.
- Loss cannot be coalesced across incompatible tags/streams, noncontiguous
  attempts or intervening Raw/control barriers; admitted observations cannot
  be evicted or reordered.
- DataHealth's recording owner is ArchiveId/capture session. An impossible
  GAP write fails recording and blocks publication; accepted contracts do not
  specify a scope-local permanent capture failure with continued neighbor
  capture and externally enforced archive Incomplete/Unknown finalization.

A bounded batch preserving every original stamp could handle a finite run of
same-tag stale rejects. It does not resolve arbitrary incompatible/barrier-
separated saturation. A protected first-loss reserve likewise prevents one
failure but does not define disposition when diagnostics need more bounded
metadata than remains. Moving an item outside the queue while reporting only
queue length would undercount the advertised total-item cap.

No new scoped-failure API policy, extra uncounted loss lane, WAL kind, fabricated
GAP or driver retain/retry obligation was silently introduced. A generic global
halt would freeze admitted drain and unrelated streams, violating this packet.

Concrete policy proposal, requiring Integrator/Architecture acceptance:
1. Define a separately counted bounded loss lane and ordering/admission barrier,
   plus terminal disposition when incompatible received inputs exhaust even
   that lane. State its total metadata/item/byte bounds explicitly.
2. Alternatively define a permanent affected-capture failure through the
   supervisor API: retain one exact consumed failure identity, freeze that
   scope across reconnects, drain its admitted records diagnostically, preserve
   neighbor service, and require the archive owner to expose Incomplete/Unknown
   and forbid successful finalization/publication after the unrecordable input.
   This needs an explicit archive-wide recording-failure/finalization policy;
   QueueExhausted + temporary Degraded is not enough.

Neither proposal is implemented or presented as an accepted contract.
The existing invariant forbidding unsafe capture continuation remains the
required outcome. Q1 depends on choosing/accepting this policy, not on a
networking dependency or a larger ordinary queue.

Three executable forensic public-API reproductions deliberately assert the
observed outstanding defect, not Q1 correctness:
- `q1_blocker_single_stream_legal_five_reproduces_consumed_attempt_reuse`:
  epoch 1->2, reconnect/ack/snapshot, five separately stamped 1.1 MB previous-
  generation inputs occupy five zero-payload diagnostics; consumed current
  candidate 8 gets no GAP/invalidation and continuous current suffix reuses 8.
- `q1_blocker_two_stream_legal_nine_stale_a_suppresses_first_b_loss`:
  nine bounded old-A diagnostics block B's first current loss; B's consumed
  candidate 3 is reused by a continuous suffix with no B loss barrier.
- `q1_blocker_noncoalescing_saturation_preserves_admitted_raw_control_and_exact_scopes`:
  Raw/control/stale/current barriers preserve admitted FIFO and B subscribe;
  exact separate ranges A5/B3/A7/B5 remain unmerged, while repeated rejected
  A inputs reuse candidate 8 at full capacity. Zero stale payload bytes and
  exact stamps/tags/frontiers/frame/byte/item counters are checked.

Passing these reproduction tests means the required Q1 behavior is still
**FAIL**. Their assertions must be replaced by accepted safe-disposition
regressions when Q1 is repaired. The existing F2 test separately checks
admitted attempt 1 / rejected 2..=33 / count 32 and neighbor serviceability.

### Q2 — heartbeat timer ownership and cancellation

Disposition: **FIXED in code**, subject to fresh containing-head CI and full
independent QA. The public supervisor API and accepted domain/WAL are unchanged.

Private per-stream boolean flags are replaced by optional TimerId owners.
Timer admission checks TimerId and commits its owner only after bounded queue
admission succeeds. IDs remain checked, archive-long and never reset at epoch
change. An operational timer must match connection epoch, Up/no terminal
transition, timer type, exact current deadline and its TimerId owner.

After persisted Pong/Connected replaces the schedule, previously queued timers
still persist their original existing Control::Timer/receive Context and emit
HeartbeatTimerRecorded. They send no ping, create no pong deadline, do not
preflight disconnect, and mutate no schedule/owner. Matching TimerId matters
even when old and new deadlines have the same numeric value. Terminal Down and
successful epoch completion clear ownership; a canceled timeout cannot create
a second Down/Close/epoch transition/reconnect.

This supersedes historical wording that every queued terminal PongTimeout
necessarily records another Down: a timeout without active ownership now
records Timer only. Actual duplicate Disconnected observations still retain
their existing Down recording and single logical transition ownership.

Deadline policy is local FIFO observation order, not an exchange guarantee.
A persisted Pong dequeued before timeout replaces the heartbeat schedule at
D-1, D or D+1. A timeout observation becomes eligible at tick >=D and remains
terminal when dequeued first; subsequent Pong is the existing ObsoleteControl.
Original sample order may differ from API admission order; samples are never
rewritten to manufacture an exchange deadline guarantee.

Canceled Timer still legitimately advances recorded evaluation time for the
accepted downstream reducer. Private heartbeat ownership is a live supervisor
admission/effect rule, not a new persisted cancellation schema. These tests do
not prove a complete replay heartbeat-command reducer or physical driver send;
REC-001F composition and external delivery remain outside this task.

Nine Q2 integration regressions:
- `pong_before_queued_timeout_preserves_heartbeat_at_deadline_boundaries`;
- `pong_cancels_old_ping_timer_without_clearing_equal_deadline_new_owner`;
- `canceled_timeout_does_not_change_new_heartbeat_cycle_or_queued_owner`;
- `obsolete_timers_skip_impossible_operational_time_and_epoch_preflight`;
- `timer_cancellation_of_one_stream_preserves_neighbor_timeout`;
- `timeout_before_queued_pong_remains_terminal_under_fifo_policy`;
- `connected_schedule_replacement_cancels_already_queued_ping_timer`;
- `disconnect_cancels_queued_timeout_without_duplicate_down_or_close`;
- `active_and_canceled_timers_preserve_all_storage_failure_dispositions`.

The last test covers 12 combinations: active/canceled x PingTimer/PongTimeout x
persist error/mismatched receipt/insufficient gate. Each requires the original
typed error, halt, no operation commit/command, no confirmed failed record and
no repeated persistence. Obsolete timers at u64::MAX time/epochs prove they do
not perform nonexistent operational preflight. Other tests verify matching-owner
single actions, deadline boundaries, repeated ticks, next cycle and neighbor
isolation. The original 63 H1 fault cases and H2 private unit tests remain.

### Related-path audit and remaining limits

| Path | Reachable outcome |
|---|---|
| Diagnostics consume loss reserve | Q1 remains reachable and BLOCKED; first-loss claim corrected above. |
| Current rejected raw with available loss slot | Exact known QueueOverflow range reaches the gate before queued current suffix; tail coalescing checks stream/tag/consecutive attempt and never crosses a barrier. This does not prove saturation safe. |
| Unknown connection/unsupported epoch | Rejected before attempt allocation; only registered current/previous raw is within the supported capture path. |
| Exact pong bytes | Control observation, intentionally no CaptureAttempt allocation. |
| Non-halting ordinary control capacity error | QueueExhausted leaves previously admitted work drainable; no unprovided driver retain/retry guarantee or claim that rejected external controls were persisted. |
| queue_tick partial multi-stream admission | Earlier admitted stream timer retains its owner and can drain; later capacity error constructs no command and commits no rejected owner's counter/flag. |
| Obsolete timer after Pong/Connected/Down | Truthful Timer persistence only; no preflight or mutation of newer owners/deadlines. |
| Active timer | Checked operational arithmetic precedes persistence. Timeout may confirm Timer before a failed Down, yielding explicit error/halt and an incomplete prefix. |
| Mandatory Close/subscribe/ping/reconnect | No reachable fallible operation follows construction before return; H1's separate completion call is retained. Private fixed maps make post-persist missing-stream lookups unreachable. |
| Raw pop before storage failure | Accounting can decrement before failure; halt forbids retry/publication. No rollback or complete-archive claim. |
| Multi-record persistence | Timer then Down, empty stale Raw then UnknownGap, Raw then source/decode Gap, epochs x3 may retain a confirmed prefix plus ambiguous failed bytes. Original errors halt; failed receipts are never called confirmed. |
| Checked counters | CaptureAttempt/TimerId/RecordNo/epoch/reconnect exhaustion remain terminal checked paths. QueueGap count overflow is unreachable before allocated attempt exhaustion in valid state. |

This is a reachability audit of the named supervisor paths, not proof of every
physical storage/driver boundary. Q1 remains an explicit blocking exception;
green CI cannot establish full bounded-loss compliance.

### Verification, scope and next gate

Only supervisor source, its integration tests and this handoff change. No
architecture tests, domain, recording, accepted specs/ADR, workflow, composition
or networking dependencies change. Previous F1-F6, liveness/N1-N3, H1/H2 and real
WalWriter/WalReader tests remain in the full workspace suite.

Local mandatory commands were attempted and each returned shell exit 127
(`cargo: command not found`): fmt, clippy -D warnings and workspace tests are
**NOT_RUN locally**. Local workspace build is **NOT_RUN locally**. Newly added
Rust reproductions/tests are **NOT_RUN locally**. Live WS smoke is **NOT_RUN**;
the accepted external socket-driver boundary remains, and this is not a runnable
live connector.

The containing immutable SHA and fresh CI run are recorded after this handoff
commit in PR #34 / Issue #20 metadata, with no self-referential SHA. Require
fmt, clippy -D warnings, Cargo.lock verification, workspace build, workspace
tests/real CLI and clean checkout on that exact final SHA. Historical
`37587058448` belongs only to rejected `82742c2e...` and does not transfer.

Intermediate run `37591605000` on `0dcf18b7a1269c2822311c2ceed2db88cd0d4f58`
executed all 56 supervisor integration tests, the H2 private tests, full
workspace tests/real CLI, Cargo.lock verification, workspace build and that
job's clean checkout: **PASS**. The three Q1 forensic reproductions confirmed
required behavior **FAIL**. Rust-fmt was **FAIL**; its exact formatter diffs
were applied before this containing commit. Clippy was still in progress at
file authorship. No intermediate result transfers to the final head.

Later CI on formatted `0797a6e1da3aba3a643cb9c15f651ecad166117f`, run
`37591808120`, passed fmt, all workspace tests/real CLI, lockfile, build and
those clean-checkout checks. Clippy failed only the two new forensic tests'
constant `chunks_exact(2)` uses (`chunks_exact_to_as_chunks`). They were changed
to `as_chunks::<2>().0.iter()` before this containing commit. This intermediate
run is historical evidence, not the final exact-head gate.

Preserved: U-09 UNKNOWN/BLOCKED, U-10 UNKNOWN/BLOCKED,
U-20 NOT_PROVEN/FORBIDDEN, C-01 BLOCKED, C-03 UNKNOWN.
No REST healing, RPI normalization, quantity inference, zero-to-DeleteLevel,
canonical book mutation, private API, strategy/execution or REC-001E/F.

Worker disposition: **BLOCKED on Q1 contract policy; Q2 prepared for independent
QA**. The existing PR remains Draft while Q1 is unresolved. Do not mark the
whole REC-001D READY_FOR_INDEPENDENT_QA or READY_FOR_OWNER_REVIEW on the strength
of the partial Q2 repair. After an accepted Q1 policy/remediation, the mandatory
next gate remains full independent QA of the entire new immutable head, including
Q1/Q2, all 63 H1 faults, H2 and every prior fix. No merge/auto-merge is authorized.

## Bounded Q1 design revision — ADR 0003 proposal

Current disposition: **DESIGN_PROPOSED / Q1_BLOCKED**. Architecture direction:
**ARCHITECTURE_DIRECTION_SET / DESIGN_REVISION_REQUIRED**. Q1 remains
**BLOCKED / NOT_FIXED**; Q2 remains **FIXED_IN_CODE / QA_PENDING**. This section
supersedes the earlier open alternatives for the next design review, without
changing production behavior or erasing the forensic evidence above.

Existing lineage is unchanged: Issue #20 / parent #5, claim `6024304772`,
recovery `6025249885`, branch `feat/REC-001D-ws-supervisor`, Draft PR #34,
incoming partial-completion record `6033824631`. Preflight verified actual main
`39ff0dba797eb010586238ef06fb80e996340401` and branch/PR head
`91e3702f75346e0f310532359a06daa9db701829`, with zero later commits; PR was
open/Draft, unmerged, auto-merge unset. Current GitHub policies/contracts were
read against the immutable head. A stale local WAL-spec copy was refreshed
from its exact GitHub blob before reasoning; no accepted file is changed in
the proposal commit. This workspace is a materialized snapshot rather than a
git checkout; the commit uses the existing head tree with an expected-head
lease, and its exact two-path diff must be verified remotely.

The selected proposal is [ADR 0003 — terminal capture saturation and archive
session authority](../adr/0003-ws-capture-saturation.md). It specifies:

- irreversible affected-stream termination, one exact diagnostic first-failure
  identity and consumed candidate without Raw/GAP/accounted-frontier fiction;
- an archive/session-wide recording-failure latch, diagnostic-only admitted
  drain/neighbor recording, and revocation of all publication/finalization;
- a full `W + N + 1 <= M` ledger including reserved stream/archive owners,
  pending/in-flight jobs and leases, explicit metadata/payload/buffer bounds,
  reporting API, legal cap5/cap9 and mixed/F2 progressions;
- existing RecordingEvidence as an optional truthful earlier-watermark failure
  observation after the admitted record-bearing ingress prefix, never missing-attempt/GAP
  coverage; marker failure remains explicit without a durability/rollback claim;
- owner-bound sink/writer, unique borrowed SessionTurn, opaque command leases,
  sealed current-state publication guard, one-use quiescence and enforced seal
  checks. Mandatory Close remains dispatchable through storage/closure failure;
- a one-segment bounded owner profile, pollable diagnostic-close lifecycle, truthful
  reader status/quality separation and crash/external-inventory limits;
- complete API outcomes, compatibility/minimum future-path matrix and an
  acceptance matrix for independent QA after approved implementation.

The proposal explicitly names deltas requiring approval: saturation policy v2,
envelopes/borrowed input/bound sink, scoped CaptureAttempt exhaustion instead
of the prior global-Halted H2 disposition, Close-dispatch-dependent H1 completion
ownership, terminal admission Close without a claimed durable Down, irreversible
replay failure classification and canonical owner boundaries. Existing H1
durable-result, H2 checked-error/frontier/no-wrap/no-reuse and Q2 cancellation
invariants remain required. No test assertion change is made in this docs task.

Allowed/changed paths in this revision are exactly:

- `docs/adr/0003-ws-capture-saturation.md` (new proposal);
- `docs/handoffs/REC-001D.md` (this delivery record).

No source, domain/recording/composition implementation, existing accepted
spec/ADR, workflow or other path changes. No new claim/Issue/branch/PR,
force-push, merge, auto-merge, REC-001F, networking, REST healing, RPI,
quantity/delete inference, private API or execution. U-09/U-10 stay
UNKNOWN/BLOCKED, U-20 NOT_PROVEN/FORBIDDEN, C-01 BLOCKED, C-03 UNKNOWN.

Verification for this proposal: local fmt, clippy and workspace test commands
were attempted; each returned exit 127 (`cargo: command not found`), therefore
**NOT_RUN locally**. No new Rust tests or behavioral acceptance tests were
executed locally. Proposal link/path/provenance/content and exact remote diff
checks are reported factually in PR #34 / Issue #20 along with fresh exact-head
CI results. Every ADR acceptance row is **REQUIRED / NOT_RUN** for future
implementation. Passing docs-only CI (including unchanged Rust tests) cannot
prove Q1 repaired. Historical production-head CI is not a proposal acceptance.

The containing proposal SHA is recorded in existing PR/Issue metadata after
commit, avoiding a self-referential SHA. PR #34 remains Draft. The next gate is
**exact-SHA Integrator/Architecture review of ADR 0003**. Production remediation
starts only after explicit **DESIGN_APPROVED_FOR_IMPLEMENTATION** naming approved
API semantics and allowed paths. Then the complete implemented result requires
full independent QA of a new immutable head, including Q1/Q2, H1/H2 and all
previous regressions. No self-approval or READY_FOR_OWNER_REVIEW is claimed.

## Architecture-review revision — ADR 0003 R1–R3

Disposition: **DESIGN_PROPOSED / Q1_BLOCKED**. Incoming Architecture verdict:
**DESIGN_REVISION_REQUIRED / Q1_BLOCKED**. Q1 remains **BLOCKED / NOT_FIXED**;
Q2 remains **FIXED_IN_CODE / QA_PENDING**. **DESIGN_APPROVED_FOR_IMPLEMENTATION
has not been issued**. This is a docs-only continuation of the same lineage,
with the selected terminal affected capture / irreversible archive failure /
bounded diagnostic drain direction preserved.

Starting reviewed proposal head:
`750e85a7ecdd65b0c788726d05a4302b0b42225d`.
Actual main/base: `39ff0dba797eb010586238ef06fb80e996340401`.
Issue #20 / parent #5; claim `6024304772`; recovery `6025249885`;
branch `feat/REC-001D-ws-supervisor`; existing Draft PR #34;
previous proposal-delivery record `6034472539`.
Preflight independently confirmed branch/PR head equals the reviewed SHA,
base equals the supplied reference, zero later/intervening commits, Draft/open,
unmerged and auto-merge unset. All 106 available materialized file blobs matched
that exact remote tree before edits. Current policies/accepted contracts were
read without modifying them. The supplied R1–R3 Architecture task packet is
the review evidence used here; no unseen review content is claimed.

[Revised ADR 0003](../adr/0003-ws-capture-saturation.md) contains this mapping:

| Review finding | Normative revision | Required future acceptance evidence |
|---|---|---|
| R1 — pre-cut GAP must not absorb post-cut loss | Sections4–5 fix CutSide by admission order, include already-admitted/in-flight record-bearing owners/timers, freeze pre-cut stamps/ranges/counts, and require a separate counted PostCut owner or neighbor's reserved exact failure. Marker failure never reopens the cut. | Pre-cut tail B GAP -> A failure/cut -> contiguous B loss before marker, free-W and W-exhausted variants; exact counts/frontiers/cap9 plus marker-failure immutability. |
| R2 — mandatory Close lacks concrete recovery API | Sections3/3.2 define bounded outstanding_close_owners, authority-bound reclaim_close/confirm_closed and Pending->Leased->Settled. Drop/error returns the same owner to Pending; double reclaim/foreign ref cannot mutate it. No new nonce/counter or hidden owner. Idempotent epoch-bound retry may follow ambiguous physical effect. | Drop/error reclaim, double reclaim, foreign authority/owner, stop/Closing/DiagnosticClosed, settled/stale owner and reuse of existing Down W owner; unchanged ledger during reclaim and at most one active lease. |
| R3 — quiescence must not lose ticket on NotReady | Sections3/6 use quiesce(&mut SessionTurn, &CloseTicket)->QuiescenceReport. Bounded NotReady performs no drain and preserves ticket/generation; first Ready atomically consumes issuance and yields one proof. Foreign errors preserve rightful state; Closing failure enters DiagnosticClosing without reopening admission and invalidates ticket/proof terminally. | Repeated NotReady, drain/Close settlement then sole proof, duplicate issuance, foreign ticket, failure during Closing before/after proof, one-use finalize/proof reuse. |

API signatures, owner state tables, full item/metadata/byte bounds, cut/marker
order, diagnostic closure and compatibility/acceptance matrices are revised
together. Already-admitted generated required records remain pre-cut; only
still-unadmitted generated completion output can be deferred behind marker.
Transferred Down/Close/plan ownership stays counted until fully settled; inline
Close stays inside its reserved terminal owner. Diagnostic descriptor closure
does not make a pending Close disappear. No consuming-quiesce or unspecified
Close-retry contract remains in the current ADR.

Only two files change in this revision:

- `docs/adr/0003-ws-capture-saturation.md`;
- `docs/handoffs/REC-001D.md` (this appended history; earlier sections preserved).

Production source/tests, existing accepted specs/ADR, domain/recording,
dependencies, workflow and composition remain unchanged. No new
claim/Issue/branch/PR, force-push, merge, auto-merge, REC-001F, networking,
REST healing, RPI, quantity/delete inference, private API or execution.
U-09/U-10 UNKNOWN/BLOCKED, U-20 NOT_PROVEN/FORBIDDEN,
C-01 CONTRACT_CONFLICT/BLOCKED, C-03 DOC_CONFLICT/UNKNOWN remain.

Actual local Rust attempts in this revision: fmt, clippy -D warnings and
workspace tests each returned exit127 (`cargo: command not found`), so all are
**NOT_RUN locally**. No new behavioral acceptance scenario/Rust test was run
locally. Every ADR acceptance row, including R1–R3, remains
**REQUIRED / NOT_RUN** for approved implementation; design consistency/link/
provenance checks are not execution evidence. Exact two-doc diff and fresh
exact-head CI are verified/reported in PR/Issue metadata after commit; historical
`37596622811` belongs only to the reviewed starting SHA. Docs-only CI cannot
prove Q1 fixed, and no Q2/whole-task independent-QA acceptance is claimed.

The new immutable proposal head and actual CI result are recorded in existing
PR #34 / Issue #20 metadata after commit, avoiding a self-referential SHA.
PR remains Draft. Next gate: **repeat Architecture review of the whole new
proposal SHA, with mandatory R1–R3 retest and ADR consistency review**.
**DESIGN_APPROVED_FOR_IMPLEMENTATION has not been issued**; production
remediation waits for explicit approval of API semantics and allowed paths.
After implementation the new immutable head requires full independent QA of
Q1/Q2, H1/H2 and all prior regressions. No worker self-approval or owner/merge
readiness is declared.

## Architecture approval and bounded implementation delivery — 2026-10-07

Current worker disposition: **IMPLEMENTED / QA_PENDING**. Q1 and Q2 are
**FIXED_IN_CODE / QA_PENDING**. This records implementation and worker verification,
not an independent finding that Q1 is fixed or whole-task acceptance. It supersedes
the blocked/proposal dispositions above; all previous forensic and review history
is preserved as an exact prefix. **DESIGN_APPROVED_FOR_IMPLEMENTATION** was issued
for the whole immutable ADR proposal `cff1e398c3226bc2a86b51442e02054c5996e86a`.
The supplied `REC-001D-Architecture-cff1e398.txt` contains the exact approval,
allowlist, R1/R2/R3 retest and full-ADR consistency verdict. The design gate is
cleared; independent implementation QA is not.

Same lineage: repository `al-gri/pro-sclpng`; Issue #20 / parent #5;
claim `6024304772`; recovery `6025249885`; branch
`feat/REC-001D-ws-supervisor`; existing Draft PR #34. Incoming head and sole
implementation parent is `cff1e398c3226bc2a86b51442e02054c5996e86a`;
main/base is `39ff0dba797eb010586238ef06fb80e996340401`.
Actual branch/PR/base were independently verified against those values, with
zero foreign/intervening commits, open/Draft/unmerged state and auto-merge unset.
Current repository policies, accepted contracts and the whole approved ADR were
read before implementation. All129 original file blobs were materialized and
matched the immutable tree; this workspace has no git checkout. Delivery uses
that parent tree and a non-force expected-head ref update, followed by exact
remote compare/tree/CI checks. The containing implementation SHA and fresh CI
run are recorded in existing PR/Issue metadata after commit, avoiding a
self-referential SHA. Earlier proposal CI `37601404244` is historical only.

### Implemented contract and review mapping

| Finding / boundary | Implementation and worker evidence |
|---|---|
| R1 | Authority fixes each counted record-bearing owner's CutSide by admission order. No PreCut GAP stamp/range/count changes after cut. Separate PostCut owner or exact reserved neighbor failure; free/exhausted cap9 traces and six marker-fault variants cover both paths. A direct valid archive-wide Failed observation uses the same cut and immutable archive descriptor, even when it returns NotQuiescent before prefix drain. Supervisor synchronizes authoritative cut/prefix/marker state; later scope failure cannot replace the first archive stamp/reason/kind. |
| R2 | Bounded outstanding Close discovery, opaque ref, checked affine reclaim/dispatch and Pending/Leased/Settled. Drop/error returns the same owner Pending; AlreadyLeased creates no second lease. Foreign dispatch returns the legitimate lease. Existing Down-owned W is reused; reserved terminal Close stays inline. Discovery/reclaim/dispatch survives storage stop/Closing/DiagnosticClosed. Cross-scope reuse of one W and held settled aliases are rejected without mutation; internal alias capacity returns a typed error instead of panic/wrap. Physical dispatch error remains effect Unknown; retry is idempotent for that same epoch, not exactly-once. |
| R3 | Borrowed CloseTicket survives repeated bounded NotReady without implicit drain. First Ready atomically consumes issuance; duplicate/foreign calls do not issue another proof. Proof is affine and consumed once. Failure before/after Ready or after proof consumption invalidates finalization terminally, preserves closed admission and prevents successful final SegmentSeal/ArchiveSeal/finish. |
| Terminal scope / archive | First unrepresentable validated received input retains exact original identity and consumes its candidate once as evidence, never Raw/GAP coverage. Archive failure is irreversible and diagnostic-only before report use. Already-admitted FIFO work drains through the concrete bound gate; active neighbors retain admissible transport/diagnostic service. CaptureAttempt MAX and received AdmissionOrder exhaustion retain exact failures without wrap/reuse. Same-side F2 tail coalescing uses no additional owner/admission identity. |
| Writer / publication | Fresh exclusive one-segment filesystem owner, frozen bounded bootstrap, pointer-bound handle/sink and unique SessionTurn. Canonical seals/finish enforce latch and one-use proof; no writer export/adoption. Pure domain SessionRecordWriter is a trusted backend/conformance interface, not a guarantee about arbitrary implementations. Production publication remains PublicationUnavailable until sealed guard plus authenticated canonical producer exist. No application/transport/inventory wiring is added. |
| Persistence / recovery | Existing RecordingEvidence schema and truthful earlier nonregressing watermark. One immutable failure marker descriptor, authenticated confirmation only after actual gate, no duplicate marker. Persist error/mismatch/weak gate is explicit and sticky; no rollback/durable-marker/absent-failed-bytes promise. Unsealed reader returns ValidPrefixIncomplete with normally None quality; live owner reports Unknown completeness. Torn/segment-only/legacy Complete bytes retain accepted physical semantics. No recovery of a nonexistent observation; absent archives require external inventory. |

[ADR0003](../adr/0003-ws-capture-saturation.md) §11 records concrete Rust APIs,
byte/accounting formulas and a row-for-row mapping of all31 acceptance families
to worker tests. Full independent QA of every applicable row remains
**REQUIRED / NOT_RUN**. Source-level peer audits are worker checks, not that gate.
Opaque authenticated closure has private conformance coverage and dispatch
settlement works; an actual trusted transport producer remains out of scope.

### Exact implementation scope

The delivery changes17 paths, all inside the18-path approval allowlist:

- `crates/domain/src/capture_session.rs` (new);
- `crates/domain/src/lib.rs`;
- `crates/domain/tests/capture_session.rs` (new);
- `crates/market-data/src/ws_supervisor.rs`;
- `crates/market-data/src/lib.rs`;
- `crates/market-data/tests/ws_supervisor.rs`;
- `crates/recording/src/capture_session.rs` (new);
- `crates/recording/src/lib.rs`;
- `crates/recording/src/file.rs`;
- `crates/recording/tests/capture_session.rs` (new);
- `crates/domain/tests/support/health/mod.rs`;
- `crates/domain/tests/support/health/transitions.rs`;
- `crates/domain/tests/support/publication.rs`;
- `crates/domain/tests/cases/health.rs`;
- `crates/domain/tests/cases/publication.rs`;
- `docs/adr/0003-ws-capture-saturation.md`;
- `docs/handoffs/REC-001D.md`.

Allowed `crates/recording/tests/wal.rs` remains byte-for-byte unchanged; its26
regressions run. The other116 original blobs remain unchanged, including accepted
specs/ADR0002, dependencies/manifests/lock/toolchain/workflow and composition.
The market-data production dependency remains domain-only; recording stays dev-only.
No new claim/Issue/branch/PR, force-push, merge, auto-merge, networking,
REST healing, RPI/quantity/delete inference, private API, execution or REC-001F.
U-09/U-10 **UNKNOWN/BLOCKED**, U-20 **NOT_PROVEN/FORBIDDEN**,
C-01 **CONTRACT_CONFLICT/BLOCKED**, C-03 **DOC_CONFLICT/UNKNOWN** are preserved.

Approved H1/H2 deltas are explicit: Down/Close is returned before fallible
completion, which now waits for mandatory Close settlement; CaptureAttempt
exhaustion terminates its affected scope while admitted diagnostic drain and
neighbor service continue. Checked error/frontier/no-wrap/no-reuse and H1
successful durable-result ownership are preserved. Q2 owner/cancellation rules
and all prior regressions remain. The three forensic `q1_blocker_*` tests are
replaced by actual normative cap5/cap9/mixed regressions. Two caller-forged
RecordNo integration fixtures are replaced by authenticated constructor rejection
and the retained private protocol/domain counter-before-write checks; they are
not used to bypass an accepted writer. Other original supervisor tests remain.

### Actual final worker verification

Pinned Rust **1.98.1** (`48a229ceaefd4985c50990b14116b6d856af0985`),
Cargo1.98.1 (`797e8a9bc`), LLVM22.1.8. Official pinned components were verified
and installed into the transient workspace without changing repository toolchain,
manifest, lock or dependencies. The restricted runtime lacks `/proc/self/exe`;
Cargo's fmt/clippy wrappers and the default lld wrapper fail current-executable
lookup. Direct pinned rustfmt/Clippy driver work; tests use the same sysroot with
GNU linker and a writable temporary directory. Historical cargo-unavailable
attempts above remain historical, not the result of this implementation run.

| Check | Actual result |
|---|---|
| `cargo test --workspace --locked` | **PASS**, exit0:363 tests across19 suite results; no failed/ignored tests. Includes domain19 conformance +10 authority integration +80 contracts +9 identity +18 numeric +27 wire; market-data32 unit +10 fixtures +7 architecture +13 input-safety +65 supervisor integration; real CLI15; recording1 unit +21 owner +26 WAL;3 domain +3 existing market-data +4 recording compile-fail doctests. |
| Direct pinned `rustfmt --edition 2024 --check` on changed crate roots/tests and domain contract root | **PASS**, exit0; recursively checks changed modules/support cases. Canonical local `cargo fmt --all -- --check` wrapper is **NOT_RUN** after its environment lookup failure (exit101); the canonical command is required in fresh CI. |
| Pinned `clippy-driver` as `RUSTC_WORKSPACE_WRAPPER`, `cargo check --workspace --all-targets --locked`, `-D warnings` | **PASS**, exit0 across the complete workspace. Canonical local `cargo clippy --workspace --all-targets --locked -- -D warnings` wrapper is **NOT_RUN** after the same environment failure (exit101); fresh CI runs it canonically. |
| H1/Q2 fault loops | **PASS** in current supervisor suite: all63 H1 variants (9 immediate,9 Pong,36 four barrier variants,9 neighbor) and12 Q2 storage-fault variants. D-1/D/D+1/FIFO/obsolete owner and checked near-MAX scenarios also run. |
| R1/R2/R3, guard/finalization, crash/WAL, negative/compile-fail | **PASS** in the mapped worker suites; direct Failed marker, cross-scope Close and foreign/duplicate/after-consumption proof regressions included. No independent acceptance is claimed. |
| AdmissionOrder MAX | Actual private domain counter is set to MAX and rejects reservation without wrap/new W. Source tests inject only that negative reservation outcome against a genuine accepted owner for Raw/Pong/Connected/Disconnected/NoSuccessor, then verify exact failure/reclaim/drain/neighbor service; no public test hook or fabricated successful gate. Separate open/PostCut F2 case proves zero reservation, while PreCut cannot coalesce. |
| Static/byte audit | No outside-allowlist change; untouched blobs match baseline; earlier handoff is an exact prefix; accepted dependency/architecture tests pass; all31 normative rows and31 worker mappings retained; UTF-8/newline/links/trailing whitespace checked. Remote commit compare/tree and fresh exact-head CI are reported in PR/Issue after save. |

Early integration failures were corrected before these final passes: source-only
fixture cleanup violated the unchanged architecture scanner; a Close test compared
modeled alias bytes as though they were unchanged known backing; the immutable
Durable marker fixture needed its proper watermark kind. The negative fault,
ledger and byte expectations were preserved. Worker audit additionally found and
fixed cross-scope W/Close reuse, direct Failed cut synchronization and received
AdmissionOrder failure accounting. No out-of-scope test guard was weakened.

### Allocation/item evidence

Std-only thread-local allocator instrumentation measures requested Layout bytes,
not RSS/usable allocator overhead or a network adapter. Known Vec/Rc/path/backend
backing and explicit modeled inline charges are distinct; private BTree/decoder/
encoder allocations are covered by derived ceilings. Representative final
executed `--nocapture` traces:

| Boundary / profile | Peak requested bytes | Computed full profile ceiling | Repeat / teardown evidence |
|---|---:|---:|---|
| Concrete owner/sink/supervisor N1/M5/P4096 | 55,882 | 10,255,419 |100 failed-ingress/reclaim cycles keep live bytes/W flat; final tracked live0. |
| Concrete owner/sink/supervisor N2/M9/P4096 | 59,328 | 10,408,870 |Same flat/released0 assertions; held result/command and dense nested decode included. |
| Owner/sink cap5 | 29,576 | 8,574,170 |Constructed/terminal live21,206;100 reclaim cycles flat; final live0. |
| Owner/sink cap9 | 31,303 | 8,709,172 |Constructed/terminal live22,291;100 reclaim cycles flat; final live0. |

Numbers are observations of these bounded offline traces, not a throughput,
RSS or exhaustive platform benchmark. Tiny borrowed slices from an8MiB-capacity
caller Vec retain only the copied slice capacity. W+N+1<=M, exact raw capacities,
PreCut/PostCut counts/frontiers and reclaim/Drop/error transfers are asserted
throughout. The static ceilings include the maximum allowed profile/workspace;
these small-cap probes do not pretend to measure every maximum-sized input.

### Immutable delivery and next gate

The commit is a single child of the approved proposal. Its exact SHA,17-path
compare, preserved base/Draft state and fresh CI results are attached to the
existing PR/Issue metadata after the expected-head save. The Rust source snapshot
checked locally must match that immutable tree; any later code change requires
fresh exact-head verification.

**Next gate: full independent QA of the new immutable implementation head**,
including all31 ADR acceptance families, Q1/Q2, all63 H1 faults, H2 and prior
F1–F4/N1–N3/decoder/continuity/WAL/CLI regressions. Worker tests and CI do not
constitute independent QA, Integrator/task acceptance or owner/merge readiness.
PR #34 remains **Draft**; merge and auto-merge are not authorized. External
inventory and canonical application/transport/publication producers remain
deferred. No old QA or docs-only CI result is transferred to this new code head.


## B1/B2/B3 remediation after independent QA — 2026-10-07

**Worker disposition: REMEDIATED / QA_PENDING.** Q1: **FIXED_IN_CODE / QA_PENDING**; Q2: **FIXED_IN_CODE / QA_PENDING**. This does not override the independent **CHANGES_REQUIRED** verdict on the prior head or establish accepted Q1 correctness. Full independent QA of the new immutable containing head is required.

### Provenance, inputs and scope

Same Issue20/parent5, claim6024304772/recovery6025249885, branch `feat/REC-001D-ws-supervisor` and Draft PR34. Starting/QA-rejected immutable head: `ee4e85e5b9c74c8699628e39ea13281a2d91e611`; sole approved contract: `cff1e398c3226bc2a86b51442e02054c5996e86a`; actual main/base: `39ff0dba797eb010586238ef06fb80e996340401`. Ref/Draft/open/unmerged/auto-merge-unset preflight and all133 source blobs were verified. No intervening foreign commits were present. The containing remediation SHA, expected-head save, exact compare and fresh CI are recorded in the existing PR/Issue metadata after commit creation; this file does not invent its own future SHA.

Input blocker6038694010 was superseded by availability record6039297849. All four attached inputs were read; ZIP embedded copies matched byte-for-byte. Originals were not edited:

| Input | SHA256 |
|---|---|
| independent-QA report | `07df31f097339f69756df2c25dbfc90fccbdbee45544242266c40c996daac41b` |
| reproductions patch | `435dcc6b91a3d2175c216ba5ae90907648ed9eeb57d5e7ad8eba8cdc6062b28e` |
| evidence ZIP | `d5056680267c4401562176e656b6c5b6900a58d676dfad1ddd4a543640522774` |
| Flushed supplemental patch | `2e2c5a04e3dbc95a7a97f6f3dbed78ac83852cde3671b61aa7fa8fc567f6fe63` |

Exact remediation delta is **eight existing allowed paths**: domain source/tests `capture_session.rs`; market-data source/tests `ws_supervisor.rs`; recording source/tests `capture_session.rs`; this handoff and ADR0003. All125 other blobs remain unchanged. The original17-path cff→ee4 delta remains historical. No manifest/dependency/lock/toolchain/workflow, accepted ADR/spec, codec/reader, `file.rs`, existing WAL tests, networking or composition delta is added. QA's recording→market-data dev dependency was confined to a temporary reproduction copy; the production recording graph remains domain-only. No new claim/Issue/branch/PR, force-push, merge or auto-merge.

### Corrections and normative regressions

| Finding | Concrete correction / evidence |
|---|---|
| B1 | A fixed per-W received/generated obligation and identity outlive Rust references. Drop flags bounded abandonment only; rightful serialized reconciliation latches explicit OwnershipAbandoned logical stop, freezes first cut if absent, preserves first failure/descriptor/prefix and mandatory Close. Abandoned W is retained/reportable even with zero references; descriptor closure reports it without seals or fabricated marker/Raw/GAP. Current epoch Close replaces an older settled owner; same owner is reused for pending Down. Generic set_kind cannot settle/cancel received obligations. Completion uses authenticated receipts, requires both stale diagnostic writes/three fresh epoch writes, and rejects partial-write storage stop; no-write suppression is limited to actual prior-Down Connected/Pong. |
| B2 | `finalize(&mut SessionTurn, &mut QuiescenceProof)` and borrowed `consume_proof` preserve the exact caller proof on foreign rejection. All six genuine A/B owner/turn/proof mismatches preserve both states/ledgers/watermarks/WAL bytes; the same proofs then finalize their rightful owners once. Validated consumption precedes I/O; real closed-backend failure leaves it spent with no seals/retry. No Clone/replacement ticket/proof/generation; affine/reentry and overlapping-proof-borrow compile-fail coverage. |
| B3 | Bound generic setter returns StorageProfileBound. One backend-issued opaque StorageMemoryAuthority is private to concrete owner; actual closure capacities refresh through it. Checked profile components/intermediates/aggregates and admitted numeric headroom precede mutation. Invalid pre-write backend budget performs zero writes/changes; invalid post-write change explicitly stops storage and preserves old profile with ambiguous suffix. checked_retention_report is available; no saturated fictitious ceiling. Genuine zero/MAX tests cover all six components and stable owner/supervisor reports in debug/release. |

B1 regression lives in existing market-data integration tests already using recording. Five normative functions cover ten queued Raw/GAP/control/timer/current-epoch2 traces before/after Closing, dropped pending Down with held aliases, PreCut/PostCut6-owner retention/first-failure preservation, two genuine in-flight/reclassified-received cases, and healthy authenticated drain/settled Drop with physical Complete finalization. Close remains discoverable/reclaimable after logical stop and diagnostic closure; repeats do not free W through fictional settlement or grow retained state.

Supplemental additions were transferred as normative assertions using **original Durable fixtures**: stale CloseRef after actual epoch/W recycle rejects without corrupting the new leased owner, and two repeated Halted drains preserve the private RecordNo-error prefix/snapshot. No Durable→Flushed adaptation was applied. The reviewed Failed/through=None marker restriction remains a nonfinding.

The reproduction patch was applied only to an isolated exact-source copy. Allfour factual probes PASS there because they demonstrate the old defects/nonfinding; Linux additionally confirms wrongful successful FinalizedArchive and physical Complete bytes for B1. New B1/B3 normative tests both compile and **FAIL behaviorally on ee4** (exit101: W disappears; backend metadata zero override accepted), then PASS after correction. B2's old factual probe confirms stranded proof; revised borrowed-signature regression is API-incompatible with ee4 and is not misreported as an old-head runtime assertion.

Worker peer audit raised/corrected foreign-ticket-before-reconciliation ordering, current-epoch Close replacement, received-origin cancellation/marker barriers, partial-receipt completion and duplicate Down's mistaken generated-plan owner. The initial workspace debug attempt compiled before the last duplicate-owner correction and stopped at65 PASS/7 FAIL in supervisor tests; the corrected full rerun below passes without weakening H1/Down assertions. An initial B3 post-write test expected Pending incorrectly; it now asserts the existing Unconfirmed(error) stop semantics and passes. Peer review is not independent QA.

### Actual Linux checks

Linux x86_64, kernel6.18.44; pinned Rust1.98.1 (`48a229ceaefd4985c50990b14116b6d856af0985`), Cargo1.98.1. Official package SHA256 values and155 installed files were verified against downloaded manifests/archives. Scratch-only toolchain, offline four-local-package workspace, direct std sysroot/GNU bfd; no repository environment/dependency change. Initial toolkit LLVM copy was truncated; exact archive member was restored/verified before Rust runs.

Commands below ran from the materialized workspace via `source ../rust-tools/env.sh` (root invocations used equivalent `--manifest-path repo/Cargo.toml`). Logs are retained in the execution workspace; the saved source tree must match the tested tree. No Flushed adaptation:

| Actual command/check | Result |
|---|---|
| `cargo test --workspace --locked` | **PASS/exit0**, all19 suites, **379 passed /0 failed /0 ignored**. Counts:21,13,80,9,18,27,32,10,7,13,72,0,15,3,22,26,4,3,4. |
| `cargo test --workspace --release --locked` | **PASS/exit0**, same379/0/0. Includes optimized B1/B2/B3 and overflow behavior. |
| `rustfmt --edition 2024 --check` on changed crate roots/test roots | **PASS/exit0**, recursive modules covered. |
| `RUSTC_WORKSPACE_WRAPPER=.../clippy-driver RUSTFLAGS='--sysroot ... -C linker-features=-lld -D warnings' cargo check --workspace --all-targets --locked` | **PASS/exit0**, actual Clippy driver/all-target warnings denied. |
| `cargo generate-lockfile --offline`; original Cargo.lock blob comparison | **PASS**, unchanged blob `8fce3a61cc8dc1f72727ddf2c82c453c31be94bc`. |
| `cargo build --workspace --locked` | **PASS/exit0**. |
| Canonical local `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings` | Both wrapper attempts exit101 before checking because `/proc/self/exe` is unavailable. **NOT_RUN canonically locally**; direct equivalents above ran. Fresh exact-head canonical CI is required and recorded separately in PR/Issue after save. |
| Full independent QA of the new immutable head | **REQUIRED / NOT_RUN** by worker; no owner readiness or merge approval. |
| Actual socket/live smoke/application/inventory producer | **NOT_RUN / OUT_OF_SCOPE**, those producers remain unimplemented and forbidden by this approval. |

The complete debug and release suites execute the existing31-family ADR mapping, all63 H1 fault variants (9 immediate+9 timeout+36 barriers+9 neighbor), checked/scoped H2,12 Q2 storage-fault variants, D−1/D/D+1/timer-owner/FIFO boundaries and F1–F6/N1–N3. Existing decoder/continuity/input-safety/DataHealth/contracts/publication/WAL and all15 real CLI tests pass, including the two Unix-only CLI cases missing from Windows. Canonical production publication still remains PublicationUnavailable until its sealed guard and actual authenticated producer exist; passing pure conformance is not application publication evidence.

Cap5/cap9 requested-allocation probes were rerun explicitly with `--exact --nocapture` in debug and release;100 failed/reclaim repeats remain flat and all tracked boundary objects release to0 bytes:

| Probe | Debug peak | Release peak | Computed ceiling / teardown |
|---|---|---|---|
| Full N1/M5/P4096 |56,494 B |56,493 B |10,256,027 B /0 |
| Full N2/M9/P4096 |60,388 B |60,387 B |10,409,926 B /0 |
| Concrete owner cap5 |29,587 B |29,587 B |8,574,778 B /0 asserted |
| Concrete owner cap9 |31,314 B |31,314 B |8,710,228 B /0 asserted |

These are requested Layout bytes and small-profile probes, not RSS/network/end-to-end or proof of actual worst-case heap overflow. B3's confirmed claim stays reporting substitution/debug panic; no actual unbounded allocation/heap/RSS violation is asserted. Honest fixed per-W metadata covers abandonment identity/state/counters inside W+N+1≤M; no eviction/history lane is added.

Original immutable Windows319 PASS/42 platform failures and two Unix-only NOT_COMPILED cases, historical CI37612763851, and supplemental24 Flushed commands/29 function PASS remain separate attached evidence. They do not replace this original Linux/Durable rerun, nor independent re-QA of the corrected head. Reader physical Complete and stored quality remain unchanged; unsealed diagnostic prefix reports ValidPrefixIncomplete/None, owner completeness Unknown. Crash before marker cannot reconstruct missing observation; external inventory remains deferred.

### Delivery boundary

Prior handoff remains an exact byte prefix. ADR §§3/4/6/12 and existing31-row matrix preserve approved R1–R3, H1 completion-after-Close-settlement, scoped H2, Q1/Q2 and U-09/U-10/U-20/C-01/C-03 constraints. The new immutable containing SHA, eight-path compare, full-tree preservation, fresh exact-head CI checkout/commands/clean state and local post-save source match are verified in the existing PR/Issue record. No old PASS is transferred to a later head.

**Next gate: complete independent QA of the new immutable remediation SHA**, covering B1/B2/B3, all31 acceptance families, Q1/Q2, all63 H1 faults, H2,12 Q2 faults and every prior regression. Worker PASS and CI do not grant Integrator/task acceptance, owner readiness, merge or auto-merge. PR #34 remains Draft.

## QA-D1 remediation after independent QA — 2026-10-07

**Worker disposition: REMEDIATED / QA_PENDING.** Q1 and Q2: **FIXED_IN_CODE / QA_PENDING**, worker code/check disposition only. Independent **CHANGES_REQUIRED** on rejected `dfe2c43efc2df0591d67daef4beac0d97fa3b050` is preserved; this delivery requires full independent QA of the new immutable containing SHA. Worker/CI PASS does not grant Integrator acceptance, owner readiness or merge approval.

### Current provenance, inputs and scope

Same Issue20/parent5, claim6024304772/recovery6025249885, branch `feat/REC-001D-ws-supervisor` and Draft PR34. Rejected start: `dfe2c43efc2df0591d67daef4beac0d97fa3b050`; sole approved contract: `cff1e398c3226bc2a86b51442e02054c5996e86a`; actual main/base: `39ff0dba797eb010586238ef06fb80e996340401`. Starting parent `ee4e85e5b9c74c8699628e39ea13281a2d91e611`, tree `d0f7b930013b8cd80ccfe7e27640f725119c9975`. Actual refs/Draft/open/unmerged/auto-merge=null and all133 starting blobs were verified before edits; no intervening foreign commit. Existing continuation record6041999036 records QA-D1 work, not a new claim.

Three **current** originals are accessible/read and unmodified:

| Current attached input | Bytes | SHA256 |
|---|---:|---|
| 03-REC-001D-QA-dfe2c43.md |40,713 | `900c0ce21e54e49ed5d7eed7314e278ae1df75d76683ce2a51fde77e178b3a93` |
| 01-REC-001D-QA-dfe2c43-evidence.zip |174,252 | `d2e27b7c3e84bc06a1cb4a467baecd10504fff54e8e30fcc56a33020b51b671e` |
| 02-REC-001D-QA-dfe2c43-reproductions.patch |16,577 | `4182515b0df7f3c6c604d7b66ceb57498ab68393563ad588a1db46bb69269282` |

Safe ZIP extraction verified47 entries/753,288 expanded bytes and all43 manifest SHA256 entries. Embedded report and both embedded patch copies match uploaded bytes exactly. The report's four completed independent original Linux/Durable runs and exact-head CI37636862096 each show379 PASS/0 FAIL; these missed QA-D1 and remain rejected-head evidence. Other29 normative rows passed their stated library boundaries; rows6/26 failed. Original B1 Drop trigger and B2/B3 closure passed under the reviewed boundary; full B1/Q1/R2 cancellation intersection was not accepted.

The new delta is exactly **four existing allowed paths**: market-data source/tests `ws_supervisor.rs`, ADR0003 and this handoff. All129 other original blobs are unchanged. No dependency/manifest/lock/workflow/toolchain, accepted specs/ADR, domain/recording source/tests, codec/reader/WAL, networking or composition change. §8 table and all31 §9 rows remain preserved; previous handoff is an exact byte prefix. No new claim/Issue/branch/PR, force-push, merge or auto-merge. The new immutable containing SHA, exact parent→head diff and fresh canonical CI are recorded in existing PR/Issue metadata after save, avoiding a self-referential SHA.

### Minimal trigger and restored settlement

Real gate-confirmed Down creates a Pending/Generated epoch-completion obligation and returns a Close on that same transferred W. Hold Close, drop or retain Down-result aliases, then saturate **the same scope**: cap5 has Down plus two Connected barriers; cap9 has A Down plus four A barriers and admitted B Raw. Rejected local install_received_failure reported cancelled_plan=true after removing the plan and changing WorkKind only. Later successful Close and alias release exposed the unsatisfied obligation as OwnershipAbandoned; next drain incorrectly Halted and prevented B's admitted Raw/marker.

The common private `cancel_pending_disconnect(&mut SessionTurn, StreamId) -> Result<bool, SupervisorError>` now validates rightful turn and retained matching pair, calls checked `cancel_generated_plan`, reclassifies only after successful settlement, and clears/removes last. Local ingress failure, external scope terminal synchronization and applicable DiagnosticClosing/Closed paths use it. No remove/take/blanket clear or expect precedes fallible settlement. No pair returns false; attempted checked rejection retains the same plan, owner and Close and returns a typed error. Local cancelled_plan=true means successful settlement; cancellation error returns false with original terminal failure/cut and mandatory Close explicit. A scope-local failure cancels only its own plan.

Gated Down/original stamp/WorkOwner/CloseOwnerRef and held legitimate lease survive. Reclaim creates no second lease/slot; Drop or ambiguous dispatch error leaves the same Close Pending. Settled is not reissued. Cancel settles only generated unadmitted output, never received Raw/GAP/control/timer; B1 reference-independent obligation retention remains. W is released only after legitimate remaining alias/Close settlement, not a WorkKind change. First failure/cut/prefix, pre-cut FIFO, neighbor service and W+N+1<=M remain unchanged.

Completion ownership validation runs while still registered; on error the original owner/ingress is restored before fallible reclassification. Healthy generated completion requires three **fresh** receipts and exactly one epoch advance after Close settlement. StorageStopped preserves partially confirmed generated plans/owners; no wholly-uncommitted cancellation claim, successful accounting, retry suffix or rollback. Authority's authenticated trusted watermark remains the last good receipt; actual backend physical bytes/watermarks may include a rejected mismatch/weak-gate write. Descriptor closure reports undrained state and preserves mandatory Close. Lifecycle immediately revokes further generated effects; eligible supervisor ownership settlement occurs at the next rightful synchronize/drain boundary, potentially DiagnosticClosed, without owner-driven hidden drain.

### Normative regressions

Before production edits, all133 rejected-source hashes were matched, copied to an isolated baseline and the supplied patch applied only there (no manifest delta). Commands:

`cargo test --manifest-path d1-baseline/Cargo.toml -p market-data --test ws_supervisor independent_qa_s -- --nocapture`
and the same command with `--release`.

Both compile and **FAIL behaviorally**, exit101 each:1 stale-Close control PASS/2 normative FAIL. The retained tests are:

- `independent_qa_scope_cut_cancels_generated_down_plan_without_stopping_drain`;
- `independent_qa_same_scope_plan_cut_preserves_queued_neighbor_raw`.

After correction both names PASS debug/release. Assertions now require **full drain through the marker**, no false OwnershipAbandoned/Halted, exact B Raw bytes/tag/stamps/attempt/frontier, original Down, immutable first failure/cut and no canceled EpochAdvance/final seals. Actual canonical owner/sink and physical WalReader are used with **Durable** receipts.

Allfour current QA patch functions were retained, including old-tag Candidate6/non-reuse/original identity and F2 Raw1 bytes/stamp10, GAP2..33/count32/stamp11 and neighbor service. Existing stale Close after actual epoch/W recycle and repeated Halted prefix/snapshot assertions remain.

Four extra integration functions cover6 heldDown/Close release-order/reclaim/error/foreign/settled/repeat cases, external A terminal cancellation preserving B's3 fresh completion receipts/single advance,2 DiagnosticClosing/Closed cases and6 partial completion storage-fault cases. The private genuine-owner `checked_generated_cancellation_error_retains_plan_owner_close_and_rightful_retry` rejects changed-kind/foreign cancellation without removing the original plan/owner/lease, preserves false cancellation and first cut/prefix, then retries rightful settlement with that same owner. Total additions:8 integration functions+1 private function.

Targeted commands after correction:

| Filter/command | Debug | Release |
|---|---|---|
| MD integration `independent_qa_`, --locked -- --nocapture |5 PASS/exit0 (four new QA functions plus existing staleClose) |5 PASS/exit0 |
| MD integration `qa_d1_`, --locked -- --nocapture |4 PASS/exit0 |4 PASS/exit0 |
| Qualified private cancellation-error/retry function, --lib --locked |1 PASS/exit0, isolated target |1 PASS/exit0 |

Initial test-authoring failures are distinct from product behavior: Close's W association is nested, so the incorrect general CommandLease.work_owner_id accessor was replaced with existing opaque CloseOwnerRef.storage and identity assertions. An incorrect equality of physical and authenticated watermarks was replaced with explicit separate assertions (the real backend can append/sync before mismatch/weak-gate rejection). An EpochPersistenceFault comparison compile error was corrected with matches!. Shared-target executable PermissionDenied and first direct-rustfmt missing-library attempts did not execute relevant checks; isolated/debug and correctly configured pinned runs then passed. No product semantic/assertion protection was weakened.

### Complete original Linux/Durable checks

Same pinned Linux x86_64 Rust1.98.1/compiler48a229ceaefd4985c50990b14116b6d856af0985, Cargo1.98.1/LLVM22.1.8, scratch-only verified toolchain and offline local workspace. Source env is `source ../rust-tools/env.sh`; root commands use equivalent `--manifest-path repo/Cargo.toml`. No source/fixture/gate/manifest or Flushed adaptation.

| Actual command/check | Result |
|---|---|
| `cargo test --workspace --locked` | **PASS/exit0,388 passed/0 failed/0 ignored**,19 summaries. Counts21,13,80,9,18,27,33,10,7,13,80,0,15,3,22,26,4,3,4. |
| `cargo test --workspace --release --locked` | **PASS/exit0,388/0/0**, same19 summaries. Initial isolated-target release attempt failed linking unchanged domain contracts before any test ran (exit101); unadapted same-command rerun using the working target passed. |
| `rustfmt --edition 2024 --check` all68 tracked Rust files | **PASS/exit0**, all workspace roots/modules/tests. |
| Pinned clippy-driver wrapper, `cargo check --workspace --all-targets --locked` with `-D warnings` | **PASS/exit0**, complete workspace. |
| `cargo generate-lockfile --offline`; original lock comparison | **PASS/exit0**, unchanged Git blob `8fce3a61cc8dc1f72727ddf2c82c453c31be94bc`. |
| `cargo build --workspace --locked` | **PASS/exit0**. |
| Canonical local fmt/Clippy wrappers | Attempts exit101 before checking due absent /proc/self/exe: **NOT_RUN canonically locally**. Direct equivalents above actually ran; fresh exact-head canonical CI is recorded separately in PR/Issue. |
| Full independent QA of new immutable head | **REQUIRED / NOT_RUN by worker**. |
| Actual live socket/application/publication/closure/inventory producers | **NOT_RUN / OUT_OF_SCOPE**, no networking/composition expansion. |

All31 §9 families remain exercised with strengthened6/26; B1/B2/B3 retained; all63 H1 fault variants (9+9+36+9), H2,12 Q2 storage faults, D−1/D/D+1/timer/FIFO, F1–F6/N1–N3, decoder/continuity/input safety/DataHealth/publication/WAL and real CLI pass in both original profiles. All26 unchanged WAL tests/all15 CLI tests and11 affine/reentry/borrow compile-fail tests pass. Approved H1 completion after Close settlement and scoped H2 exhaustion remain. Production PublicationUnavailable remains until sealed authenticated producers exist; tests do not implement those deferred applications.

Allocation probes were rerun with --exact --nocapture in debug/release (four commands, exit0).100 failed/reclaim repeats remain flat; all tracked boundary objects release to0 requested bytes:

| Probe | Debug peak | Release peak | Existing ceiling |
|---|---:|---:|---:|
| Full N1/M5/P4096 |56,495 B |56,495 B |10,256,027 B |
| Full N2/M9/P4096 |60,389 B |60,389 B |10,409,926 B |
| Concrete owner cap5 |29,588 B |29,588 B |8,574,778 B |
| Concrete owner cap9 |31,315 B |31,315 B |8,710,228 B |

Requested Layout bytes, not RSS/allocator usable size/end-to-end networking or a universal heap theorem. New helper retains no additional item/metadata lane; source types/formulas and ceilings unchanged. No unbounded allocation/actual heap overflow/RSS claim is added.

### Delivery and independent gate

Prior handoff/history is preserved exactly. Current QA report/ZIP/reproduction identity was reauthenticated; previous Windows319 PASS/42 failures, Flushed supplemental runs, older379/363 PASS and their CIs remain historical evidence, never acceptance of this head. No reader change conceals Complete bytes; unsealed prefix normally ValidPrefixIncomplete/quality None, owner completeness Unknown. Crash before marker cannot reconstruct missing observations; external inventory remains deferred. U-09/U-10/U-20/C-01/C-03 unchanged.

The new immutable containing SHA, four-path actual diff,133-blob saved/tested source match, non-force expected-head update, fresh exact-head canonical fmt/Clippy/lock/build/test/clean-checkout results and post-save original debug/release runs are verified in existing PR/Issue metadata. PR #34 stays Draft/open/unmerged, auto-merge unset.

**Next gate: full independent QA of the new immutable remediation SHA**, including QA-D1, all31 acceptance families (especially6/26), B1/B2/B3, Q1/Q2,63 H1 faults, H2,12 Q2 faults and every prior regression. Worker/CI PASS does not grant owner readiness or merge authorization.

## QA-D2 continuation — public generated cancellation, 2026-10-07

**Worker disposition: REMEDIATED / QA_PENDING.** Q1 and Q2 remain **FIXED_IN_CODE / QA_PENDING** as worker code/check dispositions. Independent **CHANGES_REQUIRED / MEDIUM QA-D2** on `8a8969acce1e0e7873929736daef5d69d78a7e41` is preserved. This repair is not independent acceptance; the new containing immutable SHA requires full independent QA.

### Lineage, refs and policies

Same repository `al-gri/pro-sclpng`, Issue #20 / parent #5, claim `6024304772`, recovery `6025249885`, branch `feat/REC-001D-ws-supervisor` and Draft PR #34. Rejected start: `8a8969acce1e0e7873929736daef5d69d78a7e41`, tree `ba89fdc99e9a1b68e862e141c835caded41622b9`, sole parent `dfe2c43efc2df0591d67daef4beac0d97fa3b050`. Actual main/base: `39ff0dba797eb010586238ef06fb80e996340401`. Sole approved contract: `cff1e398c3226bc2a86b51442e02054c5996e86a`.

Before edits, actual branch/head/base, Draft/open/unmerged/auto-merge=null and all133 tracked starting blobs were verified against the fetched Git tree. There was no intervening foreign commit or local source change. Current AGENTS, PROJECT_STATE, ARCHITECTURE, INVARIANTS, WORKFLOW, Issue/parent and ADR were inspected. This continuation restores the approved partial-completion settlement contract inside the same §8 allowlist; no new claim/Issue/branch/PR or permission is introduced. Existing Issue continuation record `6043191341` reports input availability and the reproduced failure, not a new claim. Final containing head, parent, exact diff, CI and metadata verification belong in existing PR/Issue records; a containing document cannot embed its own SHA.

### Current QA provenance

The two current attachments were read. The full report is inside the ZIP; it was read in full. Safe extraction and every embedded checksum were verified:

| Current input | Bytes | SHA256 |
|---|---:|---|
| REC-001D-QA-8a8969ac-evidence.zip |240,176 | `4ba81e081ddd24ff2ccd0ad33038ba4cb6837281fd0ab8d2c157234cf3eed159` |
| Uploaded REC-001D-QA-8a8969ac-reproductions.patch |13,635 | `988bfd08198c9a58eceb94786f740c957b60dd35c4815011d06b4c6205cec1c4` |
| Embedded REC-001D-QA-8a8969ac.md |69,897 | `4582796a05af3cc8722795af50f44cfdeef463eb21696ba1b4e89ff573e27967` |

ZIP112 entries /746,789 expanded bytes, all111 manifest hashes match; no absolute or parent-traversal paths. The embedded reproduction patch matches the uploaded patch byte-for-byte, and extracted copies match archive bytes. Its minimal patch adds seven test functions: six public failures plus one canonical control containing12 internal variants. The report's broader supplemental copy adds a separate stale-diagnostic six-case control, hence its raw logs show **2 PASS /6 FAIL**; that is not the count of the current minimal patch. The previous D1 three-input hashes embedded in this report are historical provenance, not the identities of these current attachments.

Inputs are available and authenticated; **BLOCKED_ON_QA_INPUTS does not apply**. QA's original388 PASS debug/release and CI37653209456 are exact rejected-head evidence. They missed the direct public cancellation boundary and are not acceptance of this repair.

### Minimal trigger and restored public contract

Genuine owner registration exposes a public `SupervisorSessionHandle` before transfer into the canonical supervisor. Actual Durable Down persistence and completion create the same-W mandatory Close; after Close settlement, normal PendingPlan/retain-generated transition resets fresh generated receipts to0. One/two gate-confirmed generated epoch writes followed by BeforeWrite, post-write ReceiptMismatch or WeakGate produce a retained partial obligation. DiagnosticClosed previously permitted public cancellation to return Ok; Drop then incorrectly released W and erased undrained accounting. Canonical supervisor's own StorageStopped guard already prevented that route, but could not enforce the public handle boundary.

`SupervisorSessionHandle::cancel_generated_plan(&self, &mut SessionTurn, &WorkOwner) -> Result<(), AuthorityError>` retains its signature. Rightful turn and WorkOwner validation now precede reconciliation. Existing PendingPlan/Generated/Pending checks remain. Existing StorageStopped returns `StorageStopped`; otherwise any nonzero fresh generated receipt count returns `NotQuiescent`. These checks precede Pending->Settled. Exact scope failure or DiagnosticClosing/DiagnosticClosed is still required for a zero-record/no-stop success. No blanket ensure-storage-writable check blocks legitimate DiagnosticClosed cancellation. Earlier authenticated Down is excluded by the existing fresh-counter reset.

Typed rejection preserves progress, W, original storage error, trusted prefix, first failure/cut, Close identity and actual backend watermarks/bytes. After alias/steward Drop, the same cell may become bounded Abandoned; rightful reconciliation retains W and undrained work and never replaces an earlier storage stop. No fictitious settlement, suffix retry, replacement work, rollback, missing-byte reconstruction or received Raw/GAP/control/timer cancellation is introduced. Zero fresh receipts with a storage stop may still have physically written bytes; tests separately assert trusted and physical prefixes. Failed latch, publication and final-seal denial remain irreversible.

Severity remains **MEDIUM**, accounting/undrained loss only. Healthy finalization revival, actual heap overflow, unbounded allocation and RSS violation were not proved by QA and are not claimed here.

### Normative reproductions and additional cases

Before implementation, an isolated copy of all133 exact starting blobs received only the uploaded test patch. No dependency/manifest/source/gate adaptation. Both actual commands compiled then failed behaviorally, exit101:

```sh
cargo test -p market-data --test ws_supervisor --locked qa8_independent -- --nocapture
cargo test -p market-data --test ws_supervisor --release --locked qa8_independent -- --nocapture
```

Each profile: **1 canonical control PASS /6 public FAIL**. The six retained exact public test names are `qa8_independent_direct_partial_{one,two}_{beforewrite,mismatch,weakgate}`. After repair all six and the canonical12-variant control PASS in each profile (seven functions). The canonical control retains Open/Closing × one/two fresh receipts × three storage faults, alias Drop, original stop and trusted/physical prefix assertions.

Five additional genuine-owner/concrete-Durable integration functions cover:

- One/two fresh confirmations without StorageStopped, across scope-local failure, DiagnosticClosing and DiagnosticClosed: six internal cases reject NotQuiescent; repeats preserve state/bytes, aliases later retain Abandoned W.
- Zero fresh confirmations with BeforeWrite, actual post-write mismatch and weak gate: three cases reject StorageStopped; physical rejected records are distinguished from authenticated progress; original stop and terminal proof invalidation survive Drop/closure.
- Zero fresh/no-stop cancellation in all three eligible states: earlier Down receipt does not block settlement; held Close and aliases retain the same W, Drop/error/reclaim/settlement preserves one logical command and frees capacity only truthfully.
- Healthy completion needs three fresh receipts, settles once, then canonical owner finalizes once; reader is physically Complete. This control does not relax failed-session finalization.
- Pending abandonment of an additional received Raw before first cut: four foreign turn/handle/WorkOwner combinations leave rightful state/ledger/WAL unchanged. Rightful reconciliation later reports exact Raw identity and undrained2 for separate lost Raw and live Generated obligations. No fabricated Raw/GAP/marker.

The two independent new negative families were initially run while the old domain boundary was still present: compiled debug tests failed behaviorally on both nonzero/no-stop and zero/stop guards, while positive zero/no-stop and healthy completion controls passed. No compile failure is represented as a product reproduction. Final additions total **12 integration test functions**, not their loop-variant totals; existing80 functions remain intact and the suite now contains92. QA-D1's exact two regressions, old-tag Candidate6, original F2 Raw/GAP stamps/identity, stale Close after actual epoch/W recycle and repeated Halted prefix/snapshot remain unchanged.

### Defensive-validation remark

QA's private swapped-plan probe directly interchanges same-authority A/B registry owners during DiagnosticClosing. Static review confirms that the private helper checks paired presence and runtime plan epoch, but does not independently compare opaque WorkOwner scope/epoch to its registry key. Canonical Down construction maintains that association; no natural public route producing the swap was established. ADR §13.1 now states the precise validation and construction limit instead of claiming rejection of every inconsistent pair. The historical private probe's debug/release behavioral failures remain explicit evidence; it is not a second public blocker, foreign-authority escape or silently repaired route. No new owner-identity API/policy is added.

### Complete original Linux/Durable verification

Linux x86_64; pinned Rust1.98.1/compiler48a229ceaefd4985c50990b14116b6d856af0985, Cargo1.98.1/LLVM22.1.8. `source rust-tools/env.sh` selects the verified local toolchain, offline Cargo home and GNU-linker adjustment for this sandbox. Root commands use `--manifest-path repo/Cargo.toml`; isolated rejected commands use `--manifest-path d2-baseline/Cargo.toml`. Final debug target is `d2-clean-debug-target`, release `d2-final-release-target`; `CARGO_BUILD_JOBS=1`. No Flushed supplemental adaptation or production/manifest/dependency delta.

| Actual command/check | Result |
|---|---|
| Original `cargo test --workspace --locked` | **PASS/exit0,400 passed/0 failed/0 ignored**,19 summaries. Counts21,13,80,9,18,27,33,10,7,13,92,0,15,3,22,26,4,3,4. |
| Original `cargo test --workspace --release --locked` | **PASS/exit0,400/0/0**, same19 summaries. |
| Targeted `qa8_independent` debug/release | **7 PASS/exit0** each, including six previously failing public tests and one12-case canonical control. |
| Targeted `qa_d2_` debug/release | **5 PASS/exit0** each, also included in both complete workspace profiles. |
| `rustfmt --edition 2024 --check`, all68 tracked Rust files | **PASS/exit0** after final source edits. |
| Pinned direct clippy-driver wrapper, `cargo check --workspace --all-targets --locked`, warnings denied | **PASS/exit0** after final source edits. |
| `cargo generate-lockfile --offline` and original comparison | **PASS/exit0**, SHA256 `b3286f591e5f63975f5bc575eb95cbae6f15823fefed46fff6c40c40e62fce4c` unchanged, Git blob `8fce3a61cc8dc1f72727ddf2c82c453c31be94bc`. |
| `cargo build --workspace --locked` | **PASS/exit0**. |
| Canonical local fmt/Clippy | Actual attempts exit101 before checking: absent /proc/self/exe in tool wrappers, **NOT_RUN canonically locally**. Direct checks above ran; fresh exact-head standard CI is verified separately in PR/Issue. |
| Full independent QA of new immutable SHA | **REQUIRED / NOT_RUN by worker**, separate next gate. |
| Private swapped-registry supplemental probe | Historical rejected-head debug/release FAIL inspected; **NOT_RUN on repaired source by worker**. Current static source audit confirms the documented opaque-identity validation limit; no natural public swap route established. |
| Live networking/application/publication/inventory producers | **NOT_RUN / OUT_OF_SCOPE**. |

One initial full debug attempt used an old cached WAL binary whose execute bit was absent (mode0644), so that binary never ran, exit101 after earlier suites. No test failure or source adaptation was inferred. Fresh debug build directory then ran the entire unmodified workspace successfully. An intermediate full release run passed399 tests before the added foreign-pending-abandonment control; it is superseded by the complete final400 run. These attempts remain separate logs and are not transferred into final counts.

All31 §9 families were rerun through the complete workspace, especially11/15/16/26 with the public boundary additions; existing QA-D1/B1/B2/B3/R1–R3 preserved. All63 H1 faults (9+9+36+9), scoped H2,12 Q2 storage faults, timer/FIFO boundaries, F1–F6/N1–N3, decoder/continuity/input safety/DataHealth/publication contracts,26 WAL tests,15 actual CLI tests and11 affine/borrow/reentry compile-fail tests PASS in both profiles. Approved H1 completion after Close settlement and scoped H2 exhaustion are unchanged. Passing library conformance does not implement deferred application producers or prove all future external behavior.

Cap5/cap9 requested-allocation probes were separately rerun `--exact --nocapture` in debug/release (four commands, all exit0).100 failed/reclaim repeats remain flat, tracked teardown bytes0:

| Probe | Debug peak | Release peak | Existing ceiling |
|---|---:|---:|---:|
| Full N1/M5/P4096 |56,493 B |56,493 B |10,256,027 B |
| Full N2/M9/P4096 |60,387 B |60,387 B |10,409,926 B |
| Concrete owner cap5 |29,587 B |29,587 B |8,574,778 B |
| Concrete owner cap9 |31,314 B |31,314 B |8,710,228 B |

These unchanged allocation fixtures use their original Flushed full-supervisor and Written owner-only profiles. They measure requested Layout bytes, not RSS or actual worst-case usable heap; they do not replace the original Durable functional checks above. No bounds/type/metadata lane changed. Different temporary path lengths affect measured bytes; the small differences from historical peaks are not a claimed memory improvement.

### Allowlisted delivery and next gate

Only four paths change: `crates/domain/src/capture_session.rs`, `crates/market-data/tests/ws_supervisor.rs`, `docs/adr/0003-ws-capture-saturation.md`, `docs/handoffs/REC-001D.md`. Domain change is confined to public cancellation validation/comment; supervisor implementation and all other production code/tests are untouched. No accepted spec/ADR, dependency/manifest, networking, composition, reader or schema change. ADR §8 allowlist and full31-row §9 normative matrix remain byte-identical to the rejected head. The preceding124,040 handoff bytes remain exact (SHA256 `4efabe64450bdb5291d8106fe5c2f35eba9fea0ba069dbb5814acc6dd365223a`). All129 other tracked blobs are preserved and verified after delivery.

Historical Windows319 PASS/42 platform failures, Flushed supplemental checks and prior worker/CI runs remain separate evidence. Reader Complete bytes are not concealed or relabelled: unsealed diagnostic prefix stays ValidPrefixIncomplete with absent quality label, owner completeness Unknown; crash cannot reconstruct a missing marker and external inventory remains deferred. U-09/U-10/U-20/C-01/C-03 stay unchanged.

**Next gate: full independent QA of the new immutable remediation SHA**, including public QA-D2, QA-D1, all31 families, B1/B2/B3, R1–R3, Q1/Q2,63 H1 faults, H2,12 Q2 faults and previous regressions. Worker PASS and fresh CI do not grant owner readiness, Integrator acceptance, merge or auto-merge approval. PR #34 remains Draft.


## QA-NEW-01 bounded continuation and proposed Timer contract — 2026-10-08

Disposition: **PARTIAL_IDENTITY_RESTORATION / QA_PENDING** for represented
metadata and stages; **DESIGN_PROPOSED / TIMER_CONTRACT_BLOCKED** for Timer
kind/disposition-dependent stages; **ENV-01 / RELEASE_LINUX_DURABLE_NOT_RUN**.
Whole QA-NEW-01 is not REMEDIATED, READY or ACCEPTED. Worker evidence is not
independent acceptance. Issue20, parent5 and existing Draft PR34 remain open.
The existing claim6024304772/recovery6025249885 and branch
`feat/REC-001D-ws-supervisor` continue; no replacement lineage is created.

### Inputs, preflight and failed-parent evidence

Rejected source: `c9ddf41275dd6c6ced19ab537310e924d7b0be53`, tree
`9ce6413fdd234c8ed7b1934ee18404568d181c54`; inspected unchanged main/base
`39ff0dba797eb010586238ef06fb80e996340401`. Approved prior design:
`cff1e398c3226bc2a86b51442e02054c5996e86a`.

The three supplied inputs were available and hashed before changes:

| Input | Bytes | SHA256 |
|---|---:|---|
| 01-REC-001D-QA-Verification.txt | 5390 | 126a32ed6328c1dfa7967e808e9457ce5ebccedf5a544fe7baea9f44bb0e3784 |
| 02-REPORT.txt | 46764 | c56b6c1a1e61ff1ae8897b2a6fcd9eca8e362d049ffb9a61cbbd3bb278760a8d |
| 03-REC-001D-QA-c9ddf412-evidence.zip | 2315478 | 21cd9abebdb143077a656cc0aaff62389e6fddc9a7a7d1918d35b5c5c90cd769 |

ZIP audit:288 entries,287 manifested payloads,5,701,127 payload bytes;
missing/mismatched/undeclared/duplicate/unsafe entries all0. Embedded REPORT
matches the supplied REPORT. Immutable source snapshots were verified by
SHA256 and Git blob identity:133 candidate paths/125 base paths. Both supplied
patches were independently applied to their frozen snapshots in memory:
133/133 paths reconstructed,132 unchanged for each one-file patch.

The original independent public-identity patch remains4334 bytes, SHA256
`bc3f54d011fd52978cdaf03598e1acb32efb96d9978ee84223e3e22eb0a36295`;
the original Q2 probe remains5718 bytes, SHA256
`8a5bc487e35688d75d9f0a52a78f9eddf1f2343418d9da394aa0167ab7e1f76f`.
No changed parent test is presented as the original independent probe.

On c9 plus the exact supplied public patch, pinned1.98.1 Windows debug/release
both exit101: matching Raw control PASS; unrelated Timer and changed-stamp Raw
behavioral FAIL; original Durable probe fails bootstrap on Unsupported Unix
metadata-directory sync, so its behavior is NOT_RUN. The negative Written
probe physically observes ArchiveStatus::Complete,7 Unknown records and both
seals before terminal sync returns an error. This is false physical sealing,
not successful Linux/Durable FinalizedArchive. The initial parent log's version
metadata was collected outside the repository and is retained as superseded;
the corrected pinned-parent logs identify both the1.98.1 child and metadata.

### Trigger, implementation and represented-contract limits

At c9 `persist_owned` checked authority/kind/cut without matching the original
ObservationIdentity; any authenticated gated frame incremented a generic count,
which could discharge another admitted job and permit false quiescence.

Source-only correction commit: `0d0aebb3937fea15fb3dab2dfe817879af016d3e`,
tree `5810414bed0bab84ec3763532d22d6765ad91351`, sole parent c9. Exactly two
source/test paths changed; no conditional allowlist path was needed:

- `crates/domain/src/capture_session.rs`: original metadata authentication,
  private distinct received stages and bounded original generated-plan snapshot.
- `crates/recording/tests/capture_session.rs`:25 added test functions, including
  the four supplied names retained with preservation/recovery assertions.

Domain file SHA256:
`7c6e9a0d0c7c0da230a71e57ec94ad5e7da5b764fb5947ee4c0118dd9210309d`.
Recording test file:102975 bytes, SHA256
`129c469a785d719896f816d815d7a3fc192122cc10b08ea49a2d119db87618cd`.
Exact correction diff is c9..0d0aebb; the following documentation-only proposal
changes only this handoff and ADR0003. Its immutable containing SHA and exact
docs diff are published externally after commit, avoiding a self-reference.

Raw binds original scope/full tag, attempt, both stamp samples and active
context; a permitted no-loss single-target Unknown/DecodeRejected/SourceGap is
a distinct optional same-observation stage. Stale Raw requires exact empty Raw
then exact diagnostic GAP. QueueOverflow GAP binds target/tag/range/loss_count/
stamp, and no post-receipt coalescing can rewrite confirmed identity. Up/Down
bind scope/connection/epoch/stamp; authenticated same-epoch Down permits the
existing no-write obsolete Up/Pong exception and prevents terminal Up revival.
Unrelated, replaced or repeated stages reject before backend write.

Common Timer matching checks original stream, context, stamp, ID and deadline;
optional Down must match its original connection/epoch/stamp and cannot repeat.
The current API still lacks original Ping/Timeout kind and authority-owned
active/obsolete eligibility. It cannot prove whether Down is required/allowed;
the implementation does not invent those fields or select a new effect policy.
The owner explicitly confirmed this absent approved runtime contract and
requested ADR0003§15 as DESIGN_PROPOSED. Only dependent Timer effect stages are
TIMER_CONTRACT_BLOCKED; the representable original identity check is implemented.

Received Down matching establishes represented record metadata, not proof of
the entire Down/Close job: the existing canonical path creates mandatory Close
later. Existing R2 mechanics are preserved. Generated H1 retention separately
requires authenticated same-W Down plus its matching existing Close and a fixed
old tag/BookId snapshot; epoch writes wait for that Close to settle and match
distinct ordered connection/subscription/book stages, original stamp and checked
expected/next values. Earlier Down and repeated Connection are not fresh progress.
Raw admission carries no payload/digest or required decoder-disposition plan;
this correction makes no payload-content or decoder-required-stage claim.

Ownership/turn/sink foreign checks precede reconciliation. Rejections preserve
original Pending/W, cut, Close, prefix and watermarks. Progress advances only
after gate-confirmed authenticated receipts; storage errors preserve trusted
prefix and physical ambiguity. Last-steward drop conserves original Abandoned
identity/W and prevents proof/seals. QA-D1 checked cancellation precedes removal;
QA-D2 fresh progress excludes earlier Down and partial-confirmed/StorageStopped
work cannot be canceled as uncommitted. Received work cannot use generated
cancellation. B1-B3/R1-R3 and sole borrowed affine proof remain.

### New regression and positive-control scope

The four supplied names remain. Tests now compare ledger/Pending identity,
watermarks, inventory and physical WAL bytes before/after typed rejection, then
complete the rightful original exactly once and reject repeat/foreign completion.
Missing-input/drop cases retain exact Abandoned identity and deny repeated proof
or seals. Original Durable healthy finalization uses the real concrete owner.

Ten new shared helpers each have Written and Durable test functions:
Raw12 substitutions; abandoned identity; stale two-stage; GAP13 substitutions
and coalescing; Raw3 permitted diagnostics; controls3 classes×4 mismatches;
obsolete Up/Pong; Timer common identity; cut plus held Close; Generated H1 original
fresh ordered stages. One additional Durable healthy finalization plus four
supplied names gives25 new functions; recording capture-session suite47.
Loop variants are not extra test functions. Timer common-identity positives do
not establish approved Ping/Timeout disposition semantics.

### Commands, profiles, memory and CI

All logs retain exact command, real exit code, stdout/stderr, source inventory,
HEAD/tree/status,1.98.1 Rust/Cargo, OS/architecture, relevant non-secret overrides
and byte/SHA256 identities. Precommit runs are labeled c9+modified source inventory;
they are not relabeled as a clean immutable checkout. Corrected-head CI is new.

| Required command | Actual worker result |
|---|---|
| `cargo fmt --all -- --check` | Canonical Windows wrapper exit0 on corrected source; new source-head Linux CI exit0. |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Canonical Windows wrapper exit0 after fixing one collapsible_if warning; original failed log retained. New source-head Linux CI exit0. |
| `cargo build --workspace --locked` | Windows exit0; new source-head Linux CI exit0. |
| `cargo test --workspace --locked` | New source-head Linux CI exit0:425 PASS/0 FAIL/0 ignored, including original Durable fixtures and real CLI. Local full Windows rerun NOT_RUN; it cannot close ENV-01. |
| `cargo test --workspace --release --locked` | Linux/Durable NOT_RUN/BLOCKED: no available local Linux executor; existing workflow has no release step. Windows Written targeted release results are supplemental only. |

Fresh source-head [Rust CI run37696079119](https://github.com/al-gri/pro-sclpng/actions/runs/37696079119)
on exact0d0aebb: rust-fmt113048014746, rust-clippy113048014525,
rust-tests113048014707 all success. Each raw log shows expected SHA equals
checked-out0d0aebb, pinned Rust/Cargo1.98.1 and clean checkout checks at completion.
The workspace test log totals425 function/doctest passes, including11 existing
affine compile-fail cases. Debug CI does not establish release or independent QA.
The subsequent immutable documentation head requires its own fresh exact-head CI;
its URL and final command results are recorded in external PR/Issue metadata and
the evidence REPORT after commit. Historical rejected-c9 CI is not transferred.

Pinned Windows Written new-helper debug/release:10 PASS each. Supplied-name
corrected debug/release:3 Written PASS,1 Durable bootstrap environment FAIL,
exit101; Durable behavioral result NOT_RUN. No skips, ignores, feature changes,
portability changes or weakened durability were introduced. The earlier9-helper
logs precede Generated coverage and are retained as historical only. Final
strengthened repeat-abandonment assertions receive exact immutable-head verification
after this documentation commit, recorded in the external REPORT.

Fixed WorkCell metadata grows176->248 bytes (+72 per W), automatically included
in existing size_of budget formulas:cap5 adds216 bytes;cap9 adds432 bytes.
Pinned layout measurement is supplementary, not an acceptance command or RSS.
Existing allocation test profiles stay Written/Flushed. Owner retention/teardown
probes pass debug/release:cap5 peak29571/29570 versus ceiling8575010;
cap9 peak31298/31297 versus8710676. Full-supervisor Flushed cap5/cap9 and100-repeat
probes pass debug/release:peak56710/60820 versus10256259/10410374, released0.
These are requested Layout allocation bytes, not usable heap, RSS or network
memory. Source fingerprints identify the modified tree actually measured.

### Preserved acceptance contract, proposal and remaining gates

Approved cff, rejected c9 and source-head §8 table are byte-identical:4919 bytes,
SHA256 `784f9e20d61806872deba9d305023660cbf471784bac48bd36677d46eae872af`.
All31 §9 requirements remain byte-identical:11593 bytes, SHA256
`7f450da4b09d80a453c2b08344b2d20b8b1023bd406c3f8bc75cee037f4afa84`.
Accepted WAL/schema, dependencies, Cargo.lock, manifests, toolchain/workflow,
recovery/codec, applications and neighboring governance files are unchanged.
Existing inherited PR paths are not cleaned up.

ADR0003§15 recommends **Option A: bounded authority-owned scheduler and opaque
original-token/frozen output plan**. Option B is a sealed supervisor capability
with explicit issuer/revocation/direct-route design; caller-supplied kind or
disposition boolean alone is rejected. The proposal defines Ping/Timeout identity,
serialized authority transitions, obsolete recording, required receipt stages,
same-W mandatory Close, Q2 FIFO, bounded W and positive/negative review tests.
It also closes the interstage design gap: a mutable turn prevents reentrancy in
one operation, while a frozen per-scope plan is needed across serial operations.
This is proposed runtime/API behavior and is not implemented or approved.

Required next gates: Architecture/owner approves an exact Timer authority contract
before dependent extension; Integrator supplies genuine Linux/Unix-sync executor
for pinned full release and independent debug/release QA of all31 families and
corrective scope (Q1/Q2,R1-R3,B1-B3,QA-D1/D2,63 H1,scoped H2,12 Q2 faults,
F1-F6/N1-N3,decoder/continuity/DataHealth/publication/WAL,real CLI,11 compile-fail,
cap5/cap9 bounds/repeats/teardown). Worker debug traversal is mapped separately
in the evidence; no unchanged row inherits independent PASS.

The full reviewed branch must remain frozen during new independent QA. Owner
merge, post-merge exact-head push CI and closing Issue20 are subsequent separate
gates. Parent fullM1 stays open. Deferred adapters/networking/REST/RPI/execution
scope and U09/U10 UNKNOWN/BLOCKED,U20 NOT_PROVEN/FORBIDDEN,C01 BLOCKED,C03 UNKNOWN
remain unchanged.
