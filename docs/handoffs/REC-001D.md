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
