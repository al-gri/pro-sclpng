# ADR 0003 — terminal capture saturation and archive session authority

Status: **PRIOR CONTRACT DESIGN_APPROVED_FOR_IMPLEMENTATION; QA-NEW-01 PARTIAL_IDENTITY_RESTORATION / QA_PENDING; RECEIVED DOWN/CLOSE FIXED_IN_CODE / QA_PENDING; TIMER A DESIGN_APPROVED_A / IMPLEMENTATION_PENDING / QA_PENDING**. Date: 2026-10-08.
Architecture direction: **ARCHITECTURE_DIRECTION_SET**.
This is the bounded REC-001D design revision for [Issue #20](https://github.com/al-gri/pro-sclpng/issues/20), parent [#5](https://github.com/al-gri/pro-sclpng/issues/5), existing Draft [PR #34](https://github.com/al-gri/pro-sclpng/pull/34).
Claim `6024304772`; recovery `6025249885`; latest incoming partial completion [6033824631](https://github.com/al-gri/pro-sclpng/issues/20#issuecomment-6033824631).
Inspected immutable production head: `91e3702f75346e0f310532359a06daa9db701829`.
Inspected main/base: `39ff0dba797eb010586238ef06fb80e996340401`.
Architecture-approved immutable proposal: `cff1e398c3226bc2a86b51442e02054c5996e86a`. Earlier reviewed proposal: `750e85a7ecdd65b0c788726d05a4302b0b42225d`.
The owner supplied **DESIGN_APPROVED_FOR_IMPLEMENTATION** for the complete contract at `cff1e398…`, with R1/R2/R3 **DESIGN_RETEST_PASS** and whole-ADR consistency **DESIGN_REVIEW_PASS**. Exact allowlist and conditions are recorded in the supplied `REC-001D-Architecture-cff1e398.txt`. Earlier production/proposal provenance remains historical.

Normative MUST/MUST NOT statements in §§1–14 are the **approved implementation contract**; §15 is a **DESIGN_PROPOSED** extension requiring new explicit Architecture/owner approval. The approved contract is restricted to the exact paths in §8 and the two delivery documents. At the approved docs-only head they were **not implemented**: Q1 was **BLOCKED / NOT_FIXED**, Q2 **FIXED_IN_CODE / QA_PENDING**. Approval clears the design gate; it is not implementation acceptance or independent QA. Historical worker implementation evidence is recorded in §11; independent QA rejected `ee4e85e5b9c74c8699628e39ea13281a2d91e611` with B1/B2/B3. Their corrective boundaries are in §12. Subsequent independent QA rejected `dfe2c43efc2df0591d67daef4beac0d97fa3b050` with QA-D1: local same-scope failure omitted generated-plan settlement. Section13 records its restoration. Independent QA then rejected `8a8969acce1e0e7873929736daef5d69d78a7e41` with MEDIUM QA-D2: the public registered handle could settle partially confirmed Generated output although the canonical supervisor rejected it. Section14 restores that public boundary, and the appended [REC-001D handoff](../handoffs/REC-001D.md) records factual execution. Earlier worker/CI PASS and docs-only CI cannot establish acceptance of a later head.

## 1. Problem, authority and approval boundary

At the inspected head, sufficiently many separately stamped diagnostics or incompatible loss/control barriers consume all item capacity. The next received raw can fail `queue_gap_loss`, lose its payload without truthful Raw/GAP accounting, and leave its candidate CaptureAttempt reusable by a later suffix. A larger ordinary queue or an extra unadvertised lane does not resolve arbitrary non-coalescing input. Global `Halted` would also freeze admitted drain and healthy neighbors.

The selected design is **terminal affected capture, irreversible archive failure, bounded admitted diagnostic drain**. It reserves the first terminal failure of every configured scope inside the advertised cap and gives the writer owner enforceable authority over publication and finalization. It does not promise to capture every later input of a terminated scope.

Read against current [AGENTS](../../AGENTS.md), [workflow](../WORKFLOW.md), [architecture](../ARCHITECTURE.md), [invariants](../INVARIANTS.md), [project state](../PROJECT_STATE.md), accepted [ADR 0002](0002-domain-event-wal-contracts.md), [WAL v1](../../specs/recording/wal-v1.md) and [DataHealth v1](../../specs/market-data/data-health-v1.md). Existing accepted files remain unchanged. The API/health deltas below have explicit **DESIGN_APPROVED_FOR_IMPLEMENTATION**, restricted to §8 paths. After implementation a new immutable head requires full independent QA; no merge, readiness or task acceptance follows from design approval.

U-09 **UNKNOWN/BLOCKED**, U-10 **UNKNOWN/BLOCKED**, U-20 **NOT_PROVEN/FORBIDDEN**, C-01 **CONTRACT_CONFLICT/BLOCKED**, C-03 **DOC_CONFLICT/UNKNOWN** remain unchanged. No networking, REST healing, RPI normalization, quantity/delete inference, private API, execution, REC-001F or application composition implementation is authorized by this ADR.

## 2. Session and scope states

The authority key is the accepted `(ArchiveId, CaptureSessionId, ClockId)` from ArchiveStart. The owner creates it once, before capture starts, and registers the fixed configured stream/binding set. Equal identifier values alone do not authenticate a handle: supervisor, sink and writer must share the same owner-minted authority instance. Current REC-001D's one unique connection/book per stream remains required; shared-connection multiplexing is not added.

| State owner | States / irreversible transition | Meaning |
|---|---|---|
| Configured stream | `Active -> CaptureTerminated(first_failure)` | No new raw/control capture or operational revival for that stream in this session. Already-admitted work remains owned. |
| Archive session | `Open -> FailedDiagnostic -> DiagnosticClosing -> DiagnosticClosed`; independently `Open -> Closing -> Finalized`; failure during Closing goes directly to DiagnosticClosing | First stream failure atomically sets the archive failure latch; any terminal hard-stop also enters failed lifecycle. Failure during Closing preserves closed admission and invalidates finalization. Failed sessions cannot successfully finalize. |
| Storage boundary | `Writable -> StorageStopped` | Persist error, bad receipt, weak gate or any retained terminal supervisor/storage/order hard-stop stops further writes and latches archive recording failure. Failure is explicit even if bytes were partially written. |
| Mandatory Close owner | `Pending -> Leased -> Settled`; Drop/dispatch error returns `Leased -> Pending` | One logical epoch-bound command and at most one active lease; reclaim changes no ledger ownership or identity. Settled never returns to Pending. |
| Finalization ticket | `Active -> Consumed` on first successful quiescence; Active becomes Invalidated on failure | NotReady leaves Active and its generation unchanged. Failure also invalidates any issued proof authorization; consumed ticket cannot issue another proof. |

The archive latch is monotonic, is set **during the failing admission call before its report is returned**, and revokes all waiting candidates in the same serialized operation. Later Pong, reconnect, Healthy observation, Raw success, watermark or StorageFence cannot clear it. Physical storage frontiers may still advance during diagnostic drain; effective recording health remains Failed. No reset/reopen method exists. An explicit owner action must close/abandon the old session and construct a fresh archive/session; no append-after-restart, inherited attempts or automatic reset is permitted.

### 2.1 Exact first terminal identity

Each preallocated stream owner has one fixed-size `TerminalFailure` slot containing:

- stream and connection identity; observed original EpochTag (including old tag), current affected connection epoch, active Context;
- original ReceiveStamp, input class (`Raw`, `Pong`, `Connected`, `Disconnected`), and typed cause;
- `AttemptIdentity::Candidate(CaptureAttemptNo)` for non-Pong raw; `NotRaw` for controls; `NoRepresentableSuccessor { frontier: u64::MAX }` when checked attempt allocation cannot produce a successor;
- a fixed failure identifier `(session, stream)` and one inline mandatory-Close descriptor/state (`Pending`, `Leased`, `Settled`), or an opaque reference to the existing Down-owned Close; no second Close owner.

No failed payload, unbounded error string or list of later rejected inputs is retained. This exact identity is **diagnostic evidence**, not a confirmed RawInput, GAP, RecordNo or durable observation. For a representable raw candidate, advance the supervisor's *consumed-input frontier* once to that candidate when installing failure; do not advance the WAL *accounted frontier*. Expose both concepts separately. Never invent `MAX+1`, wrap, claim an accounted attempt, or reuse the candidate. The stream cannot admit a suffix that would need this missing attempt.

The first received input of a known configured scope that cannot be represented by bounded Raw/loss/control accounting MUST install this slot and latch the archive. This includes stale diagnostics and received controls when no work slot remains, not only current raw. An invalid unknown connection/unrecognized epoch fails validation before capture admission and is not assigned to a guessed scope. After a known stream has terminated, its terminal status takes precedence over epoch/decoder/reconnect processing for every subsequent known-connection ingress.

## 3. Chosen public API contract

The approved API is **owner-bound handles plus explicit admission/drain envelopes and consumed command leases**. These semantic signatures are implemented within the bounded library profile; concrete Rust type/report spelling and execution evidence are recorded in §11:

```rust
CaptureSessionOwner::create_new(path, accepted_start, bounded_profile)
    -> Result<(CaptureSessionOwner, SessionTurn), OwnerError>;
CaptureSessionOwner::register_supervisor(&mut SessionTurn, bindings, budget)
    -> Result<(SupervisorSessionHandle, BoundRecordSink), OwnerError>;
PublicWsSupervisor::new(config, SupervisorSessionHandle)
    -> Result<PublicWsSupervisor, SupervisorError>;

// queue_text keeps the existing connection/epoch/stamp meaning, but borrows
// input bytes and copies only bounded retained payload into the session arena.
queue_text(&mut SessionTurn, connection, epoch, stamp, bytes: &[u8]) -> AdmissionReport;
queue_connected(&mut SessionTurn, connection, epoch, stamp) -> AdmissionReport;
queue_disconnected(&mut SessionTurn, connection, epoch, stamp) -> AdmissionReport;
queue_tick(&mut SessionTurn, stamp) -> AdmissionReport;
start_commands(&mut SessionTurn) -> AdmissionReport;
drain_one(&mut SessionTurn, &mut BoundRecordSink) -> DrainReport;

CaptureSessionOwner::dispatch(&mut SessionTurn, command: CommandLease, effect) -> DispatchReport;
CaptureSessionOwner::outstanding_close_owners() -> CloseOwnerSnapshot;
CaptureSessionOwner::reclaim_close(&mut SessionTurn, CloseOwnerRef) -> CloseLeaseReport;
CloseLease::into_command(self) -> CommandLease;
CaptureSessionOwner::confirm_closed(&mut SessionTurn, CloseOwnerRef, AuthenticatedClosure)
    -> CloseSettlementReport;
CaptureSessionOwner::register_publication_guard(&mut SessionTurn, OwnerBoundPublicationGuard)
    -> Result<(), OwnerError>;
CaptureSessionOwner::publish(&mut SessionTurn, candidate, fence, consumer) -> PublicationReport;
CaptureSessionOwner::begin_finalization(&mut SessionTurn) -> Result<CloseTicket, OwnerError>;
PublicWsSupervisor::quiesce(&mut SessionTurn, &CloseTicket) -> QuiescenceReport;
CaptureSessionOwner::finalize(&mut SessionTurn, &mut QuiescenceProof) -> Result<FinalizedArchive, OwnerError>;
CaptureSessionOwner::close_diagnostic(&mut SessionTurn) -> DiagnosticCloseReport;
```

`AdmissionReport { session_disposition, scope_disposition, outcome, commands }` places disposition **outside** its inner typed `Result`. On the first failure `outcome` retains `QueueExhausted { stream }` or `CounterExhausted("CaptureAttemptNo")`; the same report carries the installed failure identity, CloseOwnerRef and at most one terminal Close lease. Initial Close issuance uses the same Pending->Leased transition as reclaim; an existing Leased/Settled Down owner yields no second lease. An error must not discard mandatory Close ownership or hide archive failure. Ordinary received input has `Admitted`, `CoalescedLoss` or `AlreadyTerminated` outcome with the immutable failure identity in the outer scope/failure fields. New typed terminal/closed/authority errors are explicit API deltas. Reports use bounded fixed-capacity records/events/command views; they cannot grow a history.

`DrainReport { session_disposition, outcome: Result<Option<DrainResult>, SupervisorError> }` exposes `DiagnosticOnly { archive, session, failure_id }` even on `None` or error. At the instant the archive latch is set, all previously admitted observations and all future healthy-neighbor observations become diagnostic-only. Drain checks that latch before releasing the result. Already returned results carry observation identity and no reusable publication permit; publication must revalidate through `owner.publish` at use time. A pre-failure result held across failure therefore cannot authorize a later market effect.

`BoundRecordSink` is private-field, non-clonable and minted by this owner with the supervisor handle. This initial profile registers exactly one supervisor for its complete fixed N-scope registry. `new` validates config's active Context, recording gate, segment and next RecordNo against that bound writer/accepted bootstrap state; they are not independent caller authority to assign an archive prefix. Replace the freely interchangeable `&mut impl RecordSink` at the canonical boundary; wrong owner/session/gate is rejected before a write. A test adapter must implement the same authority and gate validation, not manufacture a successful receipt to bypass it. The bounded filesystem sink binding is implemented by the approved recording owner. A socket adapter remains outside scope.

### 3.1 Complete method outcomes

| Public surface | Active / healthy-neighbor behavior | Terminated scope / failed archive behavior |
|---|---|---|
| `new` / registration | Validate accepted IDs/context/gate, configured unique owners, legal checked budget; reserve slots before receiving input. | Cannot adopt a failed/sealed writer, foreign handle or old session. Failure returns no started supervisor. |
| `start_commands` | Once only; reserve bounded Connect jobs, return their leases. `AlreadyStarted` on repeat. Pre-admission capacity error leaves start state unchanged. | No Connect for terminated scopes; a stale lease is denied at dispatch. An archive already failed can service only still-active neighbors diagnostically. |
| `queue_text` non-Pong raw | Allocate exactly one archive-wide per-stream candidate; admit Raw, truthful existing stale diagnostic or exact compatible same-cut-side tail GAP. | First unrepresentable input terminates scope and consumes candidate as evidence only. Post-cut neighbor loss cannot extend a pre-cut GAP; it needs a new counted owner or terminates that neighbor exactly. Failed-scope repeats return `AlreadyTerminated`, with no payload retention/allocation/decode/new command. |
| `queue_text` exact Pong | Received control, no CaptureAttempt. Preserve existing FIFO/Q2 cancellation rules while active. | First unrepresentable Pong terminates with `NotRaw`; repeats do not clear deadlines/latch, record Up or revive capture. |
| `queue_connected` / `queue_disconnected` | Retain original epoch/stamp; existing reconnect-time and recognized-epoch validation; admitted controls drain. | First capacity failure terminates with `NotRaw`; later ingress returns `AlreadyTerminated`, including reconnect/Connected at a newer epoch. No epoch advancement/reconnect can restart that scope. |
| `queue_tick` | Generated proposals, not received source inputs. Deterministic stream iteration. Reserve a work unit before committing TimerId/owner flags. Partial admission reports exact admitted scopes and first typed capacity error. | Skip terminated scopes. Active neighbors can admit timers. A capacity-rejected proposed timer is retriable without changing its counter/flags or creating a terminal input identity. Other checked timer/time errors keep their explicit hard-stop policy. |
| `drain_one` | One FIFO observation or separately owned ready transition; validate each write through RecordingGate before dependent effects. | Admitted work drains diagnostically until empty or StorageStopped. Scope termination alone does not set legacy global `Halted`. StorageStopped returns the original typed error once and terminal storage disposition thereafter. |
| `snapshot(stream)` | Existing snapshot fields plus capture status, consumed frontier, accounted frontier and archive disposition. Unknown stream stays `None`. | First failure is immutable. Never report a missing candidate as recorded/accounted or clear failure on historical Up. |
| `queued_items`, `is_halted` | Retain queue-length/storage-stop meanings for compatibility; neither is total retained accounting or publication authority. | `queued_items == 0` can still mean pending work/marker/terminal metadata. `is_halted == false` can coexist with archive FailedDiagnostic. |
| New `retention_report`, `session_status`, `terminal_failure(stream)` | Constant-bounded snapshots with explicit reserved/used/allocation counts. | Readable after failure/storage stop; no state changes, acknowledgement, reset or authority grant. |
| New `outstanding_close_owners`, `reclaim_close`, `confirm_closed` | Fixed-capacity owner discovery; Pending->Leased reclaim; authenticated same-command closure settles ownership. | Available after StorageStopped, Closing, DiagnosticClosing and DiagnosticClosed. Leased/Settled/foreign-ref outcomes create no second lease or owner; Drop/error recovery preserves the same pending Close. |
| `begin_finalization`, borrowed `quiesce`, `finalize` | One CloseTicket; repeated NotReady keeps it; first Ready consumes its issuance right and returns one affine proof; finalize consumes proof once. | Archive failure returns terminal FinalizationInvalidated, never an endless NotReady; foreign authority changes no rightful state. Errors never reopen admission or mint a new ticket/generation. |
| `reconnect_delay_ns` / policy constants | Existing deterministic pure helper remains; delay is engineering policy. Proposed saturation policy is version 2. | Computing a delay grants no command/capture permission. |

Before start, ingress/drain still reports `NotStarted`; terminal metadata getters remain readable. Unknown connection, invalid configuration, unrecognized epoch and `ReconnectTooEarly` retain typed validation outcomes when no terminal known scope supersedes them. Identity/time/storage errors are never recast as a successful GAP.

### 3.2 Already-admitted controls, plans and commands

No admitted observation is evicted, assigned a different identity, reordered or replaced by a fabricated record. Raw, stale diagnostic and GAP keep their original stream/tag/attempt/stamp; recording is permitted only if existing WAL validation accepts them. Admitted TimerFired remains a truthful timer observation even when its operational owner is now obsolete.

For a terminated scope, admitted Connected/Pong may record truthful historical Transport Up **only where existing terminal-Down/obsolete-control rules allow it**; they cannot change live capture/scheduling or issue Subscribe. Already terminal same-generation Connected/Pong remains `ObsoleteControl`, not an invented Up. Admitted Disconnected records its historical Down through the gate and may deliver an idempotent Close; it cannot create a new reconnect plan. An admitted raw subscription acknowledgement produces diagnostic evidence only, without reopening the subscription. Active neighbor controls keep their normal subscribe, Ping, Close and reconnect service, tagged with archive DiagnosticOnly.

Pending disconnect completion is an owned generated plan, not a received observation. Ordinary Down, its returned Close lease and its pending epoch-completion plan share one transferred W owner with fixed-size state. Completion waits until that Close is successfully dispatched or the owner has authenticated external closure evidence; it cannot overwrite an outstanding Close or require an unreserved unit after irreversible Down. The same owner can then hold the bounded completion result/one ReconnectAfter lease. This is an explicit scheduling/API delta: a caller dispatches Down's Close before a later drain can complete that transition. On scope termination cancel only uncommitted epoch/reconnect completion, preserve every earlier successful Down/record and outstanding mandatory Close, and release W only when its remaining ownership is settled. Do not fabricate EpochAdvance records for canceled completion. Already persisted epoch records remain historical; a partial completion error is not rolled back. H1's successful Down/Close return precedes fallible completion for an active neighbor and is never lost inside a later error.

Checked generated-plan settlement under the rightful SessionTurn MUST precede removing its runtime plan/WorkOwner or changing WorkKind. `cancelled_plan=true` means settlement succeeded, not merely that a plan was found or removed. Reclassification alone cannot cancel an obligation. A failed checked cancellation returns its typed error with the same plan/owner/Close retained. Scope-local termination cancels only that scope's generated plan; diagnostic lifecycle cancellation is explicit for each applicable owner. Received Raw/GAP/control/timer obligations never enter this cancellation path. See §13 for the concrete transition and partial-write guard.

All commands are opaque, non-clonable leases bound to authority, stream, epoch and command owner. Dispatch consumes the lease and rechecks current scope/lifecycle under the same serialized boundary. A pre-failure Connect, Subscribe, Ping or ReconnectAfter for the failed scope returns `CommandRevoked` without effect. Mandatory Close targets the actual live connection epoch, even when the failed input carried an old tag. If an earlier Down already owns that epoch's Close, the terminal slot references the same transferred W owner; it does not mint another owner/lease. Only when no existing Close owns that epoch does the reserved terminal slot own an inline Close. Reclaim/dispatch remain available after StorageStopped, Closing, DiagnosticClosing and DiagnosticClosed; these states deny only Connect/Subscribe/Ping/Reconnect. Dropping/canceling a nonmandatory lease releases its counted work. No detachable command/permit from an old result bypasses dispatch.

**R2 — concrete reclaim and discovery.** `owner.outstanding_close_owners()` returns a fixed-capacity `CloseOwnerSnapshot` of at most N entries (one current Close per configured connection), each containing its opaque CloseOwnerRef and Pending/Leased state. It is read-only and available even after storage stop/diagnostic descriptor closure; it never allocates a side registry or grants dispatch permission. Settled is omitted from outstanding discovery but an already obtained reference may be checked by reclaim. `CloseOwnerRef` names an existing logical owner by private authority binding, stream/connection, original connection epoch and Close command identity/storage association, not a recyclable W slot address alone. This metadata and the one per-scope current-owner index are included in K_scope/K_work. No unbounded tombstone/history map is retained. A retired reference cannot alias a new job/epoch in a reused slot; it returns OwnerRetired (or Settled while the same settled owner is still known), without creating a Close.

`owner.reclaim_close(&mut SessionTurn, CloseOwnerRef) -> CloseLeaseReport` validates turn/authority/ref identity before any mutation. Its bounded outcomes are `Leased(CloseLease)`, `AlreadyLeased`, `Settled`, `OwnerRetired`, `AuthorityMismatch` or `InvalidOwner`. `CloseLease` is the affine mandatory-Close token; `CloseLease::into_command(self) -> CommandLease` moves it into that opaque command's mandatory-Close variant without allocation, new owner, identity/state change or duplicate lease. The caller dispatches `owner.dispatch(turn, close_lease.into_command(), effect)`; it cannot keep/reuse the moved CloseLease. Both a transferred W owner from ordinary Down and an inline reserved terminal Close obey exactly this table:

| Current state | Operation | Result / next state |
|---|---|---|
| Pending | Initial issuance or reclaim | One affine non-clonable CloseLease; Pending->Leased. |
| Leased | Second reclaim | AlreadyLeased, no mutation or second active lease. |
| Leased | Drop lease or dispatch error | The same owner returns to Pending; no new epoch/command identity/work owner. |
| Pending or Leased | Same-owner authenticated closure | Settled; outstanding lease authorization is disarmed. |
| Leased | Successful synchronous Close dispatch | Settled; consumed lease is disarmed. |
| Settled | Reclaim, stale-lease Drop or attempted dispatch | Settled/AlreadySettled, no new Close or effect; never Pending again. |
| Any rightful state | Foreign turn/authority/ref or invalid owner | Typed error; rightful owner/state/lease unchanged. |

There is **no lease nonce/generation counter**. The affine lease and its matching owner state enforce at most one active lease. Drop bookkeeping uses an isolated private state Cell (or equivalent non-panicking mechanism) included in the existing owner metadata, not a RefCell borrow that may be held across an effect callback. Drop resets only its still-matching Leased owner; a lease disarmed by dispatch/authenticated closure cannot undo Settled or mutate a recycled owner. Existing epochs remain checked/no-wrap; reclaim increments none of them and cannot strand Close because a counter has no successor.

`owner.confirm_closed(turn, ref, AuthenticatedClosure)` accepts only opaque trusted owner-bound closure evidence for that exact connection/epoch; a boolean, wrong-epoch event or foreign token cannot settle it. It is a pure acknowledgement contract, not a new network adapter. `DispatchReport` distinguishes `Dispatched`, `AlreadySettled`, `Denied { error, returned_lease: CommandLease }` and `DispatchFailed { effect: Unknown }`. A foreign turn/owner/authority denial validates before effect and returns the same still-valid affine lease in its bounded report, leaving rightful Leased ownership unchanged. If that returned lease/report is subsequently dropped, ordinary Drop returns its same owner to Pending. An effect error instead consumes/disarms the dispatched lease and returns the same owner to Pending; it can follow an actual physical Close, so the caller may explicitly reclaim the **same epoch-bound idempotent Close**. The adapter contract makes repeated Close of that epoch harmless/already-closed; it must not close a newer connection that reused an external socket identifier. Neither a successful logical dispatch nor a retry promises exactly-once physical execution or unambiguous remote delivery.

Discovery, reclaim, Drop and dispatch-error recovery leave W, reserved-owner counts and command identity unchanged. Close settlement also does not itself allocate/free a work unit: a transferred W owner is released only when its whole Down/plan/result ownership is settled; an inline terminal slot remains reserved regardless of Close state. Diagnostic closure, dropped lease or arbitrary cancellation cannot fabricate Settled/external closure. Mandatory Close remains discoverable/reclaimable/dispatchable until settled.

The terminal Close is a fail-safe transport cessation command emitted on admission failure, **without claiming a durable Down**. This is an explicit gate-before-effects exception requiring approval; successful historical Down/Close still follows the existing gate. No networking is added by specifying the pure dispatch decision.

## 4. Full retained bounds and admission progression

Let `N` be fixed configured scopes (`1..=4`), `M = max_total_items` with existing legal lower bound `M > 4N`, `F_s` the per-stream frame limit, `B_s` the per-stream byte limit, `P` the maximum retained raw-message size. All arithmetic/allocation checks occur at construction.

Reserve **N stream-owner/terminal slots and one archive-owner/marker slot inside M**. Let `W` count retained work-owner units, including admitted/generated obligations with zero live Rust references after abandonment, across queued ingress, in-flight drain, generated pending transitions, outstanding command/result ownership (including a Down-owned mandatory Close) and waiting publication candidates retained by this session. Inline terminal Close remains inside its reserved stream owner. Transferring queued work to a pending plan/result does not remove its unit. Authenticated completion or explicitly permitted cancellation settles its obligation; release also requires all remaining references/leases to finish. Drop cannot substitute for accounting; abandoned obligations remain inside their original W cells until the boundary itself is destroyed, with an explicit terminal outcome at the next rightful serialized call. A generated candidate/command requiring a separate owner first reserves a free work unit. One unit owns one observation/job, with a fixed-size bounded output plan; it cannot contain an unbounded batch of received observations.

```text
W_max                  = M - N - 1
retained_item_bound    = W + N + 1 <= M
raw_work_limit         = min(M - 4N, W_max)
raw_frames_s           <= F_s, payload-bearing Raw owners R <= raw_work_limit
retained_payload_bytes <= sum_s min(B_s, F_s * P)
```

The N stream owners include binding/runtime state, optional first failure and one inline terminal Close descriptor; the archive owner includes latch, lifecycle, first-failure marker descriptor/state and bounded frontier/cut metadata. They count even before failure. No separate failure/Close/marker queue is added outside M. R counts only payload-bearing Raw buffers (queued or in-flight), not zero-payload stale diagnostics or GAP owners; those still each consume W. Timer-owner identifiers are references to counted work, not extra queued observations; their fixed bytes are counted. An output plan is at most three RecordNos, one command and four event descriptors for one work owner (start has up to N separately counted Connect jobs); replace variable Vec/String retention with fixed arrays/bounded text. Down's mandatory Close must settle before the same owner emits ReconnectAfter. A pending publication candidate occupies a work unit, not an invisible side map. Admission is allowed only if **all** required owner units can be reserved atomically.

This deliberately replaces the inspected implementation's overlapping `4N` control / `2N` loss reserve admission policy with one work ledger plus guaranteed terminal slots; it is a version-2 API/admission change. Ordinary current-loss tail coalescing remains: only same stream, exact tag, contiguous attempts, compatible reason, **same admission side of the failure cut**, and no intervening Raw/control barrier. A permitted same-side tail update consumes no new work unit and retains its original range observation stamp; no per-attempt samples are invented. Stale diagnostics requiring distinct stamps cannot be silently folded into that GAP.

**R1 — immutable failure-cut membership.** Before first archive failure, admitted record-bearing owners belong to the open admission phase. The first failure atomically fixes the one session cut: every already-admitted record-bearing owner, including queued/in-flight observations and admitted timers/generated required records, is PreCut; every subsequently admitted record-bearing owner is PostCut. Membership follows **admission order**, never physical queue tail/position. Each owner has fixed-size CutSide metadata inside K_work; the archive owner retains the fixed-cut state/counts inside K_archive. A finite scan of the bounded ledger can fix membership; no new unbounded admission-sequence history or numeric counter is needed solely for this boundary.

Once the cut is fixed, the stamps/ranges/counts of every PreCut GAP are frozen. A PostCut attempt cannot extend a PreCut GAP, even if stream/tag/reason match, its attempt is contiguous and that GAP is still the physical tail. A PostCut loss must reserve a **separate counted PostCut owner** with its own original observation stamp (or coalesce only into an eligible PostCut tail). If W is exhausted, the affected still-active neighbor terminates truthfully through its own reserved failure slot and consumes its candidate exactly once as diagnostic evidence. Never move that input before the marker, mutate old GAP fields, or reuse the candidate. Marker failure does not reset/reopen the cut or permit new coalescing into PreCut; StorageStopped continues to forbid new ingress/write, while existing identities remain explicit.

### 4.1 Metadata and bytes

Construction computes and advertises constants `K_archive`, `K_scope`, `K_work`, `K_registry`, `K_encoder`, `K_backend`, including actual backing allocation/capacity, fixed maps/arrays, command text limits, error descriptors, validator state, registered guard and session-handle overhead. CutSide, fixed-cut counters, current Close-owner references/lease-state Cells and finalization ticket/proof state are included in those existing owner capacities. The bounded profile includes explicit candidate-content/effect-reference limits; a candidate exceeding those limits cannot be retained by this owner. `K_work` includes the largest admitted candidate/job representation, not only ingress enum size. The bound for the complete session-owned capture/storage boundary is:

```text
metadata_bound = K_archive + N*K_scope + W_max*K_work + K_registry
payload_bound  = sum_s min(B_s, F_s*P)
retained_bytes_bound = metadata_bound + payload_bound + K_encoder + K_backend
```

`K_registry` covers the fixed bootstrap definition/artifact-binding registry for the accepted configured profile. This owner API freezes those definitions after bootstrap; dynamic config/spec/stream redefinition is not admitted through it. Generic WAL v1 remains capable of definitions, but an arbitrary already-populated unbounded writer cannot be adopted as a bounded capture owner. Validator cloning, bounded decoder scratch and any second live representation of one raw are included in `K_encoder` (bounded maximum frame/workspace), not hidden in payload counters. The writer/backend buffering allowance is explicit. The implementation must reject a profile for which it cannot compute these capacities; reporting only `size_of` or payload lengths is insufficient.

This initial bounded-owner profile permits **one segment only**, with a UTF-8 path of at most 4096 bytes; it rejects rotation before any non-final seal is appended. This is an explicit owner API restriction, not a schema/recovery revision. Generic low-level multi-segment WAL support remains unchanged. `K_backend` includes that bounded path/descriptor/buffer. `DiagnosticCloseReport.physical_report` means the fixed-size known writer-frontier/closure assessment, not an eager reopen/scan of every historical path. If a prior external multi-segment archive is inspected, `WalReader::open_segments` remains an independently budgeted recovery operation outside this fresh owner; it cannot be adopted into this bounded profile. No hidden growing segment inventory is retained.

Borrowed incoming bytes belong to the caller; retention copies into a bounded session arena. A caller Vec with tiny length and huge capacity must not become retained capacity. Once the call returns, failed/oversized input storage is not owned by the supervisor. Transient bounded encoder/decoder copies remain included above. Copies of snapshots kept by callers are outside this owner's retention and must not be retained again inside a hidden owner cache. No end-to-end network adapter memory bound is claimed.

`retention_report()` returns `item_cap`, fixed owner reservations, queued/in-flight/pending/lease/candidate work counts, W used/free, PreCut/PostCut record-bearing ownership counts and immutable-cut status, Close states/storage associations, ticket/proof state, filled failure slots, marker state, per-stream raw frame/length/allocated-capacity counts, actual metadata/buffer allocations and the computed byte ceiling. References and bounded report copies do not become new retained work owners. The sum is asserted at each mutation, including reclaim/Drop/error/repeat/cancel/drain paths. `queued_items()` alone does not advertise M. Error strings are bounded typed descriptors. Repeated terminated ingress retains neither a new stamp nor a payload nor a new diagnostic item.

### 4.2 Legal cap traces (normative; execution evidence in §11)

Use a setup that drains epoch 1->2 reconnect/ack/snapshot, giving a consumed/accounted frontier `c=2` and empty work ledger. Releases of returned command/result leases are explicit. Each old input below is separately stamped, oversize, and requires its own existing stale diagnostic.

| Trace | W / reserved owners | Proposed exact progression |
|---|---|---|
| One stream, M=5 | W_max=3; 1 stream + 1 archive owner; raw_work_limit=1 | Old candidates 3,4,5 occupy three work units. Old candidate 6 cannot fit: F_A records original old tag/stamp/candidate 6, consumes frontier once and sets archive Failed. Item accounting is `3+1+1=5`. Next old/current/Pong/reconnect ingress is AlreadyTerminated, no candidate 7. Drain attempts 3,4,5 in order; marker follows the admitted cut. No Raw/GAP for 6. |
| Two streams, M=9 | W_max=6; 2 stream + 1 archive owner; raw_work_limit=1 | Six old-A diagnostics take W=6. Seventh old-A candidate installs F_A; eighth/ninth calls retain nothing. B's first unrepresentable current candidate has its own reserved F_B even at W=6: `6+2+1=9`. One archive marker only; failure slots remain exact and distinct. |
| Same M=9, drain before B | Drain releases one W before B ingress | B can admit truthful raw/loss if its own limits allow; B remains active, but its report/drain/publication state is DiagnosticOnly because A already failed. No A revival. |
| F2 tail, attempts 1 then 2..=33 | One admitted Raw owner + one compatible GAP owner | First loss creates known range 2; later losses coalesce to 2..=33/count32 with no extra work. No terminal failure merely from the 32 compatible calls. At M=9 neighbor B can use available work; normal-cap F2 serviceability also remains required. |
| Mixed/non-coalescing saturation | W reaches W_max through distinct Raw/control/stale/tag/stream barriers | Each admitted owner retains exact FIFO identity. The next incompatible received input uses its scope's reserved first-failure slot, never merges across a barrier or evicts a neighbor. Every other scope still has its own reservation. |
| R1 pre-cut tail B, free-W variant, M=9 | Persisted B raw1; five other owners + tail GAP B 2..3/count2 make W=6. A fails and fixes cut. Drain/release one earlier owner before B loss4. | W becomes5; contiguous B loss4 creates new PostCut GAP 4..4/count1, W returns6 and total9. Old GAP stamp/range/count stay identical. WAL drains old GAP, marker, then new GAP; B consumed frontier4 is distinct from accounted frontier1->3->4 at truthful drain boundaries. |
| Same R1 trace, W exhausted | Keep all six owners when contiguous B loss4 arrives before marker. | No extension/new GAP. Exact F_B(candidate4, original tag/stamp/cause) fills B's already reserved slot, W stays6 and total9. B consumed frontier4; accounted frontier reaches only3 after old GAP drain. Marker/cut remain unchanged; no B suffix or attempt reuse. |

These traces intentionally stop accepting diagnostics earlier than the inspected Q1 forensic tests' five/nine queue entries: those tests assert the defect under policy v1, not an obligation to hide extra items under the same cap. Allocation and publication leases competing for W can cause earlier truthful terminal admission; they never defeat the guaranteed failure slots.

## 5. Persistence and diagnostic lifecycle

Use existing tag-8 `Control::Recording(RecordingEvidence { health: Failed, reason: QueueOverflow, kind, through })` (wire name **RecordingEvidence**) as **one archive-wide observation** for the first saturation failure. No schema/Control tag/Gap shape changes are proposed. The reserved archive owner captures the original first-failure ReceiveStamp/active Context and a finite admission cut. It does not encode the exact terminal input's stream/tag/attempt in WAL; that evidence remains runtime-only.

The marker's finite persistence cut uses section4's **immutable admission-order membership**, including Raw, stale diagnostics, GAP, Connected, Pong, Disconnected, admitted PingTimer/PongTimeout and already-admitted/in-flight generated required records. Drain every PreCut record-bearing owner's required records in existing dependency/FIFO order, then attempt the reserved marker, then drain PostCut records. A physically adjacent tail GAP is not permission to cross that boundary: PreCut fields remain frozen; a later neighbor loss has a separately counted PostCut owner or exact terminal failure. There is no mutation/relocation of post-cut input before marker.

Still-unadmitted generated epoch-completion output, outstanding command/result leases and waiting candidates remain counted ownership but are **not marker blockers**. Defer only that unadmitted generated output until the marker attempt has settled; its eventual record-bearing admission is PostCut and obeys original same-generation barriers, including post-cut observations, and mandatory-Close settlement. Already-admitted generated records cannot be reclassified as an unadmitted plan to evade the cut. This avoids a cycle where a pending completion waits for post-cut input while the marker waits for that still-unadmitted completion. Canceled failed-scope generated completion is explicitly settled, not recorded as an invented observation. Later stream failures fill their own slots and do not enqueue additional archive markers. No marker preempts/replaces an admitted PreCut Raw/control/GAP/Timer/required record. Finalization quiescence, unlike this persistence cut, requires all generated/lease ownership to settle.

At marker emission choose the latest trusted **strictly earlier contiguous watermark** of the stated WatermarkKind; `through < marker RecordNo`, consistent with all previously recorded watermark observations. Sampling at emission avoids regression relative to a receipt in the admitted prefix. If no known prior bound exists, Failed may truthfully carry `None`; None does not erase a known frontier. Never use the marker's own receipt as its body watermark, a future RecordNo, or a weaker completion as the required RecordingGate. The marker itself must obtain an exact receipt covering its assigned RecordNo at the configured gate before being reported recorded.

This marker **does not account the missing CaptureAttempt**, advance its accounted frontier, authorize a later raw suffix, or substitute for GAP. QueueOverflow remains the actual cause of saturation; a CaptureAttempt counter-boundary terminal uses `Reason::Unknown` rather than falsely claiming QueueOverflow. If storage/order failure prevents a marker, the latch and terminal evidence remain explicit; no durable-marker promise, retry/replacement under an assigned RecordNo, rollback, or assertion of absent failed bytes is made.

Persist error, receipt mismatch and weak gate retain the original typed error, stop storage globally and leave marker state `Unconfirmed(error)`; a later successful-looking receipt cannot unstop storage or clear the latch. **The cut and each owner's PreCut/PostCut membership remain fixed on marker failure**; neither failure nor diagnostic closure reopens old GAPs for coalescing. A prior admitted-prefix write failure can prevent reaching the marker at all. Successful earlier records/commands remain reported; failed or partially written records are not confirmed. No automatic writer swap/retry occurs. Diagnostic closure may report additional flush/sync failure without changing the original failure.

On first capture failure, the already-registered owner policy is `ContinueDiagnosticUntilExplicitClose`: retain/drain the admitted cut, attempt the marker once, and continue bounded raw/control diagnostics and permitted transport service for still-active neighbors while storage remains writable. Suppress **all** market publication.

`close_diagnostic(turn)` is **pollable**, not a recording-to-market-data call. Its first invocation in FailedDiagnostic atomically enters DiagnosticClosing, stops ingress and non-Close operational commands, cancels remaining generated reconnect plans/nonmandatory leases with explicit bounded outcomes, and returns `DiagnosticClosing` while writable admitted record-bearing ingress/marker ownership remains. The caller continues `supervisor.drain_one(turn, bound_sink)` to settle those observations/marker through the gate; closing makes operational timer/subscription owners obsolete but does not discard admitted records. A later close invocation performs optional truthful flush/sync and closes the descriptor **without final seals** once that work is settled. With StorageStopped it may close immediately and report the unconfirmed undrained owners rather than attempting impossible writes. Recording does not import or secretly drive the supervisor.

Called in Open/Closing/Finalized it returns `WrongLifecycle` without implicit abandonment/reset. Failure during Closing instead enters DiagnosticClosing directly and invalidates finalization while keeping admission closed; diagnostic close can then be polled without a new lifecycle generation. Repeated calls in DiagnosticClosing poll the same bounded state; in DiagnosticClosed they return its current closure/remaining-Close report without new writes. The terminal result is `DiagnosticClosed { input_completeness: Unknown, physical_report, watermarks, marker_state, failure, outstanding_close_owners, undrained_owners }`, never FinalizedArchive. Its Close discovery references use section3.2's bounded snapshot: Pending/Leased Close remains counted, reclaimable and dispatchable until Settled, even after descriptor closure. Any undrainable ingress observation is explicitly retained/reported as unconfirmed, never silently evicted to obtain closure. StorageStopped permits reporting, reclaim/mandatory Close and descriptor closure, not further append. Dropping the writer owner does not finalize; any surviving close lease retains its authority/owner state. This profile creates no non-final seals/rotation; recovery of previously created external segment seals remains historical byte interpretation only.

## 6. Enforced publication and finalization boundary

Choose a pure opaque `CaptureSessionAuthority` in domain and `CaptureSessionOwner` in recording. The initial implementation contract is single-threaded/single-writer, non-Send/non-Sync handles sharing private `Rc<RefCell<...>>` state (or an equivalent borrow-checked private representation). Creation issues exactly one non-clonable `SessionTurn`; every mutating canonical method requires `&mut SessionTurn` from that same authority for the full call. There is no method to mint another turn. Section3.2's non-panicking Drop permits only bounded private Close-state housekeeping and per-W abandonment notifications. It cannot settle an unconfirmed admitted obligation, admit input, write records, change the cut/lifecycle or grant publication. Canonical abandonment reconciliation requires the rightful `&mut SessionTurn`; see §12.1. A dispatch/publication callback receives only its bounded view, never the turn or a capture mutation capability; safe Rust cannot capture that already mutably borrowed turn to call ingress reentrantly. Compile-fail tests must establish this boundary. Thus a valid received-ingress call cannot return generic Busy and silently discard its input; known received saturation follows section2.1. Read-only snapshots may be called without a turn. Cross-thread/async support requires a separately reviewed synchronization contract. An exposed boolean is informational only.

The owner **exclusively owns the fresh WalWriter** and its accepted ArchiveStart/session binding. It exports neither `&mut WalWriter`, `DerefMut`, `into_writer` nor an appendable file handle. The bound writer itself validates the authority/lifecycle at `append`, `rotate` and `finish`: arbitrary generic append of final SegmentSeal or ArchiveSeal cannot bypass the owner. Unbound low-level WalWriter use can remain for unrelated tests/tools; it cannot attach to an active capture session or produce a canonical FinalizedArchive/publication capability for it. No presealed writer can be adopted. Canonical consumers accept owner-issued typed results, not a caller's physical path or hand-built permit.

`OwnerBoundPublicationGuard` is a proposed **sealed opaque domain type**, not a caller-implementable boolean-returning trait, with private authority binding and mutable current canonical state/candidate registry; it is registered once before publication is enabled. Its state updates require an owner-bound canonical recorded-step capability and must evaluate the accepted usable-data, scope, candidate-current/revocation, mode/gate and finite-fence relations from validated current canonical state, not from caller-supplied booleans or the candidate alone. No public DTO/parsed proof token can construct that capability. The owner-owned registry and bounded candidate content count in the ledger/profile above. No production canonical guard currently exists in recording; the pure domain implementation, capability-issuance boundary and reference conformance tests are required future paths below. Until that guard and its canonical state producer are present, `publish` returns `PublicationUnavailable`, even for an otherwise healthy archive. Updating canonical guard state and consumption serialize through the same authority; recording does not import market-data or pretend a candidate/fence contains current reducer state. This proposal does not implement the later application wiring or a market publisher.

`owner.publish(turn, candidate, fence, consumer)` asks that registered guard and validates authority, effective recording latch, **no StorageStopped/terminal hard-stop**, Open lifecycle, current nonrevoked candidate and all accepted DataHealth/StorageFence predicates **at the actual consumption boundary**. Every preserved terminal supervisor/storage/order hard-stop atomically latches archive failure too; it cannot leave an Open healthy owner using an old fence. The consumer performs the synchronous effect while that serialized boundary is held; it cannot turn the return value into a deferred permit. No raw reusable Permit is exported. A success returns an observation receipt, not permission for another use. Failure revokes every waiting candidate and invalidates old publication/finalization leases/nonces; mandatory Close ownership remains valid under section3.2. A delayed fence after failure returns `ArchiveFailed`, regardless of its achieved prefix or Healthy observations. Already completed publication before the failure is historical and cannot be withdrawn; no future publication from that archive succeeds. This specifies a pure commit contract, not an external publisher or exactly-once send implementation.

**R3 — repeatable borrowed quiescence.** `begin_finalization(turn)` atomically closes admission for every registered supervisor and creates exactly one opaque non-clonable CloseTicket tied to the same authority/registry/Closing generation. It fails immediately when latched or StorageStopped/terminally stopped. Calling begin again while Closing returns AlreadyClosing, not a replacement ticket/generation. During Closing received ingress returns SessionClosing. No error reopens admission.

`supervisor.quiesce(&mut SessionTurn, &CloseTicket) -> QuiescenceReport` uses its moved private handle and **borrows** the ticket. The authority retains fixed-size ticket/proof state inside K_archive; borrowing does not duplicate issuance rights. Validate turn, ticket authority/identity and registered supervisor before mutating rightful state. Required outcomes:

| Boundary | QuiescenceReport / ticket effect |
|---|---|
| Active ticket; admitted/in-flight/pending/command/candidate/marker ownership unsettled | NotReady(UnsettledSummary); same ticket remains Active, unconsumed/unreplaced, with identical Closing generation/admission state. |
| Active ticket; all ownership settled; healthy Closing authority | Atomically mark ticket Consumed and issue Ready(one opaque affine QuiescenceProof). No second proof can be minted. |
| Consumed ticket; no terminal failure | TicketConsumed; no mutation/new proof. |
| Wrong turn/foreign ticket or owner identity | AuthorityMismatch/InvalidTicket; rightful ticket, proof state, ledger and admission unchanged. |
| Archive failure/storage stop before proof | FinalizationInvalidated(ArchiveFailed); Active ticket becomes Invalidated, terminal outcome rather than NotReady. |
| Archive failure after proof issuance | FinalizationInvalidated(ArchiveFailed); proof authorization invalidated by latch, ticket remains consumed and cannot reissue. This terminal failure takes precedence over duplicate-ticket reporting. |

`UnsettledSummary` has fixed category counts, marker state and at most N Close-owner views, not an accumulating history/list or a hidden drain. It identifies queued/in-flight observations, uncommitted generated plans, pending/leased Close, other command/result leases and candidates still owned. The caller can drain observations, settle/drop permitted ownership, discover/reclaim/dispatch mandatory Close through section3.2, then repeat quiesce with **the same borrowed ticket**. A transferred W owner cannot disappear merely because its Close lease was dropped; an undrainable owner yields explicit failure once storage stops, never a fabricated Ready or endless NotReady. No in-flight work is excluded.

Successful issuance consumes the ticket's issuance right, not the caller's ticket object; later checks observe Consumed. Foreign errors and repeated NotReady do not consume it. Proof is non-clonable/one-use: `owner.finalize(&mut SessionTurn, &mut QuiescenceProof) -> Result<FinalizedArchive, OwnerError>` borrows the same affine proof and consumes its authorization only after owner/turn/proof identity validation, before seal effects; foreign rejection preserves that exact caller-held proof; internal duplicate/stale proof use returns ProofConsumed/InvalidProof without another seal. A terminal failure invalidates every active ticket and issued proof for this authority without changing its generation; failure while Closing transitions to DiagnosticClosing with admission still closed. Proof/ticket errors never create a new lifecycle generation or automatic reset.

`finalize(turn, &mut proof)` validates same authority before any rightful mutation, unconsumed/noninvalidated proof authorization, closed admission, exact registry/generation, empty ownership ledger and no failure, then consumes that proof once and constructs/validates the accepted seal bodies. It rechecks the irreversible latch immediately before **final SegmentSeal**, **ArchiveSeal**, and successful `finish`/sync result. Those operations use the same serialized authority; there is no old finalization token usable after failure or another finalization. Any failure denies successful FinalizedArchive. A capture-failed session cannot append final SegmentSeal or ArchiveSeal at all, even with quality Unknown; only explicitly permitted diagnostic prefix writes/flushes may continue. Physical bytes already written on a failed storage call cannot be retracted or relabeled by this contract.

The runtime latch dominates parsed recording observations. Current reference code in `crates/domain/tests/support/health/transitions.rs` assigns `recording.health = evidence.health` for each observation; that is **not** an irreversible latch. After approval add a separate terminal-session-failure state and derive effective recording health/publication revocation from it while preserving truthful observed health/watermarks. The proposed canonical replay classification is exact: **every valid RecordingEvidence with health Failed latches terminal archive recording failure, regardless of reason**. This includes `Failed/QueueOverflow` and the counter-boundary `Failed/Unknown`; no extra wire discriminator is invented. Later Healthy can remain a truthful observed storage-health value but cannot restore effective health/publication. Nonterminal Degraded/Unknown observations retain their ordinary accepted behavior. This is an explicit replay-health semantic delta, not a claim that the existing reference model already enforces it. The marker still cannot reconstruct the missing raw or exact runtime failure identity; absence of a marker proves no pre-crash health/completeness fact.

## 7. Recovery limits

No ArchiveSeal at an ordinary valid EOF yields existing `WalReader` **ValidPrefixIncomplete**; an EOF after a non-final SegmentSeal yields **SegmentSealedArchiveIncomplete**. Neither promises a quality label: the normal unsealed prefix has `input_quality == None`. An unresolved local loss window can independently produce its existing Unknown diagnostic; it is not justification to report `Some(Unknown)` for every unsealed prefix. Reader physical status/quality APIs remain unchanged.

The live owner separately reports **unknown input completeness** for a failed capture session and forbids canonical publication/finalization. A runtime flag cannot rename physically valid fully sealed bytes from Complete to Incomplete. If a bypass/legacy writer already produced such bytes, report the actual reader result and explicit owner-contract violation; do not edit the interpretation of seals or invent an on-disk marker. Durable-finalization success remains separate from parsed physical completion.

Crash before marker persistence loses the volatile exact failure identity/latch. Recovery cannot recover a nonexistent observation, infer its candidate/stamp, or prove that the missing input never arrived. Because this owner prohibits final seals after failure, its surviving unsealed bytes remain conservative incomplete diagnostic prefix. Crash after a fully valid marker permits replay to reconstruct archive failure, not exact missing input. A marker torn/failed write follows the existing truncated/corrupt/invalid prefix behavior, with no skip/rejoin or repair. Restart creates a new explicitly owned archive/session, never continues the old missing-attempt stream.

An **external inventory** is required to know expected archives/sessions and detect an entirely absent archive; it can also record open/failed/diagnostic-closed lifecycle and expected segment set if separately persisted truthfully. This proposal does not implement an inventory, promise its atomicity/durability, or let it fabricate WAL observations. Without a persisted marker/inventory entry, the precise pre-crash cause remains unknown. Even inventory presence cannot recover unrecorded raw bytes, an original ReceiveStamp or historical sync/send success.

## 8. Compatibility and minimum future implementation paths

The exact paths below are **approved for bounded implementation** by the Architecture decision for `cff1e398…`; only these paths and this ADR/handoff may change. There is no wildcard expansion. Existing accepted specs/ADR, manifests, dependencies and application composition remain unchanged; this ADR carries the approved semantic extension.

| Area | Existing contract preserved / exact proposed delta | Owner/replay/publication/finalization effect | Minimum proposed implementation paths |
|---|---|---|---|
| WAL schema/codec | Frame/schema1, record/control tags, Context, Raw/GAP bodies, dense RecordNo, attempt accounting and seal bodies unchanged. Use existing RecordingEvidence; no terminal identity wire field. | Marker records archive failure only; missing attempt stays missing, no resume on that stream. | No codec/schema change; recording owner tests exercise existing encoding/validation. |
| Shared authority | New opaque session identity/latch, bounded registry/ownership ledger, immutable CutSide, unique SessionTurn, CloseOwnerRef/affine lease state and borrowed CloseTicket/one-use proof, including sealed current-state guard and canonical-step capability boundary. | Equal IDs cannot substitute for authority; failure invalidates publication/finalization capabilities; NotReady/foreign errors preserve rightful ticket; mandatory Close can be discovered/reclaimed after stop/closure without nonce growth. | New `crates/domain/src/capture_session.rs`; `crates/domain/src/lib.rs`; new `crates/domain/tests/capture_session.rs`. |
| Supervisor | `new` needs bound handle; received calls return envelopes; borrowed text input; sink binding; terminal scope/archive status; counted work/byte reporting; same-cut-side coalescing; borrowed quiesce returns bounded report; saturation policy version2. | Received overflow terminates scope; neighbor diagnostics/drain survive; pre-cut GAP cannot absorb post-cut loss; CaptureAttempt never reused; repeated NotReady does not consume ticket. | `crates/market-data/src/ws_supervisor.rs`, `crates/market-data/src/lib.rs`, `crates/market-data/tests/ws_supervisor.rs` (including module-private boundary tests). |
| Writer owner | New fresh-writer ownership/bootstrap profile (one segment, path <=4096 bytes, frozen definitions), bound sink, outstanding_close_owners/reclaim_close/confirm_closed, pollable diagnostic close and canonical finalization. Enforcement inside bound writer methods, not getter convention. | No final seals/finish after failure; truthful fixed-cut marker/reporting; no writer export/adoption bypass; one Close lease per same owner; failure during Closing preserves closed admission; generic multi-segment WAL remains outside this profile. | New `crates/recording/src/capture_session.rs`; `crates/recording/src/lib.rs`, `crates/recording/src/file.rs`; new `crates/recording/tests/capture_session.rs` plus targeted `crates/recording/tests/wal.rs`. |
| Recording health/replay guard | Add irreversible terminal session state: any valid health Failed is terminal, independent of reason; preserve separately observed health and frontiers and ordinary nonterminal Degraded/Unknown behavior. No claim current Healthy assignment already latches. | Failed saturation/counter/other recording observation dominates later Healthy; waiting candidates/fences never restore publication. | `crates/domain/tests/support/health/mod.rs`, `crates/domain/tests/support/health/transitions.rs`, `crates/domain/tests/support/publication.rs`, `crates/domain/tests/cases/health.rs`, `crates/domain/tests/cases/publication.rs`; new domain authority consumed by canonical guard. |
| Recovery | Existing reader physical status, quality Option, CRC/reference/order/accounting behavior preserved. | ValidPrefixIncomplete normally has None quality. Owner separately reports Unknown completeness. Marker supplies no missing identity. | Existing `crates/recording/src/recovery.rs` need not change; tests in recording paths above verify current behavior. |
| Counter/H1/H2/Q2 | Checked counters, no wrap/reuse, error provenance and H1 durable-result boundary preserved. **CaptureAttempt exhaustion changes from global Halted to scoped terminal + diagnostic drain**; completion now waits for its mandatory Close to settle within one W owner. Other storage/RecordNo/epoch/timer hard stops remain explicit. Q2 owner/cancellation rules retained. | H2 no-reuse/error/frontier checks remain; former all-methods-Halted assertions and H1 driver-dispatch scheduling assertions require approved updates, not weakened durability/no-loss checks. | Supervisor/tests above; no unrelated decoder, reducer, domain event or application change. |
| Archive owner responsibilities | Explicit new-session action, exclusive writer, bounded profile, immutable cut, idempotent/ambiguous-effect Close dispatch adapter contract, reclaim/settlement and repeatable borrowed quiescence, current disposition, inventory and unknown-completeness reporting. | Canonical APIs enforce publication/seals; physical path/getter is insufficient; no exactly-once physical Close claim or hidden owner-driven drain. | Recording/domain API paths above only. Wiring actual applications, publisher, network adapter or inventory is deferred, not REC-001F work here. |

Implementation must establish these library boundaries before any later application composition may claim the policy enforced. It must not describe an unbound generic sink/writer path as canonical. No new permission for networking or market semantics follows from adding pure owner types.

## 9. Acceptance matrix for the approved implementation

At the approved docs-only SHA all 31 rows were **REQUIRED / NOT_RUN**. The matrix remains the normative implementation/independent-QA contract. Current worker execution evidence is mapped in §11 and the handoff; full independent QA of the new immutable implementation SHA is still **REQUIRED / NOT_RUN**. Historical forensic tests and docs-only CI do not establish these new behaviors.

| Case | Stimulus / boundary | Required observable result |
|---|---|---|
| Single-stream cap5 | N=1, M=5, empty setup c=2; distinct stamped old diagnostics 3,4,5 then 6 and repeated current input. | W=3, reservations=2, total5; first failure exact candidate6/tag/stamp; consumed frontier6, accounted prefix unchanged until drain; no attempt7/suffix; admitted diagnostics FIFO; marker after cut. |
| Two-stream cap9 | N=2, M=9; six distinct A diagnostics, first A failure, repeated A, then unrepresentable B current. Also drain one before B variant. | Total never exceeds9; F_A/F_B independently representable, one marker; no candidate reuse; B variant remains active DiagnosticOnly with truthful Raw/GAP and transport service. |
| Mixed/non-coalescing saturation | Raw/control/stale/current barriers across streams/tags with exact attempts and original stamps. | No eviction/identity replacement/cross-barrier merge; first incompatible received input terminal; pending/in-flight/leases count toward M; reports/capacities remain exact. |
| Ordinary F2 | Raw attempt1 followed by contiguous same-tail/same-cut-side rejected attempts2..=33; available neighbor work. | Exact known GAP 2..=33/count32; no new items per permitted coalesce; normal tail rules/original observation stamp preserved; B can service admissible commands/raw. |
| Repeated failed-scope ingress/reconnect | Old/current raw, exact Pong, Connected/Disconnected for current/new epoch, tick/start and a stale command lease. | Same first failure identity; no allocation/frontier/stamp/history growth, revival, attempt reuse or unsafe command; one idempotent Close for live epoch; tick skips scope. |
| Admitted FIFO/drain and neighbor commands | Failure with queued Raw/GAP/Timer/Connected/Pong/Disconnected and pending H1 transition; healthy B Subscribe/Ping/reconnect; hold mandatory Close through storage/closure error. | Gate-confirmed original prefix remains drainable DiagnosticOnly; terminal-control suppression retained; failed-scope reconnect plans canceled explicitly; B commands serviceable; result disposition visible before use; Down/Close never erased or denied by later completion/storage/closure error; completion waits for Close settlement without extra W. |
| Marker persist error | Inject error before/during marker, or earlier admitted-prefix persist failure. | Latch already Failed; exact typed error; marker unconfirmed/not reached; storage stops; earlier receipts retained; no durable promise/replacement/rollback/claim that failed bytes are absent. |
| Marker mismatch / weak gate | Wrong receipt RecordNo; required Durable with Flushed receipt; wrong authority/session sink. | Exact mismatch/gate/binding rejection; no marker-confirmed outcome or dependent effect; permanent failure; byte/frontier ambiguity reported honestly. |
| Marker order / watermark | Pre-cut timer/timeout, pending neighbor completion blocked by post-cut same-generation raw, held Close lease, prior known stronger watermark and later Healthy observation. | All pre-cut record-bearing ingress then marker then post-cut ingress; pending generated completion remains counted and can follow its post-cut blockers without deadlock; strictly earlier truthful nonregressing through; later Healthy/Pong/fence never clears effective Failed. |
| Publication before/after failure | Absent/foreign registered guard, changing canonical state, valid pre-failure candidate/fence; retained result/candidate/old lease; failure then delayed stronger fence or Healthy receipt. | Missing/foreign guard fails closed; guard checks current accepted state at use; completed earlier publication stays historical; every later use denied at owner commit, including old valid permit/candidate; all neighbors blocked for market publication. |
| Finalization boundary | Foreign/old proof/turn, unsettled ledger, generic bound append of seals, compile-fail ingress reentry from commit callback, terminal hard-stop with old fence, healthy close and pollable failed diagnostic close. | Unique borrowed turn prevents canonical reentry; same-authority quiescence required; admission closed atomically; all publication/final-seal/finish checks enforce latch and hard-stop; failed session produces no final SegmentSeal/ArchiveSeal/FinalizedArchive; wrong-lifecycle diagnostic close rejected; writable close waits for caller drain, storage-stopped close reports undrained ownership. Healthy accepted finalization still works. |
| Crash before marker | Crash after failure before marker write; no external persisted failure; absent whole archive inventory variant. | Valid unsealed prefix reports ValidPrefixIncomplete and normally quality None; exact failure unrecoverable; owner/inventory completeness Unknown; absent archive detectable only with external inventory; no resumed suffix. |
| Crash after marker / torn marker | Valid marker before EOF; torn/failed marker write; old non-final segment seal; legacy fully sealed bytes variant. | Replay valid marker latches Failed; no reconstructed missing attempt; torn record uses existing prefix diagnostics; segment-only incomplete status; physical Complete stays Complete when bytes really satisfy seals, with explicit owner violation rather than runtime relabeling. |
| CaptureAttempt boundary | Set consumed frontier MAX-1, admit MAX where valid; next received raw at MAX, current and old-tag variants. | Original CounterExhausted; frontier MAX unchanged; exact NoRepresentableSuccessor evidence, no wrap/MAX+1/GAP/raw fabrication; scoped terminal, admitted diagnostic drain and healthy-neighbor service; no reuse. |
| Retention bytes/owners | Cap5/9 at every transfer; very large caller Vec capacity with small slice; decoder/encoder clone, pending plan, command/candidate lease, cut labels, Close refs/Cell and ticket/proof state, errors/repeats. | Retention report covers all owned units/reservations/backing capacity and scratch/buffers; item and byte ceilings hold; reclaim creates no item; failed payload released; no hidden side lane or unbounded error/definition/tombstone history. |
| Q2 / H1 / H2 / regressions | All Q2 D-1/D/D+1 FIFO Pong/timeout/canceled-owner cases, checked near-MAX stale timers, H1 success/fault prefixes, H2 counter boundaries; prior F1-F4/N1-N3/decoder/continuity/WAL regressions. | Q2 remains fixed; obsolete timer has no operational preflight/effect; H1 reports successful Down/Close before completion failure; H2 checked exhaustion/error/no reuse retained with explicitly approved scoped-disposition assertion updates; no accepted wire/accounting/unknown-semantic regression. Full independent QA on new implementation SHA. |
| R1 — pre-cut tail GAP, free W | N=2/M=9; B raw1 persisted, five other owners plus tail GAP B 2..3/count2, W=6; A first failure/cut; release one earlier owner, then contiguous B loss4 before marker. | Old GAP bytes/stamp/range/count identical; W5->6 with separate PostCut GAP4..4/count1 after marker; total<=9; consumed B4, accounted B1->3->4 only as receipts confirm; no physical-tail coalescing across cut. |
| R1 — same tail, W exhausted | Same trace but no owner released before B loss4. | W=6 unchanged; B terminal in its own reserved slot with exact candidate4/tag/stamp/cause; old GAP unchanged; no new GAP or reuse; accounted frontier reaches3 only, total9. |
| R1 — marker failure preserves cut | Either R1 variant, then marker persist error/mismatch/weak gate and repeated neighbor ingress. | Same fixed PreCut/PostCut ownership and frozen old GAP; storage stops explicitly; no reopened coalescing, relocation before marker, new owner/attempt or late success claim. |
| R2 — Drop then reclaim | Discover a Pending Down or inline owner, reclaim one lease, Drop, reclaim again for both storage associations. | Same opaque owner/epoch/command identity: Pending->Leased->Pending->Leased; W/reservations identical at every step; exactly one active lease, no new terminal slot. |
| R2 — dispatch error then reclaim | Close callback returns error after possible physical effect, reclaim same owner and retry. | DispatchFailed(effect Unknown), owner Pending; same idempotent epoch-bound Close can be leased again; unchanged counts/identity; no exactly-once/absence-of-effect promise; retry cannot close a newer connection. |
| R2 — double reclaim | Reclaim already Leased owner while retaining first lease. | AlreadyLeased without mutation/new lease/allocation; first lease remains legitimate; ledger counts unchanged. |
| R2 — foreign authority/owner | Foreign turn/ref or invalid/recycled-slot association targets a legitimate Pending/Leased owner; dispatch the reclaimed/converted lease through a foreign owner. | AuthorityMismatch/InvalidOwner/OwnerRetired; rightful owner/state/active lease unchanged. Foreign dispatch denies before effect and returns that same valid lease; later Drop returns same owner Pending. No counter/epoch mutation or new owner. |
| R2 — storage stop/closing/diagnostic closure | Hold/drop Close through StorageStopped, Closing, DiagnosticClosing and DiagnosticClosed, then discover/reclaim/dispatch it. | Bounded discovery remains available; same Pending owner reclaimable and dispatchable; descriptor closure cannot cancel it; discovery/reclaim/Drop/error leave counts identical; no writer append/resumption. |
| R2 — settled/stale owner | Successful dispatch or authenticated exact-epoch closure; reclaim settled ref, Drop old disarmed lease, try stale ref after W-slot reuse/new epoch. | Settled/AlreadySettled or OwnerRetired, no new Close/effect; never revert Settled or touch newer owner. Close settlement itself changes no ledger count; whole transferred owner releases only when all other ownership settles. |
| R2 — reuse prior Down owner | Existing Down owner has Pending or Leased Close when scope first fails. | Terminal slot references that W owner; no second inline owner or active lease; report/reclaim uses same identity/counts; mandatory Close survives cancellation of only the uncommitted reconnect plan. |
| R3 — repeated NotReady | Begin healthy Closing; keep admitted/pending/leased ownership; quiesce repeatedly with same borrowed ticket. | Bounded stable unsettled description; ticket Active/unconsumed/unreplaced, same generation and closed admission; no hidden drain/lease settlement/ledger mutation. |
| R3 — settlement and sole proof | Drain received work, reclaim/dispatch Close, settle all other owners, retry same ticket. | Exactly one Ready(proof), atomic ticket Active->Consumed; proof non-clonable; no fabricated ready with outstanding W/Close/marker. |
| R3 — duplicate issuance / foreign ticket | Quiesce consumed ticket again; separately present foreign ticket/turn while rightful ticket remains Active or Consumed. | Healthy consumed ticket returns TicketConsumed, no second proof; foreign error leaves rightful state/generation/ledger/admission untouched. |
| R3 — failure during Closing | Gate/order/counter failure before Ready, and separately after Ready; repeat quiesce/finalize. | Terminal FinalizationInvalidated(ArchiveFailed), not NotReady loop; Closing->DiagnosticClosing with admission still closed; active ticket/issued proof invalidated, consumed issuance stays spent; no new generation or successful final seals. |
| R3 — proof reuse | Finalize with first valid proof, then internal stale/duplicate authorization use; safe-Rust attempt to clone/reuse moved proof. | One-use consumption; ProofConsumed/InvalidProof/no duplicate seals, compile-fail affine reuse; failed/invalidated proof never finalizes; errors do not reopen admission. |

## 10. Approval provenance and next gate

Architecture approved the entire immutable proposal `cff1e398c3226bc2a86b51442e02054c5996e86a` with **DESIGN_APPROVED_FOR_IMPLEMENTATION**. R1/R2/R3 were **DESIGN_RETEST_PASS** and whole-ADR consistency **DESIGN_REVIEW_PASS**. The approval explicitly restricts implementation to the exact §8 allowlist plus ADR/handoff, preserves the dependency graph and every U/C constraint, and requires all applicable acceptance rows and earlier regressions. Proposal CI `37601404244` passed on that docs-only SHA; it is historical design provenance and does not verify the implementation.

This implementation continues the original claim/recovery, branch and Draft PR #34. No new claim, Issue, branch or PR, force-push, merge or auto-merge is authorized. The new implementation head and fresh exact-head CI belong in existing PR/Issue metadata after commit; the document cannot embed its own containing SHA without changing that SHA.

The next gate is **full independent QA of the new immutable implementation head**, including all 31 acceptance rows, Q1/Q2, all 63 H1 faults, H2 and prior F1–F4/N1–N3/decoder/continuity/WAL regressions. Worker tests, allocation measurements and CI are evidence for that review; they do not constitute independent QA, Integrator acceptance, owner readiness or merge approval. Later code changes require a new exact-head verification and invalidate transferred implementation-QA evidence.

## 11. Initial implementation mapping and historical worker evidence

This mapping was introduced with `ee4e85e5b9c74c8699628e39ea13281a2d91e611`, whose worker disposition was **IMPLEMENTED / QA_PENDING** before independent QA required B1/B2/B3 changes. That head is rejected. The retained regression mapping below is supplemented by §12's corrections and the appended remediation handoff; it is not acceptance of the historical head. Full independent QA of the new containing remediation SHA remains **REQUIRED / NOT_RUN**.

### 11.1 Concrete library boundary

| Boundary | Implemented Rust API / behavior |
|---|---|
| Fresh filesystem owner | `CaptureSessionOwner::create_new(path, &RecordFrame, BoundedCaptureProfile<'_>) -> Result<(Self, SessionTurn), OwnerError>` accepts ArchiveStart plus borrowed frozen bootstrap (at most64 records/65536 encoded bytes). Path is UTF-8 and at most4096 bytes. The exclusive single-segment writer has no export/adoption API. |
| Registration | `owner.register_supervisor(&mut SessionTurn, &[ScopeBinding], RetentionBudget)` returns the non-clonable supervisor handle and bound sink. Accepted full `StreamBinding` values, prefix, gate and registry are checked, including full tag/profile rather than equal numeric scope IDs. `PublicWsSupervisor::new(config, handle)` cannot substitute its own prefix/context. |
| Ingress | `queue_text` borrows `&[u8]`; other received methods preserve connection/epoch/stamp. `AdmissionReport` puts session/scope disposition and failure evidence outside the typed outcome, with fixed arrays for at most4 command leases/admitted scopes and explicit plan cancellation. `AlreadyTerminated` has the immutable failure in the outer fields. |
| Drain and ownership | `drain_one(turn, &mut BoundRecordSink) -> DrainReport`; `DrainResult` retains its private W owner while the caller holds it. Results have at most3 RecordNos,1 command and4 events. Queue, in-flight result, pending Down plan and leases transfer/share the same counted unit. There is no public legacy core/free `RecordSink` boundary. |
| Ordinary versus marker writes | `BoundRecordSink::persist_owned(turn, frame, gate, &WorkOwner)` checks authority, current prefix, counted ownership and cut ordering; `persist_marker` uses the reserved archive slot. Compatibility `persist` accepts only the failure marker. Gated confirmation is authenticated through the bound sink, not a caller-supplied RecordNo/receipt DTO. A valid direct archive-wide Failed observation fixes the same authoritative cut and retains an immutable bounded original context/reason/kind descriptor even when it must wait for PreCut drain. Confirmed direct writes advance the authenticated prefix without duplicate marker generation. |
| Mandatory Close | Fixed `outstanding_close_owners`; `reclaim_close(turn, CloseOwnerRef) -> CloseLeaseReport`; consuming `CloseLease::into_command`; `dispatch(turn, CommandLease, effect) -> DispatchReport`; `confirm_closed` accepts an opaque `AuthenticatedClosure`. Drop/error returns the same owner Pending; foreign dispatch returns the legitimate lease. A W job is bound to one Close scope, including held old aliases; cross-scope reuse is rejected before mutation. Lease sharing is checked, never wrapped/panicked into a second active lease. |
| H1 / epochs | Successful Down and Close return before fallible completion. Completion waits for Close settlement, then three gated epoch records authenticate advancement. Terminal failure cancels only uncommitted reconnect completion. The earlier Down and Close owner survive storage error/diagnostic closure. |
| Finalization | `begin_finalization(turn)` creates the sole `CloseTicket`; `supervisor.quiesce(turn, &ticket)` performs no implicit drain. NotReady preserves it; Ready atomically consumes issuance and yields one affine proof. `finalize(turn, &mut proof)` preserves the proof on foreign rejection and consumes authorization before I/O, checks latch before each final seal/finish, and never succeeds for a failed session. Bound generic seal append cannot reuse owner authorization. |
| Diagnostic closure | `close_diagnostic(turn) -> DiagnosticCloseReport` is pollable. Writable DiagnosticClosing waits for caller-driven admitted drain/marker; StorageStopped can close descriptors while explicitly reporting undrained ownership. Close discovery/reclaim/dispatch remains available afterward. No hidden Drop flush/retry/seal or storage restart. |
| Publication / trusted interfaces | The pure domain authority and `SessionRecordWriter` are a trusted backend/conformance contract: an implementation must report its actual accepted prefix/gate. They do not authenticate arbitrary implementations or another filesystem owner's authority. Canonical filesystem capture uses only the concrete owner-minted handle/sink. Sealed guard, canonical step/candidate/fence and external closure evidence have no public DTO constructors. **Production publication stays PublicationUnavailable** until both its sealed guard and authenticated canonical producer exist; application/transport producers are deferred. |
| Replay / recovery | Reference health/publication interpreters latch every valid Failed recording observation while retaining later observed health separately. WAL schema/codec/reader remain unchanged. Marker/crash fixtures preserve physical status and quality Option; absent archives require external inventory, which is not implemented here. |

Supervisor entry points synchronize authoritative CutSide, marker/prefix and scope termination rather than assuming all failure originated in their own ingress call. PreCut GAP remains frozen when a valid archive-wide failure was initiated directly through the concrete bound sink; a later scope failure cannot replace the earlier archive marker descriptor.

Historical Connected/Pong admitted before any terminal Down can record gated diagnostic Up after failure without reviving live scheduling/subscription. The original same-generation terminal-Down suppression remains. Storage errors retain the original undrainable ingress/work identity or pending completion; they do not fabricate replacement records, promise rollback, or silently resume writes.

### 11.2 Honest item, metadata and allocation evidence

`OwnershipReport` exposes M, N+1 reservations, W used/free via the fixed limit, total references and PreCut/PostCut/before-failure counts. `SessionStatus` and `unsettled_summary` expose marker, lifecycle, failure, ticket/proof and Close state; `SupervisorRetentionReport` adds queued/pending work, exact raw Vec capacities, cut state and storage profile. The public reports distinguish known backing from modeled inline owner capacity and conservative metadata/workspace ceilings. Private BTree nodes are included in a structural ceiling for the pinned toolchain, not described as measured allocation. Decoder bounds include aggregate nested JSON/container/string growth and output staging; frozen validator cloning/encoding and path/backend buffering are explicit.

The total ceiling is the §4 formula, realized as authority metadata ceiling + storage metadata ceiling + supervisor metadata ceiling + payload ceiling + decoder workspace ceiling + storage workspace/backend ceilings. W+N+1 remains within M at transfers/reclaim/Drop/error/repeats. Inline terminal Close and marker stay inside their reservations; no second owner lane is added. Cross-scope W/Close misuse and checked admission/RecordNo/CaptureAttempt boundaries have negative regressions. A received input rejected by exhausted AdmissionOrder keeps the original typed counter error and installs exact reserved scope failure evidence; generated work keeps its explicit checked outcome. Eligible F2 tail coalescing reserves neither a new W nor a new admission identity. The domain test sets the actual private counter to MAX; the supervisor test injects only its negative reservation outcome against a genuine accepted filesystem owner and then exercises public diagnostic drain/reclaim/neighbor paths.

Std-only test allocator probes count requested allocation Layout sizes (not RSS, allocator usable size, or end-to-end network memory). They cover the full concrete owner/sink/supervisor lifecycle at N1/M5 and N2/M9, dense/nested legal decode, held result/command leases, tiny borrowed slices from an8MiB-capacity caller Vec and100 failed-ingress/reclaim cycles. Observed values and final command results are recorded in the handoff. They assert peak below the computed complete boundary ceiling, flat repeated-call retention and zero tracked live bytes after all boundary objects are dropped. The separate owner-only probe exercises the same cap/terminal/reclaim transfer discipline.

### 11.3 Mapping all31 acceptance families to worker regressions

MD means `crates/market-data/tests/ws_supervisor.rs`; MU its module-private source tests. DO/DU mean domain capture-session integration/conformance tests; RO means recording capture-session integration tests. Names below are unique test prefixes when abbreviated. This mapping records worker coverage, **not independent QA acceptance** or an end-to-end application/socket/inventory test.

| §9 row | Executed worker regression family |
|---|---|
| 1 Single-stream cap5 | MD `q1_single_stream_legal_five_…`: exact candidate6, immutable failure/repeats, FIFO prefix, marker, no suffix. |
| 2 Two-stream cap9 | MD `q1_two_stream_legal_nine_…`: both reserved failures and drain/free-W neighbor variant. |
| 3 Mixed saturation | MD `q1_noncoalescing_saturation_…`: admitted raw/control/stale identity and exact scopes. |
| 4 Ordinary F2 | MD `sustained_overflow_coalesces_loss_…`: Raw1/GAP2..33/count32 and neighbor service. |
| 5 Repeated ingress/reconnect | MD cap5/cap9 repeats plus `held_pre_failure_connect_lease_…`: counted old command revoked before callback effect. |
| 6 FIFO / neighbor controls | MD `admitted_connected_and_pong_without_prior_down_…`, `terminal_…`, `marker_precedes_post_cut_…` and H1 neighbor cases. |
| 7 Marker error | MD `canonical_terminal_close_reclaim_…` and RO `marker_error_mismatch_and_weak_gate_…`; prefix errors in H1/Q2 suites. |
| 8 Mismatch / weak / authority | Same three marker fault modes; DU authenticated marker confirmation, RO full binding and foreign owner negatives. |
| 9 Marker ordering / watermark | MD `marker_precedes_post_cut_neighbor_raw_and_unadmitted_completion_with_held_close`; MU direct archive-wide marker/cut/prefix regressions; DU immutable archive descriptor and `earlier_stronger_watermark_…`; RO valid Failed/later Healthy. |
| 10 Publication | DU full current projection, quiet expiry/mode, authenticated continuous fence, one-use publication and late-fence failure revocation; reference health/publication cases. Actual application producer remains unavailable. |
| 11 Finalization boundary | MD borrowed quiescence/Closing storage fault; RO healthy finalization, generic seal rejection and diagnostic-close polling; affine/reentry compile-fail tests. |
| 12 Crash before marker | RO `unsealed_prefix_before_marker_…`: no fabricated observation/quality. No external inventory producer exists; absent-whole-archive detection remains its explicitly documented responsibility. |
| 13 After / torn / legacy seals | RO `valid_marker_retains_…`, `torn_failure_marker_…`, `old_nonfinal_segment_seal_…`, `physically_valid_legacy_sealed_bytes_…`. |
| 14 CaptureAttempt MAX | MU `canonical_capture_attempt_exhaustion_…` current/old-tag variants plus original checked/near-MAX regressions; received AdmissionOrder exhaustion and zero-reservation F2 regressions. |
| 15 Retention bytes / owners | MD full-supervisor allocator/tiny-slice tests; RO owner allocator; DO bounded sharing/ledger; DU cross-scope Close rejection. |
| 16 Q2 / H1 / H2 / prior | All supervisor regressions,63 H1 fault variants,12 Q2 storage-fault variants, checked H2 tests and complete existing workspace/real-CLI/WAL suites. |
| 17 R1 free W | MD `r1_pre_cut_tail_gap_…` free-W branch: old GAP frozen, separate counted PostCut GAP4, marker between owners. |
| 18 R1 exhausted W | Same test exhausted branch: exact B candidate4 failure, no GAP extension/reuse, W6/total9. |
| 19 R1 marker failure | MD `r1_marker_fault_…`: both W variants times error/mismatch/weak gate; immutable cut/old GAP, explicit stopped retained work. |
| 20 R2 Drop / reclaim | MD canonical inline Close; DO `mandatory_close_drop_error_…`; RO `down_close_drop_error_…`: same owner and counts. |
| 21 R2 dispatch error | Same families: effect Unknown, Pending retry, no exactly-once/absence-of-effect claim. |
| 22 R2 double reclaim | Same families: AlreadyLeased, same snapshot and no second lease. |
| 23 R2 foreign | DO foreign reclaim/dispatch; RO `foreign_dispatch_returns_…`; MD canonical fault suite; DU foreign/stale opaque closure. |
| 24 R2 lifecycle | MD all marker faults through DiagnosticClosed; DO storage-stop/descriptor closure; RO `closing_keeps_down_close_…`. |
| 25 R2 settled / stale | Same Close suites plus DU foreign/stale closure and canonical epoch scope tests; old lease alias retains its counted W until Drop. |
| 26 R2 prior Down reuse | RO `down_close_drop_error_…`; MD held Close/deferred completion ordering; no extra terminal Close or W. |
| 27 R3 repeated NotReady | MD canonical borrowed quiescence; DO borrowed-quiescence; RO `repeated_not_ready_…`; stable bounded summary/no drain. |
| 28 R3 settlement / proof | Same families plus RO `healthy_owner_finalizes_…`: same ticket then sole proof. |
| 29 R3 consumed / foreign | MD/DO/RO foreign ticket/turn and duplicate issuance tests; rightful issuance unchanged on error. |
| 30 R3 failure during Closing | MD storage failure before Ready; DO failure before/after Ready; RO failed issued proof; DU failure after proof consumption before seals. |
| 31 R3 proof reuse | DU `foreign_and_duplicate_proof_authorizations_…`; RO healthy single finalization; safe-Rust affine proof/reentry compile-fail regressions. |

Final local Rust results, tool-wrapper limitations, allocation measurements, exact allowed diff and fresh exact-head CI are recorded in the appended handoff and existing PR/Issue metadata. No historical docs-only CI or worker conformance result is transferred into independent-QA acceptance.


## 12. B1/B2/B3 remediation after independent QA

This section describes the corrective boundaries introduced at `dfe2c43efc2df0591d67daef4beac0d97fa3b050`. Subsequent independent QA confirmed the original B1 trigger and B2/B3 under their stated library boundaries, but rejected full acceptance for QA-D1 (rows6/26). Its worker/CI PASS is historical evidence, not closure of that cancellation intersection. Section13 records its restoration.

The attached independent-QA report rejects immutable `ee4e85e5b9c74c8699628e39ea13281a2d91e611`. Its Windows319 PASS/42 platform failures, original CI363 PASS and Flushed supplemental checks are separate historical evidence. The correction continues the same approved `cff1e398…` contract and exact §8 paths; it changes no wire schema, reader status/quality, dependencies, accepted ADR/specs, networking or application composition. The containing remediation SHA and fresh CI belong in the existing PR/Issue metadata, avoiding a self-referential SHA in this document.

### 12.1 Authenticated admitted obligations and bounded abandonment

Each existing W cell retains a fixed `ObservationIdentity` (scope/epoch, original receive stamp, class, optional full tag, attempt range and loss count), an immutable received/generated origin, obligation state, authenticated receipt count and bounded abandonment kind. These bytes are included by `size_of(WorkCell)` in the authority metadata ceiling; no new item/history lane is added. W occupancy is independent of reference count: pending or abandoned obligations occupy W even with zero aliases. `pending_observations`, `abandoned_work`, `UnsettledSummary.abandoned` and `SessionStatus.first_abandonment` report the retained disposition.

Admission and successful completion use the non-clonable `SupervisorSessionHandle`, consumed privately by the canonical supervisor. Generic `WorkOwner::set_kind` changes neither origin nor accounting settlement. Only actual bound-sink gated receipts advance its confirmation count. Successful core completion settles the original received job; stale diagnostic Raw requires its two writes, and generated epoch completion its three new receipts. StorageStopped prevents completion after partial writes. The sole no-write obsolete disposition is Connected/Pong after an authenticated same-epoch Down; it cannot settle Raw/GAP/Timer. Explicit failed-scope/diagnostic-close cancellation applies only to still-unadmitted generated plans, never received work. Healthy settled result/command Drop retains ordinary release/finalization behavior.

`WorkOwner::drop` only decrements bounded alias accounting and flags unsatisfied last-reference abandonment. `PublicWsSupervisor::drop` flags loss of queue/pending-plan stewardship even while caller-held result/Close aliases survive. Both use Cell-only non-panicking housekeeping, no I/O, drain, allocation, received stamp rewriting or cut/lifecycle transition.

`authority.synchronize_obligations(&mut SessionTurn) -> Result<(), AuthorityError>` validates the rightful turn, then selects the lowest admitted work_id among pending abandonment notifications at that first serialized boundary (not wall-clock Drop chronology), freezes that bounded identity, latches archive failure and fixes the first admission-order cut if absent. Existing first terminal failure, archive observation and cut are preserved. The explicit logical stop is `PersistErrorKind::OwnershipAbandoned`, not a claim of filesystem I/O failure. Lost admitted content is undrainable: append stops, marker remains explicitly Unconfirmed if not already confirmed, and diagnostic descriptor closure reports every retained unconfirmed owner. No marker is fabricated or moved across the prefix hole. No Raw/GAP/CaptureAttempt or replacement proof is synthesized.

Read-only disposition/storage/finalization guards reject pending abandonment before reconciliation; canonical admission, sink, dispatch, publication, quiescence, proof consumption and diagnostic closure reconcile under the serialized mutable turn. Foreign turn/ticket/proof/command identity rejection precedes rightful mutation. Closing then reaches terminal `FinalizationInvalidated(ArchiveFailed)`, not endless NotReady; Open cannot begin healthy finalization. Ordinary effects cannot use a pre-abandonment authorization. Mandatory Close remains inside its existing W association or reserved scope slot; an older settled epoch's Close is replaced with the actual current epoch's reserved descriptor, preserving stale-ref rejection. Reclaim/dispatch/settlement after StorageStopped/DiagnosticClosed does not release abandoned W or restore final seals.

### 12.2 Borrowed consumable proof

Concrete signatures are `authority.consume_proof(&mut SessionTurn, &mut QuiescenceProof) -> Result<(), AuthorityError>` and `owner.finalize(&mut SessionTurn, &mut QuiescenceProof) -> Result<FinalizedArchive, OwnerError>`. The proof remains opaque, non-Clone and affine. Foreign owner/turn validation leaves caller value, both owners' states, ledgers, watermarks and WAL bytes unchanged; rightful use retries with that same value. No ticket reset, replacement proof or lifecycle generation exists. Validated consumption is marked before seal I/O and remains spent after storage failure or success. The same value cannot authorize a second finalization. Existing moved-value/Clone/reentry compile-fail coverage is retained; overlapping mutable proof borrows have additional compile-fail coverage.

### 12.3 Backend-authorized truthful memory profile

`StorageMemoryProfile::validate() -> Result<(), AuthorityError>` checks each backing/ceiling pair, all component/aggregate sums and representable headroom before mutation. The generic `set_storage_memory_profile` is limited to standalone pre-bind conformance; after binding it returns `StorageProfileBound`, even with a legitimate turn. First successful `bind_sink_with_memory_authority` returns the bound sink plus one opaque non-Clone `StorageMemoryAuthority`; a canonical recording owner privately retains it. No token is exported through sink/handle/authority getters or minted after binding. Its `update(turn, profile)` permits actual backend-authorized capacity refresh after descriptor/buffer closure. Ordinary `bind_sink` remains compatible with pure trusted-backend conformance.

The bound sink centrally validates `SessionRecordWriter::checked_memory_profile()` even for an overriding backend before write/cut/prefix mutation; actual FileSink uses checked encoder/registry/backend arithmetic. Invalid pre-write budgets leave prior state/profile/prefix unchanged and perform zero writes. An invalid profile revealed after a real write instead stops storage with explicit ambiguous suffix and preserves the last admitted profile; it promises no rollback or absence of bytes.

Storage ceiling sum and complete authority/supervisor/payload/decoder subtotal each must fit at most half the numeric range. Checked products/intermediates and sums are admitted before a supervisor is returned. This makes their aggregate representable; no saturated MAX/zero fictitious ceiling is used. `checked_retention_report() -> Result<SupervisorRetentionReport, SupervisorError>` is available, and the existing infallible report is backed by this admitted invariant. Concrete owner/supervisor actual capacity reports stay consistent after closure. B3 is a reporting-substitution/debug-overflow correction; it claims no independently demonstrated actual heap overflow, unbounded allocation or RSS violation.

### 12.4 Regression and delivery evidence

The four attached inputs were hashed and the ZIP copies matched byte-for-byte. Original reproductions ran only on an isolated exact-source copy with their temporary QA dev dependency. That manifest/lock delta is absent from the production branch. On Linux the four factual probes reproduce the old defects, including successful wrongful FinalizedArchive for B1. Normative B1 and B3 tests compile on the rejected source and fail behaviorally. B2's old factual probe demonstrates lost proof; the revised borrowed-signature test cannot compile against the old API and is not presented as an old-head runtime regression.

Normative regressions use genuine canonical owner/sink boundaries: Raw/GAP/control/timer and epoch2 Drop before/after Closing; held in-flight/pending-plan ownership; PreCut/PostCut retention with first failure/cut/prefix preserved; mandatory Close after abandonment/closure; flat repeats/no fictitious capacity release; healthy authenticated drain/settled Drop. B2 covers all six mismatched genuine A/B combinations and real closed-backend error after validated consumption. B3 covers zero/MAX for all six components, intermediate/aggregate overflow, transactional pre-write rejection, post-write ambiguity, consistent reports and truthful closure. Debug/release and cap5/cap9 allocation/teardown results and the complete original Linux/Durable workspace scope are recorded factually in the handoff.

All31 §9 families remain mandatory, along with63 H1 faults, H2,12 Q2 storage faults, timer/FIFO boundaries, F1–F6/N1–N3, decoder/continuity/DataHealth/publication/WAL and real CLI. The supplemental stale Close after actual epoch/W recycle and repeated Halted/unchanged prefix/snapshot assertions are retained using the original Durable profile. No Flushed adaptation is transferred. Worker PASS or fresh CI is not independent QA, Integrator acceptance, owner readiness or merge approval. Full independent QA of the new immutable containing head is still required; PR #34 remains Draft.

## 13. QA-D1 restoration of generated-plan settlement

Independent QA rejected `dfe2c43efc2df0591d67daef4beac0d97fa3b050` with one HIGH finding: local same-scope ingress failure removed a genuine pending disconnect plan and reported cancellation without settling its generated obligation. After mandatory Close settlement and alias release, that orphan became OwnershipAbandoned and incorrectly halted admitted diagnostic drain, including neighbor Raw. The correction restores approved §3.2/§9 rows6 and26; it adds no public API, wire schema, lifecycle generation or policy permission.

### 13.1 Trigger and checked ordering

The minimal cap5 trigger is gate-confirmed Down at epoch1, its held Close and transferred generated W owner, two admitted Connected barriers (W=3), then same-scope Raw saturation. The cap9 variant uses the same A Down, four A barriers and one admitted B Raw (W=6). The first missing A candidate remains exact terminal diagnostic evidence; it is not Raw/GAP. The pre-cut received FIFO and failure/marker cut remain fixed.

The private `cancel_pending_disconnect(&mut SessionTurn, StreamId) -> Result<bool, SupervisorError>` is the common transition for local failure installation, external terminal synchronization and applicable DiagnosticClosing/Closed cancellation. It validates rightful authority, paired registry presence and the runtime plan epoch against the runtime binding while both remain registered. The original WorkOwner scope/epoch association relies on canonical Down construction; this helper does not independently compare that opaque observation identity to the registry key. It calls `handle.cancel_generated_plan(turn, work)` to settle the wholly uncommitted Generated obligation, then checked `set_kind(Command)`, and only afterward clears the runtime plan/removes the pending owner. No `remove`/`take`/blanket clear or settlement `expect` precedes the fallible checks. No pair means Ok(false); an inconsistent presence/runtime-epoch pair or authority/accounting rejection returns its typed error retaining that same plan/owner/Close. `cancelled_plan=true` is emitted only after Ok(true); a local cancellation error is returned with the installed terminal failure and existing Close still explicit, and false cancellation status.

Cancellation of A does not cancel B's plan, alter queued observations or fabricate epoch records. The original gate-confirmed Down, original stamp, same WorkOwner and CloseOwnerRef persist. Held legitimate Close stays Leased; no second command lease or terminal slot is created. Dropped/error Close remains reclaimable; Settled Close is never reissued. Retained Down-result aliases are already accounted for by the same W. Its generated obligation is settled by the checked transition; capacity is released only when every remaining alias/Close obligation is also settled. Reclassification and Drop never settle received Raw/GAP/control/timer.

### 13.2 Completion, error recovery and bounds

Canonical completion selects only a ready active plan after Close settlement, outside any pre-cut received prefix blocking its marker. Healthy completion still requires three fresh gate-confirmed epoch records and exactly one runtime epoch advance. Before drain, fallible kind validation runs while the original owner remains registered. After a drain error, the same pending owner or original queued observation is restored before any fallible reclassification; confirmed receipts/prefix remain historical and the typed storage stop remains explicit.

StorageStopped owners, including one/two confirmed epoch writes of a failed completion, are not canceled as wholly uncommitted plans. External synchronization preserves those runtime/owner pairs; drain returns the existing Halted/storage-error disposition. Diagnostic descriptor closure reports them undrained and keeps mandatory Close discoverable/reclaimable. Lifecycle transition immediately revokes generated/non-Close effects; the supervisor settles eligible unadmitted plans at its next rightful synchronization/drain boundary, including DiagnosticClosed. The owner does not import or secretly drain the supervisor. Authenticated trusted watermark remains the last good receipt; actual backend bytes/watermarks may include a mismatch/weak-gate fault record written before rejection. There is no rollback, fabricated success, retry of epoch suffix or claim that failed bytes are absent. This is conservative enforcement of the existing partial-write contract, not an additional independently demonstrated blocker.

The helper uses the existing fixed registry and bounded configured-scope snapshot; it adds no owner, history, receipt or allocation lane. W+N+1<=M, first failure/cut, accounted frontiers and byte ceilings retain the existing formulas. A canceled generated obligation shares the original W until legitimate retained aliases release. The cap5/cap9 full-boundary and owner allocation/repeat/teardown probes remain mandatory.

### 13.3 Normative regressions and evidence boundary

The supplied `independent_qa_scope_cut_cancels_generated_down_plan_without_stopping_drain` and `independent_qa_same_scope_plan_cut_preserves_queued_neighbor_raw` compile and fail behaviorally on the rejected source in both debug and release. The corrected tests require complete diagnostic drain through the marker, no false OwnershipAbandoned/Halted or canceled EpochAdvance, and exact neighbor Raw bytes/tag/stamps/accounted frontier. Cancellation preserves the confirmed prefix; subsequent authenticated admitted drain advances it while first failure/cut remain unchanged. Additional coverage exercises held/Drop/dispatch-error/reclaim/Settled Close, retained Down-result alias release orders, repeated calls, foreign authority, external/lifecycle cancellation, checked cancellation rejection, storage faults and healthy fresh-receipt completion.

Current QA's old-tag Candidate6 and F2 original Raw/GAP identity/stamp assertions are retained alongside stale Close after actual epoch/W recycle and repeated Halted prefix/snapshot assertions. Fixtures keep the original Durable gate; no Flushed adaptation is applied. The QA-D1 three-input SHA256/ZIP-copy identity, rejected-head failures, final actual commands/results and scope audit are recorded in its appended handoff section. Its containing head `8a8969acce1e0e7873929736daef5d69d78a7e41` passed worker/CI388 tests, but independent QA rejected its public boundary with QA-D2. Historical independent379 PASS/CI37636862096 did not cover QA-D1; neither that evidence nor388 PASS accepts the correction in §14. Full independent QA of the new SHA remains required; worker/CI PASS does not authorize merge or change Draft PR status.

## 14. QA-D2 public partial-completion accounting

### 14.1 Concrete cancellation boundary

The existing public signature remains:

```rust
SupervisorSessionHandle::cancel_generated_plan(
    &self, turn: &mut SessionTurn, owner: &WorkOwner,
) -> Result<(), AuthorityError>;
```

The owner-minted handle is a public authority boundary even before it is moved into the canonical supervisor. Its own checks MUST enforce the approved settlement rule. Validation of the rightful turn and WorkOwner precedes serialized reconciliation. The owner must still be PendingPlan, Generated and Pending. An existing StorageStopped rejects with `AuthorityError::StorageStopped`; otherwise any nonzero fresh generated confirmation count rejects with `AuthorityError::NotQuiescent`. Only zero fresh confirmations with no storage stop may proceed to the existing exact-scope-failure or DiagnosticClosing/DiagnosticClosed eligibility check, then Pending->Settled. Received Raw/GAP/control/timer never use this cancellation path.

`retain_generated_plan` resets the fresh generated confirmation count after authenticated Down completion. That earlier Down receipt is not one of the generated completion's three required receipts. DiagnosticClosed remains eligible for legitimate zero-record/no-stop cancellation despite descriptor closure; blanket `ensure_storage_writable()` would reject that approved transition and is not used. This is a restoration of existing semantics, with no new API, lifecycle generation, wire schema, writer permission or proof.

| Generated state before cancellation | Result / retained disposition |
|---|---|
| Zero fresh confirmations, no stop, eligible scope/lifecycle | Successful checked settlement; same W/Close identity and retained aliases. Capacity releases only under ordinary settled-owner rules. |
| One/two fresh confirmations, no stop | NotQuiescent; no obligation, progress, W, prefix, first failure/cut or Close mutation. |
| Zero/one/two fresh confirmations, StorageStopped | StorageStopped; original error/physical ambiguity preserved. No suffix retry, replacement work, rollback or fictitious settlement. |
| Foreign turn/owner | Typed rejection before rightful reconciliation or settlement; legitimate stewardship remains unchanged. |
| Wrong origin/state | Typed rejection without settlement after rightful bounded reconciliation. |

After a rejected cancellation, loss of the last work steward may mark the same retained cell Abandoned. Rightful reconciliation reports that bounded disposition without releasing W or undrained accounting, changing the original storage stop or reconstructing missing output. First failure/cut and trusted prefix remain fixed. Mandatory Close retains its existing discoverable/reclaimable/Settled identity. No successful finalization or publication is revived.

### 14.2 Physical and authenticated progress

The minimal public trigger is an authenticated Down, same-owner mandatory Close settlement, Generated fresh-count reset, one/two successful concrete Durable epoch writes, then a BeforeWrite error or post-write mismatch/weak gate, DiagnosticClosed and direct public cancellation. Rejected `8a8969ac` incorrectly returned Ok and let Drop erase W/undrained work. The failed latch survived throughout: severity remains MEDIUM obligation/accounting loss, not a proven finalization revival or heap/RSS violation.

Even zero fresh confirmations with StorageStopped cannot establish that no bytes were written. BeforeWrite preserves both watermarks; post-write mismatch/weak gate may advance the concrete backend watermark and physical records while the trusted gate prefix remains earlier. Cancellation rejection and repeated diagnostic reports retain that distinction and original typed storage error. Healthy generated completion still requires three fresh authenticated receipts and settles once; partial completion never becomes successful accounting through cancellation or Drop.

### 14.3 Regression, compatibility and evidence

The supplied six public tests compile and fail behaviorally on the rejected source in original Linux/Durable debug and release. They are transferred as normative tests and must pass after repair. Their canonical partial-completion/Drop control is one test function with12 internal cases (Open/Closing × one/two fresh confirmations × three faults), not12 test functions. Independent additions cover partial progress without stop, zero-progress stop, zero/no-stop scope/DiagnosticClosing/DiagnosticClosed success, foreign identities, repeats, held aliases/Drop, Close ownership and healthy three-receipt completion. Existing QA-D1, B1/B2/B3, R1–R3 and all31 §9 families remain mandatory, especially11/15/16/26.

Current report also contains a private same-authority registry swap. It directly replaces A/B WorkOwner associations during DiagnosticClosing and demonstrates the narrower validation limit described in §13.1. No natural public operation producing that swap was established. It is recorded as a defensive-validation/documentation discrepancy, not a second public blocker or evidence of foreign-authority escape. No new opaque-owner identity API is introduced to extend this task's policy.

Implementation changes are restricted to `crates/domain/src/capture_session.rs` and `crates/market-data/tests/ws_supervisor.rs`, plus this ADR and handoff. Public signatures, accepted schema/reader, backend profile authority, affine proof, bounds and finalization gates are unchanged. The two current attachments, manifest/embedded patch identity, actual debug/release commands and final counts are recorded in the appended handoff. New immutable SHA, exact four-path diff and fresh canonical CI with clean checkout belong in existing PR/Issue records. Historical388 PASS and CI37653209456 missed this public cancellation boundary and do not accept the new head. Full independent QA remains required; Draft PR #34 is neither merge-authorized nor owner-ready.

## 15. QA-NEW-01 identity restoration and Timer contract proposal

Status: **DESIGN_PROPOSED / TIMER_CONTRACT_BLOCKED**. Date: 2026-10-08.
This appendix records the existing-lineage bounded continuation after independent
CHANGES_REQUIRED on `c9ddf41275dd6c6ced19ab537310e924d7b0be53`, tree
`9ce6413fdd234c8ed7b1934ee18404568d181c54`. It does not amend accepted WAL bytes,
dependencies, §8 allowed paths or any of the 31 §9 acceptance requirements.
The owner explicitly confirmed that no approved runtime Timer-kind/disposition
contract exists and requested this proposal before dependent implementation.
The earlier cff design approval does not approve the proposal below.

### 15.1 Independent restoration and remaining boundary

The Timer-independent worker implementation binds representable received metadata to its
original W and commits private distinct progress only after a gate-confirmed
receipt. Raw, empty stale Raw plus its diagnostic GAP, exact QueueOverflow GAP,
original Up/Down and common Timer identity must reject unrelated/replaced/repeated
frames before backend write. This is metadata authentication, not admitted-payload
content authentication. Raw's permitted diagnostic GAP remains a distinct optional
stage; empty stale Raw requires its exact second stage. Existing no-write obsolete
Up/Pong requires authenticated same-epoch Down. Generated completion needs a
private original same-W Down/Close association and fresh distinct epoch stages;
no generic receipt count can establish that association. Actual worker source
changes and tests are enumerated in the handoff, without transferring prior PASS.
Received Down matching authenticates its represented metadata; existing canonical
mandatory Close creation remains a later operation. This restoration does not
claim public complete-observation proof of a full Down/Close job. Raw payload bytes
and decoder-dependent required diagnostics are not represented by admission and
are not authenticated by metadata matching.

Timer identity currently contains `(stream, connection epoch, ReceiveStamp,
timer_id, deadline_ns)`. `observation_identity` merges private PingTimer and
PongTimeout into that identical DTO. The active timeout writes Timer then Down;
Ping and obsolete timeout write Timer only. Earlier FIFO Pong/Connected can revoke
eligibility after admission. The public authority has no original kind or current
scheduler association and cannot authenticate this difference from the DTO or
the accepted TimerFired bytes. Exact matching of the common Timer record is
independent and implementable; required/authorized effect stages remain blocked.
Allowing optional matching Down preserves existing behavior but does not prove
that a Ping or obsolete timeout was entitled to Down or that an active timeout
could settle without it. Full QA-NEW-01 is not closed by that partial restoration.

### 15.2 Options and recommendation

| Option | Authority and compatibility | Decision |
|---|---|---|
| A. Bounded authority-owned scheduler and opaque plan | One authoritative schedule per fixed scope; private tokens bind kind, generation, original W and current output plan; no caller disposition flag. Existing supervisor computes/uses the same frozen engineering policy through this authority. | **Recommended / PROPOSED**. Best fit for the reachable pre-supervisor public-handle boundary. |
| B. Sealed supervisor disposition capability | A canonical supervisor could mint an opaque capsule whose issuer is authenticated by domain. Requires an explicit cross-crate sealing/issuer design, revocation protocol, and removal/restriction of the direct public-handle disposition route. | Alternative requiring Architecture review; a public constructor or trusted boolean would not satisfy it. |
| C. New caller-supplied kind/effect fields only | Adds a DTO but the same caller can replace active/obsolete disposition or claim success; accepted backend receipts still would not authenticate effect entitlement. | **Rejected** as insufficient. |

Option A has no network/clock adapter or domain->market-data dependency. It moves
only the bounded eligibility ledger to the existing authority, with an explicit
runtime API delta subject to approval. WAL/replay still records existing Timer,
Transport and epoch controls. Replay does not claim physical ping/Close delivery.

### 15.3 Proposed original runtime identity and capability

At registration freeze the existing supervisor engineering heartbeat policy and
its revision. Select active/obsolete disposition before operational preflight:
an obsolete Timer records its original identity even at MAX time/epochs, without
attempting deadline, reconnect or epoch arithmetic. For an active plan, perform
checked timer/deadline arithmetic before its effectful writes.
For each fixed scope retain a schedule generation, epoch, deadline, kind
(`Ping`/`Timeout`) and state. A queued original timer retains its immutable
common DTO, kind and opaque schedule token inside its already-counted W.
Tokens carry private authority identity and scope/epoch/generation; equality of
numeric IDs, deadlines or session identifiers does not authenticate a token.
There is no public constructor for an active/obsolete output permit.

The proposed operation that admits a due timer derives kind/ID/deadline from the
authority's current schedule and accepts the original observed ReceiveStamp;
the caller does not select a success flag or active disposition. Capacity is
reserved before committing generation/queued state. A rejected proposal leaves
the schedule retriable and creates no admitted original obligation.
Repeating a due proposal for an already queued schedule creates no second W or
token. TimerId/schedule-generation exhaustion returns a typed counter error
without wrap/reset, preserves the original schedule/queued owners, and follows
the existing terminal counter policy. A multi-scope tick reserves in the existing
deterministic stream order; partial success reports each admitted scope and the
first typed rejection, leaves rejected scope flags unchanged, and invents no
terminal received-input identity for a capacity-rejected timer proposal.

Under the rightful SessionTurn, the authority derives and freezes an output plan
immediately before the first Timer write from the current authenticated schedule
state and token. The plan is one of `TimerOnly(Ping)`, `TimerOnly(Obsolete)` or
`TimerThenDown(ActiveTimeout, same-W Close)`. Once a stage is confirmed, the plan
cannot be replaced, canceled as wholly uncommitted, or reselected from submitted
frames. The original identity and admission CutSide never change. API spelling,
issuer access and registration policy validation require explicit approval before
implementation; these proposed semantics are not permission to add functions now.

For the proposed active Timeout, reserve its one epoch-bound same-W Close in the
existing fixed scope cell at plan selection, before Timer I/O. This is a proposed
runtime semantic delta, not a claim about the old H1 behavior. The reservation
proves planned closure ownership, not a recorded Down. It becomes dispatchable
after authenticated Down or terminal storage failure; if Timer/Down writing
fails, keep the selected plan and reserved Close, distinguish trusted stages
from potentially written bytes, and report the storage error. A foreign/live
incompatible Close reservation rejects plan selection before any write; an
eligible same-original reservation is reused without a second owner or lease.

### 15.4 Authoritative serialized transitions

| Trigger under the same SessionTurn | Proposed authoritative transition |
|---|---|
| Gate-confirmed Connected/Up or current Pong/Up | Replace the eligible schedule for that scope/epoch and advance its bounded checked generation. Older admitted timer observations remain owned and recordable but their tokens become obsolete. Pong first in FIFO cancels a timeout even at D-1/D/D+1, preserving Q2. |
| Due Ping admission | Reserve W then attach the current Ping token; no operational effect at admission. If still eligible at drain, its exact Timer receipt precedes a bound Ping lease and the authority's AwaitingPong schedule. If revoked first, record Timer only and leave the newer schedule unchanged. |
| Due Timeout admission | Reserve W then attach original Timeout token. A preceding Pong/Connected/Down makes it obsolete at plan selection. A still-current timeout freezes Timer->Down with one mandatory same-W Close; its Down cannot be selected by a caller boolean. |
| Confirmed Down or scope terminal failure | Revoke operational schedules/leases for that epoch, preserve queued original Timer identities and current original mandatory Close. No later Up/Pong for the terminal epoch may revive it. |
| Authenticated epoch completion | Require distinct fresh connection/subscription/book stages and settled original Close. Install the new epoch with no eligible old tokens; generation does not wrap/reset in the archive. New eligibility requires the normal new-epoch Up path. |
| Receipt error, weak gate, mismatch, StorageStopped | Preserve earlier trusted progress and potentially written suffix truthfully. Retain the frozen plan/Pending W and, for an already selected active Timeout, its reserved Close even if no Down was authenticated. A Timer-only plan invents no Close. Invalidate successful finalization; no rollback or physical absence is promised. |

The unique mutable turn prevents reentrancy within one authority operation. It
does not prevent a caller from submitting Pong/Connected in a separate operation
between Timer and Down. A fixed per-scope frozen-plan state therefore retains the
original W and prevents such an operation from revoking or replacing an already
selected active Timeout plan until its Down is authenticated or terminal storage
failure is recorded. A later queued Pong/Connected remains owned in FIFO and is
drained after Down under the existing obsolete-control rule; a typed interstage
rejection preserves its identity, W and CutSide. Other scopes may continue.
Submitted frame content cannot choose a transition. A revoked but still admitted
original token remains recordable as obsolete at plan selection. A consumed or
replayed token cannot create a new W or receipt; these states are distinct.
Updating the authority and supervisor view must follow authenticated progress,
never a speculative mirror update before the gate. The schedule transition is
committed in that serialized authority gate-confirmation operation, rather than
depending on a later fallible supervisor mirror callback. Tests must establish that
only the authority's schedule is an entitlement source, including direct handles.

### 15.5 Proposed receipt stages and settlement

Every Timer plan requires one exact original Timer record: original context,
stream, both stamp samples, timer ID and deadline. A repeat, foreign token,
replaced stamp or unrelated record creates no fresh progress and is rejected
before write. A Timer-only plan settles after that stage; obsolete disposition
does not emit Ping, Down, epoch advancement or reconnect, and does not clear a
newer schedule owner. Operational Ping remains an authority-bound command lease,
with existing ambiguous-effect dispatch semantics and counted ownership.

An active Timeout plan additionally requires exact same-observation Down and
registration/retention of the same mandatory Close owner before settlement.
Close may be Pending/Leased when observation ownership transfers to the retained
generated plan; W and its Close remain counted. Finalization/epoch completion
still requires authenticated settlement of that Close, never a caller assertion.
Duplicate Down cannot create another Close or epoch plan. Exact Timer alone may
not settle an active Timeout. Exact Down alone may not settle any Timer.

Generated H1 uses a bounded immutable old-tag/BookId snapshot associated with that
same original Down/Close W. Connection, subscription and book receipts must be
distinct, ordered, have original stamp/plan owners and checked expected/next
values, and be fresh after the earlier Down. A repeated Connection receipt is
not a substitute for Subscription or Book. QA-D1 checked cancellation precedes
removal/reclassification; QA-D2 allows cancellation only with no fresh generated
stage and no StorageStopped. Received timers never use generated cancellation.

### 15.6 Bounds, tests and approval gate

No unbounded history, side lane, payload copy or externally supplied success bit
is proposed. State is fixed per scope plus inline metadata in its counted W;
obsolete queued tokens continue consuming that original W. Use checked finite
generations and preserve `W+N+1<=M`. Include actual `size_of`/allocation changes
in metadata ceilings and cap5/cap9 retention/100-repeat/teardown evidence; requested
Layout bytes are not RSS/usable heap or network-memory measurements.

Required positives: active Ping; active Timeout exact Timer->Down/one Close and
fresh H1 stages; obsolete timeout/ping after FIFO Pong/Connected/Down; both FIFO
orders and D-1/D/D+1; equal deadline/new-generation alias; admitted timers across
epoch change; rightful recovery after rejection; foreign rejection preserves the
original owner; storage faults retain authenticated prefix/Close; healthy original
completion and one borrowed final proof. Required negatives: kind swap, replaced
disposition/capsule, foreign/repeated/retired schedule token, changed stream/epoch/
stamp/ID/deadline, unrelated records, repeated stage, premature active Timeout
settlement, omitted/wrong Down or Close, earlier Down counted as fresh H1,
generated receipt for another plan, last-steward abandonment and attempted seals.
Include direct Timer-confirmed -> intervening Pong/Connected -> Down attempts:
the frozen plan cannot be overridden, the intervening original stays owned, and
unrelated-scope controls still progress. A revoked admitted token must record its
exact obsolete Timer; consumed/replayed tokens must reject without extra work.
Run genuine Linux/Durable debug/release and the unchanged full 31-row/corrective
scope, including Q2's 12 storage faults, 63 H1 variants, R1-R3/B1-B3/QA-D1/D2.

**Architecture/owner decision required:** approve one exact runtime authority and
transition contract and its allowed paths before implementing kind/disposition
extensions. Until then dependent Timer stages remain TIMER_CONTRACT_BLOCKED.
ENV-01 independently blocks required Linux/Durable execution on this Windows
surface. This proposal is not ACCEPTED, independent QA, owner readiness or merge
approval. Existing branch/PR stays in its original lineage and Draft state.

## 16. Received Down completion retains mandatory Close — 2026-10-08

Status: **DOWN_CLOSE_FIXED_IN_CODE / QA_PENDING** under the already approved
§3.2/§4/R2/§12.1 contract. Whole continuation remains PARTIAL / QA_PENDING;
§15 Timer proposal remains DESIGN_PROPOSED / TIMER_CONTRACT_BLOCKED, without
Architecture approval or runtime extension. This section supersedes only the
historical d85 received Down/full-job limitation described in §15.1. Section15
itself and its unapproved Timer recommendation are retained unchanged.

On reviewed `d85fa6876258b80a9594b158e87ad5cc7004d799`, a rightful public
owner-minted handle could persist exact original Disconnected Down, complete the
received observation, drop W and receive Ready while no Close had been installed.
The supplied Integrator probe was unexecuted on delivery. Worker compilation
and Written debug/release now fail behaviorally at missing Close0 versus1; an
additional separately preserved probe demonstrates completion Ok -> Drop,
work_used0, outstanding Close0, Ready=true. These are Windows Written results,
not parent Linux/Durable acceptance. Original Durable parent compilation succeeds
but setup fails Unsupported Unix metadata sync; behavioral result NOT_RUN.

Public received Disconnected completion now uses the existing serialized R2
mandatory_close transition before Pending->Settled. The first ordinary Down
installs a discoverable Pending Close on that original W. Pending/Leased Close
does not prevent received completion, but retains W and prevents Ready until
rightful dispatch/authenticated closure. The canonical later lease_command
reuses the same owner; no supervisor-only ordering fix or second W is introduced.

An existing same-epoch original Close is retained. A different live W is reusable
only when its retained original scope/epoch and authenticated Down or original
Generated Down association establish a valid prior owner. Unrelated None/Raw/
Timer-only live owners reject before settlement without overwriting Close or
changing original Pending/progress, prefix, watermarks or physical records.
Already fulfilled Close and exact reserved terminal Close retain existing R2
reuse; duplicates create no second owner/lease. Installation/reference errors
remain typed and preserve rightful retry. No rollback of already written Down
or unconfirmed postwrite bytes is promised. The existing TimerDown evidence is
only recognized as historical Down metadata; no Timer kind/eligibility decision
is added by this correction.

Source fix `f80ea21948dd88676e4cfa293f35789bbd3569f6`, parent d85, changes only
domain capture_session.rs and recording capture_session tests. Initial Linux CI
found one inaccurate new fixture expectation: the concrete finalized seal records
InputQuality::Unknown, not NoKnownLoss. Test-only child
`18937c96de95648ba5254203be6f0c2e274da5dd`, tree
`d85d20b7189c12d2e3ed77eb664c7bbfefaca06a`, corrects only that expectation.
Production completeness, physical Complete/unique seals and proof checks stay
unchanged. Fresh [source-head CI37740622434](https://github.com/al-gri/pro-sclpng/actions/runs/37740622434)
verifies exact18937c96, pinned1.98.1 and final clean checks:439 PASS/0 FAIL/
0 ignored, including all61 recording functions and11 affine compile-fail cases.
This is worker Linux debug evidence; release/independent acceptance is not implied.

Six paired Written/Durable families cover automatic first Close and foreign
completion/lease operations; alias exhaustion with preserved Pending and retry;
authentic prior/terminal reuse; incompatible live owners; fixed cut, neighbor
drain, marker faults/ambiguity and abandonment; reserved terminal states. Two
supplied probe names are retained. All47 previous recording functions remain;
61 now means14 added functions, not loop variants. Existing control/H1 positives
discover the automatic Close after completion; obsolete Up/Pong retains the actual
Close/W until dispatch rather than relying on the historical missing-Close path.

No fields, public API, dependencies or layout change. WorkCell remains248 bytes;
cap5/cap9 metadata ceilings and Written/Flushed allocation profiles stay intact.
Actual owner/full-supervisor debug/release retention/repeat/teardown results are
in the appended handoff. §8 and all31 §9 requirements remain byte-identical;
accepted schema, recovery/specs, manifests/lockfile/toolchain/workflow and inherited
whole-PR paths are unchanged. Only the two source/test paths and two delivery
documents are used. The immutable docs child and its new exact-head CI/command
results are recorded externally after commit, avoiding a containing-SHA self-reference.

Genuine full Linux/Durable release remains NOT_RUN/BLOCKED: no accessible local
Linux executor; the unchanged workflow has no release step. Timer A1–A5 remains
an Architecture request, not approval. Full new independent debug/release QA of
the final immutable head and all31/corrective scope is still required. Draft PR34,
Issue20, parent5 and existing claim/recovery remain; no readiness/merge is granted.


## Timer A approval provenance — 2026-10-08

Current Timer status: **DESIGN_APPROVED_A / IMPLEMENTATION_PENDING / QA_PENDING**.
Decision: `ARCH-REC-001D-TIMER-A-20261008`. The owner delivered the complete
approved contract through `WORKER-REC-001D-Timer-A-approved.zip`,7263901 bytes,
SHA256 `26cf0ae5721481ab16e5a910294a2641ab80820f264ebecc11ccecc0356d75f4`.
Root TASK.txt22723B SHA256 `8ab8184af20f3c530142e4b4c324af178e263ee615d6e2688afb1773c91b83d2`
is the current authorized routing. Full normative addendum33986B SHA256
`cf585a92e3bd8372caf90326c67436274fef5abb5a8d163b389aa0a42ab838ba`
is copied below byte-for-byte, including its complete provenance and T01–T16.
Its source approval archive367258B SHA256
`ef492ce7b4ec07e7ed288a827c08412d473574dbdc76c55325806e48c55b30d1`
and the original previous Down/Close packet are retained unchanged as inputs.

Approved contract is exact ADR0003§15 at immutable
`d85fa6876258b80a9594b158e87ad5cc7004d799`,tree
`3d702b6a9d4f01703134a6c1b6a19b64e3aab4e6`, PLUS the complete §15A below.
§15A prevails over open/incomplete proposal language. Historical §15 and prior
Timer DESIGN_PROPOSED/TIMER_CONTRACT_BLOCKED statements remain as history;
they are superseded for this explicitly approved bounded A implementation.
Option B and caller-supplied disposition remain unauthorized. This approval
does not approve runtime results, QA-NEW-01, acceptance, READY or merge.

Packet snapshot18937c96de95648ba5254203be6f0c2e274da5dd is an ancestor of actual
fresh start `8e4e2b49930d38e16d5b48bb165ee0d8d69dd690`,tree
`eb01f5d529c1e3ffbe65d70c048bca085e1350d0`. The intervening two commits are our
Down/Close handoff and its recorded allocation-number correction, only ADR/handoff
paths. Source/test Down/Close commits f80ea219 and18937c96 are preserved.
Live main/base remains39ff0dba797eb010586238ef06fb80e996340401, PR34 is open/Draft/
unmerged, canonical checkout initially clean, Rust/Cargo1.98.1. Same Issue20/
parent5, claim6024304772/recovery6025249885 and feat/REC-001D-ws-supervisor.

This docs-only approval provenance must be committed before dependent Timer
production edits. Its containing immutable SHA is reported after commit in the
existing PR/Issue metadata, avoiding a containing-commit self-reference.
§8, all31 §9 rows and prior Down/Close contracts remain. Only the13 exact allowed
paths in §15A.6 may implement A. Genuine new-head Linux/Durable debug AND release,
fresh exact-head CI, updated metadata bounds/T01–T16/31-family evidence and full
new independent QA remain required. Architecture runtime NOT_RUN is provenance,
not a worker result. No force push/main write/merge/auto-merge/readiness/closure.
## 15A. Architecture decision — bounded Timer authority, option A

Decision ID: ARCH-REC-001D-TIMER-A-20261008.
Status: DESIGN_APPROVED_A / APPROVED_FOR_BOUNDED_IMPLEMENTATION.
Reviewed immutable proposal: d85fa6876258b80a9594b158e87ad5cc7004d799.
Reviewed tree: 3d702b6a9d4f01703134a6c1b6a19b64e3aab4e6.
Proposal parent: 0d0aebb3937fea15fb3dab2dfe817879af016d3e.
Accepted main/reference: 39ff0dba797eb010586238ef06fb80e996340401.
Date: 2026-10-08 Europe/Warsaw. Authority: project's Architecture role, as requested by owner and TASK.txt.

The approved design is the exact §15 proposal at the reviewed SHA PLUS this complete mandatory addendum. This addendum resolves A1–A5 and prevails where the proposal leaves an API undecided or differs from the decisions below. §15 alone, cff approval alone, an implementation appendix or green CI is not this approval. Option A is selected; B is not authorized; caller-supplied disposition C is rejected. This decision releases only the bounded Timer design/implementation gate. It does not accept the partial d85 code, close QA-NEW-01, resolve the separately reported Down/Close finding, issue READY, or authorize merge.

MUST/MUST NOT below are normative. Worker MUST copy this exact accepted addendum and its decision provenance to the existing ADR/handoff before implementing the dependent Timer extension. Preserve previous proposal/rejected-head history. Record the new docs/implementation SHA separately; no d85 test result transfers to it.

### 15A.1 A1 — actual admission order and direct-route FIFO

Invariant: INV-03/04/09; ADR0003 §2/4 and Q2 FIFO; original identity, immutable CutSide and conservation of W.

The existing WorkOwner reservation sequence is NOT sufficient evidence of observation admission order: a direct caller can reserve a slot early and associate a received identity later. Keep WorkOwner.id/reservation sequence unchanged. Add one checked, archive-lifetime authority record-admission counter and fixed inline record_admission_order metadata in the existing counted W cells. A received observation gets its immutable order atomically on successful identity admission, not on reserve_work. admit_due_timer reserves/admit-associates its W and order atomically. No caller supplies an ordinal. No new queue, retained history or second work lane is permitted.

For a Timer first-stage operation or a schedule-affecting Up/Pong/Disconnected/epoch-record operation x, let order(x) be its authority-assigned actual record admission order and scope(x) its registered stream scope. Reject iff the bounded W ledger contains y satisfying ALL:

  scope(y) == scope(x)
  order(y) < order(x)
  y has an already-admitted required record-bearing stage
  that stage has neither an authenticated exact receipt nor an authenticated existing no-write obsolete settlement.

Use private distinct-stage progress and original registered scope, not alias count, mutable WorkKind, generic receipt total, timestamp comparison or numeric Timer ID. Choose the earliest blocking order deterministically. Return AuthorityError::TimerOrderBlocked { earlier_work_id } before plan freeze, Close reservation, operational arithmetic, schedule mutation or backend I/O. Preserve both original identities, Pending/W, CutSide, plan, schedule, prefix/watermarks and Close. Rightful FIFO progress then retries the same owner. Existing marker/cut and epoch-dependency gates apply additionally; the new predicate cannot bypass them.

Apply the predicate on the PUBLIC authority/bound-sink path both to Timer selection and authenticated schedule-changing controls. Checking Timer alone is insufficient: a later Pong must not be written first and revoke an earlier admitted Timeout before its plan is selected. A receipt-confirmed earlier stage does not block merely because a result/command alias is held or observation completion has not yet been called. A required unconfirmed stale diagnostic stage does block. Authenticated no-write obsolete control settlement after original Down remains available.

A planned/unadmitted generated H1 output is not a record-bearing FIFO blocker. Merely retaining a generated plan or holding its original Down/Close aliases MUST NOT make it one. If a generated stage is actually activated for record persistence, give that stage a checked order only at its authority-controlled admission, preserve its original provenance separately, and apply the same rule. Stage activation is bounded inline metadata; it follows existing settled-Close/epoch/cut dependencies. Prospective activation checks the predecessors before committing its ordinal; an unready plan cannot install a barrier ahead of received work. F2 coalescing retains the original admitted order and allocates no new ordinal.

Once an active Timeout plan is frozen by the first Timer persistence operation, a same-scope Up/Pong/Disconnected/epoch operation cannot revoke, replace or skip its required Down. An attempt between Timer and Down returns TimerPlanInProgress { work_id } (or the earlier-order error when applicable) before I/O. The intervening observation stays owned. After authenticated same-epoch Down, intervening Connected/Pong may use the existing authenticated no-write obsolete-control settlement. Received Disconnected and legitimate generated epoch stages retain their original exact record/dependency requirements; this decision grants them no new no-write settlement. Unrelated scopes can progress under their own gates. A caller's earlier slot reservation cannot create retroactive precedence.

### 15A.2 A2 — sole authority registration/admission and minimal API

Invariant: INV-03/09/11/18/19; §15 original token/plan association, fixed budgets and no caller disposition.

The owner-minted CaptureSessionAuthority owns the sole scheduler and eligibility ledger. Register it once with the accepted scope registry, retention budget and prefix through the existing CaptureSessionOwner registration path. The domain-level conformance registration uses the same policy. Freeze HeartbeatPolicy::SupervisorV2, revision 2: Ping interval 30_000_000_000 ns and Pong timeout 15_000_000_000 ns. These are the current d85 local engineering constants, not an exchange guarantee, freshness proof or trading threshold. Domain owns this dependency-free fixed preset; market-data consumes/re-exports it without a domain→market-data dependency. Arbitrary durations/revisions are not constructor inputs. Re-registration/policy replacement returns AlreadyRegistered; there is no mutable policy/install-schedule API. Existing binding/budget/lifecycle validation precedes registration commit. A retained compatibility registration wrapper MUST delegate to this same frozen preset, never omit the authority scheduler.

Per configured scope, fixed metadata tracks Disabled/AwaitingPing/AwaitingPong, current epoch, checked schedule generation, due deadline, timer-ID frontier and queued-original association. Init generation/ID frontier is 0 with no eligible schedule. Installing an eligible replacement uses checked next generation; accepted Timer IDs are positive and advance on successful due admission. Epoch/reconnect does not reset either counter. Revocation/Closing disables eligibility without incrementing a generation or calculating a future deadline, so obsolete originals remain recordable at MAX.

Approve the following minimal surface (signature sketch; existing error/wrapper return types retain their established owner-bound meaning):

  pub enum HeartbeatPolicy { SupervisorV2 }
  pub enum TimerKind { Ping, Timeout } // read-only original information

  CaptureSessionOwner::register_supervisor(
      &mut self, turn: &mut SessionTurn, scopes: &[ScopeBinding],
      budget: RetentionBudget, heartbeat_policy: HeartbeatPolicy
  ) -> Result<(SupervisorSessionHandle, BoundRecordSink), OwnerError>;

  CaptureSessionAuthority::register_supervisor(
      &self, turn: &mut SessionTurn, scopes: &[ScopeBinding],
      budget: RetentionBudget, prefix: PrefixBinding,
      heartbeat_policy: HeartbeatPolicy
  ) -> Result<SupervisorSessionHandle, AuthorityError>;

  SupervisorSessionHandle::admit_due_timer(
      &self, turn: &mut SessionTurn, stream: StreamId,
      observed_stamp: ReceiveStamp
  ) -> Result<TimerAdmission, AuthorityError>;

  pub enum TimerAdmission {
      NotDue,
      AlreadyQueued { original_work_id: u64 },
      Admitted(AdmittedTimer),
  }

  SupervisorSessionHandle::timer_progress(
      &self, turn: &mut SessionTurn, timer_owner: &WorkOwner
  ) -> Result<TimerProgressView, AuthorityError>;

  SupervisorSessionHandle::take_timer_ping(
      &self, turn: &mut SessionTurn, timer_owner: &WorkOwner
  ) -> Result<CommandLease, AuthorityError>;

AdmittedTimer has private fields with read-only access to original kind/identity and borrowed/consuming access to its original WorkOwner. Its authority/scope/epoch/generation/original-ID token is private fixed metadata in that same W, with no public token or permit constructor. TimerProgressView is a bounded read-only diagnostic view of Unselected, TimerOnly(Ping), TimerOnly(Obsolete) or TimerThenDown, exact authenticated stages and associated Close. It is never an input that grants entitlement.

Keep BoundRecordSink::persist_owned as the actual receipt boundary and complete_observation as the settlement boundary. Within the FIRST exact Timer persist_owned operation, authority validates the original identity/token, applies A1, derives active/obsolete eligibility, preflights/reserves the fixed output plan, then invokes backend and commits authenticated progress in one serialized SessionTurn operation. Identity/order and any active Close conflict checks precede terminal operational arithmetic, so a conflicting/foreign call cannot cause TimeOverflow instead of a preserving rejection. Do not introduce a public prepare/install-active operation or a gap in which an external plan/callback chooses entitlement. A caller uses timer_progress after authenticated progress; its mirror is not authority. Repeated stage calls authenticate against the frozen plan, not a new frame-selected plan. For an original Timer, complete_observation derives its required stages from this private plan, never caller obsolete/disposition input: missing required progress returns NotQuiescent; StorageStopped returns StorageStopped; successful complete settles once; repeated completion returns the existing InvalidOwner/OwnerRetired outcome without new progress.

admit_due_timer accepts no caller WorkOwner, kind, ID, deadline, generation, active bool, output plan or success flag. It derives all original fields from the current eligible schedule and supplied original ReceiveStamp; observed monotonic time must be >= original deadline. No eligible/due schedule -> NotDue, no work; already queued current schedule -> AlreadyQueued for the same original, no new W/token/ID. Capacity failure -> WorkExhausted with all frontiers/generation/queued flags unchanged and no fabricated received Timer/failure identity. Capacity is reserved before admission commit. A multi-scope tick visits ascending configured StreamId, reports bounded admitted scopes plus the first typed error, and leaves the rejected proposal retryable. Scope terminal failure/lifecycle/StorageStopped use their existing typed rejection and admission rules.

Close BOTH legacy routes before they can mutate rightful state:
  admit_observation(... ObservationClass::Timer { ... }) -> TimerAuthorityRequired;
  generic command(... SendText("ping"), ...) -> TimerAuthorityRequired.
Historical confirmed_timer, arbitrary W or exact common Timer bytes do not mint a Ping. Owner-bound persistence of a Timer without the new original token/plan also returns TimerAuthorityRequired. Existing generic WAL outside the canonical capture profile is unchanged and is not represented as enforcing this authority contract.

Counter names are TimerId, TimerScheduleGeneration and RecordAdmissionOrder; exhaustion is checked with no wrap/reuse/reset. Due proposals follow the existing generated hard-stop counter/time policy, without inventing a received input identity. Genuine received observation admission-order exhaustion follows the existing reserved terminal received-input failure policy with the represented original identity; it does not fabricate an ordinal. Active deadline overflow retains TimeOverflow provenance. These terminal representational failures are distinct from A5's retryable identity/order rejection. Preflight all needed Timer-plan counters/alias capacity before its backend attempt. Preserve existing RecordNo/epoch/H1/H2 hard-stop behavior; a later H1 failure cannot retract earlier authenticated Down/Close. Do not add speculative future-epoch requirements to obsolete Timer processing.

### 15A.3 A3 — atomic active Ping receipt and one-shot command

Invariant: same-W ownership, exact distinct receipts, no speculative eligibility, INV-03/04/09; B1 and Q2.

For active Ping, before the first Timer backend attempt authority MUST check and reserve the next schedule generation, deadline = original Timer ReceiveStamp.monotonic_ns.checked_add(15_000_000_000), and fixed same-W Ping command ownership/alias capacity. Exact original Timer gate confirmation commits ALL of these atomically: exact stage receipt; AwaitingPong epoch/generation/deadline; original Timer observation progress; retained counted same-W Ping entitlement. No fallible share(), counter/deadline calculation or supervisor callback may be needed afterward to establish that ownership/eligibility. A failed receipt grants no Ping and installs no successful AwaitingPong transition.

take_timer_ping moves the sole previously retained original entitlement into one affine CommandLease without reserving another W, choosing a new disposition or requiring a new alias allocation. Before confirmed active Timer return PingNotReady; after transfer return PingAlreadyTaken; authority-side revocation of an untransferred Ping returns CommandRevoked. Obsolete/non-Ping originals cannot supply a lease. complete_observation may settle the Timer record obligation after its exact required receipt, but cannot erase an untransferred/held command reference or fictionally free W. Caller Drop never implies receipt settlement.

The Ping lease binds original Timer W/epoch and the newly committed AwaitingPong generation. Dispatch revalidates that exact current association, permitted lifecycle/scope, no StorageStopped and no active Timeout freeze. Later authenticated Pong/Connected/Down, scoped terminal failure, Closing or epoch replacement revokes an unsent old Ping. Authority-held nonmandatory command references release truthfully on revocation; held external leases remain counted until consumed/Drop. Neither path settles pending record obligations.

Physical dispatch remains the existing affine command effect. An invoked effect failure is DispatchFailed with AmbiguousEffect::Unknown; Drop/error does not rewind schedule, create a new Ping entitlement or promise absence of physical bytes/effect. Ping retry/reclaim is N/A and no new protocol is authorized. Close retains its separate existing reclaim contract. The AwaitingPong deadline starts from the ORIGINAL recorded Timer stamp, never dispatch time, drain time, a fresh clock sample or a later mirror update.

An authenticated current Connected/Pong Up installs AwaitingPing at original Up/Pong ReceiveStamp.monotonic_ns + 30_000_000_000 with checked generation/deadline prepared before its effectful record. Authentication/order/frozen-plan checks precede that arithmetic. Obsolete Ping records its exact original Timer only, bypasses active arithmetic, emits no command/Down/epoch/reconnect and cannot clear/change the newer schedule.

### 15A.4 A4 — lifecycle transition table

Invariant: §2/3.2/5/6, Q2, R1–R3, B1–B3 and QA-D1/D2; Close remains owned independently of descriptors and aliases.

State                         | New due admission / operational service          | Already-admitted Timer/record progress                           | Close / completion
Open, healthy scope           | Eligible policy scheduling allowed              | FIFO; derive active/obsolete before first Timer I/O               | Active Timeout rules A5; existing H1 after settled Close
FailedDiagnostic, failed scope| No new operational Timer/lease for failed scope | Unselected originals TimerOnly(Obsolete); frozen Timeout preserved| Same original terminal Close; no revival/reconnect for failed scope
FailedDiagnostic, healthy neighbor | DiagnosticOnly transport service remains: lawful Ping/Subscribe/reconnect | Normal authority eligibility/FIFO while storage writable     | Existing neighbor H1 and Close; archive publication/seals remain denied
Normal Closing                | Admission closed; operational schedules/leases revoked | Unselected admitted Timer -> TimerOnly(Obsolete); already-frozen Timeout still requires Timer->Down | Reserved/held mandatory Close remains; no NEW H1/reconnect plan from a Down confirmed after Closing
DiagnosticClosing, writable   | No new operational scheduling/leases           | Drain entitled admitted originals/marker under fixed cut; frozen Timeout remains required | Mandatory Close available under A5; generated cancellation only existing checked D1/D2 conditions
DiagnosticClosed              | No new admission, operational service or append| Retain/report originals, frozen plan, receipts and undrained W; no fictionally successful completion | Existing mandatory Close discovery/reclaim/dispatch remains; no seals/epoch revival
StorageStopped (overlay)      | No new admission/non-Close operational effect   | No Timer/Down/suffix retry; retain trusted stages, possible suffix and undrained ownership | Selected active Timeout's same Close becomes ready; discovery/reclaim survives descriptor closure
Finalized                     | No new schedule/admission/progress             | Only already-final immutable reports                            | No new owner/lease; settled Close is never reissued

Normal Closing is not a new generated cancellation entitlement. H1 already owned BEFORE Closing means a generated obligation registered before that transition; a future H1 snapshot inside an unfinished Timer plan alone is not such an obligation. Already-owned H1 may drain its originally required distinct records under existing Close/epoch/cut gates, but grants no Connect/Subscribe/Ping/Reconnect effect and activates no eligible schedule. No new H1 plan may be created from a Timeout Down confirmed after Closing: settle the received Timer->Down obligation only after retaining its same mandatory Close, then keep W counted for that Close/aliases until truthful release.

In DiagnosticClosing, partial fresh H1 output or StorageStopped cannot be canceled under D2. Any originally entitled writable suffix remains controlled by its frozen plan/existing gates; otherwise report it explicitly undrained. Never erase it, claim settlement or issue a final proof merely to obtain closure. Timers are received obligations and never use generated-plan cancellation.

Closing/failed diagnostic closure does not replace, reclassify or discard an already-frozen active Timeout, even if only Timer was confirmed. Preserve its exact required Down and same Close until authenticated progress or explicit terminal storage stop. Normal/Diagnostic Closing makes UNSELECTED Timer originals obsolete without active deadline/epoch arithmetic. Revocation requires no counter increment. StorageStopped takes precedence over writable-drain permissions in every row.

### 15A.5 A5 — rejection, storage stop and new Close reservation delta

Invariant: original Pending/W/identity/CutSide, truthful prefix, R2 one Close, R3 irreversible finalization and D2 no false settlement.

Prewrite validation errors for foreign turn/handle/sink/W/token, replaced identity, wrong/repeated stage, FIFO order or incompatible Close reservation MUST be typed and occur before backend I/O AND rightful reconciliation/schedule/plan/Close mutation. They preserve rightful retry, exact identity, W, cut, schedule, frozen plan, prefix/watermarks and existing Close; they do not create StorageStopped. This guarantee applies on the new Timer and schedule-affecting routes, including complete_observation/take_timer_ping identity checks. Existing rightful reconciliation of genuine abandonment remains a separate operation; a foreign rejected call cannot trigger it. Backend invocation count for these errors is zero.

Actual backend error (including reported before-write failure), weak achieved gate or postwrite receipt mismatch installs/preserves the terminal storage stop and original error. Retain previous trusted progress, frozen plan, W and Close; report possible physical suffix independently from authenticated prefix. No rollback, byte absence, retry/suffix repair, writer substitution or receipt fabrication is promised. Existing terminal checked counter/time hard stops are also terminal and keep their original provenance; do not mislabel them as foreign/order rejection or a successful receipt.

Approve this NEW semantic delta explicitly: an active Timeout first operation reserves exactly one epoch-bound mandatory Close on the ORIGINAL same W before Timer I/O. It does not authenticate Down. Reuse only an existing reservation for that exact authority, scope, epoch and original W; a live incompatible reservation returns TimerCloseConflict before mutation/write. The fixed scope cell and same W carry the reservation; no second owner/lease, nonce/history lane or extra W is added.

The reservation is discoverable as Pending with read-only readiness. Ordinary reclaim before authenticated Down or terminal storage failure returns CloseNotReady without state/counter changes or a lease. After authenticated exact same-observation Down OR terminal storage stop, the SAME owner becomes reclaimable/dispatchable. Timer receipt alone is insufficient. Readiness guards EVERY Timer-associated Close lease issuance/conversion/dispatch route, not merely reclaim. Generic mandatory_close cannot mint or upgrade a Close from an unselected/Ping/obsolete Timer W; it may return only the active Timeout's already-authorized original reference. The scope/epoch and original work association never change; repeat reserve/Down/reclaim cannot produce another owner or active lease.

Compatibility exception required by pre-existing §3.2/R2: if a genuine scoped terminal-failure path occurs after selection while storage is still writable, its inherited immediate fail-safe Close permission makes this SAME reserved owner ready, even without authenticated Down. This is authority terminal-failure evidence, not a caller flag and not a second Close. Frozen Timer->Down remains a record obligation; Close dispatch cannot fake either receipt. This exception preserves terminal Close service and healthy-neighbor behavior.

Leased -> repeated reclaim is AlreadyLeased; Drop/ambiguous effect error returns the same original Close Pending; success/authenticated external closure settles it; Settled cannot be reissued. Existing opaque authenticated external closure may settle the SAME reservation without fabricating Timer/Down receipts: it does not settle their record obligation or grant a new external-evidence constructor. These semantics survive StorageStopped and DiagnosticClosed. Active Timeout complete_observation requires exact original Timer then exact original Down plus retained same-W Close association. Close may still be Pending/Leased when ownership transfers to its legitimate H1 plan. Finalization and H1 epoch completion require actual Close settlement. Timer alone, Down alone, another W's Down/Close, historical Down or numeric-equal foreign identity cannot establish it.

H1 still uses its bounded original old-tag/BookId snapshot and three distinct fresh ordered Connection/Subscription/Book receipts AFTER original Down and settled Close. Later fallible epoch/storage completion cannot erase successful Down/Close. Existing H1/H2 preflight/counter policy is preserved; do not add early future-epoch promises or arithmetic to obsolete Timer processing. No new Timer cancellation API is authorized.

### 15A.6 Exact implementation allowlist and compatibility

This is a bounded subset of unchanged approved §8; use a path only when the Timer contract actually requires it:
  crates/domain/src/capture_session.rs
  crates/domain/src/lib.rs
  crates/domain/tests/capture_session.rs
  crates/market-data/src/ws_supervisor.rs
  crates/market-data/src/lib.rs
  crates/market-data/tests/ws_supervisor.rs
  crates/recording/src/capture_session.rs
  crates/recording/src/lib.rs
  crates/recording/src/file.rs
  crates/recording/tests/capture_session.rs
  crates/recording/tests/wal.rs
  docs/adr/0003-ws-capture-saturation.md
  docs/handoffs/REC-001D.md

Recording production changes are permitted only for registration/authority/sink/Close integration that is actually necessary. No wildcard or manifest/lockfile/dependency/toolchain/workflow/spec/ADR0002/application/governance change is authorized. Do not extend raw payload/digest or decoder-required disposition authentication in this packet. Existing Timer/Transport/epoch WAL bytes, schema/tags, recovery classification, dense RecordNo, physical-versus-quality reporting and replay limitations stay unchanged. Runtime opaque token/kind metadata is not a new wire field or proof of physical delivery. Unknown exchange quantity/delete constraints remain unchanged.

All schedule/token/ordinal/plan/Ping/Close state is fixed per registered scope or inline in its already-counted W. W+N+1<=M remains invariant on admission/transfer/rejection/Drop/reclaim/storage error. No per-retry growth or token-history cache. Account for every new scalar, enum, reference and allocation in actual size_of-derived metadata ceilings. cap5/cap9 accounting and 100-repeat/teardown checks are mandatory; requested Layout bytes are not RSS, allocator usable heap or network memory.

### 15A.7 Mandatory implementation and independent-QA tests

Every group below requires positive control, negative observations, identity/stage/W/cut/schedule/Close snapshots and backend invocation/prefix assertions as applicable. Public tests use genuine owner-minted handles and original Durable filesystem profile; private counter injection may establish otherwise unreachable finite boundaries but is identified as such. Tests/logs listed here are REQUIRED, NOT_RUN by this Architecture review.

T01 A1: Pong-first/Timeout-second; direct Timeout-first rejects before I/O; rightful Pong commits; exact original Timeout records obsolete. Repeat Connected and D-1/D/D+1. Timer-first/Pong-second: direct Pong-first rejects BEFORE any Timer plan exists; rightful Timer->Down then obsolete control. Unrelated scope progresses.
T02 A1: pre-reserved owners admitted in reverse reservation order prove actual record-admission order. Earlier Raw/GAP/stale required second stage and actually admitted generated stage block; receipt-confirmed held result/command aliases and unadmitted generated plan do not deadlock. Preserve F2/R1 cut/marker behavior.
T03 A1/A5: direct active Timer confirmed -> intervening Pong/Connected/epoch attempts -> exact Down; typed rejection preserves intervening original W/CutSide and frozen plan; neighbor writes allowed under its own gates.
T04 A2: generic Timer admission, tokenless Timer write and generic SendText("ping") mint reject before mutation/I/O, including arbitrary W, obsolete Timer and historical confirmed_timer.
T05 A2/A5: foreign authority with equal numeric IDs/deadlines, wrong/retired/replayed token, substituted kind/disposition/stream/epoch/both stamps/ID/deadline, unrelated/repeated frames and attempted old-slot reuse reject; original retry succeeds once. Revoked admitted token records original obsolete Timer; consumed token creates no new W/receipt.
T06 A2: registration freezing/replacement, valid original binding, invalid binding/budget/lifecycle; policy version/values match. Due before D -> NotDue, at/after D -> one admitted original; duplicate tick -> same AlreadyQueued; partial multi-scope capacity reject is truthful and retryable with unchanged rejected frontiers/flags.
T07 A2/A3: TimerId/ScheduleGeneration/RecordAdmissionOrder/RecordNo checked MAX boundaries and no reset across epoch; active deadline overflow has original typed hard stop; obsolete timers/revocation/Closing at MAX avoid active preflight. Received versus generated counter outcomes keep their different provenance; no fabricated successor/failure identity.
T08 A3: active Ping exact Timer receipt commits AwaitingPong and same-W command atomically; complete/Drop before extraction cannot free command W; extraction once only; early/second/obsolete/foreign extraction rejected. Force alias/counter/deadline preflight failure and all receipt faults: no late callback/mirror or fallible share can grant success.
T09 A3: original stamp+15s/+30s deadlines even after delayed drain/dispatch; mutable supervisor mirror cannot choose entitlement. Held Ping revoked by Pong/Connected/Down/Timeout freeze/Closing/epoch change; no callback effect after revocation, no retry/reclaim after Drop or ambiguous dispatch error.
T10 A4: every lifecycle row, healthy DiagnosticOnly neighbor service, unselected obsolete Timer in Closing, frozen Timeout spanning Closing/DiagnosticClosing, descriptor-closed reporting, StorageStopped dominance and no final seals. Distinguish already-owned H1 drain from forbidden new Closing H1; D2 partial plan never falsely canceled.
T11 A5: Close reserved/discoverable before Timer I/O; reclaim CloseNotReady before Down; exactly same owner ready after Down OR storage stop. Genuine scoped failure activates inherited SAME fail-safe Close without Down while neighbor continues. Foreign/live-conflicting reservation rejects prewrite; repeated Down/reclaim/Drop/error/settled owner never doubles a lease or W.
T12 A5: all original Timer and Down error-before-write, actual postwrite mismatch and weak-gate faults, including zero/one confirmed Timer stage; preserve trusted versus physical prefix, original error/plan/W/Close. Correct identity/order rejection creates no stop and permits rightful retry; genuine backend-reported before-write error still stops.
T13 §15 stages/H1: active Timeout Timer-only and Down-only completion denied; obsolete/Ping Down injection denied; wrong/historical/foreign Close denied. Fresh original Connection->Subscription->Book after settled original Close succeeds; repeated/other-plan/old Down never substitutes. Later completion failure keeps prior Down/Close.
T14 W/finalization: last-steward/alias Drop before/between/after stages retains Pending/Abandoned obligations; no false free W, cancellation, proof or successful seals. Healthy full completion gives one borrowed proof/finalization; failed latch denies every later publication/finalization.
T15 bounds: cap5/cap9 true counts and updated size/allocation ceilings; obsolete queued tokens and same-W command/Close; 100 repeats and teardown release; no unbounded metadata/history/per-rejection allocation growth.
T16 regression gate: unchanged all31 §9 families, Q1/Q2 including12 Q2 storage faults, R1–R3, B1–B3, QA-D1/D2,63 H1 variants, H2, decoder/continuity/DataHealth/publication/WAL/CLI and affine compile-fail regressions. Genuine Linux/Durable debug AND release plus canonical fmt/clippy/build/lock/clean-source checks on the NEW immutable implementation head. Supplemental Written/Flushed/Windows tests do not replace that gate.

### 15A.8 Decision provenance and remaining gates

This decision's input is the owner-attached ARCHITECTURE-REC-001D-d85fa687.zip, SHA256 a458a415a5977ce333c697c2774a563e1b8650f3736930471ade0e34075e5658. All seven manifest payload sizes/hashes were verified. The complete attached ADR and handoff were matched byte-for-byte to their exact GitHub d85 blobs: ADR 53ddb7e7d49ea947630d98c62b76a8c40306781c; handoff 5717c49a903fb95f59d7b8ae43d015371bc21863. Their SHA256 values are respectively fef81a45be7ae0c52770b54cd6eaeddd8fdc13726d55acb2568eeebe02810c5c and da9920df5fc685a9ae66478d64816f6ce77b6a201cb6d8b1abf0b578f2fd5008.

Live commit/tree/parent, actual main and open/Draft/unmerged PR34 were independently checked. The proposal-parent→d85 comparison has one commit and exactly ADR/handoff paths. The supplied exact-docs-proposal.patch (SHA25645528a97de5bd82150d4d447602b686bde179715fe3cc8f3e345fae9b8105719) was applied to verified parent document copies and reproduced both exact d85 blobs. §8 table4919B SHA256784f9e20d61806872deba9d305023660cbf471784bac48bd36677d46eae872af and §9 table11593B/31 rows SHA2567f450da4b09d80a453c2b08344b2d20b8b1023bd406c3f8bc75cee037f4afa84 were recomputed from the reviewed ADR; earlier equality attestations remain identifiable Integrator evidence. Accepted main AGENTS/WORKFLOW/INVARIANTS/ARCHITECTURE, ADR0002, WAL and DataHealth specs and exact d85 domain/supervisor/recording sources informed this review. SOURCE-PROVENANCE.json records retrieved refs, Git blob IDs and SHA256 hashes.

Review method: source/contract analysis with independent FIFO/API, lifecycle/Close and bounds subreviews. No Rust tests, reproduction or production changes were performed. d85 debug425 PASS/release NOT_RUN and residual Down/Close status are supplied Integrator/worker evidence, not new Architecture QA. ENV-01 Linux/Durable release, fresh independent implementation QA, Integrator acceptance, owner merge and post-merge CI remain separate gates.

Existing lineage only: Issue20 / parent5 / claim6024304772 / recovery6025249885 / feat/REC-001D-ws-supervisor / DraftPR34. Worker and Integrator MUST preserve this exact decision ID/addendum hash in ADR/handoff and durable decision records, copy the complete accepted text rather than a summary, and identify the later implementation head independently.

## 18. Bounded Timer A implementation continuation — 2026-10-08

Decision ARCH-REC-001D-TIMER-A-20261008 remains DESIGN_APPROVED_A /
APPROVED_FOR_BOUNDED_IMPLEMENTATION. The complete 33986-byte normative addendum
above is unchanged, SHA256
cf585a92e3bd8372caf90326c67436274fef5abb5a8d163b389aa0a42ab838ba.
Approval/provenance commit982a23d14e3bbc41bb51a7b213fe4e900aaf9e09,
tree8a42f3075bb882c50e551c1c9d81fe4740f9e1e2, precedes the implementation.
Its parent8e4e2b49930d38e16d5b48bb165ee0d8d69dd690 and earlier
18937c96de95648ba5254203be6f0c2e274da5dd Down/Close work remain ancestors.
The implementation head containing this section is identified by its full SHA
and tree in the existing Issue20/PR34 records and accompanying exact-head report;
the approval SHA must never be used as implementation runtime evidence.

The authority now selects Timer kind, identity, disposition and frozen receipt
stages. Fixed SupervisorV2 policy revision2 owns 30-second Ping and 15-second Pong
deadlines, checked generation/TimerId/actual RecordAdmissionOrder, and bounded
per-scope scheduling. Due admission returns an opaque owner or the original queued
identity; legacy Timer admission and generic SendText("ping") cannot manufacture
entitlement. Actual admitted record-stage FIFO and original common/Timer fields
are checked before selection, arithmetic, abandonment reconciliation or sink I/O.
Authenticated controls cannot pass an earlier required original receipt; frozen
Timeout additionally prevents intervening schedule/epoch replacement.

An exact active Ping receipt atomically commits its original-stamp deadline and
reserved same-W affine command. One-shot extraction and dispatch revalidation
preserve revocation by control, Down, Closing, epoch change or storage stop.
An obsolete Timer records only its original Timer, without active arithmetic or
effects. An active Timeout reserves its original same-W mandatory Close before
Timer I/O. Close remains unready until the original Down, StorageStopped, or the
existing genuine scoped terminal fail-safe. All conversion/reclaim/dispatch paths
check readiness; Timer then Down receipts and the original Close settlement gate
completion and fresh H1 Connection/Subscription/Book stages. Only a settled Close
from a strictly older epoch of the same scope may be retired for the next genuine
Timeout cycle; current/live/future/foreign owners retain their rejection behavior.

Original Down/Close, Raw/GAP and Q2 obligations remain counted after Drop and
preserving rejection. Diagnostic closure counts unfinished record stages rather
than receipt-settled W retained only by its Close/command; descriptor-closed
reporting and the same mandatory Close remain truthful. Pure identity/order/stage
and epoch-proof rejection does not reconcile unrelated abandonment or invoke the
writer. Actual backend errors, mismatch or weak gate enter StorageStopped with
the original error and distinct trusted/physical prefix. Closed/stopped authority
cannot revive an epoch. Recording counts actual sink persist invocations for the
zero-I/O/fault evidence, with its fixed Rc/Cell storage included in metadata bounds.

Only seven source/test files and ADR/handoff are changed from the approval head.
Accepted WAL schema, decoder and DataHealth contracts, dependencies, lockfile,
toolchain and workflows are unchanged. Original §8 and all31 §9 families are
preserved. Fixed metadata uses W+N+1<=M; no per-event history or unbounded cache is
introduced. Updated requested-layout measurements include authority, WorkCell,
Close and retained affine Ping storage; the conservative decoder scratch bound
remains applicable and is not represented as process RSS.

Worker status is IMPLEMENTED / QA_PENDING / PARTIAL_VERIFICATION. T01–T16 mappings,
source fingerprints, actual command logs, cap5/cap9 measurements and original31
family requirements accompany the new head. Windows pure/Written tests and worker
peer analysis are supplemental. Genuine Linux/Durable debug and release, fresh
independent QA, Integrator acceptance, READY and owner merge remain separate
gates. Owner-confirmed Docker desktop-linux/WSL2/Linux amd64 Engine29.3.1 is
available, but this session's granular policy rejects require_escalated before
process launch (sandbox_approval=false); this is a worker execution-policy blocker,
not an unavailable owner executor. No Release PASS is inferred. The prepared
exact-head runner uses the named-pipe context, a clean rust:1.98.1-bookworm
temporary container, bash -c, temporary rustfmt/clippy and full stdout/stderr,
command exits, Rust/Cargo versions and before/after clean-source provenance.

The first implementation headdd2dd52f89f155bb447a2c5fb686a2f190a87c7d was tested
by [Linux CI37755315999](https://github.com/al-gri/pro-sclpng/actions/runs/37755315999).
Fmt/Clippy/build passed; tests stopped at the unchanged supervisor architecture
guard because newly added private fixtures directly read files. The bounded
correction retains the guard, uses a trusted in-memory private fixture, and adds
two public canonical integration tests with physical-prefix and real retry
evidence. Production supervisor code is unchanged by this correction. Private
profile labels are not physical durability evidence. The failed job skipped its
final clean-source check; complete failed-head logs remain distinct from any
later corrected-head verification.
