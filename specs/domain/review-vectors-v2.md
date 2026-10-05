# SPEC-001 revision 2 — named review vectors

Status: **PROPOSED**, DESIGN_REVIEW_REQUIRED. All traces are origin=synthetic, schema_version=1, proposal_revision=2. These are exact EXPECTED outcomes for design review, **NOT executed Rust tests or an implemented model**.
Contracts: [events](../market-data/events-v1.md), [health](../market-data/data-health-v1.md), [WAL](../recording/wal-v1.md), [artifacts](artifacts-v1.md), [matrix](test-matrix-v1.md), [ADR](../../docs/adr/0002-domain-event-wal-contracts.md).

## 1. Notation and explicit initial states

Each row/trace is an independent fixture; it does not mutate the next row's archive. `A=16 bytes 01`, session `S=16 bytes 02`, ClockId1. Stream1 owns Normal Book1 of slot1 InstrumentRef(SYN,Spot,spot,ABCUSD), Connection1. All ordinary versions/epochs=1 unless changed explicitly. R=Healthy unless specified. `tag1=(spec1,connection1,subscription1,book1)` means epoch values, not owner IDs.

P1: config1/norm1/profile1, UnknownOnSilence, deadline100ns, warmup_min_updates=1, warmup_min_elapsed=0, quiet=false, two_sided=true, gateDurable, pending_max_frames=2/raw_bytes=256/outputs=4/wait=50ns, quiet_max_lifetime=None. This matches AF-C1. No constants are real-feed parameters. P2 differs only in min_updates=2 and is a separately supplied synthetic configuration in its independent fixture. Q policy uses deadline=None, quiet=true,max_lifetime=10; D10 policy uses StaleAfterDeadline,deadline10. Variant fixtures must supply their matching immutable config descriptors; they are not rebinding one artifact inside a single archive.

Initial I: validated recorded prefix through r9, evaluation=9; stream registered earlier, transport Up, SourceGap at r9 established barrier9. B=Invalid(SourceGap),F=Unknown,anchor=None,progress0,witness=None,pending=[],last_data=None,last_applied_raw=None,usable_data=false. The abstract prefix is a supplied precondition, not claimed complete WAL golden bytes. Its current policy/profile/artifacts are resolved by an explicit synthetic resolver. Raw attempts in all non-accounting traces are consecutive; complete two-sided snapshot has bid2000/qty1,ask2002/qty1. Each delta output has one valid SetLevel unless specified. Source samples equal raw RecordNo and proof/timer samples equal their record number unless a t value is printed. Thus none of the ordinary r10..30 traces accidentally expires P1 freshness/pending limits.

`K(r,j,n) = ((A,r),j,n)` is the source candidate/application key. `E(r,j,n)=(A,r,j,n)` is the EFFECT EventId. `C(r,j)=(r,j)` is its EventCursor AND available_at. Every emitted E at step r has as_of.record_frontier=r; its source key is specified below. `D(r,code)` is a diagnostic attached to RecordRef(A,r), not a market EventId. `L` denotes last emitted market cursor; initially None. All zero-output steps have emitted IDs=[], cursors=[], available_at=[]; L is retained, NEVER assigned a cursor merely for a control/diagnostic. State's processed InputCursor still advances to the valid record; invalid semantic archive records stop before that record as stated.

Unmentioned state components are exactly retained; all traces not involving publication have publication_permit=false (no final fence). Every vector asserts the printed full initial state plus exact changes, error/diagnostic, emitted source/effect IDs and cursor/availability convention. No vector means merely 'is_err' or 'did not panic'.

## 2. A1 / R1 — delayed frame application

### V-R1-BARRIER and V-R1-RESYNC

Start I/P1. r10 RawSnapshot -> pending=[10], B remains Invalid(SourceGap), no effects. r11 QueueOverflow GAP(range=None,count=None) -> barrier11,B=Invalid(QueueOverflow),F=Unknown,anchor=None,progress0,witness=None,pending=[],last_data=None; R=Degraded. r12 Proof(raw10) -> D(12,PRE_BARRIER); no effects, L=None; all scope state unchanged. It cannot become a new resync merely because proof12 is later.
Continue: r13 new current RawSnapshot (closes the local loss window, next consecutive local attempt; recorded unknown count remains None); r14 applicable frame Proof(raw13) -> source K(13,0,1), effect E(14,0,1), C(14,0), B=Warming,anchor13,progress0,last_data13,F=Fresh,pending=[],barrier11. r15 RawDelta; r16 Proof(raw15) -> K(15,0,1),E(16,0,1),C(16,0),progress1,last_data15,B=Warming,F=Fresh. r17 WarmupEvidence(anchor13,count1,elapsed4,observed17,barrier11) -> B=Usable,witness17,usable_data=true,L=(16,0); emits no market event. R remains Degraded, so no publication candidate/permit. Resync and recorder recovery are separate guards.

### V-R1-REORDER

Start I/P1. r10 Snapshot; r11 Delta; r12 Proof(raw11) -> pending=[10 unready,11 ready], no effects,L=None,B=Invalid(SourceGap). r13 Proof(raw10) -> release in source order:

| Source key | Effect ID | Cursor = available_at | as_of frontier |
|---|---|---|---:|
| K(10,0,1) | E(13,0,1) | (13,0) | 13 |
| K(11,0,1) | E(13,1,1) | (13,1) | 13 |

Final: pending=[],barrier9,anchor10,B=Warming,F=Fresh,progress1,last_data11,last_applied_raw11,L=(13,1),usable_data=false. r14 truthful Warmup(count1,elapsed4) -> Usable,witness14,usable_data=true, no additional EventId. Proof receipt12 did not create effect12 or reverse the source order. Both effects retain original samples10/11, not13.

### V-R1-MULTI-DUP and V-R1-CONFLICT

Start recovered current P2 with anchor10, B=Warming,progress0,last_data10,L=(11,0),barrier9 and no pending. r15 RawDelta with exactly two BookUpdate outputs, source K(15,0,1),K(15,1,1). r16 full-frame proof -> E(16,0,1),E(16,1,1), available_at(16,0),(16,1),cf16; progress2,last_data15,last_applied_raw15,B=Warming,F=Fresh,L=(16,1). r17 equivalent proof of raw15 (different WAL RecordNo, SAME semantic descriptor) -> D(17,ALREADY_APPLIED),IDs=[],progress2,last_data15,L=(16,1),no anchor/freshness renewal. It is not a third update.
Independent conflict variant: r17 has a resolved but contradictory ordered-output commitment for the same current raw -> D(17,ProofConflict),B=Invalid(ProofConflict),F=Unknown,barrier17,anchor=None,progress0,witness=None,pending=[],last_data=None,usable=false. No new effects; previously emitted IDs remain in history. Compare semantic commitment, not proof RecordNo.

### V-R1-PENDING-DUP

I/P1, r10 Snapshot,r11 Delta,r12 Proof(raw11),r13 equivalent Proof(raw11): D(13,ALREADY_VERIFIED),pending=[10 unready,11 ready],no effects,L=None,anchor=None,usable=false. One pending frame's duplicate proof cannot release past raw10.

### V-R1-MIXED and V-R1-ATOMIC

Mixed: I/P1, r10 raw normalizes to Snapshot+Update -> D(10,MixedFrameUnsupported),barrier10,B=Invalid(MixedFrameUnsupported),F=Unknown,pending=[],anchor=None,progress0,IDs=[],L=None. No partial snapshot success.
Atomic: current Warming anchor10, r15 Delta has two outputs, first valid and second repeats(side,price) within its entries. Structural normalization rejects the entire raw at15 as DuplicateLevel; barrier15,B=Invalid(DuplicateLevel),F=Unknown,anchor=None,progress0,IDs=[],previous L retained. No update from the first output is applied.

### V-R1-PENDING-BOUNDS

Separate parameterized traces start I/P1. Count case: r10/r11 each one small pending snapshot/delta; r12 third raw exceeds max_frames2 => D(12,PendingOverflow),barrier12,B=Invalid(PendingOverflow),F=Unknown,pending=[],anchor=None,IDs=[],L=None. Byte case: first r10 raw_len257 exceeds256 => same result at10. Output case: r10 valid homogeneous delta with5 outputs exceeds cap4 => same result at10. Exact exceeded field is frames/raw_bytes/outputs respectively; no silent coalescing. The synthetic normalizer validates the structure before the declared resource test.

### V-R1-PENDING-DEADLINE

I/P1,r10 Snapshot sample10 gives deadline60. r11 Timer(t59,deadline59): pending=[10],B unchanged,IDs=[]. r12 Timer(t60,deadline60): D(12,PendingTimeout),barrier12,B=Invalid(PendingTimeout),F=Unknown,pending=[],anchor=None,L=None. Equality is expired. Independent variant r12 is Proof(raw10) with Context t60: expiry runs first; diagnostics [PendingTimeout,PRE_BARRIER], no effects. A raw sample u64::MAX-10 with wait50 produces PendingDeadlineOverflow rather than wrapped deadline.

### V-R1-DOWN-UP and V-R1-CONTEXT

Down/Up: I/P1,r10 Snapshot,r11 TransportDown,r12 Up,r13 Proof(raw10) -> T=Up,B=Invalid(TransportDown),F=Unknown,barrier11,anchor=None,pending=[],progress0,IDs=[],L=None,D(13,PRE_BARRIER). Up did not remove the barrier.
Context: I/P1,r10 Snapshot,r11 valid Config2/Norm1 (old Context1/1),r12 old Proof(raw10) in active Context2/1 -> D(12,PRE_BARRIER),barrier11,B=Invalid(ContextChanged),anchor=None,pending=[],no effects. A post-barrier current snapshot13/proof14 can create E(14,0,1) with config2; a further old proof15(raw10) leaves anchor13/Warming untouched and emits nothing.

### V-R1-OLD-SAMPLE and V-R1-EXPIRED-PROOF

Old sample: I/P1,r10 admitted Snapshot with ORIGINAL sample8,proof11 descriptor bound to barrier9/not_before9 -> scope guard fails; D(11,ProofConflict:EvidenceScopeMismatch),barrier11,B=Invalid(ProofConflict),F=Unknown,anchor=None,no effects. Admission after barrier is not proof of new resync.
Expiry: I/P1,r10 Snapshot,r11 Delta; r12 Proof(raw11,valid_until13) stored ready; r13 Proof(raw10) releases valid snapshot10 as E(13,0,1) then encounters expired proof11 at equality. D(13,ProofExpired); failed delta emits nothing; B=Invalid(ProofExpired),barrier13,F=Unknown,anchor=None,pending=[],L=(13,0). Snapshot effect remains historical, but no publication of invalid final state. There is no partial failed-frame effect.

## 3. A2 / R2 — recording and final fence

Baseline P: current usable stream1,T=Up,F=Fresh,B=Usable,anchor10,witness19,barrier9,last_data19; modeSyncBeforePublish,gateDurable,R=Healthy,known contiguous accepted/appended/written/flushed/durable through20. No invalidation while waiting unless specified. At r21 valid RecordingEvidence(Durable,through20,Healthy), evaluation21, candidate **PC=(A,21,1)** is frozen with causal_frontier21,available_at=InputCursor(A,21); market effect IDs=[], market L unchanged. State is usable, but publication_permit=false until a valid final fence. No EventCursor is invented for PC or fence.

| ID | Input / changed precondition | Exact expected |
|---|---|---|
| V-R2-MODE-GATE | All nine cells in health section2 | Buffered×Written/Flushed/Durable: valid; GroupSynced×Written/Flushed and SyncBeforePublish×Written/Flushed: INVALID_CONFIGURATION before activation; GroupSynced×Durable,SyncBeforePublish×Durable valid. Invalid cells retain old config/state, no PC/effects, semantic cursor before definition |
| V-R2-FINITE | Baseline P at21; actual writer reaches Durable21; final fence(A,S,Durable,21) | permit=true ONLY for PC(A,21,1); PC content/ID/frontier/availability unchanged; no new RecordNo/effect/evaluation advance or automatic ack |
| V-R2-NO-FENCE | receipt21 acknowledges20, no final fence | PC exists, permit=false, FenceMissing; usable_data remains true |
| V-R2-BEHIND | fence through20 while PC.frontier21 | permit=false,FenceBehindCandidate; no mutation of PC/market state |
| V-R2-WEAK | fence Written21, configured Durable | permit=false,FenceTooWeak; not Durable by CRC/readback |
| V-R2-SCOPE | fence archive=16x03 OR session=16x04 | each permit=false,FenceScopeMismatch; no scope rebinding |
| V-R2-FUTURE | accepted/achieved prefix21, fence through22 | permit=false,FenceBeyondAchieved; no frontier promotion |
| V-R2-UNKNOWN | no known achieved Durable frontier, written21 only, claimed Durable fence | permit=false,UnverifiedStorageCompletion; None is not zero/success |
| V-R2-SELF | r21 RecordingEvidence through21 | InvalidAcknowledgement; semantic receipt not accepted, no PC21/effects; prefix stops20 |
| V-R2-FUTURE-ACK | r21 RecordingEvidence through22 | same InvalidAcknowledgement and prefix20 |
| V-R2-NONE-ACK | r21 Healthy RecordingEvidence throughNone | MissingWatermark; no success/PC21; prefix20 |
| V-R2-REGRESSION | known Durable20, r21 claims Durable19 | WatermarkRegression; semantic prefix20; no PC21/permit |
| V-R2-ORDER | claimed Durable20 but known accepted prefix19, receipt record21 in supplied invalid trace | WatermarkOrderError; no permit or inferred contiguous prefix |
| V-R2-REVOKED | after PC21, r22 GAP/current TransportDown/recording Failed (separate mutations), then valid fence21 | PC revoked; CandidateRevoked,permit=false; GAP/Down resets dependent B/F/barrier22; recording fault sets R=Failed; fence cannot restore them |
| V-R2-SUPERSEDED | after PC21, relevant recorded Timer22 changes evaluation-based state; recomputed PC22 remains usable | PC21 superseded, old fence21 cannot release PC22; new PC=(A,22,1),cf22,available InputCursor22; FenceBehindCandidate until actual gate22 |
| V-R2-CONFIG-DOWNGRADE | modeGroupSynced at21; next Config with gateWritten | INVALID_CONFIGURATION, no activation/candidate; immutable mode retained |

Fence errors are diagnostics of the pure publication guard, NOT new WAL records. They do not advance any market cursor/time. The table names the proposed diagnostic vocabulary; no storage completion producer exists here. An accepted receipt is included in causal replay; the final fence is not an analytical input. Physical delivery after a crash remains Unknown.

## 4. R3 — exact freshness and quiet boundaries

Baseline F: B=Usable,anchor2,witness4,barrier1,T=Up,R=Healthy,last_data sample5,recorded evaluation5,L=(5,0),no pending. All timers have later valid RecordNo and the printed Context sample. They emit no market effects; original sample5 and L stay fixed. Unless changed, D10 policy applies; no final fence is supplied.

| ID | Recorded evaluations / proof | Expected F and diagnostic |
|---|---|---|
| V-R3-FRESH-BOUNDARY | 14,15,16 with sample5,D10 | Fresh at14; Stale at15 and16; equality expired; no disconnect or B reset |
| V-R3-UNKNOWN-BOUNDARY | same with UnknownOnSilence,D10 | Fresh at14; Unknown at15/16 |
| V-R3-NONE | UnknownOnSilence,D=None at5 then100 | Unknown at BOTH, not permanent Fresh; usable=false despite B=Usable |
| V-R3-LATE | raw sample5 released by proof at evaluation20,D10 | structural application allowed; F=Stale immediately, effect availability=current proof cursor, original sample5, not expiry30 |
| V-R3-OVERFLOW | sample=u64::MAX-5,D10 | FreshnessDeadlineOverflow,F=Unknown; no wrapping expiry |
| V-R3-CLOCK | same numeric sample but evidence ClockId2 | IncomparableClock,F=Unknown,usable=false; original data not relabeled as Clock1 |
| V-R3-QUIET | Q policy; quiet observation5,from5,until20,maxlife10; applied at evaluation14 | effective_expiry15,F=QuietVerified; timer15 and16 ->Unknown, B/anchor unchanged |
| V-R3-QUIET-LATE | same quiet object first arrives at15 | QuietExpired,F=Unknown; no lifetime extension from arrival |
| V-R3-QUIET-FUTURE | Q,obs5,from10,until20, evaluation9 | QuietNotYetValid,F=Unknown; timer10 alone does NOT activate saved artifact; a new recorded proof at10 may yield QuietVerified until15 |
| V-R3-QUIET-NO-BOUND | Q quiet proof missing from or until, or until<=from | InvalidQuietBounds,F=Unknown; no infinite quiet guarantee |
| V-R3-QUIET-OLD | Q old anchor/barrier or old config proof after reset | PRE_BARRIER/OBSOLETE_SCOPE according to referenced raw/barrier; F of recovered current scope unchanged; no old quiet witness installed |
| V-R3-QUIET-DISALLOWED | P1 allowquiet=false with otherwise valid quiet object | QuietPolicyDenied; F remains ordinary rule result, not QuietVerified |
| V-R3-FALSE-FRESH | FreshnessEvidence Fresh at evaluation15,basis sample5,D10 | FreshnessAssertionMismatch,F=Stale; proof does not rejuvenate sample |

For QUIET-OLD, concrete parameterization: old anchor raw2/barrier1, reset at r10, new anchor raw11/proof12, current Q sample11/FUnknown; proof13 points raw2 => PRE_BARRIER, anchor11 retained and no IDs. Same current raw11 with old config1 after activation config2 at10 => OBSOLETE_SCOPE; no current witness installed. Full clock/context/anchor bindings are required independently of a syntactically valid digest.

## 5. A3 / R4 — artifact identity and activation

V-R4-TIMELINE: independent valid prefix establishes a trade stream3 whose synthetic feed profile explicitly supports Norm1 AND Norm2. Config1/Norm1 active. r20 Config2/Norm1 uses Context(1,1), activates for21; r21 raw trade emits K(21,0,1)->E(21,0,1),cursor/available_at(21,0),cf21,config2. r30 Config3/Norm2 uses Context(2,1), activates for31; r31 raw trade emits K(31,0,2)->E(31,0,2),cursor/available_at(31,0),cf31,config3. Definitions20/30 have no market EventId/cursor. `(21,0)<(31,0)` is true in canonical A. Prior-effect reference from31 to(21,0) valid; reverse/future reference is FutureCausalReference. Book dependents of each activation clear pending/anchor and set barrier20/30; historical IDs survive for audit, not readiness transfer.

| ID | Mutation / input | Exact expected |
|---|---|---|
| V-R4-SAME-NORM | Config2 repeats exact Norm1 descriptor ref | valid reference, no ArtifactIdentityConflict; new config activation still invalidates dependent book scope |
| V-R4-REBIND | same namespace key normalizer/revision1, other descriptor/body digest | ArtifactIdentityConflict; Blocked before outputs; no E under old identity with new meaning |
| V-R4-EARLY | config2/Norm2/proof artifacts all supplied offline before their WAL reference | no activation/application until ConfigDefinition/VerificationEvidence; IDs=[],availability=[] for artifact discovery alone |
| V-R4-BOOTSTRAP | first ConfigDefinition Context(0,0),valid Config1/Norm1 | BootstrapContext accepted; next input Context(1,1); no ordinary version0 or EventId with norm0 |
| V-R4-BOOTSTRAP-INVALID | Context(0,1),(1,0),or raw/control/gap Context(0,0) | InvalidBootstrapContext; semantic prefix before record; no IDs |
| V-R4-STALE-CONTEXT | Config2 activates at20; raw21 still Context(1,1) | ContextMismatch; not canonical data under old config; no E21 |
| V-R4-PROFILE-NORM | select Norm2 but profile lists only AF-N1 | UnsupportedNormalizerBinding,Blocked; no guessed profile support |
| V-R4-PARSE-ONLY | well-formed ArtifactRef, no verifier/bytes | ArtifactUnverified,Blocked; ParsedRef is not Verified |
| V-R4-MISSING | required AF-V1 or dependency absent locally | MissingArtifact,Blocked; no latest/network fallback; no effect/market cursor |
| V-R4-DIGEST | flip one descriptor or body byte under same claimed ref | ArtifactDigestMismatch; if byte length also altered check length first for body; no output/availability |
| V-R4-LENGTH | body has actual length differing from descriptor | ArtifactLengthMismatch before typed interpretation |
| V-R4-KIND-SCHEMA | verification token resolves to Config, or descriptor/body schema unsupported | ArtifactKindMismatch / UnsupportedArtifactSchema respectively; Blocked |
| V-R4-GRAMMAR | uppercase digest, wrong prefix,63/65 hex digits,whitespace/nonhex | InvalidArtifactRef, no resolution attempt |
| V-R4-CLOSURE | missing dependency,cycle,duplicate/unsorted refs or cap overflow | MissingArtifact / ArtifactDependencyCycle / InvalidArtifactDependencies / ArtifactTooLarge respectively; no partial canonical success |

[AF fixtures](../../tests/fixtures/domain/artifacts-v1.md) supply exact descriptor/body lengths/refs; they are not complete archive golden bytes. A digest-valid artifact with wrong scope still fails the applicable proof guard. The fixture resolver cannot confer real SHA computation or Bitget verification on production code.

## 6. R5 — one-use local loss accounting

Every input below has current tag1 unless changed. Independent initial accounting f=0,window=None; market T=Up,R=Healthy,B=NoSnapshot,F=Unknown,barrier=registration,anchor=None,no emitted IDs/L=None. Raw source bytes may be nonmarket diagnostic-only, so accounting examples cannot accidentally create a verified book. On each valid current Gap set B=Invalid(reason),F=Unknown,barrier=that Gap record. QueueOverflow sets R=Degraded. These transitions persist unless explicitly superseded. Accounting errors stop before the bad record; last_good_offset is its start in any future binary fixture, NOT a guessed numeric byte offset here. These are logical traces, not complete encoded WAL.

| ID | Record sequence | Accounting / exact result |
|---|---|---|
| V-R5-ONE-USE | r10 raw attempt1; r11 UnknownLocalGap(None,None); r12 raw3; r13 raw100 | after12 f3,windowNone,inferred interval[2,2],recorded count remainsNone;13 UnaccountedAttemptGap,semantic RecordNo12,barrier11/BInvalid(QueueOverflow),no effects |
| V-R5-NO-JUMP-CONSUMES | raw1;UnknownLocalGap;raw2;raw4 | window consumed at2 with inferred k0 but recorded countNone retained;4 UnaccountedAttemptGap,f2; unknown permit not carried forward |
| V-R5-INITIAL-MISSING | first raw attempt3, no gap | UnaccountedAttemptGap,f0,windowNone,semantic cursor before raw,B/F unchanged |
| V-R5-INITIAL-UNKNOWN | initial UnknownLocalGap;first raw3 | f3,windowNone,inferred missing[1,2],recorded countNone;Gap semantics remain visible |
| V-R5-KNOWN | raw1;KnownLocalGap[2,3],count2;raw4 | after Gap f3;after raw f4,windowNone;loss exactly[2,3],not raw4 |
| V-R5-OVERLAP-RAW | raw1;KnownLocalGap[1,2],count2 | LossOverlap,f1;bad Gap not accepted,raw1 not counted lost |
| V-R5-OVERLAP-LOSS | raw1;KnownGap[2,3]count2;KnownGap[3,4]count2 | second Gap LossOverlap,f3;no double loss |
| V-R5-COVERAGE | raw1;KnownGap[3,4]count2 | LossCoverageGap,f1;missing2 not silently explained |
| V-R5-KNOWN-COUNT | KnownGap[2,3]count1 after raw1 | LossCountMismatch,f1;whole bad record rejected |
| V-R5-UNKNOWN-COUNT | raw1;UnknownLocalGap countSome2;raw3 | LossCountMismatch at raw3 (inferred1 !=2);f1,window still references preceding gap in retained valid prefix |
| V-R5-COUNT-ZERO | local loss_countSome0 | InvalidLossCount;no accounting/state transition from bad Gap |
| V-R5-SOURCE | raw1;SourceGap(None,None);raw3 | SourceGap invalidates B/barrier but creates NO local window;raw3 UnaccountedAttemptGap,f1 |
| V-R5-SOURCE-RANGE | SourceGap with local attempt range or countSome | InvalidLossScope;cannot assert local coverage from exchange gap |
| V-R5-SECOND-UNKNOWN | raw1;UnknownLocalGap;second local unknown/known Gap before raw | AmbiguousLossWindow;retain first window/f1 in prefix;no merged permit |
| V-R5-EPOCH | raw1;valid epoch advance;new-tag raw2 | f2 (not reset),windowNone;health new generation NoSnapshot;later raw1 AttemptOrderError |
| V-R5-OPEN-EPOCH | raw1;UnknownLocalGap;relevant epoch/config/spec change before next raw | GapScopeTransition,semantic cursor before change,f1/open window retained;no old-tag permit applied to new generation |
| V-R5-UNRESOLVED | unknown local window still open at final seals/EOF | UnresolvedLossWindow diagnostic;qualityUnknown required;physical complete seal chain may be Complete,never NoKnownLoss/GapsRecorded accounting claim;no usable book |
| V-R5-EXHAUSTED | previously accounted f=u64::MAX,another attempt | AttemptCounterExhausted, no wrap0; no suffix accepted |

Reason restrictions and simple one-window rejection are explicit proposed v1 limits, not claims that arbitrary multiple-gap workloads are supported. Known/unknown local counts do not define exchange lost-message counts.

## 7. R6 / C1 / C2 — shared owners and bounded progress

V-R6-SHARED: initial stream A=1/Book1 on Connection1 epoch1,TUp,BUsable,FFresh,anchor10,progress1,frozen witness14,barrier2,L=(13,0); stream C=3/another instrument/Book3 on Connection2 epoch1 is independently Usable/Fresh with its own anchor/witness. All evaluation samples are held at20 so registration does not cause unrelated freshness expiry. r21 registers stream B=2/RPI Book2 on existing Connection1 epoch1. Expected: shared T remainsUp; A and C state/IDs/L unchanged; B FUnknown,BNoSnapshot,barrier21,anchorNone,pending[],progress0. No market effects/cursors at21. B cannot borrow A's witness.

V-R6-DOWN-FANOUT continues: r22 Connection1 Down -> A/B TDown,FUnknown,BInvalid(TransportDown),barrier22,anchorNone/progress0/pending[]; C unchanged. r23 Connection1 advance1->2 -> shared new TUnknown; A/B BNoSnapshot,FUnknown,barrier23,connection_epoch2,subscription/book epochs unchanged; C still Connection2 epoch1/usable. r24 old-connection proof is OBSOLETE_SCOPE or PRE_BARRIER when raw<=23, no effect or restoration. No market EventIds from any administrative step.

V-C1-WRITER: register another StreamId4 for already bound Normal Book1 -> WriterRebindRequiresNewArchive, no partial registration/T reset/epoch mutation; semantic prefix before definition. Same StreamId with explicit subscription EpochAdvance is allowed, but resets ONLY its dependent continuity/barrier and not another stream's transport.

V-C2-PROGRESS: Warming,min_updates2,progress1,anchor10; one applied frame with two BookUpdate outputs at r16 emits E(16,0,1),E(16,1,1),cursors/availability(16,0),(16,1),cf16; final progress2,not3. Warmup17 with truthful count2/elapsed7 -> Usable,witness17. Further applied updates18..20 keep progress2 and witness17; last_data follows original applied samples,not the witness. Equivalent proof adds0.
V-C2-MAX: min_updates=u32::MAX,progress=MAX-1,Warming,valid anchor/witness prerequisites; two applied BookUpdate outputs in one verified frame -> progressMAX,second output does not increment it further. After truthful warm-up witness, subsequent updates keepMAX without overflow/readiness loss. Operational total-update statistics are not this counter. These boundary states are supplied model inputs, not a claim to have executed billions of updates.

## 8. Finding-to-change mapping and remaining review

Historical revision2 submission mapping follows. [Integrator review5417294202](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5417294202) subsequently marked R1–R6/C1/C2 RESOLVED at design level, not as executed Rust tests. Those semantics and vectors are unchanged in this targeted correction. Current V2-WIRE-01/02 and V2-DOC-01 remain SUBMITTED_FOR_REVIEW in section9; the worker does not close them.

| Finding | Changed sections | Named vectors | What remains unresolved / unproved |
|---|---|---|---|
| A1 / R1 | events1–4;health1,3–4;WAL3–4;artifacts4;ADR A1 | V-R1-BARRIER/RESYNC/REORDER/MULTI-DUP/CONFLICT/PENDING-DUP/MIXED/ATOMIC/PENDING-BOUNDS/PENDING-DEADLINE/DOWN-UP/CONTEXT/OLD-SAMPLE/EXPIRED-PROOF | Approval of exact projection/as_of, resource/frontier and semantic-equivalence rules; no runtime assertions yet; real source resync MD-001 |
| A2 / R2 | health2,5;WAL4 Config/RecordingEvidence,6;ADR A2 | all V-R2-* including nine mode/gate cells | Pure permit/candidate diagnostic vocabulary and storage-boundary agreement need review; actual fence provenance/OS durability REC-001 |
| R3 | health2–4;events3,7;artifacts4;WAL Control | all V-R3-* | Quiet proof applicability and exact diagnostic taxonomy await review; real policy values MD-001 |
| A3 / R4 | events1–2,6;types2–3;WAL1,3–4;artifacts1–5;ADR A3 | all V-R4-*;AF-N1/B1/C1/F1/V1 | Exact descriptor/bundle/PSCO formats, caps and supported interpreter integration not accepted; production verifier absent |
| R5 | WAL4 Gap,4.1,5,ArchiveSeal;events4;ADR R5 | all V-R5-* | Conservative one-open-window and block-on-scope-change limits require acceptance; no queue/recovery implementation |
| R6 | health1,4;events4;types1;WAL StreamDefinition | V-R6-SHARED/DOWN-FANOUT | Fan-out model execution NOT_RUN; no supervisor |
| C1 | types1;health1;WAL StreamDefinition;ADR D4 | V-C1-WRITER | No same-archive writer rebind, subject to renewed review |
| C2 | events3;health3–4;WAL WarmupEvidence;ADR | V-C2-PROGRESS/MAX plus V-R1-MULTI-DUP | No runtime boundary assertions yet; diagnostic statistics explicitly out of readiness |
| C3 | matrix retained baseline plus this exact outcome catalog | N/E/H/W baseline;all named vectors here | Independent multi-frame/multi-segment golden, all byte cuts and full Rust assertions still required after approval |

This catalog supplies exact logical state/cursor/ID expectations; byte offsets for not-yet-encoded full traces are intentionally NOT fabricated. Descriptor bytes are independently frozen in AF fixtures; W01 alone is not a full replay/recovery golden. Renewed review must be SHA-bound in the SAME Draft PR #10 before dependent implementation.

## 9. Targeted wire and link vectors

Scope: V2-WIRE-01, V2-WIRE-02 and V2-DOC-01 only, based on review5417294202 at `8758b3c2a8896146a14d397bd65dc18ac5a49415`. Status PROPOSED / SUBMITTED_FOR_REVIEW. These documentary vectors add no codec, schema or function. A1–A3/R1–R6/C1/C2 semantics are retained. All byte offsets below are zero-based; hex dump row labels are hexadecimal, prose offsets decimal.

### V2-WIRE-GAP-ORDER

Use I/P1 after valid r10 RawSnapshot(attempt1,sample10): semantic prefix10, evaluation10, f=1,window=None,pending=[10],BInvalid(SourceGap),FUnknown,RHealthy,barrier9,anchor=None,progress0,witness=None,last_data=None,L=None. All definitions/artifacts are resolved as the synthetic precondition; their prefix bytes are NOT claimed below. This is one complete Gap frame r11 in segment0, not a complete archive.

Expected header: frame_version1,record_schema_version1,record_kind7,flags0,payload_len64,RecordNo11,SegmentNo0,reserved0. Context=(unix_ns11,monotonic_ns11,config1,norm1). Gap.scope_kind at56=1 (ExplicitTargets),reason at57=4 (QueueOverflow),count at58..59=1 LE; first Target at60: StreamId1,tag(spec1,connection_epoch1,subscription_epoch1,book_epochSome1),first/last/count all None. Target36 bytes; payload24+4+36=64; total32+64+4=100. Exact bytes:

```text
0000: 50 53 52 57 01 00 01 00 07 00 00 00 40 00 00 00
0010: 0b 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
0020: 0b 00 00 00 00 00 00 00 0b 00 00 00 00 00 00 00
0030: 01 00 00 00 01 00 00 00 01 04 01 00 01 00 00 00
0040: 01 00 00 00 01 00 00 00 00 00 00 00 01 00 00 00
0050: 00 00 00 00 01 01 00 00 00 00 00 00 00 00 00 00
0060: 26 19 f4 e5
```

CRC(header+payload)=0xE5F41926, trailer at96..99=`26 19 f4 e5`. Expected no parse/accounting error; semantic InputCursor11,evaluation11,barrier11,BInvalid(QueueOverflow),FUnknown,RDegraded(QueueOverflow),pending=[],anchor=None,progress0,witness=None,last_data=None,f1,window=(gap11,left1,tag1,countNone). TUp and ordinary versions unchanged. Market IDs/cursors/available_at=[],L=None,publication_permit=false. The administrative identity is RecordRef(A,11), not a market EventId. If frame start is O, last_good_offset becomes O+100; no absolute prefix length is fabricated. EOF here is not Complete and the open loss window remains unresolved.

### V2-WIRE-GAP-REVERSED

Same independent initial state/header/target, but swap ONLY bytes56/57 and recompute the trailer for the changed protected bytes. Exact negative frame:

```text
0000: 50 53 52 57 01 00 01 00 07 00 00 00 40 00 00 00
0010: 0b 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
0020: 0b 00 00 00 00 00 00 00 0b 00 00 00 00 00 00 00
0030: 01 00 00 00 01 00 00 00 04 01 01 00 01 00 00 00
0040: 01 00 00 00 01 00 00 00 00 00 00 00 01 00 00 00
0050: 00 00 00 00 01 01 00 00 00 00 00 00 00 00 00 00
0060: f2 eb ed 35
```

CRC=0x35EDEBF2, correct trailer=`f2 eb ed 35`; checksum validation succeeds. Prefix56..59=`04 01 01 00` would read scope4,reason1,count1, but scope4 is rejected immediately: **Unsupported(field=Gap.scope_kind,value=4,frame_offset=56)**, NOT ChecksumMismatch. No target/accounting/health transition is committed: f1/windowNone,pending[10],barrier9,BInvalid(SourceGap),FUnknown,RHealthy,anchorNone,progress0,witnessNone,last_dataNone retained; semantic prefix/evaluation stay10 and interpretation becomes Blocked(Unsupported). Market IDs/cursors/available_at=[],L=None,permit=false. last_good_offset=O, not O+100. Invalid scope is not a SourceGap or a local-loss authorization.

### Policy field-view vectors

Normative table: [DataHealth 2.1](../market-data/data-health-v1.md#21-policy-byte-tags). Each row below is an independent synthetic fixture with valid enclosing grammar, matching other fields and resolved dependencies. These are policy field views, NOT complete encoded config frames/descriptor variants. For later full mutation fixtures, frame CRC and descriptor/body lengths, hashes and refs must correspond to changed bytes so failures reach the stated policy guard; never mutate AF-C1 while keeping its old digest. The original AF-C1 remains unchanged.

Initial policy-test prefix10 is administrative with BootstrapContext, no active config and no pending/data/candidate. First ConfigDefinition r11 uses BootstrapContext and defines Config1/Norm1; required normalizer/spec/profile prerequisites are supplied as appropriate. Use Buffered mode to avoid masking tag/mirror tests with a mode/gate incompatibility. All other policy values match AF-C1, including finite deadlineSome100. A successful config applies from12, after processing11, and still creates no market event/candidate; no data resync has occurred. Invalid config leaves the active configuration absent and semantic prefix/evaluation at10, blocks interpretation, and emits no IDs/cursors/available_at or permit.

| Named vector | Field bytes / variants | Exact expected |
|---|---|---|
| V2-POLICY-SILENCE-ALL | silence_rule=01 then02 in separate matched WAL/Config body fixtures | Decode UnknownOnSilence then StaleAfterDeadline; finite deadline100 satisfies both. Config1/Norm1 active for12, semantic cursor11; no error or market effects |
| V2-POLICY-GATE-ALL | recording_gate=01,02,03 in separate matched fixtures | Decode Written,Flushed,Durable; all valid under Buffered, config active for12, semantic cursor11; no error or market effects |
| V2-POLICY-SILENCE-UNSUPPORTED | every u8 outside {1,2}, explicitly00/ff; mutate WAL only, body only, or both equally | Unsupported(field=Config.silence_rule,value=x) in offending representation, no activation. Equal unsupported bytes in both do not become valid |
| V2-POLICY-GATE-UNSUPPORTED | every u8 outside {1,2,3}, explicitly00/ff; same three mutation locations | Unsupported(field=Config.recording_gate,value=x), no activation; no Unknown/bootstrap/default coercion |
| V2-POLICY-WAL-PSAD-MATCH | all six pairs (silence,gate)=(01,01),(01,02),(01,03),(02,01),(02,02),(02,03), mirrored exactly | Both representations decode identically; Config activates for12 under Buffered. Pair notation names two separate fields, not two adjacent bytes. Original AF-C1 specifically uses (01,03) |
| V2-POLICY-WAL-PSAD-MISMATCH | WAL silence02 vs body01, gate03 in both; separately WAL gate01 vs body03, silence01 in both | InvalidPayload(field=Config.silence_rule or Config.recording_gate respectively,detail=PolicyRepresentationMismatch); each tag individually supported and mode-compatible, but no activation or fallback to either representation |

For the literal AF-C1 layout: normalizer_ref and ConfigDefinition.evidence are each71 ASCII bytes plus one length byte. PSAD Config BODY offsets are silence_rule83 and recording_gate109 (body length135); WAL Config frame offsets are silence_rule137 and recording_gate163 (header32+Context24 included). These offsets are specific to the stated Some-valued option layout, not universal offsets for variable-length Config bodies. Expected field bytes at both pairs of offsets are01/03. The body SHA256 stays `d3827df81929780ebd89539b091c14a2873f9934743947c080d01a7211544546`. Offending policy-tag errors identify the corresponding field/representation/offset from this layout; body offsets are NOT PSAD descriptor-header offsets.

V2-POLICY-NO-ORDINAL-CAST: decoded gate byte03 is Durable; decoded RecordingEvidence.watermark_kind byte03 is Written. With baseline P's current candidate PC(A,21,1) requiring Durable, a known Written receipt through20 and final typed Written fence through21 cannot satisfy Durable: permit=false,FenceTooWeak. Candidate ID/content/frontier/availability remain unchanged; no new analytical input or effect. The required semantic pairs are gate Written1 -> watermark Written3, Flushed2 -> Flushed4, Durable3 -> Durable5. Compare achieved semantic levels, never raw ordinal equality. V-R2-MODE-GATE's nine cells and all receipt/fence semantics remain unchanged.

### V2-DOC-AF-LINKS and finding mapping

From each actual directory of specs/domain/artifacts-v1.md, specs/domain/test-matrix-v1.md and this file, `../../tests/fixtures/domain/artifacts-v1.md` resolves to exactly the existing repository path `tests/fixtures/domain/artifacts-v1.md`. The former three-parent form escapes the repository root and is not used as a link. Resolve each relative target against its containing document, not against the repository root or the current shell directory.

| Finding | Corrected section | Named vector |
|---|---|---|
| V2-WIRE-01 | WAL1 and4 Gap; ADR targeted correction; handoff current checkpoint | V2-WIRE-GAP-ORDER, V2-WIRE-GAP-REVERSED |
| V2-WIRE-02 | health2.1 normative tags; WAL4 ConfigDefinition and artifacts3 references; ADR/handoff | V2-POLICY-SILENCE-ALL, V2-POLICY-GATE-ALL, V2-POLICY-SILENCE-UNSUPPORTED, V2-POLICY-GATE-UNSUPPORTED, V2-POLICY-WAL-PSAD-MATCH, V2-POLICY-WAL-PSAD-MISMATCH, V2-POLICY-NO-ORDINAL-CAST |
| V2-DOC-01 | AF fixture links in artifacts-v1, test-matrix-v1, review-vectors-v2 | V2-DOC-AF-LINKS |

Documentary byte calculations (Python struct/zlib plus reflected CRC loop) checked both100-byte frames, their independent trailers, and the AF-C1 field offsets/body hash; no Rust parser/model was executed. Relative-link checks and CI evidence for the final containing commit are recorded in the PR after commit. Targeted findings await Integrator review; all contracts remain PROPOSED. Full archive/multisegment recovery goldens, executable contract tests and real verifier/storage checks remain NOT_IMPLEMENTED/NOT_RUN as previously scoped.
