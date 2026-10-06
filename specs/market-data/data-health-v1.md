# DataHealth v1 — revised transition proposal

Status: **ACCEPTED**. Proposal revision **2**, accepted by owner squash merge of [PR #10](https://github.com/al-gri/pro-sclpng/pull/10); verified main baseline `8d9d6ada6309542822e4e38dd064f1e3467f990b`.
Links: [events](events-v1.md),[types](../domain/types-v1.md),[artifacts](../domain/artifacts-v1.md),[WAL](../recording/wal-v1.md),[exact vectors](../domain/review-vectors-v2.md).
A1–A3/R3/R6/C2 are part of the accepted SPEC-001 contract. The executable DataHealth model remains test/reference support; no production supervisor, queues, book reducer, storage or replay engine is implemented by SPEC-001.

## 1. State and owners

| Axis | Owner | States |
|---|---|---|
| T transport | map(ConnectionId,ConnectionEpoch) | Unknown,Up,Down |
| F freshness | StreamId + full continuity scope | Unknown,Fresh,QuietVerified,Stale |
| B book validity | BookRef + full continuity scope | NoSnapshot,Warming,Usable,Invalid(reason) |
| R recording | ArchiveId / capture session | Unknown,Healthy,Degraded(reason),Failed(reason) |

Full scope includes SpecRef,connection/subscription/book epochs,config,normalizer,profile and invalidation_barrier. Per stream store bounded pending candidates/proofs,last_applied_raw frontier,snapshot RawFrameId,capped progress,frozen warm-up witness and original last-valid-data sample. Evaluation maximum is archive-session/clock scoped;watermarks separate archive frontiers. Canonical interpretation is Running or Blocked(error);a framing scan may continue under Blocked but cannot publish.

New StreamDefinition initializes TUnknown ONLY for a new connection owner/generation. Adding B to already-Up connection preserves T and stream A. B gets its own FUnknown/BNoSnapshot,empty pending,barrier=definition RecordNo. Definitions cannot advance an existing connection epoch;use EpochAdvance. Dynamic registration is allowed with these rules. One REGISTERED writer-stream per BookRef/archive;new StreamId for that book requires new archive,not reset/rebind.
Trades have no B and cannot restore a book. Normal/RPI states/evidence independent. Unmentioned axes in transitions are retained,not defaulted.

## 2. Versioned policy / immutable mode compatibility

ConfigDefinition carries silence_rule,optional freshness_deadline_ns,optional warmup_min_updates/elapsed_ns,allow_quiet_with_proof,require_two_sided_snapshot,recording_gate. Config descriptor MUST mirror these and adds required pending_max_frames/raw_bytes/outputs/wait_ns plus quiet_max_lifetime_ns:Opt<u64>. Mismatches are errors,not alternate values.
Durations u64 nanoseconds. Present deadline>0;StaleAfterDeadline requires it,UnknownOnSilence may use None. At least one warm-up threshold present;explicit0 is a recorded engineering/synthetic choice and still requires WarmupEvidence. Pending caps:frames1..256,raw_bytes1..16777216,outputs1..65536,wait>0. allow_quiet=true requires Some(positive max lifetime),false requires None. No real-feed universal threshold is selected;Synthetic provenance cannot establish Bitget semantics.

| Archive mode | Written gate | Flushed gate | Durable gate |
|---|---|---|---|
| Buffered | valid,weaker guarantee | valid,not durable | valid |
| GroupSynced | INVALID_CONFIGURATION | INVALID_CONFIGURATION | valid |
| SyncBeforePublish | INVALID_CONFIGURATION | INVALID_CONFIGURATION | valid |

Invalid cells fail BEFORE activation. New config cannot weaken immutable archive mode. GroupSynced batches actual sync,not early volatile publication. Unknown/None frontiers satisfy no gate. Trusted stronger completion covers its achieved weaker prefix,not an unattempted stronger operation.

### 2.1 Policy byte tags

This is the single normative policy-enum table for BOTH WAL ConfigDefinition and PSAD Config body. Each field is one u8; names and numeric encodings are not inferred from fixtures.

| Enum / field | Allowed tag | Meaning |
|---|---:|---|
| SilenceRule / silence_rule | 1 | UnknownOnSilence |
| SilenceRule / silence_rule | 2 | StaleAfterDeadline |
| RecordingGate / recording_gate | 1 | Written |
| RecordingGate / recording_gate | 2 | Flushed |
| RecordingGate / recording_gate | 3 | Durable |

Every other u8 value, including 0 and 255, is Unsupported(field=Config.silence_rule or Config.recording_gate,value). There is no Unknown/default policy variant and BootstrapContext does not relax these tags. Validate tags in each representation before comparing mirrored fields or applying the unchanged nine-cell mode/gate matrix above. A supported but unequal WAL/descriptor policy value is InvalidPayload(field=Config.silence_rule or Config.recording_gate,detail=PolicyRepresentationMismatch), not a choice of which representation to trust. Either failure blocks semantic processing before ConfigDefinition: retain the previous configuration/data state, create no candidate/effect, and grant no publication permit.

RecordingGate is NOT RecordingEvidence.watermark_kind. The latter keeps its own existing [WAL Control enum](../recording/wal-v1.md#6--control): Accepted=1, Appended=2, Written=3, Flushed=4, Durable=5. Required semantic correspondence is Written gate -> achieved Written, Flushed gate -> achieved Flushed, Durable gate -> achieved Durable (or a trusted stronger completion), never equality/comparison of raw enum ordinals. In particular, recording_gate byte3 means Durable whereas watermark_kind byte3 means Written; a Written receipt/fence cannot satisfy a Durable gate. StorageFence uses its typed achieved level, not a new wire tag. [Targeted byte vectors](../domain/review-vectors-v2.md#9-targeted-wire-and-link-vectors) exercise the encodings without changing A2/A3 or any mode/gate decision.

## 3. Deterministic time, freshness and progress

Only recorded inputs advance evaluation=max(previous,current valid Context/Timer sample) in the same session/clock. Expiries run BEFORE proof release. No Instant/SystemTime. Data sample is ORIGINAL raw,never proof receipt/release. last_valid_data_ns=max applied original samples in that clock domain;each effect also retains its own original sample. IncomparableClock=>FUnknown,no guessed duration/readiness.

Finite D:expiry=checked(sample+D);Fresh exactly sample<=evaluation<expiry. Equality/after=>Unknown for UnknownOnSilence,Stale for StaleAfterDeadline. Overflow=>FreshnessDeadlineOverflow/FUnknown. D=None=>ordinary FUnknown even just after valid data;None is not infinity. No sample=>Unknown. Delayed valid proof can improve structural B while F remains expired. No new input means no invented evaluation advance;timers must be recorded.

QuietVerified requires permitted policy and a resolved Freshness descriptor containing finite [from,until),original observed_at,basis raw,anchor,full current tag/config/normalizer/profile/barrier and session/clock. Basis already applied;book anchor current;observed_at>=basis sample;from>=observed_at;until>from. Effective expiry=min(until,checked(observed_at+policy quiet_max_lifetime)). Apply only from<=evaluation<effective expiry. Arrival cannot extend it. Missing/reversed bounds=>InvalidQuietBounds;overflow/incomparable clock gives no quiet guarantee. Future interval=>QuietNotYetValid,NOT automatically activated by later timer;new recorded proof required. Expired=>QuietExpired. At expiry use ordinary freshness above;T/B unchanged. Reset/context change clears quiet evidence;obsolete proof cannot attach to a new anchor.

FreshnessEvidence Fresh/Unknown/Stale assertions must match the ordinary predicate from the original applied basis sample;otherwise FreshnessAssertionMismatch. They are not commands to overwrite state. Quiet is the only separate bounded proof. Policy denial=>QuietPolicyDenied. Body/scope requirements and error mapping are in artifacts4.

Warm-up progress counts applied BookUpdate OUTPUTS,not frames,levels or proofs. While Warming,increment only if progress<configured min_updates;None count threshold means stored progress0. Stop increments at threshold,without unchecked/saturating lifetime arithmetic. elapsed=checked(evaluation-anchor ORIGINAL sample). New WarmupEvidence must match exact capped progress/elapsed/anchor/barrier and satisfy thresholds. After Usable,freeze progress/witness until invalidation. Equivalent witness/proof duplicate adds0. Later valid data still updates its original sample/F but not frozen progress. Operational lifetime statistics cannot overflow into lost readiness.

## 4. Step order and transition table

Order: framing/order/context/artifact resolution -> advance valid evaluation/expire -> current-scope classification -> transition -> readiness/candidate decision. Unresolvable required artifacts or invalid archive/order/config/ack records set canonical interpretation Blocked and stop dependent semantic prefix before that record;no permit,including for an earlier waiting candidate. Market scope failures have explicit invalidation transitions;pre-barrier/old-scope diagnostics do not poison recovered current data. Their valid recorded clock sample can still cause ordinary expiry,which is not an evidence side effect.

| Input / guard | Exact transition |
|---|---|
| New StreamDefinition | initialize new connection only;new stream FUnknown/BNoSnapshot/barrier=record;shared T and neighbor state retained |
| Current Up/heartbeat | TUp;no sample/anchor/progress refresh;normal recorded-time expiry still runs |
| Current Down | TDown;all dependents FUnknown/BInvalid(TransportDown),barrier=record;clear pending/anchor/progress/witness/sample;revoke candidates |
| Structurally valid current RawSnapshot/Delta | bounded pending only;no applied B/F effect;raw<=barrier rejected PRE_BARRIER |
| Proof of later pending frame | mark ready;wait for earlier pending source frames |
| Release current verified snapshot above barrier | BWarming,new anchor,progress0,original raw sample;F from section3;no automatic warm-up |
| Release current verified delta with Warming/Usable anchor | retain B,update original sample/F;count one per output only while Warming and below threshold |
| Release delta without anchor | NeedsSnapshot;no effect;BInvalid(NeedsSnapshot),barrier=record,clear dependent state |
| Equivalent already-applied / ready proof | ALREADY_APPLIED / ALREADY_VERIFIED;no new effects/sample/progress;only independent time expiry may change F |
| Pre-barrier / obsolete proof | PRE_BARRIER / OBSOLETE_SCOPE;no current scope mutation/effects |
| Current proof conflict,mixed frame,structural error,expired pending proof | BInvalid(ProofConflict/MixedFrameUnsupported/structural reason/ProofExpired),FUnknown,barrier=record,clear dependent state;no partial failed-frame effects |
| Pending overflow or deadline reached | BInvalid(PendingOverflow/PendingTimeout/PendingDeadlineOverflow),FUnknown,barrier=record,clear pending/evidence |
| Truthful current WarmupEvidence | Warming->Usable,freeze progress/witness;other guards still required |
| Bad current warm-up witness | WitnessMismatch;Warming retained,no force-ready;obsolete witness does not disturb recovered state |
| Applicable bounded quiet proof | FQuietVerified until exact exclusive expiry;not book resync |
| Timer / valid new clock sample | pending deadline/freshness/quiet expiry;silence alone does not set TDown |
| Critical current GAP | BInvalid(reason),FUnknown,barrier=record,clear dependent state;local recording loss also RDegraded |
| Connection EpochAdvance | new TUnknown;all dependents FUnknown/BNoSnapshot/barrier=record,clear evidence;other connections unchanged |
| Subscription/Book EpochAdvance | reset only selected owner's dependents;shared T unchanged |
| Valid SpecActivate/ConfigDefinition | FUnknown,BInvalid(ContextChanged),barrier=record;clear dependent state/revoke candidates;new context applies to NEXT record |
| Valid RecordingEvidence | validate earlier contiguous nonregressing achieved prefix;RHealthy is observation,not release authorization |
| Recording fault or impossible GAP write | RFailed,revoke candidates,no permit;no fictitious durable GAP |
| Restart | new archive/session,empty definitions/frontiers;no inherited clock,witness,candidate or fence |

Gap invalidates without EpochAdvance. New post-barrier snapshot AND new warm-up required. Up/Healthy recorder cannot replace market resync. For a release step with multiple frames,earlier successful complete-frame effects remain historical when a later frame fails;final scope invalidity prevents publication.

## 5. A2: publication candidate and permit

```text
usable_data = canonical_interpretation == Running
           && resolved current artifacts/definitions
           && expected identity/spec/tag/config/profile/barrier
           && T == Up
           && (F == Fresh || applicable bounded QuietVerified)
           && B == Usable && current verified anchor && valid frozen witness
```

This is data readiness,not permission to trade or publish externally. The former capture_usable shorthand is replaced by TWO relations.

Candidate projection consists of current scoped data/effect references,anchor/witness,F,R,recording observations and recorded evaluation time. At the END of a recorded step,if usable_data and RHealthy and valid configuration hold,create exactly one immutable publication_candidate for each stream whose eligible projection first exists or differs from its last candidate projection. If unchanged,retain that candidate and emit no new one. Evaluation-time or relevant recording-observation change counts;unrelated registration with unchanged clock/scope/projection does not. No live clock/storage-fence arrival participates in this comparison. This deterministic rule is a proposed pure relation,not a signal/publisher implementation.

CandidateId=(ArchiveId,creation_record_no,stream_id),at most one per stream per step. Candidate holds full scope,selected gate,ordered source effect IDs,frozen content,available_at=InputCursor(creation_record_no),causal_frontier=creation_record_no (the conservative ENTIRE recorded prefix,including the step's clock/RecordingEvidence). Candidate is not a market EventId/EventCursor. Creation does not release it. No strategy/TradePlan type is introduced.

publication_permit is true only when canonical processing is Running,candidate is current/nonrevoked,current usable_data and RHealthy still hold,mode/gate is valid,fence archive/session match,gate>=selected,known fence.through>=candidate.frontier and fence.through<=writer's known contiguous achieved prefix<=accepted prefix. No gaps/regression/future-prefix assertions. Buffered weak gates keep weak labels. A parsed record/token/CRC is not trusted storage completion.

Finite trace: inputs through20 -> actual gate reached20 -> r21 RecordingEvidence(through20) -> evaluate/freeze candidate(frontier21) -> actual storage gate reached21 -> final StorageFence(through21) -> guard may release THAT candidate. r21 is included when it affects state/time/readiness;durable20 alone is insufficient. Final fence has NO RecordNo,EventCursor,evaluation timestamp or analytical effect. It is the result of storing an already defined prefix and MUST NOT auto-generate another RecordingEvidence-awaiting-itself loop.

RecordingEvidence requires through<own RecordNo;Healthy with None=>MissingWatermark,no new successful observation. Self/future=>InvalidAcknowledgement. Known regression=>WatermarkRegression;impossible ordering/continuous prefix=>WatermarkOrderError. These invalid archive observations block semantic continuation,not merely a new candidate. Stronger success covers weaker prefix only when actual completion is trusted.

Fence guard diagnostics (no WAL record/time/cursor):FenceMissing;FenceBehindCandidate;FenceTooWeak;FenceScopeMismatch;FenceBeyondAchieved;UnverifiedStorageCompletion;CandidateRevoked (also for a superseded old candidate);WatermarkRegression for regressing trusted completion. They grant no permit or new analytical state. A old fence checked against a NEW candidate below its frontier yields FenceBehindCandidate;checked against the superseded OLD candidate yields CandidateRevoked.

Relevant new state supersedes an older candidate with a new recorded frontier;GAP/Down/context/epoch/recording failure revokes it. The guard rechecks state at release. A late fence cannot resurrect old state. A candidate object remains deterministic independently of external delivery history;no exactly-once/outbound-ack behavior is defined here.
Replay reconstructs state,candidates and recorded observations,NOT historical physical sync or send success. Crash between fence and send leaves deliveryUnknown;no automatic resend. Real storage completion provenance,writer scheduling,sync/flush/metadata/crash tests are REC-001. SPEC defines finite typed relations only;no I/O or fence producer is added now.

## 6. Vectors and approval gate

[Named vectors](../domain/review-vectors-v2.md) cover all findings with exact state,diagnostic,source/application IDs,cursors and availability. [Matrix](../domain/test-matrix-v1.md) retains numeric/full-WAL obligations. New specs/ADR PROPOSED;model/executable tests NOT_IMPLEMENTED/NOT_RUN. D1/D2 permission does not approve event/health/WAL. R1–R6 remain submitted for independent renewed review,not closed by this worker.
