# ADR-0002 — Exact values, causal application, health and bounded WAL

Status: **PROPOSED — DESIGN_REVIEW_REQUIRED**. Proposal revision **2**.
Date: 2026-10-05. Task: [SPEC-001 #3](https://github.com/al-gri/pro-sclpng/issues/3).
[Packet](https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5994665559) · [Existing claim](https://github.com/al-gri/pro-sclpng/issues/3#issuecomment-5995282901) · [Same Draft PR #10](https://github.com/al-gri/pro-sclpng/pull/10).
Base `6c520237d35865c79dba9e74fa64bd4c2c9e419f`; previously reviewed revision `98ecd7484f8345d6c5f162a85ff3effbd6a46a68`; branch `feat/SPEC-001-domain-contracts`.

## Review provenance and authority

[Integrator review5415922496](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5415922496): DESIGN_CHANGES_REQUIRED,R1–R6,C1/C2; D1/D2 APPROVED_FOR_ISOLATED_IMPLEMENTATION only.
[Architecture review5416219284](https://github.com/al-gri/pro-sclpng/pull/10#pullrequestreview-5416219284): ARCHITECTURE_DIRECTION_SET / DESIGN_REVISION_REQUIRED. A1–A3 plus R3/R5/R6/C1/C2 directions below are copied into coordinated proposals,not silently attributed to the old revision or real Bitget.
Neither review approved the whole API/wire format. This docs-only revision requests renewed approval; it does not resolve/close findings itself. Numeric implementation has not started and no code was discarded. ADR-0001 and accepted baseline remain untouched.

## Context / retained D1–D6

BOOT-001 supplies std-only Rust1.98.1 workspace; domain currently exports no market API. Before network/storage code,domain/event/health/WAL contracts must agree on exact units,identity/order,UNKNOWN,proof availability,loss accounting and publication durability.

| Decision | Retained direction / current gate |
|---|---|
| D1 | Qualified distinct u64 ticks/steps,u128 coefficient,canonical scale<=18,positive-price-only v1; isolated implementation permitted,not general API acceptance |
| D2 | ASCII parsing<=96bytes,checked intermediates,OffGrid without rounding,exact linear units/multiplier and UNKNOWN; conservative intermediate overflow retained |
| D3 | Recorded raw/control inputs,stable source/application identities,canonical cursor through recorded activations; revised by A1/A3/R4 below |
| D4 | Independent market/stream/book owners and epochs;four health axes,current resync/warm-up;one registered writer-stream per BookRef/archive;no rebind runtime |
| D5 | One-session archive,32-byte LE header,known tags,bounded payload,CRC,seals,explicit loss;completion separate from quality/durability;A2 and R5 now defined semantically |
| D6 | Known-tag-only decoding,checked bounds before allocation,independent golden/negative vectors;no dependencies or silent coalescing/skip policy |

Specs: [types](../../specs/domain/types-v1.md),[events](../../specs/market-data/events-v1.md),[health](../../specs/market-data/data-health-v1.md),[WAL](../../specs/recording/wal-v1.md),[artifacts](../../specs/domain/artifacts-v1.md),[matrix](../../specs/domain/test-matrix-v1.md),[named review vectors](../../specs/domain/review-vectors-v2.md).

## A1 — source candidate, proof and actual application

Architecture direction: reading a raw book frame creates candidates,not confirmed book effects. SourceCandidateKey retains RawFrameId/raw_sub_index/normalizer revision. Applied EventId uses the CURRENT apply_record_no/output_index; available_at is never retroactively moved back to raw. Frame-wide proof covers one homogeneous stream/tag/profile batch with complete ordered output commitment. No per-sub-event proof or partial/mixed batch success.

Later-frame proof waits for earlier pending source frames; release a ready per-stream prefix in raw order at the current step. Other streams do not wait. At each invalidation,save an explicit RecordNo barrier and clear dependent pending/anchor/warm-up. Gap even without epoch advance,TransportDown,critical overflow/timeout and context changes require a new raw snapshot ABOVE the barrier plus applicable current-source resync evidence. Up cannot clear it. Old pre-barrier proof cannot restore OR poison a recovered generation.

SourceApplicationKey/frontier guard prevents double application/progress/freshness even when duplicate proof has another RecordNo. Current-scope contradictory evidence fails closed. Immutable prefix resolution supports equivalence checks without prescribing an unbounded production history map. Original raw sample determines data age; proof/application samples only determine availability/evaluation.

Explicit revision details submitted for review: canonical market projection excludes administrative/control events (they keep RecordRef); as_of becomes CausalBasis with conservative full RecordNo prefix plus prior effect refs instead of invented raw EventCursor. One apply step may release several frames; each frame is atomic,an error in a later frame leaves earlier complete effects auditable but invalidates final scope/readiness. Pending bounds include frame count/raw bytes/output count and original-sample wait; caps and exact diagnostics appear in specs/vectors. These are not claims that a production queue exists.

## A2 — mode/gate and finite two-phase publication

Architecture's v1 matrix is adopted in health AND WAL: Buffered allows Written/Flushed/Durable with truthful weaker labels; GroupSynced and SyncBeforePublish permit ONLY Durable. Other cells INVALID_CONFIGURATION before activation. Config cannot weaken immutable archive mode; group sync describes scheduling,not volatile publication permission.

Separate usable_data,immutable publication_candidate and publication_permit. Finite sequence: causal prefix20 achieves gate -> r21 RecordingEvidence(through20) -> evaluate/freeze candidate including r21 in causal frontier -> actual storage gate reaches21 -> final StorageFence21 -> guard permits that candidate only if still valid. Receipt influences time/state and is not omitted from causality. Final fence is an operation result for an already defined prefix,not a new analytical WAL record/clock tick;it generates no self-ack recursion.

Candidate identity/provenance and guard conditions are explicit in health5. Unknown/lagging/weaker/wrong-scope/future/regressing completion fails. Relevant new state supersedes the old candidate;invalidation/failure revokes it;late fence cannot resurrect it. CRC,parsed tokens and a recorded receipt do not establish actual I/O provenance.

Rejected alternatives: capture_usable boolean overriding mode;excluding influential receipt from causal basis;unbounded ack-of-ack recursion;deferring the finite semantic relation until after freeze. Real sync/flush/metadata/receipt provenance,writer batching,crash tests and costs remain REC-001. Replay reproduces state/candidates/observations,not historical fsync or send success. Crash between fence and send yields deliveryUnknown,no exactly-once or automatic resend promise.

## A3 — explicit content-bound artifacts and activation

ArtifactRef text is exactly sha256:+64 lowercase hex digits in existing Token128 fields. CRC32 is not an artifact identity hash. [NIST FIPS180-4](https://www.nist.gov/publications/secure-hash-standard) is the primary algorithm reference; content binding is not signature/authentication or algorithmic correctness.

New [artifacts-v1](../../specs/domain/artifacts-v1.md) proposes exact PSAD descriptor bytes with kind/logical identity/revision/body schema/body length/body SHA256/dependencies and separate human provenance. The WAL ref hashes descriptor bytes; descriptor hashes its separate body,avoiding recursive self-hash. Optional PSAM sidecar only indexes bytes that must match WAL refs and full closure. Exact five synthetic descriptor/body goldens and PSCO output commitment are in [AF fixtures](../../tests/fixtures/domain/artifacts-v1.md).

Bindings: ConfigDefinition.evidence -> Config descriptor mirroring config policy and exact normalizer ref;StreamDefinition.provenance -> feed profile tied to stream/instrument/channel and supported normalizer;InstrumentSpec.provenance -> exact spec descriptor;Verification/Warmup/Freshness proof -> typed full-scope/basis/temporal descriptor. Scope includes archive/session/clock,stream/instrument/owners via registered mapping,tag,config/norm/profile and barrier. Verification has whole-frame ordered output commitment and resync basis;warm-up has current anchor/capped progress/elapsed;quiet has finite original-observation bounds. Human provenance is not overloaded as content identity.

Namespace(ArchiveId,kind,logical identity,revision) binds immutable digest. Reusing same artifact is permitted;rebinding bytes is ArtifactIdentityConflict. Missing/mismatch/unresolved/unverifiable dependencies => Blocked canonical replay/readiness. No network/latest fallback. ParsedRef is not Verified. All future artifacts being available offline does not activate them before their WAL proof/config record.

Canonical timeline: config1/norm1 -> config2/norm1 -> config3/norm2 is valid with supported immutable bindings. Context of each ConfigDefinition is OLD;NEXT record uses NEW. BootstrapContext(0,0) is a distinct administrative variant,not ordinary zero versions. Cursors compare across recorded revisions;historical references remain auditable but cannot transfer old readiness. Alternate research replay does not reuse canonical identity with new meaning.

No crypto dependency/home-grown production SHA-256/loader is added to domain. The allowed later synthetic resolver proves guard behavior only. Real artifact verifier and storage integration require subsequent accepted scope;without them production remains Unverified/Blocked. Descriptor/PSCO/cap choices are explicitly proposed concrete formats,not retroactively approved by the architecture comment.

## R3 / R5 / R6 / C1 / C2 — remaining directional changes

R3: None freshness deadline means NO ordinary Fresh guarantee. Finite Fresh on [sample,sample+deadline),exclusive upper bound. Equality expires to Unknown/Stale by policy. Quiet requires finite descriptor bounds intersected with policy lifetime from ORIGINAL observation,exact clock/current anchor/tag/config/profile/barrier. Late proof cannot extend life. Timers/clock inputs are recorded;no wall-clock call is smuggled into replay.

R5: archive-long per-stream CaptureAttemptNo starts1 and never resets at epochs. SourceGap is not local QueueOverflow. Known local ranges account exact next missing intervals without overlap;unknown local gap is one window consumed by the next raw,including zero inferred hole. It cannot authorize a later jump. Unresolved window means qualityUnknown,not zero/no loss. **Explicit conservative v1 choices for review:** at most one unresolved local window per stream;second local gap while open is AmbiguousLossWindow;relevant scope change before its right boundary is GapScopeTransition and blocks semantic continuation. No new wire fields are added. More general overlapping/multiple-loss reconciliation is not silently promised.

R6: transport map belongs to (ConnectionId,ConnectionEpoch);new StreamDefinition only initializes genuinely new owner/generation. A second stream preserves shared Up state and its neighbor's readiness,but has its own Unknown/NoSnapshot/barrier. Connection Down/advance fans out only to dependents.
C1: one REGISTERED writer-stream per BookRef/archive;another StreamId requires new archive. Same-stream resubscription uses epochs,not rebind.
C2: warm-up progress counts applied BookUpdate OUTPUTS,not proof records/frames/levels. Stop incrementing at threshold;freeze after Usable. Diagnostic lifetime statistics cannot overflow into lost readiness.

## Explicit wire / artifact / scope / timeline delta

| Area | Reviewed revision | Revision2 proposal |
|---|---|---|
| Header/kinds/field order/CRC | 32-byte header,existing tags,CRC algorithm | UNCHANGED;W01 bytes/checksum remain identical |
| Token128 provenance/evidence/proof | generic provenance/revision string | required typed ArtifactRef;legacy prose no longer semantically valid |
| Policy | WAL fields only,undefined pending/quiet lifetime | same WAL fields;required matching config descriptor adds pending caps/wait and quiet maximum,proposal_revision2 |
| Book proof | ambiguous raw_record_no scope | whole homogeneous frame,complete ordered PSCO commitment,current full scope/basis and temporal bounds |
| Event identity / as_of | raw cause and ambiguous proof availability/EventCursor basis | source key separate from application ID;actual apply cursor;CausalBasis prefix for raw/control causes |
| Activation | same-revision comparison ambiguity | one canonical cursor order through recorded activations,old Context/new-next rule,explicit BootstrapContext |
| Recording gate | selected watermark could weaken mode | full validation matrix,candidate plus non-WAL final fence;receipt included in causal prefix |
| GAP accounting | earlier unknown gap could look reusable | explicit one-use window/frontier/start/count/scope/EOF rules;no wire-field delta |
| Warm-up | unbounded update accumulation ambiguity | capped per-output progress,freeze after Usable |

No accepted wire schema is being migrated;all these contracts were and remain PROPOSED. Proposal revision2 must be reviewed as a semantic change even where byte layout is unchanged. Pure diagnostics are named in the vector catalog and shared by the corresponding spec guards;their runtime Rust representation is not frozen by documentation alone.

## Mapping / evidence / remaining questions

The [finding mapping](../../specs/domain/review-vectors-v2.md#8-finding-to-change-mapping-and-remaining-review) names every finding,changed sections,exact test vectors and outstanding proof/review. It is SUBMITTED_FOR_REVIEW,not a checklist marked closed.

Renewed review should confirm: source/effect/control projection and CausalBasis;frame-prefix atomicity,proof equivalence and bounded retention interface;candidate creation/supersession/fence diagnostics;exact binary descriptor/PSCO schemas/caps and verifier boundary;one-window/scope-change restrictions in local loss accounting;full expiry/fan-out/progress traces. These are independent of unverified real Bitget facts,which remain MD-001-owned. Full multi-frame golden,all truncation offsets and executable models are still NOT_IMPLEMENTED/NOT_RUN.

## Scope and next gate

Only permitted specs,new proposal ADR,small synthetic fixture documentation and own handoff change. Root Cargo.toml/lock,toolchain1.98.1,CI,apps/radar,PROJECT_STATE,specs registry and accepted ADR-0001 stay unchanged. No numeric code is mixed into this revision;std-only remains intact.
No connector,production book/queues/supervisor/recorder/replay,strategy/TradePlan/Telegram/execution. No performance/profitability claim. Real feed semantics BLOCKED_BY_MD_001;physical durability/loader integration downstream,not proved by Linux bootstrap CI.

Next bounded step: Architecture/Integrator SHA-bound re-review in the SAME Draft PR #10. Until explicit renewed approval,event/health/WAL implementation stays stopped. D1/D2 isolated permission remains separate. Full implementation acceptance still requires real tests/CI/independent QA/owner acceptance. Issue3 and Issue6 remain open;no self-approval,merge,auto-merge,force-push or settings changes.
