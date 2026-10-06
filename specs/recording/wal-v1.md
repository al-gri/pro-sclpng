# WAL v1 — byte, recovery, accounting and publication proposal

Status: **ACCEPTED**. Proposal revision **2**, accepted by owner squash merge of [PR #10](https://github.com/al-gri/pro-sclpng/pull/10); verified main baseline `8d9d6ada6309542822e4e38dd064f1e3467f990b`.
Design base: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Links: [types](../domain/types-v1.md), [events](../market-data/events-v1.md), [health](../market-data/data-health-v1.md), [artifacts](../domain/artifacts-v1.md), [matrix](../domain/test-matrix-v1.md), [review vectors](../domain/review-vectors-v2.md), [ADR](../../docs/adr/0002-domain-event-wal-contracts.md).
No production recorder, file I/O, queue or replay/recovery engine is implemented by SPEC-001. The accepted memory codec/recovery/reference behavior exists in test support and does not constitute production storage implementation.

## 1. Revision boundary and archive scope

One ArchiveId, one CaptureSessionId/ClockId, ordered segments and a dense authoritative RecordNo sequence. Restart creates a new archive, never appends to the old one. previous_archive is provenance, not clock/sequence continuity. Raw/control-only archive remains authoritative; there is no persisted normalized-event record kind. No Rust memory layout, usize, float, native endian or unordered map serialization.

**Explicit changes from reviewed proposal:** existing provenance/evidence/proof Token128 fields now require typed content-bound ArtifactRef values; config descriptor carries pending/quiet policy and proposal_revision=2; frame-wide proof/delayed application, activation, local GAP accounting and publication gate/fence semantics are defined below/across linked contracts. These are semantic changes to a PROPOSED format, not an assertion that the prior bytes already guaranteed them. Header, RecordKind/Control tags, payload field order and CRC coverage are unchanged. W01 remains byte-identical. No wire field is added for ArtifactRef, StorageFence or GAP windows. A legacy prose proof token fails revised semantic validation. No accepted archive migration is claimed.

Targeted correction V2-WIRE-01 restores the original scope-before-reason Gap order in section4. The reversed reason-before-scope line at `8758b3c2a8896146a14d397bd65dc18ac5a49415` was an error, not an intentional wire revision. V2-WIRE-02 restores explicit policy-tag definitions via DataHealth section2.1; A1–A3 and the nine mode/gate decisions are not redesigned.

## 2. Fixed framing — unchanged

Frame = `header[32] || payload[L] || crc32[4]`, no padding. All multi-byte integers little-endian.

| Offset | Bytes | Field | Rule |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII PSRW = 50 53 52 57 |
| 4 | 2 | frame_version | u16=1 |
| 6 | 2 | record_schema_version | u16=1 |
| 8 | 2 | record_kind | known u16 tag from section 4 |
| 10 | 2 | flags | u16=0 |
| 12 | 4 | payload_len | u32=L, payload ONLY |
| 16 | 8 | record_no | u64, 1..MAX, dense archive order |
| 24 | 4 | segment_no | u32, starts 0, advances exactly 1 |
| 28 | 4 | reserved | u32=0 |
| 32 | L | payload | exact canonical body |
| 32+L | 4 | checksum | u32 LE over header+payload |

MAX_PAYLOAD=1048576; checked frame_len=36+L<=1048612. Read/validate the fixed header, versions/tags/flags/reserved/cap, checked absolute_offset+frame_len and usize conversion BEFORE allocating from L. Nested count*minimum_entry_size/lengths must fit validated remaining bytes and own caps before allocation. Reject trailing body bytes, malformed options/bools/decimals; no skip-unknown mode.

CRC-32/ISO-HDLC: width32, poly0x04C11DB7, reflected poly0xEDB88320, init0xFFFFFFFF, refin/refout=true, xorout0xFFFFFFFF. Process stored bytes in order, 8 reflected bits per byte, complement once. Coverage exactly [0,32+L), including header and length/order fields, excluding trailer. Streaming aggregate continues internal state across chunks; final complement only on obtaining a digest. Reference: [RFC1952 section 8](https://www.rfc-editor.org/rfc/rfc1952#section-8). CRC is not authentication, artifact identity or physical durability evidence.

## 3. Payload primitives and Context

Concatenate fields in declared order. Opt<T>=u8 0 with no body, or 1 followed by T; other tags invalid. bool=u8 0/1. TokenN=u8 length then 1..N ASCII bytes from types grammar (N<=128). ExactDecimal=u128 coefficient,u8 canonical scale. InstrumentRef=venue:Token32,market_kind:u8,product_namespace:Token32,native_symbol:Token64. EpochTag=spec_version:u32,connection_epoch:u64,subscription_epoch:u64,book_epoch:Opt<u64>; present versions/epochs positive, book Some only for a book stream.

Kinds2..7 begin with Context[24]: `local_receive_unix_ns:i64, local_receive_monotonic_ns:u64, config_version:u32, normalizer_version:u32`. Session/clock inherit ArchiveStart. Context is the configuration BEFORE the current input. Wire(0,0) decodes to separate BootstrapContext ONLY before first config and only for administrative kinds2/3/4. Mixed zero/nonzero or Bootstrap RawInput/Control/Gap => InvalidBootstrapContext. Ordinary version newtypes never accept 0. Definitions and referenced raw/control records must precede use. Future refs are errors.

ConfigDefinition validates old Context and new descriptor/mode/policy, invalidates dependent scope at this RecordNo, then activates new context for the NEXT record. ConfigVersion definitions cannot repeat; reusing the same immutable normalizer/profile ref is allowed, not a redefinition. Canonical EventCursor comparison crosses recorded revisions ([events section 6](../market-data/events-v1.md#6-a3r4-canonical-activation-timeline)). Evidence artifacts available offline early are still inactive until their recorded proof.

## 4. Exact record bodies

### 1 — ArchiveStart

No Context: `archive_id:[u8;16],capture_session_id:[u8;16],clock_id:u32,durability_mode:u8,previous_archive:Opt<[u8;16]>`.
Own IDs nonzero, clock>0; previous nonzero and different from own if present. Buffered=1,GroupSynced=2,SyncBeforePublish=3. Only record1/segment0/offset0. Mode immutable; no guessed archive identity on missing start.

### 2 — InstrumentSpec

After Context: `instrument_slot:u32,spec_version:u32,InstrumentRef,price_quote_unit:Token32,price_basis_unit:Token32,quantity_unit:Token32,base_asset:Token32,price_increment:ExactDecimal,quantity_increment:ExactDecimal,quantity_to_base_multiplier:Opt<ExactDecimal>,provenance:Token128`.
Slot has one immutable identity; new spec version never overwrites old. First spec is available before stream registration; subsequent activation needs SpecActivate. Unit/multiplier rules in types; None is not 1. provenance now binds an InstrumentSpec descriptor whose body matches all preceding body fields. No prose-token fallback.

### 3 — StreamDefinition

After Context: `stream_id:u32,instrument_slot:u32,spec_version:u32,connection_id:u32,connection_epoch:u64,subscription_epoch:u64,channel:u8,book_id:Opt<u32>,book_epoch:Opt<u64>,feed_profile_version:u32,provenance:Token128`.
Register once; referenced spec exists. Book ID/epoch present together for book channels only. ConnectionId may be shared; existing current connection epoch MUST match. Creating another stream on it does not reset its transport. One registered writer-stream per BookRef per archive; another StreamId for that book => WriterRebindRequiresNewArchive. Same-stream resubscription uses epochs.
provenance is a FeedProfile descriptor matching stream/instrument/channel/version and supporting the selected normalizer. Missing or unverified real feed semantics => Blocked/BLOCKED_BY_MD_001, not guessed synthetic verification.

### 4 — ConfigDefinition

After Context: `new_config_version:u32,new_normalizer_version:u32,provenance_kind:u8,evidence:Token128,silence_rule:u8,freshness_deadline_ns:Opt<u64>,warmup_min_updates:Opt<u32>,warmup_min_elapsed_ns:Opt<u64>,allow_quiet_with_proof:bool,require_two_sided_snapshot:bool,recording_gate:u8`.
The single normative policy-tag table and unsupported/mismatched-policy behavior are in [DataHealth section 2.1](../market-data/data-health-v1.md#21-policy-byte-tags). It also governs the PSAD Config body; RecordingEvidence.watermark_kind is NOT the enum for recording_gate.
Versions positive; new ConfigVersion defined once, NormalizerVersion may reference an existing identical artifact. Provenance Engineering=1,Synthetic=2,SourceVerified=3. evidence is a Config descriptor mirroring body fields, referencing exact normalizer and adding required pending/quiet policy. Descriptor format/proposal_revision and limits in artifacts-v1. Validate mode/gate before activation: Buffered admits Written/Flushed/Durable; GroupSynced and SyncBeforePublish ONLY Durable. Invalid pair => INVALID_CONFIGURATION, old config/state retained and semantic processing stops before this record.

### 5 — RawInput

After Context: `stream_id:u32,EpochTag,capture_attempt_no:u64,payload_encoding:u8,raw_len:u32,raw_bytes:[u8;raw_len]`.
Encoding1=opaque complete source-message bytes; other tags Unsupported. raw_len exactly remaining payload; empty raw is representable for diagnostic rejection, not automatically a valid event. Attempt>0; per-stream accounting is section4.1, NOT exchange sequence. Old-tag raw may be recorded/accounted for diagnostics, but cannot apply to current book. Unknown stream/spec references fail validation. Raw book admission creates candidates; only complete frame verification can later apply effects. No normalized payload is silently inserted into WAL.

### 6 — Control

After Context: control_tag:u8 followed by EXACT variant body:

| Tag | Variant | Body order |
|---:|---|---|
| 1 | TimerFired | stream_id:u32,timer_id:u64,deadline_monotonic_ns:u64 |
| 2 | TransportObservation | connection_id:u32,connection_epoch:u64,liveness:u8 (Unknown0/Up1/Down2) |
| 3 | EpochAdvance | scope:u8 (Connection1/Subscription2/Book3),owner_id:u32,expected:u64,next:u64,reason:u8 |
| 4 | SpecActivate | instrument_slot:u32,expected_spec_version:u32,new_spec_version:u32 |
| 5 | VerificationEvidence | stream_id:u32,EpochTag,raw_record_no:u64,evidence_kind:u8 (Snapshot1/Delta2),feed_profile_version:u32,proof:Token128 |
| 6 | WarmupEvidence | stream_id:u32,EpochTag,snapshot_raw_record_no:u64,update_count:u32,elapsed_ns:u64,proof:Token128 |
| 7 | FreshnessEvidence | stream_id:u32,EpochTag,freshness:u8 (Unknown0/Fresh1/QuietVerified2/Stale3),basis_raw_record_no:Opt<u64>,proof:Token128 |
| 8 | RecordingEvidence | health:u8 (Unknown0/Healthy1/Degraded2/Failed3),watermark_kind:u8 (Accepted1/Appended2/Written3/Flushed4/Durable5),through_record_no:Opt<u64>,reason:u8 |

Reasons: UserReset1,Reconnect2,SourceGap3,QueueOverflow4,DecodeRejected5,WriteFailure6,NoFault7,Unknown255; others Unsupported. NoFault only for Healthy RecordingEvidence, never GAP/reset. timer_id>0, Context sample>=timer deadline. EpochAdvance: expected=current,next>expected, no wrap. SpecActivate: declared higher version of SAME instrument. Owner-qualified invalidation fan-out in health table; Up never clears a barrier.

The three proof fields now bind typed ArtifactRef descriptors, resolving exact full scope, barrier/anchor/basis, ordered output commitment and temporal bounds. Verification covers the ENTIRE homogeneous book frame, not a single sub-event. Delayed/reordered/duplicate proofs use events sections3–4; effects get actual apply cursors. A newer record ID is not permission to reapply a source. Warmup count is capped progress of applied BookUpdate outputs; QuietVerified has finite original-observation bounds. Arbitrary digest-looking text cannot force-ready.

RecordingEvidence is an observation about a strictly earlier prefix: through<own RecordNo. Healthy success with through=None => MissingWatermark, no new successful observation. Other health states may carry None without replacing known frontiers by zero. Self/future => InvalidAcknowledgement; regression/inconsistent prefix => WatermarkRegression/WatermarkOrderError. Receipt is included in any candidate's causal prefix when used. Final StorageFence is deliberately NOT a Control tag/record; section6 defines the finite boundary.

### 7 — Gap

After Context: `scope_kind:u8,reason:u8,target_count:u16,targets:[Target;target_count]`.
ExplicitTargets (Explicit)=1: 1..256 distinct targets sorted by StreamId. AllDeclaredStreams=2: target_count=0; expand to all declared current stream/tag, unknown range/count. Empty explicit list invalid. `Target=stream_id:u32,EpochTag,first_lost_attempt:Opt<u64>,last_lost_attempt:Opt<u64>,loss_count:Opt<u64>`.

Offsets from the beginning of the frame are fixed by header32 + Context24: scope_kind at56, reason at57, target_count little-endian at58..59, first target at60. ExplicitTargets1/QueueOverflow4/count1 encodes `01 04 01 00` at56..59. Reversed `04 01 01 00` is Unsupported(field=Gap.scope_kind,value=4,frame_offset=56), before target accounting or health changes; it is not an alternate layout. [V2-WIRE-GAP-ORDER / V2-WIRE-GAP-REVERSED](../domain/review-vectors-v2.md#9-targeted-wire-and-link-vectors) include complete frames with checksums appropriate to each byte sequence.

Both range ends present together or absent; first>0,last>=first, known count>0. Known complete range requires Some(checked(last-first+1)). Unknown range permits None or known positive count ONLY for local QueueOverflow; source/unlocalized reasons require both range/count None. Unknown never becomes zero. NoFault prohibited. Validate every target/accounting change before applying this record atomically.

GAP invalidates applicable current health and records loss, not compensation. Old-tag source GAP is diagnostic only. Local attempt accounting does not guess exchange sequence or fabricate lost RawFrameIds. Failed media may prevent even GAP: recorder Failed, archive Incomplete/Unknown; no durable GAP promise.

### 4.1 CaptureAttempt accounting — R5

Per StreamId for the WHOLE archive, maintain accounted_frontier f (integer0 before any attempt) and at most one unresolved local loss window. Next expected attempt=checked(f+1). Epoch changes never reset it. Account successful old-tag raw too, independently of market applicability.

Only QueueOverflow means pre-admission LOCAL loss eligible to cover CaptureAttempt holes. SourceGap/DecodeRejected/WriteFailure/UserReset/Reconnect/Unknown never authorize local holes; their loss target ranges/counts must be None. A source gap invalidates book continuity but says nothing about missing local attempts. Once RecordNo is assigned, loss is a failed/incomplete archive, not an attempt-window workaround.

Known local range must start EXACTLY at f+1 and count=last-first+1; accept it as the next accounted lost interval and advance f=last. First<=f is LossOverlap (successful or previously lost attempt cannot be lost again). First>f+1 is LossCoverageGap. Following raw must be f+1 unless another explicit loss advances/opens accounting. Known ranges can be contiguous separate records but not overlap.

Unknown-range local GAP opens one window `(gap RecordNo,left=f,tag,optional count)` without advancing f. Next raw of that stream closes it ONCE: require attempt a>f; k=a-f-1 is the inferred local missing interval size. If claimed count=Some(n), require k=n; otherwise preserve recorded loss_count=None (even when inferred k is known). Advance f=a and consume window, including k=0. No evidence is reusable for the later jump. A second local GAP while a window is open is AmbiguousLossWindow (v1 rejects combining/overwriting unknown windows). Known ranges may not be inserted into an open window. This explicit simplification avoids new wire fields and is submitted for review.

Without a window/range, first raw must be attempt1 and every later raw f+1; jump=>UnaccountedAttemptGap, a<=f=>AttemptOrderError. Failed validation leaves the accounting frontier unchanged and stops semantic prefix at that input; it is not silently admitted as a valid suffix. RecordNo itself remains dense in physical bytes; accounting validity is a separate check.

An open window is bound to its target tag. An intervening relevant epoch/spec/config continuity change before the right raw boundary yields GapScopeTransition and blocks semantic continuation for that ambiguous accounting trace; do not carry an old-scope permit into a new generation. A current local GAP targeting a mismatching tag similarly fails GapScopeTransition. A known already-accounted range survives epoch changes as historical accounting. Additional same-scope SourceGap does not consume/renew a local window.

EOF/finalization with an unresolved window reports UnresolvedLossWindow. Physical seals can be checked, but input_quality MUST be Unknown and dependent data cannot claim gap-free completeness. No right boundary is not zero loss. NoKnownLoss is forbidden whenever any Gap exists; unresolved windows also prohibit GapsRecorded as a fully accounted quality label. CaptureAttempt=u64::MAX can be last; advancing afterwards is AttemptCounterExhausted, no wrap.

### 8 — SegmentSeal

No Context: `prefix_frame_count:u64,prefix_physical_len:u64,prefix_crc32:u32,prior_record_no:u64,has_known_gap:bool,is_final_segment:bool` (30 bytes).
Prefix is all segment frames before seal, from offset0. Count/physical length include full trailers; prior_record_no=seal.RecordNo-1. Aggregate CRC includes only each prefix frame's header||payload, EXCLUDES its trailer (including ready trailers would produce unwanted CRC residue behavior). Seal has its own usual frame CRC. has_known_gap equals presence of Gap in segment prefix. Cannot seal with unrecorded accepted inputs.

### 9 — SegmentStart

No Context: `archive_id:[u8;16],capture_session_id:[u8;16],clock_id:u32,previous_segment_no:u32,previous_segment_seal_record_no:u64,previous_segment_seal_frame_crc32:u32` (52 bytes).
Only offset0 next segment; IDs/clock exact match, segment number previous+1 checked, RecordNo continues dense. Previous seal exists, own frame CRC/RecordNo match, final=false. Definitions/config/accounting/state inherit verified prefix. A detached segment is not a self-contained canonical archive.

### 10 — ArchiveSeal

No Context: `expected_segment_count:u32,prior_frame_count:u64,total_prefix_physical_bytes:u64,prefix_crc32:u32,prior_record_no:u64,input_quality:u8` (33 bytes).
Quality NoKnownLoss1/GapsRecorded2/Unknown3. NoKnownLoss requires no Gap; GapsRecorded requires a Gap and no unresolved local window; Unknown does not imply no loss. NoKnownLoss is absence of RECORDED known loss, not proof of exchange continuity.
Only immediately after final SegmentSeal in that same segment; only this record allowed after final seal, exact EOF afterwards. Prefix includes all earlier segments/frames, including segment starts/seals. Count/physical lengths include trailers; aggregate CRC again excludes each trailer. expected_segment_count=final_segment_no+1 and prior_record_no=own-1, checked. Verify every value against parsed chain, not trusted declarations.

## 5. Recovery and artifact limitations

Separate physical_completion (Incomplete/Complete), input_quality and canonical applicability. Complete+GapsRecorded does not restore book data. `last_good_offset` is the end of the last frame passing framing/CRC/payload/reference/order/accounting checks, with segment/local/absolute offsets and last accepted RecordNo. A framing-only scanner has a separate `framing_good_offset`; it cannot claim canonical success while artifacts are missing. Missing required artifacts stops semantic resolution with Blocked at the dependent record; physical scan may continue with explicit limitation.

| Observation | Result |
|---|---|
| Empty | NoArchive,Incomplete,offset0 |
| EOF within header/payload/trailer | TruncatedTail at incomplete frame; last_good previous boundary |
| Oversized length/nested count/checked offset overflow | LengthError BEFORE allocation; previous good boundary |
| Bad magic/flags/reserved/checksum/noncanonical payload | Corrupt/InvalidPayload/ChecksumMismatch, stop; even last frame not silently dropped |
| Unsupported schema/frame/kind/control | Unsupported, no suffix application |
| RecordNo/SegmentNo/reference/seal mismatch | OrderOrChainError,Incomplete |
| Invalid attempt accounting | exact section4.1 diagnostic; semantic offset before offending input, no suffix acceptance |
| Valid EOF boundary without ArchiveSeal | ValidPrefixIncomplete |
| Valid SegmentSeal only | SegmentSealedArchiveIncomplete |
| All seals/IDs/counts/lengths/CRCs plus exact EOF consistent | physical Complete with separate input_quality/applicability |
| Extra bytes/segments after ArchiveSeal | TrailingDataError,not Complete |

No magic-scan/skip/rejoin after middle corruption. Valid prefix can be used only as explicitly incomplete diagnostic input. Deleting whole final frame+seals, ArchiveSeal or final segment never yields Complete. Completely absent archive cannot be detected without an external inventory. Truncated-tail classification does not prove cause (length might have been corrupted). CRC/seals do not prove malicious-edit resistance, artifact validity or power-loss durability.

## 6. Watermarks, mode and finite StorageFence

Option<RecordNo> describes CONTIGUOUS achieved prefix, None unknown, not0. Known frontiers obey durable<=flushed<=written<=appended<=accepted. Accepted: RecordNo assigned by bounded owner. Appended: complete logical canonical frame. Written: writer accepted bytes, possibly userspace. Flushed: delivered to OS, not power-loss guarantee. Durable: successful platform sync/required metadata protocol.

Partial write/flush/sync does not advance its frontier. A trusted known stronger completion covers weaker prefix, never vice versa. None observation does not erase an existing known bound or establish new success. Explicit known regression is an error. Cannot preserve a dense log after losing an admitted RecordNo by substituting a new payload/GAP under its identity. Restart does not inherit in-memory success.

Buffered permits selected Written/Flushed/Durable gate with honest labels. GroupSynced and SyncBeforePublish require Durable; weaker config is INVALID_CONFIGURATION. GroupSynced batches storage operations; it does not allow early volatile publication. No latency/fsync interval is asserted.

A finite pure contract, not OS implementation:

```text
prefix through r20 -> actual chosen storage gate reached for20
r21 RecordingEvidence(through20) -> reducer computes candidate(frontier21)
actual storage operation reaches chosen gate through21
StorageFence(archive,session,gate,through21) -> guard may release that candidate
```

The candidate's full basis includes r21 if it affects state/time/readiness, so durable20 alone is insufficient. Final fence is a typed storage operation completion on an ALREADY DEFINED prefix, NOT a WAL input, RecordNo, clock sample or signal. It triggers no automatic RecordingEvidence-about-itself cycle. Every successful final fence must originate from the future verified storage boundary, not a parsed field/CRC/synthetic token. The pure guard checks scope, required gate, nonrevoked candidate and known contiguous achieved prefix including candidate frontier. Self/future receipt, absent fence, weak/wrong-scope/future/regressing fence or invalidation while waiting => no permit. Precise relations/candidate identity are in health section5.

Replay can reconstruct candidates/recorded observations, not historical sync or external send success. Crash between fence and send has Unknown delivery; neither exactly-once nor automatic resend is promised. OS completion provenance, batching, sync/flush/metadata and crash tests remain REC-001. SPEC now defines the finite semantic relation instead of deferring that ambiguity.

## 7. Rotation, finalization and remaining OS scope

Rotation stops admission into closing segment, drains accepted prefix, appends SegmentSeal(final=false), completes chosen flush/sync protocol, then starts next segment referencing seal. No skipping data to rotate. Platform metadata persistence is separately required before advertising durable creation.
Finalization drains accepted inputs, appends final SegmentSeal/ArchiveSeal, flushes/syncs bytes and required metadata successfully before advertising durable finalized storage. Parsed physical completeness is a byte property, separate from an external durable-finalization claim. Failure leaves storage completion unknown/incomplete even when some bytes can later scan as a complete sequence.
File creation/rename/directory sync/manifest atomicity/actual crash tests belong to verified OS-specific REC work. Compression, encryption/authentication, archive repair/merge, persisted normalized projections and append-after-restart require a new accepted revision. No Windows claim follows from Linux CI.

## 8. W01 golden — unchanged bytes and checksum

W01 synthetic, frame/schema1; archive16x01,session16x02,clock1,SyncBeforePublish3,previous=None; RecordNo1,SegmentNo0. Header32,payload38,total74. ArchiveStart only, NOT a complete archive.

```text
0000: 50 53 52 57 01 00 01 00 01 00 00 00 26 00 00 00
0010: 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
0020: 01 01 01 01 01 01 01 01 01 01 01 01 01 01 01 01
0030: 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02 02
0040: 01 00 00 00 03 00 13 c4 02 9e
```

CRC(header+payload)=0x9E02C413, trailer13 c4 02 9e. CRC(ASCII123456789)=0xCBF43926; empty=0. Whole-frame-with-trailer CRC=0x2144DF1C, hence aggregate excludes trailers. The original checkpoint checked zlib plus bit-loop before Rust codec execution existed. Final SPEC-001 includes the accepted memory-only codec plus multi-frame/segment and each-offset truncation assertions; production filesystem recorder/durability remains downstream.
