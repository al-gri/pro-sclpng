# Content-bound artifacts v1 — descriptor proposal

Status: **PROPOSED**. Proposal revision **2**, Architecture A3 / R4 with A1/R3 scope and time bindings.
Links: [events](../market-data/events-v1.md), [health](../market-data/data-health-v1.md), [WAL](../recording/wal-v1.md), [AF fixtures](../../tests/fixtures/domain/artifacts-v1.md), [review vectors](review-vectors-v2.md), [ADR](../../docs/adr/0002-domain-event-wal-contracts.md).
Formats and pure relations only;no production hash,loader,verifier,network fetch or event/health model is implemented.

## 1. References, identity and verification stages

ArtifactRef is exactly ASCII `sha256:` followed by64 lowercase hexadecimal digits (71bytes). Wrong prefix,length,uppercase,whitespace/nonhex =>InvalidArtifactRef. It fits existing Token128;CRC32 is never an artifact identity hash. SHA-256 is Architecture's content-binding choice;primary reference [NIST FIPS180-4](https://www.nist.gov/publications/secure-hash-standard).

The WAL ref hashes DESCRIPTOR bytes;the descriptor specifies the exact length and SHA-256 of a SEPARATE body. There is no descriptor self-hash/self-length recursion. Hash stored bytes without case/Unicode/newline normalization. An optional manifest descriptor length must match actual bytes but does not determine identity.
Namespace `(ArchiveId,kind,logical_identity,revision)` is permanently bound to descriptor/body digests. Identical ref reuse is permitted;different bytes for that key =>ArtifactIdentityConflict. ConfigVersion definition uniqueness is separate:Config2 may reference the same immutable Norm1.

ParsedRef -> BytesMatched -> ResolvedAndApplicable are distinct stages. Actual bytes/digest/length/kind/schema/dependency checks are required for BytesMatched;applicable supported interpreter/profile,scope and causal/temporal evidence are additionally required for ResolvedAndApplicable. Parsing a digest string alone never yields Verified. Content identity is not source authenticity,algorithm correctness or historical fsync.
MissingArtifact,ArtifactDigestMismatch,ArtifactLengthMismatch,ArtifactKindMismatch,UnsupportedArtifactSchema,ArtifactIdentityConflict,ArtifactDependencyCycle,ArtifactUnverified or missing applicability block dependent canonical replay/readiness. No network/latest fallback. Explicitly limited framing/CRC diagnostics may continue,not canonical success. Independent numeric parsing/grid conversion with supplied valid synthetic metadata does not depend on a production loader.

## 2. Exact descriptor and manifest encoding

All integers little-endian,no padding. TokenN,Opt and integer primitives match WAL. ArtifactRef is Token128 constrained by section1. Bounds checked before allocation:descriptor<=65536bytes,body<=16777216bytes,deps<=256,closure<=4096distinct descriptors,depth<=64. Exceeded limit=>ArtifactTooLarge,no partial closure success. These are proposed engineering limits.

Descriptor concatenation,exact EOF:

```text
magic[4] = ASCII PSAD
format_version:u16 = 1
kind:u8
logical_identity:Token64
revision:u32 > 0
body_schema:u16 = 1
body_length:u64
body_sha256:[u8;32]
provenance_kind:u8                 # Engineering1 / Synthetic2 / SourceVerified3
provenance_text_length:u16         # 0..1024
provenance_text:[u8;length]         # ASCII 0x20..0x7e,may be empty
 dependency_count:u16
 dependencies:[ArtifactRef;count] # sorted unique by71 ASCII bytes
```

Indentation is presentation only. Body digest is32 raw bytes,not a Token. Reject unknown tags,zero ordinary revision,invalid grammar,extra bytes,duplicate/unsorted dependencies. **The dependency list is exactly the direct refs REQUIRED BY THE KIND SCHEMA BELOW,including Config/FeedProfile refs resolved from evidence Scope,plus explicitly permitted optional Basis refs.** Those context refs need not be duplicated as text inside the Scope body;they resolve to already recorded immutable definitions. All dependencies are descriptor refs,no file paths,self-reference or cycles. Invalid dependency list=>InvalidArtifactDependencies.

| Tag | Kind | Logical identity / revision |
|---:|---|---|
| 1 | Normalizer | normalizer / NormalizerVersion |
| 2 | Config | config / ConfigVersion |
| 3 | FeedProfile | stream/<StreamId> / FeedProfileVersion |
| 4 | InstrumentSpec | instrument/<slot> / SpecVersion |
| 5 | Verification | stream/<id>/raw/<RecordNo> / positive evidence revision |
| 6 | Warmup | stream/<id>/anchor/<snapshot RecordNo> / positive evidence revision |
| 7 | Freshness | stream/<id>/basis/<raw RecordNo> / positive evidence revision |
| 8 | Basis | explicit Token64 / positive immutable revision |

Decimal path components have no leading zeros. ArchiveId comes from referring WAL,not a current registry default. Descriptor revision must equal the typed body's corresponding definition revision where present. Evidence revision is not an EventId.

Optional manifest: `PSAM[4],version:u16=1,entry_count:u32,entries`; each entry `descriptor_ref:ArtifactRef,descriptor_length:u32`,sorted unique,<=4096entries,exact EOF. It indexes locally supplied bytes;every WAL ref/dependency must resolve and match. Extra entries can be diagnosed/ignored and never activate anything. An arbitrary sidecar cannot override a WAL ref;no fetching/executing a locator is implied.

## 3. Definition bodies and mandatory WAL bindings

Normalizer body: exact opaque artifact/bundle bytes;optional Basis dependencies. A supported verified loader must identify its interpretation/executor. A label or hash is not executable logic;AF-N1 is explicitly a synthetic test label,not a production decoder.

Config body,exact order:

```text
proposal_revision:u16 = 2
new_config_version:u32,new_normalizer_version:u32,normalizer_ref:ArtifactRef
provenance_kind:u8
silence_rule:u8,freshness_deadline_ns:Opt<u64>
warmup_min_updates:Opt<u32>,warmup_min_elapsed_ns:Opt<u64>
allow_quiet_with_proof:bool,require_two_sided_snapshot:bool,recording_gate:u8
pending_max_frames:u32,pending_max_raw_bytes:u64,pending_max_outputs:u32,pending_wait_ns:u64
quiet_max_lifetime_ns:Opt<u64>
```

The normative encodings and unsupported/mismatched-policy behavior for silence_rule and recording_gate are in [DataHealth section 2.1](../market-data/data-health-v1.md#21-policy-byte-tags), shared with WAL ConfigDefinition. RecordingEvidence.watermark_kind is a different enum, not an encoding alternative for this body. AF-C1 bytes, digests and all nine mode/gate decisions are unchanged.

Mirrored fields MUST match ConfigDefinition. Its own evidence Token is deliberately excluded from this body,no self-reference. Common/body/WAL provenance_kind agree. normalizer_ref resolves exact declared revision. Dependencies:that normalizer_ref plus optional Basis refs for provenance. Extra pending/quiet fields are required external policy,not extra WAL bytes;health/events define their bounds. Descriptor must resolve before config activation. Preloaded future config does not activate early.

FeedProfile body: `stream_id:u32,InstrumentRef,channel:u8,feed_profile_version:u32,supported_normalizer_count:u16,supported_normalizers:[ArtifactRef;count],profile_basis:ArtifactRef`.
Supported list1..256 sorted unique;dependencies exactly that list plus profile_basis of Basis kind. Match registered instrument/channel/stream/profile. Current config's exact normalizer must be listed,otherwise UnsupportedNormalizerBinding. Profile basis must substantiate source output order,time/units,sequence/resync/quiet/UNKNOWN rules under a supported interpreter;real Bitget still BLOCKED_BY_MD_001. Listing a ref does not prove those facts.

InstrumentSpec body: exact WAL InstrumentSpec body AFTER Context,excluding final provenance Token. Its descriptor identity/revision match slot/spec;optional Basis dependencies provide source provenance. Its descriptor common provenance holds human/source origin. Numeric value types alone need no hash or loader.

Exhaustive bindings:ConfigDefinition.evidence->Config;StreamDefinition.provenance->FeedProfile;InstrumentSpec.provenance->InstrumentSpec;Verification/Warmup/Freshness.proof->respective kind. All are mandatory ArtifactRefs for canonical recorded definitions/evidence in revision2. Human/source prose goes in descriptor provenance_text,not a proof token. ArchiveStart/seals have no ref and remain byte-identical.

## 4. Evidence bodies and scope

Common Scope concatenation:

```text
archive_id:[u8;16],capture_session_id:[u8;16],clock_id:u32
stream_id:u32,instrument_slot:u32,EpochTag
config_version:u32,normalizer_version:u32,feed_profile_version:u32
invalidation_barrier:u64
```

Archive/session/clock match ArchiveStart. ConnectionId/BookId/InstrumentRef owners resolve through immutable stream/spec definitions,not equal numeric epochs. Barrier is current RecordNo frontier,not time;zero only pre-registration sentinel. BasisRecords=`count:u16,[RecordNo:u64;count]`,<=256,sorted unique,each actual record BEFORE referring proof;include raw,nonzero barrier and every asserted anchor/prior verification/evaluation input. Effect as_of separately conservatively covers complete prefix. Proof-kind dependencies REQUIRE exact Config and FeedProfile ArtifactRefs resolved by Scope (whose closure already binds normalizer);extra Basis refs are permitted only as described below.

Verification body:
`Scope,raw_record_no:u64,evidence_kind:u8(Snapshot1/Delta2),raw_sample_ns:u64,not_before_ns:u64,valid_until_ns:Opt<u64>,output_count:u32,output_sha256:[u8;32],continuity_basis:ArtifactRef,BasisRecords`.
Dependencies:current Config/current FeedProfile/continuity_basis(Basis),no undeclared replacements. Match wire stream/tag/raw/kind/profile. raw_sample equals original raw sample. not_before equals recorded evaluation maximum AT barrier;raw_sample>=not_before. A supported profile must ALSO substantiate membership of the new source resync,not just local admission after a barrier. In current scope,invalid binding is EvidenceScopeMismatch reported as ProofConflict by the frame-application guard;obsolete/pre-barrier proof is diagnostic-only by events4.
If valid_until=Some,require until>not_before and evaluation<until both at proof acceptance AND release. None means continuity-bound validity until invalidation,not infinite pending wait. Expired current pending proof=>ProofExpired and invalidation. Equivalent proof of an already applied frame remains no-op,not retroactive revocation. Missing/incomparable bounds cannot establish current resync.

Proof covers ALL source outputs in order. Snapshot exactly one snapshot;Delta>=1 homogeneous updates;count and ordered commitment match validated normalization before frame success. Mixed=>MixedFrameUnsupported,no partial success. Semantic equivalence compares scope/raw/barrier/ordered outputs/continuity/basis/bounds,not new proof RecordNo or human provenance;current conflicting decision=>ProofConflict.

PSCO commitment bytes: `PSCO[4],schema:u16=1,count:u32`,then source-order outputs. Snapshot: `tag:u8=1,entry_count:u32,entries(side:u8,price_ticks:u64,quantity_steps:u64)`;all bids before asks,sorted as events5. Update: `tag:u8=2,entry_count:u32,entries(operation:u8[Set1/Delete2],side:u8,price_ticks:u64,quantity_steps:u64 ONLY for Set)`. No padding/implicit fields. Existing checked counts/entry limits apply. SHA256 covers entire PSCO bytes. Scope/raw are bound in descriptor,not duplicated in PSCO. No Trade commitment in book proof v1. Actual hashing/comparison needs verified loader,not home-grown production crypto in domain.

Warmup body:
`Scope,snapshot_raw_record_no:u64,update_count:u32,elapsed_ns:u64,observed_at_ns:u64,BasisRecords`.
Dependencies:Config/FeedProfile plus optional Basis. Match wire anchor/count/elapsed;new witness observed_at=recorded evaluation maximum of referring record,elapsed=checked(observed_at-anchor ORIGINAL sample),count=exact capped progress,thresholds hold. Validity starts at recorded witness and ends at invalidation of anchor/scope/barrier;not retroactive. After Usable,equivalent witness no-op;no new progress. Current mismatched witness=>WitnessMismatch;obsolete witness never poisons recovered state.

Freshness body:
`Scope,anchor_raw_record_no:Opt<u64>,basis_raw_record_no:u64,freshness:u8,observed_at_ns:u64,valid_from_ns:Opt<u64>,valid_until_ns:Opt<u64>,BasisRecords`.
Dependencies:Config/FeedProfile plus optional Basis. Match wire kind/basis. Wire basisNone remains representable as diagnostic but cannot be applicable proof:MissingFreshnessBasis. Basis must already be applied,current;book quiet requires Some(current anchor),trades None with their own applied basis. Quiet bounds both finite,valid_from>=observed_at,until>from,observed_at>=basis original sample;current clock/scope and permitted lifetime intersection in health3. Missing/reversed bounds=>InvalidQuietBounds;policy denial=>QuietPolicyDenied;before interval=>QuietNotYetValid;at/after effective end=>QuietExpired. No auto-activation of a future proof without a new recorded evidence input.
For Fresh/Unknown/Stale,both boundsNone,observed_at=ORIGINAL basis sample;assertion must match ordinary freshness predicate or FreshnessAssertionMismatch. Clock mismatch=>IncomparableClock/FUnknown. Arrival time never rejuvenates data. A digest-valid but unsubstantiated observation stays ArtifactUnverified;it cannot force-ready.

Basis body:exact source-evidence bytes interpreted ONLY by pinned supported profile;optional acyclic Basis dependencies. Opaque facts remain ArtifactUnverified without a supported verifier. Synthetic resolver must explicitly enumerate supported facts;this is not a generic escape hatch to assert real Bitget properties.

## 5. Resolution and activation protocol

Future loader resolves local bytes reachable from WAL refs,checks descriptor hash,body length/hash,tags/schema,grammar,namespace,closure and supported interpreters,then supplies immutable applicable objects to pure guards. For body,check length before digest;wrong bytes fail even if version number matches. Missing/unverifiable dependencies block dependent canonical processing,not an opportunity to invent outputs/IDs. No network,latest-default or automatic invocation of artifact code is specified.

Required config/profile/normalizer artifacts must be applicable BEFORE raw/proof use. Dynamic evidence may create new immutable descriptors whose refs are recorded later;ArchiveStart need not predict future hashes. Early offline availability is distinct from recorded analytical activation. Canonical timeline still follows events6;alternate research replacements cannot reuse that namespace silently.
Std-only/Rust1.98.1 retained. SPEC defines parsing/identity/resolution relations and later synthetic guards,not production SHA/loader implementation. No new cryptographic dependency or custom production SHA-256. Actual verifier/loader/storage integration needs subsequent agreed scope;absent verifier =>production Unverified/Blocked.

## 6. Fixtures and renewed review

AF-N1/B1/C1/F1/V1 freeze exact descriptor/body bytes,hash refs and PSCO commitment outside the tested-code path. They are synthetic,not a full canonical archive or production normalization bundle. Negative named vectors bind missing/mismatch/scope/time/activation to exact expected diagnostics and no premature availability. Calculations are not runtime resolver/model tests.
This is an explicit semantic narrowing of existing Token128 fields and a NEW external PSAD/PSAM/PSCO proposal. No silent header/tag/CRC/golden change:W01 is unchanged. Config body proposal_revision2 distinguishes required revised policy interpretation. All new contracts remain PROPOSED;new descriptor/cap/interface choices require renewed Architecture/Integrator design approval.
