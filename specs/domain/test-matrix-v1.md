# SPEC-001 — proposed positive / negative test matrix

Status: **PROPOSED**. Proposal revision **2**, DESIGN_REVIEW_REQUIRED; not a report of executed domain tests.
Base: `6c520237d35865c79dba9e74fa64bd4c2c9e419f`.
Contracts: [types](types-v1.md), [events](../market-data/events-v1.md), [health](../market-data/data-health-v1.md), [WAL](../recording/wal-v1.md), [artifacts](artifacts-v1.md), [ADR](../../docs/adr/0002-domain-event-wal-contracts.md).
**Exact review traces and finding mapping:** [review-vectors-v2](review-vectors-v2.md). **Independently specified descriptor bytes:** [AF fixtures](../../../tests/fixtures/domain/artifacts-v1.md).

## 1. Checkpoint and fixture protocol

No new Rust value types, validators, model, codec or executable contract tests exist at this docs-only checkpoint. D1/D2 have isolated numeric implementation permission but are NOT_STARTED here. Event/health/WAL implementation awaits renewed explicit approval. All baseline and new named test rows are NOT_IMPLEMENTED / NOT_RUN as Rust tests. Offline byte/hash/length calculations are separately reported, not substituted for runtime assertions.

Every future fixture: id,origin=synthetic,schema_version=1,proposal_revision=2,concrete policy_version or not_applicable,input,exact expected result/error/state/IDs/cursor/available_at,rationale. No network,secrets,live clock or real Bitget constants. Synthetic resolver outcomes are explicitly supplied, not production verification. New review traces define complete initial state and effect/control cursor distinctions; no raw/control record gets a fabricated market EventCursor.
Future integration tests belong in crates/domain/tests/**, memory-only helpers in crates/domain/tests/support/**, fixtures in tests/fixtures/domain/** using manifest-relative includes. No tests only in the virtual workspace root; no new dependencies/features/build scripts. Documentation vectors alone do not prove any implementation.

## 2. Numeric baseline — D1/D2 retained

| ID | Input | Expected |
|---|---|---|
| N01 | tick0.05,price100.10 | PriceTicks2002,reverse100.1,correct SpecRef/units |
| N02 | step0.001,qty1.234 | QuantitySteps1234,reverse1.234 |
| N03 | 000100.1000 / increment0.0500 | canonical(1001,1)/(5,2),ticks2002 |
| N04 | price100.11,tick0.05 | OffGrid,no rounding |
| N05 | qty1.2345,step0.001 | OffGrid,not1234/1235 |
| N06 | empty,.5,1.,1e2,NaN,inf,comma,whitespace,Unicode digits | InvalidSyntax for each separate case |
| N07 | +1,-1,-0 | InvalidSign |
| N08 | 96 then97 ASCII zero bytes | first canonical numeric0;second InputTooLong before parsing;price0 separately ZeroPrice |
| N09 | 1.0000000000000000000 vs0.0000000000000000001 | canonical(1,0) vs ScaleTooLarge19 |
| N10 | counts1/u64MAX,increment1 | both valid PriceTicks and exact reverse |
| N11 | 18446744073709551616,increment1 | CountOutOfRange,no narrowing |
| N12 | u128MAX coefficient vs MAX+1 | first parses;second Overflow(CoefficientAdd) |
| N13 | append digit when acc*10>u128MAX | Overflow(CoefficientMultiply),no panic |
| N14 | zero increment,direct scale19,noncanonical metadata | InvalidIncrement / ScaleTooLarge / constructor noncanonical error per types;never division by zero |
| N15 | u128MAX atscale0,increment(1,18) | alignment Overflow,not saturation or big-integer workaround |
| N16 | u64MAX count * u128MAX increment coefficient | reverse Overflow(Multiply) |
| N17 | steps123,step1 contract,multiplier0.01 base/contract | exact base1.23 with units |
| N18 | multiplierNone,zero,wrong units,nonlinear request | UnknownMultiplier / InvalidMultiplier / UnitMismatch / UnsupportedConversion;grid conversion remains independent |
| N19 | intermediate product overflow;scale36 not reducible to<=18 | exact operation Overflow / ScaleTooLarge,no intermediate rounding |
| N20 | qty0 in numeric,SetLevel,snapshot,Trade,Delete | numeric0 valid;live levels/trade reject;Delete has no quantity field |
| N21 | same count under other identity/spec | IdentityMismatch/SpecMismatch before application |

Bounded exhaustive plan remains coefficient0..200,scale0..3,positive increments1..20,steps0..100,with exact count/reverse or OffGrid assertions; separate MAX/intermediate cases remain mandatory. These loops have NOT_RUN. Precise checked-constructor variant nomenclature must align with numeric API review, not an independent test-only naming scheme.

## 3. Identity / order / UNKNOWN baseline, aligned with revision2

| ID | Input | Expected / exact expanded trace |
|---|---|---|
| E01 | same symbol Spot/Perpetual | distinct InstrumentRef |
| E02 | Normal/RPI same instrument | independent BookRef/epochs/state |
| E03 | foreign owner/spec | IdentityMismatch/SpecMismatch,no borrowed usability |
| E04 | raw admission10/exchange_ts200 then11/100 | source order10->11;book effects may be later,NEVER timestamp-sorted;V-R1-REORDER |
| E05 | two book outputs in one raw | distinct SOURCE indices;distinct APPLY indices at proof release;V-R1-MULTI-DUP |
| E06 | identical/conflicting consumer EventId | no-op / IdentityConflict;proof duplicates separately use SourceApplicationKey |
| E07 | RecordNo duplicate/hole;source/output index hole | RecordOrderError/SubEventOrderError at first bad position |
| E08 | current clock/random varies outside inputs | canonical IDs/order unchanged;external clock is not a reducer input |
| E09 | config1/norm1->config2/norm1->config3/norm2 | V-R4-TIMELINE exact Context,IDs,cursors;one canonical timeline |
| E10 | missing/rebound config/profile/normalizer artifact | V-R4-MISSING/REBIND/PROFILE-NORM;Blocked,no latest |
| E11 | same time value,other clock/session | IncomparableClock,no fake cross-restart duration |
| E12 | late raw sample,Unix jump | original sample retained,evaluation max not rolled back;V-R3-LATE |
| E13 | future record/effect/sub-index reference | FutureCausalReference;as_of now CausalBasis,not raw EventCursor |
| E14 | missing timestamp/aggressor/link/RPI | Unknown retained,not local time/0/Buy/Sell/Proven |
| E15 | new capture vs replay old capture | new archive IDs allowed;old canonical archive stable;not exchange dedup |
| E16 | epoch next<=current or exhausted IDs | EpochRollback/Overflow,no wrap/current-scope transfer |
| E17 | >4096 entries or duplicate level | EventTooLarge/DuplicateLevel,no partial failed frame;V-R1-ATOMIC |

## 4. DataHealth baseline, aligned with A1–A3

Each row must assert all four health axes plus scoped barrier/anchor/progress and permit, not just one bool. H-A is an independent synthetic policy: StaleAfterDeadline,D10,min_updates2,min_elapsed5,quietfalse,two-sidedtrue,Durable. Verified means a resolved frame proof and actual apply step, not a raw snapshot accepted structurally.

| ID | Case | Expected / exact expanded trace |
|---|---|---|
| H01 | registered/Up/heartbeat without snapshot | FUnknown,BNoSnapshot,usablefalse;heartbeat not data |
| H02 | H-A snapshot sample0,two applied updates samples2/5,truthful witness at evaluation5 | Warming until witness;then Usable,progress2,elapsed5,Fresh;frame/step IDs asserted explicitly in concrete fixture |
| H03 | H02 plus recording receipt and final storage fence covering candidate | permit only after finite two-phase sequence,V-R2-FINITE/NO-FENCE;receipt alone is insufficient |
| H04 | current critical GAP/overflow | invalid barrier,clear pending/anchor/warm-up;V-R1-BARRIER/PENDING-BOUNDS |
| H05 | new post-barrier snapshot+new warm-up | V-R1-RESYNC;late old proof cannot substitute |
| H06 | epoch advance then old proof/witness | no restoration/no poisoning of recovered scope |
| H07 | shared connection fan-out,third unrelated stream | V-R6-SHARED/DOWN-FANOUT |
| H08 | Normal proof with empty RPI state | RPI NoSnapshot,not borrowed usability |
| H09 | sample5,D10,evaluation14/15/16 | Fresh/Stale/Stale exactly;V-R3-FRESH-BOUNDARY |
| H10 | UnknownOnSilence,None or finite expiry | V-R3-NONE/UNKNOWN-BOUNDARY;None not infinity |
| H11 | current bounded quiet proof,then expiry/obsolete scope | V-R3-QUIET/LATE/FUTURE/OLD;no automatic renewal |
| H12 | false warm-up counts/elapsed/wrong scope | WitnessMismatch or obsolete diagnostic;no force-ready |
| H13 | config/spec/normalizer activation | barrier/current-scope invalidation;V-R1-CONTEXT,V-R4-TIMELINE |
| H14 | Down then Up with pending snapshot | V-R1-DOWN-UP;Up does not clear barrier |
| H15 | Failed recorder cannot write GAP | RFailed,no permit,no fictitious durable GAP |
| H16 | recorder Healthy after gap,book not resynced | RHealthy,B remains invalid |
| H17 | self/future/None/regressing receipt | V-R2-SELF/FUTURE-ACK/NONE-ACK/REGRESSION,no permit |
| H18 | new archive/session restart | no inherited ready/candidate/fence/clock |
| H19 | unverified real Bitget profile | BLOCKED_BY_MD_001,not synthetic Verified;generic vectors independent |

C2 progress requirements: V-C2-PROGRESS/MAX,applied BookUpdate outputs counted,threshold cap and freeze after Usable. R1 requires also reordered/duplicate/mixed/expired proofs and bounded pending failure; all exact traces are retained in review-vectors-v2.

## 5. WAL baseline — full golden/recovery obligations retained

W01 is independently specified in WAL section8. W02 is STILL REQUIRED after approval: a small ArchiveStart/InstrumentSpec/StreamDefinition/ConfigDefinition/raw/control/GAP/final SegmentSeal/ArchiveSeal sequence WITH required artifact closure,plus a multiple-segment variant. Full bytes/offsets have not yet been produced; W01 and AF descriptor fixtures are not substitutes.

| ID | Input / mutation | Required assertion |
|---|---|---|
| W00 | CRC ASCII123456789/empty | 0xCBF43926 /0 |
| W01 | literal unchanged ArchiveStart74 bytes | header32,payload38,CRC9E02C413,last_good_offset74,ValidPrefixIncomplete |
| W02 | independently encoded full raw/control/Gap/seal chain plus multisegment variant | exact bytes,fields,offsets,counts/CRCs/chain;Complete+GapsRecorded only with accounted loss |
| W03 | unsupported frame/schema/kind/control | Unsupported at exact offset,no suffix application |
| W04 | bad magic/flags/reserved/options/bools/noncanonical payload/trailing bytes | specific Corrupt/InvalidPayload |
| W05 | protected or checksum byte flip | ChecksumMismatch,last_good previous frame boundary |
| W06 | L1048577/u32MAX or nested length/count over remaining | LengthError BEFORE unchecked allocation |
| W07 | absolute offset/count arithmetic overflow | checked Overflow/LengthError before wrap;checked usize conversion |
| W08 | W01 cut at every k0..73 | k0 NoArchive,otherwise TruncatedTail,last_good0 |
| W09 | complete W02 cut at EVERY header/payload/trailer offset | exact previous accepted boundary/no partial record;boundary EOF alone not Complete |
| W10 | corrupted middle frame then valid-looking later magic | stop at corruption;no magic scanning/suffix joining |
| W11 | delete whole last raw/control/seal/final segment | never Complete merely because remaining EOF is a valid boundary |
| W12 | wrong seal count/length/aggregate CRC/reference with recomputed own CRC | semantic seal/chain error;own CRC cannot authorize false counts |
| W13 | SegmentStart wrong archive/session/clock/segment | OrderOrChainError/IdentityMismatch,Incomplete |
| W14 | valid SegmentSeal only;extra bytes after ArchiveSeal | SegmentSealedArchiveIncomplete /TrailingDataError |
| W15 | unknown/count0/range overflow | unknown staysNone,InvalidLossCount or checked overflow;V-R5-* |
| W16 | local attempts1,3 without/with exact local Gap2 | UnaccountedAttemptGap / accounted loss;SourceGap cannot authorize it |
| W17 | loss AFTER RecordNo assignment | Failed/Incomplete,no replacement payload or continued dense-looking lie |
| W18 | partial write/flush/sync | corresponding watermark not advanced,continuous prefix ordering retained |
| W19 | all mode×gate pairs,receipt/fence boundary | V-R2-*;GroupSynced/SyncBeforePublish weaker gates invalid |
| W20 | aggregate CRC incorrectly includes ready trailers | reject wrong oracle method;aggregate header+payload ONLY |

R5 adds one-use unknown windows,initial boundary,overlap/count/scope-change/EOF cases with exact logical outcomes. Full binary offsets for those not-yet-encoded traces remain NOT_RUN,not fabricated. Framing-only success when artifacts are missing is not successful canonical replay.

## 6. Executed checks versus planned assertions

Historical КП1 calculated numeric illustrations and W00/W01 CRC,not Rust parser execution. This revision checked W01 literal length/CRC unchanged and five AF descriptor/body digests with Python hashlib/OpenSSL. It does not implement/execute the transition or recording models above.
Existing CI on a NEW final head must verify build/fmt/clippy/workspace tests/unchanged lockfile/clean checkout. At this stage those are the unchanged BOOT-001 baseline (15 Linux CLI tests,0 domain tests),NOT proof that the design traces are correct. Old head/base CI cannot substitute for the new run. Exact SHA and logs are post-commit PR evidence.
After explicit design approval,allowed typed values/conversions/pure validators/test models and assertions are implemented in the same branch/PR. Required commands remain:

```text
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo test -p domain --locked
```

Each invocation needs its own evidence;workspace does not claim standalone domain command executed. Local Rust unavailable =>NOT_RUN;Windows11/PowerShell5.1 separately NOT_RUN until owner evidence. Physical crash/fsync/power-loss NOT_RUN and out of implementation scope. Independent QA on actual implementation head remains mandatory.

## 7. Review ownership

[Finding → section → named vector → remaining work](review-vectors-v2.md#8-finding-to-change-mapping-and-remaining-review) covers A1–A3,R1–R6,C1/C2/C3. All changes are submitted for review,not self-closed findings. D1/D2 isolated permission does not release event/health/WAL implementation. Issue3 is not DONE;Issue6 remains independent QA. No merge is requested by this matrix.
