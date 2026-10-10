# REC-001F-3 implementation pre-allocation stop

Status: **BLOCKED_ALLOCATION_GATE / PARTIAL_IMPLEMENTATION / NOT_ACCEPTED**.
GitHub Issue #45 is authoritative. This is a stopped experimental increment,
not the intended capture/WAL/replay result and not independent QA.

## Authority and executor

Accepted base `273bfac01bc7a7954644e5270eb96cc99d787fab`, tree
`abfca247bbaa6fbad0c9f78d643dc9e8df7b3961`; related governance diff is docs-only.
Architecture reviewed at `830f8953dca540e751fbd5645d0532735b8c6527`.
Sole Integrator implementing sequentially, claim
https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6097721380.
One branch `feat/REC-001F-3-public-text-capture`; final head/tree and command
receipts are pinned in its one Draft PR and #45 after all commits.

Actual host: Windows 10.0.26200 x64, PowerShell 7.6.5, Git 2.55.0.windows.3,
gh 2.97.0; discovery used Get-Command. Docker Desktop 4.68.0, Engine 29.3.1,
desktop-linux, Debian 12 amd64 / WSL2 kernel 6.6.87.2.
Image `rust:1.98.1-bookworm`, image/RepoDigest
`sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e`.
Container `gate45-impl-20261010`, ID
`a29ad0b138348f8a8a2d3052d96c3dc75f772df72230f4fe3bde3cab42bba108`.
Rust 1.98.1 `48a229ceaefd4985c50990b14116b6d856af0985`, Cargo 1.98.1
`797e8a9bca276c1c9f9f738d2a20f484fa4eea9d`, Linux GNU target.
New `CARGO_HOME=/cargo` bind cache had zero files before locked preparation;
target directory `/evidence/target` is outside the checkout.

## Implemented and tested scope

Unactivated transport primitives validate passive server Text and produce a
fresh ring secure_random mask per outgoing frame. The send helper enqueues once
and flushes that same frame against the original deadline and atomic stop flag.
Its caller must supply counted authority, prepaid allocation and bounded I/O;
no owner callback, Ready proof or live capture entry point is provided.

Linux tests include the actual helpers and exact locked library sources:
partial writes/WouldBlock/physical flush, cancellation after three bytes,
expired deadline before writes, unsupported native opcodes without automatic
writes, fragments/RSV/masked server/binary/UTF8 refusal, oversized advertised
length before reserve, real numeric TCP slow drip with the unchanged first
progress +2s deadline, TLS12/13 final drain and TLS101 + first Text in one TLS
record, invalid Accept/redirect/extension refusals.

All loopback certificates, keys, fault inputs and clocks are explicitly
synthetic. The embedded localhost key is not an operational secret. The live
route mode is opt-in through GATE45_PROBE_MODE=live and an externally prepared
GATE45_NUMERIC_ADDRESS. Ordinary CI never selects it. It obtains only the first
Text, retains its bytes privately and closes by dropping the probe socket; it
does not claim recording-owner Close, ACK acceptance, Books50, WAL or replay.

## Exact stop and minimum continuation

rustls 0.23.45 `set_buffer_limit(65536)` controls application buffers, not the
decoded certificate list. `msgs/codec.rs` Vec::read uses infallible push before
certificate validation. TLS12/13 structurally accepted synthetic flights cause
decoded storage realloc from 393216 to 786432 bytes. Reserving old+new before
System allocation requires 1241088–1246007 bytes beyond the diagnostic baseline.
This is a conservative moving-realloc envelope, **not a measured retained
1MiB breach**, and the diagnostic baseline includes fixtures/harness.

The probe guard denies before System. Both guarded library calls terminate with
SIGABRT (6), shell equivalent 134. They cannot return an error through the
owner callback. Those outcomes are KILLED/Unknown/incomplete, never successful
Close or seal. The guard is diagnostic, not a production allocator: it is not
category-complete TLS lifetime accounting or whole-app ownership accounting.
Measured peak, baseline-plus-allowance and a passing expected-abort test do not
establish the required production bounds.

Consequently TLS/certificate pre-allocation enforcement is **BLOCKED** and the
absolute whole-application 8MiB composition remains **NOT_RUN**. Following #45's
failed-allocation-verification stop, the dependent owner-bound capture, ACK
binding, real WAL, two replay processes and live acceptance were not enabled.
This does not assert that every possible adapter is impossible.

The one next step is to continue this same branch/PR with a source-backed,
checked production allocation boundary: count retained TLS objects and
existing application ownership, reserve conservative realloc coexistence,
refuse before the over-budget allocator call, and expose a lawful error or
honest KILLED/Unknown/incomplete outcome without fabricating owner completion.
Prove the fixed 1MiB TLS allowance and total 8MiB under normal and adversarial
TLS12/13 inputs before enabling their dependent path. If that requires new
input limits, patched/substituted dependencies, public APIs or paths, first
obtain the explicit scope/ADR decision required by #45; do not relax budgets
or remove TLS12 silently. Then complete the remaining authorized implementation
and evidence in the existing PR. QA starts only after a real final implementation
head exists; its packet must bind that head/tree and private WAL receipt.

## Dependency and validation interpretation

Four exact candidates only: tungstenite0.30.0 handshake/no defaults,
rustls0.23.45 std/ring/tls12/no defaults, webpki-roots1.0.9,
signal-hook0.4.5/no defaults. Sole Integrator generated the root lock and added
explicit online locked fetch before offline checks in each of the three CI
jobs; no parallel root-file writer or unrelated upgrade.

Current cache audit verifies all 57 registry archives, sparse-index/lock
checksums, 2733 extracted files, licenses/provenance and resolved features.
43 registry packages are active on Linux. Maximum declared dependency MSRV
is 1.85; five undeclared MSRVs remain unknown. Compilation with pinned1.98.1
proves the active Linux closure's compatibility, not all-platform/minimum-MSRV
proof. Source inventory SHA256:
`936be4d5c22a3fc27639feba1bd0d8415fb91f3b2dc7fedce852eaaf1efa152c`.

Actual test totals and final source/log hashes are reported in the GitHub
receipt; 577 historical tests are not reused as a new-head result. Frozen
F-1/F-2, crate APIs/specs/ADRs and root toolchain remain byte-identical to base.
Missing captured WAL/evidence/replay digests are explicitly NOT_RUN, not empty
or synthetic replacements. usable_data remains false; M1 is not accepted,
#21 is not launched, M2 remains BLOCKED, merge/acceptance remain with the owner.

---

# ALLOC45-D1 — production allocation boundary / scope decision

Date: 2026-10-10. Sole Integrator / implementation executor #45; existing
`feat/REC-001F-3-public-text-capture`, Draft PR47. This dossier supersedes the
previous allocation continuation recommendation, preserves its historical
evidence, and proposes a decision; it is **not an accepted ADR or gate PASS**.

## Exact source and status

- Actual accepted main/base: `273bfac01bc7a7954644e5270eb96cc99d787fab`;
  tree `abfca247bbaa6fbad0c9f78d643dc9e8df7b3961`.
- Runtime inspected/built/probed: `e6ccef18075f56d73f42f043dfcd506039a975c0`;
  tree `e53017a5c747680d630d4620bc553d619831a9ad`.
  The publication commit changes only this permitted handoff. Its actual
  head/tree are recorded in the new #45/47 receipt, not guessed here.
- [#45](https://github.com/al-gri/pro-sclpng/issues/45) remains the authority
  for files_allowed, acceptance and stops; [sole claim6097721380](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6097721380)
  remains the only implementation claim. [Historical receipt6097976919](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6097976919)
  and CI38055358740 remain exact-e6 evidence, not a new runtime or QA result.
- **SOURCE/LIFETIME RESEARCH: scoped PASS. PRODUCTION ENFORCEMENT: absent.
  PRODUCTION ALLOCATION GATE: FAIL / NOT_PROVEN; BLOCKED_SCOPE_DECISION.**
  This is an uncovered category/pre-admission stop, not a demonstrated
  retained-1MiB breach or proof that every adapter is impossible.
- Capture/WAL/two captured replay processes, dependent live activation,
  independent QA and final-implementation acceptance: **NOT_RUN**.
  Expected-abort repetition: **NOT_RUN**. No production activation or policy
  change was made. Draft PR47 stays open; no merge or issue closure.

## Budget and accounting contract

Keep exactly `H=1,048,576` certificate/handshake bytes, `65,536` bytes for
each TLS application buffer, `T=8,388,608` checked retained application
allocation bytes. The H and buffer sublimits are **inside T**. Diagnostics
remain `131,072` bytes inside T. Do not subtract a test baseline: any TLS
config, root descriptors, owners, queues, storage, decoder or evidence that
exists in production participates from its creation to its actual release.
Borrowed static roots/code, stack, native allocator overhead and OS memory
are disclosed separately; this is not an RSS/process memory cap.

Count each heap backing by actual capacity/layout once, including heap
control blocks and collection descriptors. Inline layouts below are
layout facts, not extra heap charges when embedded in an already charged
allocation. True clones allocate independently; aliases retain the same
backing. An Rc/Arc inner value drops at last strong reference, while its
control allocation can survive until the last weak reference. A phase
transition does not release or reclassify a still-live object. A moving
realloc must reserve the entire new allocation while old is still charged,
then release old only after its free; allocation failure releases only the
new reservation. All arithmetic/reservations must be checked and atomic
with respect to competing admissions. Do not sum overlapping diagnostic
owner/supervisor reports with a heap ledger.

The table inventories current library/probe objects and accepted components
that would be used by the application. **It does not invent live instances
of absent production components.** `C` means actual backing capacity;
`sizeof` means the verified Linux amd64 layout. `A` means included in T;
`H` means included in both H and T. A precise category-to-allocation map for
TLS auxiliary state is still unimplemented and part of the proposed review.

## Source-backed object / lifetime / admission table

| Object, capacities and count | Owner; required category | Creation → actual release | Simultaneously retained bytes; aliases/phase | Admission BEFORE allocator; source/test |
|---|---|---|---|---|
| Root descriptors: 121 TrustAnchor entries, 72 bytes each; borrowed root DER | RootCertStore / verifier / config; A | RootStore extension during config construction → final owning verifier/root store drop | At least 8,712 copied descriptor bytes; actual Vec capacity and Arc metadata additional. Static DER is borrowed and separately disclosed | No T admission. Layout probe + R1, D1/D4/D5 |
| CryptoProvider, verifier, ClientConfig and config clones | Config/provider/verifier Arc owners; A, with handshake working state assigned explicitly | Provider/config builder before connection or ACTIVE → backing final free | Inline provider112/config344; algorithm Vecs, verifier/root descriptors and Arc headers separate. Arc clone shares; a rebuilt config allocates anew | No T admission. Builder also creates default session/compression caches before later resumption-disable; construction transient still counts. R1, D1/D5 |
| TLS connection/core/state, traffic secrets, verification intermediates, handshake messages | ClientConnection; A/H mapping incomplete | New connection → message/state replacement/drop or connection drop | Inline ClientConnection1,056; Box/state/Vec backing, DER/webpki/ring work coexist with input and config. Not bounded by inline size | No complete H/T admission. D1/D5/D6; normal TLS12/13 and adversarial modes |
| Deframer byte buffer, handshake joining spans | TLS connection; H/T while handshake, category retained until free | prepare_read/fragment joining → actual shrink/free/drop | Wire handshake maximum65,535 is logical; observed capacity65,536. Span Vec starts16 and grows; old+new resize/shrink capacity and spans coexist | Private infallible resize/push/shrink; wire cap is not heap cap. D2; tls12/13 fragmented traces |
| TLS12 certificate list descriptors | Handshake parser then peer certificate owner; H | Vec::read push before verification → borrowed-list drop; owned chain persists through traffic to connection drop | CertificateDer24; observed C16,384→32,768 descriptors: old393,216 + new786,432 = **1,179,648 required moving reservation**, before other H allocations | No H pre-admission/fallible API. D2/D4; tls12, tls12-fragment; not measured retained breach |
| TLS13 certificate entry/extension vectors | Handshake parser; H | Vec::read and owned extension conversion before verifier → entry conversion/drop | Observed same393,216→786,432 realloc; descriptors, extension lists and message input coexist. Empty-DER synthetic fixtures intentionally exercise parsing before BadEncoding | No H pre-admission. D2/D6; tls13, tls13-fragment, tls13-ocsp |
| DER copies, OCSP copies, retained peer chain | Certificate payload / client handshake / peer_certificates; H even in traffic | Borrowed DER into_owned copies; TLS13 extension OCSP becomes owned, end_entity_ocsp clones then client .to_vec copies → each owner drop; peer chain → connection drop | DER capacities + owned descriptor Vec + original input/list; OCSP300 fixture has multiple copies with overlapping lifetimes. Owned CertificateDer clone copies bytes; owned into_owned moves | No H pre-admission; verifier is downstream of parsing/copying. D2/D4/D6; TLS12/13/OCSP traces |
| TLS received plaintext and sendable plaintext/TLS chunks | CommonState ChunkVecBuffer owners; each TLS application buffer sublimit65,536, inside T | record handling/app write/read → each chunk popped/final drop | VecDeque descriptors plus sum full chunk capacities; partial read keeps full chunk and prefix_used. received_plaintext default16KiB; set_buffer_limit changes only two send queues. Logical length alone cannot bound capacities/descriptor heap | set_buffer_limit/append limit checks lengths, not a common retained-capacity ledger. D1/D3; transport probe, source-only capacity coverage |
| FrameSocket ingress BytesMut, shared Bytes metadata | FrameCodec and any received payload/clone; A | FrameCodec::new allocates131,072 → final backing alias drop, not socket drop | Test: payload1 + clone1 retains full131,072 backing +40 shared metadata after socket drop; baseline3831 restored only after both aliases dropped. Count backing once | Oversized advertised length rejected before additional reserve (test requests0), but initial/growth retained backing has no T admission. D7/D8; frame probe |
| HTTP request/response staging, handshake buffer and first-frame tail | Connect/upgrade caller, library MidHandshake and FrameSocket; A | Request/header/read staging → actual buffers/strings/drop; handoff may retain tail | Header8,192 limit and loopback tail65,536; live-probe staging C12,288; library initial4,096 read buffer. from_partially_read creates/reserves destination while source may still exist; account request/response fields and both backings | Logical header bounds only, no complete T admission. D7/D8, R1/R2; transport101+first Text coalescing |
| Client mask/payload and FrameSocket outbound encoded buffer | Caller, owned Frame/Bytes and codec; A | masked_text try_reserve_exact/copy → frame plus encoded output → completion/drop | One outbound payload≤4,096; payload backing, output capacity and sharing metadata may coexist. Partial flush retains buffered bytes to original deadline | Size checks / try_reserve_exact handle length/OOM, no common T reservation; codec reserve infallible. R1/D7; mask/partial-write/deadline probes |
| Session authority/scopes/Close and WorkCells; SessionTurn/leases | Accepted sole CaptureSessionAuthority and handle graph; A | Authority/supervisor registration → final Rc/Weak release of each backing | N1, M16, R4; W=M-N-1=14; scope bindings/registries/work vectors + Rc cells + cloned StreamBinding strings. Rc handles alias; SessionTurn8/WorkOwner24/CommandLease176 inline are not separate mallocs | Accepted work/memory-slot admission is not an application heap admission. R3/R4; accepted tests / source inspection, production composition absent |
| Supervisor queue/queued owners/registry/config/raw input | PublicWsSupervisor sole owner; A | with_limits/config binding/queue_text → actual pop/owner completion/drop | Queue/work owner capacities14; FixedRegistry4; one-stream raw bytes≤min(B,R×65,536)=262,144. Config/queue descriptors and payload copies coexist; inline supervisor264 excluded from extra heap charge | queue work admission precedes payload copy but does not reserve T for all backing capacities. R4; component tests historical, new composition NOT_RUN |
| Recording owner scope/bindings, closure handles and path metadata | CaptureSessionOwner; A | Owner construction (scope/binding Vec initial C4 each) and binding clones → final Rc/Weak/backing drop | Inline owner192; bindings160 inline each plus cloned text; scope16 inline each. Authority storage report overlaps authority/sink objects; do not count both reports and underlying allocations | Source ownership report is conservative diagnostic ownership, not T allocator enforcement. R3/R5; accepted tests, production composition absent |
| Filesystem sink/File/Writer validator and transactional clone | WalWriter, Rc sink handles and FileSink backend; A for application heap; kernel File separately | Writer construction/path, append clone before write → old validator drop after success or clone drop on failure; final owner drop | Box backend, path capacity, Rc fault/call cells, six validator BTree structures + bindings; original validator and clone coexist. Registry path/sink clone may alias or copy independently | No shared T allocation reservation; synchronous returned storage outcome still required. R5/R6; protected composition/WAL NOT_RUN |
| WAL encoding payload Vec and final frame Vec | Append encoder / Writer; A | Writer::new and per-field growth → append returns and buffer drop | MAX_PAYLOAD1,048,576; MAX_FRAME_LEN1,048,612; payload and encoded frame can overlap. Owner workspace report8×MAX_FRAME_LEN=8,388,896 plus registry metadata is conservative, **not measured allocation or impossibility proof** | Logical encoded-length checks before extending Vec; no retained-capacity admission across vectors. Need tight app-path proof or actual ledger; accepted API stays frozen. R5/R6; production WAL NOT_RUN |
| JSON parse strings/arrays/object slots and decoded wire fields | Decoder call + supervisor handler/queued raw; A | Parse push/owned String/levels construction → parser/decoded result actual drop | JsonValue32/object slot56/WireTrade136/WireLevel48. Tree/string/level clones coexist with retained Raw bytes. Existing source report at P65,536 is24,146,960 (conservative formula, not an actual heap measurement); do not sum it into T as actual allocation | Logical decoder limits do not prepay all infallible Vec/String growth. Requires actual bounded permitted composition; no accepted decoder API edit is authorized. R4/R7; production composition NOT_RUN |
| Diagnostic/evidence slots, clocks/profile/ACK binding/replay fields | Future application owner; A | Must be pre-reserved before attempt/refusal → evidence release after use | Diagnostics131,072 inside T; proposed evidence includes retained bytes, digests, path/config/binding strings and metadata. Current stdout cap is a diagnostic harness bound, **not this production slot** | Production components not yet instantiated; allocation/ownership/pre-admission NOT_RUN, not silently zero. R2/#45; no whole-app proof demanded before they exist |
| signal flag/hook, runtime/CLI config, standard-library and foreign/native work | Future app flag/hook/runtime, libc/ring/kernel separately | Registration/setup/operation → unregister/final free | Atomic flag can be static/inline; registration lists and config strings are heap if allocated. Thread stacks, code/static roots, allocator overhead and socket/kernel memory are separate disclosures | signal-hook dependency present but app path unactivated. No signal-handler owner/WAL work. Native/OS isolation NOT_RUN; R2/D5 and ancillary measurement below |

## Source references and integrity

Repository references are exact runtime e6; dependency paths below refer to
publisher archives pinned by unchanged Cargo.lock. Fresh archive SHA256 and
**every extracted source file** were checked against the publisher archive:
57 packages / 2,733 files, inventory SHA256
`58fd629ec430542ec37da0da4e20c34549d7a1ca9eeb9b3ef98bbd10d731fd52`.
Cargo.lock SHA256
`283ec6c56c00d0d0306cd2ff171bf9752f09a5195a5f215a4af981d46657dbed`.
The inventory format differs from the historical inventory; both bind the
same unchanged pinned source, and the historical hash is not overwritten.

- R1: [application transport](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/apps/radar/src/public_capture/transport.rs),
  especially masked_text and one-enqueue physical flush helper.
- R2: [integrated probes](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/apps/radar/tests/support/public_capture/mod.rs),
  config around537, diagnostic allocator935–1075, TLS fixtures1135+, live
  probe1365+ and staging1524+. [Test entry](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/apps/radar/tests/public_capture.rs).
- R3: [authority](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/crates/domain/src/capture_session.rs),
  registration3909+, ownership report4889+; cells/leases and final backing
  ownership are library semantics, not a production allocator ledger.
- R4: [supervisor](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/crates/market-data/src/ws_supervisor.rs),
  construction636+, queue2263+, checked report2449+, decoder formula2539+.
- R5: [recording owner](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/crates/recording/src/capture_session.rs),
  construction195/240, ownership/workspace312–340, filesystem sink482+.
- R6: [WalWriter](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/crates/recording/src/file.rs#L326),
  transactional validator clone333; [encoder](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/crates/recording/src/codec.rs#L280),
  [bounded binary writer](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/crates/recording/src/binary.rs#L193).
- R7: [JSON parser](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/crates/market-data/src/json.rs),
  array113/object143/string170 growth; [decoder](https://github.com/al-gri/pro-sclpng/blob/e6ccef18075f56d73f42f043dfcd506039a975c0/crates/market-data/src/decoder.rs),
  owned fields and levels475/498.
- D1: rustls0.23.45 `src/conn.rs:519` set_buffer_limit changes only
  sendable_plaintext/sendable_tls; `src/common_state.rs:87,1064`
  received_plaintext; `src/client/builder.rs:154–199` config/cache setup;
  `src/conn.rs:1173` Message::try_from before handshake verifier.
- D2: rustls `src/msgs/codec.rs:219–235` infallible generic Vec::read;
  `src/msgs/handshake.rs:1703+` TLS12 chain; `1800+` TLS13 owned extensions,
  into_owned and end_entity_ocsp; `src/msgs/deframer/buffers.rs:209–249`
  private Vec resizing; `src/msgs/deframer/handshake.rs:14,61,72,235`
  fragmented-handshake spans. `src/conn/unbuffered.rs:42–136` uses the same
  core.deframe/process_msg; switching to unbuffered alone does not remove
  certificate parser allocation.
- D3: rustls `src/vecbuf.rs:15–26,88,149,160` ChunkVecBuffer length,
  VecDeque<Vec>, append and partial-consumption retention;
  `src/common_state.rs:483` bytes.into_vec then append.
- D4: rustls-pki-types1.15.1 `src/lib.rs:1034–1049` owned Vec clone and
  borrowed into_owned copy. Owned into_owned moves; these are different
  lifetimes from Bytes/Rc/Arc aliases.
- D5: rustls0.23.45 client/config/provider/verifier setup and ring0.17.14;
  webpki-roots1.0.9 static roots; source/layout inspection does not measure
  every verification temporary/native allocation or prove their cap.
- D6: rustls `src/client/tls13.rs:1106,1174,1201` OCSP extra copy, verify,
  peer-chain retention; `src/client/tls12.rs:885,935` verify then retention.
  Phase completion is not peer-chain deallocation.
- D7: tungstenite0.30.0 `src/protocol/frame/mod.rs:126,137,180–200,228,260`
  initial131,072 BytesMut, handoff reserve, max-length check before reserve,
  split_to/freeze and outbound reserve; `src/buffer.rs:11–31` initial4,096.
- D8: bytes1.12.1 `src/bytes_mut.rs:76,626,1106,1543,1895` Shared,
  growth/promotion/release/clone; `src/bytes.rs:1325,1365,1448,1541`
  backing ownership/refcount. Size alone does not describe retained bytes.

rustls archive SHA256 (lock):
`0d41d731c7d2f962d1ccc364cec258de3c0e93b38c2fb3ba97ac74513048d634`.
Layout values are fresh debug Linux amd64 measurements against these exact
rlibs; no private accepted API was changed. Formula results remain diagnostic
upper estimates, not evidence of actual allocations or universal failure.

## Fresh source/test evidence and limits

Same Windows PowerShell7.6.5 → Debian12/amd64 Linux Docker / RustCargo1.98.1.
The previous stopped runner was no longer present. A replacement runner was
created on **the same pinned image digest**, existing source and cache; this
is restoration inside this task, not another executor or setup task.
Image/RepoDigest:
`sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e`.
Fresh Linux/tool/source identity, online locked fetch, offline workspace
build, integrated-test compilation and lock/clean checks all exited0.

Fresh commands (cwd `/repo`; Windows launches `docker exec
gate45-boundary-20261010 bash /evidence/boundary-preflight.sh` and
`... bash /evidence/boundary-probes.sh`, both wrapper exit0):

```text
rustc -vV / cargo -Vv / rustfmt --version / cargo clippy --version : 0
git rev-parse HEAD HEAD^{tree} / git status --porcelain : 0; exact e6, clean
CARGO_NET_OFFLINE=false cargo fetch --locked : 0
cargo build --workspace --locked --offline : 0
cargo test -p radar --test public_capture --locked --offline --no-run : 0
git diff --exit-code -- Cargo.lock / final git status --porcelain : 0; clean
GATE45_PROBE_MODE=<mode> cargo test -p radar --test public_capture \
  --locked --offline -- --exact implementation_integrated_probes \
  --nocapture --test-threads=1 : 0 for all eight modes below
source-audit.py : 0; 57 verified archives,2733 verified files
layout-inventory.sh : 0; type layouts/source formula, not production total
ancillary.py : 0; direct synthetic transport process sampling
```

| Mode | Result and exact scope | Log SHA256 |
|---|---|---|
| transport | 1 passed; normal TLS12/13, resumption disabled/0RTT false, native-control refusal, masks, same-frame partial flush/original deadlines, actual TCP slow-drip and HTTP101+first Text. Diagnostic transport only | `1213dff1e07c386fe0932d191b01fbc5545ab0f9ed268490addc8eb2380c3dff` |
| frame | 1 passed; initial131,072; advertised oversize requests0; one-byte payload + clone retains131,072 +40 through socket drop to final alias release | `590e8e60151b5d8e539d9420c8db95479b07eef9624cc4fcacdcd3bdb0173d51` |
| budget8m-refusal | 1 passed; request8,388,609 denied before System; try_reserve returns Err and capacity/live unchanged. Diagnostic calibration only | `fb392567805f714b5d1056ae53836349b26125029b5d0b314f62f09c8d10be91` |
| tls12 | 1 passed; 21,800 empty DER entries; wire65,427/fragment16,384; actual diagnostic peak1,063,697, request envelope1,456,457 | `e9d76a73cb54a80874ef3ce2ec07a73e8310b6a5ae27391a691f156688674112` |
| tls12-fragment | 1 passed; 20,000 entries/wire64,697/fragment64; peak1,058,006, request envelope1,450,766 | `00c90088a00d4f1c8f6151e313acc3abaecbd2c41606681cb50c0b3a4456c9ca` |
| tls13 | 1 passed; 12,800 entries/wire64,555/fragment16,384; peak933,576, request envelope1,325,888; returned InvalidCertificate(BadEncoding) after parsing | `f15dd7d7a6dcce1b512ec2b0f38b9b7b439a14211066061bf7fa0030011b6576` |
| tls13-fragment | 1 passed; 9,000 entries/wire62,395/fragment64; peak950,526, request envelope1,342,838; returned BadEncoding | `e6d3eb20c0fe3cac62848f4974061af41b0119dee74c25a9755c97dd6787fb4b` |
| tls13-ocsp | 1 passed; 9,000 entries + OCSP300, wire62,813/fragment64; peak950,950, request envelope1,343,134; returned BadEncoding | `4e5d27d67cb78ac4a4c8ef3f0adec7f039720a751b3770df00c7910ba4752e8d` |

These TLS absolute process figures include fixtures/harness/config and are
**not classified production H/T totals**. Certificate descriptor moving
reservation old393,216 + new786,432 exceeds H independently of a baseline,
but is a required pre-admission envelope, **not observed simultaneous
retention**. No claim is made from historical baseline-relative
1,241,088–1,246,007. Synthetic malformed certificate test PASS means the
test observed its expected parsing outcome, not certificate authenticity or
production gate acceptance.

Other logs: identity
`763a76f0d72f56d4ce2ccfa46eb1d25bc2e7be5a3be7ee734d96d040f1069c99`;
source identity
`04759961747045ad5fa533d8d56b573ead729bc0af1f6956b10691bba13fe78b`;
locked workspace build
`cffe41475ec9d803d4e06823c9275221f8166a326ba73b8ba335bdc73f6c9d9e`;
test compile
`977e3a1f43440ae3756b3c651d843c9e76e052fbe6e346715c7104e2ce044539`;
source audit
`86b97675e1dc2414d54a85805ef89a1db5581287eb1442b765bf879611d4eae5`;
layout
`46ed59f35da56194a881ebce7abfea42d929546e7d675b5dd2eee60da6f388fb`.
Successful fetch/lock-clean/clean-status logs are empty SHA256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
Fresh full workspace runtime/fmt/Clippy/release were not rerun locally for
this docs-only result; CI38055358740 provides historical exact-e6 checks
(three SUCCESS jobs;578 tests including11 expected compile-fail).
New publication-head CI is reported separately after readback.

Scratch failures were corrected before source/layout claims: initial public
type import compilation exit1, private StreamBinding import exit1; login
shell lost rustc PATH (underlying127, wrapper1); first audit assumed absent
cache .cargo-checksum.json (exit1), replaced by direct verified archive/file
comparison. None is production behavior or a suppressed passing result.
Initial ordinary shell lacked native gh/Docker access; sanctioned native
access on the same Windows host succeeded. No automatic approval rejection
occurred. No new dependency patch was tried.

## Separate RSS / stack / native / OS disclosure

Fresh direct transport-test process, /proc sampling10ms,199 samples, exit0:
sampled VmRSS/VmHWM6,240KiB; main-stack mapping VmStk132KiB; up to3 threads,
3 socket descriptors; owned TCP queue sampled RX1/TX0 bytes. RLIMIT_STACK
soft8,388,608/hard unlimited is an OS stack limit, not measured stack usage.
GNU size on the diagnostic test executable: text5,499,752/data250,464/
bss83,448 bytes, total5,833,664. These include test harness/fixtures and do
not measure the future production image. TCP queue occupancy is not socket
buffer allocation. VmStk is not all thread stacks or actual call-stack use.

RUSAGE_CHILDREN11,360KiB was read after the separate GNU size subprocess;
it includes fork/pre-exec launcher/helper high water and is **not target RSS**.
The corrected report labels this explicitly and preserves the original.
Native allocations/allocator metadata, precise thread stack usage and OS
socket allocation were **not independently isolated/measured**. Complete
production decomposition is NOT_RUN because that composition is absent.
These measurements disclose a scoped diagnostic observation, not a cap.
Ancillary report SHA256
`616eed8e76cfdec026053f546c01805b94998903a2ed589a95537b5c648a2fc0`;
direct transport log
`1d5eea049227d1a3b249e26ab67c45a6a34108772a42da34df7f41d098060a9a`.

## Why this is a scope stop

The present diagnostic allocator tracks LIVE but applies its CAP only in
ACTIVE intervals. It has no persistent allocation category/identity map.
Its load/check then System call then counter update is not an atomic
reservation; saturating/fetch arithmetic is not the checked admission
protocol required for competing callers. Fixed stack tracing/libc write
avoids diagnostic logging allocation, but does not establish a production
recursion/race/failure contract. Its pre-System denial calibration is real
and scoped; extending ACTIVE is not a production solution.

Pinned rustls parses the private generic infallible certificate Vec and
owned OCSP/DER transformations **before** its public certificate verifier.
An app verifier cannot make those earlier allocations fallible.
set_buffer_limit governs two logical queues, not this parser or complete
buffer capacities; the unbuffered API calls the same parser. Returning
null from the global allocator on an infallible Vec growth reaches the
allocation-error path rather than a returned dispatch error. Unwinding
from GlobalAlloc is undefined behavior under the
[Rust allocator safety contract](https://doc.rust-lang.org/std/alloc/trait.GlobalAlloc.html#safety).
Historical SIGABRT therefore stays **KILLED / Unknown / incomplete**, never
physical owner completion, returned Err, Close/Ready or seal.

This source-backed reason is sufficient to stop the **selected current
route**. It does not prove all possible adapter designs impossible. Inert
owner/WAL/application work is permitted in #45, but it cannot repair these
private dependency allocations in an allowed application file. Pessimistic
owner/decoder reports are not used to demand an accepted crate/API change
or an impossible whole-app proof for absent components.

## One concrete decision: ALLOC45-D1

**Owner/Architecture must ACCEPT or REJECT only the following explicit
scope/contract delta for a budgeted-client fork of pinned rustls0.23.45.**
Acceptance authorizes experimental implementation/verification in the same
branch and Draft PR47; it does not accept a gate, implementation, M1 or merge.
No fork/ADR/public API change has been made in this result.

Necessary delta for this chosen remediation:

1. Add `vendor/rustls-0.23.45/**` as a provenance-preserving copy of the
   exact119 publisher files, licenses included. Permit modifications only
   to the client allocation closure: `src/msgs/{codec,handshake,base}.rs`,
   `src/msgs/deframer/{buffers,handshake}.rs`, `src/{vecbuf,conn,common_state,error}.rs`,
   `src/conn/unbuffered.rs`, `src/client/{builder,client_conn,handy,hs,tls12,tls13}.rs`,
   `src/lib.rs`, its manifest, and new `src/budget.rs`,
   `src/client/budgeted.rs`, `tests/retained_budget.rs`. Proposed optional
   namespace: `rustls::client::budgeted`, with `RetainedBudget`,
   `BudgetedClientConfigBuilder`, `BudgetedClientConnection` and typed
   `BudgetedClientError::ResourceRefused`; all allocating builder/connection
   operations return Result and acquire admission before allocation.
   Exact signatures/category mapping are part of the requested ADR review,
   not permission to silently add another API/path.
   All other copied files remain checksum-identical; further modification
   or another dependency patch is a fresh stop, not implicit permission.
2. Permit one new decision artifact
   `docs/adr/0004-public-capture-allocation-boundary.md` documenting the
   precise optional client budget/fallible-admission contract and category
   map, failure/lifetime behavior, reviewed file list and validation.
   Accepted ADR0002/0003 and accepted project crate APIs remain unchanged.
3. In already allowed `apps/radar/Cargo.toml`, select this local fork with
   exact version `=0.23.45`, same minimal std/ring/tls12 features. Sole
   Integrator regenerates Cargo.lock sequentially. No root manifest,
   toolchain, ring/webpki/bytes/tungstenite substitution or TLS policy change.
   Existing allowed capture paths hold the application ledger and glue.

Required fork behavior/contract (proposal, not an existing API):

- Optional shared retained-budget handle accepted **before configuration
  and connection allocation**, with persistent allocation identity,
  original category and actual capacity/layout through last backing free.
  Budgeted client entry points return a typed synchronous resource refusal;
  no panic, allocator unwind or abort is an accepted completion mechanism.
- Reserve checked T and relevant sublimit in one transaction before the
  underlying allocation. Roll back on allocation failure, retain old charge
  through realloc, count aliases once and true copies separately. The
  ledger/logging/failure path must not recursively allocate and must be
  race-safe; a signal flag must not enter owner/WAL or a heap-using ledger.
- Fallible pre-scan/exact reservation for certificate descriptors where
  needed, plus fallible DER/OCSP copies, fragment spans, buffers, config and
  reachable client-state allocations. Reject on **the existing allocation
  budget**, preserving both TLS12/13, verification and original deadlines.
  No new wire/certificate-count/input cap. Parser budgeting does not bypass
  normal certificate validation. Retained peer chain remains H in traffic.
- Actual TLS application backing capacities/metadata honor65,536 per
  buffer and T. Category placement of auxiliary TLS state is explicit;
  no automatic reclassification on state change and no baseline exclusion.
- Over-budget refusal returns through the protected synchronous dispatch
  route, preserves Unknown and the original pending Close on Err, and does
  not invent physical cessation or Ready. Diagnostic evidence uses its
  pre-reserved slot. Any further unbudgeted dependency allocation/API/path
  discovered stops for a new explicit decision.

Migration: fork opt-in for the private capture adapter; non-budgeted API
behavior remains separately identifiable and cannot be called protected.
Keep upstream archive/file hashes plus fork diff provenance. Preserve WAL
v1 bytes/schema, accepted owner/supervisor/decoder contracts and frozen F1/F2;
no persisted-record or replay-profile migration is authorized. Config
construction, all normal handshake phases and final free are inside the
new accounting lifetime. No assertion is made that this proposed delta
already guarantees a full8MiB composition.

Validation required after acceptance, in this same implementation branch:

1. Exact-limit, overflow, refusal/rollback/free tests; moving old+new;
   aliases vs deep clones/phase transfers; concurrency admissions and
   allocator recursion. Show zero underlying allocator calls for every
   rejected over-budget request and lawful returned outcomes, not SIGABRT.
2. Real protected normal/adversarial TLS12/13, fragmented certificate
   flights/OCSP, config creation before former ACTIVE, traffic retention
   and final free traces; preserve deadlines and physical owner semantics.
3. Build necessary permitted inert application components and verify only
   their actual composition: owners/supervisor/sink/decoder/evidence + TLS
   including static/inline versus heap distinction, without report overlap.
   Measure/disclose RSS/stack/native/OS separately and mark unknowns.
   Whole-app PASS waits for its actual protected composition; missing
   future components are NOT_RUN, not an upfront impossible proof demand.
4. Provenance/features/locked fetch before offline pinned1.98.1 checks,
   fmt/Clippy/build/tests/release and decoded exact-head CI; frozen paths
   unchanged. Dependency/path/API expansion beyond this decision stops.

All fixed limits remain: N1/M16/R4/B262,144, payload65,536, outbound4,096,
capture60s/4096 messages/16MiB payload, WAL32MiB/32768 records; TCP2s + TLS3s
+ upgrade2s within7s, frame2s, passive quantum≤50ms, send1s, Close1s/
256 steps/two original-Close attempts, cooperative5s only after storage
returns. TimerA, sole owner/SessionTurn, native-control fail-stop, ACK
NotReconstructed and usable_data=false are preserved. M1 unaccepted;
#21 and M2 are not launched.

**One next step:** owner/Architecture records ACCEPT/REJECT of ALLOC45-D1
   in #45, approving the named optional budget API/new module/test paths and
   precise ADR contract if accepted. Until that decision, dependent implementation and
activation remain stopped. After an accepted delta the same Integrator
continues this branch; no second executor/PR or QA is started. Only after a
proved actual gate may bounded Text→WAL→two captured diagnostic replays
with genuine ACK binding become the next implementation substep. QA packet
is emitted only for a completed final implementation head.

## ADR-first docs continuation — 2026-10-10

This appendix supersedes the historical next-action packet above; all older
receipts, probes and ALLOC45-D1 history are retained. Architecture accepted
**ALLOC45-D1 AS_EXPERIMENTAL_DIRECTION / ADR_FIRST** in current
[#45](https://github.com/al-gri/pro-sclpng/issues/45), authorizing only this
Handoff and [PROPOSED ADR0004](../adr/0004-public-capture-allocation-boundary.md).
**Exact ADR contract/API and full implementation scope remain unaccepted.**
No vendor/runtime/manifest/lock/CI/accepted ADR/spec is changed in this docs
increment. Same sole Integrator [claim6097721380](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6097721380),
branch `feat/REC-001F-3-public-text-capture`, existing Draft PR47. No second
executor, PR, QA, merge or tracking-issue closure.

Design input publication head `9c1d7c5483f0989a18b62d143fbfc9278905f2e8`,
tree `140ae5c18650f9eef3c06b7c140d8578b21bfd8c`; accepted actual main/base
`273bfac01bc7a7954644e5270eb96cc99d787fab`, tree
`abfca247bbaa6fbad0c9f78d643dc9e8df7b3961`. Runtime remains exactly
`e6ccef18075f56d73f42f043dfcd506039a975c0`, tree
`e53017a5c747680d630d4620bc553d619831a9ad`. Subsequent delivery receipt in
existing #45/47 binds the actual ADR commit/tree, document checks and new
exact-head CI; neither file embeds its own future commit hash.

Unchanged environment: Windows PowerShell→Linux Docker/RustCargo1.98.1,
same Integrator/toolchain/access. Full environment setup is not repeated.
This stage performs source/document verification, not new runtime/protected
route experiments. Prior [receipt6099645626](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6099645626),
[evidence6099645308](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6099645308)
and [receipt6097976919](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6097976919)
remain historical, with their exact scopes. Prior exact9c
[CI38066971016](https://github.com/al-gri/pro-sclpng/actions/runs/38066971016)
is three jobs SUCCESS,578/0/0 including11 expected compile-fail; no gate or
independent QA follows from that regression result.

ADR0004 now proposes:

- Exact public Result signatures for ledger/config builder/config sharing
  and owned clones, connection I/O/processing/plaintext, borrowed peer chain
  and explicit H copies, local retirement and fixed typed error mapping.
  Exact private provider/verifier/root/parser/DER/OCSP/buffer/crypto signatures
  identify the fallible adapter obligations without pretending upstream
  direct-return Box traits implement them.
- Explicit T/H/three TLS-buffer/diagnostic membership; T8388608 includes
  ledger/core/headers/control/evidence, H1048576 includes retained chain/
  verification/crypto work, each TLS buffer65536, diagnostic131072 inside T.
  No baseline subtraction, phase reclassification, Rust-heap-as-native
  exclusion, double-counted aliases or realloc delta discount.
- Actual-backing lifetime through last free/Weak, checked transactional
  pre-admission/rollback and prepaid old+new moving growth/shrink; refusal
  has zero rejected underlying allocator calls and no heap recursion,
  quota panic/unwind/SIGABRT. Stable storage/contended free remain explicit
  design stops, not already proved implementation.
- Source-backed C00–C25 Layout/capacity/ownership/free/pre-reservation map
  over pinned rustls/pki-types/webpki/ring/std/callbacks. Unknown sizes/
  counts are NOT_ESTABLISHED, not invented safe envelopes.
- Phase-specific before/after-I/O refusal preserves accepted dispatch
  Unknown and existing original pending same Close; no fictional Ready,
  completion, retry or shutdown. FrameSocket/bytes/owner/WAL/decoder and
  the remaining actual application composition need their own proof.
- Exact original future119 publisher copies,17 .rs+Cargo.toml and three
  new vendor files; remaining copies checksum-identical. This is a proposal,
  never permission to implement or silently expand that list.
- Opt-in private capture migration, fork/update provenance and maintenance,
  complete validation matrix; all protected implementation cells NOT_RUN.

Concrete unresolved review decisions in ADR §8.1:

| Stop | Required disposition |
|---|---|
| U1 | Helper/message/hash/provider/verifier paths and direct-return crypto traits outside17 require an exact additional path/API decision or a source-complete equivalent confined design; no implicit expansion |
| U2 | pki owned conversions/clones and webpki EKU/OID error Vecs require lawful unchanged-verification pre-admission/fallible storage, or separately reviewed dependency delta |
| U3 | ring RSA BoxedLimbs/Montgomery/clone working heap requires exact simultaneous bounds and lawful null outcome; it is Rust heap, not native |
| U4 | Pinned stable1.98.1 storage/std interoperability, bootstrap/header/Weak and linearizable nonallocating deadline-safe reclamation need a concrete accepted mechanism |
| U5 | Future vendor workspace/features/provenance/offline manifest/lock integration is not instantiated or authorized |
| U6 | Remaining real application composition is unprotected by the fork; missing components NOT_RUN, accepted project APIs untouched |
| U7 | All individual Codec/Clone/error/drop/callback/feature call sites and Layout bounds still need exhaustive source-backed coverage |

Production gate **FAIL / NOT_PROVEN**. Fork/runtime/dependent activation
**STOPPED** until concrete ADR and complete implementation-scope acceptance.
Capture/WAL/replay/independent QA **NOT_RUN**. SIGABRT remains
KILLED/Unknown/incomplete. No measured retained1MiB breach or impossibility
of every adapter is inferred from diagnostic peaks/old+new arithmetic.

Preserved: TLS12/13/certificate verification; exact budgets and all #45
deadlines; TimerA/sole owner/SessionTurn; native-control fail-stop; ACK
NotReconstructed, usable_data=false; accepted WALv1/F1/F2/project APIs/
ADRs/specs byte-identical. M1 unaccepted; #21/M2 not launched.

**One next step:** sequential Architecture/owner review of PROPOSED ADR0004
on its actual publication head. The complete copy-ready review packet,
actual source/check/CI refs and hashes are published in existing #45/47.
Reviewer returns DESIGN_CHANGES_REQUIRED or
PROPOSED_CONTRACT_ACCEPTED_WITH_EXPLICIT_STOPS, plus explicit status of full
implementation scope; a direction-only acceptance cannot unblock code.
No implementation executor/QA is launched by preparing that packet.

## ADR0004 corrective continuation R1–R3 — 2026-10-10

This appendix preserves the entire preceding Handoff/history and supersedes
only its completed review-request next action. Canonical
[ARCH-ALLOC45-ADR4-D1-20261010](https://github.com/al-gri/pro-sclpng/pull/47#issuecomment-6100334905)
returned **DESIGN_CHANGES_REQUIRED / FULL_IMPLEMENTATION_SCOPE_NOT_ACCEPTED**
on reviewed/correction-base `d1c3cf543a8cf529eed825512ff16f667c24a348`, tree
`85bc560211e1536819452062e9347db986888318`. Direction remains experimental
ALLOC45-D1 / ADR_FIRST; exact ADR contract and implementation scope are not
accepted. Same sole Integrator claim6097721380, existing branch/Draft PR47.

Accepted actual main/base remains
`273bfac01bc7a7954644e5270eb96cc99d787fab`, tree
`abfca247bbaa6fbad0c9f78d643dc9e8df7b3961`; reviewed parent9c and unchanged
runtime `e6ccef18075f56d73f42f043dfcd506039a975c0` remain historical inputs.
The new corrective publication SHA/tree/parent and two related diffs are
bound by the following #45/47 receipt, never guessed in their own commit.

Only this Handoff appendix and [PROPOSED ADR0004](../adr/0004-public-capture-allocation-boundary.md)
change. Runtime/vendor/manifests/Cargo.lock/CI/frozen contracts/accepted
ADRs/specs/APIs remain byte-identical to d1c3. No second executor/PR/QA or
new application accounting bridge. No publisher archive/file audit is rerun.
Same environment/toolchain/access: no full setup repetition or new local
live/protected/release suite for this docs correction.

| Required correction | Proposed document decision / unresolved proof |
|---|---|
| R1, §3/§3.1/§3.3/§4 | Explicit PROPOSED deadline/stop parameter delta on14 previously context-free methods; exhaustive27-method T/S/O/C policy table. Timed/setup/refcount/copy methods get the governing original caller context; setup is no bypass for accepted dispatch. Pure borrowed/scalar observations are bounded/nonblocking; local retire/Drop/free/rollback finish or truthfully retain existing accounting after stop/expiry, without allocation/new effects/deadline. Bootstrap uses inline original context before a ledger exists; private Op uses explicit arguments/existing phase facts thereafter. Bounded std/deallocator proof and actual caller mapping remain U4/U6 |
| R2, §3.2/§4/U4 | Each TLS-error child backing keeps its charge through actual free after safe public source extraction/replacement/moves, beyond wrapper/connection/config/user budget lifetime, aliases/Weak and leaks. Outer wrapper Drop cannot release live extracted backing. Allocation-attached header/deallocator is possible proposed enforcement, not proved stable std interoperability or a demonstrated fatal escape; original error semantics unchanged |
| R3, §7/U6 | Public TLS surface is not a complete application accounting ABI. Same-T FrameSocket/bytes/owner/WAL/decoder/evidence bridge is undefined/unaccepted; exact cross-crate ownership/admission/free contract and scope require separate review before integration/activation. This increment neither designs nor implements it; missing components NOT_RUN, no whole-app PASS demanded upfront |

U1–U7 remain **UNRESOLVED**; C00–C25 remains source inventory, not complete
closure. Historical future119 copies/17rs+Cargo.toml/3new files are not
implementation permission. Extra future path/API/dependency needs its own
exact scope decision. Signature snippets remain design declarations only.

Historical exact-d1c [CI38070738775](https://github.com/al-gri/pro-sclpng/actions/runs/38070738775)
has three SUCCESS jobs and578/0/0 including11 compile-fail; regression of
unchanged runtime, not exact-new-head CI or protected API proof. Following
receipt records actual new-head CI/status and its source applicability.
Historical [delivery6100154754](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6100154754)
and [audit6100200309](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6100200309)
remain untouched. One canonical full next-review packet is saved in GitHub;
current #45/47 receipts point to it instead of duplicating its full body.

Preserve H1048576/T8388608/three TLS buffers65536each/Diagnostic131072
inside T; actual layouts/metadata/Weak/last backing free/prepaid old+new;
no baseline subtraction/double-count. RSS/stack/native/OS remain separate.
TLS12/13/verification, all #45 limits/deadlines, TimerA/sole owner/SessionTurn,
Unknown/original pending same Close/physical cessation/native-control
fail-stop, ACK NotReconstructed and usable_data=false remain unchanged.
No new input/cert-count cap, budget increase or dependency substitution.

**One next step:** sequential Architecture/owner review of corrected
PROPOSED ADR0004 on the actual new publication head, focused on R1–R3 with
all U1–U7 stops retained. Production gate **FAIL/NOT_PROVEN**;
fork/runtime/dependent activation **STOPPED**; capture/WAL/replay/independent
QA **NOT_RUN**. M1 unaccepted; #21/M2 idle. No merge/auto-merge/force-push,
settings changes or issue closure.

## ALLOC45-U4-D1 — PROPOSED ownership feasibility dossier

Source input: `3a59dc5bccb6ee11891dd98e28c4983df6b89bbe`, tree
`d8223cb3a9183f6d03b6ca4e143dee5a6acbe9d8`; accepted main/base remains
`273bfac01bc7a7954644e5270eb96cc99d787fab`. Same sole Integrator/claim6097721380,
branch and Draft PR47. [D2 review6100686192](https://github.com/al-gri/pro-sclpng/pull/47#issuecomment-6100686192)
gave R1–R3 DESIGN_PASS / PROPOSED_CONTRACT_ACCEPTED_WITH_EXPLICIT_STOPS for
partial TLS requirements; it did not accept full implementation scope.

Added only [ADR0004 §12, ALLOC45-U4-D1](../adr/0004-public-capture-allocation-boundary.md#alloc45-u4-d1).
Entire prior ADR and Handoff byte prefixes are preserved. Actual publication
SHA/tree/parent/diffs/hashes, checks and new-head CI are bound by the canonical
receipt in existing #45/47; this input SHA is not a guessed publication head.

**DESIGN_FEASIBLE_FOR_BOUNDED_EXPERIMENT / PROPOSED / SCOPE_DECISION_PENDING:**
one prepaid, universally headered GlobalAlloc family, explicit fallible
System preparation, exact plan for a named infallible POD Box leaf and
deferred physical reclamation. A compatible deallocator frees original
base/physical Layout and retains ledger independently of extracted children.
Pinned official Rust1.98.1 source commit matches the existing compiler;
stable Box/Arc try_new remain unavailable. No experiment/component install
or repeated publisher audit was performed.

Exactly one next step: Architecture/owner decision on **ALLOC45-U4-CORE-E1**,
the separately proposed three-path isolated fixture scope: new
`apps/radar/tests/support/alloc45_u4_core.rs`, new
`apps/radar/tests/alloc45_u4_core.rs`, feature/target-only edit of
`apps/radar/Cargo.toml`. Nothing in those paths is changed now. E1 tests POD/
bytes/String, custom strong/Weak and real General(String) source extraction;
upstream Arc/full TLS errors/provider/parser/crypto and application composition
remain unprotected. Default CI would not exercise that nondefault target;
any CI delta needs a separately named decision.

All U1–U7 UNRESOLVED; ADR remains PROPOSED, full implementation scope
NOT_ACCEPTED, dependent activation STOPPED. H/T/buffers/Diagnostic and
accepted #45 bounds/deadlines/TimerA/sole owner/SessionTurn/same original Close/
Unknown/native-control/WAL/ACK NotReconstructed/usable_data=false unchanged.
Production gate FAIL/NOT_PROVEN; capture/WAL/replay/independent QA NOT_RUN.
M1 not accepted; #21/M2 idle; no merge or second executor/task/PR/QA.

E1 reclamation custody is explicit: a charged CoreReclaimOwner stays with the
fixture driver after user budget handles disappear, drains actual backing and
frees the ledger last. Premature custody loss retains pending/orphaned charges;
production owner binding remains U6, not an application bridge designed here.


## ALLOC45-U4-ARC-D1 — PROPOSED Arc-child dossier

The [ADR0004 §13 dossier](../adr/0004-public-capture-allocation-boundary.md#alloc45-u4-arc-d1)
records **PROPOSED / UNAVAILABLE_WITH_CURRENT_STABLE_PUBLIC_BOUNDARY /
BLOCKED_WITH_NAMED_DELTA: ARC-DELTA-01**. Locked rustls0.23.45's current
stable public OtherError Arc surface provides no established full
pre-allocation returned-refusal/bounded-sharing route. Conditional
allocation-attached lifetime accounting is separate: original class charges
must survive owning extraction, aliases, outer context Drop and Weak until
matching physical free returns. Unknown Layouts remain symbolic; arbitrary
destructor, custody, foreign/thread-exit and System latency remain stops.
No universal impossibility claim or implemented adapter follows.

[Docs-only transfer](https://github.com/al-gri/pro-sclpng/pull/47#issuecomment-6102972726)
and [AUTHOR-v2](https://github.com/al-gri/pro-sclpng/pull/47#issuecomment-6102980219)
appoint the current local Integrator as sole author of exactly these two
appends. Earlier claim6097721380 history is retained; runtime/shared-file
ownership remains frozen. Input HEAD4f599f5ab687ca1fddb43c2dd8a814e3cd70ad5a,
TREE8c9cf1f05ca9c85f140ad53087a925cc3a31d845; actual main
TARGETcae130b70f68a501c63e20ca109e421d2d7f8f4d. The containing commit's
identity, author checks and new freeze are saved after commit in PR47.

ARC-DELTA-01 names a later ownership/API decision at rustls
src/error.rs::OtherError and src/webpki/mod.rs::{pki_error,crl_error};
it is **UNACCEPTED / NOT_IMPLEMENTED**, neither a future edit allowlist nor
a sufficient complete closure. Existing document prefixes are retained.
ADR0004 PROPOSED, D2 DESIGN_PASS, completed E1 PASS_WITH_RESIDUAL_STOPS,
U1–U7 UNRESOLVED/FULL_IMPLEMENTATION_SCOPE_NOT_ACCEPTED and every accepted
budget/TLS/error/deadline/TimerA/owner/SessionTurn/Unknown/same-original-Close/
native-control/WAL/ACK gate remain unchanged. Production FAIL/NOT_PROVEN;
capture/WAL/replay/final production QA NOT_RUN; activation STOPPED;
usable_data=false; F1/F2 frozen; M1 unaccepted; #21/M2 idle.
No E1/runtime/probe rerun or source/API implementation is performed here.

Next after new candidate freeze: fresh independent STRICT documentary QA and
existing exact-head CI, then separate sequential Architecture/owner disposition.
