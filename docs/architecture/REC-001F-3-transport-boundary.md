# REC-001F-3: public transport boundary feasibility

Verdict: **API_COMPATIBLE_WITH_EXPLICIT_LIMITATIONS** for the single-shot,
Text-only diagnostic slice specified below. This is a source-level feasibility
decision, not implementation acceptance, a live result, or an approval of full M1.
Normal RFC 6455 Ping/Pong service, a complete WebSocket transcript, asynchronous
owner completion, and an unconditional deadline for blocking filesystem operations
are **not** supported by this verdict. The conditional ACR section identifies
those unsupported requirements precisely.

- Child: [#40](https://github.com/al-gri/pro-sclpng/issues/40); Parent:
  [#22](https://github.com/al-gri/pro-sclpng/issues/22).
- Authoritative assignment/packet:
  [existing ARCH-REC-001F-3 packet](https://github.com/al-gri/pro-sclpng/issues/22#issuecomment-6095975783).
  This report implements that packet; it does not replace or duplicate it.
- Reviewed main/base: `8baf610b4ac15122dfcdbfe07730bc30c6558d73`.
- Reviewed tree: `ef5ae29b1ab546641c50110c9053834ad2891219`.
- Branch: `docs/ARCH-REC-001F-3-transport-boundary`.
- Review date: 2026-10-10. One assigned Architecture reviewer; independent
  read-only replay and API challenge sessions supported the review.

## 1. Baseline and authority

The base includes accepted F-1 diagnostic replay and accepted F-2
synthetic/unverified capture composition. Runtime bytes match implementation
main `cfda8a29e29c2b15faf7350faa2fa7679d08915e`; the later docs merge does not
add a transport. The final [PR #38 receipt](https://github.com/al-gri/pro-sclpng/pull/38#issuecomment-6095953590)
supersedes the stale OPEN/UNMERGED wording in current PROJECT_STATE. The
[PR #39 integration receipt](https://github.com/al-gri/pro-sclpng/pull/39#issuecomment-6095083871)
and [fresh verification](https://github.com/al-gri/pro-sclpng/pull/39#issuecomment-6095262376)
establish only its partial synthetic/unverified acceptance. Historical packets
and handoffs remain evidence, including their then-current NOT_RUN states.

The Integrator separately verified exact-main [push CI 38039982818](https://github.com/al-gri/pro-sclpng/actions/runs/38039982818):
three successful jobs, exact base checkout, Rust/Cargo 1.98.1, 577 tests
(566 runtime plus 11 expected compile-fail), zero failed/ignored. That is
baseline evidence, not a run of a future transport. Local Rust/Cargo/rustup
are absent in this review environment. Local build/fmt/clippy/tests/release,
dependency installation, DNS, public connection, capture and live replay are
**NOT_RUN**.

Read authority at the exact base: AGENTS, PROJECT_STATE, ARCHITECTURE,
INVARIANTS, WORKFLOW, DEFINITION_OF_DONE and Integrator prompt; ADR-0002;
ADR-0003 including accepted Timer A §15A and §§18–20 restorations; domain
types/artifacts, events, DataHealth and WAL v1; both exchange evidence reports;
owner/supervisor implementations and corresponding tests; F-1 and F-2
profiles, drivers, tests, handoffs and the frozen F-2 packet. The prompt's
BOOT-001 wording and PROJECT_STATE's PR #38 wording need separate current-status
docs maintenance; neither reopens accepted code.

INV-01/03/04/05/06/07/08/09/10/11/21/22 remain effective. REC-001E and U-09/U-10
remain blocked; U-20 REST healing is forbidden, C-01 blocked and C-03 unknown.
No canonical book, quantity normalization, verifier, trading, or readiness
claim is introduced. `canonical_status=NotEvaluated`,
`canonical_applicability=BLOCKED_UNVERIFIED`, `usable_data=false` remain mandatory.

## 2. Actual API facts and consequences

Repository links in this table point to the reviewed base, not moving main.

| Actual surface | Verified behavior | Consequence for a real adapter |
| --- | --- | --- |
| [domain capture_session](https://github.com/al-gri/pro-sclpng/blob/8baf610b4ac15122dfcdbfe07730bc30c6558d73/crates/domain/src/capture_session.rs), `CaptureSessionAuthority::dispatch` | Consumes `CommandLease`; callback is `FnOnce(CommandView<'_>) -> Result<(), E>` while `&mut SessionTurn` remains borrowed. Callback success is `Dispatched`; every callback error is `DispatchFailed { effect: AmbiguousEffect::Unknown }`. | Complete the selected operation inside the callback. Copying a view into a queue and returning Ok is invalid. No later-completion API is available. |
| Same file, authority / turn | Authority uses `Rc<RefCell<...>>`; `SessionTurn` is affine. Equal IDs do not authenticate a foreign authority. Compile-fail tests prevent turn duplication and callback reentry. | One thread owns owner, supervisor, bound sink, socket and turn. A network helper gets no owner/turn and cannot call admission while dispatch is active. |
| Same file, `CommandKind` | Only Connect, SendText, Close and ReconnectAfter. Generic SendText("ping") minting is rejected; Timer A mints its one-shot Ping. | No WS opcode Pong, binary send or free Flush command. Text "ping" is the Bitget application heartbeat, not RFC opcode Ping. |
| Same file, Close | Callback Ok immediately sets original Close to Settled; Err returns original Close to Pending. Drop also returns leased Close to Pending. `CloseOwnerRef` retains authority/stream/connection/epoch/original ownership. | Close success must mean that this exact local epoch can perform no further I/O. A queued close frame, `write` acceptance, or planned worker stop is insufficient. |
| Same file, `AuthenticatedClosure` | Fields are private; no public producer. | External asynchronous closure cannot be authenticated by an app DTO. This does not block a truthful synchronous idempotent Close callback. |
| [recording owner](https://github.com/al-gri/pro-sclpng/blob/8baf610b4ac15122dfcdbfe07730bc30c6558d73/crates/recording/src/capture_session.rs) | `create_new`/`register_supervisor` mint one owner-bound sink/handle. `finalize` consumes one quiescence proof. `close_diagnostic` reports descriptor closure, original watermarks, unconfirmed suffix and undrained ownership separately. | Never append directly through an independent WalWriter. A closed descriptor is not proof of drained records or settled socket ownership. |
| [PublicWsSupervisor](https://github.com/al-gri/pro-sclpng/blob/8baf610b4ac15122dfcdbfe07730bc30c6558d73/crates/market-data/src/ws_supervisor.rs) | Public ingress is Connected, Disconnected, Text and Tick. Configuration validates lowercase `bitget`, Perpetual, `usdt-futures`, regular Books50. Raw ACK is persisted before classification. Exact text `pong` is a special control route. | First real slice must use this supported regular category; spot is not interchangeable. Binary/native control payloads cannot be passed as Text. |
| Same file, `classify_inbound` | Private bounded classifier recognizes matching subscribe ACK/error and Books50. Public market decoder does not parse ACK. | Capture can verify `SubscriptionAccepted`; replay cannot pretend the Books50 decoder understands ACK. See §9. |
| [file backend](https://github.com/al-gri/pro-sclpng/blob/8baf610b4ac15122dfcdbfe07730bc30c6558d73/crates/recording/src/file.rs) and sink | File write/flush/sync and metadata finalization are synchronous. No cancellation/deadline handle is exposed. | Network deadlines cannot establish a hard wall-clock bound for disk stalls. No successful finalization can be reported before the real calls return. |

The accepted contracts describe authenticated ownership and completion, not
exactly-once peer delivery. A completed local write remains separate from
subscription ACK; a local Close remains separate from peer Close acknowledgement.
Admission, record persistence and physical transport effects have three different
completion boundaries.

## 3. Selected transport design

Use one synchronous owner loop with one fixed socket slot keyed by
`(ConnectionId, ConnectionEpoch)`. Use a raw frame library, not an automatic
WebSocket responder. Every Connect and SendText runs to actual local completion
inside `owner.dispatch`; passive receive does not write application protocol
responses. Close destroys the exact epoch's local transport synchronously. No
background send task, detached socket clone, split read/write owner, second
market socket, automatic reconnect, or copied executable CommandView exists.

The concrete candidate route is `tungstenite::protocol::frame::FrameSocket`
over an explicitly driven `rustls::ClientConnection` and a deadline-aware
numeric-address TCP stream. `FrameSocket::read(max_size)` requires only Read;
its publisher source has no Ping/Pong/Close responder in that raw layer.
`FrameSocket::write` only queues a frame; `send` includes flush. These are
verified library facts, not proof that a future adapter is correctly written.
Client outbound Text must use a fresh unpredictable mask for every frame through
the library frame primitives (the raw layer has no client-role masking); retain
no outgoing batch and allow only the current leased text (at most 4096 bytes).
[Official FrameSocket API](https://docs.rs/tungstenite/0.30.0/tungstenite/protocol/frame/struct.FrameSocket.html)
and [publisher source](https://docs.rs/tungstenite/0.30.0/src/tungstenite/protocol/frame/mod.rs.html).

The application accepts only one final, unextended, correctly shaped UTF-8 Text
frame at a time. Continuation, fragmented data, binary, native Ping/Pong/Close,
RSV/extension/masking violations and oversized frames end this attempt with a
bounded diagnostic and local mandatory Close. No native Ping is answered as
text "pong"; no binary payload is relabeled as Text. This is an intentionally
limited diagnostic client, **not a generally compliant RFC 6455 client**:
RFC §5.5.2 normally requires matching Pong on received Ping. The slice aborts
that unsupported exchange rather than claim it handled it.
[RFC 6455 §§5.3–5.5, 7.1](https://www.rfc-editor.org/rfc/rfc6455.html).

On unsupported input, preserve a bounded original header/opcode/stamp/payload
diagnostic outside the WAL (payload at most the frame limit), mark the attempted
capture unsuccessful, and report `unrecorded_observation=true`,
`unknown_suffix=true`, `input_quality=Unknown`. Never invent a RawInput, known
QueueOverflow range, received Down, or successful capture. A physically sealed
WAL prefix can still be Complete/Unknown; that is not successful coverage of
the refused frame. If preservation of those extra opcode payloads **inside
accepted WAL** is required, use the ACR gate in §11 instead. The success-path
promise is raw market/ACK Text bytes plus the accepted contracted controls,
not every TLS/TCP/WebSocket wire byte.

Handshake must use the library's validated HTTP client handshake, keep
`Host`/SNI/certificate name `ws.bitget.com` and `/v3/ws/public`, refuse redirects,
extensions, compression, credentials and early data. Preserve bytes already
received beyond HTTP headers. In the proposed synchronous handoff, an adapter
stages at most 8192 header bytes through the first CRLFCRLF using
private one-byte plaintext reads, then serves the complete staged header in
large chunks to the validated handshake. It does not validate HTTP or replace
the library handshake; it prevents overread. Do **not** return one byte per
Read directly to tungstenite: its AttackCheck rejects more than 64 successful
reads with average below 128 bytes. The staged header has one cumulative byte
counter and absolute upgrade deadline; malformed/incomplete/overlong headers
fail before conversion. TLS's already-buffered plaintext stays in the same
stream. An implementation test must place the
first WS frame immediately after the upgrade response and prove no dropped
or duplicated byte. A naive high-level `WebSocket::into_inner` that discards
prefetched protocol bytes is forbidden. Failure of this preservation technique
is a stop condition, not permission for a homemade upgrade implementation.
[Publisher handshake source](https://docs.rs/tungstenite/latest/src/tungstenite/handshake/machine.rs.html).

Use rustls's explicit `read_tls`/`process_new_packets`/`reader` and
`writer`/`write_tls` boundaries. Do not use a convenience stream that silently
drives writes during a passive read. TLS handshake writes belong to the
authorized Connect; application Text and its TLS flush belong to SendText.
Unexpected post-handshake TLS traffic requiring a write is another fail-stop
limit for this slice, serviced by local Close rather than an unleased write.
Disable resumption/0-RTT; enforce hostname/root verification. Connect must drain
`wants_write` even after `is_handshaking` becomes false. Receive respects
`wants_read` backpressure and drains available plaintext before ingesting more
ciphertext. `set_buffer_limit`
bounds application write buffers, not every certificate/handshake allocation;
total memory proof still needs the implementation's library/source/allocator
audit. [Official rustls explicit-I/O guidance](https://docs.rs/rustls/latest/rustls/index.html)
and [ClientConnection API](https://docs.rs/rustls/latest/rustls/client/struct.ClientConnection.html).

## 4. Command and effect lifecycle

All callback budgets are measured against one absolute monotonic deadline,
including all partial reads/writes and retries within that invocation. Per-call
socket timeouts alone do not bound slow-drip loops. Never restart a budget after
WouldBlock, a partial write, library buffering, or TLS progress.

| Owner command | Callback may return Ok only after | Error/Unknown and cancellation | Epoch and shutdown ownership |
| --- | --- | --- | --- |
| Connect | Numeric TCP connect, TLS authentication and bounded validated upgrade complete; exact socket is installed in its slot. Then, after dispatch returns, sample/submit actual `queue_connected`. | Any error is dispatch Unknown, including partial TCP/TLS/upgrade. Destroy local temporary resources before returning; retain diagnostic stage and bytes-may-have-left. Never submit Connected from enqueue or TCP-only success. | Reject existing different slot/epoch; no detached connector survives callback. A stop flag cancels preparation/handshake, cleanup completes before result. |
| Subscribe / other SendText | Exact framed Text bytes, masking, TLS output and underlying write/flush have completed locally within budget. | Partial/error is Unknown; consume command, stop reading/writing that socket, no automatic resend of buffered suffix. ACK has not been established. | Check exact slot/epoch. Subsequent real ACK is a distinct stamped Text input. Closing/stale lease is revoked before callback by authority. |
| Timer A Ping | Same local SendText completion for exactly the authority-issued `ping`. | Unknown consumes Ping; no reclaim/retry. Stop socket, report effect Unknown; do not shift Pong deadline to dispatch time. | Held Ping may be revoked by Pong, Down, Closing or epoch change. Dispatch revalidates current generation before effect. |
| Close | Exact socket and all its local pending operations/buffers are removed/dropped; no local future can emit more bytes. An already absent exact epoch is idempotently ceased. | Failure to establish local cessation returns Err/Unknown; original owner stays Pending. At most two attempts at the **same** original Close; refusal remains explicit. | Never close a newer epoch in a reused slot. No peer ACK or clean TLS/WS handshake claim. No constructor of AuthenticatedClosure is needed. |
| ReconnectAfter | Only a bounded local observation of the authorized backoff boundary, or an explicit policy refusal before execution. | Do not say enqueue is completed delay. This single-shot slice performs no future Connect: settle/cancel an unactivated inherited plan through existing `cancel_pending_disconnect` where legal; otherwise retain NotReady diagnostic ownership. | No second socket. Never drop an active frozen Timer/partial H1 plan as though settled. At capture stop, pending plan is explicitly canceled only if the accepted API permits it. |

For a policy-denied effect, return/report refusal; never manufacture Dispatched
to make quiescence ready. Denied dispatch returns the rightful lease, Revoked
does not run the callback, AlreadySettled does not issue another effect. Results
and leases count against retained work until lawfully released. At finalization
inspect the outer disposition even if an inner Result is Ok.

## 5. Clock sampling, Timer A and wake-ups

Create one monotonic origin at session startup and one ClockId; a restart is a
new archive/session/clock. Every real frame completion, successful full upgrade,
peer EOF/read failure and wake-up gets a fresh checked pair from Instant elapsed
and SystemTime. Sample when the owner observes that input, before admission or
parsing, not after its WAL write. Timestamp multiple already-buffered frames
individually in their observed order. UNIX jumps remain original signed samples;
they never order input or supply durations. Checked nanosecond conversion/time
overflow is terminal, never wrap or a synthetic constant.

The loop's 50 ms maximum passive-I/O quantum is a wake-up mechanism, not another
scheduler or heartbeat FSM. At a wake-up, sample and call existing `queue_tick`,
then service original admitted work in order. Only the authority decides NotDue,
AlreadyQueued, the original TimerId/deadline, Ping entitlement, and Timeout
stages. Preserve ADR-0003 §15A: Ping at original observation +30 s, Pong deadline
at original Timer observation +15 s, same-scope FIFO, one-shot extraction,
revocation, same-W Timeout Close and Timer->Down receipts before operational
completion. Delayed drain/dispatch never adds time to the original deadline.

Exact received Text `pong` follows accepted `queue_text` control handling; it
persists TransportUp with the real receive stamp, not RawInput or a new raw
attempt. Consequently raw `pong` bytes and its cause cannot be reconstructed
from TransportUp alone. This limitation must be named in manifests/projection;
neither Connected-Up nor Pong-Up proves freshness, subscription ACK, market
continuity, or Ping delivery. Control payload fidelity means the **contracted
representation**, not a complete raw heartbeat transcript.

If a callback returns late, the next wake-up uses the actual late sample and
the unchanged original deadline. A timeout can therefore already be due before
the application reads a queued Pong. Preserve observed/admitted FIFO; do not
backdate Pong or fabricate a timely heartbeat. A blocked filesystem operation
can starve both wake-ups and signals: report missed service/Unknown; no live
Timer liveness SLA is claimed under that failure.

## 6. Resource and time profile

These are proposed engineering limits, not Bitget guarantees. Checked size
accounting, actual allocations and cancellation must be verified on the final
implementation SHA. A documentation number is not an enforced allocator bound.

| Resource or stage | Proposed limit and exact behavior |
| --- | --- |
| Public market connection | Exactly one numeric TCP peer / one TLS / one regular `usdt-futures` Books50 stream / `BTCUSDT`; zero retries or second market connections. |
| Runtime DNS | **Disabled.** Require numeric `SocketAddr`; do not invoke ToSocketAddrs on a hostname, `lookup_host`, or a blocking resolver worker. DNS in this app is NOT_RUN. External address preparation requires its own bounded (3 s) resolution receipt and provenance before any live acceptance; its result does not replace certificate/SNI authentication. Missing address/receipt means live NOT_RUN. |
| TCP / TLS / HTTP upgrade | 2 s / 3 s / 2 s absolute deadlines; total Connect at most 7 s; HTTP request/response each <=8192 bytes; no redirect/fallback/retry. Peer/certificate/handshake cause retained. |
| Passive read | <=50 ms per owner-loop service quantum; an incomplete frame has a 2 s absolute budget from first observed frame progress, never renewed by slow drip. Idle alone is not Disconnected. |
| Text frame/message | <=65536 payload bytes; only FIN Text with no extension. Reject oversize before payload allocation using the library max-size route. Non-supported frames stop unsuccessfully. |
| Text write | <=4096 payload bytes, one outstanding command; 1 s absolute completion budget including all TLS/flush work; no suffix resend after Unknown. |
| Supervisor retention | N=1, M=16; R=4 raw frames, B=262144 raw bytes; max message=65536. `W + N + 1 <= M`; all reports/plans/leases/Close ownership are counted. No additional ingress queue. |
| Memory | Proposed total retained application allocation ceiling 8 MiB: accepted owner/supervisor/sink accounting plus transport/library buffers and bounded diagnostics. Frame ingress allowance 262144 bytes, TLS application buffers 65536 each, certificate/handshake allowance 1048576 bytes, diagnostics 131072 bytes; remaining checked metadata/decoder/backend budget must fit the aggregate. Inspect library allocations before claiming enforcement; over-budget construction/input ends capture. Static roots/code/allocator overhead/OS buffers/RSS are separate, measured and disclosed. If a selected library cannot enforce the ceiling before allocation, stop this dependent route; do not relabel a peak measurement as a bound. |
| Capture interval / output | At most 60 s after Connected, 4096 observed Text messages or 16 MiB payload, whichever occurs first. WAL one segment, cap 32 MiB, at most 32768 records including controls/seals. Check storage headroom before admitting another input; no unbounded archive/cache/diagnostic vector. |
| Shutdown | Stop new admissions on signal/budget; Close transport with <=1 s network operation budget; <=256 drain/quiesce steps and <=2 attempts per original Close; cooperative total target 5 s. This is conditional on synchronous storage calls returning; **not a hard fsync/kernel deadline**. |

The raw FrameSocket's publisher source uses a 128 KiB read buffer; account for
that actual capacity and any max-frame growth rather than infer 64 KiB total
memory from the payload cap. It does not expose the high-level configurable
outbound cap; the one <=4096-byte write, mandatory flush and no retry/batch
policy bounds the selected usage. Cap the underlying Read chunk and use
`FrameSocket::read(Some(65536))`; advertised length must be rejected before
payload allocation. After a frame is queued, WouldBlock permits only flushing
that same buffered frame within the original deadline, never writing the same
frame again. The raw frame writer does not add client-role masking: each
outbound frame needs its own fresh unpredictable mask. These facts and buffer
growth must be proved against the exact locked source. OS socket buffer sizes
and actual allocated capacity can differ from
requested sizes and need an explicit separate report.

The numeric-address choice is a genuine capability restriction. Standard
`ToSocketAddrs` can block resolving a hostname, while `TcpStream::connect_timeout`
requires an already-resolved SocketAddr. A timeout wrapped around unjoined
blocking DNS is not cancellation. [Official standard-library ToSocketAddrs](https://doc.rust-lang.org/stable/std/net/trait.ToSocketAddrs.html)
and [TcpStream](https://doc.rust-lang.org/stable/std/net/struct.TcpStream.html).
A later task can add a fully audited resolver, but this recommendation does not
silently include it.

## 7. Shutdown and failure traces

Signal delivery only sets an atomic flag. It does not borrow a SessionTurn,
touch the WAL, call Close, sample a fake received Down, or execute a callback
from a signal handler. The sole owner checks the flag at its service boundaries.
Normal local stop follows F-2's lawful `mandatory_close(..., None)` /
reclaim/dispatch pattern. Freeze admission, settle exact Close, drain already
admitted obligations, cancel only legally unactivated generated plans, obtain
the one borrowed Ready proof and finalize. Stop reading an endless feed to
"drain the socket"; unread OS/peer bytes are an Unknown suffix, not a known gap.

| Trace | Real observation / expected accepted records | Effect and archive outcome |
| --- | --- | --- |
| Nominal | Connect completes; real Connected stamp -> TransportUp. Subscribe locally completes; ACK Text -> exact RawInput / SubscriptionAccepted. Books Text -> exact RawInput, attempts and accepted continuity diagnostics in receive order. | No synthetic receive clocks. All bytes read back by existing WalReader. Local stop can physically finalize Complete/Unknown; usability stays false. |
| Text heartbeat Pong | Real tick -> original TimerFired; authority Ping sent once; real Text `pong` -> TransportUp at real stamp. | No ACK of Ping send inferred. Deadline policy remains original Timer A; no raw-Pong transcript claim. |
| Timeout | Wake-up reaches original Pong deadline without an earlier admitted Pong; TimerFired then exact Down; original same-W Close reservation. | CloseNotReady before lawful Down/storage stop; then one local cessation. Later H1/epoch plan completes only via accepted stages or explicit legal cancellation; single-shot capture does not reconnect. |
| Native WS Ping/Pong/Close | Real opcode/stamp/payload captured in bounded refusal diagnostic; never submitted as Text/Pong control. | No automatic Pong/Close write. Native Close followed by actual EOF can be a received Disconnected; mere Close opcode is not invented EOF. Attempt fails, suffix Unknown. |
| Transport EOF/error | Actual peer EOF or concrete read/connect failure at current epoch -> genuine queue_disconnected/TransportDown and lawful restoration diagnostics where the API permits. | No copied observation from local stop. Old Close must cease old slot before any H1 stage. Connect failure before Up is not Connected. Unknown physical writes retained. |
| Partial TLS/upgrade failure | Failed stage/real stamp; no Connected-Up, no subscribed state. | Dispatch Unknown even if the local temporary socket has been removed. Do not report no bytes sent. End one attempt; existing reader sees valid prefix or explicit incomplete tail. |
| Send/Ping failure | Gate passed, real write partially completed or fails; owner returns Unknown. | Do not retry Ping/Subscribe or library buffered suffix. Request/settle same local Close; ACK/remote delivery remain unknown. |
| B/R/message overflow | Actual bounded queue_text admission; existing compatible local loss -> exact GAP where lawful. If no counted owner can represent it, immutable terminal failure and mandatory Close. | Inspect outer disposition. No payload replacement or unobserved OS-byte count. Failed scope cannot resume; diagnostic WAL is incomplete, no false final seals. |
| Storage failure | Real persistence error, weak gate or bad receipt in admitted Raw/Timer/Down. Trusted prefix remains separate from possible physical suffix; StorageStopped. | No retry/repair/substitution, no seal/finalization proof. Original ready terminal Close remains serviceable; `close_diagnostic` reports actual descriptor/undrained owners. |
| Normal signal / budget stop | Actual flag/budget transition is local intent; no received Disconnected manufactured. Already-admitted controls retain their original stamps/order. | Same epoch mandatory Close, then lawful drain/quiesce. NotReady remains NotReady; one proof at most. Complete/Unknown is only reported after real successful finalization. |
| Pending effect / shutdown refusal | A callback is still running, original Close is Pending/Leased, or an admitted frozen Timer/H1 record stage is unsettled. | Cannot declare Done while callback continues. Enqueue cannot settle. Legal Close retry keeps exact identity; otherwise record refusal/Unknown and diagnostic incomplete outcome. |
| Stale epoch / ambiguous Close | Old lease is revalidated; a reused slot has newer epoch. Old Close first destroys only its exact old resource or finds that exact epoch ceased. | Never act on new slot. Err preserves same Close Pending; retry/drop count bounded, no new owner. Settled Close is never reissued. |
| Timer starvation / disk stall | Real storage call does not return before service target; no invented timely Tick/Pong. | Hard total deadline is unsupported. An external process watchdog may abort the attempt and record KILLED/Unknown; it cannot mint owner settlement/seals or promise kernel descriptor-close timing. Recover using WalReader once possible. |

An external watchdog is a last-resort operational abort, not a new owner
completion lane. Its fixed status record belongs to external evidence; if the
process cannot be reaped or an uninterruptible syscall remains, report that
explicitly. No five-second PASS is justified by sending a kill signal.

## 8. Honest captured identities and provenance

Keep real exchange identity lowercase `bitget` / Perpetual / `usdt-futures` /
`BTCUSDT` / BookNormal, as validated by the accepted supervisor. Local IDs and
epochs are engineering identities; create fresh nonzero archive/session IDs,
record them once, and never regenerate them in replay. Bootstrap receives real
setup clock samples, not F-2's synthetic UNIX base arithmetic.

This is the known configured public wire identity needed for the diagnostic
subscription, not verified canonical instrument applicability or numeric
metadata. Unknown/unrecognized identity still blocks stream registration under
types-v1 §1; a profile must not substitute a plausible name for an unknown
received identity.

The required numeric definition remains an explicitly **synthetic/unverified
placeholder**, with distinct `quantity_unit=UNKNOWN`, positive placeholder
increments and `quantity_to_base_multiplier=None`; do not label it exchange
metadata or perform grid/quantity conversion. Config/time/limits are Engineering
policy. Artifact references are parseable unverified placeholders, not proof
tokens. Keep per-field/per-record provenance: wire market/ACK input is real;
numeric/artifact assumptions are synthetic/unverified; timer/limit policy is
engineering; sampled clocks are real. A global `synthetic=false` would hide
bootstrap uncertainty and is forbidden.

This composition can create a diagnostic owner/WAL under accepted structural
definitions, but missing production artifact bytes/verifier continue to block
canonical interpretation under artifacts-v1 §§1/3/5. CRC, TLS authentication,
Complete, Durable, ACK and plausible units do not remove those gates.

## 9. Replay plan for the captured WAL

Frozen F-1 `apps/radar/src/replay/**`, its fixture/golden and builder remain
byte-for-byte unchanged. It pins synthetic IDs/clock/definitions and capitalized
`Bitget`; its RawInput route accepts Books50 through the public market decoder.
It therefore rejects real ACK, and is not the captured-WAL replay profile.
F-2's lowercase/ACK profile was accepted without claiming that compatibility.

Add one separate app-level captured diagnostic profile and binary. Read solely
through accepted `WalReader::open`/`next_record`/`report`, in dense RecordNo order;
do not sort exchange timestamps or skip corruption/unknown tags. Keep physical
frontier and diagnostic projection frontier separate. Build ClockScope from
recorded ArchiveStart, and use each record's original Context/Timer sample.
Structural start/seals reuse the last recorded sample without creating one.
No networking, Instant/SystemTime, random ID, path or PID enters the replay
projection. Two fresh processes of exactly one WAL must have byte-identical
fixed JSON and digest.

| Recorded input | Accepted reducer route / diagnostic treatment |
| --- | --- |
| First StreamDefinition / ConfigDefinition | Validate supported captured profile/structural bootstrap, then RegisterStream plus dense administrative Noop. Artifacts remain Unverified/Blocked; this is an explicitly limited diagnostic profile, not canonical activation. |
| Raw Books50 Text | Existing `decode_message_with_limits`, `ContinuityClassifier` and `BookFrameObservation` -> existing `DataHealthReducer::step`. Keep original raw record/tag/sample; no VerifiedFrame/Warmup proof minting or canonical application. |
| Raw ACK evidenced by capture's genuine supervisor event | Preserve original bytes, record and stamps in deterministic diagnostics; dense `HealthObservation::Noop` only in the explicitly supported administrative-raw slot. The genuine `SubscriptionAccepted`/`SubscriptionFailed` capture receipt and exact readback establish acceptance; replay alone calls this opaque/non-market Raw and does not classify it as authenticated ACK. A malformed book or arbitrary decoder error cannot take this route. |
| Transport / EpochAdvance / Gap / TimerFired | Existing HealthObservation Transport/EpochAdvance/Gap/Timer with recorded scope/time. Obsolete/current semantics are the accepted reducer's. Timer alone never fabricates Down or peer acknowledgement. |
| RecordingEvidence | Preserve its original assertion/watermark diagnostic, including monotonic failed latch; use dense Noop where the public DataHealth API has no recording-observation variant. Never infer historical StorageFence/fsync or healthy publication permission. |
| Other controls, later config/spec, proof inputs | Stop dependent projection with exact record/reason unless explicitly supported by this separate profile. No silent Noop for canonical activation/evidence and no proof construction from a parsed ref. |

ACK checking is mandatory in **capture** tests: feed genuine matching ACK bytes
through accepted supervisor, observe `SubscriptionAccepted`, and require exactly
those bytes at its read-back RawInput. Replay must preserve that record in both
independent projections, not classify it as malformed Books50 or erase it.
The private `classify_inbound` has no public read-only export. If independent
typed ACK classification in replay is required, §11's ACR gate applies; copying
the private parser into a second protocol decoder is not this recommendation.
Replay diagnostic output must clearly distinguish opaque/non-market Raw from
Books50 and report `ack_semantics=NotReconstructed` where appropriate. Define
one bounded administrative-raw slot before first Books50 in the captured
profile, tied by the separate capture evidence to its genuine supervisor ACK
event; no general `decode error -> ACK/Noop` fallback exists. An unrecognized
book, additional raw administrative/state-changing input, malformed/unknown
payload or missing capture event evidence stops the dependent projection at
that record. Physical scan can continue only as explicit read-only diagnostics.
The two replay processes use the same supported profile/evidence binding and
WAL; neither creates a new classification from current network state.

TransportUp cannot distinguish Connected from Text Pong; original effect
completion, Ping delivery, native WS opcodes, unread bytes and physical sync are
not encoded by these records. Report these as NotReconstructed/Unknown, never
as zero loss or successful delivery. Preserve raw ACK/books fidelity and all
supported contracted controls while making that limitation visible.

## 10. Cargo and CI preparation

The chosen candidate dependency route is one stack, not permission to try
several transports in parallel. Versions below were verified on the publishers'
API documentation on the review date; none was installed or compiled here.

| Candidate | Intended use / verification gate |
| --- | --- |
| `tungstenite = =0.30.0` | Library client handshake and raw FrameSocket; use minimal handshake feature, disable default/native TLS/client convenience resolution. Verify exact selected features/source, masking and no auto writes. |
| `rustls = =0.23.45` | Explicit client TLS I/O; std plus one deliberate crypto provider. Prefer ring to avoid silently pulling both providers; confirm exact feature availability, MSRV and transitive closure with Cargo. No dangerous verifier, session resumption or 0-RTT. |
| `webpki-roots = =1.0.9` | Compiled roots; use TLS_SERVER_ROOTS with hostname validation. Verify locked rustls compatibility and root construction. [Official API](https://docs.rs/webpki-roots/latest/webpki_roots/). |
| `signal-hook = =0.4.5` | SIGINT/SIGTERM atomic flag registration only; no owner/network work in handler. Confirm platform/features/registration failures. [Official flag registration](https://docs.rs/signal-hook/latest/signal_hook/flag/fn.register.html). |

Do not add Tokio/async DNS by habit. A current-thread executor could execute a
fully bounded future inside a synchronous callback without moving SessionTurn,
but timeout-drop is not completion of detached/blocking work. That alternative
is not selected. Rustls manual I/O and raw frames still require actual dependency
and allocation review; the version table is not a compile or cancellation PASS.

Shared Cargo.lock is Integrator-owned and changes sequentially. Worker first
supplies exact manifest feature proposals and a Cargo-generated proposed lock
delta from pinned Rust/Cargo 1.98.1; Integrator validates and commits the lock
once in the same implementation branch. No hand-edited lock, parallel lock
writers, unrelated dependency refresh or fabricated checksum/version.

Current CI globally sets `CARGO_NET_OFFLINE=true` and contains no `cargo fetch`.
On a clean Linux runner, introduce an explicit preparation step before offline
checks: exact checkout and toolchain; `CARGO_NET_OFFLINE=false cargo fetch
--locked`; inspect the locked dependency/source/feature graph; then restore
offline mode. Fetch prepares the registry/package cache only, not live smoke.
Each job that needs crates must prepare its own cache or consume an explicitly
validated equivalent; fmt does not prove dependency availability. Existing
exact-head, three-job, generated-lock equality, locked build and clean-tree
gates remain. Then execute build/fmt/clippy/test with `--locked --offline`
where applicable, repeat lock-generation equality offline, and verify no
tracked mutation. Fresh runner fetch/install failure is FAIL/BLOCKED, not a
successful offline test. Network-disabled runtime tests and separately opted
real public smoke have separate receipts.

## 11. Conditional ACR: requirements beyond the selected slice

**No public-contract delta is required for §3's fail-stop Text-only diagnostic
slice.** Do not implement the following alternatives without a separately
accepted ADR. These are concrete unsupported traces, not a request to generalize
Timer A or redesign ownership now.

| Requirement / unsupported trace | Minimal proposed change and impact |
| --- | --- |
| Continue a compliant socket after native Ping | Read real opcode Ping(payload P), persist/authenticate observation, emit matching opcode Pong(P), continue capture. No existing CommandKind can authorize that effect; automatic library read writes bypass dispatch. Minimal ACR: owner-minted, <=125-byte same-epoch protocol-control response lease tied to the original admitted observation, with same synchronous completion/Unknown semantics and counted ownership. Timer A text-Ping identity/deadlines unchanged. Raw opcode persistence is a separate wire decision if requested; do not smuggle a new tag into WAL v1. |
| Preserve every native frame/fragment/raw Text Pong in WAL | Current queue_text normalizes exact text Pong into Up and has no binary/opcode ingress. Minimal ACR: versioned typed raw transport observation/storage representation with bounded payload and receive identity, migration/reader behavior and replay projection specified. Existing v1 bytes/goldens remain valid and frozen. No fabricated CaptureAttempt for non-raw controls. |
| Finish owner effect asynchronously after callback return | Enqueue Connect/Close; callback returns Ok; actual operation times out later. This falsely settles Close and releases authority. Minimal ACR: affine in-flight effect token and authenticated later completion/Unknown/cancellation on the original SessionTurn, retaining W and exact epoch until settled. Do not expose a generic completion DTO or free external closure constructor. |
| Guarantee successful five-second shutdown despite stalled fsync | Begin finalization; synchronous backend never returns; turn cannot observe signal or finish proof. Minimal ACR: separately defined cancellable/isolated storage operation lifecycle with truthful pending/Unknown prefix and finalization refusal. Killing the process is operational abort, not successful completion. Existing sync APIs/Timer A cannot supply that guarantee. |
| Independently classify ACK in replay through accepted parser | Read exact ACK RawInput; public Books50 decoder rejects it while private supervisor classifier can recognize it. Minimal ACR: public bounded pure read-only classification result exported from the existing implementation, with exact profile/error behavior and tests; no second parser/FSM or WAL change. Diagnostic opaque-ACK Noop remains a preserving alternative. |

Required ADR review covers contract/version, exact affected paths, positive and
negative traces, foreign/old epoch/Drop/error/retry, all new retained bytes and
work counts, WAL migration/profile behavior and independent QA on a specific
head. A proposal is not approval. Changing the chosen slice to any row changes
the verdict to **ACR_REQUIRED / BLOCKED_IMPLEMENTATION** for that dependency.

## 12. One later implementation recommendation

Recommend exactly one task, **REC-001F-3-PUBLIC-TEXT-CAPTURE**: one explicit
numeric-peer public socket, `usdt-futures` / Books50 / `BTCUSDT`, bounded 60 s
single-shot Text capture to accepted filesystem owner/WAL, with separate
captured diagnostic replay. No DNS implementation, automatic reconnect,
native protocol-control service, canonical book/units/decisions, production
verifier, F-1/F-2 rewrite or full-M1 claim. Runtime hostname resolution is
refused; live acceptance needs the address-preparation receipt in §6.

Integrator must publish its Issue, exact refreshed base, branch, precise paths,
dependencies, acceptance and stop conditions after this review; this paragraph
is the recommendation, not a second architectural task packet. Suggested app
scope: new `capture_public_text` example/binary and private public-capture
transport/clock/profile modules; separate captured replay binary/modules;
focused real-binary offline tests; DEVELOPMENT and one implementation handoff.
Only explicitly named radar manifest, root Cargo.lock and CI preparation paths
need Integrator integration. Accepted crate/spec/ADR files and all frozen
F-1/F-2 profile/fixture/history paths stay excluded.

Acceptance on exact final head must include: actual captured ACK/books bytes
and controls accepted by existing WalReader; two fresh offline replay processes
with identical projection/digest; genuine ACK acceptance plus replay preservation;
real sampled clock provenance and receive order; Timer A boundary/heartbeat/
timeout/revoked-Ping tests; transport and partial TLS/upgrade failure; native
control/fragment/binary/oversize fail-stop without automatic writes; real queue
overflow versus unknown unread loss; storage failure during Closing; normal
signal stop, pending/ambiguous Close, refused shutdown and post-finalization
effect denial; checked resource budgets and no buffered resend. Independently
verify no keys/private APIs/orders and unchanged frozen source/fixtures.

Tests use controlled clocks/loopback faults as identified synthetic evidence,
then a separately opted real public capture. If real access/address preparation
is unavailable, report live **NOT_RUN**; an offline green CI cannot establish
that the captured stream was real. Independent QA examines the final specific
head, dependencies, decoded CI and trace evidence. Issues #37/#22/#5 remain open;
M2 #29 remains blocked until the owner separately accepts M1.

Real captured WAL stays in a private/local owner-approved evidence location;
publish only its size, digest, provenance and receipt references. Do not commit
raw market archives or attach them to public releases. Offline fault fixtures
may be small, generated and explicitly synthetic; they do not substitute for
the privately retained real-WAL acceptance evidence.

Stop on unsupported lifecycle/allocation/cancellation, source/API contradiction,
unavailable required evidence, extra paths, changed base/conflicting assignment,
dependency fetch/feature/MSRV failure or a requirement in §11. Retain useful
independent docs; stop only dependent code until the appropriate decision.
No merge, auto-merge, settings, force-push, issue closure or implementation launch
is authorized by this report.
