# ADR0004 — proposed public capture retained-allocation boundary

Status: **PROPOSED / DESIGN_REVIEW_REQUIRED / IMPLEMENTATION_STOPPED**.
Date: 2026-10-10. Author: sole Integrator #45, claim6097721380.
Direction: **ACCEPT_AS_EXPERIMENTAL_DIRECTION / ADR_FIRST** only.
This document is not an accepted contract, fork permission, allocation proof,
independent QA result or production activation authority.

## 1. Authority, exact inputs and requested decision

Current [#45](https://github.com/al-gri/pro-sclpng/issues/45) authorizes this
ADR and [existing Handoff](../handoffs/REC-001F-3-PUBLIC-TEXT-CAPTURE.md) only,
in the existing branch `feat/REC-001F-3-public-text-capture` / [Draft PR47](https://github.com/al-gri/pro-sclpng/pull/47).
No vendor, runtime, manifest, lock, CI or accepted document is changed here.
There is no second executor, PR or QA. Merge remains with the owner.

Design input publication: `9c1d7c5483f0989a18b62d143fbfc9278905f2e8`, tree
`140ae5c18650f9eef3c06b7c140d8578b21bfd8c`. Accepted main/base:
`273bfac01bc7a7954644e5270eb96cc99d787fab`, tree
`abfca247bbaa6fbad0c9f78d643dc9e8df7b3961`. Runtime source remains
`e6ccef18075f56d73f42f043dfcd506039a975c0`, tree
`e53017a5c747680d630d4620bc553d619831a9ad`. The actual ADR publication
head/tree are bound by the subsequent #45/47 receipt; this document cannot
contain its own future commit hash.

Corrective revision: **R1–R3 only**, following
[ARCH-ALLOC45-ADR4-D1-20261010](https://github.com/al-gri/pro-sclpng/pull/47#issuecomment-6100334905),
DESIGN_CHANGES_REQUIRED / FULL_IMPLEMENTATION_SCOPE_NOT_ACCEPTED on
`d1c3cf543a8cf529eed825512ff16f667c24a348`, tree
`85bc560211e1536819452062e9347db986888318`. That reviewed head is this
correction's input, not its future publication identity. All U1–U7 remain
unresolved; C00–C25 remains an inventory, not complete allocation closure.

Inputs: [receipt6099645626](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6099645626),
[bounded evidence6099645308](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6099645308),
[sole claim](https://github.com/al-gri/pro-sclpng/issues/45#issuecomment-6097721380),
[AGENTS](../../AGENTS.md), [WORKFLOW](../WORKFLOW.md),
[PROJECT_STATE](../PROJECT_STATE.md), accepted
[ADR0002](0002-domain-event-wal-contracts.md),
[ADR0003](0003-ws-capture-saturation.md) including TimerA §15A/§§18–20,
[types](../../specs/domain/types-v1.md),
[artifacts](../../specs/domain/artifacts-v1.md),
[events](../../specs/market-data/events-v1.md),
[DataHealth](../../specs/market-data/data-health-v1.md) and
[WALv1](../../specs/recording/wal-v1.md). Those contracts stay unchanged.

Requested decision on the actual ADR head: Architecture/owner reviews the
exact proposed interface, category map, refusal/lifetime contract and U1–U7
below. **The original seventeen-file patch list does not yet close the
reachable allocation graph.** A closed implementation scope must be recorded
separately; accepting the direction or this design with unresolved stops
does not authorize implementing those extra paths/dependencies.

Production gate remains **FAIL / NOT_PROVEN**. Capture/WAL/replay/dependent
activation/independent QA are **NOT_RUN**. Absent application components are
not assigned fictitious zero bytes or required to pass before they exist.

## 2. Unchanged budgets and proposed category map

All values are bytes and checked retained application allocation:

| Counter | Exact limit | What is charged |
|---|---:|---|
| T | 8,388,608 | Every application Rust heap backing, allocation header, shared/control block, ledger backing and failure evidence; no diagnostic-baseline subtraction |
| H, inside T | 1,048,576 | Certificate/handshake buffers, descriptors, DER/OCSP copies, parser/transcript/state/crypto verification work and retained peer chain; includes their allocation metadata |
| RxPlain, inside T | 65,536 | Received application plaintext backing + queue/chunk descriptors + associated allocation metadata |
| TxPlain, inside T | 65,536 | Pending outgoing application plaintext backing + descriptors/metadata |
| TxRecord, inside T | 65,536 | Buffered outgoing TLS records backing + descriptors/metadata, including handshake records queued here |
| Diagnostic, inside T | 131,072 | Preallocated refusal/native-control/evidence storage; its control/header overhead is additionally charged to T |

This proposal explicitly names the three upstream ChunkVecBuffer owners;
they are individually bounded, never three allowances outside T. A byte in
TxRecord is charged once to T and its buffer sublimit; handshake record
bytes also carry H until freed, so counter membership can overlap without
adding them twice to T. H and the other sublimits are predicate views over
the same allocation IDs, not additive extra memory pools.

Configuration/provider/verifier/root descriptors and server-name backing
are T/configuration allocations. Protocol-state boxes, transcript/deframer
allocations and crypto work originating in the TLS connection are H; a
retained certificate or cipher/state backing stays H in traffic. New traffic
protocol-control/verification work also uses H. Pure plaintext queues use
their buffer category. A mixed allocation has a fixed membership bitset at
creation (for example H+TxRecord); it is never charged twice to T.
Replacing an object is a new allocation; changing a protocol enum phase
does not change its backing's original bitset. This conservative map needs
explicit Architecture acceptance; it does not enlarge any allowance.

Charge the actual allocation Layout size, collection capacity and owned
headers/descriptors, not logical payload length. Inline fields already in
a charged allocation are not separately charged. Borrowed static root DER
and code are separately disclosed; copied root descriptor Vecs and any
owned root bytes are in T. Rust heap crypto work is H/T, **not native**.
RSS, actual stack use, non-Rust native allocation/allocator overhead and OS
socket memory are separately measured/disclosed, never claimed capped by T.

## 3. Exact proposed Rust surface

Namespace: `rustls::client::budgeted`, std/ring/tls12, default features off.
The declarations below are a signature specification, not compiled code or
available methods. All opaque types have private fields. Every named helper
type/variant/method here is part of the proposed API review; no implicit
public extension is authorized. No Deref/as_inner/into_inner exposes an
unbudgeted ClientConfig, ClientConnection or mutable provider/verifier.

```rust,ignore
pub struct RetainedBudget { /* private */ }
pub struct RetainedBudgetWeak { /* private */ }
pub struct BudgetedClientConfigBuilder { /* private */ }
pub struct BudgetedClientConfig { /* private */ }
pub struct BudgetedClientConnection { /* private */ }
pub struct BudgetedBytes { /* private */ }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AllocationClass {
    Configuration, Handshake, RxPlain, TxPlain, TxRecord,
    HandshakeTxRecord, Diagnostic, Ledger,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsPhase { Setup, Handshake, Traffic, Retiring, Retired }
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefusalReason {
    TotalQuota, HandshakeQuota, BufferQuota, ArithmeticOverflow,
    AllocatorNull, ReferenceCountOverflow, AdmissionBusy,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceRefusal {
    pub class: AllocationClass,
    pub requested: usize, // includes header/alignment charge
    pub retained_total: usize,
    pub retained_handshake: usize,
    pub reason: RefusalReason,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IoProgress {
    pub read_attempted: bool,
    pub write_attempted: bool,
    pub read_bytes: usize,
    pub written_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrorContext { pub phase: TlsPhase, pub io: IoProgress }
#[derive(Debug)] // no Clone; quota construction never formats/allocates
pub enum BudgetedClientError {
    ResourceRefused { context: ErrorContext, refusal: ResourceRefusal },
    DeadlineExpired { context: ErrorContext },
    Stopped { context: ErrorContext },
    Io { context: ErrorContext, kind: std::io::ErrorKind,
         raw_os_error: Option<i32> },
    Tls { context: ErrorContext, source: rustls::Error },
    InvalidConfiguration, InvalidState,
    UnresolvedClosure { site: &'static str },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetSnapshot {
    pub total: usize, pub handshake: usize,
    pub buffers: [usize; 3], pub diagnostics: usize,
    pub pending_reservations: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessSummary {
    pub plaintext_available: usize, pub peer_closed: bool,
    pub handshaking: bool, pub wants_write: bool,
}

impl RetainedBudget {
    pub fn new(deadline: std::time::Instant, stop: &std::sync::atomic::AtomicBool)
        -> Result<Self, BudgetedClientError>;
    pub fn try_share(&self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<Self, BudgetedClientError>;
    pub fn try_downgrade(&self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<RetainedBudgetWeak, BudgetedClientError>;
    pub fn snapshot(&self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<BudgetSnapshot, BudgetedClientError>;
    pub fn last_refusal(&self) -> Option<ResourceRefusal>;
}
impl RetainedBudgetWeak {
    pub fn try_share(&self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<Self, BudgetedClientError>;
    pub fn try_upgrade(&self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<Option<RetainedBudget>, BudgetedClientError>;
}
impl BudgetedClientConfigBuilder {
    pub fn new_ring(
        budget: &RetainedBudget,
        roots: &'static [rustls::pki_types::TrustAnchor<'static>],
        deadline: std::time::Instant, stop: &std::sync::atomic::AtomicBool,
    ) -> Result<Self, BudgetedClientError>;
    pub fn build(self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<BudgetedClientConfig, BudgetedClientError>;
}
impl BudgetedClientConfig {
    pub fn try_share(&self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<Self, BudgetedClientError>;
    pub fn try_clone_owned(&self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<Self, BudgetedClientError>;
    pub fn budget(&self) -> &RetainedBudget;
}
impl BudgetedClientConnection {
    pub fn new(
        config: &BudgetedClientConfig,
        server_name: rustls::pki_types::ServerName<'_>,
        deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool,
    ) -> Result<Self, BudgetedClientError>;
    pub fn read_tls(
        &mut self, socket: &mut std::net::TcpStream,
        deadline: std::time::Instant, stop: &std::sync::atomic::AtomicBool,
    ) -> Result<usize, BudgetedClientError>;
    pub fn process_new_packets(
        &mut self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool,
    ) -> Result<ProcessSummary, BudgetedClientError>;
    pub fn write_tls(
        &mut self, socket: &mut std::net::TcpStream,
        deadline: std::time::Instant, stop: &std::sync::atomic::AtomicBool,
    ) -> Result<usize, BudgetedClientError>;
    pub fn write_plaintext(
        &mut self, bytes: &[u8], deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool,
    ) -> Result<usize, BudgetedClientError>;
    pub fn read_plaintext(&mut self, out: &mut [u8], deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<usize, BudgetedClientError>;
    pub fn peer_certificate_count(&self) -> usize;
    pub fn peer_certificate(&self, index: usize) -> Option<&[u8]>;
    pub fn try_copy_peer_certificate(&self, index: usize, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<BudgetedBytes, BudgetedClientError>;
    pub fn phase(&self) -> TlsPhase;
    pub fn wants_write(&self) -> bool;
    pub fn retire_local(&mut self) -> Result<(), BudgetedClientError>;
}
impl BudgetedBytes {
    pub fn as_bytes(&self) -> &[u8];
    pub fn try_share(&self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<Self, BudgetedClientError>;
    pub fn try_clone_owned(&self, deadline: std::time::Instant,
        stop: &std::sync::atomic::AtomicBool) -> Result<Self, BudgetedClientError>;
}
```

**R1 PROPOSED signature delta relative to reviewed d1c3:** add the existing
`deadline: Instant, stop: &AtomicBool` parameter pair to exactly fourteen
previously context-free methods: RetainedBudget::{new,try_share,
try_downgrade,snapshot}, RetainedBudgetWeak::{try_share,try_upgrade},
BudgetedClientConfigBuilder::{new_ring,build},
BudgetedClientConfig::{try_share,try_clone_owned},
BudgetedClientConnection::{read_plaintext,try_copy_peer_certificate}, and
BudgetedBytes::{try_share,try_clone_owned}. No new public type, variant or
method is added. These declarations supersede the earlier signature proposal
for review only; they do not accept or implement an API change.

### 3.1 Constructor, configuration and callback semantics

`RetainedBudget::new` takes no configurable limits. It checks/bootstrap-reserves
the real ledger core/control allocation and diagnostic storage before either
underlying allocation (§4), returning a fixed-size error on failure. It is
created before any application heap setup; integrating the remaining app
allocations into the same ledger is a separate proof (§7/U6).

`new_ring` builds the same default ring suites/groups and WebPki server
verification as the current route, both TLS12/13 enabled, no client auth,
resumption/0RTT disabled from construction. It does not create a default
session cache and subsequently pretend those transient bytes never existed.
Root input for #45 is the existing pinned webpki-roots static set. Borrowed
root descriptors may be copied fallibly; static DER is borrowed. A purported
static TrustAnchor can own heap bytes: such inputs require existing ledger
ownership provenance, and the unproved import is `UnresolvedClosure`, not
silently excluded by the lifetime annotation. No unrestricted `with_provider`,
`with_verifier`, ALPN/client-auth/key-log/session-store/compressor setter is
exposed in this selected surface. The unchanged route's built-in callbacks
still require proof: fixing their identity is not a no-allocation guarantee.

`build` consumes the builder; failure drops only the new constructor's
actually-created objects and their charges. A caller's original budget/root
ownership remains. Config `try_share` shares one backing/control block;
`try_clone_owned` creates independently charged configuration/root descriptor
and callback-owned backing, with a complete pre-reservation or returned
refusal. Neither config nor connection uses infallible Clone. A connection
cannot be cloned, forked, reconnected or resumed by this API. Borrowed peer
certificate access allocates nothing; explicit copies are H BudgetedBytes
through actual free, even when made during traffic. BudgetedBytes aliases
share one backing; owned clones pay new bytes and control metadata.

Input strings are copied fallibly using validated ServerName semantics;
moving an existing accounted name is not a deep copy. No DNS resolution is
performed by this module. Caller-provided TcpStream is already bound to the
one owner/epoch and original bounded I/O policy; arbitrary Read/Write
callbacks that may allocate are not accepted by the proposed signatures.
This avoids pretending any third-party callback is covered. It still leaves
pinned TcpStream/std internals in U4 and app transport ownership in U6.

All **timed/setup/refcount methods** below receive the original caller
deadline/stop explicitly and check them before allocation/admission, optional
waiting, I/O and bounded processing segments. The two context-free classes
are pure observations and mandatory local cleanup, with the exceptions below;
the earlier unqualified "all operations" promise does not apply to them.
`process_new_packets` and I/O calls receive the original deadline of the
current accepted operation (TLS handshake, frame read, Send or original Close
as applicable), not a new timeout or the expired handshake deadline reused
for traffic. No method starts a fresh deadline/stop flag. The caller retains
the same originals across all leaves/repetitions of that operation; private
owner glue must reject a later replacement. Its concrete mapping is U6.

For setup calls before a timed dispatch, the source is the caller's already
governing finite setup-operation deadline and stop flag, passed as arguments;
this ADR creates no new setup timeout/allowance. In an accepted timed dispatch,
setup/refcount helpers use that dispatch's original context, not a setup
exception. The new_ring→build construction sequence retains its original
construction context through build or failure; splitting it cannot renew the
deadline. If the caller cannot supply/prove the governing original context,
that route is unproved/stopped under U6, not implicitly unlimited setup.

Table covers **all27 declared public methods**. T means timed operation;
S means setup/refcount/copy with the same explicit original-context checks;
O means bounded nonblocking borrowed/scalar observation, without allocation,
admission/refcount change, waiting or free; C means required local cleanup.

| Every public method (fully qualified) | Policy / exact context or exception |
|---|---|
| RetainedBudget::new | S; explicit caller deadline/stop, inline bootstrap context before any heap ledger exists; no configurable budget/setup timeout |
| RetainedBudget::{try_share,try_downgrade} | S; explicit current caller context, even for a fixed refcount transaction; no context-free share exception |
| RetainedBudget::snapshot | T; explicit caller context for consistent ledger synchronization; AdmissionBusy/DeadlineExpired/Stopped rather than an unbounded wait |
| RetainedBudget::last_refusal | O; fixed-size consistent observation only, no hidden blocking lock, clone allocation or physical-completion inference; concrete bounded read is U4 |
| RetainedBudgetWeak::{try_share,try_upgrade} | S; explicit caller context for checked refcounts/liveness; no new backing or expired-dispatch upgrade entitlement |
| BudgetedClientConfigBuilder::{new_ring,build} | S; explicit original construction/caller context, including provider/root/verifier work; build failure still performs C cleanup |
| BudgetedClientConfig::{try_share,try_clone_owned} | S; explicit caller context; owned clone pre-admits every new backing, sharing checks refcounts without changing existing backing charge |
| BudgetedClientConfig::budget | O; borrowed reference only, no retained-handle clone/admission |
| BudgetedClientConnection::{new,read_tls,process_new_packets,write_tls,write_plaintext} | T; existing explicit deadline/stop from current original operation; phase/engine I/O facts supply ErrorContext |
| BudgetedClientConnection::read_plaintext | T; new explicit deadline/stop for copying/popping chunks; a stopped/expired call starts no new read/pop, but required cleanup for work already performed is C |
| BudgetedClientConnection::try_copy_peer_certificate | T; new explicit current original context before H copy/reservation; borrowed input does not exempt its owned output |
| BudgetedClientConnection::{peer_certificate_count,peer_certificate,phase,wants_write} | O; borrowed/scalar state access only, no allocation/wait/free or command completion authority |
| BudgetedClientConnection::retire_local | C; no deadline/stop argument or fresh context; mark unusable and truthfully release/defer existing local backing without I/O/new admission, even after expiry/stop; no Ready/Close success |
| BudgetedBytes::as_bytes | O; borrowed slice only; no partial-backing charge release |
| BudgetedBytes::{try_share,try_clone_owned} | S; new explicit caller context; distinguish checked alias refcount from separately pre-admitted true copy |

O/C exceptions cannot authorize allocating work, optional waiting, network
effects or continued non-Close service in an expired/stopped accepted dispatch.
Any observed value is diagnostic only. Required Drop/free/rollback/refcount
decrement and cleanup of a partially executed T/S call are **C**, even after
the parent context expires/stops: never skip them or remove a still-live charge
because deadline checks failed. C may not allocate, upgrade/share a new owner,
retry an effect or wait indefinitely. Completion of actual free/accounting,
or honest retention in already-paid deferred metadata, is mandatory. Bounded
std/storage/deallocator/reclamation proof is still U4; exact app ownership/
deadline composition is U6. A textual exception is not that proof.

Within one leaf call no retry/reservation/callback may extend its deadline.
Processing
never writes to the network. `write_plaintext` can enqueue/encrypt locally
but never performs socket I/O. `write_tls` may write only previously queued
records, with preflight for its entire reachable operation closure; it is not
implicit complete_io. No automatic TLS/WebSocket network shutdown, alert
flush, Pong, Close, retry or control response is added. Protocol errors may
have a budgeted queued TLS alert, but it is not sent after an allocation
failure; unexpected traffic wants_write preserves the current fail-stop rule.

`read_plaintext` copies into caller storage and releases full chunk backing
only when it is actually popped; partial reads do not release its capacity.
`retire_local` marks the engine unusable and releases locally owned TLS
objects where possible; it does not shut down the borrowed TcpStream, create
Close, return Ready, prove owner completion or erase external aliases. Drop
performs no I/O, emits no owner success and has no allocation/formatting path.

### 3.2 Error mapping and indivisible operations

| Origin | Exact proposed mapping and effect |
|---|---|
| Checked size/add/multiply/Layout failure | ResourceRefused/ArithmeticOverflow before underlying allocator call; old values/state remain |
| T/H/buffer quota, refcount overflow, admission lock unavailable | ResourceRefused with corresponding reason; no allocation/network call for rejected proposal; no automatic retry |
| Fallible allocator returns null after valid admission | ResourceRefused/AllocatorNull; reservation rolled back, old backing remains. Quota refusal itself never calls allocator |
| Original deadline/stop | DeadlineExpired/Stopped with phase and cumulative engine I/O facts; no restarted deadline |
| Borrowed TcpStream OS error | Io with scalar kind/raw_os_error; no allocating io::Error::new, to_string or boxed source. Any underlying std allocation still needs U4 proof |
| Normal certificate/protocol rejection | Tls holding the original rustls::Error and its charged backing; original verification is preserved. Error constructors/clones are in the closure, not free metadata |
| Uncovered allocation/provider/root import/callback | UnresolvedClosure before entering that unsupported operation; activation remains blocked until that stop is resolved |
| Invalid state/configuration | Fixed inline variant before effects; no replacement owner, config or new epoch |

An operation that needs several allocations reserves the checked sum of their
maximum simultaneously live charges, including old+new realloc and metadata,
before its first irreversible mutation or I/O; it commits allocations against
that reservation and releases unused credit. Exact capacities proven for
fallible primitives may be admitted one by one before purely local mutation;
failure retains/drops those actual objects honestly. An infallible downstream
call is callable only with a source-proved allocation/lifetime upper bound
and a mechanism providing fallible construction or prepaid storage that
cannot quota-fail internally. A guessed envelope or allocator null fallback
is insufficient. System-OOM/Arc/Box conversion compatibility is U4: this ADR
does not claim that prepaying quota makes infallible System allocation lawful.

Partial write/read progress is reported as actual returned bytes, never
rolled back. IoProgress accumulates checked totals for the engine lifetime;
caller Connect/Send/Close effects predating this engine are additionally
retained by the adapter. A scalar progress value is diagnostic, not an
authority-minted completion/absence-of-effect proof. **R2:** each backing
reachable from `Tls.source: rustls::Error` stays charged through its **actual
free**, not merely the outer BudgetedClientError's Drop. Safe moves or
extraction of the public source/its owned fields, replacement of that field
or enum, and ownership outside the connection/config/user-facing budget
handle do not release or reclassify surviving backing. An extracted source
may outlive all those handles. Its aliases/control blocks remain charged
through last actual backing/Weak free; leaked backing remains charged.
Separately copied error backing requires its own identity/pre-admission;
copying/formatting outside this API remains part of U4/U6, not protected by
the wrapper. Original certificate/protocol error semantics are unchanged.

### 3.3 Required private provider/verifier/parser/storage interfaces

These are proposed **internal** contracts, not additional public project APIs,
existing rustls methods or permission to alter upstream traits. `Op` carries
the same ledger, original deadline/stop and cumulative effect facts as §3;
it creates no heap or new timeout. Private storage types own their charged
backing and have no infallible Clone or escape to untracked Vec/Box/Arc.
The declarations fix inputs/outputs/error propagation for the required
adapters; U1–U4 still block their implementation and interoperability.

R1 context construction: T/S leaves build private Op from their explicit
original deadline/stop and existing ledger, never Instant::now()+timeout or
a new flag. Connection methods copy their current phase/cumulative I/O facts;
standalone setup/ledger/config/bytes methods have local Setup/zero-I/O facts,
which say nothing about earlier caller effects. The adapter preserves those
earlier facts separately. Before RetainedBudget::new has a ledger, bootstrap
uses the same borrowed deadline/stop and inline Setup facts; it does not
construct an Op requiring a nonexistent budget. Once initialized, it can
borrow that ledger. O needs no Op. C cleanup must not synthesize a fresh Op
to evade expiry or demand a still-live operation token; it uses existing
ownership/accounting metadata and the mandatory §4 reclamation obligation.

```rust,ignore
struct Op<'a> {
    budget: &'a RetainedBudget,
    deadline: std::time::Instant,
    stop: &'a std::sync::atomic::AtomicBool,
    context: ErrorContext,
}
struct BudgetedRoots { /* private charged ownership */ }
struct BudgetedProvider { /* fixed ring implementation; private */ }
struct BudgetedVerifier { /* unchanged WebPki policy; private */ }
struct BudgetedPeerChain { /* private H backing */ }
struct BudgetedParsedHandshake { /* private H backing */ }
struct BudgetedTlsBuffer { /* private fixed membership */ }
struct BudgetedKeyExchange { /* private H backing */ }
struct BudgetedHash { /* private H backing */ }
struct BudgetedHmacKey { /* private H backing */ }
struct BudgetedHkdf { /* private H backing */ }
struct BudgetedCipher { /* private H backing */ }

fn try_own_roots(op: &mut Op<'_>,
    roots: &'static [rustls::pki_types::TrustAnchor<'static>])
    -> Result<BudgetedRoots, BudgetedClientError>;
fn try_new_ring_provider(op: &mut Op<'_>)
    -> Result<BudgetedProvider, BudgetedClientError>;
fn try_new_verifier(op: &mut Op<'_>, roots: &BudgetedRoots,
    provider: &BudgetedProvider) -> Result<BudgetedVerifier, BudgetedClientError>;

impl BudgetedVerifier {
    fn try_verify_server_cert(&self, op: &mut Op<'_>,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8], now: rustls::pki_types::UnixTime)
        -> Result<rustls::client::danger::ServerCertVerified, BudgetedClientError>;
    fn try_verify_tls12_signature(&self, op: &mut Op<'_>, message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct)
        -> Result<rustls::client::danger::HandshakeSignatureValid, BudgetedClientError>;
    fn try_verify_tls13_signature(&self, op: &mut Op<'_>, message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct)
        -> Result<rustls::client::danger::HandshakeSignatureValid, BudgetedClientError>;
}

fn try_parse_handshake(op: &mut Op<'_>, version: rustls::ProtocolVersion,
    encoded: &[u8]) -> Result<BudgetedParsedHandshake, BudgetedClientError>;
fn try_copy_der(op: &mut Op<'_>, der: &[u8])
    -> Result<BudgetedBytes, BudgetedClientError>;
fn try_copy_ocsp(op: &mut Op<'_>, ocsp: &[u8])
    -> Result<BudgetedBytes, BudgetedClientError>;
impl BudgetedPeerChain {
    fn try_from_parsed(op: &mut Op<'_>, message: BudgetedParsedHandshake)
        -> Result<Self, BudgetedClientError>;
    fn try_share(&self, op: &mut Op<'_>) -> Result<Self, BudgetedClientError>;
    fn try_clone_owned(&self, op: &mut Op<'_>) -> Result<Self, BudgetedClientError>;
}
impl BudgetedTlsBuffer {
    fn try_new(op: &mut Op<'_>, class: AllocationClass)
        -> Result<Self, BudgetedClientError>;
    fn try_reserve(op: &mut Op<'_>, buffer: &mut Self, additional: usize)
        -> Result<(), BudgetedClientError>;
    fn try_push_copy(&mut self, op: &mut Op<'_>, bytes: &[u8])
        -> Result<(), BudgetedClientError>;
    fn try_push_owned(&mut self, op: &mut Op<'_>, bytes: BudgetedBytes)
        -> Result<(), BudgetedClientError>;
    fn try_read_into(&mut self, op: &mut Op<'_>, out: &mut [u8])
        -> Result<usize, BudgetedClientError>;
}

impl BudgetedProvider {
    fn try_start_kx(&self, op: &mut Op<'_>, group: rustls::NamedGroup)
        -> Result<BudgetedKeyExchange, BudgetedClientError>;
    fn try_start_hash(&self, op: &mut Op<'_>, algorithm: rustls::crypto::hash::HashAlgorithm)
        -> Result<BudgetedHash, BudgetedClientError>;
    fn try_hmac_key(&self, op: &mut Op<'_>, algorithm: rustls::crypto::hash::HashAlgorithm,
        key: &[u8]) -> Result<BudgetedHmacKey, BudgetedClientError>;
    fn try_hkdf_extract(&self, op: &mut Op<'_>, algorithm: rustls::crypto::hash::HashAlgorithm,
        salt: Option<&[u8]>, secret: &[u8]) -> Result<BudgetedHkdf, BudgetedClientError>;
    fn try_hkdf_from_okm(&self, op: &mut Op<'_>, algorithm: rustls::crypto::hash::HashAlgorithm,
        okm: &rustls::crypto::tls13::OkmBlock) -> Result<BudgetedHkdf, BudgetedClientError>;
    fn try_tls12_prf(&self, op: &mut Op<'_>, algorithm: rustls::crypto::hash::HashAlgorithm,
        output: &mut [u8], secret: &[u8], label: &[u8], seed: &[u8])
        -> Result<(), BudgetedClientError>;
    fn try_new_cipher(&self, op: &mut Op<'_>, suite: rustls::CipherSuite,
        key: rustls::crypto::cipher::AeadKey, iv: &[u8], extra: &[u8], encrypt: bool)
        -> Result<BudgetedCipher, BudgetedClientError>;
    fn try_fill_random(&self, op: &mut Op<'_>, out: &mut [u8])
        -> Result<(), BudgetedClientError>;
}
impl BudgetedKeyExchange {
    fn try_complete(self, op: &mut Op<'_>, version: rustls::ProtocolVersion,
        peer_public_key: &[u8]) -> Result<BudgetedBytes, BudgetedClientError>;
}
impl BudgetedHash {
    fn try_update(&mut self, op: &mut Op<'_>, input: &[u8]) -> Result<(), BudgetedClientError>;
    fn try_fork(&self, op: &mut Op<'_>) -> Result<Self, BudgetedClientError>;
    fn try_finish(self, op: &mut Op<'_>)
        -> Result<rustls::crypto::hash::Output, BudgetedClientError>;
}
impl BudgetedHmacKey {
    fn try_sign(&self, op: &mut Op<'_>, input: &[&[u8]])
        -> Result<rustls::crypto::hmac::Tag, BudgetedClientError>;
}
impl BudgetedHkdf {
    fn try_expand(&self, op: &mut Op<'_>, info: &[&[u8]], output: &mut [u8])
        -> Result<(), BudgetedClientError>;
}
impl BudgetedCipher {
    fn try_encrypt(&mut self, op: &mut Op<'_>, content_type: rustls::ContentType,
        version: rustls::ProtocolVersion, seq: u64, plaintext: &[u8])
        -> Result<BudgetedBytes, BudgetedClientError>;
    fn try_decrypt_in_place<'a>(&mut self, op: &mut Op<'_>, content_type: rustls::ContentType,
        version: rustls::ProtocolVersion, seq: u64, ciphertext: &'a mut [u8])
        -> Result<&'a [u8], BudgetedClientError>;
}
```

Parsing handles every currently reachable Codec variant, preserving existing
wire validation/limits; no cert-count cap follows from the signatures.
DER/OCSP output and shared-secret bytes are H; encrypt output has fixed
TxRecord membership with H overlay for handshake records. Buffer constructors
accept only the three queue classes and HandshakeTxRecord; other classes or
cross-ledger ownership transfers return InvalidState before mutation.
`try_reserve(additional)` means checked required capacity at least
length+additional with a source-proved actual Layout; it is not permission
for unchecked geometric growth. Peer-chain phase transfer moves ownership,
without reclassifying H or deep copying. Errors consuming owned inputs may
drop their real backing; they never release surviving aliases or caller
roots/config. Clone/share constructors use §3.2 mapping and §4 transactions.

Verification passes the same certificate/name/time/signature inputs to the
same policy and preserves actual OCSP handling; it does not claim that the
current verifier validates stapled OCSP merely because it receives it.
Direct-return upstream `Hash::start/Context::fork`, `Hmac::with_key`,
`Hkdf::extract_*/expander_for_okm` and `Tls12/13AeadAlgorithm::{encrypter,
decrypter}` do not implement these Result signatures. Their source traits
(`crypto/{hash,hmac,tls12,tls13,cipher}.rs`) and private callback storage are
explicit U1/U4 interoperability edges. Wrapping their return in Ok after an
infallible allocation is forbidden. A reviewed fallible/prepaid adapter must
be shown source-complete before these operations can be called; otherwise
they return UnresolvedClosure before callback entry. None of the private
signature types is an assertion that this can be implemented within17.

## 4. Ledger, backing lifetime and failure-path contract

Proposed implementation representation: an owned, fallibly allocated ledger
core with fixed counters/nonrecursive synchronization; per-allocation
intrusive metadata/header records rather than a growing heap HashMap; one
fallibly allocated131,072-byte diagnostic slot. The actual private Rust
layouts must be measured in the future implementation, not invented here.
For a payload Layout L and header Layout M:

```text
effective_layout = M.extend(L)?.0.pad_to_align()
charge = effective_layout.size()               // checked, includes padding
new T = checked_add(retained_T, pending_T, charge)
new views = checked_add(view_live, view_pending, charge for each member bit)
accept iff new T<=8388608, H<=1048576, every buffer<=65536,
           Diagnostic payload<=131072
```

Zero-sized payloads need no System allocation, but any nonzero shared control
or header backing is still charged. Metadata carries allocation identity,
original membership, payload/effective Layout, owner/reference/deallocation
state and original charge. No header identity/token nonce history Vec exists.
Requested Layout is the cap's allocation accounting unit; native allocator
usable-size rounding/metadata is separately disclosed, not hidden Rust
allocator-managed backing.

Bootstrap first computes **all** ledger/control/header/diagnostic layouts
and checks their simultaneous sum against T. With no heap ledger yet, an
exclusive inline bootstrap reservation record accounts that sum. Direct
fallible allocation must return null as an ordinary constructor Err; it must
not call handle_alloc_error. Only then is the permanent ledger initialized
with those charges, without momentarily charging both bootstrap and core for
the same backing. Partial bootstrap failure frees only successfully created
backing, releasing its charge after physical free. This requires a reviewed
stable allocator/storage implementation (U4), not Arc::new followed by a check.

All reserve/commit/rollback/free/refcount transactions are linearizable over
T and all membership views together. A single load/check followed by separate
fetch_add is forbidden. Pending reservations consume allowance. A reservation
is an affine token with no allocating Drop; on null/failure only its unused
new credit rolls back. Both constructor pre-reservation and consumption into
allocation identity are checked; a foreign/duplicate token does not mutate
legitimate ownership or counters. No saturating arithmetic/wraparound.

Moving growth **and shrinking** reserve the full new effective Layout while
old remains charged, including temporary control metadata. Allocate-copy-
publish-free-old ordering is mandatory; do not use an unproved in-place
realloc discount. On new allocation failure old ownership/capacity/content
remain valid. Only confirmed old free releases the old charge. If the actual
capacity exceeds the reserved bound, the operation is unproved and stops;
checking it after allocation cannot retroactively establish pre-admission.

Aliases retain one backing/ID; true copies add separate IDs. Partial VecDeque
consumption, Bytes split/freeze, retained slices and transferred peer chains
keep the full backing charge. Shared payload drops at last strong reference;
the shared allocation/control block remains through last Weak. For a combined
shared block with payload inline, its full Layout remains charged through
Weak; separately owned child buffers can release when physically freed at
last strong. Ledger core itself survives while any allocation/deallocator or
weak handle references it; its charge is not removed when ACTIVE/handshake or
the last user-facing connection ends. Leaked/referenced backing stays charged.

R2 includes public rustls::Error source extraction/replacement and moves of
its String/Vec/other owned children. The allocation identity, original H/T
membership and ledger retention must follow **each allocation**, independent
of which Rust wrapper currently contains its pointer. Dropping an outer
error guard cannot free a live extracted source's charge. An allocation-
attached header/deallocator retaining the ledger is a possible proposed
mechanism, not established interoperability with ordinary stable std-owned
storage or every copied/error variant. Source moved beyond the connection/
config/budget, aliases/Weak and leaks must obey the same physical-free rule.
U4 explicitly blocks any claim of lawful enforcement until that mechanism
is proved; this precision neither demonstrates a fatal escape nor changes
accepted error semantics.

Admission synchronization never holds its lock while invoking provider,
verifier, user callback, logging, System allocator or I/O. Reservation and
commit are separate short transactions while reserved bytes remain visible.
An admission unable to acquire synchronization in its original operation
deadline returns the nonallocating AdmissionBusy/DeadlineExpired outcome.
No signal handler enters the ledger; it sets the existing atomic flag only.
Free/rollback cannot be silently discarded on contention: charge stays until
physical free and authenticated accounting update; any deferred record must
use already-paid metadata and retain backing safely. The exact nonblocking
reclamation protocol, deadlines and Arc interoperability are **U4**, not a
claim that this text proves them. Race/recursive-free tests must close U4.

R1 cleanup is required even after stop/deadline expiry. Operational admission
may refuse, but existing allocations/reservations/refcount decrements cannot
be abandoned by a stop check. If bounded reclamation cannot finish, retain
real ownership/charges with already-paid metadata and report the unresolved
state; do not claim free, zero retained bytes or physical owner completion.
No unbounded cleanup allowance, new deadline or invented successful result
is granted; its actual storage/deallocator bound remains U4/U6.

Refusal is a fixed inline enum/scalars plus a bounded slot overwrite; no
format!, Vec, String, Arc/Box creation, IO error boxing, unwinding, panic,
logging callback or recursive allocator call. Diagnostic slot/control bytes
are already in T. Evidence truncation is explicit, never grows the slot or
pretends lost evidence was captured. Atomic nonallocating counters/fixed
trace records report rejected underlying calls=0. Underlying allocation count
for an allocator-null failure is1, distinct from quota rejection0.

The diagnostic ACTIVE allocator in the historical probes is not this ledger.
The [GlobalAlloc safety contract](https://doc.rust-lang.org/std/alloc/trait.GlobalAlloc.html#safety)
forbids allocator unwinding; current documentation is1.99, not a pinned1.98.1
implementation-source audit. [Arc API documentation](https://doc.rust-lang.org/std/sync/struct.Arc.html#method.try_new)
is likewise background only: no nightly/unstable fallible Arc API or private
std Arc layout is assumed available on pinned1.98.1. U4 requires exact proof.

## 5. Reachable allocation closure and explicit uncovered edges

Pinned closure from unchanged Cargo.lock: rustls0.23.45 std/ring/tls12,
rustls-pki-types1.15.1, rustls-webpki0.103.15 std/ring, ring0.17.14 alloc,
bytes1.12.1/tungstenite0.30.0 for the separate app boundary. Publisher source
audit in receipt6099645626 verified57 archives/2733 files, inventory SHA256
`58fd629ec430542ec37da0da4e20c34549d7a1ca9eeb9b3ef98bbd10d731fd52`.
rustls archive SHA256
`0d41d731c7d2f962d1ccc364cec258de3c0e93b38c2fb3ba97ac74513048d634`;
Cargo.lock SHA256
`283ec6c56c00d0d0306cd2ff171bf9752f09a5195a5f215a4af981d46657dbed`.
Source paths/line numbers below refer to these exact archives, not latest.
They can be reproduced from publisher
[rustls](https://static.crates.io/crates/rustls/rustls-0.23.45.crate),
[pki-types](https://static.crates.io/crates/rustls-pki-types/rustls-pki-types-1.15.1.crate),
[webpki](https://static.crates.io/crates/rustls-webpki/rustls-webpki-0.103.15.crate),
[ring](https://static.crates.io/crates/ring/ring-0.17.14.crate) and the lock.

```text
new_ring/build
  -> ring provider vectors -> root descriptors/owned inputs
  -> verifier/config/cache/control blocks -> std allocation/refcounts
new/ClientHello
  -> client state/extensions -> codec/encoding/transcript
  -> ring KX + hash/HMAC/HKDF + shared secret -> std/crypto working heap
read_tls/process
  -> deframer/spans -> Message/Codec -> every parsed TLS payload/extension
  -> cert/DER/OCSP ownership -> WebPki server/name/signature validation
  -> webpki EKU errors / ring RSA limbs and working copies -> std heap
  -> TLS12/13 state/keys/peer chain + ticket/control/alert/error paths
traffic/read_plaintext/write_plaintext/write_tls/retire/drop
  -> received/send queues + record prefixed encoding + crypto objects
  -> NewSessionTicket/KeyUpdate + error paths + final backing release
```

Both successful and rejected TLS messages are reachable. Disabled resumption
does not prove that parser/session-value allocations before a no-op store
are absent. Nonnegotiated/unknown extensions can allocate before rejection.
The map includes those edges, constructors, error conversions and cleanup.
It is **a source-backed inventory with unresolved closure**, not a certified
complete call-site bound. Unknown capacities/call counts are labelled stops;
none is replaced by a universal safe envelope or native exclusion.

Notation: `L(T,C)=Layout::array::<T>(C)` and `B(T)=Layout::new::<T>()` on
the actual future pinned target; add header/alignment with §4. `C` is actual
capacity, `n` input length/count. A formula identifies the required layout,
not a measured numeric bound. Each row covers the listed constructors,
growth and deep-clone sites; unlisted calls remain stopped under U7.

| ID / source and allocation moment | Layout/capacity; owner, category and free point | Pre-reservation / lawful outcome / scope status |
|---|---|---|
| C00 new budget/core/header/evidence (§4) | B(LedgerCore/control), per-ID header Layout, diagnostic131072; T/ledger through final backing/Weak free | Checked bootstrap sum before allocator; direct fallible null→ResourceRefused. Actual representation/reclamation proof U4 |
| C01 rustls `crypto/ring/mod.rs:31–38` default_provider .to_vec | L(SupportedCipherSuite,9), L(&dyn SupportedKxGroup,3) before config; T/config until final provider owner free | New builder must make these fallible without first calling default_provider unaccounted; numeric element layouts NOT_MEASURED. Outside17, U1; existing Vec call not protected |
| C02 `client/builder.rs:53–65,157–186`, `client/client_conn.rs:360,497,515`; `webpki/server_verifier.rs:161–205` | Provider/root/verifier/config Arc blocks; root descriptors121×72=8712 minimum, config inline344/provider112 are not heap totals. Cache/compression/no-auth/keylog/time controls coexist, strong/Weak lifetimes | Include transient default caches before later disable; new builder can construct disabled state directly. Fallible shared storage/std interoperability U4; verifier construction outside17 U1 |
| C03 `builder.rs:228–232` supported KX algorithm Vec/filter; `webpki/verify.rs:87–92` supported_schemes collect | L(KeyExchangeAlgorithm,C), L(SignatureScheme,C); config validation/ClientHello, T or H by owner; actual Vec free | Fixed provider lists give count basis, not proof of exact realloc Layout. Both files outside17; U1. Could bypass helper only with equivalent verification/selection proof, not disable algorithms |
| C04 pki-types `server_name.rs:61–65,199–202`, `lib.rs:1034–1049` borrowed→owned and owned Clone | L(u8,C) DNS/DER; T/name or H/certificate; actual copy owner drop, original and copy coexist | Copy through fallible budgeted storage before wrapping moved Vec; no infallible into_owned/Clone. Avoidable at allowed callers if every call audited; pki implementation patch not allowed, U2/U7 |
| C05 `client/hs.rs:215–389` ClientExtensions Box, group/keyshare/scheme/cookie/extension Vecs and encoding | B(ClientExtensions), L(private entry,C), byte capacities; H until extension/message/state actual drop | Checked count/pre-scan and exact fallible capacities; existing constructors infallible; allowed hs.rs/handshake/codec, but helper closure U1/U7 |
| C06 `msgs/deframer/buffers.rs:209–262`, `msgs/deframer/handshake.rs:61,72,235` | Byte Vec observed65536 and spans initial16/growth; H until backing actually freed, including traffic retention; old+new for resize/shrink | Existing65535 handshake wire bound is not expanded heap proof. Fallible old+new admission required, no new wire/count cap. These paths in17; future tests NOT_RUN |
| C07 `msgs/codec.rs:104,219–235`, all `msgs/handshake.rs` Codec lists/extensions and length-prefixed owned payloads | L(T,C) for each parsed list, encoded byte Vec and nested lists; H; input/borrowed descriptors/owned descriptors coexist | Pre-scan existing wire encoding, checked counts/bytes and fallible exact reserve; malformed/nonnegotiated payloads included. Observed TLS12 certificate24-byte descriptors393216→786432 require1179648 moving admission before other H bytes; not measured retained breach. In17 but full tagged payload closure U7 |
| C08 `msgs/base.rs:43,166,214`, handshake TLS12 chain1703+, TLS13 CertificateEntry1800+ | L(u8,C) DER/unknown-extension/OCSP copies + L(CertificateDer/CertificateEntry,C); H through actual payload/chain free | Fallible copy/ownership conversion, old input and new storage simultaneously charged. Certificate/OCSP may fail before validation; do not defer admission to public verifier. In17; U2 for downstream ownership methods |
| C09 `client/tls13.rs:1106,1174,1201`, `client/tls12.rs:885,935`, `client/common.rs:31` | OCSP owned extension→end_entity_ocsp clone→.to_vec; peer chain descriptors and DER; H across handshake→traffic→last connection/copy free | Replace redundant/deep copies lawfully with fallible/moved ownership; borrowed chain APIs in §3 prevent untracked export. client/common.rs outside17 is U1, not automatically covered by tls12/13 changes |
| C10 `client/tls12.rs` and `client/tls13.rs` every state Box::new/into_owned; `common_state.rs:225` | B(each concrete state); H. Old state and successor may coexist; embedded config Arc is an alias, nested owned Vecs are separate | Source-size Layout per concrete state before allocation; successor refusal cannot erase old ownership/effects. Listed files allowed; concrete layout/call-count implementation proof NOT_RUN |
| C11 `vecbuf.rs:15–26,88,140,149,160`, `common_state.rs:461,483,774` | L(Vec<u8>,deque capacity), L(u8,chunk C) plus controls; RxPlain/TxPlain/TxRecord, H overlay when relevant. Partial prefix keeps full chunk until pop | Entire buffer membership≤65536; set_buffer_limit only two logical queue lengths, received queue default16KiB. Fallible descriptor/chunk pre-reserve and copy; paths in17 but encoding/crypto edges U1 |
| C12 `msgs/message/mod.rs:87–105,204,223`, `message/outbound.rs:79,209,219–225,261` | Encoded/owned message copies, PrefixedPayload capacity HEADER_SIZE(5)+n+cipher expansion, old input/output overlap; H or buffer memberships until actual release | Checked arithmetic with actual suite encrypted_len; fallible exact record construction before callback or enqueue. Both module files outside17, U1 |
| C13 `hash_hs.rs:47,118,181`; `crypto/ring/hash.rs:17,48`; `crypto/ring/hmac.rs:17` | Transcript Vec C, B(digest Context), B(HMAC Key), context fork/copy; H until actual free, old/new/context may coexist | hash/digest fixed arrays are inline within Box; Box remains Rust heap. Private type Layout and retained call count need proof; all listed files outside17, U1 |
| C14 `crypto/ring/kx.rs:42–59`; `crypto/ring/mod.rs:189–201`; `crypto/mod.rs:650–655` | B(KeyExchange); SharedSecret Vec from borrowed secret (algorithm-size bytes); H even through key derivation/drop. Secret wiping is not deallocation | KX start Result does not make Box::new fallible; shared-secret conversion infallible. New fallible provider entry or prepaid storage callback contract needed, U1; no group/suite removal |
| C15 `crypto/ring/tls12.rs:136,148,185,195`, `crypto/ring/tls13.rs:174,182,268,280,287`; `crypto/tls13.rs:57,63,70,91` | B(each concrete encrypter/decrypter/expander), extract-secret Vec; H through traffic/KeyUpdate until free; replacing keys pays old+new | Several provider trait methods return Box directly, not Result; outer wrapping cannot catch quota abort. Private Layout/exact callback bound or fallible provider contract is unresolved U1/U4, no dependency substitution |
| C16 `tls12/mod.rs:63,198,222,248–250` PRF/keyblock/export/random concatenation and cipher derivation | L(u8,C) sized by suite/key/IV/PRF output plus source/copy; H till keyblock/result free | Fallible internal storage or exact source-proved prepaid envelope, not unconstrained export method. This module outside17, U1 |
| C17 `client/tls13.rs:1478–1520`, `client/tls12.rs:1204,1223–1242`; `msgs/persist.rs:245,248,414` | Session secret/ticket Vec, Arc peer chain/ticket and ServerName copy before store insertion; H/T by original owner until actual free | Disabled store does not bypass session-value creation. Skip useless construction only with equivalent disabled-resumption semantics at allowed client caller; otherwise msgs/persist.rs outside17 U1; default cache/limited_cache growth closure unresolved |
| C18 `webpki/server_verifier.rs:264–289`; rustls-webpki `verify_cert.rs:79–147,786–825`; `ring_algs.rs:35–45` | Parsed cert/path borrowed stack array (not extra heap); dynamic signature callback can reach RSA heap. Original verifier/roots/config controls retained | Chain, time, EKU, name and signature checks unchanged. Core path array does not establish no-heap verifier. Full normal/error callback bound U2/U3 |
| C19 webpki `verify_cert.rs:565–585,667–670` EKU diagnostic Vec/collect; error conversion through rustls webpki mapping | L(Vec<usize>,C) plus each OID's L(usize,C); H through original error owner drop; variable count/input, geometric growth possible before RequiredEkuNotFoundContext | No proved prepaid capacity/call bound or fallible error construction; exact webpki file delta would need new dependency scope. **U2**, cannot call it native or silently delete verification/EKU policy |
| C20 ring `rsa/verification.rs:198–229`→`rsa/public_key.rs:111,161–194`→`bigint/{modulusvalue,modulus}.rs` and `boxed_limbs.rs:51–74` | Rust Box<[Limb]> zero/Clone for modulus, Montgomery values and exponentiation temporaries; L(Limb,C), C based on modulus (existing ring maximum); H till each real free. Result buffer1024 is stack, limb boxes are heap | Individual limb formula is known; maximum simultaneously retained count/copies and null/fallible outcome are not closed. Source Result<_,Unspecified> does not make vec!/Box cloning fallible. U3; no ring patch/substitution/algorithm narrowing authorized |
| C21 ring ECDSA/Ed25519/agreement/digest/HMAC/HKDF/rand via selected callbacks | Fixed inline crypto structures/arrays where source proves them; provider boxes separately C13–15. SIMD/FFI/native working memory distinct only with source evidence | Production RSA path remains alloc-active. Test-only padding Vec (`rsa/padding.rs:167`) is not a client-verify allocation; do not invent it as production evidence. Exact active-platform initialization/getrandom/std/native closure still U3/U4 |
| C22 rustls `check.rs:49,64`, `rand.rs:13`, `x509.rs:32,48`; `Error`/webpki conversion and Clone | Expected-type Vecs, generated random Vec, DER wrapping/error owned data; H until error/output backing free, including adverse messages | Error/refusal path must prepay, not allocate because the main operation already failed. These helpers outside17 U1/U2; formatting is forbidden on quota failure |
| C23 Rust std alloc/Vec/Box/Arc/String/VecDeque and TcpStream error/timeout/clock implementations | Actual effective Layout, capacity-growth, Box/Arc control alignment, Weak retained control; category inherited from owning allocation; any Rust heap std temporary included | No dependency/version exception and no unstable Arc/private-layout assumption. Stable fallible storage, allocator-null return and deadline-safe reclamation are U4; exact pinned platform source needed |
| C24 provider/verifier/time/keylog/session/compressor/IO callbacks, aliases and cleanup | Their owned heap/control, temporary heap, fixed inline/foreign native state must be separately classified; controls persist to real last free | Proposed surface fixes built-in callback identities but does not waive their allocation closure. Unaudited callback admission returns UnresolvedClosure; unrestricted callback support is not proposed |
| C25 `record_layer.rs:42–43`→`crypto/cipher.rs:163–170`; cipher error conversion `:93–95` | Initial invalid encrypter/decrypter Box allocations (concrete zero-sized placeholders may be zero Layout, but must prove this); General error String conversion if reachable; H until actual free | Zero-sized Box is not automatically evidence of a heap call; audit concrete Layout/callback branch. Fallible replacement/error mapping or proved no-allocation branch required; cipher trait/storage file outside17 U1/U4, record_layer.rs is a named reachable edge requiring U7 branch proof before any proposed edit |

The table supplies exact source anchors and Layout formulas; unknown numeric
layouts/capacities/call-count bounds are **NOT_ESTABLISHED**. Historical inline
sizes and diagnostic peaks are not substituted for them. No branch is
excluded merely because a search for Vec::new found nothing: trait dispatch,
derived Clone/collect, error conversion and std internals remain in U7.

Nonnegotiated resumption/0RTT, no client-auth/key loading, no ECH/QUIC route,
disabled logging and no enabled zlib/brotli are the current route/features,
not new peer input caps. Their parser/error paths are still C07/C22.
Unbuffered `conn/unbuffered.rs:42–136` reuses core.deframe/process_msg; it is
not an alternate allocation-proof bypass. Post-handshake KeyUpdate/tickets
must be accounted even when the application then rejects wants_write.

## 6. Phase-specific refusal and accepted owner semantics

| Point | Lawful result and retained facts | Forbidden inference |
|---|---|---|
| Ledger/config constructor before socket work | Returned fixed Err; rollback new reservations/free completed objects; caller root/budget still retained | No assertion about a caller socket/owner action that happened earlier |
| Connection/ClientHello construction before its TLS write | ResourceRefused/DeadlineExpired/Stopped without engine network call; retained caller config, existing epoch/Close unaffected | No fictional Connect completion/Closed/Ready; TCP may already have been opened by the outer attempt |
| Before read/enqueue/process/write callback | Source-proved operation envelope admitted first; refusal counters show rejected underlying alloc/I/O calls0 | A generic accepted dispatch error is not promoted to known-no-effect or retry entitlement |
| After any returned read or potential partial write | Cumulative scalar progress retained; failure returned with Handshake/Traffic phase, already-read input/output/state accounted; no byte rollback or new deadline | No retry/re-enqueue of an ambiguous frame, no regenerated Ping/Close/Timer or resubscription |
| Parser/verifier/crypto refusal after receive | Typed error through engine, no diagnostic allocator abort/panic/unwind; all still-owned backing charged; no alert flush on failure | No certificate validation success, no owner completion or implicit remote/local cessation |
| Retire/drop and owner Close | Retire has no network effect; app's sole owner must execute lawful synchronous shutdown of this exact epoch, under the existing Close lease/deadline, before genuine completion | TLS object drop or shutdown error is not Ready, Close success or seal; remaining aliases/callback work cannot be erased |

Every error from an **invoked accepted CommandLease dispatch** maps to the
existing DispatchFailed/AmbiguousEffect::Unknown, even if this leaf API
recorded no write. Identity/order rejections before dispatch keep their
accepted preserving semantics. An existing original mandatory Close stays
Pending on Err/Drop; only accepted discovery/reclaim/readiness may issue that
same Close. No new Close identity, slot, lease, epoch, W or retry is created
by ResourceRefused. This ADR adds no project owner API.

After engine refusal, cease non-Close operational service. Use already-paid
failure evidence and the lawful accepted diagnostic/storage lifecycle; do
not fabricate a received Disconnected, authenticated Timer/Down receipt or
storage success. TimerA original +30s/+15s/FIFO/token/one-shot rules persist.
Pending same Close and unsettled obligations keep Unknown/NotReady/incomplete;
only a genuine borrowed Ready proof can authorize finalization, with accepted
disk-failure diagnostic close. A successful native shutdown alone is not a
substitute for full exact local-epoch physical cessation.

SIGABRT/OOM panic/unwind is not the quota outcome specified here. Any actual
process kill remains KILLED/Unknown/incomplete, not returned dispatch Err,
Close/Ready or seal. Zero allocator calls on quota refusal must be verified
on protected paths; the previous expected-abort PASS is insufficient.

## 7. Sufficiency boundary and unchanged application contracts

Even a proved fork would cover only its **actually protected** constructor,
operation, callback and free closure. It does not close FrameSocket/Bytes
initial131072 ingress/shared backing, encoded outbound buffers, HTTP handoff,
authority Rc/Weak/work owners, supervisor queues/payload copies, WalWriter
validator clones/encoder Vecs, JSON/decoded copies, signals, config/evidence
or capture/replay ownership. They need the same T ledger and separate source/
pre-admission/lifetime proof of the real application composition.

**R3 / explicit U6 ABI stop:** the §3 public TLS surface is **not a complete
application accounting ABI**. Borrowing/sharing RetainedBudget does not define
a general cross-crate admission/storage/free interface. The same-T bridge
for FrameSocket/bytes/config/owner/supervisor/WAL/decoder/evidence is still
**undefined and unaccepted**. Its exact cross-crate ownership, admission,
allocation identity, alias/free/lifetime contract and permitted API/path
scope require a separate Architecture/owner review before U6 integration
or activation. This corrective increment does not design or implement that
bridge or add a general-purpose app budget API. Absent components remain
NOT_RUN; there is no demand for whole-app PASS before they exist.

An existing one-byte Bytes payload+clone retains131072 backing +40 shared
metadata until final alias drop. Conservative owner encoding workspace
8×MAX_FRAME_LEN and supervisor decoder report24,146,960 are reports, not
measured allocations or justification to change accepted APIs/budgets.
Do not sum overlapping reports/inline layouts with charged heap IDs. Inert
permitted components may be built after their own implementation permission;
absence is NOT_RUN and never a whole-app PASS or an upfront impossibility
claim. U6 is a proof gap, not permission to edit accepted crates.

Preserve N1/M16/R4/B262144, message65536/outbound4096, diagnostics131072,
capture60s/4096 messages/16MiB payload, WAL32MiB/32768 records, TCP2s/TLS3s/
upgrade2s within7s, frame2s/passive quantum≤50ms/send1s, Close1s/256steps/
two attempts at the same original Close, cooperative5s only after storage
returns. Preserve sole owner/SessionTurn, TimerA, native-control fail-stop,
ACK NotReconstructed and usable_data=false. No new wire/input/cert-count
cap, budget increase, TLS12 removal, dependency substitution or altered
certificate verification/revocation policy. WALv1/F1/F2 and all accepted
project APIs/specs/ADRs remain byte-identical. M1 not accepted; #21/M2 idle.

## 8. Exact future allowlist and publisher provenance

**PROPOSED ONLY — not implementation permission.** ALLOC45-D1's119 exact
publisher files would be copied into `vendor/rustls-0.23.45/`, preserving
licenses/source archive hash and a manifest of every copied file checksum.
Only these seventeen existing .rs files plus normalized Cargo.toml may
change under that original proposal:

```text
src/msgs/codec.rs
src/msgs/handshake.rs
src/msgs/base.rs
src/msgs/deframer/buffers.rs
src/msgs/deframer/handshake.rs
src/vecbuf.rs
src/conn.rs
src/common_state.rs
src/error.rs
src/conn/unbuffered.rs
src/client/builder.rs
src/client/client_conn.rs
src/client/handy.rs
src/client/hs.rs
src/client/tls12.rs
src/client/tls13.rs
src/lib.rs
Cargo.toml
```

Exactly three proposed new vendor files:

```text
src/budget.rs
src/client/budgeted.rs
tests/retained_budget.rs
```

All remaining publisher copies, including Cargo.toml.orig, are checksum-
identical; no wildcard edit authority follows from copying the119 files.
Application manifest selection/sole sequential Cargo.lock regeneration and
existing allowed capture glue are future work only after full scope approval.
Root manifest/toolchain/CI and other dependencies remain outside this docs
increment; no root patch, second writer or unrelated upgrade is proposed.
The vendor's Cargo workspace isolation/dev dependencies/features must be
verified before accepting a future integration; unchanged workspace members
and no unapproved packages are required, not assumed (U5).

### 8.1 Concrete unresolved deltas and required reviewer dispositions

| Stop | Concrete evidence / missing delta | Required Architecture/owner decision; no implementation now |
|---|---|---|
| U1 rustls helper/provider closure beyond17 | At minimum `src/builder.rs`, `src/client/common.rs`, `src/msgs/message/mod.rs`, `src/msgs/message/outbound.rs`, `src/hash_hs.rs`, `src/crypto/mod.rs`, `src/crypto/ring/{mod,kx,hash,hmac,tls12,tls13}.rs`, `src/crypto/{hash,hmac,tls12,tls13,cipher}.rs`, `src/tls12/mod.rs`, `src/webpki/{server_verifier,verify}.rs`, `src/check.rs`, `src/rand.rs`, `src/x509.rs`, `src/msgs/persist.rs` contain C03/C09/C12–17/C22/C25 allocations or direct-return allocation callback contracts outside17; `src/record_layer.rs` reaches initial cipher constructors and needs explicit zero-sized Layout/branch proof, not an automatic edit | Request a reviewed, exact additional rustls path/API delta OR a source-complete equivalent bypass/prepaid implementation design confined to17. This ADR requests neither automatic expansion nor algorithm removal; original17 cannot be accepted as a closed scope without that resolution |
| U2 pki-types/webpki ownership/error closure | pki-owned Clone/into_owned plus webpki `src/verify_cert.rs:565–585,667–670` owned EKU diagnostic growth and rustls error conversion | Decide how a lawful pre-admitted upper bound/fallible storage preserves all verification/EKU/error semantics. A pki/webpki patch would require separately named version/files/contract/provenance; not in ALLOC45-D1 and not approved here |
| U3 ring RSA working heap | ring `src/arithmetic/bigint/{boxed_limbs,modulus,modulusvalue}.rs`, `src/arithmetic/bigint.rs`, `src/rsa/{verification,public_key,public_modulus}.rs` reachable zero/clone/Montgomery temporaries; callback Result does not make them fallible | Require complete per-algorithm allocation multiplicity/lifetime/upper-bound and lawful allocator-null mechanism. Any ring source/API change is a separate dependency delta; keep all current verification algorithms, no native exclusion or substitution |
| U4 stable storage/std/ledger concurrency | Box/Arc direct-return interfaces, Weak backing, pinned1.98.1 std allocation/IO/error source, intrusive header alignment, atomic reservation and contention-safe free/refcount/bootstrap not yet implemented/proved; R1 timed-versus-required-cleanup bounds and R2 public extracted/replaced TLS-error backing lifetime still need actual stable allocation/deallocator interoperability | Review concrete stable fallible ownership/storage plus linearizable nonallocating reclamation and original-context contract before implementation. Each extracted error child/alias/Weak/leak remains charged to actual free beyond wrapper/connection/config/budget lifetime; allocation-attached enforcement is only a proposal. No proven fatal escape or accepted error change; no nightly, private Arc Layout, unwind, null→abort or ACTIVE baseline workaround |
| U5 integration/provenance | Vendor workspace membership, publisher119-file manifest, fork feature graph/dev deps/offline resolution and Cargo-generated lock not instantiated | Full future allowlist must name permitted manifest/lock integration and preserve other package versions. No vendor/manifests/lock change during docs stage |
| U6 remaining application proof / accounting ABI | FrameSocket/bytes/owner/WAL/decoder/evidence/config/signal paths are not protected by this fork; full application is absent; R3 same-T cross-crate ownership/admission/free bridge and original caller-context mapping are undefined/unaccepted, and §3 is not a complete application accounting ABI | Separate review of exact bridge contract/API/path scope is required before U6 integration/activation, followed by actual composition/admission/lifetime proof. This increment neither designs that bridge nor grants scope; accepted project APIs untouched. Missing components NOT_RUN, no upfront whole-app proof demand, live gate blocked |
| U7 complete call-site audit | Current source map follows reachable families, not compiler-certified exhaustive graph; derived Clone/Codec variants/error/drop/callback feature branches need individual coverage | Implementation packet must attach all source sites/Layouts/capacities, callback envelopes and tests on exact versions. Any new reachable path/API/dependency is a stop; never mark this inventory complete because representative probes pass |

These are concrete blockers to a full implementation scope, not permission
to patch extra files. The requested review may accept parts of the contract
with explicit stops or require changes; it must not label U1–U7 resolved
without evidence. Only a separately accepted closed scope permits the next
implementation increment by this same Integrator in this same Draft branch.

## 9. Migration and maintenance

Fork is opt-in for private capture; ordinary rustls APIs remain identifiable
as unprotected. No default/global-provider installation or magic global
ACTIVE interval is a protected entry point. Upstream version stays exact
0.23.45; archive119-file manifest, edited-file diff hashes and three new-file
hashes bind each implementation head. Keep licenses/notices and disallow
modification outside accepted file lists. Integrator owns fork updates,
manifest/Cargo.lock changes and CI integration sequentially.

An upstream update, security fix, feature/provider/platform change invalidates
affected allocation/callback/Layout/provenance proofs and needs a new explicit
scope review; no silent latest-version fetch or maintenance exception. Re-run
locked preparation before offline checks and regenerate receipts bound to
actual head. Unchanged checks may be cited only with their exact source and
scope. Source-reading design review is not independent implementation QA.

No WAL record/schema/profile/owner identity migration. No capture/replay data
is fabricated for this design. After full protected gate proof, the next
allowed implementation substep remains bounded Text→WAL→two captured
diagnostic replay processes with genuine ACK binding. ACK semantics remain
NotReconstructed, numeric/bootstrap provenance stays synthetic/unverified,
usable_data=false. QA packet belongs to a completed final implementation head.

## 10. Validation matrix and present evidence

All future protected-route cells below are **NOT_RUN**, not marked PASS by
the existing diagnostic probes. Standards/ABI/callback/storage U stops must
be resolved before those tests can justify gate acceptance.

| Family | Required observations / acceptance | Present status |
|---|---|---|
| Bootstrap/control/evidence | First allocation preceded by full T precheck; core/headers/diagnostics/control all included; partial null rollback and last Weak free | NOT_RUN |
| Exact limits/overflow | T/H/every buffer limit−1/equal/+1; checked Layout/add/mul/refcounts, no saturation; refusal counters and underlying alloc calls0 | NOT_RUN |
| Allocation/realloc/null | Fallible alloc success/null, prepaid old+new moving growth and shrink, failure preserves old pointer/cap/content, old free ordering; allocator-null calls1 distinguished | NOT_RUN |
| Aliases/true copies/phase | Config/root/peer-chain sharing versus deep copies, partial chunk, Weak controls, phase transfer, TLS-error public source extraction/replacement/moved children outliving connection/config/budget, leaks and each actual final free; ledger itself remains charged | NOT_RUN |
| Concurrency/recursion | Competing T/H/buffer admissions never overcommit; reserve/consume/free/rollback/refcount races; nonallocating refusal/logging/drop; deadline-safe contention, no recursive allocator lock | NOT_RUN |
| Normal TLS12/13 | Protected config creation→handshake→traffic→retire/free, real cert/name/signature verification, both protocol versions, no resumption/0RTT | NOT_RUN |
| Adversarial TLS | Fragmented large lists/empty DER/malformed DER, nested extensions/OCSP, wrong EKU/OID errors, RSA/ECDSA/Ed25519 chains, tickets/key updates/alerts and unknown messages; no new input/cert-count cap | NOT_RUN |
| Returned outcomes | Over-budget zero underlying alloc/I/O calls before rejected proposal; typed returned ResourceRefused, no quota SIGABRT/panic/unwind; no invented certificate/owner/Close success | NOT_RUN |
| Network/deadlines/owner | R1 all27-method policy/context mapping, original-context T/S checks and nonblocking O/C exceptions; mandatory free/rollback/Drop after stop/expiry without new effects/deadline; refusal before/after actual TCP/TLS read/partial write, original deadlines/stop/quantum; genuine dispatch Unknown, same Close Pending on Err/Drop and only genuine physical cessation completion | NOT_RUN |
| Remaining application | Actual FrameSocket/bytes/config/authority/supervisor/storage/decoder/evidence composition, all backing counted once, baseline included; separate RSS/stack/native/OS disclosure | NOT_RUN |
| Migration/regression | Frozen F1/F2, accepted crates/ADR/spec/WAL byte hashes unchanged; same ownership/TimerA/native-control/ACK diagnostics | Runtime unchanged in this docs stage; future implementation NOT_RUN |
| Build/provenance | Exact publisher/fork source and feature closure, pinned1.98.1, locked online preparation then offline fmt/Clippy/build/tests AND release; decoded exact-head CI and clean checkout | Future implementation NOT_RUN; current docs CI reported in #45/47 after publication |

Prior scoped evidence: receipt6099645626 eight non-abort targeted probes exit0
and publisher source audit; exact9c CI38066971016 three jobs SUCCESS,
578 tests/0failed/ignored including11 expected compile-fail. Those results
apply to unchanged runtime e6 and do not execute this proposed interface.
The standard CI's expected-SIGABRT children remain KILLED/Unknown/incomplete.
Moving descriptor old393216+new786432 is a1,179,648 admission envelope,
not a measured retained-H breach or impossibility of every possible adapter.
Current RSS/stack/native/OS observations remain scoped diagnostic disclosures
from the historical receipt, not production limits or complete decomposition.

## 11. One next step and result format

Architecture/owner reviews **this actual ADR head**, returning one of
DESIGN_CHANGES_REQUIRED or PROPOSED_CONTRACT_ACCEPTED_WITH_EXPLICIT_STOPS,
and separately states FULL_IMPLEMENTATION_SCOPE_NOT_ACCEPTED or a later
fully named accepted implementation scope. It must address exact §3 API,
§2 categories/§4 ledger, C00–C25 and U1–U7, same budgets and preserved
contracts. A statement accepting direction alone does not accept this ADR.

Record source-backed findings/dispositions in existing #45/47, identify
reviewed base/head/tree/diff and actual checks/CI; update only this ADR/
Handoff if docs correction is authorized. Preserve historical receipts.
Then give one next step and a full copy-ready packet. Until concrete ADR
and complete scope acceptance: fork/runtime/dependent activation STOPPED,
production gate FAIL/NOT_PROVEN, capture/WAL/replay/QA NOT_RUN; no merge.

<a id="alloc45-u4-d1"></a>

## 12. ALLOC45-U4-D1 — PROPOSED stable ownership feasibility dossier

**PROPOSED / DESIGN_FEASIBLE_FOR_BOUNDED_EXPERIMENT / SCOPE_DECISION_PENDING.**
This is a design feasibility conclusion for the small, separately proposed
experiment below, not a U4 resolution, implemented mechanism, production gate
or permission to run it. [D2 review6100686192](https://github.com/al-gri/pro-sclpng/pull/47#issuecomment-6100686192)
on input `3a59dc5bccb6ee11891dd98e28c4983df6b89bbe` accepted R1–R3 partial
requirements with explicit stops. §§1–11 remain unchanged and historical;
all U1–U7 remain UNRESOLVED, full implementation scope NOT_ACCEPTED.

### 12.1 Pinned source facts and feasibility boundary

The read-only source target is **Rust1.98.1**, commit
`48a229ceaefd4985c50990b14116b6d856af0985`, Linux
`x86_64-unknown-linux-gnu`/amd64. Official annotated tag1.98.1 resolves through
`18ed059b1465ce6195154de3250a668f1dd3b1fa` to that exact compiler commit in
the existing runner's identity receipt. Cargo remains1.98.1
`797e8a9bca276c1c9f9f738d2a20f484fa4eea9d`. No component was installed:
existing runner has neither rust-src nor rust-docs; exact official source
files were read through GitHub, not substituted with latest/nightly docs.

| ID | Pinned source fact (not the proposed implementation) |
|---|---|
| S1 | [core GlobalAlloc](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/core/src/alloc/global.rs#L90-L113): allocator unwind is undefined behavior; allocation calls can be optimized away. `alloc` may return null; `dealloc` requires its allocation pointer and the same Layout. A null response does not itself make an infallible caller return an error. |
| S2 | [Box constructor](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/boxed.rs#L247-L291) calls Global.allocate and handle_alloc_error on failure. [Box::try_new](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/boxed.rs#L367-L378) is allocator_api/unstable. Its absence on stable cannot be repaired with a Result wrapper around Box::new. |
| S3 | [Arc::new/control](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/sync.rs#L382-L437) allocates a Box of private ArcInner; [Arc::try_new](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/sync.rs#L578-L599) remains unstable. [Internal layout/allocation](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/sync.rs#L2171-L2224) includes control and can invoke handle_alloc_error. No public API supplies that private layout before construction. |
| S4 | [Arc clone](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/sync.rs#L2408-L2430) and [Weak clone](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/sync.rs#L3424-L3446) can abort on reference-count overflow; [Weak upgrade](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/sync.rs#L3280-L3307) uses checked increment/CAS. [Last Weak Drop](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/sync.rs#L3497-L3525), rather than last strong alone, deallocates control backing through its allocator. These are not bounded fallible share APIs. |
| S5 | [Vec ownership contract](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/vec/mod.rs#L54-L67) uses Global and Layout::array of actual capacity; [stable raw ownership transfer](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/vec/mod.rs#L535-L645) requires matching allocator/alignment/capacity/initialization. [try_reserve_exact](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/vec/mod.rs#L1532-L1579) returns TryReserveError; [RawVec growth](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/alloc/src/raw_vec/mod.rs#L503-L568) computes its requested capacity/layout. Capacity arithmetic and allocator failure both require explicit mapping. Ordinary reserve/collect/clone/shrink paths are not thereby protected. |
| S6 | [Stable Layout::extend](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/core/src/alloc/layout.rs#L486-L507) returns combined layout/offset; pad_to_align finalizes it. [std System contract](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/std/src/alloc.rs#L55-L80) forbids mixing incompatible backing allocator interfaces. [Linux System](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/std/src/sys/alloc/unix.rs#L5-L65) uses malloc/posix_memalign/free and may use realloc. This source supplies no finite latency guarantee for System.alloc/free. |
| S7 | [Pinned native TLS macro](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/std/src/sys/thread_local/native/mod.rs#L1-L15) uses a direct native TLS value for const initialization without Drop; [expansion](https://github.com/rust-lang/rust/blob/48a229ceaefd4985c50990b14116b6d856af0985/library/std/src/sys/thread_local/native/mod.rs#L56-L90) distinguishes this from lazy/destructor storage. This permits proposing a const Cell of a raw borrowed plan pointer, not arbitrary thread_local!/thread::current/logging in a hook. Native TLS/OS storage is separately disclosed, not deducted from T. |

Only these focused std sources and four already locked dependency files were
read for this dossier; no repeated publisher/call-site audit or runtime probe.
Source formulas below are symbolic until a separately accepted prototype
reports actual Layouts. No numerical Header, Arc control or allocator usable
size is asserted. Requested physical Rust Layout includes padding; libc
bookkeeping/rounding, RSS, stack, native TLS and OS allocation remain separate
disclosures, not an8MiB process cap.

### 12.2 One recommended mechanism: prepaid storage plus one allocator family

Recommend **one process-wide GlobalAlloc family with universal headers,
explicit fallible preparation and deferred physical reclamation**. Its first
scope is only the isolated experimental integration-test process in §12.6;
no allocator is installed in radar/capture binaries or rustls by this proposal.
A later private capture process allocator would require a separately named
root/path/scope decision. This is not a global provider, allocation ACTIVE
interval or baseline subtraction.

1. A private fallible `try_prepare` receives the existing original deadline/
   stop, allocation class and exact payload Layout `L`. It computes
   `(F, offset) = Layout::new::<Header>().extend(L)?`, then `F.pad_to_align()`.
   Checked transactional admission pays **F.size()** plus any separately
   allocated plan/control storage, using the unchanged §2 memberships.
   Overflow, deadline/stop, quota, refcount limit or AdmissionBusy returns
   scalar evidence **before any System allocation or downstream call**.
2. After valid admission, `try_prepare` calls **System.alloc(F)** directly,
   once for this backing. Null rolls back the reservation and returns the
   existing AllocatorNull meaning without handle_alloc_error, formatting,
   panic/unwind/SIGABRT or network effects. Multi-block preparation frees any
   successful earlier blocks before rolling back their charges. It returns
   a `Prepaid` owning actual storage, not merely quota credits.
3. For one specifically verified infallible leaf, a stack-resident exact
   `CallPlan` borrows those prepaid blocks. A const/no-Drop native TLS Cell
   publishes only its borrowed pointer for the synchronous call. The global
   hook consumes a matching ticket and returns `base.add(offset)` without
   invoking System or admitting more memory. Byte/Vec raw ownership helpers
   first consume their prepaid ticket through stable std::alloc::alloc(L),
   so the resulting pointer is allocated by the selected Global family,
   before Vec::from_raw_parts; they do not adopt a bare System pointer.
   Initial infallible leaf: Box::new of a
   named sealed POD value, whose S2 allocation is Layout::new::<Pod>(); a
   Box-to-trait unsizing coercion retains that same allocation. Fallible
   preparation precedes this infallible call, so preparation null is returned
   by its outer operation. Unconsumed tickets after allocation elision stay
   charged until reclaimed; correctness does not require std to allocate.
4. **An unplanned infallible call is not permitted to execute.** There is no
   lawful general way to turn its later missing ticket/null into the proposed
   returned error. A complete finite source-derived plan, including every
   possible branch/callback request, is required before entering a protected
   leaf. Unknown nested allocations remain U1/U2/U3/U7 stops. Returning null
   from the hook to Box/Arc, catch_unwind, emergency uncharged fallback or
   a guessed reserve pool is not an accepted solution.
5. Every allocation made through this experimental process's global family,
   including unprotected harness allocations, uses the same header/pointer
   convention. Untagged headers select normal System-backed unprotected
   behavior; their existence proves no production category coverage. Tagged
   headers carry immutable class, identity, original physical Layout and an
   independent ledger lifetime reference. Global dealloc receives payload
   pointer `p` and original `L`, recomputes offset/F and identifies its own
   header; it never passes `p` to System.dealloc. Untagged harness deallocation
   uses base/F through System and is not claimed bounded by the tagged core.
   No reading before a foreign
   malloc/System pointer is legal: foreign-family ownership transfers stop.
6. For a tagged block, global dealloc publishes RETIRED into its prepaid
   record with one release atomic store and returns without allocating or
   waiting. The fixture's separately retained CoreReclaimOwner scans a bounded
   number of records and claims retired blocks; only that reclaimer
   calls **System.dealloc(base, F)**, and only after that call returns may
   its charge and ledger backing reference be released. A pending or blocked
   physical free remains charged and prevents completion. Hook pointer/Layout
   agreement, record synchronization and reclaimer safety are experimental
   proof obligations, not accomplished enforcement.

### 12.3 Compatibility matrix

`F(L)` denotes the header-extended/padded physical Layout above. T counts each
physical allocation once; sublimit membership is the unchanged §2 predicate.
The global family observes the actual Layout supplied by std; it does not
manufacture private ArcInner pointers or recover them through guessed offsets.

| Object / compatibility | Allocation entry and effective Layout | Admission / owner, aliases and lifetime | Physical deallocation and lawful result / remaining delta |
|---|---|---|---|
| Custom owned POD/byte storage; **proposed core compatible** | try_prepare uses public Layout::new/array, checked capacity and F(L); inline fields are already paid. | Original context; quota/overflow before System; valid admission+System null returns AllocatorNull. Header and record retain ledger independently of user handle; custom strong/Weak controls have their own known charged Layout. | Retire publishes the record; reclaimer System.dealloc(base,F). Last actual backing free, not wrapper Drop, permits debit. Custom refcount/record proof belongs to U4 experiment. |
| Ordinary Vec/String/Box; **compatible only with explicit prepared creation/operations** | Family-produced Global allocation has L=Layout::array::<T>(actual capacity), byte capacity for String, or Layout::new::<T> for sized Box. Header/alignment/padding are F(L). Stable raw transfers must satisfy S5 and Box's Global ownership contract; UTF-8 validation/move must not create an unplanned copy. | Fallible helper acquires storage first; Vec spare-capacity writes or a verified single Box leaf need no further allocation. Each actual clone/new backing needs a new admission. Ordinary clone/collect/push beyond capacity, shrink-to-fit and formatting are unprotected until their plan is proved. | std Drop calls selected family with p,L, then physical base,F is freed. Passing the prefixed payload to ordinary System/free, using logical length as capacity, or changing allocator family is incompatible. No blanket protection from a Result wrapper or raw transfer alone. |
| Upstream Arc/Weak; **lifetime interception possible, fallible/bounded construction and sharing NOT ESTABLISHED** | Actual L includes private strong/weak control and data per S3; hook can receive L at runtime, but stable Arc::try_new is unavailable. No private Arc ABI or numeric pre-construction layout is assumed. | If origin were lawfully prepaid, allocation-attached charge would survive aliases and last strong until last Weak. The required prior full Layout bound/plan and bounded overflow-safe shares/upgrades are not supplied by this dossier. | Last Weak reaches global family (S4); physical free would release charge. Current proposed wrappers cannot silently replace Arc<T> in upstream trait signatures. A separately reviewed U1/U4 private ownership/API delta or source-complete prepayment/refcount solution is required; no std patch/nightly permission. |
| Direct-return Box/Arc trait objects; **only the named POD Box leaf is a feasible core test** | Concrete Box leaf Layout::new::<Pod>, followed by unsizing, preserves backing; dyn Drop supplies that object's effective Layout. Provider/verifier objects may own additional heap and Arc control. | The POD leaf's storage is prepaid before invocation; nested constructor/clone/destructor/callback closure is not inferred from trait-object size. Upstream direct-return Arc callbacks remain stopped. | Compatible family deallocation for the leaf; general rustls provider/callback proof needs named U1 paths/API plan. No change to certificate verification algorithms or trait semantics is accepted. |
| Publicly extracted rustls::Error children; **General(String) fixture feasible, complete enum closure NOT ESTABLISHED** | Locked rustls0.23.45 src/error.rs:26/36 contains Vec fields,81 General(String),444–490 owned certificate-context data,1048 OtherError(pub Arc<dyn StdError+Send+Sync>). All actual child allocations require their own F(L), including nested strings/OIDs and controls. | Each tagged child retains immutable H/T membership and ledger independently after source extraction/replacement, config/connection/budget Drop, aliases/Weak or leak. No outer error guard can discharge it. An ordinary new clone needs separate preparation. | Child std dealloc reaches the family even after the outer wrapper disappears. Only actual free permits debit. Error::General(String) will exercise this in the proposed fixture; Arc OtherError, all other variants and pki/webpki construction remain U1/U2/U4/U7 stops. Error meaning is unchanged. |

### 12.4 Bootstrap, growth, concurrency and reclamation proposal

**Bootstrap:** before a ledger exists, caller stack scalars check the original
context and reserve the aggregate physical Layout of LedgerCore (including
fixed record array/refcounts), its header and the131,072-byte Diagnostic
storage plus its T-charged metadata. No heap bootstrap object is exempt.
First System null returns Copy/scalar refusal without requiring a ledger or
allocating failure evidence; later partial null frees/debits actual earlier
backings. A successful core imports this reservation exactly once. Its
diagnostic/record storage is owned internally, not circularly counted as
external ledger references. Existing TLS/config/owner/etc must later be
admitted in the actual composition; harness/test baseline cannot exclude them.

**Growth/shrink:** new F(Lnew) is admitted and physically prepaid while old
F(Lold) remains live. Copy/move only after success; null/overflow/quota leaves
old ownership/capacity intact. Old charge persists through retirement to
physical free. Shrink uses the same moving old+new rule, including smaller
but newly allocated headers; no System.realloc in-place shortcut is assumed.
Global realloc for tagged storage must implement this rule through a verified
fallible leaf/plan; an infallible resize is stopped before entry. Zero-sized
values/capacity-zero use std's lawful dangling/nonallocating convention and
do not invent a physical block or free.

**Admission:** one nonallocating try-lock/CAS serializes the entire checked
T+membership+record+ledger-reference transaction. A failed attempt returns
AdmissionBusy before allocation; no unbounded spin or new deadline. Failure
rollback uses the same prepaid record, not an allocating RAII queue. Counters
and record generation change together; reusing a record before physical free
and debit is forbidden. Custom try_share/try_upgrade use checked bounded CAS;
overflow/refusal precedes mutation. Required last-reference decrement and
retirement after stop/expiry use finite atomic operations, not timed admission.
This does not make external std Arc clone/Weak upgrade bounded or fallible.

**Reclamation:** a fixed, prepaid record table avoids a dynamically allocating
queue and unbounded Treiber retry in GlobalAlloc::dealloc. Retired records keep
base/F/class/generation/ledger reference; a bounded scan may fail to claim a
contended record and leave it charged. No record is reused while a producer,
deallocator or reclaimer can access its old generation. After physical free,
a debit-pending record may conservatively retain a charge until a serialized
nonallocating debit completes; pending debit/control is explicit, not a fake
live backing or duplicate allocation. A bootstrap-paid CoreReclaimOwner is
the raw physical ledger custodian held by the existing fixture driver,
independently of public strong/Weak counts. It remains available after user
budget handles disappear; it is neither a second executor nor an app bridge.
Ledger/control may outlive every public
handle. Leaks stay live/charged; last Weak may keep control after payload death.
The final core release occurs only after external references, live/retired
records and pending debits are gone, then Diagnostic/records/core themselves
are physically freed. Bootstrap backings are raw internally owned family
blocks with no external self-reference; final reclamation holds their base/F
and last scalar witness on the caller stack, never accesses a freed ledger,
and never places these raw bootstrap pointers into std ownership. No
self-referential header/core ownership cycle is allowed. The custodian cannot
finish with external references/backings/debits remaining. Premature custodian
Drop retains the raw core and outstanding charges with a nonallocating
orphaned/pending witness; it neither frees live backing nor reports completion.
E1's fixture driver drains and destroys the custodian last. Production custody
and owner binding remain an unaccepted U6 contract.

**Boundedness limit:** native S6 System alloc/free latency is not guaranteed.
The hook's finite retirement work can be tested independently; physical drain
must report retained pending work if it has not returned. Arbitrary T::drop,
foreign allocator callbacks, thread exit and process shutdown are not proved
nonallocating/bounded. The first fixture uses sealed POD and explicitly owned
byte/String drops only. A stalled reclaimer does not imply physical cessation,
Ready, seal or owner completion. Original dispatch/Unknown/pending same Close
and native-control fail-stop remain §6/U6 obligations; this core is not an
application shutdown adapter. No budget refusal goes through panic/unwind/abort.

### 12.5 Named dependencies and residual unknowns

- **U1:** rustls0.23.45 original17/new budget files remain a future proposal.
  At minimum the already named `src/crypto/mod.rs`, `src/crypto/ring/{mod,kx,hash,hmac,tls12,tls13}.rs`,
  `src/crypto/{hash,hmac,tls12,tls13,cipher}.rs`, `src/webpki/{server_verifier,verify}.rs`
  and helpers in §8 still need concrete construction/callback/clone/drop plans
  or a separately proposed private ownership/API delta. The mechanism alone
  does not make direct-return Box/Arc/provider calls fallible.
- **U2:** locked rustls-pki-types1.15.1 `src/lib.rs` owned DER/Bytes conversion
  and `src/server_name.rs` owned names, plus rustls-webpki0.103.15
  `src/verify_cert.rs` EKU/OID growth and `src/error.rs` owned diagnostic data,
  need a separately named fallible/prepaid ownership/error API decision if
  existing calls cannot be source-completely planned. These are candidate
  deltas, not a closed dependency allowlist or changed verification policy.
- **U3:** ring0.17.14 `src/arithmetic/bigint/{boxed_limbs,modulus,modulusvalue}.rs`,
  `src/arithmetic/bigint.rs` and `src/rsa/{verification,public_key,public_modulus}.rs`
  remain the named heap/clone/Montgomery multiplicity/null-plan gap. All current
  algorithms remain; no substitution, removal or Rust-heap-as-native exclusion.
- **U4 residuals:** unsafe family/header/raw-ownership validity; finite plan
  completeness; std Arc construction/layout/refcount solution; generation and
  reclaim races; core final-free/bootstrap; unbounded System/destructor latency;
  foreign allocator/FFI/dynamic-library/thread-exit interoperability. These are not solved by the
  symbolic layout formula, source reading or header sketch.
- **U5/U6/U7:** vendor/features/lock maintenance, exact application ABI bridge/
  original-context mapping/protected composition, and exhaustive allocation
  closure stay unresolved. No bridge is designed here. Missing components
  remain NOT_RUN. Original119 publisher copies/17.rs+manifest/3new files are
  unchanged historical proposal; this dossier neither copies nor authorizes them.

### 12.6 One exact proposed experimental scope and decision request

**Request ALLOC45-U4-CORE-E1, separately PROPOSED, not accepted or started:**
authorize the same Integrator, same Draft branch, to implement only the family,
prepaid POD/byte leaves and custom core strong/Weak/reclamation fixture. This
bounded experiment is feasible from S1/S2/S5/S6/S7 without nightly/private Arc
ABI or dependency patch; its outcome may still reject the production direction.

| Future path | Exact proposed change / copies |
|---|---|
| `apps/radar/tests/support/alloc45_u4_core.rs` | **New**, private experimental FamilyAllocator:GlobalAlloc, Header, LedgerCore with inline16-record fixture table, CoreStrong/CoreWeak/CoreReclaimOwner, CoreClass/CoreFailure, Prepaid/CallPlan/CoreOp, Snapshot/ReclaimReport and sealed Pod. Zero publisher/std copies; no rustls facade or accepted project API. |
| `apps/radar/tests/alloc45_u4_core.rs` | **New**, isolated integration-test binary installs this one #[global_allocator], includes the support module and allocation/null fault fixtures, exercises real pinned rustls::Error::General(String) source extraction in a fixture wrapper. No sockets/TLS/provider/production activation. |
| `apps/radar/Cargo.toml` | **Edit only**: nondefault feature `alloc45-u4-experiment = []` and this [[test]] target with required-features=["alloc45-u4-experiment"]. No dependency/version/MSRV/default-feature changes. Cargo.lock remains identical. |

No other code path, CI/workspace/std/vendor/dependency copy or accepted API edit
is included. Sixteen records is a bounded **experiment fixture**, not a new
production input/certificate-count cap or allocation policy. Production record
capacity/backing bounds and their admission require a later exact Layout proof
inside T; this fixture must not be activated as a production limit.

Exact proposed private API families (signatures belong only to this new scope):

```rust,ignore
fn try_bootstrap(deadline: Instant, stop: &AtomicBool) -> Result<(CoreStrong, CoreReclaimOwner), CoreFailure>;
impl CoreStrong {
    fn try_share(&self, deadline: Instant, stop: &AtomicBool) -> Result<Self, CoreFailure>;
    fn try_downgrade(&self, deadline: Instant, stop: &AtomicBool) -> Result<CoreWeak, CoreFailure>;
}
impl CoreWeak {
    fn try_upgrade(&self, deadline: Instant, stop: &AtomicBool) -> Result<Option<CoreStrong>, CoreFailure>;
}
fn try_prepare(op: &CoreOp<'_>, class: CoreClass, layout: Layout) -> Result<Prepaid, CoreFailure>;
fn try_vec_bytes(op: &CoreOp<'_>, class: CoreClass, capacity: usize) -> Result<Vec<u8>, CoreFailure>;
fn try_string(op: &CoreOp<'_>, class: CoreClass, text: &str) -> Result<String, CoreFailure>;
fn try_prepaid_pod_box(op: &CoreOp<'_>, class: CoreClass, value: Pod) -> Result<Box<Pod>, CoreFailure>;
fn try_resize_bytes(op: &CoreOp<'_>, class: CoreClass, bytes: &mut Vec<u8>, capacity: usize) -> Result<(), CoreFailure>;
fn collect_retired(owner: &mut CoreReclaimOwner, max_steps: usize) -> ReclaimReport;
```

CoreOp borrows the original caller context/core; CoreFailure is fixed scalar
evidence mapping to the unchanged §3 refusal/deadline/stop meanings, not a new
TLS error semantics. Mandatory collect/Drop retirement receives no fresh
deadline and cannot claim successful physical drain until frees/debits finish.
The ordinary GlobalAlloc method signatures remain exactly S1. No general
closure-taking or arbitrary T/Arc conversion API is proposed for E1.

**Meaningful future validation, NOT_RUN now:** bootstrap aggregate admission,
first/later System null and rollback with scalar evidence; quota/overflow/
stop/deadline before allocator and downstream calls (rejected calls=0);
actual Layout/alignment/padded headers including over-aligned Pod; POD Box
ticket consumed or safely unused; compatible std Vec/String/Box final frees;
moving growth AND shrink old+new peaks/refusal leaving old intact; aliases,
custom Weak, leaked backing and real extracted/replaced General(String)
outliving wrapper and user core handle while the charged custodian remains;
premature custodian Drop retains orphaned/pending accounting; exact-once debit;
concurrent admission/share/upgrade/retire/free and generation reuse; bounded
retirement/contended drain after stop/expiry; hook recursion counter=0 and no
logging/allocating TLS/queue/mutex/thread::current path. Instrument the direct
family/preparation boundaries; optimizer-elided std calls cannot be used as
proof that an allocator was invoked or failed. No expected-abort test is PASS.

The exact accepted E1 head would require pinned locked/offline compilation,
targeted `cargo test -p radar --test alloc45_u4_core --features alloc45-u4-experiment --locked --offline`
and applicable fmt/clippy checks, with actual commands/exits/hashes/traces.
Existing default CI does not select this nondefault target: its exact-head
SUCCESS would remain regression only. A CI edit/remote E1 proof route needs a
separate named U5 decision; no silent CI scope expansion. Normal/adversarial
TLS12/13/fragmented certificate/OCSP, complete extracted error enum/Arc paths,
physical application deadlines, owner/WAL/decoder/evidence composition and
capture/replay/independent QA remain unprotected/NOT_RUN in E1.

**One decision requested:** Architecture/owner either accepts this exact
three-path E1 experiment only, with these limits and residual stops, or returns
one named minimal design/scope delta. Acceptance of the recommended mechanism
alone is not experimental permission. Do not authorize dependency expansion,
full fork scope, U6 integration or production gate with that decision.
H1048576/T8388608/three buffers65536each/Diagnostic131072 inside T and every
accepted #45 bound/deadline/TimerA/SessionTurn/Unknown/same original Close/
native-control/ACK NotReconstructed/usable_data=false remain unchanged.
Production gate FAIL/NOT_PROVEN; capture/WAL/replay/independent QA NOT_RUN;
dependent activation STOPPED. No experiment has been executed for this dossier.
