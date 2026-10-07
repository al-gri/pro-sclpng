//! Owner-bound, single-threaded authority for bounded capture sessions.
//!
//! `SessionTurn` is affine: callbacks cannot borrow it while a canonical call
//! holds its mutable borrow. Equal archive identifiers never authenticate a
//! second authority. There is deliberately no reset or second-turn API.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::rc::{Rc, Weak};

use crate::event::{ActiveContext, EventId};
use crate::identity::*;
use crate::policy::{DurabilityMode, RecordingGate, WatermarkKind};
use crate::record::{
    Control, EpochChange, Freshness, Reason, Record, RecordFrame, RecordKind, RecordingHealth,
    Transport, WireContext,
};

pub const MAX_CAPTURE_SCOPES: usize = 4;
pub const MAX_RETAINED_ITEMS: usize = 256;
const MAX_WORK_SHARES: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionBinding {
    pub archive: ArchiveId,
    pub session: CaptureSessionId,
    pub clock: ClockId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrefixBinding {
    pub context: ActiveContext,
    pub recording_gate: RecordingGate,
    pub segment: SegmentNo,
    pub next_record: RecordNo,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeBinding {
    pub stream: StreamId,
    pub connection: ConnectionId,
    pub epoch: ConnectionEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionBudget {
    pub item_cap: usize,
    pub raw_frame_limit: usize,
    pub raw_byte_limit: usize,
    pub max_message_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageMemoryProfile {
    pub metadata_backing_bytes: usize,
    pub metadata_ceiling_bytes: usize,
    pub workspace_backing_bytes: usize,
    pub workspace_ceiling_bytes: usize,
    pub backend_backing_bytes: usize,
    pub backend_ceiling_bytes: usize,
}

impl StorageMemoryProfile {
    /// Check every component and aggregate before accepting a backend report.
    /// Half the addressable numeric range remains reserved for the bounded
    /// authority/supervisor terms; this is arithmetic headroom, not heap usage.
    pub fn validate(&self) -> Result<(), AuthorityError> {
        if self.metadata_backing_bytes > self.metadata_ceiling_bytes
            || self.workspace_backing_bytes > self.workspace_ceiling_bytes
            || self.backend_backing_bytes > self.backend_ceiling_bytes
        {
            return Err(AuthorityError::InvalidBudget);
        }
        let backing = self
            .metadata_backing_bytes
            .checked_add(self.workspace_backing_bytes)
            .and_then(|value| value.checked_add(self.backend_backing_bytes));
        let ceiling = self
            .metadata_ceiling_bytes
            .checked_add(self.workspace_ceiling_bytes)
            .and_then(|value| value.checked_add(self.backend_ceiling_bytes));
        match (backing, ceiling) {
            (Some(backing), Some(ceiling)) if backing <= ceiling && ceiling <= usize::MAX / 2 => {
                Ok(())
            }
            _ => Err(AuthorityError::InvalidBudget),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorityError {
    AuthorityMismatch,
    InvalidBinding,
    InvalidBudget,
    AlreadyRegistered,
    NotRegistered,
    SinkAlreadyBound,
    StorageProfileBound,
    SessionClosing,
    SessionClosed,
    ArchiveFailed,
    StorageStopped,
    WorkExhausted,
    CounterExhausted(&'static str),
    WorkShareExhausted,
    InvalidOwner,
    OwnerRetired,
    CommandRevoked,
    AlreadyClosing,
    WrongLifecycle,
    InvalidTicket,
    TicketConsumed,
    InvalidProof,
    ProofConsumed,
    NotQuiescent,
    PublicationUnavailable,
    CandidateRevoked,
    FenceInsufficient,
    CanonicalBlocked,
    StaleCanonicalStep,
    DataNotUsable,
    RecordingNotHealthy,
    GateConfiguration,
    FenceMissing,
    FenceScopeMismatch,
    FenceBeyondAchieved,
    FenceUntrusted,
    WatermarkRegression,
}

impl fmt::Display for AuthorityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for AuthorityError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionLifecycle {
    Open,
    FailedDiagnostic,
    Closing,
    DiagnosticClosing,
    DiagnosticClosed,
    Finalized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CutSide {
    BeforeFailure,
    PreCut,
    PostCut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiveStamp {
    pub unix_ns: i64,
    pub monotonic_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptIdentity {
    Candidate(CaptureAttemptNo),
    NotRaw,
    NoRepresentableSuccessor { frontier: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureCause {
    QueueOverflow,
    CaptureAttemptExhausted,
    CounterExhausted(&'static str),
    TimeOverflow,
    StorageFailure,
    ReceiptMismatch,
    WeakGate,
    OrderingFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputClass {
    Raw,
    Pong,
    Connected,
    Disconnected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FailureId {
    pub session: CaptureSessionId,
    pub stream: StreamId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TerminalFailure {
    pub stream: StreamId,
    pub connection: ConnectionId,
    pub observed_tag: EpochTag,
    pub current_epoch: ConnectionEpoch,
    pub context: ActiveContext,
    pub stamp: ReceiveStamp,
    pub input_class: InputClass,
    pub attempt: AttemptIdentity,
    pub cause: FailureCause,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionDisposition {
    CaptureEligible(SessionBinding),
    DiagnosticOnly {
        binding: SessionBinding,
        failure: Option<FailureId>,
    },
    Closed(SessionBinding),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeDisposition {
    Active,
    CaptureTerminated(FailureId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarkerState {
    NotRequired,
    Pending,
    Confirmed(RecordNo),
    Unconfirmed(PersistError),
}

/// Immutable original archive-wide failure observation descriptor. It is
/// runtime diagnostic ownership, not a confirmed RecordNo or missing-input GAP.
/// The strictly earlier `through` watermark is resampled only at marker emission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveFailureObservation {
    pub context: WireContext,
    pub reason: Reason,
    pub kind: WatermarkKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionStatus {
    pub binding: SessionBinding,
    pub lifecycle: SessionLifecycle,
    pub failed: bool,
    pub storage_stopped: Option<PersistError>,
    pub first_failure: Option<TerminalFailure>,
    pub archive_observation: Option<ArchiveFailureObservation>,
    pub marker: MarkerState,
    pub cut_sequence: Option<u64>,
    /// First lost record/job stewardship, distinct from a failed received input.
    pub first_abandonment: Option<AbandonedObservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkKind {
    QueuedObservation,
    InFlightObservation,
    PendingPlan,
    Result,
    Command,
    Candidate,
}

/// Fixed identity retained inside the original W cell. It does not claim Raw,
/// GAP, a new CaptureAttempt, or a durable failure marker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationIdentity {
    pub stream: StreamId,
    pub epoch: ConnectionEpoch,
    pub stamp: ReceiveStamp,
    pub class: ObservationClass,
    pub tag: Option<EpochTag>,
    pub attempts: Option<(CaptureAttemptNo, CaptureAttemptNo)>,
    pub loss_count: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationClass {
    Raw,
    RejectedStaleRaw,
    Gap,
    Connected,
    Disconnected,
    Pong,
    Timer { timer_id: u64, deadline_ns: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbandonedObservation {
    pub work_id: u64,
    pub kind: WorkKind,
    pub identity: ObservationIdentity,
    pub cut_side: CutSide,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObservationObligation {
    None,
    Pending,
    Settled,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObligationOrigin {
    Received,
    Generated,
}

struct WorkCell {
    sequence: Cell<u64>,
    close_scope: Cell<Option<StreamId>>,
    references: Cell<usize>,
    kind: Cell<WorkKind>,
    cut_side: Cell<CutSide>,
    obligation: Cell<ObservationObligation>,
    observation: Cell<Option<ObservationIdentity>>,
    confirmed_records: Cell<usize>,
    abandoned_kind: Cell<Option<WorkKind>>,
    obligation_origin: Cell<ObligationOrigin>,
}

impl WorkCell {
    fn is_retained(&self) -> bool {
        self.references.get() > 0
            || matches!(
                self.obligation.get(),
                ObservationObligation::Pending | ObservationObligation::Abandoned
            )
    }

    fn abandon(&self) {
        if self.obligation.get() == ObservationObligation::Pending {
            self.abandoned_kind.set(Some(self.kind.get()));
            self.obligation.set(ObservationObligation::Abandoned);
        }
    }

    fn blocks_marker(&self) -> bool {
        self.is_retained()
            && ((self.obligation_origin.get() == ObligationOrigin::Received
                && matches!(
                    self.obligation.get(),
                    ObservationObligation::Pending | ObservationObligation::Abandoned
                ))
                || (self.obligation.get() == ObservationObligation::None
                    && matches!(
                        self.kind.get(),
                        WorkKind::QueuedObservation | WorkKind::InFlightObservation
                    )))
    }

    fn abandonment(&self) -> Option<AbandonedObservation> {
        (self.obligation.get() == ObservationObligation::Abandoned).then(|| AbandonedObservation {
            work_id: self.sequence.get(),
            kind: self.abandoned_kind.get().unwrap_or(self.kind.get()),
            identity: self
                .observation
                .get()
                .expect("admitted obligation identity"),
            cut_side: self.cut_side.get(),
        })
    }
}

/// A counted logical owner. Sharing is bounded and does not create another W.
/// A queue, result, pending plan and command may transfer this same owner.
pub struct WorkOwner {
    authority: Weak<RefCell<AuthorityState>>,
    cell: Rc<WorkCell>,
    sequence: u64,
}

impl fmt::Debug for WorkOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkOwner")
            .field("id", &self.sequence)
            .field("cut_side", &self.cut_side())
            .finish()
    }
}

impl WorkOwner {
    pub const fn id(&self) -> u64 {
        self.sequence
    }

    pub fn cut_side(&self) -> CutSide {
        self.cell.cut_side.get()
    }

    pub fn share(&self) -> Result<Self, AuthorityError> {
        let count = self.cell.references.get();
        if count >= MAX_WORK_SHARES {
            return Err(AuthorityError::WorkShareExhausted);
        }
        self.cell.references.set(count + 1);
        Ok(Self {
            authority: self.authority.clone(),
            cell: Rc::clone(&self.cell),
            sequence: self.sequence,
        })
    }

    fn share_for_close(&self) -> Result<Self, AuthorityError> {
        let count = self.cell.references.get();
        // One reserved internal alias belongs to the mandatory lease. Public
        // sharing cannot consume it. Invalid callers never cause a panic or
        // mutate Pending ownership when its bounded alias capacity is full.
        if count > MAX_WORK_SHARES {
            return Err(AuthorityError::WorkShareExhausted);
        }
        self.cell.references.set(count + 1);
        Ok(Self {
            authority: self.authority.clone(),
            cell: Rc::clone(&self.cell),
            sequence: self.sequence,
        })
    }
    pub fn set_kind(&self, turn: &mut SessionTurn, kind: WorkKind) -> Result<(), AuthorityError> {
        if !Weak::ptr_eq(&self.authority, &Rc::downgrade(&turn.authority.state)) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        self.cell.kind.set(kind);
        Ok(())
    }

    /// Bounded Cell-only lost-stewardship notification. Lifecycle transitions
    /// and mandatory Close installation wait for a rightful SessionTurn.
    pub fn abandon_observation(&self) {
        self.cell.abandon();
    }
}

impl Drop for WorkOwner {
    fn drop(&mut self) {
        // Cell-only housekeeping is safe even while a canonical callback runs.
        let count = self.cell.references.get();
        self.cell.references.set(count.saturating_sub(1));
        if count <= 1 {
            self.cell.abandon();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseState {
    Pending,
    Leased,
    Settled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseStorage {
    WorkOwner(u64),
    ReservedTerminal,
}

struct CloseCell {
    identity: Cell<Option<CloseIdentity>>,
    state: Cell<CloseState>,
    work: RefCell<Option<WorkOwner>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CloseIdentity {
    stream: StreamId,
    connection: ConnectionId,
    epoch: ConnectionEpoch,
    storage: CloseStorage,
}

/// Opaque epoch/owner identity, not a recyclable slot address.
#[derive(Clone)]
pub struct CloseOwnerRef {
    authority: CaptureSessionAuthority,
    identity: CloseIdentity,
}

impl fmt::Debug for CloseOwnerRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloseOwnerRef")
            .field("stream", &self.identity.stream)
            .field("epoch", &self.identity.epoch)
            .field("storage", &self.identity.storage)
            .finish()
    }
}

impl PartialEq for CloseOwnerRef {
    fn eq(&self, other: &Self) -> bool {
        self.authority.same_authority(&other.authority) && self.identity == other.identity
    }
}

impl Eq for CloseOwnerRef {}

impl CloseOwnerRef {
    pub const fn stream(&self) -> StreamId {
        self.identity.stream
    }

    pub const fn connection(&self) -> ConnectionId {
        self.identity.connection
    }

    pub const fn epoch(&self) -> ConnectionEpoch {
        self.identity.epoch
    }

    pub const fn storage(&self) -> CloseStorage {
        self.identity.storage
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloseOwnerView {
    pub owner: CloseOwnerRef,
    pub state: CloseState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloseOwnerSnapshot {
    pub owners: [Option<CloseOwnerView>; MAX_CAPTURE_SCOPES],
}

impl CloseOwnerSnapshot {
    pub fn iter(&self) -> impl Iterator<Item = &CloseOwnerView> {
        self.owners.iter().flatten()
    }
}

pub struct CloseLease {
    owner: CloseOwnerRef,
    cell: Rc<CloseCell>,
    work: Option<WorkOwner>,
    armed: bool,
}

impl fmt::Debug for CloseLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloseLease")
            .field("owner", &self.owner)
            .finish()
    }
}

impl CloseLease {
    pub fn owner(&self) -> &CloseOwnerRef {
        &self.owner
    }
    pub fn work_owner_id(&self) -> Option<u64> {
        self.work.as_ref().map(WorkOwner::id)
    }

    pub fn into_command(self) -> CommandLease {
        CommandLease {
            authority: self.owner.authority.clone(),
            stream: self.owner.identity.stream,
            connection: self.owner.identity.connection,
            epoch: self.owner.identity.epoch,
            kind: CommandKind::Close,
            work: None,
            close: Some(self),
        }
    }
}

impl Drop for CloseLease {
    fn drop(&mut self) {
        if self.armed
            && self.cell.identity.get() == Some(self.owner.identity)
            && self.cell.state.get() == CloseState::Leased
        {
            self.cell.state.set(CloseState::Pending);
        }
    }
}

#[derive(Debug)]
pub enum CloseLeaseReport {
    Leased(CloseLease),
    AlreadyLeased,
    AlreadySettled,
    Rejected(AuthorityError),
}

pub struct AuthenticatedClosure {
    authority: CaptureSessionAuthority,
    connection: ConnectionId,
    epoch: ConnectionEpoch,
}

// No public DTO constructor: a future trusted transport-owner producer must
// mint this capability. Successful canonical dispatch already settles Close.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseSettlementReport {
    Settled,
    AlreadySettled,
    Rejected(AuthorityError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandKind {
    Connect { endpoint: &'static str },
    SendText { text: String },
    Close,
    ReconnectAfter { delay_ns: u64 },
}

/// An affine command. Only the owning authority can synchronously dispatch it.
pub struct CommandLease {
    authority: CaptureSessionAuthority,
    stream: StreamId,
    connection: ConnectionId,
    epoch: ConnectionEpoch,
    kind: CommandKind,
    work: Option<WorkOwner>,
    close: Option<CloseLease>,
}

impl fmt::Debug for CommandLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandLease")
            .field("stream", &self.stream)
            .field("connection", &self.connection)
            .field("epoch", &self.epoch)
            .field("kind", &self.kind)
            .finish()
    }
}

impl CommandLease {
    pub const fn stream(&self) -> StreamId {
        self.stream
    }
    pub const fn connection(&self) -> ConnectionId {
        self.connection
    }
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }
    pub fn kind(&self) -> &CommandKind {
        &self.kind
    }
    pub fn work_owner_id(&self) -> Option<u64> {
        self.work.as_ref().map(WorkOwner::id)
    }
    pub fn view(&self) -> CommandView<'_> {
        CommandView {
            stream: self.stream,
            connection: self.connection,
            epoch: self.epoch,
            kind: &self.kind,
        }
    }
    pub fn close_owner(&self) -> Option<&CloseOwnerRef> {
        self.close.as_ref().map(CloseLease::owner)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandView<'a> {
    pub stream: StreamId,
    pub connection: ConnectionId,
    pub epoch: ConnectionEpoch,
    pub kind: &'a CommandKind,
}

#[derive(Debug)]
pub enum DispatchReport<E> {
    Dispatched,
    AlreadySettled,
    DispatchFailed {
        error: E,
        effect: AmbiguousEffect,
    },
    Denied {
        reason: AuthorityError,
        command: CommandLease,
    },
    Revoked(AuthorityError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AmbiguousEffect {
    Unknown,
}

pub struct TerminationReport {
    pub failure_id: FailureId,
    pub first: bool,
    pub close_owner: CloseOwnerRef,
    pub close: Option<CloseLease>,
}

impl fmt::Debug for TerminationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TerminationReport")
            .field("failure_id", &self.failure_id)
            .field("first", &self.first)
            .field("close_owner", &self.close_owner)
            .field("close", &self.close)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistErrorKind {
    Adapter,
    Io,
    Validation,
    ReceiptMismatch,
    WeakGate,
    Authority,
    Counter,
    /// Lost admitted/job stewardship; this is a logical stop, not an I/O error.
    OwnershipAbandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistError {
    pub kind: PersistErrorKind,
    pub detail: &'static str,
}

impl PersistError {
    pub const fn typed(kind: PersistErrorKind, detail: &'static str) -> Self {
        Self { kind, detail }
    }
    pub fn new(_detail: impl AsRef<str>) -> Self {
        Self::typed(PersistErrorKind::Adapter, "persistence adapter error")
    }
}

impl fmt::Display for PersistError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.detail)
    }
}
impl std::error::Error for PersistError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistenceReceipt {
    pub through: RecordNo,
    pub achieved_gate: RecordingGate,
}

/// Trusted backend contract used by the standalone pure authority harness.
/// A successful receipt MUST describe the actual accepted contiguous prefix
/// and achieved physical gate, not a caller assertion or synthetic success.
/// The bound wrapper checks identity, sequence and semantic gate strength;
/// it cannot authenticate an arbitrary trait implementation's physical I/O.
/// Filesystem capture uses recording's exclusive `CaptureSessionOwner` minted
/// handle/sink. Constructing a separate authority never authenticates that
/// owner, even when identifier values match.
pub trait SessionRecordWriter {
    fn persist(
        &mut self,
        frame: &RecordFrame,
        required_gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistError>;

    fn memory_profile(&self) -> StorageMemoryProfile {
        StorageMemoryProfile::default()
    }

    fn checked_memory_profile(&self) -> Result<StorageMemoryProfile, AuthorityError> {
        let profile = self.memory_profile();
        profile.validate()?;
        Ok(profile)
    }
}

pub struct BoundRecordSink {
    authority: CaptureSessionAuthority,
    writer: Box<dyn SessionRecordWriter>,
}

/// Opaque authority retained privately by the backend owner that first bound
/// the sink. A generic session authority or mutation turn cannot mint another
/// token or update the concrete backend's accepted memory profile.
pub struct StorageMemoryAuthority {
    authority: CaptureSessionAuthority,
}

impl StorageMemoryAuthority {
    pub fn update(
        &mut self,
        turn: &mut SessionTurn,
        profile: StorageMemoryProfile,
    ) -> Result<(), AuthorityError> {
        self.authority.update_backend_memory_profile(turn, profile)
    }
}

impl fmt::Debug for BoundRecordSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundRecordSink").finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistBoundaryError {
    Authority(AuthorityError),
    Persistence(PersistError),
    ReceiptMismatch {
        expected: RecordNo,
        actual: RecordNo,
    },
    WeakGate {
        required: RecordingGate,
        achieved: RecordingGate,
    },
}

impl BoundRecordSink {
    pub fn binding(&self) -> SessionBinding {
        self.authority.binding()
    }
    pub fn authority(&self) -> &CaptureSessionAuthority {
        &self.authority
    }
    /// Compatibility entry accepts only the reserved failure marker.
    pub fn persist(
        &mut self,
        turn: &mut SessionTurn,
        frame: &RecordFrame,
        gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistBoundaryError> {
        self.persist_marker(turn, frame, gate)
    }

    pub fn persist_marker(
        &mut self,
        turn: &mut SessionTurn,
        frame: &RecordFrame,
        gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistBoundaryError> {
        if !matches!(&frame.value,Record::Control(record) if matches!(&record.value,Control::Recording(evidence) if evidence.health==RecordingHealth::Failed))
        {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidOwner,
            ));
        }
        self.persist_checked(turn, frame, gate, None)
    }

    pub fn persist_owned(
        &mut self,
        turn: &mut SessionTurn,
        frame: &RecordFrame,
        gate: RecordingGate,
        owner: &WorkOwner,
    ) -> Result<PersistenceReceipt, PersistBoundaryError> {
        self.authority
            .synchronize_obligations(turn)
            .map_err(PersistBoundaryError::Authority)?;
        if matches!(&frame.value,Record::Control(record) if matches!(&record.value,Control::Recording(evidence) if evidence.health==RecordingHealth::Failed))
        {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidOwner,
            ));
        }
        if !Weak::ptr_eq(&owner.authority, &Rc::downgrade(&self.authority.state)) {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::AuthorityMismatch,
            ));
        }
        if owner.cell.sequence.get() != owner.sequence
            || owner.cell.references.get() == 0
            || !matches!(
                owner.cell.kind.get(),
                WorkKind::InFlightObservation | WorkKind::PendingPlan
            )
        {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidOwner,
            ));
        }
        let state = self.authority.state.borrow();
        let generated = owner.cell.obligation_origin.get() == ObligationOrigin::Generated
            || (owner.cell.obligation.get() == ObservationObligation::None
                && owner.cell.kind.get() == WorkKind::PendingPlan);
        let side = if generated && state.failed {
            CutSide::PostCut
        } else {
            owner.cut_side()
        };
        if state.failed {
            if side == CutSide::PostCut && !matches!(state.marker, MarkerState::Confirmed(_))
                || side == CutSide::PreCut && matches!(state.marker, MarkerState::Confirmed(_))
            {
                return Err(PersistBoundaryError::Authority(
                    AuthorityError::InvalidBinding,
                ));
            }
            let stream = match &frame.value {
                Record::RawInput(raw) => Some(raw.stream),
                _ => None,
            };
            if side == CutSide::PostCut
                && stream.is_some_and(|stream| {
                    state
                        .scopes
                        .iter()
                        .any(|scope| scope.binding.stream == stream && scope.failure.is_some())
                })
            {
                return Err(PersistBoundaryError::Authority(
                    AuthorityError::CommandRevoked,
                ));
            }
        }
        drop(state);
        self.persist_checked(turn, frame, gate, Some(owner))
    }

    fn raise_watermarks(watermarks: &mut [Option<RecordNo>; 5], receipt: PersistenceReceipt) {
        for (index, watermark) in watermarks.iter_mut().enumerate() {
            let covered = match index {
                0..=2 => true,
                3 => receipt.achieved_gate.covers(RecordingGate::Flushed),
                _ => receipt.achieved_gate.covers(RecordingGate::Durable),
            };
            if covered {
                *watermark =
                    Some(watermark.map_or(receipt.through, |old| old.max(receipt.through)));
            }
        }
    }
    fn persist_checked(
        &mut self,
        turn: &mut SessionTurn,
        frame: &RecordFrame,
        gate: RecordingGate,
        owner: Option<&WorkOwner>,
    ) -> Result<PersistenceReceipt, PersistBoundaryError> {
        self.authority
            .synchronize_obligations(turn)
            .map_err(PersistBoundaryError::Authority)?;
        self.authority
            .ensure_storage_writable()
            .map_err(PersistBoundaryError::Authority)?;
        // A backend report must be representable before any write boundary.
        // Rejection neither consumes prefix identity nor replaces the prior
        // accepted profile, even for a trusted conformance backend override.
        self.writer
            .checked_memory_profile()
            .and_then(|profile| profile.validate())
            .map_err(PersistBoundaryError::Authority)?;
        frame
            .validate_shape()
            .map_err(|_| PersistBoundaryError::Authority(AuthorityError::InvalidBinding))?;
        let prefix = self
            .authority
            .prefix()
            .map_err(PersistBoundaryError::Authority)?;
        if gate != prefix.recording_gate
            || frame.segment_no != prefix.segment
            || frame.record_no != prefix.next_record
        {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBinding,
            ));
        }
        if !matches!(
            frame.value,
            Record::RawInput(_) | Record::Control(_) | Record::Gap(_)
        ) {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBinding,
            ));
        }
        if let Record::RawInput(raw) = &frame.value {
            let budget = self
                .authority
                .state
                .borrow()
                .budget
                .expect("registered sink");
            if raw.bytes.len() > budget.max_message_bytes {
                return Err(PersistBoundaryError::Authority(
                    AuthorityError::InvalidBudget,
                ));
            }
        }
        if let Record::Control(record) = &frame.value
            && matches!(&record.value,Control::Recording(evidence) if evidence.health==RecordingHealth::Failed)
        {
            let mut state = self.authority.state.borrow_mut();
            if let Control::Recording(evidence) = &record.value {
                let index = match evidence.kind {
                    WatermarkKind::Accepted => 0,
                    WatermarkKind::Appended => 1,
                    WatermarkKind::Written => 2,
                    WatermarkKind::Flushed => 3,
                    WatermarkKind::Durable => 4,
                };
                let known = state.trusted_watermarks[index];
                if evidence.through != known
                    || evidence
                        .through
                        .is_some_and(|through| through >= frame.record_no)
                {
                    return Err(PersistBoundaryError::Authority(
                        AuthorityError::InvalidBinding,
                    ));
                }
            }
            let Control::Recording(evidence) = &record.value else {
                unreachable!("matched recording control")
            };
            let descriptor = ArchiveFailureObservation {
                context: record.context,
                reason: evidence.reason,
                kind: evidence.kind,
            };
            if state
                .archive_observation
                .is_some_and(|original| original != descriptor)
                || matches!(state.marker, MarkerState::Confirmed(_))
            {
                return Err(PersistBoundaryError::Authority(
                    AuthorityError::InvalidBinding,
                ));
            }
            if state.archive_observation.is_none() {
                state.archive_observation = Some(descriptor);
            }
            CaptureSessionAuthority::latch(&mut state);
            if state
                .work
                .iter()
                .any(|cell| cell.blocks_marker() && cell.cut_side.get() == CutSide::PreCut)
            {
                return Err(PersistBoundaryError::Authority(
                    AuthorityError::NotQuiescent,
                ));
            }
        }
        let next = frame.record_no.checked_next().map_err(|_| {
            let _ = self.authority.storage_stopped(
                turn,
                PersistError::typed(PersistErrorKind::Counter, "RecordNo exhausted"),
            );
            PersistBoundaryError::Authority(AuthorityError::CounterExhausted("RecordNo"))
        })?;
        let confirmed_records = owner
            .map(|owner| {
                owner.cell.confirmed_records.get().checked_add(1).ok_or(
                    PersistBoundaryError::Authority(AuthorityError::CounterExhausted(
                        "WorkReceipt",
                    )),
                )
            })
            .transpose()?;
        let receipt = self.writer.persist(frame, gate).map_err(|error| {
            if let Ok(profile) = self.writer.checked_memory_profile() {
                let _ = self.authority.update_backend_memory_profile(turn, profile);
            }
            let _ = self.authority.storage_stopped(turn, error);
            PersistBoundaryError::Persistence(error)
        })?;
        let profile = self
            .writer
            .checked_memory_profile()
            .and_then(|profile| {
                profile.validate()?;
                Ok(profile)
            })
            .map_err(|error| {
                // A backend can report an invalid change only after real
                // bytes may have been written. Preserve the accepted profile,
                // stop further writes, and retain that ambiguous suffix.
                let _ = self.authority.storage_stopped(
                    turn,
                    PersistError::typed(PersistErrorKind::Validation, "backend memory profile"),
                );
                PersistBoundaryError::Authority(error)
            })?;
        self.authority
            .update_backend_memory_profile(turn, profile)
            .map_err(PersistBoundaryError::Authority)?;
        if receipt.through != frame.record_no {
            let _ = self.authority.storage_stopped(
                turn,
                PersistError::typed(
                    PersistErrorKind::ReceiptMismatch,
                    "receipt RecordNo mismatch",
                ),
            );
            return Err(PersistBoundaryError::ReceiptMismatch {
                expected: frame.record_no,
                actual: receipt.through,
            });
        }
        if !receipt.achieved_gate.covers(gate) {
            let _ = self.authority.storage_stopped(
                turn,
                PersistError::typed(PersistErrorKind::WeakGate, "receipt gate too weak"),
            );
            return Err(PersistBoundaryError::WeakGate {
                required: gate,
                achieved: receipt.achieved_gate,
            });
        }
        self.authority
            .state
            .borrow_mut()
            .prefix
            .as_mut()
            .expect("registered sink")
            .next_record = next;
        Self::raise_watermarks(
            &mut self.authority.state.borrow_mut().trusted_watermarks,
            receipt,
        );
        self.authority.confirm_persisted_frame(frame);
        if let (Some(owner), Some(confirmed_records)) = (owner, confirmed_records) {
            owner.cell.confirmed_records.set(confirmed_records);
        }
        Ok(receipt)
    }
}

struct ScopeState {
    binding: ScopeBinding,
    failure: Option<TerminalFailure>,
    close: Rc<CloseCell>,
    confirmed_up: Option<ConnectionEpoch>,
    confirmed_down: Option<ConnectionEpoch>,
    confirmed_timer: Option<RecordNo>,
    confirmed_advance: Option<(ConnectionEpoch, ConnectionEpoch, RecordNo)>,
    confirmed_subscription: Option<(SubscriptionEpoch, SubscriptionEpoch, RecordNo)>,
    confirmed_book: Option<(BookEpoch, BookEpoch, RecordNo)>,
    current_tag: Option<EpochTag>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TicketState {
    Absent,
    Active,
    Consumed,
    Invalidated,
}

struct AuthorityState {
    binding: SessionBinding,
    lifecycle: SessionLifecycle,
    failed: bool,
    storage_stopped: Option<PersistError>,
    first_failure: Option<TerminalFailure>,
    first_abandonment: Option<AbandonedObservation>,
    archive_observation: Option<ArchiveFailureObservation>,
    marker: MarkerState,
    cut_sequence: Option<u64>,
    sequence: u64,
    scopes: Vec<ScopeState>,
    work: Vec<Rc<WorkCell>>,
    budget: Option<RetentionBudget>,
    prefix: Option<PrefixBinding>,
    registered: bool,
    sink_bound: bool,
    ticket: TicketState,
    proof_issued: bool,
    proof_consumed: bool,
    finalization_authorized: bool,
    guard: Option<OwnerBoundPublicationGuard>,
    storage_memory: StorageMemoryProfile,
    confirmed_marker: Option<RecordNo>,
    accepted_bindings: Vec<StreamBinding>,
    trusted_watermarks: [Option<RecordNo>; 5],
}

#[derive(Clone)]
pub struct CaptureSessionAuthority {
    state: Rc<RefCell<AuthorityState>>,
}

impl fmt::Debug for CaptureSessionAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.status().fmt(f)
    }
}

/// The sole mutation turn cannot be cloned or captured by a commit callback.
///
/// ```compile_fail
/// use domain::capture_session::SessionTurn;
/// fn duplicate(turn: &SessionTurn) -> SessionTurn { turn.clone() }
/// ```
///
/// ```compile_fail
/// use domain::capture_session::{CaptureSessionAuthority, CommandLease, SessionTurn};
/// fn reenter(authority: &CaptureSessionAuthority, turn: &mut SessionTurn, command: CommandLease) {
///     authority.dispatch(turn, command, |_| {
///         authority.begin_finalization(turn).map(|_| ())
///     });
/// }
/// ```
pub struct SessionTurn {
    authority: CaptureSessionAuthority,
}

impl fmt::Debug for SessionTurn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionTurn").finish_non_exhaustive()
    }
}

pub struct SupervisorSessionHandle {
    authority: CaptureSessionAuthority,
}

impl fmt::Debug for SupervisorSessionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupervisorSessionHandle")
            .field("binding", &self.authority.binding())
            .finish()
    }
}

impl SupervisorSessionHandle {
    pub fn authority(&self) -> &CaptureSessionAuthority {
        &self.authority
    }
    pub fn archive_failure_observation(&self) -> Option<ArchiveFailureObservation> {
        self.authority.archive_failure_observation()
    }
    pub fn prefix(&self) -> PrefixBinding {
        self.authority.prefix().expect("registered handle")
    }
    pub fn trusted_watermark(&self, kind: WatermarkKind) -> Option<RecordNo> {
        self.authority.trusted_watermark(kind)
    }
    pub fn scopes(&self) -> [Option<ScopeBinding>; MAX_CAPTURE_SCOPES] {
        self.authority.scopes()
    }
    pub fn validate_stream_bindings(
        &self,
        bindings: &[StreamBinding],
    ) -> Result<(), AuthorityError> {
        let state = self.authority.state.borrow();
        if state.accepted_bindings != bindings {
            return Err(AuthorityError::InvalidBinding);
        }
        Ok(())
    }
    pub fn budget(&self) -> RetentionBudget {
        self.authority
            .state
            .borrow()
            .budget
            .expect("registered handle")
    }
    pub fn reserve_work(
        &self,
        turn: &mut SessionTurn,
        kind: WorkKind,
    ) -> Result<WorkOwner, AuthorityError> {
        self.authority.reserve_work(turn, kind)
    }
    /// Only this non-clonable registered supervisor handle can admit and
    /// complete its jobs; generic authority kind changes cannot settle them.
    pub fn admit_observation(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
        identity: ObservationIdentity,
    ) -> Result<(), AuthorityError> {
        self.authority.synchronize_obligations(turn)?;
        self.authority.ensure_admission_open(turn)?;
        self.validate_work(owner)?;
        let state = self.authority.state.borrow();
        if owner.cell.obligation.get() != ObservationObligation::None
            || owner.cell.kind.get() != WorkKind::QueuedObservation
            || !state
                .scopes
                .iter()
                .any(|scope| scope.binding.stream == identity.stream)
        {
            return Err(AuthorityError::InvalidOwner);
        }
        owner.cell.observation.set(Some(identity));
        owner.cell.obligation.set(ObservationObligation::Pending);
        owner.cell.obligation_origin.set(ObligationOrigin::Received);
        Ok(())
    }

    pub fn extend_gap_observation(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
        identity: ObservationIdentity,
    ) -> Result<(), AuthorityError> {
        self.authority.synchronize_obligations(turn)?;
        self.validate_work(owner)?;
        let old = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        let state = self.authority.state.borrow();
        if owner.cell.obligation.get() != ObservationObligation::Pending
            || old.class != ObservationClass::Gap
            || old.stream != identity.stream
            || old.epoch != identity.epoch
            || old.stamp != identity.stamp
            || old.tag != identity.tag
            || identity.class != ObservationClass::Gap
            || old.attempts.map(|range| range.0) != identity.attempts.map(|range| range.0)
            || old
                .attempts
                .zip(identity.attempts)
                .is_none_or(|(old, new)| new.1 < old.1)
            || old
                .loss_count
                .zip(identity.loss_count)
                .is_none_or(|(old, new)| new < old)
            || (state.failed && owner.cut_side() != CutSide::PostCut)
        {
            return Err(AuthorityError::InvalidOwner);
        }
        owner.cell.observation.set(Some(identity));
        Ok(())
    }

    fn validate_work(&self, owner: &WorkOwner) -> Result<(), AuthorityError> {
        if !Weak::ptr_eq(&owner.authority, &Rc::downgrade(&self.authority.state)) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        if owner.cell.sequence.get() != owner.sequence || owner.cell.references.get() == 0 {
            return Err(AuthorityError::OwnerRetired);
        }
        Ok(())
    }

    /// Complete the canonical job after all its required writes/dispositions.
    /// Receipts are authenticated by the bound sink. A no-write obsolete Up or
    /// Pong is accepted only after actual same-epoch Down was gate-confirmed.
    pub fn complete_observation(
        &self,
        turn: &mut SessionTurn,
        sink: &BoundRecordSink,
        owner: &WorkOwner,
        obsolete: Option<(StreamId, ConnectionEpoch)>,
    ) -> Result<(), AuthorityError> {
        self.authority.synchronize_obligations(turn)?;
        self.validate_work(owner)?;
        if !self.authority.same_authority(&sink.authority) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        if owner.cell.obligation.get() != ObservationObligation::Pending {
            return Err(AuthorityError::InvalidOwner);
        }
        self.authority.ensure_storage_writable()?;
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        let required = if owner.cell.obligation_origin.get() == ObligationOrigin::Generated {
            3
        } else if identity.class == ObservationClass::RejectedStaleRaw {
            2
        } else {
            1
        };
        if owner.cell.confirmed_records.get() == 0 {
            if !matches!(
                identity.class,
                ObservationClass::Connected | ObservationClass::Pong
            ) || obsolete != Some((identity.stream, identity.epoch))
                || !self.authority.state.borrow().scopes.iter().any(|scope| {
                    scope.binding.stream == identity.stream
                        && scope.confirmed_down == Some(identity.epoch)
                })
            {
                return Err(AuthorityError::NotQuiescent);
            }
        } else if owner.cell.confirmed_records.get() < required {
            return Err(AuthorityError::NotQuiescent);
        }
        owner.cell.obligation.set(ObservationObligation::Settled);
        Ok(())
    }

    pub fn retain_generated_plan(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
    ) -> Result<(), AuthorityError> {
        self.authority.synchronize_obligations(turn)?;
        self.validate_work(owner)?;
        if owner.cell.obligation.get() != ObservationObligation::Settled
            || owner.cell.kind.get() != WorkKind::PendingPlan
        {
            return Err(AuthorityError::InvalidOwner);
        }
        owner.cell.obligation.set(ObservationObligation::Pending);
        owner
            .cell
            .obligation_origin
            .set(ObligationOrigin::Generated);
        owner.cell.confirmed_records.set(0);
        Ok(())
    }

    /// Explicit bounded cancellation of generated output with no fresh confirmed
    /// records and no storage stop. The earlier Down receipt is not fresh plan
    /// progress: `retain_generated_plan` resets its counter. Received observations
    /// cannot use this path, and ordinary Drop cannot substitute for this
    /// serialized lifecycle decision.
    pub fn cancel_generated_plan(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
    ) -> Result<(), AuthorityError> {
        self.authority.validate_turn(turn)?;
        self.validate_work(owner)?;
        self.authority.synchronize_obligations(turn)?;
        let state = self.authority.state.borrow();
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        if owner.cell.kind.get() != WorkKind::PendingPlan
            || owner.cell.obligation_origin.get() != ObligationOrigin::Generated
            || owner.cell.obligation.get() != ObservationObligation::Pending
        {
            return Err(AuthorityError::InvalidOwner);
        }
        if state.storage_stopped.is_some() {
            return Err(AuthorityError::StorageStopped);
        }
        if owner.cell.confirmed_records.get() != 0 {
            return Err(AuthorityError::NotQuiescent);
        }
        if !(matches!(
            state.lifecycle,
            SessionLifecycle::DiagnosticClosing | SessionLifecycle::DiagnosticClosed
        ) || state
            .scopes
            .iter()
            .any(|scope| scope.binding.stream == identity.stream && scope.failure.is_some()))
        {
            return Err(AuthorityError::InvalidOwner);
        }
        owner.cell.obligation.set(ObservationObligation::Settled);
        Ok(())
    }
    pub fn terminate(
        &self,
        turn: &mut SessionTurn,
        failure: TerminalFailure,
    ) -> Result<TerminationReport, AuthorityError> {
        self.authority.terminate(turn, failure)
    }
    pub fn mandatory_close(
        &self,
        turn: &mut SessionTurn,
        stream: StreamId,
        epoch: ConnectionEpoch,
        work: Option<&WorkOwner>,
    ) -> Result<CloseOwnerRef, AuthorityError> {
        self.authority.mandatory_close(turn, stream, epoch, work)
    }
    pub fn command(
        &self,
        turn: &mut SessionTurn,
        stream: StreamId,
        epoch: ConnectionEpoch,
        kind: CommandKind,
        work: &WorkOwner,
    ) -> Result<CommandLease, AuthorityError> {
        self.authority.command(turn, stream, epoch, kind, work)
    }
    pub fn quiesce(&self, turn: &mut SessionTurn, ticket: &CloseTicket) -> QuiescenceReport {
        self.authority.quiesce(turn, ticket)
    }
}

pub struct CloseTicket {
    authority: CaptureSessionAuthority,
}
impl fmt::Debug for CloseTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloseTicket").finish_non_exhaustive()
    }
}
/// Finalization authorization is affine. A validated consumption spends it;
/// a foreign owner or turn rejection leaves the same value available.
///
/// ```compile_fail
/// use domain::capture_session::QuiescenceProof;
/// fn duplicate(proof: QuiescenceProof) {
///     let _first = proof;
///     let _second = proof;
/// }
/// ```
///
/// ```compile_fail
/// use domain::capture_session::{CaptureSessionAuthority, QuiescenceProof, SessionTurn};
/// fn overlap(authority: &CaptureSessionAuthority, turn: &mut SessionTurn,
///            proof: &mut QuiescenceProof) {
///     let first = &mut *proof;
///     let second = &mut *proof;
///     authority.consume_proof(turn, first).unwrap();
///     authority.consume_proof(turn, second).unwrap();
/// }
/// ```
pub struct QuiescenceProof {
    authority: CaptureSessionAuthority,
}
impl fmt::Debug for QuiescenceProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuiescenceProof").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnsettledSummary {
    pub queued: usize,
    pub in_flight: usize,
    pub pending_plans: usize,
    pub results: usize,
    pub commands: usize,
    pub candidates: usize,
    pub work_total: usize,
    pub abandoned: usize,
    pub marker: MarkerState,
    pub close_owners: CloseOwnerSnapshot,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Fixed <=4-scope snapshot avoids an unadvertised heap owner.
pub enum QuiescenceReport {
    NotReady(UnsettledSummary),
    Ready(QuiescenceProof),
    TicketConsumed,
    FinalizationInvalidated(AuthorityError),
    Rejected(AuthorityError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnershipReport {
    pub item_cap: usize,
    pub reserved_scopes: usize,
    pub reserved_archive: usize,
    pub work_limit: usize,
    pub work_used: usize,
    pub pre_cut: usize,
    pub post_cut: usize,
    pub before_failure: usize,
    pub work_references: usize,
    /// Admitted/generated obligations independent of live Rust aliases.
    pub pending_observations: usize,
    pub abandoned_work: usize,
    /// Known authority-owned allocation capacity: Rc/RefCell storage, fixed
    /// registries, vector backing and bounded immutable token storage. This
    /// excludes caller-held inline capability values and is not an allocator
    /// instrumentation measurement.
    pub metadata_backing_bytes: usize,
    /// Conservative charge for currently retained affine aliases. This is an
    /// explicit fixed-layout accounting model, not measured backing bytes.
    pub inline_accounted_capacity_bytes: usize,
    /// Full fixed alias/command capacity permitted by this profile.
    pub inline_ceiling_bytes: usize,
    /// Known allocation capacity plus the full inline/command ceiling.
    pub metadata_ceiling_bytes: usize,
    pub storage_memory: StorageMemoryProfile,
}

// Opaque capabilities have no public constructor. The trusted canonical
// producer is deliberately absent; these private value types model the full
// accepted relation for pure conformance checks, never a caller DTO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CanonicalScope {
    binding: SessionBinding,
    stream: StreamId,
    slot: InstrumentSlot,
    tag: EpochTag,
    context: ActiveContext,
    profile: FeedProfileVersion,
    barrier: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CandidateIdentity {
    archive: ArchiveId,
    creation_record: RecordNo,
    stream: StreamId,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CanonicalProjection {
    scope: CanonicalScope,
    gate: RecordingGate,
    anchor: RecordNo,
    witness_record: RecordNo,
    effects: [Option<EventId>; 4],
    freshness: Freshness,
    observed_recording: RecordingHealth,
    last_receipt: RecordNo,
    evaluation_ns: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CandidateState {
    identity: CandidateIdentity,
    causal_frontier: RecordNo,
    available_at: RecordNo,
    projection: CanonicalProjection,
    revoked: bool,
}
#[allow(dead_code)] // Constructed only by the not-yet-wired canonical producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CanonicalPhase {
    Running,
    Blocked,
}
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BookUsability {
    Unknown,
    Usable,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VerificationWitness {
    scope: CanonicalScope,
    anchor: RecordNo,
    record: RecordNo,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QuietWitness {
    scope: CanonicalScope,
    expires_ns: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CanonicalGuardState {
    scope: CanonicalScope,
    phase: CanonicalPhase,
    transport: Transport,
    book: BookUsability,
    anchor: Option<RecordNo>,
    witness: Option<VerificationWitness>,
    freshness: Freshness,
    allow_quiet_with_proof: bool,
    quiet: Option<QuietWitness>,
    evaluation_ns: u64,
    observed_recording: RecordingHealth,
    mode: DurabilityMode,
    recording_gate: RecordingGate,
    current: Option<CandidateState>,
}
/// An authenticated canonical recorded step cannot be minted from a public
/// DTO, boolean or parsed artifact. No production producer is wired yet.
pub struct CanonicalRecordedStep {
    authority: CaptureSessionAuthority,
    recorded_at: RecordNo,
    state: CanonicalGuardState,
}
pub struct OwnerBoundPublicationGuard {
    authority: Weak<RefCell<AuthorityState>>,
    candidate_work: Option<WorkOwner>,
    state: Option<CanonicalGuardState>,
    last_applied: Option<RecordNo>,
    revoked: bool,
}
impl OwnerBoundPublicationGuard {
    pub fn apply_recorded_step(
        &mut self,
        turn: &mut SessionTurn,
        step: CanonicalRecordedStep,
    ) -> Result<(), AuthorityError> {
        if !Weak::ptr_eq(&self.authority, &Rc::downgrade(&step.authority.state)) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        step.authority.validate_turn(turn)?;
        if self
            .last_applied
            .is_some_and(|previous| step.recorded_at <= previous)
        {
            return Err(AuthorityError::StaleCanonicalStep);
        }
        let prefix = step.authority.prefix()?;
        if step.recorded_at >= prefix.next_record
            || step.state.scope.binding != step.authority.binding()
        {
            return Err(AuthorityError::InvalidBinding);
        }
        let registered = step.authority.state.borrow();
        let accepted = registered
            .accepted_bindings
            .iter()
            .find(|binding| binding.id == step.state.scope.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        let runtime = registered
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == accepted.id)
            .ok_or(AuthorityError::InvalidBinding)?;
        if step.state.scope.slot != accepted.instrument_slot
            || step.state.scope.profile != accepted.feed_profile
            || step.state.scope.tag.spec != accepted.tag.spec
            || Some(step.state.scope.tag) != runtime.current_tag
            || step.state.scope.context != prefix.context
            || step.state.recording_gate != prefix.recording_gate
        {
            return Err(AuthorityError::InvalidBinding);
        }
        drop(registered);
        if let Some(current) = step.state.current
            && (current.available_at > step.recorded_at
                || current.causal_frontier > step.recorded_at)
        {
            return Err(AuthorityError::InvalidBinding);
        }
        if step.authority.status().failed {
            self.revoked = true;
            self.candidate_work.take();
        } else if step.state.current.is_some() && self.candidate_work.is_none() {
            self.candidate_work = Some(step.authority.reserve_work(turn, WorkKind::Candidate)?);
        } else if step.state.current.is_none() {
            self.candidate_work.take();
        }
        self.state = Some(step.state);
        self.last_applied = Some(step.recorded_at);
        Ok(())
    }
}
pub struct PublicationCandidate {
    authority: CaptureSessionAuthority,
    state: CandidateState,
    work: WorkOwner,
}
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompletionEvidence {
    TrustedContinuous,
    Untrusted,
    Discontinuous,
}
pub struct PublicationFence {
    authority: CaptureSessionAuthority,
    work: Option<WorkOwner>,
    binding: SessionBinding,
    through: Option<RecordNo>,
    achieved: Option<RecordNo>,
    accepted: Option<RecordNo>,
    previous: Option<RecordNo>,
    gate: RecordingGate,
    evidence: CompletionEvidence,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationView {
    pub binding: SessionBinding,
    pub candidate_identity: u64,
}
#[derive(Debug)]
pub enum PublicationReport<E> {
    Published,
    Denied(AuthorityError),
    EffectFailed(E),
}

fn publication_relation(
    state: &CanonicalGuardState,
    requested: &CandidateState,
    fence: &PublicationFence,
) -> Result<(), AuthorityError> {
    if state.phase != CanonicalPhase::Running {
        return Err(AuthorityError::CanonicalBlocked);
    }
    if requested.revoked
        || state.current != Some(*requested)
        || requested.projection.scope != state.scope
        || requested.identity.archive != state.scope.binding.archive
        || requested.identity.stream != state.scope.stream
        || requested.identity.creation_record != requested.available_at
    {
        return Err(AuthorityError::CandidateRevoked);
    }
    let freshness_ok = state.freshness == Freshness::Fresh
        || (state.freshness == Freshness::QuietVerified
            && state.allow_quiet_with_proof
            && state.quiet.is_some_and(|quiet| {
                quiet.scope == state.scope && state.evaluation_ns < quiet.expires_ns
            }));
    let (Some(anchor), Some(witness)) = (state.anchor, state.witness) else {
        return Err(AuthorityError::DataNotUsable);
    };
    if !freshness_ok
        || state.transport != Transport::Up
        || state.book != BookUsability::Usable
        || anchor.get() <= state.scope.barrier
        || witness.scope != state.scope
        || witness.anchor != anchor
        || requested.projection.anchor != anchor
        || requested.projection.witness_record != witness.record
        || requested.projection.freshness != state.freshness
        || requested.projection.evaluation_ns != state.evaluation_ns
    {
        return Err(AuthorityError::DataNotUsable);
    }
    if state.observed_recording != RecordingHealth::Healthy
        || requested.projection.observed_recording != state.observed_recording
    {
        return Err(AuthorityError::RecordingNotHealthy);
    }
    if requested.projection.gate != state.recording_gate {
        return Err(AuthorityError::GateConfiguration);
    }
    state
        .mode
        .validate_gate(requested.projection.gate)
        .map_err(|_| AuthorityError::GateConfiguration)?;
    if fence.binding != state.scope.binding {
        return Err(AuthorityError::FenceScopeMismatch);
    }
    if !fence.gate.covers(requested.projection.gate) {
        return Err(AuthorityError::FenceInsufficient);
    }
    let (Some(through), Some(achieved), Some(accepted)) =
        (fence.through, fence.achieved, fence.accepted)
    else {
        return Err(AuthorityError::FenceMissing);
    };
    if fence.evidence != CompletionEvidence::TrustedContinuous {
        return Err(AuthorityError::FenceUntrusted);
    }
    if fence.previous.is_some_and(|previous| through < previous) {
        return Err(AuthorityError::WatermarkRegression);
    }
    if through > achieved || achieved > accepted {
        return Err(AuthorityError::FenceBeyondAchieved);
    }
    let effect_frontier = requested
        .projection
        .effects
        .iter()
        .flatten()
        .map(|effect| effect.cursor.apply_record)
        .max();
    let minimum = anchor
        .max(witness.record)
        .max(requested.projection.last_receipt)
        .max(effect_frontier.unwrap_or(anchor));
    if requested.causal_frontier < minimum
        || requested.available_at < requested.causal_frontier
        || through < requested.causal_frontier
    {
        return Err(AuthorityError::FenceInsufficient);
    }
    if requested.projection.effects.iter().flatten().any(|effect| {
        effect.archive != state.scope.binding.archive
            || effect.normalizer != state.scope.context.normalizer
    }) {
        return Err(AuthorityError::CandidateRevoked);
    }
    Ok(())
}

impl CaptureSessionAuthority {
    /// Construct a standalone pure authority with one mutation turn. Backend
    /// integration must supply a truthful accepted bootstrap prefix and trusted
    /// `SessionRecordWriter`; canonical filesystem capture uses the recording
    /// owner factory instead. This does not adopt or authenticate another owner.
    pub fn new(binding: SessionBinding) -> (Self, SessionTurn) {
        let authority = Self {
            state: Rc::new(RefCell::new(AuthorityState {
                binding,
                lifecycle: SessionLifecycle::Open,
                failed: false,
                storage_stopped: None,
                first_failure: None,
                first_abandonment: None,
                archive_observation: None,
                marker: MarkerState::NotRequired,
                cut_sequence: None,
                sequence: 0,
                scopes: Vec::new(),
                work: Vec::new(),
                budget: None,
                prefix: None,
                registered: false,
                sink_bound: false,
                ticket: TicketState::Absent,
                proof_issued: false,
                proof_consumed: false,
                finalization_authorized: false,
                guard: None,
                storage_memory: StorageMemoryProfile::default(),
                confirmed_marker: None,
                accepted_bindings: Vec::new(),
                trusted_watermarks: [None; 5],
            })),
        };
        let turn = SessionTurn {
            authority: authority.clone(),
        };
        (authority, turn)
    }

    pub fn same_authority(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.state, &other.state)
    }
    pub fn validate_turn(&self, turn: &SessionTurn) -> Result<(), AuthorityError> {
        if self.same_authority(&turn.authority) {
            Ok(())
        } else {
            Err(AuthorityError::AuthorityMismatch)
        }
    }
    /// Serialize bounded Drop notifications into canonical failure. Drop itself
    /// never borrows this registry, advances the lifecycle/cut, or performs I/O.
    pub fn synchronize_obligations(&self, turn: &mut SessionTurn) -> Result<(), AuthorityError> {
        self.validate_turn(turn)?;
        let mut state = self.state.borrow_mut();
        let first = state
            .work
            .iter()
            .filter_map(|cell| cell.abandonment())
            .min_by_key(|work| work.work_id);
        let Some(first) = first else {
            return Ok(());
        };
        Self::latch(&mut state);
        if state.first_abandonment.is_none() {
            state.first_abandonment = state
                .work
                .iter()
                .filter_map(|cell| cell.abandonment())
                .find(|work| work.work_id == first.work_id);
        }
        // The admitted prefix hole cannot be repaired or crossed by a marker.
        // Explicit logical stop permits diagnostic descriptor closure while
        // preserving each undrained obligation, rather than endless NotReady.
        let error = PersistError::typed(
            PersistErrorKind::OwnershipAbandoned,
            "admitted observation ownership abandoned",
        );
        if state.storage_stopped.is_none() {
            state.storage_stopped = Some(error);
        }
        if matches!(
            state.marker,
            MarkerState::Pending | MarkerState::NotRequired
        ) {
            state.marker = MarkerState::Unconfirmed(error);
        }
        for scope in &state.scopes {
            let abandoned = state.work.iter().any(|cell| {
                cell.abandonment()
                    .is_some_and(|work| work.identity.stream == scope.binding.stream)
            });
            let needs_close = scope.close.identity.get().is_none_or(|identity| {
                identity.epoch != scope.binding.epoch
                    && scope.close.state.get() == CloseState::Settled
            });
            if abandoned && needs_close {
                scope.close.identity.set(Some(CloseIdentity {
                    stream: scope.binding.stream,
                    connection: scope.binding.connection,
                    epoch: scope.binding.epoch,
                    storage: CloseStorage::ReservedTerminal,
                }));
                scope.close.state.set(CloseState::Pending);
            }
        }
        Ok(())
    }
    pub fn binding(&self) -> SessionBinding {
        self.state.borrow().binding
    }
    pub fn prefix(&self) -> Result<PrefixBinding, AuthorityError> {
        self.state
            .borrow()
            .prefix
            .ok_or(AuthorityError::NotRegistered)
    }
    pub fn set_storage_memory_profile(
        &self,
        turn: &mut SessionTurn,
        profile: StorageMemoryProfile,
    ) -> Result<(), AuthorityError> {
        self.validate_turn(turn)?;
        if self.state.borrow().sink_bound {
            return Err(AuthorityError::StorageProfileBound);
        }
        self.update_backend_memory_profile(turn, profile)
    }

    fn update_backend_memory_profile(
        &self,
        turn: &mut SessionTurn,
        profile: StorageMemoryProfile,
    ) -> Result<(), AuthorityError> {
        self.validate_turn(turn)?;
        profile.validate()?;
        self.state.borrow_mut().storage_memory = profile;
        Ok(())
    }
    pub fn trusted_watermark(&self, kind: WatermarkKind) -> Option<RecordNo> {
        let index = match kind {
            WatermarkKind::Accepted => 0,
            WatermarkKind::Appended => 1,
            WatermarkKind::Written => 2,
            WatermarkKind::Flushed => 3,
            WatermarkKind::Durable => 4,
        };
        self.state.borrow().trusted_watermarks[index]
    }
    pub fn scopes(&self) -> [Option<ScopeBinding>; MAX_CAPTURE_SCOPES] {
        let state = self.state.borrow();
        std::array::from_fn(|index| state.scopes.get(index).map(|scope| scope.binding))
    }
    pub fn status(&self) -> SessionStatus {
        let s = self.state.borrow();
        SessionStatus {
            binding: s.binding,
            lifecycle: s.lifecycle,
            failed: s.failed,
            storage_stopped: s.storage_stopped,
            first_failure: s.first_failure,
            archive_observation: s.archive_observation,
            marker: s.marker,
            cut_sequence: s.cut_sequence,
            first_abandonment: s.first_abandonment,
        }
    }
    pub fn archive_failure_observation(&self) -> Option<ArchiveFailureObservation> {
        self.state.borrow().archive_observation
    }
    pub fn disposition(&self) -> SessionDisposition {
        let s = self.state.borrow();
        if s.failed
            || s.work
                .iter()
                .any(|cell| cell.obligation.get() == ObservationObligation::Abandoned)
        {
            SessionDisposition::DiagnosticOnly {
                binding: s.binding,
                failure: s.first_failure.map(|f| FailureId {
                    session: s.binding.session,
                    stream: f.stream,
                }),
            }
        } else if matches!(
            s.lifecycle,
            SessionLifecycle::DiagnosticClosed | SessionLifecycle::Finalized
        ) {
            SessionDisposition::Closed(s.binding)
        } else {
            SessionDisposition::CaptureEligible(s.binding)
        }
    }
    pub fn terminal_failure(&self, stream: StreamId) -> Option<TerminalFailure> {
        self.state
            .borrow()
            .scopes
            .iter()
            .find(|s| s.binding.stream == stream)
            .and_then(|s| s.failure)
    }
    pub fn advance_epoch(
        &self,
        turn: &mut SessionTurn,
        stream: StreamId,
        expected: ConnectionEpoch,
        next: ConnectionEpoch,
    ) -> Result<(), AuthorityError> {
        self.synchronize_obligations(turn)?;
        if next.get()
            != expected
                .get()
                .checked_add(1)
                .ok_or(AuthorityError::CounterExhausted("ConnectionEpoch"))?
        {
            return Err(AuthorityError::InvalidBinding);
        }
        let mut state = self.state.borrow_mut();
        let scope = state
            .scopes
            .iter_mut()
            .find(|scope| scope.binding.stream == stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        if scope.failure.is_some() || scope.binding.epoch != expected {
            return Err(AuthorityError::CommandRevoked);
        }
        if scope.close.identity.get().is_some() && scope.close.state.get() != CloseState::Settled {
            return Err(AuthorityError::NotQuiescent);
        }
        let tag = scope.current_tag.ok_or(AuthorityError::InvalidBinding)?;
        let (_, _, connection_record) = scope
            .confirmed_advance
            .filter(|(old, new, _)| *old == expected && *new == next)
            .ok_or(AuthorityError::InvalidBinding)?;
        let subscription_next = tag
            .subscription
            .checked_next()
            .map_err(|_| AuthorityError::CounterExhausted("SubscriptionEpoch"))?;
        let (_, _, subscription_record) = scope
            .confirmed_subscription
            .filter(|(old, new, record)| {
                *old == tag.subscription && *new == subscription_next && *record > connection_record
            })
            .ok_or(AuthorityError::InvalidBinding)?;
        let book_next = if let Some(book) = tag.book {
            let next = book
                .checked_next()
                .map_err(|_| AuthorityError::CounterExhausted("BookEpoch"))?;
            scope
                .confirmed_book
                .filter(|(old, new, record)| {
                    *old == book && *new == next && *record > subscription_record
                })
                .ok_or(AuthorityError::InvalidBinding)?;
            Some(next)
        } else {
            None
        };
        scope.current_tag = Some(EpochTag {
            connection: next,
            subscription: subscription_next,
            book: book_next,
            ..tag
        });
        scope.binding.epoch = next;
        Ok(())
    }
    pub fn scope_disposition(&self, stream: StreamId) -> Result<ScopeDisposition, AuthorityError> {
        let s = self.state.borrow();
        let scope = s
            .scopes
            .iter()
            .find(|s| s.binding.stream == stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        Ok(scope.failure.map_or(ScopeDisposition::Active, |_| {
            ScopeDisposition::CaptureTerminated(FailureId {
                session: s.binding.session,
                stream,
            })
        }))
    }

    pub fn register_supervisor(
        &self,
        turn: &mut SessionTurn,
        scopes: &[ScopeBinding],
        budget: RetentionBudget,
        prefix: PrefixBinding,
    ) -> Result<SupervisorSessionHandle, AuthorityError> {
        self.validate_turn(turn)?;
        let n = scopes.len();
        if n == 0
            || n > MAX_CAPTURE_SCOPES
            || budget.item_cap > MAX_RETAINED_ITEMS
            || budget.item_cap <= 4 * n
            || budget.raw_frame_limit == 0
            || budget.raw_byte_limit == 0
            || budget.max_message_bytes == 0
            || budget.raw_frame_limit > 64
            || budget.raw_byte_limit > 1_000_000
            || budget.max_message_bytes > 1_000_000
        {
            return Err(AuthorityError::InvalidBudget);
        }
        for (i, scope) in scopes.iter().enumerate() {
            if scopes[..i]
                .iter()
                .any(|old| old.stream == scope.stream || old.connection == scope.connection)
            {
                return Err(AuthorityError::InvalidBinding);
            }
        }
        let mut s = self.state.borrow_mut();
        if s.registered {
            return Err(AuthorityError::AlreadyRegistered);
        }
        if s.lifecycle != SessionLifecycle::Open || s.failed {
            return Err(AuthorityError::WrongLifecycle);
        }
        let limit = budget.item_cap - n - 1;
        s.scopes = scopes
            .iter()
            .map(|binding| ScopeState {
                binding: *binding,
                failure: None,
                confirmed_up: None,
                confirmed_down: None,
                confirmed_timer: None,
                confirmed_advance: None,
                confirmed_subscription: None,
                confirmed_book: None,
                current_tag: None,
                close: Rc::new(CloseCell {
                    identity: Cell::new(None),
                    state: Cell::new(CloseState::Pending),
                    work: RefCell::new(None),
                }),
            })
            .collect();
        s.work = (0..limit)
            .map(|_| {
                Rc::new(WorkCell {
                    sequence: Cell::new(0),
                    close_scope: Cell::new(None),
                    references: Cell::new(0),
                    kind: Cell::new(WorkKind::QueuedObservation),
                    cut_side: Cell::new(CutSide::BeforeFailure),
                    obligation: Cell::new(ObservationObligation::None),
                    observation: Cell::new(None),
                    confirmed_records: Cell::new(0),
                    abandoned_kind: Cell::new(None),
                    obligation_origin: Cell::new(ObligationOrigin::Received),
                })
            })
            .collect();
        s.prefix = Some(prefix);
        s.budget = Some(budget);
        s.registered = true;
        if let Some(through) = prefix
            .next_record
            .get()
            .checked_sub(1)
            .and_then(|no| RecordNo::new(no).ok())
        {
            BoundRecordSink::raise_watermarks(
                &mut s.trusted_watermarks,
                PersistenceReceipt {
                    through,
                    achieved_gate: prefix.recording_gate,
                },
            );
        }
        Ok(SupervisorSessionHandle {
            authority: self.clone(),
        })
    }

    pub fn set_accepted_stream_bindings(
        &self,
        turn: &mut SessionTurn,
        bindings: &[StreamBinding],
    ) -> Result<(), AuthorityError> {
        self.validate_turn(turn)?;
        let mut state = self.state.borrow_mut();
        if !state.accepted_bindings.is_empty() {
            return Err(AuthorityError::AlreadyRegistered);
        }
        if bindings.len() != state.scopes.len() || bindings.len() > MAX_CAPTURE_SCOPES {
            return Err(AuthorityError::InvalidBinding);
        }
        for binding in bindings {
            binding
                .validate()
                .map_err(|_| AuthorityError::InvalidBinding)?;
            if !state.scopes.iter().any(|scope| {
                scope.binding.stream == binding.id
                    && scope.binding.connection == binding.connection_id
                    && scope.binding.epoch == binding.tag.connection
            }) {
                return Err(AuthorityError::InvalidBinding);
            }
        }
        for scope in &mut state.scopes {
            scope.current_tag = bindings
                .iter()
                .find(|binding| binding.id == scope.binding.stream)
                .map(|binding| binding.tag);
        }
        state.accepted_bindings = bindings.to_vec();
        Ok(())
    }

    pub fn bind_sink(
        &self,
        turn: &mut SessionTurn,
        writer: Box<dyn SessionRecordWriter>,
    ) -> Result<BoundRecordSink, AuthorityError> {
        self.bind_sink_with_memory_authority(turn, writer)
            .map(|(sink, _)| sink)
    }

    /// The first backend binding alone receives its non-cloneable profile
    /// update authority. Validate the report before consuming that binding.
    pub fn bind_sink_with_memory_authority(
        &self,
        turn: &mut SessionTurn,
        writer: Box<dyn SessionRecordWriter>,
    ) -> Result<(BoundRecordSink, StorageMemoryAuthority), AuthorityError> {
        self.validate_turn(turn)?;
        let profile = writer.checked_memory_profile()?;
        profile.validate()?;
        let mut s = self.state.borrow_mut();
        if !s.registered {
            return Err(AuthorityError::NotRegistered);
        }
        if s.sink_bound {
            return Err(AuthorityError::SinkAlreadyBound);
        }
        s.storage_memory = profile;
        s.sink_bound = true;
        Ok((
            BoundRecordSink {
                authority: self.clone(),
                writer,
            },
            StorageMemoryAuthority {
                authority: self.clone(),
            },
        ))
    }

    pub fn ensure_admission_open(&self, turn: &SessionTurn) -> Result<(), AuthorityError> {
        self.validate_turn(turn)?;
        let s = self.state.borrow();
        if s.storage_stopped.is_some() {
            return Err(AuthorityError::StorageStopped);
        }
        match s.lifecycle {
            SessionLifecycle::Open | SessionLifecycle::FailedDiagnostic => Ok(()),
            SessionLifecycle::Closing | SessionLifecycle::DiagnosticClosing => {
                Err(AuthorityError::SessionClosing)
            }
            SessionLifecycle::DiagnosticClosed | SessionLifecycle::Finalized => {
                Err(AuthorityError::SessionClosed)
            }
        }
    }
    fn confirm_persisted_frame(&self, frame: &RecordFrame) {
        let mut state = self.state.borrow_mut();
        if let Record::Control(record) = &frame.value {
            match record.value {
                Control::Transport {
                    connection,
                    epoch,
                    value: Transport::Up,
                } => {
                    if let Some(scope) = state
                        .scopes
                        .iter_mut()
                        .find(|scope| scope.binding.connection == connection)
                    {
                        scope.confirmed_up = Some(epoch);
                    }
                }
                Control::Transport {
                    connection,
                    epoch,
                    value: Transport::Down,
                } => {
                    if let Some(scope) = state
                        .scopes
                        .iter_mut()
                        .find(|scope| scope.binding.connection == connection)
                    {
                        scope.confirmed_down = Some(epoch);
                    }
                }
                Control::Timer { stream, .. } => {
                    if let Some(scope) = state
                        .scopes
                        .iter_mut()
                        .find(|scope| scope.binding.stream == stream)
                    {
                        scope.confirmed_timer = Some(frame.record_no);
                    }
                }
                Control::EpochAdvance {
                    change:
                        EpochChange::Connection {
                            owner,
                            expected,
                            next,
                        },
                    ..
                } => {
                    if let Some(scope) = state
                        .scopes
                        .iter_mut()
                        .find(|scope| scope.binding.connection == owner)
                    {
                        scope.confirmed_advance = Some((expected, next, frame.record_no));
                        scope.confirmed_subscription = None;
                        scope.confirmed_book = None;
                    }
                }
                Control::EpochAdvance {
                    change:
                        EpochChange::Subscription {
                            owner,
                            expected,
                            next,
                        },
                    ..
                } => {
                    if let Some(scope) = state
                        .scopes
                        .iter_mut()
                        .find(|scope| scope.binding.stream == owner)
                    {
                        scope.confirmed_subscription = Some((expected, next, frame.record_no));
                    }
                }
                Control::EpochAdvance {
                    change:
                        EpochChange::Book {
                            owner,
                            expected,
                            next,
                        },
                    ..
                } => {
                    let stream = state
                        .accepted_bindings
                        .iter()
                        .find(|binding| binding.book_id == Some(owner))
                        .map(|binding| binding.id);
                    if let Some(scope) = state
                        .scopes
                        .iter_mut()
                        .find(|scope| Some(scope.binding.stream) == stream)
                    {
                        scope.confirmed_book = Some((expected, next, frame.record_no));
                    }
                }
                Control::Recording(ref evidence) if evidence.health == RecordingHealth::Failed => {
                    Self::latch(&mut state);
                    state.confirmed_marker = Some(frame.record_no);
                    state.marker = MarkerState::Confirmed(frame.record_no);
                }
                _ => {}
            }
        }
    }
    pub fn ensure_storage_writable(&self) -> Result<(), AuthorityError> {
        let s = self.state.borrow();
        if s.storage_stopped.is_some()
            || s.work
                .iter()
                .any(|cell| cell.obligation.get() == ObservationObligation::Abandoned)
        {
            return Err(AuthorityError::StorageStopped);
        }
        if matches!(
            s.lifecycle,
            SessionLifecycle::DiagnosticClosed | SessionLifecycle::Finalized
        ) {
            return Err(AuthorityError::SessionClosed);
        }
        Ok(())
    }
    pub fn ensure_finalization_authorized(&self) -> Result<(), AuthorityError> {
        let s = self.state.borrow();
        if s.failed
            || s.work
                .iter()
                .any(|cell| cell.obligation.get() == ObservationObligation::Abandoned)
        {
            return Err(AuthorityError::ArchiveFailed);
        }
        if s.storage_stopped.is_some() {
            return Err(AuthorityError::StorageStopped);
        }
        if s.lifecycle != SessionLifecycle::Closing || !s.finalization_authorized {
            return Err(AuthorityError::InvalidProof);
        }
        Ok(())
    }
    pub fn permit_writer(&self, kind: RecordKind) -> Result<(), AuthorityError> {
        self.ensure_storage_writable()?;
        if matches!(kind, RecordKind::ArchiveSeal | RecordKind::SegmentSeal) {
            self.ensure_finalization_authorized()?;
        }
        if kind == RecordKind::SegmentStart {
            return Err(AuthorityError::InvalidBinding);
        }
        Ok(())
    }

    pub fn reserve_work(
        &self,
        turn: &mut SessionTurn,
        kind: WorkKind,
    ) -> Result<WorkOwner, AuthorityError> {
        self.synchronize_obligations(turn)?;
        self.ensure_admission_open(turn)?;
        let mut s = self.state.borrow_mut();
        let cell = s
            .work
            .iter()
            .find(|c| !c.is_retained())
            .cloned()
            .ok_or(AuthorityError::WorkExhausted)?;
        let sequence = s
            .sequence
            .checked_add(1)
            .ok_or(AuthorityError::CounterExhausted("AdmissionOrder"))?;
        s.sequence = sequence;
        cell.sequence.set(sequence);
        cell.close_scope.set(None);
        cell.references.set(1);
        cell.obligation.set(ObservationObligation::None);
        cell.observation.set(None);
        cell.confirmed_records.set(0);
        cell.abandoned_kind.set(None);
        cell.obligation_origin.set(ObligationOrigin::Received);
        cell.kind.set(kind);
        cell.cut_side.set(if s.failed {
            CutSide::PostCut
        } else {
            CutSide::BeforeFailure
        });
        Ok(WorkOwner {
            authority: Rc::downgrade(&self.state),
            cell,
            sequence,
        })
    }

    fn latch(s: &mut AuthorityState) {
        if !s.failed {
            s.failed = true;
            s.cut_sequence = Some(s.sequence);
            for cell in &s.work {
                if cell.is_retained() {
                    cell.cut_side.set(CutSide::PreCut);
                }
            }
            s.marker = MarkerState::Pending;
        }
        s.lifecycle = match s.lifecycle {
            SessionLifecycle::Open => SessionLifecycle::FailedDiagnostic,
            SessionLifecycle::Closing => SessionLifecycle::DiagnosticClosing,
            other => other,
        };
        if s.ticket == TicketState::Active {
            s.ticket = TicketState::Invalidated;
        }
        s.finalization_authorized = false;
        if let Some(g) = s.guard.as_mut() {
            g.revoked = true;
            g.candidate_work.take();
        }
    }

    pub fn terminate(
        &self,
        turn: &mut SessionTurn,
        failure: TerminalFailure,
    ) -> Result<TerminationReport, AuthorityError> {
        self.synchronize_obligations(turn)?;
        let first;
        {
            let mut s = self.state.borrow_mut();
            let index = s
                .scopes
                .iter()
                .position(|scope| {
                    scope.binding.stream == failure.stream
                        && scope.binding.connection == failure.connection
                })
                .ok_or(AuthorityError::InvalidBinding)?;
            first = s.scopes[index].failure.is_none();
            if first {
                if !s.failed && s.archive_observation.is_none() {
                    let prefix = s.prefix.ok_or(AuthorityError::NotRegistered)?;
                    s.archive_observation = Some(ArchiveFailureObservation {
                        context: WireContext {
                            unix_ns: LocalUnixNs::new(failure.stamp.unix_ns),
                            monotonic_ns: MonotonicNs::new(failure.stamp.monotonic_ns),
                            context: crate::event::InputContext::Active(failure.context),
                        },
                        reason: match failure.cause {
                            FailureCause::QueueOverflow => Reason::QueueOverflow,
                            _ => Reason::Unknown,
                        },
                        kind: match prefix.recording_gate {
                            RecordingGate::Written => WatermarkKind::Written,
                            RecordingGate::Flushed => WatermarkKind::Flushed,
                            RecordingGate::Durable => WatermarkKind::Durable,
                        },
                    });
                }
                if s.first_failure.is_none() {
                    s.first_failure = Some(failure);
                }
                s.scopes[index].failure = Some(failure);
                Self::latch(&mut s);
            }
        }
        let stored = self
            .terminal_failure(failure.stream)
            .expect("installed failure");
        let owner = self.mandatory_close(turn, stored.stream, stored.current_epoch, None)?;
        let close = if first {
            match self.reclaim_close(turn, owner.clone()) {
                CloseLeaseReport::Leased(lease) => Some(lease),
                _ => None,
            }
        } else {
            None
        };
        Ok(TerminationReport {
            failure_id: FailureId {
                session: self.binding().session,
                stream: stored.stream,
            },
            first,
            close_owner: owner,
            close,
        })
    }

    pub fn hard_stop(
        &self,
        turn: &mut SessionTurn,
        error: PersistError,
    ) -> Result<(), AuthorityError> {
        self.storage_stopped(turn, error)
    }
    pub fn storage_stopped(
        &self,
        turn: &mut SessionTurn,
        error: PersistError,
    ) -> Result<(), AuthorityError> {
        self.synchronize_obligations(turn)?;
        let mut s = self.state.borrow_mut();
        if s.storage_stopped.is_none() {
            s.storage_stopped = Some(error);
        }
        Self::latch(&mut s);
        if matches!(s.marker, MarkerState::Pending | MarkerState::NotRequired) {
            s.marker = MarkerState::Unconfirmed(error);
        }
        Ok(())
    }
    pub fn marker_confirmed(
        &self,
        turn: &mut SessionTurn,
        record: RecordNo,
    ) -> Result<(), AuthorityError> {
        self.synchronize_obligations(turn)?;
        let mut s = self.state.borrow_mut();
        if s.storage_stopped.is_some() {
            return Err(AuthorityError::StorageStopped);
        }
        if s.marker == MarkerState::Confirmed(record) && s.confirmed_marker == Some(record) {
            return Ok(());
        }
        if s.marker != MarkerState::Pending {
            return Err(AuthorityError::InvalidBinding);
        }
        if s.confirmed_marker != Some(record) {
            return Err(AuthorityError::InvalidBinding);
        }
        if s.work
            .iter()
            .any(|cell| cell.blocks_marker() && cell.cut_side.get() == CutSide::PreCut)
        {
            return Err(AuthorityError::NotQuiescent);
        }
        s.marker = MarkerState::Confirmed(record);
        Ok(())
    }

    pub fn mandatory_close(
        &self,
        turn: &mut SessionTurn,
        stream: StreamId,
        epoch: ConnectionEpoch,
        work: Option<&WorkOwner>,
    ) -> Result<CloseOwnerRef, AuthorityError> {
        self.synchronize_obligations(turn)?;
        if let Some(w) = work
            && !Weak::ptr_eq(&Rc::downgrade(&self.state), &w.authority)
        {
            return Err(AuthorityError::AuthorityMismatch);
        }
        if let Some(work) = work {
            if work.cell.sequence.get() != work.sequence || work.cell.references.get() == 0 {
                return Err(AuthorityError::OwnerRetired);
            }
            if work
                .cell
                .close_scope
                .get()
                .is_some_and(|owner| owner != stream)
            {
                return Err(AuthorityError::InvalidOwner);
            }
        }
        let s = self.state.borrow();
        let scope = s
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == stream)
            .ok_or(AuthorityError::InvalidOwner)?;
        if epoch != scope.binding.epoch {
            // An already-existing old Close remains addressable; minting a new
            // owner for guessed future/retired epochs is forbidden.
            if let Some(current) = scope.close.identity.get()
                && current.epoch == epoch
            {
                return Ok(CloseOwnerRef {
                    authority: self.clone(),
                    identity: current,
                });
            }
            return Err(AuthorityError::OwnerRetired);
        }
        if let Some(current) = scope.close.identity.get() {
            if current.epoch == epoch {
                return Ok(CloseOwnerRef {
                    authority: self.clone(),
                    identity: current,
                });
            }
            if scope.close.state.get() != CloseState::Settled {
                return Err(AuthorityError::InvalidOwner);
            }
            if epoch <= current.epoch {
                return Err(AuthorityError::OwnerRetired);
            }
        }
        let identity = CloseIdentity {
            stream,
            connection: scope.binding.connection,
            epoch,
            storage: work.map_or(CloseStorage::ReservedTerminal, |w| {
                CloseStorage::WorkOwner(w.id())
            }),
        };
        let retained = work.map(WorkOwner::share).transpose()?;
        if let Some(work) = work {
            work.cell.close_scope.set(Some(stream));
        }
        *scope.close.work.borrow_mut() = retained;
        scope.close.identity.set(Some(identity));
        scope.close.state.set(CloseState::Pending);
        Ok(CloseOwnerRef {
            authority: self.clone(),
            identity,
        })
    }
    fn close_cell(&self, owner: &CloseOwnerRef) -> Result<Rc<CloseCell>, AuthorityError> {
        if !self.same_authority(&owner.authority) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        let s = self.state.borrow();
        let scope = s
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == owner.identity.stream)
            .ok_or(AuthorityError::InvalidOwner)?;
        if scope.close.identity.get() != Some(owner.identity) {
            return Err(AuthorityError::OwnerRetired);
        }
        Ok(Rc::clone(&scope.close))
    }
    pub fn outstanding_close_owners(&self) -> CloseOwnerSnapshot {
        let s = self.state.borrow();
        CloseOwnerSnapshot {
            owners: std::array::from_fn(|index| {
                s.scopes
                    .get(index)
                    .filter(|scope| scope.close.state.get() != CloseState::Settled)
                    .and_then(|scope| {
                        scope.close.identity.get().map(|identity| CloseOwnerView {
                            owner: CloseOwnerRef {
                                authority: self.clone(),
                                identity,
                            },
                            state: scope.close.state.get(),
                        })
                    })
            }),
        }
    }
    pub fn close_state(
        &self,
        stream: StreamId,
        epoch: ConnectionEpoch,
    ) -> Result<CloseState, AuthorityError> {
        let state = self.state.borrow();
        let scope = state
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == stream)
            .ok_or(AuthorityError::InvalidOwner)?;
        let identity = scope
            .close
            .identity
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        if identity.epoch != epoch {
            return Err(AuthorityError::OwnerRetired);
        }
        Ok(scope.close.state.get())
    }
    pub fn reclaim_close(&self, turn: &mut SessionTurn, owner: CloseOwnerRef) -> CloseLeaseReport {
        if let Err(e) = self.validate_turn(turn) {
            return CloseLeaseReport::Rejected(e);
        }
        let cell = match self.close_cell(&owner) {
            Ok(cell) => cell,
            Err(e) => return CloseLeaseReport::Rejected(e),
        };
        match cell.state.get() {
            CloseState::Pending => {
                let work = match cell
                    .work
                    .borrow()
                    .as_ref()
                    .map(WorkOwner::share_for_close)
                    .transpose()
                {
                    Ok(work) => work,
                    Err(error) => return CloseLeaseReport::Rejected(error),
                };
                cell.state.set(CloseState::Leased);
                CloseLeaseReport::Leased(CloseLease {
                    owner,
                    cell,
                    work,
                    armed: true,
                })
            }
            CloseState::Leased => CloseLeaseReport::AlreadyLeased,
            CloseState::Settled => CloseLeaseReport::AlreadySettled,
        }
    }
    pub fn confirm_closed(
        &self,
        turn: &mut SessionTurn,
        owner: CloseOwnerRef,
        evidence: AuthenticatedClosure,
    ) -> CloseSettlementReport {
        if let Err(e) = self.validate_turn(turn) {
            return CloseSettlementReport::Rejected(e);
        }
        let cell = match self.close_cell(&owner) {
            Ok(cell) => cell,
            Err(e) => return CloseSettlementReport::Rejected(e),
        };
        if !self.same_authority(&evidence.authority)
            || evidence.connection != owner.identity.connection
            || evidence.epoch != owner.identity.epoch
        {
            return CloseSettlementReport::Rejected(AuthorityError::InvalidOwner);
        }
        if cell.state.get() == CloseState::Settled {
            return CloseSettlementReport::AlreadySettled;
        }
        cell.state.set(CloseState::Settled);
        cell.work.borrow_mut().take();
        CloseSettlementReport::Settled
    }

    pub fn command(
        &self,
        turn: &mut SessionTurn,
        stream: StreamId,
        epoch: ConnectionEpoch,
        kind: CommandKind,
        work: &WorkOwner,
    ) -> Result<CommandLease, AuthorityError> {
        self.validate_turn(turn)?;
        if !Weak::ptr_eq(&Rc::downgrade(&self.state), &work.authority) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        if kind == CommandKind::Close {
            return Err(AuthorityError::InvalidOwner);
        }
        self.ensure_admission_open(turn)?;
        let s = self.state.borrow();
        let scope = s
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        if scope.failure.is_some() {
            return Err(AuthorityError::CommandRevoked);
        }
        if scope.binding.epoch != epoch {
            return Err(AuthorityError::CommandRevoked);
        }
        match &kind {
            CommandKind::SendText { text } if text == "ping" => {
                if scope.confirmed_timer.is_none() {
                    return Err(AuthorityError::InvalidBinding);
                }
            }
            CommandKind::SendText { .. } => {
                if scope.confirmed_up != Some(epoch) {
                    return Err(AuthorityError::InvalidBinding);
                }
            }
            CommandKind::ReconnectAfter { .. }
                if scope
                    .confirmed_advance
                    .is_none_or(|(_, next, _)| next != epoch) =>
            {
                return Err(AuthorityError::InvalidBinding);
            }
            _ => {}
        }
        if matches!(&kind,CommandKind::SendText {text} if text.len()>4096 || text.capacity()>4096) {
            return Err(AuthorityError::InvalidBudget);
        }
        Ok(CommandLease {
            authority: self.clone(),
            stream,
            connection: scope.binding.connection,
            epoch,
            kind,
            work: Some(work.share()?),
            close: None,
        })
    }

    pub fn dispatch<E>(
        &self,
        turn: &mut SessionTurn,
        mut command: CommandLease,
        effect: impl FnOnce(CommandView<'_>) -> Result<(), E>,
    ) -> DispatchReport<E> {
        if let Err(reason) = self.validate_turn(turn) {
            return DispatchReport::Denied { reason, command };
        }
        if !self.same_authority(&command.authority) {
            return DispatchReport::Denied {
                reason: AuthorityError::AuthorityMismatch,
                command,
            };
        }
        if let Err(reason) = self.synchronize_obligations(turn) {
            return DispatchReport::Denied { reason, command };
        }
        if let Some(close) = command.close.as_ref() {
            if self.close_cell(&close.owner).is_err() {
                return DispatchReport::Revoked(AuthorityError::OwnerRetired);
            }
            if close.cell.state.get() == CloseState::Settled {
                return DispatchReport::AlreadySettled;
            }
            if close.cell.state.get() != CloseState::Leased {
                return DispatchReport::Revoked(AuthorityError::InvalidOwner);
            }
        } else {
            let s = self.state.borrow();
            let scope = s
                .scopes
                .iter()
                .find(|scope| scope.binding.stream == command.stream);
            if s.storage_stopped.is_some()
                || !matches!(
                    s.lifecycle,
                    SessionLifecycle::Open | SessionLifecycle::FailedDiagnostic
                )
                || scope.is_none_or(|scope| {
                    scope.failure.is_some() || scope.binding.epoch != command.epoch
                })
            {
                return DispatchReport::Revoked(AuthorityError::CommandRevoked);
            }
        }
        let result = effect(CommandView {
            stream: command.stream,
            connection: command.connection,
            epoch: command.epoch,
            kind: &command.kind,
        });
        if let Some(close) = command.close.as_mut() {
            close.armed = false;
            if close.cell.identity.get() == Some(close.owner.identity) {
                match result {
                    Ok(()) => {
                        close.cell.state.set(CloseState::Settled);
                        close.cell.work.borrow_mut().take();
                    }
                    Err(_) => close.cell.state.set(CloseState::Pending),
                }
            }
        }
        match result {
            Ok(()) => DispatchReport::Dispatched,
            Err(error) => DispatchReport::DispatchFailed {
                error,
                effect: AmbiguousEffect::Unknown,
            },
        }
    }

    pub fn ownership_report(&self) -> OwnershipReport {
        let s = self.state.borrow();
        let used = s.work.iter().filter(|cell| cell.is_retained()).count();
        let metadata = std::mem::size_of::<RefCell<AuthorityState>>()
            + s.scopes.capacity() * std::mem::size_of::<ScopeState>()
            + s.work.capacity() * std::mem::size_of::<Rc<WorkCell>>()
            + s.work.len() * (std::mem::size_of::<WorkCell>() + 2 * std::mem::size_of::<usize>())
            + s.scopes.len()
                * (std::mem::size_of::<CloseCell>() + 2 * std::mem::size_of::<usize>())
            + 2 * std::mem::size_of::<usize>()
            + s.accepted_bindings.capacity() * std::mem::size_of::<StreamBinding>()
            + s.accepted_bindings
                .iter()
                .map(|binding| {
                    binding.spec.instrument.venue.as_str().len()
                        + binding.spec.instrument.product_namespace.as_str().len()
                        + binding.spec.instrument.native_symbol.as_str().len()
                })
                .sum::<usize>();
        let inline_owners = s
            .work
            .iter()
            .map(|cell| {
                cell.references.get()
                    * (std::mem::size_of::<WorkOwner>() + std::mem::size_of::<CandidateState>())
            })
            .sum::<usize>();
        let inline_owner_ceiling = s.work.len()
            * (MAX_WORK_SHARES + 1)
            * (std::mem::size_of::<WorkOwner>()
                + std::mem::size_of::<CandidateState>()
                + std::mem::size_of::<CommandLease>()
                + 4096);
        let mut report = OwnershipReport {
            item_cap: s.budget.map_or(0, |b| b.item_cap),
            reserved_scopes: s.scopes.len(),
            reserved_archive: 1,
            work_limit: s.work.len(),
            work_used: used,
            pre_cut: 0,
            post_cut: 0,
            before_failure: 0,
            work_references: 0,
            pending_observations: 0,
            abandoned_work: 0,
            metadata_backing_bytes: metadata,
            inline_accounted_capacity_bytes: inline_owners,
            inline_ceiling_bytes: inline_owner_ceiling,
            metadata_ceiling_bytes: metadata + inline_owner_ceiling,
            storage_memory: s.storage_memory,
        };
        for cell in &s.work {
            if cell.is_retained() {
                report.work_references += cell.references.get();
                report.pending_observations +=
                    usize::from(cell.obligation.get() == ObservationObligation::Pending);
                report.abandoned_work +=
                    usize::from(cell.obligation.get() == ObservationObligation::Abandoned);
                match cell.cut_side.get() {
                    CutSide::BeforeFailure => report.before_failure += 1,
                    CutSide::PreCut => report.pre_cut += 1,
                    CutSide::PostCut => report.post_cut += 1,
                }
            }
        }
        report
    }
    pub fn unsettled_summary(&self) -> UnsettledSummary {
        let s = self.state.borrow();
        let mut r = UnsettledSummary {
            queued: 0,
            in_flight: 0,
            pending_plans: 0,
            results: 0,
            commands: 0,
            candidates: 0,
            work_total: 0,
            abandoned: 0,
            marker: s.marker,
            close_owners: CloseOwnerSnapshot {
                owners: std::array::from_fn(|_| None),
            },
        };
        for cell in &s.work {
            if cell.is_retained() {
                r.work_total += 1;
                r.abandoned +=
                    usize::from(cell.obligation.get() == ObservationObligation::Abandoned);
                match cell.kind.get() {
                    WorkKind::QueuedObservation => r.queued += 1,
                    WorkKind::InFlightObservation => r.in_flight += 1,
                    WorkKind::PendingPlan => r.pending_plans += 1,
                    WorkKind::Result => r.results += 1,
                    WorkKind::Command => r.commands += 1,
                    WorkKind::Candidate => r.candidates += 1,
                }
            }
        }
        drop(s);
        r.close_owners = self.outstanding_close_owners();
        r
    }

    pub fn begin_finalization(
        &self,
        turn: &mut SessionTurn,
    ) -> Result<CloseTicket, AuthorityError> {
        self.synchronize_obligations(turn)?;
        let mut s = self.state.borrow_mut();
        if s.failed {
            return Err(AuthorityError::ArchiveFailed);
        }
        if s.lifecycle == SessionLifecycle::Closing {
            return Err(AuthorityError::AlreadyClosing);
        }
        if s.lifecycle != SessionLifecycle::Open {
            return Err(AuthorityError::WrongLifecycle);
        }
        if !s.registered {
            return Err(AuthorityError::NotRegistered);
        }
        s.lifecycle = SessionLifecycle::Closing;
        s.ticket = TicketState::Active;
        Ok(CloseTicket {
            authority: self.clone(),
        })
    }
    pub fn quiesce(&self, turn: &mut SessionTurn, ticket: &CloseTicket) -> QuiescenceReport {
        if let Err(e) = self.validate_turn(turn) {
            return QuiescenceReport::Rejected(e);
        }
        if !self.same_authority(&ticket.authority) {
            return QuiescenceReport::Rejected(AuthorityError::AuthorityMismatch);
        }
        if let Err(e) = self.synchronize_obligations(turn) {
            return QuiescenceReport::Rejected(e);
        }
        let mut s = self.state.borrow_mut();
        if s.failed {
            return QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed);
        }
        if s.ticket == TicketState::Consumed {
            return QuiescenceReport::TicketConsumed;
        }
        if s.ticket != TicketState::Active || s.lifecycle != SessionLifecycle::Closing {
            return QuiescenceReport::Rejected(AuthorityError::InvalidTicket);
        }
        let unsettled = s.work.iter().any(|cell| cell.is_retained())
            || s.scopes.iter().any(|scope| {
                scope.close.identity.get().is_some()
                    && scope.close.state.get() != CloseState::Settled
            })
            || s.marker == MarkerState::Pending;
        if unsettled {
            drop(s);
            return QuiescenceReport::NotReady(self.unsettled_summary());
        }
        s.ticket = TicketState::Consumed;
        s.proof_issued = true;
        QuiescenceReport::Ready(QuiescenceProof {
            authority: self.clone(),
        })
    }
    /// Validate the owner, turn and sole issued proof before spending its
    /// authorization. Rejections preserve the borrowed value and rightful
    /// state; successful consumption is irreversible even if storage fails.
    pub fn consume_proof(
        &self,
        turn: &mut SessionTurn,
        proof: &mut QuiescenceProof,
    ) -> Result<(), AuthorityError> {
        self.validate_turn(turn)?;
        if !self.same_authority(&proof.authority) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        self.synchronize_obligations(turn)?;
        let mut s = self.state.borrow_mut();
        if s.failed {
            return Err(AuthorityError::ArchiveFailed);
        }
        if s.proof_consumed {
            return Err(AuthorityError::ProofConsumed);
        }
        if s.lifecycle != SessionLifecycle::Closing
            || !s.proof_issued
            || s.ticket != TicketState::Consumed
        {
            return Err(AuthorityError::InvalidProof);
        }
        if s.work.iter().any(|c| c.is_retained())
            || s.scopes.iter().any(|scope| {
                scope.close.identity.get().is_some()
                    && scope.close.state.get() != CloseState::Settled
            })
        {
            return Err(AuthorityError::NotQuiescent);
        }
        s.proof_consumed = true;
        s.finalization_authorized = true;
        Ok(())
    }
    pub fn finalization_finished(&self, turn: &mut SessionTurn) -> Result<(), AuthorityError> {
        self.synchronize_obligations(turn)?;
        self.ensure_finalization_authorized()?;
        let mut s = self.state.borrow_mut();
        s.lifecycle = SessionLifecycle::Finalized;
        s.finalization_authorized = false;
        Ok(())
    }
    pub fn begin_diagnostic_close(
        &self,
        turn: &mut SessionTurn,
    ) -> Result<SessionLifecycle, AuthorityError> {
        self.synchronize_obligations(turn)?;
        let mut s = self.state.borrow_mut();
        match s.lifecycle {
            SessionLifecycle::FailedDiagnostic => {
                s.lifecycle = SessionLifecycle::DiagnosticClosing;
                Ok(s.lifecycle)
            }
            SessionLifecycle::DiagnosticClosing | SessionLifecycle::DiagnosticClosed => {
                Ok(s.lifecycle)
            }
            _ => Err(AuthorityError::WrongLifecycle),
        }
    }
    pub fn diagnostic_closed(&self, turn: &mut SessionTurn) -> Result<(), AuthorityError> {
        self.synchronize_obligations(turn)?;
        let mut s = self.state.borrow_mut();
        if s.lifecycle != SessionLifecycle::DiagnosticClosing {
            return Err(AuthorityError::WrongLifecycle);
        }
        if s.storage_stopped.is_none()
            && (s.work.iter().any(|c| c.blocks_marker()) || s.marker == MarkerState::Pending)
        {
            return Err(AuthorityError::NotQuiescent);
        }
        s.lifecycle = SessionLifecycle::DiagnosticClosed;
        Ok(())
    }

    pub fn register_publication_guard(
        &self,
        turn: &mut SessionTurn,
        guard: OwnerBoundPublicationGuard,
    ) -> Result<(), AuthorityError> {
        self.validate_turn(turn)?;
        if !Weak::ptr_eq(&Rc::downgrade(&self.state), &guard.authority) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        self.synchronize_obligations(turn)?;
        let mut s = self.state.borrow_mut();
        if s.guard.is_some() {
            return Err(AuthorityError::AlreadyRegistered);
        }
        s.guard = Some(guard);
        Ok(())
    }
    pub fn apply_publication_step(
        &self,
        turn: &mut SessionTurn,
        step: CanonicalRecordedStep,
    ) -> Result<(), AuthorityError> {
        self.validate_turn(turn)?;
        if !self.same_authority(&step.authority) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        self.synchronize_obligations(turn)?;
        let mut guard = self
            .state
            .borrow_mut()
            .guard
            .take()
            .ok_or(AuthorityError::PublicationUnavailable)?;
        let result = guard.apply_recorded_step(turn, step);
        self.state.borrow_mut().guard = Some(guard);
        result
    }
    pub fn publish<E>(
        &self,
        turn: &mut SessionTurn,
        candidate: PublicationCandidate,
        fence: PublicationFence,
        consumer: impl FnOnce(PublicationView) -> Result<(), E>,
    ) -> PublicationReport<E> {
        if let Err(e) = self.validate_turn(turn) {
            return PublicationReport::Denied(e);
        }
        if !self.same_authority(&candidate.authority) || !self.same_authority(&fence.authority) {
            return PublicationReport::Denied(AuthorityError::AuthorityMismatch);
        }
        if let Err(e) = self.synchronize_obligations(turn) {
            return PublicationReport::Denied(e);
        }
        let s = self.state.borrow();
        if s.failed {
            return PublicationReport::Denied(AuthorityError::ArchiveFailed);
        }
        if s.lifecycle != SessionLifecycle::Open {
            return PublicationReport::Denied(AuthorityError::SessionClosing);
        }
        let Some(guard) = s.guard.as_ref() else {
            return PublicationReport::Denied(AuthorityError::PublicationUnavailable);
        };
        if guard.revoked {
            return PublicationReport::Denied(AuthorityError::CandidateRevoked);
        }
        let Some(current) = guard.state.as_ref() else {
            return PublicationReport::Denied(AuthorityError::PublicationUnavailable);
        };
        if !Weak::ptr_eq(&candidate.work.authority, &Rc::downgrade(&self.state))
            || fence
                .work
                .as_ref()
                .is_some_and(|work| !Weak::ptr_eq(&work.authority, &Rc::downgrade(&self.state)))
        {
            return PublicationReport::Denied(AuthorityError::AuthorityMismatch);
        }
        if guard
            .candidate_work
            .as_ref()
            .is_none_or(|work| work.id() != candidate.work.id())
            || fence
                .work
                .as_ref()
                .is_none_or(|work| work.id() != candidate.work.id())
        {
            return PublicationReport::Denied(AuthorityError::CandidateRevoked);
        }
        if let Err(error) = publication_relation(current, &candidate.state, &fence) {
            return PublicationReport::Denied(error);
        }
        let view = PublicationView {
            binding: s.binding,
            candidate_identity: candidate.state.identity.creation_record.get(),
        };
        drop(s);
        let result = consumer(view);
        if let Some(guard) = self.state.borrow_mut().guard.as_mut() {
            if let Some(state) = guard.state.as_mut() {
                state.current = None;
            }
            guard.candidate_work.take();
        }
        match result {
            Ok(()) => PublicationReport::Published,
            Err(error) => PublicationReport::EffectFailed(error),
        }
    }
}

#[cfg(test)]
mod conformance {
    use super::*;
    use crate::event::InputContext;
    use crate::policy::WatermarkKind;
    use crate::record::{ControlRecord, Reason, RecordingEvidence, WireContext};

    #[test]
    fn backend_memory_profile_components_and_aggregate_reject_before_mutation() {
        struct MemoryWriter(StorageMemoryProfile);
        impl SessionRecordWriter for MemoryWriter {
            fn memory_profile(&self) -> StorageMemoryProfile {
                self.0
            }
            fn persist(
                &mut self,
                _: &RecordFrame,
                _: RecordingGate,
            ) -> Result<PersistenceReceipt, PersistError> {
                Err(PersistError::new("unused memory conformance writer"))
            }
        }
        let (authority, mut turn, _) = session(10);
        let original = StorageMemoryProfile {
            metadata_backing_bytes: 3,
            metadata_ceiling_bytes: 7,
            workspace_backing_bytes: 11,
            workspace_ceiling_bytes: 17,
            backend_backing_bytes: 19,
            backend_ceiling_bytes: 23,
        };
        authority
            .set_storage_memory_profile(&mut turn, original)
            .unwrap();
        for component in 0..6 {
            let mut invalid = original;
            match component {
                0 => invalid.metadata_backing_bytes = usize::MAX,
                1 => invalid.metadata_ceiling_bytes = usize::MAX,
                2 => invalid.workspace_backing_bytes = usize::MAX,
                3 => invalid.workspace_ceiling_bytes = usize::MAX,
                4 => invalid.backend_backing_bytes = usize::MAX,
                _ => invalid.backend_ceiling_bytes = usize::MAX,
            }
            assert_eq!(
                authority.set_storage_memory_profile(&mut turn, invalid),
                Err(AuthorityError::InvalidBudget)
            );
            assert_eq!(authority.ownership_report().storage_memory, original);
            assert_eq!(
                authority
                    .bind_sink_with_memory_authority(&mut turn, Box::new(MemoryWriter(invalid)))
                    .err(),
                Some(AuthorityError::InvalidBudget)
            );
            assert_eq!(authority.ownership_report().storage_memory, original);
        }
        // Each component fits individually; their aggregate and the reserved
        // report headroom still reject before consuming the first binding.
        let overflowing = StorageMemoryProfile {
            metadata_backing_bytes: usize::MAX / 2,
            metadata_ceiling_bytes: usize::MAX / 2,
            workspace_backing_bytes: usize::MAX / 2,
            workspace_ceiling_bytes: usize::MAX / 2,
            backend_backing_bytes: 2,
            backend_ceiling_bytes: 2,
        };
        assert_eq!(
            authority.set_storage_memory_profile(&mut turn, overflowing),
            Err(AuthorityError::InvalidBudget)
        );
        assert_eq!(authority.ownership_report().storage_memory, original);
        struct UnvalidatedWriter;
        impl SessionRecordWriter for UnvalidatedWriter {
            fn checked_memory_profile(&self) -> Result<StorageMemoryProfile, AuthorityError> {
                Ok(StorageMemoryProfile {
                    metadata_ceiling_bytes: usize::MAX,
                    ..StorageMemoryProfile::default()
                })
            }
            fn persist(
                &mut self,
                _: &RecordFrame,
                _: RecordingGate,
            ) -> Result<PersistenceReceipt, PersistError> {
                Err(PersistError::new("unused memory conformance writer"))
            }
        }
        assert_eq!(
            authority
                .bind_sink_with_memory_authority(&mut turn, Box::new(UnvalidatedWriter))
                .err(),
            Some(AuthorityError::InvalidBudget)
        );
        assert_eq!(authority.ownership_report().storage_memory, original);
        let (_sink, mut token) = authority
            .bind_sink_with_memory_authority(&mut turn, Box::new(MemoryWriter(original)))
            .unwrap();
        assert_eq!(
            authority.set_storage_memory_profile(&mut turn, StorageMemoryProfile::default()),
            Err(AuthorityError::StorageProfileBound)
        );
        assert_eq!(
            token.update(&mut turn, overflowing),
            Err(AuthorityError::InvalidBudget)
        );
        let (_, mut foreign_turn, _) = session(10);
        assert_eq!(
            token.update(&mut foreign_turn, StorageMemoryProfile::default()),
            Err(AuthorityError::AuthorityMismatch)
        );
        assert_eq!(authority.ownership_report().storage_memory, original);
        let closed = StorageMemoryProfile {
            backend_backing_bytes: 0,
            ..original
        };
        token.update(&mut turn, closed).unwrap();
        assert_eq!(authority.ownership_report().storage_memory, closed);
        assert_eq!(
            authority
                .bind_sink_with_memory_authority(&mut turn, Box::new(MemoryWriter(original)))
                .err(),
            Some(AuthorityError::SinkAlreadyBound)
        );
    }

    #[test]
    fn backend_memory_preflight_rejects_overflow_before_record_or_cut_mutation() {
        struct MutableMemoryWriter {
            profile: Rc<Cell<StorageMemoryProfile>>,
            calls: Rc<Cell<usize>>,
            change_on_write: Rc<Cell<bool>>,
        }
        impl SessionRecordWriter for MutableMemoryWriter {
            fn checked_memory_profile(&self) -> Result<StorageMemoryProfile, AuthorityError> {
                Ok(self.profile.get())
            }
            fn persist(
                &mut self,
                frame: &RecordFrame,
                gate: RecordingGate,
            ) -> Result<PersistenceReceipt, PersistError> {
                self.calls.set(self.calls.get() + 1);
                if self.change_on_write.get() {
                    self.profile.set(StorageMemoryProfile {
                        workspace_ceiling_bytes: usize::MAX,
                        ..StorageMemoryProfile::default()
                    });
                }
                Ok(PersistenceReceipt {
                    through: frame.record_no,
                    achieved_gate: gate,
                })
            }
        }
        let (authority, mut turn, _) = session(10);
        let profile = Rc::new(Cell::new(StorageMemoryProfile::default()));
        let calls = Rc::new(Cell::new(0));
        let change_on_write = Rc::new(Cell::new(false));
        let mut sink = authority
            .bind_sink(
                &mut turn,
                Box::new(MutableMemoryWriter {
                    profile: Rc::clone(&profile),
                    calls: Rc::clone(&calls),
                    change_on_write: Rc::clone(&change_on_write),
                }),
            )
            .unwrap();
        let original_status = authority.status();
        let original_prefix = authority.prefix().unwrap();
        let original_memory = authority.ownership_report().storage_memory;
        profile.set(StorageMemoryProfile {
            workspace_ceiling_bytes: usize::MAX,
            ..StorageMemoryProfile::default()
        });
        assert_eq!(
            sink.persist_marker(&mut turn, &marker(10), RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBudget
            ))
        );
        assert_eq!(calls.get(), 0);
        assert_eq!(authority.status(), original_status);
        assert_eq!(authority.prefix().unwrap(), original_prefix);
        assert_eq!(authority.ownership_report().storage_memory, original_memory);
        profile.set(StorageMemoryProfile::default());
        change_on_write.set(true);
        assert_eq!(
            sink.persist_marker(&mut turn, &marker(10), RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBudget
            ))
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(authority.prefix().unwrap(), original_prefix);
        assert_eq!(authority.ownership_report().storage_memory, original_memory);
        let stopped = authority.status();
        assert!(stopped.failed);
        assert_eq!(
            stopped.storage_stopped,
            Some(PersistError::typed(
                PersistErrorKind::Validation,
                "backend memory profile"
            ))
        );
        assert_eq!(
            stopped.marker,
            MarkerState::Unconfirmed(PersistError::typed(
                PersistErrorKind::Validation,
                "backend memory profile"
            ))
        );
        assert_eq!(
            sink.persist_marker(&mut turn, &marker(10), RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::StorageStopped
            ))
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(authority.status(), stopped);
    }

    fn session(
        next: u64,
    ) -> (
        CaptureSessionAuthority,
        SessionTurn,
        SupervisorSessionHandle,
    ) {
        let (authority, mut turn) = CaptureSessionAuthority::new(SessionBinding {
            archive: ArchiveId::new([1; 16]).unwrap(),
            session: CaptureSessionId::new([2; 16]).unwrap(),
            clock: ClockId::new(1).unwrap(),
        });
        let handle = authority
            .register_supervisor(
                &mut turn,
                &[ScopeBinding {
                    stream: StreamId::new(1).unwrap(),
                    connection: ConnectionId::new(1).unwrap(),
                    epoch: ConnectionEpoch::new(1).unwrap(),
                }],
                RetentionBudget {
                    item_cap: 5,
                    raw_frame_limit: 1,
                    raw_byte_limit: 64,
                    max_message_bytes: 64,
                },
                PrefixBinding {
                    context: ActiveContext {
                        config: ConfigVersion::new(1).unwrap(),
                        normalizer: NormalizerVersion::new(1).unwrap(),
                    },
                    recording_gate: RecordingGate::Durable,
                    segment: SegmentNo::new(0),
                    next_record: RecordNo::new(next).unwrap(),
                },
            )
            .unwrap();
        (authority, turn, handle)
    }

    struct Writer {
        calls: Rc<Cell<usize>>,
        mode: u8,
    }
    impl SessionRecordWriter for Writer {
        fn persist(
            &mut self,
            frame: &RecordFrame,
            _gate: RecordingGate,
        ) -> Result<PersistenceReceipt, PersistError> {
            self.calls.set(self.calls.get() + 1);
            match self.mode {
                1 => Err(PersistError::typed(
                    PersistErrorKind::Io,
                    "injected marker error",
                )),
                2 => Ok(PersistenceReceipt {
                    through: RecordNo::new(frame.record_no.get() + 1).unwrap(),
                    achieved_gate: RecordingGate::Durable,
                }),
                3 => Ok(PersistenceReceipt {
                    through: frame.record_no,
                    achieved_gate: RecordingGate::Flushed,
                }),
                _ => Ok(PersistenceReceipt {
                    through: frame.record_no,
                    achieved_gate: RecordingGate::Durable,
                }),
            }
        }
    }
    fn marker(number: u64) -> RecordFrame {
        RecordFrame {
            record_no: RecordNo::new(number).unwrap(),
            segment_no: SegmentNo::new(0),
            value: Record::Control(ControlRecord {
                context: WireContext {
                    unix_ns: LocalUnixNs::new(1),
                    monotonic_ns: MonotonicNs::new(2),
                    context: InputContext::Active(ActiveContext {
                        config: ConfigVersion::new(1).unwrap(),
                        normalizer: NormalizerVersion::new(1).unwrap(),
                    }),
                },
                value: Control::Recording(RecordingEvidence {
                    health: RecordingHealth::Failed,
                    kind: WatermarkKind::Durable,
                    through: RecordNo::new(number - 1).ok(),
                    reason: Reason::QueueOverflow,
                }),
            }),
        }
    }

    #[test]
    fn marker_confirmation_requires_successful_bound_gate_not_plain_record_number() {
        let (authority, mut turn, _) = session(10);
        {
            let mut state = authority.state.borrow_mut();
            CaptureSessionAuthority::latch(&mut state);
        }
        assert_eq!(
            authority
                .marker_confirmed(&mut turn, RecordNo::new(10).unwrap())
                .unwrap_err(),
            AuthorityError::InvalidBinding
        );
        let calls = Rc::new(Cell::new(0));
        let mut sink = authority
            .bind_sink(
                &mut turn,
                Box::new(Writer {
                    calls: Rc::clone(&calls),
                    mode: 0,
                }),
            )
            .unwrap();
        sink.persist(&mut turn, &marker(10), RecordingGate::Durable)
            .unwrap();
        authority
            .marker_confirmed(&mut turn, RecordNo::new(10).unwrap())
            .unwrap();
        assert_eq!(
            authority.status().marker,
            MarkerState::Confirmed(RecordNo::new(10).unwrap())
        );
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn marker_error_mismatch_and_weak_gate_permanently_stop_storage() {
        for mode in 1..=3 {
            let (authority, mut turn, _) = session(10);
            let calls = Rc::new(Cell::new(0));
            let mut sink = authority
                .bind_sink(
                    &mut turn,
                    Box::new(Writer {
                        calls: Rc::clone(&calls),
                        mode,
                    }),
                )
                .unwrap();
            assert!(
                sink.persist(&mut turn, &marker(10), RecordingGate::Durable)
                    .is_err()
            );
            assert!(authority.status().failed);
            assert!(matches!(
                authority.status().marker,
                MarkerState::Unconfirmed(_)
            ));
            assert!(matches!(
                sink.persist(&mut turn, &marker(10), RecordingGate::Durable),
                Err(PersistBoundaryError::Authority(
                    AuthorityError::StorageStopped
                ))
            ));
            assert_eq!(calls.get(), 1);
        }
    }

    #[test]
    fn record_no_and_admission_order_do_not_wrap_or_write_on_exhaustion() {
        let (authority, mut turn, _) = session(u64::MAX);
        let calls = Rc::new(Cell::new(0));
        let mut sink = authority
            .bind_sink(
                &mut turn,
                Box::new(Writer {
                    calls: Rc::clone(&calls),
                    mode: 0,
                }),
            )
            .unwrap();
        assert_eq!(
            sink.persist(&mut turn, &marker(u64::MAX), RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::CounterExhausted("RecordNo")
            ))
        );
        assert_eq!(calls.get(), 0);
        let (authority, mut turn, handle) = session(10);
        authority.state.borrow_mut().sequence = u64::MAX;
        assert_eq!(
            handle
                .reserve_work(&mut turn, WorkKind::QueuedObservation)
                .unwrap_err(),
            AuthorityError::CounterExhausted("AdmissionOrder")
        );
        assert_eq!(authority.ownership_report().work_used, 0);
    }

    fn accepted_binding() -> StreamBinding {
        StreamBinding {
            id: StreamId::new(1).unwrap(),
            instrument_slot: InstrumentSlot::new(1).unwrap(),
            spec: SpecRef {
                instrument: InstrumentRef {
                    venue: Token::new("bitget").unwrap(),
                    market: MarketKind::Perpetual,
                    product_namespace: Token::new("usdt-futures").unwrap(),
                    native_symbol: Token::new("BTCUSDT").unwrap(),
                },
                version: SpecVersion::new(1).unwrap(),
            },
            connection_id: ConnectionId::new(1).unwrap(),
            channel: Channel::BookNormal,
            book_id: Some(BookId::new(1).unwrap()),
            tag: EpochTag {
                spec: SpecVersion::new(1).unwrap(),
                connection: ConnectionEpoch::new(1).unwrap(),
                subscription: SubscriptionEpoch::new(1).unwrap(),
                book: Some(BookEpoch::new(1).unwrap()),
            },
            feed_profile: FeedProfileVersion::new(1).unwrap(),
        }
    }
    fn canonical(binding: SessionBinding) -> CanonicalGuardState {
        let scope = CanonicalScope {
            binding,
            stream: StreamId::new(1).unwrap(),
            slot: InstrumentSlot::new(1).unwrap(),
            tag: accepted_binding().tag,
            context: ActiveContext {
                config: ConfigVersion::new(1).unwrap(),
                normalizer: NormalizerVersion::new(1).unwrap(),
            },
            profile: FeedProfileVersion::new(1).unwrap(),
            barrier: 2,
        };
        let candidate = CandidateState {
            identity: CandidateIdentity {
                archive: binding.archive,
                creation_record: RecordNo::new(8).unwrap(),
                stream: scope.stream,
            },
            causal_frontier: RecordNo::new(8).unwrap(),
            available_at: RecordNo::new(8).unwrap(),
            revoked: false,
            projection: CanonicalProjection {
                scope,
                gate: RecordingGate::Durable,
                anchor: RecordNo::new(3).unwrap(),
                witness_record: RecordNo::new(4).unwrap(),
                effects: [None; 4],
                freshness: Freshness::Fresh,
                observed_recording: RecordingHealth::Healthy,
                last_receipt: RecordNo::new(8).unwrap(),
                evaluation_ns: 10,
            },
        };
        CanonicalGuardState {
            scope,
            phase: CanonicalPhase::Running,
            transport: Transport::Up,
            book: BookUsability::Usable,
            anchor: Some(RecordNo::new(3).unwrap()),
            witness: Some(VerificationWitness {
                scope,
                anchor: RecordNo::new(3).unwrap(),
                record: RecordNo::new(4).unwrap(),
            }),
            freshness: Freshness::Fresh,
            allow_quiet_with_proof: false,
            quiet: None,
            evaluation_ns: 10,
            observed_recording: RecordingHealth::Healthy,
            mode: DurabilityMode::SyncBeforePublish,
            recording_gate: RecordingGate::Durable,
            current: Some(candidate),
        }
    }
    fn fence(authority: &CaptureSessionAuthority) -> PublicationFence {
        PublicationFence {
            authority: authority.clone(),
            work: authority
                .state
                .borrow()
                .guard
                .as_ref()
                .and_then(|guard| guard.candidate_work.as_ref())
                .map(|work| work.share().unwrap()),
            binding: authority.binding(),
            through: Some(RecordNo::new(9).unwrap()),
            achieved: Some(RecordNo::new(9).unwrap()),
            accepted: Some(RecordNo::new(10).unwrap()),
            previous: Some(RecordNo::new(8).unwrap()),
            gate: RecordingGate::Durable,
            evidence: CompletionEvidence::TrustedContinuous,
        }
    }
    fn candidate(
        authority: &CaptureSessionAuthority,
        turn: &mut SessionTurn,
        state: CandidateState,
    ) -> PublicationCandidate {
        let work = authority
            .state
            .borrow()
            .guard
            .as_ref()
            .and_then(|guard| guard.candidate_work.as_ref())
            .map(WorkOwner::share);
        let work = match work {
            Some(work) => work.unwrap(),
            None => authority.reserve_work(turn, WorkKind::Candidate).unwrap(),
        };
        PublicationCandidate {
            authority: authority.clone(),
            state,
            work,
        }
    }
    fn install(
        authority: &CaptureSessionAuthority,
        turn: &mut SessionTurn,
        state: CanonicalGuardState,
    ) {
        authority
            .set_accepted_stream_bindings(turn, &[accepted_binding()])
            .unwrap();
        let mut guard = OwnerBoundPublicationGuard {
            authority: Rc::downgrade(&authority.state),
            candidate_work: None,
            state: None,
            last_applied: None,
            revoked: false,
        };
        guard
            .apply_recorded_step(
                turn,
                CanonicalRecordedStep {
                    authority: authority.clone(),
                    recorded_at: RecordNo::new(9).unwrap(),
                    state,
                },
            )
            .unwrap();
        authority.register_publication_guard(turn, guard).unwrap();
    }

    #[test]
    fn canonical_guard_requires_full_current_projection_and_usable_health() {
        let (authority, _, _) = session(10);
        let base = canonical(authority.binding());
        let requested = base.current.unwrap();
        let fence = fence(&authority);
        assert_eq!(publication_relation(&base, &requested, &fence), Ok(()));
        let mut changed = base;
        changed.phase = CanonicalPhase::Blocked;
        assert_eq!(
            publication_relation(&changed, &requested, &fence),
            Err(AuthorityError::CanonicalBlocked)
        );
        let mut old = requested;
        old.projection.scope.slot = InstrumentSlot::new(2).unwrap();
        assert_eq!(
            publication_relation(&base, &old, &fence),
            Err(AuthorityError::CandidateRevoked)
        );
        let mut old = requested;
        old.projection.scope.tag.subscription = SubscriptionEpoch::new(2).unwrap();
        assert_eq!(
            publication_relation(&base, &old, &fence),
            Err(AuthorityError::CandidateRevoked)
        );
        let mut old = requested;
        old.projection.scope.context.config = ConfigVersion::new(2).unwrap();
        assert_eq!(
            publication_relation(&base, &old, &fence),
            Err(AuthorityError::CandidateRevoked)
        );
        let mut old = requested;
        old.projection.scope.profile = FeedProfileVersion::new(2).unwrap();
        assert_eq!(
            publication_relation(&base, &old, &fence),
            Err(AuthorityError::CandidateRevoked)
        );
        let mut old = requested;
        old.revoked = true;
        assert_eq!(
            publication_relation(&base, &old, &fence),
            Err(AuthorityError::CandidateRevoked)
        );
        for missing in 0..6 {
            let mut state = base;
            match missing {
                0 => state.transport = Transport::Down,
                1 => state.book = BookUsability::Unknown,
                2 => state.anchor = None,
                3 => state.witness = None,
                4 => state.witness.as_mut().unwrap().anchor = RecordNo::new(2).unwrap(),
                _ => state.scope.barrier = 3,
            }
            let error = publication_relation(&state, &requested, &fence).unwrap_err();
            assert!(matches!(
                error,
                AuthorityError::DataNotUsable | AuthorityError::CandidateRevoked
            ));
        }
        let mut state = base;
        state.observed_recording = RecordingHealth::Degraded;
        assert_eq!(
            publication_relation(&state, &requested, &fence),
            Err(AuthorityError::RecordingNotHealthy)
        );
    }

    #[test]
    fn quiet_proof_expiry_scope_and_mode_gate_are_checked_at_use() {
        let (authority, _, _) = session(10);
        let mut state = canonical(authority.binding());
        let fence = fence(&authority);
        state.freshness = Freshness::QuietVerified;
        state.current.as_mut().unwrap().projection.freshness = Freshness::QuietVerified;
        let requested = state.current.unwrap();
        assert_eq!(
            publication_relation(&state, &requested, &fence),
            Err(AuthorityError::DataNotUsable)
        );
        state.allow_quiet_with_proof = true;
        state.quiet = Some(QuietWitness {
            scope: state.scope,
            expires_ns: 11,
        });
        assert_eq!(publication_relation(&state, &requested, &fence), Ok(()));
        state.quiet.as_mut().unwrap().expires_ns = 10;
        assert_eq!(
            publication_relation(&state, &requested, &fence),
            Err(AuthorityError::DataNotUsable)
        );
        state.quiet.as_mut().unwrap().expires_ns = 11;
        state.quiet.as_mut().unwrap().scope.barrier += 1;
        assert_eq!(
            publication_relation(&state, &requested, &fence),
            Err(AuthorityError::DataNotUsable)
        );
        let mut state = canonical(authority.binding());
        state.recording_gate = RecordingGate::Written;
        state.current.as_mut().unwrap().projection.gate = RecordingGate::Written;
        assert_eq!(
            publication_relation(&state, &state.current.unwrap(), &fence),
            Err(AuthorityError::GateConfiguration)
        );
        state.mode = DurabilityMode::Buffered;
        assert_eq!(
            publication_relation(&state, &state.current.unwrap(), &fence),
            Ok(())
        );
    }

    #[test]
    fn fence_requires_authenticated_continuous_scoped_nonregressing_causal_prefix() {
        let (authority, _, _) = session(10);
        let state = canonical(authority.binding());
        let requested = state.current.unwrap();
        for case in 0..9 {
            let mut fence = fence(&authority);
            let expected = match case {
                0 => {
                    fence.binding.session = CaptureSessionId::new([3; 16]).unwrap();
                    AuthorityError::FenceScopeMismatch
                }
                1 => {
                    fence.gate = RecordingGate::Flushed;
                    AuthorityError::FenceInsufficient
                }
                2 => {
                    fence.through = None;
                    AuthorityError::FenceMissing
                }
                3 => {
                    fence.achieved = None;
                    AuthorityError::FenceMissing
                }
                4 => {
                    fence.evidence = CompletionEvidence::Untrusted;
                    AuthorityError::FenceUntrusted
                }
                5 => {
                    fence.evidence = CompletionEvidence::Discontinuous;
                    AuthorityError::FenceUntrusted
                }
                6 => {
                    fence.previous = Some(RecordNo::new(10).unwrap());
                    AuthorityError::WatermarkRegression
                }
                7 => {
                    fence.accepted = Some(RecordNo::new(8).unwrap());
                    AuthorityError::FenceBeyondAchieved
                }
                _ => {
                    fence.through = Some(RecordNo::new(7).unwrap());
                    fence.previous = None;
                    AuthorityError::FenceInsufficient
                }
            };
            assert_eq!(
                publication_relation(&state, &requested, &fence),
                Err(expected)
            );
        }
        let mut state = state;
        state.current.as_mut().unwrap().causal_frontier = RecordNo::new(7).unwrap();
        assert_eq!(
            publication_relation(&state, &state.current.unwrap(), &fence(&authority)),
            Err(AuthorityError::FenceInsufficient)
        );
    }

    #[test]
    fn sealed_guard_producer_and_one_use_publication_are_authority_bound() {
        let (authority, mut turn, _) = session(10);
        let state = canonical(authority.binding());
        let absent = candidate(&authority, &mut turn, state.current.unwrap());
        assert!(matches!(
            authority.publish(&mut turn, absent, fence(&authority), |_| Ok::<_, ()>(())),
            PublicationReport::Denied(AuthorityError::PublicationUnavailable)
        ));
        install(&authority, &mut turn, state);
        assert_eq!(authority.ownership_report().work_used, 1);
        let requested = candidate(&authority, &mut turn, state.current.unwrap());
        assert!(matches!(
            authority.publish(&mut turn, requested, fence(&authority), |_| Ok::<_, ()>(())),
            PublicationReport::Published
        ));
        assert_eq!(authority.ownership_report().work_used, 0);
        let duplicate = candidate(&authority, &mut turn, state.current.unwrap());
        assert!(matches!(
            authority.publish(
                &mut turn,
                duplicate,
                fence(&authority),
                |_| -> Result<(), ()> { panic!("duplicate publication") }
            ),
            PublicationReport::Denied(AuthorityError::CandidateRevoked)
        ));
    }

    #[test]
    fn failure_revokes_waiting_guard_candidate_even_after_late_stronger_fence() {
        let (authority, mut turn, _) = session(10);
        let state = canonical(authority.binding());
        install(&authority, &mut turn, state);
        let candidate = candidate(&authority, &mut turn, state.current.unwrap());
        authority
            .storage_stopped(
                &mut turn,
                PersistError::typed(PersistErrorKind::Io, "failure before publication"),
            )
            .unwrap();
        let mut delayed = fence(&authority);
        delayed.through = Some(RecordNo::new(20).unwrap());
        delayed.achieved = delayed.through;
        delayed.accepted = delayed.through;
        assert!(matches!(
            authority.publish(&mut turn, candidate, delayed, |_| -> Result<(), ()> {
                panic!("publication after failure")
            }),
            PublicationReport::Denied(AuthorityError::ArchiveFailed)
        ));
        assert_eq!(authority.ownership_report().work_used, 0);
    }

    #[test]
    fn foreign_recorded_step_and_closure_cannot_mutate_rightful_ownership() {
        let (authority, mut turn, _) = session(10);
        authority
            .set_accepted_stream_bindings(&mut turn, &[accepted_binding()])
            .unwrap();
        let (foreign, _, _) = session(10);
        let mut guard = OwnerBoundPublicationGuard {
            authority: Rc::downgrade(&authority.state),
            candidate_work: None,
            state: None,
            last_applied: None,
            revoked: false,
        };
        let step = CanonicalRecordedStep {
            authority: foreign.clone(),
            recorded_at: RecordNo::new(9).unwrap(),
            state: canonical(foreign.binding()),
        };
        assert_eq!(
            guard.apply_recorded_step(&mut turn, step).unwrap_err(),
            AuthorityError::AuthorityMismatch
        );
        assert!(guard.state.is_none());
        let owner = authority
            .mandatory_close(
                &mut turn,
                StreamId::new(1).unwrap(),
                ConnectionEpoch::new(1).unwrap(),
                None,
            )
            .unwrap();
        let lease = match authority.reclaim_close(&mut turn, owner.clone()) {
            CloseLeaseReport::Leased(lease) => lease,
            _ => panic!("lease"),
        };
        let fake = AuthenticatedClosure {
            authority: foreign,
            connection: owner.connection(),
            epoch: owner.epoch(),
        };
        assert_eq!(
            authority.confirm_closed(&mut turn, owner.clone(), fake),
            CloseSettlementReport::Rejected(AuthorityError::InvalidOwner)
        );
        let valid = AuthenticatedClosure {
            authority: authority.clone(),
            connection: owner.connection(),
            epoch: owner.epoch(),
        };
        assert_eq!(
            authority.confirm_closed(&mut turn, owner.clone(), valid),
            CloseSettlementReport::Settled
        );
        assert!(matches!(
            authority.dispatch(&mut turn, lease.into_command(), |_| -> Result<(), ()> {
                panic!("settled Close effect")
            }),
            DispatchReport::AlreadySettled
        ));
        assert!(matches!(
            authority.reclaim_close(&mut turn, owner),
            CloseLeaseReport::AlreadySettled
        ));
    }

    #[test]
    fn invalid_failure_dto_is_transactional_but_valid_marker_cannot_cross_pre_cut_records() {
        let (authority, mut turn, handle) = session(10);
        let calls = Rc::new(Cell::new(0));
        let mut sink = authority
            .bind_sink(
                &mut turn,
                Box::new(Writer {
                    calls: Rc::clone(&calls),
                    mode: 0,
                }),
            )
            .unwrap();
        let mut invalid = marker(10);
        if let Record::Control(record) = &mut invalid.value
            && let Control::Recording(evidence) = &mut record.value
        {
            evidence.reason = Reason::NoFault;
        }
        assert!(
            sink.persist_marker(&mut turn, &invalid, RecordingGate::Durable)
                .is_err()
        );
        assert!(!authority.status().failed);
        assert_eq!(calls.get(), 0);
        let mut invalid = marker(10);
        if let Record::Control(record) = &mut invalid.value
            && let Control::Recording(evidence) = &mut record.value
        {
            evidence.through = Some(RecordNo::new(10).unwrap());
        }
        assert!(
            sink.persist_marker(&mut turn, &invalid, RecordingGate::Durable)
                .is_err()
        );
        assert!(!authority.status().failed);
        assert_eq!(calls.get(), 0);
        let owner = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        assert_eq!(
            sink.persist_marker(&mut turn, &marker(10), RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::NotQuiescent
            ))
        );
        assert!(authority.status().failed);
        assert_eq!(owner.cut_side(), CutSide::PreCut);
        assert_eq!(calls.get(), 0);
        drop(owner);
        sink.persist_marker(&mut turn, &marker(10), RecordingGate::Durable)
            .unwrap();
        assert_eq!(calls.get(), 1);
    }
    #[test]
    fn canonical_steps_cannot_roll_back_or_change_unaccepted_full_epoch_scope() {
        let (authority, mut turn, _) = session(10);
        authority
            .set_accepted_stream_bindings(&mut turn, &[accepted_binding()])
            .unwrap();
        let valid = canonical(authority.binding());
        let mut guard = OwnerBoundPublicationGuard {
            authority: Rc::downgrade(&authority.state),
            candidate_work: None,
            state: None,
            last_applied: None,
            revoked: false,
        };
        guard
            .apply_recorded_step(
                &mut turn,
                CanonicalRecordedStep {
                    authority: authority.clone(),
                    recorded_at: RecordNo::new(8).unwrap(),
                    state: valid,
                },
            )
            .unwrap();
        let mut blocked = valid;
        blocked.phase = CanonicalPhase::Blocked;
        blocked.current = None;
        guard
            .apply_recorded_step(
                &mut turn,
                CanonicalRecordedStep {
                    authority: authority.clone(),
                    recorded_at: RecordNo::new(9).unwrap(),
                    state: blocked,
                },
            )
            .unwrap();
        let stale = CanonicalRecordedStep {
            authority: authority.clone(),
            recorded_at: RecordNo::new(8).unwrap(),
            state: valid,
        };
        assert_eq!(
            guard.apply_recorded_step(&mut turn, stale).unwrap_err(),
            AuthorityError::StaleCanonicalStep
        );
        assert_eq!(guard.state, Some(blocked));
        assert_eq!(authority.ownership_report().work_used, 0);
        let mut guard = OwnerBoundPublicationGuard {
            authority: Rc::downgrade(&authority.state),
            candidate_work: None,
            state: None,
            last_applied: None,
            revoked: false,
        };
        let mut wrong = valid;
        wrong.scope.tag.subscription = SubscriptionEpoch::new(2).unwrap();
        assert_eq!(
            guard
                .apply_recorded_step(
                    &mut turn,
                    CanonicalRecordedStep {
                        authority: authority.clone(),
                        recorded_at: RecordNo::new(9).unwrap(),
                        state: wrong
                    }
                )
                .unwrap_err(),
            AuthorityError::InvalidBinding
        );
        assert!(guard.state.is_none());
    }

    #[test]
    fn earlier_stronger_watermark_survives_later_weaker_receipt_and_marker() {
        struct Gates {
            calls: usize,
        }
        impl SessionRecordWriter for Gates {
            fn persist(
                &mut self,
                frame: &RecordFrame,
                _: RecordingGate,
            ) -> Result<PersistenceReceipt, PersistError> {
                self.calls += 1;
                Ok(PersistenceReceipt {
                    through: frame.record_no,
                    achieved_gate: if self.calls == 1 {
                        RecordingGate::Durable
                    } else {
                        RecordingGate::Written
                    },
                })
            }
        }
        let (authority, mut turn, handle) = session(10);
        authority
            .state
            .borrow_mut()
            .prefix
            .as_mut()
            .unwrap()
            .recording_gate = RecordingGate::Written;
        let mut sink = authority
            .bind_sink(&mut turn, Box::new(Gates { calls: 0 }))
            .unwrap();
        for number in 10..=11 {
            let work = handle
                .reserve_work(&mut turn, WorkKind::InFlightObservation)
                .unwrap();
            let frame = RecordFrame {
                record_no: RecordNo::new(number).unwrap(),
                segment_no: SegmentNo::new(0),
                value: Record::Control(ControlRecord {
                    context: WireContext {
                        unix_ns: LocalUnixNs::new(1),
                        monotonic_ns: MonotonicNs::new(2),
                        context: InputContext::Active(handle.prefix().context),
                    },
                    value: Control::Transport {
                        connection: ConnectionId::new(1).unwrap(),
                        epoch: ConnectionEpoch::new(1).unwrap(),
                        value: Transport::Up,
                    },
                }),
            };
            sink.persist_owned(&mut turn, &frame, RecordingGate::Written, &work)
                .unwrap();
            drop(work);
        }
        assert_eq!(
            authority.trusted_watermark(WatermarkKind::Durable),
            Some(RecordNo::new(10).unwrap())
        );
        assert_eq!(
            authority.trusted_watermark(WatermarkKind::Written),
            Some(RecordNo::new(11).unwrap())
        );
        let mut frame = marker(12);
        if let Record::Control(record) = &mut frame.value
            && let Control::Recording(evidence) = &mut record.value
        {
            evidence.through = Some(RecordNo::new(10).unwrap());
        }
        sink.persist_marker(&mut turn, &frame, RecordingGate::Written)
            .unwrap();
        authority
            .marker_confirmed(&mut turn, RecordNo::new(12).unwrap())
            .unwrap();
        assert_eq!(
            authority.trusted_watermark(WatermarkKind::Durable),
            Some(RecordNo::new(10).unwrap())
        );
    }

    #[test]
    fn repeated_stop_preserves_original_typed_storage_and_marker_error() {
        let (authority, mut turn, _) = session(10);
        let original = PersistError::typed(PersistErrorKind::Io, "original IO failure");
        authority.storage_stopped(&mut turn, original).unwrap();
        let later = PersistError::typed(PersistErrorKind::WeakGate, "later close error");
        authority.storage_stopped(&mut turn, later).unwrap();
        assert_eq!(authority.status().storage_stopped, Some(original));
        assert_eq!(
            authority.status().marker,
            MarkerState::Unconfirmed(original)
        );
        authority.begin_diagnostic_close(&mut turn).unwrap();
        authority.storage_stopped(&mut turn, later).unwrap();
        assert_eq!(
            authority.status().marker,
            MarkerState::Unconfirmed(original)
        );
    }
    #[test]
    fn foreign_and_duplicate_proof_authorizations_cannot_consume_rightful_state() {
        fn authorization_state(
            authority: &CaptureSessionAuthority,
        ) -> (TicketState, bool, bool, bool, SessionLifecycle) {
            let state = authority.state.borrow();
            (
                state.ticket,
                state.proof_issued,
                state.proof_consumed,
                state.finalization_authorized,
                state.lifecycle,
            )
        }
        let (authority, mut turn, handle) = session(10);
        let (foreign, mut foreign_turn, foreign_handle) = session(10);
        let ticket = authority.begin_finalization(&mut turn).unwrap();
        let foreign_ticket = foreign.begin_finalization(&mut foreign_turn).unwrap();
        // Private stale authorizations stand in for an internal duplicate;
        // safe Rust callers cannot construct or clone a QuiescenceProof.
        let before = authorization_state(&authority);
        assert_eq!(
            authority
                .consume_proof(
                    &mut turn,
                    &mut QuiescenceProof {
                        authority: authority.clone()
                    }
                )
                .unwrap_err(),
            AuthorityError::InvalidProof
        );
        assert_eq!(authorization_state(&authority), before);
        let mut proof = match handle.quiesce(&mut turn, &ticket) {
            QuiescenceReport::Ready(proof) => proof,
            other => panic!("{other:?}"),
        };
        let mut foreign_proof = match foreign_handle.quiesce(&mut foreign_turn, &foreign_ticket) {
            QuiescenceReport::Ready(proof) => proof,
            other => panic!("{other:?}"),
        };
        let rightful = authorization_state(&authority);
        let foreign_before = authorization_state(&foreign);
        assert_eq!(
            foreign
                .consume_proof(
                    &mut foreign_turn,
                    &mut QuiescenceProof {
                        authority: authority.clone()
                    }
                )
                .unwrap_err(),
            AuthorityError::AuthorityMismatch
        );
        assert_eq!(authorization_state(&authority), rightful);
        assert_eq!(authorization_state(&foreign), foreign_before);
        assert_eq!(
            authority
                .consume_proof(
                    &mut foreign_turn,
                    &mut QuiescenceProof {
                        authority: authority.clone()
                    }
                )
                .unwrap_err(),
            AuthorityError::AuthorityMismatch
        );
        assert_eq!(authorization_state(&authority), rightful);
        authority.consume_proof(&mut turn, &mut proof).unwrap();
        let consumed = authorization_state(&authority);
        assert_eq!(
            authority
                .consume_proof(
                    &mut turn,
                    &mut QuiescenceProof {
                        authority: authority.clone()
                    }
                )
                .unwrap_err(),
            AuthorityError::ProofConsumed
        );
        assert_eq!(authorization_state(&authority), consumed);
        authority.ensure_finalization_authorized().unwrap();
        foreign
            .consume_proof(&mut foreign_turn, &mut foreign_proof)
            .unwrap();
        foreign.ensure_finalization_authorized().unwrap();
        authority.finalization_finished(&mut turn).unwrap();
        assert_eq!(
            authority
                .consume_proof(
                    &mut turn,
                    &mut QuiescenceProof {
                        authority: authority.clone()
                    }
                )
                .unwrap_err(),
            AuthorityError::ProofConsumed
        );
        assert_eq!(authority.status().lifecycle, SessionLifecycle::Finalized);
    }

    #[test]
    fn failure_after_proof_consumption_revokes_seal_authorization_without_reissuance() {
        let (authority, mut turn, handle) = session(10);
        let ticket = authority.begin_finalization(&mut turn).unwrap();
        let mut proof = match handle.quiesce(&mut turn, &ticket) {
            QuiescenceReport::Ready(proof) => proof,
            other => panic!("{other:?}"),
        };
        authority.consume_proof(&mut turn, &mut proof).unwrap();
        authority
            .storage_stopped(
                &mut turn,
                PersistError::typed(PersistErrorKind::Io, "failure before final seal"),
            )
            .unwrap();
        assert_eq!(
            authority.ensure_finalization_authorized().unwrap_err(),
            AuthorityError::ArchiveFailed
        );
        assert_eq!(
            authority
                .consume_proof(
                    &mut turn,
                    &mut QuiescenceProof {
                        authority: authority.clone()
                    }
                )
                .unwrap_err(),
            AuthorityError::ArchiveFailed
        );
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
        ));
        assert_eq!(
            authority.finalization_finished(&mut turn).unwrap_err(),
            AuthorityError::ArchiveFailed
        );
        let state = authority.state.borrow();
        assert_eq!(state.ticket, TicketState::Consumed);
        assert!(state.proof_consumed);
        assert!(!state.finalization_authorized);
        assert_eq!(state.lifecycle, SessionLifecycle::DiagnosticClosing);
    }
    #[test]
    fn settled_owner_with_retained_lease_cannot_transfer_its_work_to_another_scope() {
        let binding = session(10).0.binding();
        let (authority, mut turn) = CaptureSessionAuthority::new(binding);
        let scopes: [ScopeBinding; 3] = std::array::from_fn(|index| ScopeBinding {
            stream: StreamId::new(index as u32 + 1).unwrap(),
            connection: ConnectionId::new(index as u32 + 1).unwrap(),
            epoch: ConnectionEpoch::new(1).unwrap(),
        });
        let handle = authority
            .register_supervisor(
                &mut turn,
                &scopes,
                RetentionBudget {
                    item_cap: 13,
                    raw_frame_limit: 1,
                    raw_byte_limit: 64,
                    max_message_bytes: 64,
                },
                PrefixBinding {
                    context: ActiveContext {
                        config: ConfigVersion::new(1).unwrap(),
                        normalizer: NormalizerVersion::new(1).unwrap(),
                    },
                    recording_gate: RecordingGate::Durable,
                    segment: SegmentNo::new(0),
                    next_record: RecordNo::new(10).unwrap(),
                },
            )
            .unwrap();
        let work = handle
            .reserve_work(&mut turn, WorkKind::PendingPlan)
            .unwrap();
        let owner = authority
            .mandatory_close(&mut turn, scopes[0].stream, scopes[0].epoch, Some(&work))
            .unwrap();
        let lease = match authority.reclaim_close(&mut turn, owner.clone()) {
            CloseLeaseReport::Leased(lease) => lease,
            other => panic!("{other:?}"),
        };
        let evidence = AuthenticatedClosure {
            authority: authority.clone(),
            connection: owner.connection(),
            epoch: owner.epoch(),
        };
        assert_eq!(
            authority.confirm_closed(&mut turn, owner.clone(), evidence),
            CloseSettlementReport::Settled
        );
        let before = authority.ownership_report();
        for scope in &scopes[1..] {
            assert_eq!(
                authority
                    .mandatory_close(&mut turn, scope.stream, scope.epoch, Some(&work))
                    .unwrap_err(),
                AuthorityError::InvalidOwner
            );
            assert_eq!(authority.ownership_report(), before);
        }
        assert!(matches!(
            authority.reclaim_close(&mut turn, owner.clone()),
            CloseLeaseReport::AlreadySettled
        ));
        assert!(matches!(
            authority.dispatch(&mut turn, lease.into_command(), |_| -> Result<(), ()> {
                panic!("settled Close effect")
            }),
            DispatchReport::AlreadySettled
        ));
        assert_eq!(authority.ownership_report().work_used, 1);
    }

    #[test]
    fn exhausted_internal_alias_capacity_rejects_reclaim_without_mutating_pending_close() {
        let (authority, mut turn, handle) = session(10);
        let work = handle
            .reserve_work(&mut turn, WorkKind::PendingPlan)
            .unwrap();
        let owner = authority
            .mandatory_close(
                &mut turn,
                StreamId::new(1).unwrap(),
                ConnectionEpoch::new(1).unwrap(),
                Some(&work),
            )
            .unwrap();
        let original = work.cell.references.get();
        // Fault-inject an impossible extra internal alias. The public boundary
        // still returns a typed outcome instead of panicking or leasing twice.
        work.cell.references.set(MAX_WORK_SHARES + 1);
        let before = authority.ownership_report();
        assert!(matches!(
            authority.reclaim_close(&mut turn, owner.clone()),
            CloseLeaseReport::Rejected(AuthorityError::WorkShareExhausted)
        ));
        assert_eq!(
            authority
                .close_state(owner.stream(), owner.epoch())
                .unwrap(),
            CloseState::Pending
        );
        assert_eq!(authority.ownership_report(), before);
        work.cell.references.set(original);
        let lease = match authority.reclaim_close(&mut turn, owner.clone()) {
            CloseLeaseReport::Leased(lease) => lease,
            other => panic!("{other:?}"),
        };
        assert!(matches!(
            authority.dispatch(&mut turn, lease.into_command(), |_| Ok::<_, ()>(())),
            DispatchReport::Dispatched
        ));
    }
    #[test]
    fn external_failed_observation_owns_cut_and_original_descriptor_until_prefix_drain() {
        let (authority, mut turn, handle) = session(10);
        let work = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        let calls = Rc::new(Cell::new(0));
        let mut sink = authority
            .bind_sink(
                &mut turn,
                Box::new(Writer {
                    calls: Rc::clone(&calls),
                    mode: 0,
                }),
            )
            .unwrap();
        let mut original = marker(10);
        if let Record::Control(record) = &mut original.value {
            record.context.unix_ns = LocalUnixNs::new(41);
            record.context.monotonic_ns = MonotonicNs::new(42);
            if let Control::Recording(evidence) = &mut record.value {
                evidence.reason = Reason::WriteFailure;
            }
        }
        let descriptor = ArchiveFailureObservation {
            context: WireContext {
                unix_ns: LocalUnixNs::new(41),
                monotonic_ns: MonotonicNs::new(42),
                context: InputContext::Active(handle.prefix().context),
            },
            reason: Reason::WriteFailure,
            kind: WatermarkKind::Durable,
        };
        assert_eq!(
            sink.persist_marker(&mut turn, &original, RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::NotQuiescent
            ))
        );
        assert_eq!(authority.archive_failure_observation(), Some(descriptor));
        assert_eq!(handle.archive_failure_observation(), Some(descriptor));
        assert_eq!(authority.status().archive_observation, Some(descriptor));
        assert!(authority.status().failed);
        assert!(authority.status().first_failure.is_none());
        assert_eq!(authority.status().storage_stopped, None);
        assert_eq!(authority.status().marker, MarkerState::Pending);
        assert_eq!(work.cut_side(), CutSide::PreCut);
        assert_eq!(calls.get(), 0);
        let failure = TerminalFailure {
            stream: StreamId::new(1).unwrap(),
            connection: ConnectionId::new(1).unwrap(),
            observed_tag: accepted_binding().tag,
            current_epoch: ConnectionEpoch::new(1).unwrap(),
            context: handle.prefix().context,
            stamp: ReceiveStamp {
                unix_ns: 900,
                monotonic_ns: 901,
            },
            input_class: InputClass::Raw,
            attempt: AttemptIdentity::NoRepresentableSuccessor { frontier: u64::MAX },
            cause: FailureCause::CaptureAttemptExhausted,
        };
        let terminal = handle.terminate(&mut turn, failure).unwrap();
        drop(terminal.close);
        assert_eq!(authority.archive_failure_observation(), Some(descriptor));
        assert_eq!(authority.status().first_failure, Some(failure));
        let before = authority.ownership_report();
        let mut wrong = original.clone();
        if let Record::Control(record) = &mut wrong.value {
            record.context.monotonic_ns = MonotonicNs::new(43);
        }
        assert_eq!(
            sink.persist_marker(&mut turn, &wrong, RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBinding
            ))
        );
        assert_eq!(authority.archive_failure_observation(), Some(descriptor));
        assert_eq!(authority.ownership_report(), before);
        work.set_kind(&mut turn, WorkKind::InFlightObservation)
            .unwrap();
        let prefix = RecordFrame {
            record_no: RecordNo::new(10).unwrap(),
            segment_no: SegmentNo::new(0),
            value: Record::Control(ControlRecord {
                context: WireContext {
                    unix_ns: LocalUnixNs::new(3),
                    monotonic_ns: MonotonicNs::new(4),
                    context: InputContext::Active(handle.prefix().context),
                },
                value: Control::Transport {
                    connection: ConnectionId::new(1).unwrap(),
                    epoch: ConnectionEpoch::new(1).unwrap(),
                    value: Transport::Up,
                },
            }),
        };
        sink.persist_owned(&mut turn, &prefix, RecordingGate::Durable, &work)
            .unwrap();
        drop(work);
        let retry = RecordFrame {
            record_no: RecordNo::new(11).unwrap(),
            segment_no: SegmentNo::new(0),
            value: Record::Control(ControlRecord {
                context: descriptor.context,
                value: Control::Recording(RecordingEvidence {
                    health: RecordingHealth::Failed,
                    kind: descriptor.kind,
                    through: authority.trusted_watermark(descriptor.kind),
                    reason: descriptor.reason,
                }),
            }),
        };
        sink.persist_marker(&mut turn, &retry, RecordingGate::Durable)
            .unwrap();
        assert_eq!(calls.get(), 2);
        assert_eq!(
            authority.status().marker,
            MarkerState::Confirmed(RecordNo::new(11).unwrap())
        );
        assert_eq!(authority.archive_failure_observation(), Some(descriptor));
        authority
            .marker_confirmed(&mut turn, RecordNo::new(11).unwrap())
            .unwrap();
        assert_eq!(
            authority
                .marker_confirmed(&mut turn, RecordNo::new(10).unwrap())
                .unwrap_err(),
            AuthorityError::InvalidBinding
        );
        let duplicate = RecordFrame {
            record_no: RecordNo::new(12).unwrap(),
            segment_no: SegmentNo::new(0),
            value: Record::Control(ControlRecord {
                context: descriptor.context,
                value: Control::Recording(RecordingEvidence {
                    health: RecordingHealth::Failed,
                    kind: descriptor.kind,
                    through: authority.trusted_watermark(descriptor.kind),
                    reason: descriptor.reason,
                }),
            }),
        };
        assert_eq!(
            sink.persist_marker(&mut turn, &duplicate, RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBinding
            ))
        );
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn first_terminal_descriptor_cannot_be_replaced_by_external_failure_observation() {
        let (authority, mut turn, handle) = session(10);
        let failure = TerminalFailure {
            stream: StreamId::new(1).unwrap(),
            connection: ConnectionId::new(1).unwrap(),
            observed_tag: accepted_binding().tag,
            current_epoch: ConnectionEpoch::new(1).unwrap(),
            context: handle.prefix().context,
            stamp: ReceiveStamp {
                unix_ns: 900,
                monotonic_ns: 901,
            },
            input_class: InputClass::Raw,
            attempt: AttemptIdentity::Candidate(CaptureAttemptNo::new(6).unwrap()),
            cause: FailureCause::QueueOverflow,
        };
        let terminal = handle.terminate(&mut turn, failure).unwrap();
        drop(terminal.close);
        let original = authority.archive_failure_observation().unwrap();
        assert_eq!(original.context.unix_ns.get(), 900);
        assert_eq!(original.context.monotonic_ns.get(), 901);
        assert_eq!(original.reason, Reason::QueueOverflow);
        let calls = Rc::new(Cell::new(0));
        let mut sink = authority
            .bind_sink(
                &mut turn,
                Box::new(Writer {
                    calls: Rc::clone(&calls),
                    mode: 0,
                }),
            )
            .unwrap();
        assert_eq!(
            sink.persist_marker(&mut turn, &marker(10), RecordingGate::Durable),
            Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBinding
            ))
        );
        assert_eq!(authority.archive_failure_observation(), Some(original));
        assert_eq!(calls.get(), 0);
        let valid = RecordFrame {
            record_no: RecordNo::new(10).unwrap(),
            segment_no: SegmentNo::new(0),
            value: Record::Control(ControlRecord {
                context: original.context,
                value: Control::Recording(RecordingEvidence {
                    health: RecordingHealth::Failed,
                    kind: original.kind,
                    through: authority.trusted_watermark(original.kind),
                    reason: original.reason,
                }),
            }),
        };
        sink.persist_marker(&mut turn, &valid, RecordingGate::Durable)
            .unwrap();
        assert_eq!(
            authority.status().marker,
            MarkerState::Confirmed(RecordNo::new(10).unwrap())
        );
        assert_eq!(calls.get(), 1);
    }
}
