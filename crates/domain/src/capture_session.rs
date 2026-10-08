//! Owner-bound, single-threaded authority for bounded capture sessions.
//!
//! `SessionTurn` is affine: callbacks cannot borrow it while a canonical call
//! holds its mutable borrow. Equal archive identifiers never authenticate a
//! second authority. There is deliberately no reset or second-turn API.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::rc::{Rc, Weak};

use crate::event::{ActiveContext, EventId, InputContext};
use crate::identity::*;
use crate::policy::{DurabilityMode, RecordingGate, WatermarkKind};
use crate::record::{
    Control, EpochChange, Freshness, GapScope, Reason, Record, RecordFrame, RecordKind,
    RecordingHealth, Transport, WireContext,
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
    TimerAuthorityRequired,
    TimerOrderBlocked { earlier_work_id: u64 },
    TimerPlanInProgress { work_id: u64 },
    TimerCloseConflict,
    CloseNotReady,
    PingNotReady,
    PingAlreadyTaken,
    TimeOverflow,
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

/// Frozen local engineering policy; no caller-selected durations or revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatPolicy {
    SupervisorV2,
}

impl HeartbeatPolicy {
    pub const fn revision(self) -> u32 {
        2
    }
    pub const fn ping_interval_ns(self) -> u64 {
        30_000_000_000
    }
    pub const fn pong_timeout_ns(self) -> u64 {
        15_000_000_000
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimerKind {
    Ping,
    Timeout,
}

#[derive(Debug)]
pub enum TimerAdmission {
    NotDue,
    AlreadyQueued { original_work_id: u64 },
    Admitted(AdmittedTimer),
}

/// Only due admission can construct an original Timer capability.
#[derive(Debug)]
pub struct AdmittedTimer {
    owner: WorkOwner,
}

impl AdmittedTimer {
    pub fn kind(&self) -> TimerKind {
        self.owner.cell.timer.get().expect("original Timer").kind
    }
    pub fn identity(&self) -> ObservationIdentity {
        self.owner.cell.observation.get().expect("original Timer")
    }
    pub fn owner(&self) -> &WorkOwner {
        &self.owner
    }
    pub fn into_owner(self) -> WorkOwner {
        self.owner
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TimerProgressView {
    Unselected,
    TimerOnlyPing {
        timer_confirmed: bool,
        ping_taken: bool,
    },
    TimerOnlyObsolete {
        timer_confirmed: bool,
    },
    TimerThenDown {
        timer_confirmed: bool,
        down_confirmed: bool,
        close: CloseOwnerView,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimerPlan {
    Unselected,
    Ping {
        next_generation: u64,
        pong_deadline_ns: u64,
    },
    Obsolete,
    Timeout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PingTransfer {
    None,
    Retained,
    Taken,
    Revoked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TimerOriginal {
    kind: TimerKind,
    generation: u64,
    plan: TimerPlan,
    ping: PingTransfer,
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

/// Distinct authenticated stages inside one bounded original obligation.
/// Timer effect eligibility is not represented by ObservationIdentity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReceivedProgress {
    Unconfirmed,
    Raw,
    RawDiagnostic,
    StaleRaw,
    StaleDiagnostic,
    Gap,
    Up,
    Down,
    Timer,
    TimerDown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GeneratedPlan {
    connection: ConnectionId,
    book: BookId,
    original: EpochTag,
    next_connection: ConnectionEpoch,
    next_subscription: SubscriptionEpoch,
    next_book: BookEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GeneratedProgress {
    Unconfirmed,
    Connection,
    Subscription,
    Book,
}

enum OwnedProgress {
    Received(ReceivedProgress),
    Generated(GeneratedProgress),
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
    received_progress: Cell<ReceivedProgress>,
    generated_plan: Cell<Option<GeneratedPlan>>,
    generated_progress: Cell<GeneratedProgress>,
    abandoned_kind: Cell<Option<WorkKind>>,
    obligation_origin: Cell<ObligationOrigin>,
    record_admission_order: Cell<Option<u64>>,
    generated_stage_order: Cell<Option<u64>>,
    timer: Cell<Option<TimerOriginal>>,
}

impl WorkCell {
    fn required_record_pending(&self) -> bool {
        if !matches!(
            self.obligation.get(),
            ObservationObligation::Pending | ObservationObligation::Abandoned
        ) {
            return false;
        }
        if self.obligation_origin.get() == ObligationOrigin::Generated {
            return self.generated_stage_order.get().is_some();
        }
        let Some(identity) = self.observation.get() else {
            return false;
        };
        match identity.class {
            ObservationClass::Raw => self.received_progress.get() == ReceivedProgress::Unconfirmed,
            ObservationClass::RejectedStaleRaw => {
                self.received_progress.get() != ReceivedProgress::StaleDiagnostic
            }
            ObservationClass::Gap => self.received_progress.get() != ReceivedProgress::Gap,
            ObservationClass::Connected | ObservationClass::Pong => {
                self.received_progress.get() != ReceivedProgress::Up
            }
            ObservationClass::Disconnected => {
                self.received_progress.get() != ReceivedProgress::Down
            }
            ObservationClass::Timer { .. } => match self.timer.get().map(|timer| timer.plan) {
                Some(TimerPlan::Timeout) => {
                    self.received_progress.get() != ReceivedProgress::TimerDown
                }
                _ => self.received_progress.get() == ReceivedProgress::Unconfirmed,
            },
        }
    }

    fn active_record_order(&self) -> Option<u64> {
        if self.obligation_origin.get() == ObligationOrigin::Generated {
            self.generated_stage_order.get()
        } else {
            self.record_admission_order.get()
        }
    }

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
    ready: Cell<bool>,
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
    pub ready: bool,
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

    pub fn into_command(self) -> Result<CommandLease, AuthorityError> {
        self.owner.authority.close_cell(&self.owner)?;
        self.owner.authority.ensure_close_service()?;
        if !self.cell.ready.get() {
            return Err(AuthorityError::CloseNotReady);
        }
        Ok(CommandLease {
            authority: self.owner.authority.clone(),
            stream: self.owner.identity.stream,
            connection: self.owner.identity.connection,
            epoch: self.owner.identity.epoch,
            kind: CommandKind::Close,
            work: None,
            close: Some(self),
            timer_ping: None,
        })
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
    timer_ping: Option<(u64, u64)>,
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

struct ScheduleUpdate {
    epoch: ConnectionEpoch,
    generation: u64,
    kind: TimerKind,
    deadline: u64,
}

#[derive(Default)]
struct PreparedOwnedEffects {
    retained_ping: Option<WorkOwner>,
    schedule: Option<ScheduleUpdate>,
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
    fn validate_frame_binding(
        &self,
        frame: &RecordFrame,
        gate: RecordingGate,
    ) -> Result<(), AuthorityError> {
        frame
            .validate_shape()
            .map_err(|_| AuthorityError::InvalidBinding)?;
        let state = self.authority.state.borrow();
        let prefix = state.prefix.ok_or(AuthorityError::NotRegistered)?;
        if gate != prefix.recording_gate
            || frame.segment_no != prefix.segment
            || frame.record_no != prefix.next_record
            || !matches!(
                frame.value,
                Record::RawInput(_) | Record::Control(_) | Record::Gap(_)
            )
        {
            return Err(AuthorityError::InvalidBinding);
        }
        if let Record::RawInput(raw) = &frame.value
            && raw.bytes.len()
                > state
                    .budget
                    .ok_or(AuthorityError::NotRegistered)?
                    .max_message_bytes
        {
            return Err(AuthorityError::InvalidBudget);
        }
        if let Record::Control(record) = &frame.value
            && let Control::Recording(evidence) = &record.value
            && evidence.health == RecordingHealth::Failed
        {
            let index = match evidence.kind {
                WatermarkKind::Accepted => 0,
                WatermarkKind::Appended => 1,
                WatermarkKind::Written => 2,
                WatermarkKind::Flushed => 3,
                WatermarkKind::Durable => 4,
            };
            let descriptor = ArchiveFailureObservation {
                context: record.context,
                reason: evidence.reason,
                kind: evidence.kind,
            };
            if evidence.through != state.trusted_watermarks[index]
                || evidence
                    .through
                    .is_some_and(|through| through >= frame.record_no)
                || state
                    .archive_observation
                    .is_some_and(|original| original != descriptor)
                || matches!(state.marker, MarkerState::Confirmed(_))
            {
                return Err(AuthorityError::InvalidBinding);
            }
        }
        Ok(())
    }
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
            .validate_turn(turn)
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
        if owner.cell.sequence.get() != owner.sequence || owner.cell.references.get() == 0 {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidOwner,
            ));
        }
        if matches!(&frame.value, Record::Control(record) if matches!(record.value, Control::Timer { .. }))
            && owner.cell.timer.get().is_none()
        {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::TimerAuthorityRequired,
            ));
        }
        if !matches!(
            owner.cell.kind.get(),
            WorkKind::InFlightObservation | WorkKind::PendingPlan
        ) {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidOwner,
            ));
        }
        self.authority
            .ensure_storage_overlay_writable()
            .map_err(PersistBoundaryError::Authority)?;
        self.validate_frame_binding(frame, gate)
            .map_err(PersistBoundaryError::Authority)?;
        if owner.cell.observation.get().is_none()
            && matches!(&frame.value, Record::Control(record) if matches!(record.value, Control::Transport { .. } | Control::EpochAdvance { .. }))
        {
            return Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidOwner,
            ));
        }
        self.next_owned_progress(frame, owner)
            .map_err(PersistBoundaryError::Authority)?;
        if owner.cell.obligation_origin.get() == ObligationOrigin::Generated
            || owner.cell.observation.get().is_some_and(|identity| {
                matches!(
                    identity.class,
                    ObservationClass::Timer { .. }
                        | ObservationClass::Connected
                        | ObservationClass::Pong
                        | ObservationClass::Disconnected
                )
            })
        {
            self.authority
                .validate_record_order(owner)
                .map_err(PersistBoundaryError::Authority)?;
        }
        if let Some(timer) = owner.cell.timer.get()
            && timer.plan == TimerPlan::Unselected
            && timer.kind == TimerKind::Timeout
            && self
                .authority
                .timer_active(owner, timer)
                .map_err(PersistBoundaryError::Authority)?
        {
            self.authority
                .validate_timer_close(owner)
                .map_err(PersistBoundaryError::Authority)?;
        }
        self.authority
            .synchronize_obligations(turn)
            .map_err(PersistBoundaryError::Authority)?;
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

    fn next_owned_progress(
        &self,
        frame: &RecordFrame,
        owner: &WorkOwner,
    ) -> Result<Option<OwnedProgress>, AuthorityError> {
        if owner.cell.obligation.get() == ObservationObligation::None {
            return Ok(None);
        }
        if owner.cell.obligation_origin.get() == ObligationOrigin::Generated {
            return self
                .next_generated_progress(frame, owner)
                .map(|progress| Some(OwnedProgress::Generated(progress)));
        }
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        if matches!(identity.class, ObservationClass::Timer { .. })
            && owner.cell.timer.get().is_none()
        {
            return Err(AuthorityError::TimerAuthorityRequired);
        }
        if owner.cell.obligation.get() != ObservationObligation::Pending {
            return Err(AuthorityError::InvalidOwner);
        }
        let state = self.authority.state.borrow();
        let scope = state
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == identity.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        let context = frame
            .value
            .context()
            .ok_or(AuthorityError::InvalidBinding)?;
        if context.unix_ns.get() != identity.stamp.unix_ns
            || context.monotonic_ns.get() != identity.stamp.monotonic_ns
            || context.context
                != InputContext::Active(state.prefix.ok_or(AuthorityError::InvalidBinding)?.context)
        {
            return Err(AuthorityError::InvalidBinding);
        }
        let progress = owner.cell.received_progress.get();
        let raw_identity = identity
            .tag
            .is_some_and(|tag| tag.connection == identity.epoch)
            && identity.attempts.is_some_and(|(first, last)| first == last)
            && identity.loss_count.is_none();
        let exact_raw = |raw: &crate::record::RawInput| {
            raw_identity
                && raw.stream == identity.stream
                && Some(raw.tag) == identity.tag
                && identity.attempts == Some((raw.attempt, raw.attempt))
        };
        let exact_gap = |gap: &crate::record::Gap,
                         range: Option<(CaptureAttemptNo, CaptureAttemptNo)>,
                         loss_count: Option<u64>| {
            identity
                .tag
                .is_some_and(|tag| tag.connection == identity.epoch)
                && matches!(
                    &gap.scope,
                    GapScope::ExplicitTargets(targets)
                        if targets.len() == 1
                            && targets[0].stream == identity.stream
                            && Some(targets[0].tag) == identity.tag
                            && targets[0].range == range
                            && targets[0].loss_count == loss_count
                )
        };
        let control_identity =
            identity.tag.is_none() && identity.attempts.is_none() && identity.loss_count.is_none();
        let next = match (identity.class, progress, &frame.value) {
            (ObservationClass::Raw, ReceivedProgress::Unconfirmed, Record::RawInput(raw))
                if exact_raw(raw) =>
            {
                ReceivedProgress::Raw
            }
            (ObservationClass::Raw, ReceivedProgress::Raw, Record::Gap(gap))
                if raw_identity
                    && matches!(
                        gap.reason,
                        Reason::Unknown | Reason::DecodeRejected | Reason::SourceGap
                    )
                    && exact_gap(gap, None, None) =>
            {
                ReceivedProgress::RawDiagnostic
            }
            (
                ObservationClass::RejectedStaleRaw,
                ReceivedProgress::Unconfirmed,
                Record::RawInput(raw),
            ) if exact_raw(raw) && raw.bytes.is_empty() => ReceivedProgress::StaleRaw,
            (ObservationClass::RejectedStaleRaw, ReceivedProgress::StaleRaw, Record::Gap(gap))
                if raw_identity && gap.reason == Reason::Unknown && exact_gap(gap, None, None) =>
            {
                ReceivedProgress::StaleDiagnostic
            }
            (ObservationClass::Gap, ReceivedProgress::Unconfirmed, Record::Gap(gap))
                if identity.attempts.is_some()
                    && identity.loss_count.is_some()
                    && gap.reason == Reason::QueueOverflow
                    && exact_gap(gap, identity.attempts, identity.loss_count) =>
            {
                ReceivedProgress::Gap
            }
            (
                ObservationClass::Connected | ObservationClass::Pong,
                ReceivedProgress::Unconfirmed,
                Record::Control(record),
            ) if control_identity
                && scope.confirmed_down != Some(identity.epoch)
                && matches!(record.value, Control::Transport { connection, epoch, value: Transport::Up }
                    if connection == scope.binding.connection && epoch == identity.epoch) =>
            {
                ReceivedProgress::Up
            }
            (
                ObservationClass::Disconnected,
                ReceivedProgress::Unconfirmed,
                Record::Control(record),
            ) if control_identity
                && matches!(record.value, Control::Transport { connection, epoch, value: Transport::Down }
                    if connection == scope.binding.connection && epoch == identity.epoch) =>
            {
                ReceivedProgress::Down
            }
            (
                ObservationClass::Timer {
                    timer_id,
                    deadline_ns,
                },
                ReceivedProgress::Unconfirmed,
                Record::Control(record),
            ) if control_identity
                && matches!(record.value, Control::Timer { stream, timer_id: actual_id, deadline_ns: actual_deadline }
                    if stream == identity.stream && actual_id == timer_id && actual_deadline == deadline_ns) =>
            {
                ReceivedProgress::Timer
            }
            (ObservationClass::Timer { .. }, ReceivedProgress::Timer, Record::Control(record))
                if control_identity
                    && owner
                        .cell
                        .timer
                        .get()
                        .is_some_and(|timer| timer.plan == TimerPlan::Timeout)
                    && matches!(record.value, Control::Transport { connection, epoch, value: Transport::Down }
                    if connection == scope.binding.connection && epoch == identity.epoch) =>
            {
                ReceivedProgress::TimerDown
            }
            _ => return Err(AuthorityError::InvalidBinding),
        };
        Ok(Some(OwnedProgress::Received(next)))
    }

    fn next_generated_progress(
        &self,
        frame: &RecordFrame,
        owner: &WorkOwner,
    ) -> Result<GeneratedProgress, AuthorityError> {
        if owner.cell.obligation.get() != ObservationObligation::Pending {
            return Err(AuthorityError::InvalidOwner);
        }
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        let plan = owner
            .cell
            .generated_plan
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        let state = self.authority.state.borrow();
        let scope = state
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == identity.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        if scope.binding.epoch != identity.epoch
            || scope.current_tag != Some(plan.original)
            || scope.close.state.get() != CloseState::Settled
            || !scope.close.identity.get().is_some_and(|close| {
                close.stream == identity.stream
                    && close.connection == plan.connection
                    && close.epoch == identity.epoch
                    && close.storage == CloseStorage::WorkOwner(owner.id())
            })
        {
            return Err(AuthorityError::NotQuiescent);
        }
        let Record::Control(record) = &frame.value else {
            return Err(AuthorityError::InvalidBinding);
        };
        if record.context.unix_ns.get() != identity.stamp.unix_ns
            || record.context.monotonic_ns.get() != identity.stamp.monotonic_ns
            || record.context.context
                != InputContext::Active(state.prefix.ok_or(AuthorityError::InvalidBinding)?.context)
        {
            return Err(AuthorityError::InvalidBinding);
        }
        let next = match (owner.cell.generated_progress.get(), &record.value) {
            (
                GeneratedProgress::Unconfirmed,
                Control::EpochAdvance {
                    change:
                        EpochChange::Connection {
                            owner: connection,
                            expected,
                            next,
                        },
                    reason: Reason::Reconnect,
                },
            ) if *connection == plan.connection
                && *expected == plan.original.connection
                && *next == plan.next_connection =>
            {
                GeneratedProgress::Connection
            }
            (
                GeneratedProgress::Connection,
                Control::EpochAdvance {
                    change:
                        EpochChange::Subscription {
                            owner: stream,
                            expected,
                            next,
                        },
                    reason: Reason::Reconnect,
                },
            ) if *stream == identity.stream
                && *expected == plan.original.subscription
                && *next == plan.next_subscription =>
            {
                GeneratedProgress::Subscription
            }
            (
                GeneratedProgress::Subscription,
                Control::EpochAdvance {
                    change:
                        EpochChange::Book {
                            owner: book,
                            expected,
                            next,
                        },
                    reason: Reason::Reconnect,
                },
            ) if *book == plan.book
                && Some(*expected) == plan.original.book
                && *next == plan.next_book =>
            {
                GeneratedProgress::Book
            }
            _ => return Err(AuthorityError::InvalidBinding),
        };
        Ok(next)
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
    fn prepare_owned_effects(
        &self,
        turn: &mut SessionTurn,
        frame: &RecordFrame,
        owner: &WorkOwner,
        progress: &Option<OwnedProgress>,
    ) -> Result<PreparedOwnedEffects, PersistBoundaryError> {
        let Some(identity) = owner.cell.observation.get() else {
            return Ok(PreparedOwnedEffects::default());
        };
        if let Some(mut timer) = owner.cell.timer.get()
            && owner.cell.obligation_origin.get() == ObligationOrigin::Received
            && timer.plan == TimerPlan::Unselected
        {
            if !self
                .authority
                .timer_active(owner, timer)
                .map_err(PersistBoundaryError::Authority)?
            {
                timer.plan = TimerPlan::Obsolete;
                owner.cell.timer.set(Some(timer));
                return Ok(PreparedOwnedEffects::default());
            }
            if timer.kind == TimerKind::Timeout {
                self.authority
                    .reserve_timer_close(owner)
                    .map_err(PersistBoundaryError::Authority)?;
                timer.plan = TimerPlan::Timeout;
                owner.cell.timer.set(Some(timer));
                // Both original required records must have representable successors.
                if frame
                    .record_no
                    .checked_next()
                    .and_then(RecordNo::checked_next)
                    .is_err()
                {
                    return Err(PersistBoundaryError::Authority(
                        self.authority.stop_counter(turn, "RecordNo"),
                    ));
                }
                return Ok(PreparedOwnedEffects::default());
            }
            if owner.cell.references.get() >= MAX_WORK_SHARES {
                return Err(PersistBoundaryError::Authority(
                    AuthorityError::WorkShareExhausted,
                ));
            }
            let state = self.authority.state.borrow();
            let scope = state
                .scopes
                .iter()
                .find(|scope| scope.binding.stream == identity.stream)
                .ok_or(PersistBoundaryError::Authority(
                    AuthorityError::InvalidBinding,
                ))?;
            let generation = scope.schedule_generation.checked_add(1);
            let deadline = identity
                .stamp
                .monotonic_ns
                .checked_add(state.heartbeat_policy.pong_timeout_ns());
            drop(state);
            let generation = generation.ok_or_else(|| {
                PersistBoundaryError::Authority(
                    self.authority.stop_counter(turn, "TimerScheduleGeneration"),
                )
            })?;
            let deadline = deadline
                .ok_or_else(|| PersistBoundaryError::Authority(self.authority.stop_time(turn)))?;
            let retained = owner.share().map_err(PersistBoundaryError::Authority)?;
            timer.plan = TimerPlan::Ping {
                next_generation: generation,
                pong_deadline_ns: deadline,
            };
            owner.cell.timer.set(Some(timer));
            return Ok(PreparedOwnedEffects {
                retained_ping: Some(retained),
                schedule: Some(ScheduleUpdate {
                    epoch: identity.epoch,
                    generation,
                    kind: TimerKind::Timeout,
                    deadline,
                }),
            });
        }
        if matches!(
            progress,
            Some(OwnedProgress::Received(ReceivedProgress::Up))
        ) {
            let state = self.authority.state.borrow();
            let scope = state
                .scopes
                .iter()
                .find(|scope| scope.binding.stream == identity.stream)
                .ok_or(PersistBoundaryError::Authority(
                    AuthorityError::InvalidBinding,
                ))?;
            if !matches!(
                state.lifecycle,
                SessionLifecycle::Open | SessionLifecycle::FailedDiagnostic
            ) || scope.failure.is_some()
                || scope.binding.epoch != identity.epoch
            {
                return Ok(PreparedOwnedEffects::default());
            }
            let generation = scope.schedule_generation.checked_add(1);
            let deadline = identity
                .stamp
                .monotonic_ns
                .checked_add(state.heartbeat_policy.ping_interval_ns());
            drop(state);
            let generation = generation.ok_or_else(|| {
                PersistBoundaryError::Authority(
                    self.authority.stop_counter(turn, "TimerScheduleGeneration"),
                )
            })?;
            let deadline = deadline
                .ok_or_else(|| PersistBoundaryError::Authority(self.authority.stop_time(turn)))?;
            return Ok(PreparedOwnedEffects {
                retained_ping: None,
                schedule: Some(ScheduleUpdate {
                    epoch: identity.epoch,
                    generation,
                    kind: TimerKind::Ping,
                    deadline,
                }),
            });
        }
        Ok(PreparedOwnedEffects::default())
    }

    fn commit_owned_effects(
        &self,
        owner: &WorkOwner,
        progress: &Option<OwnedProgress>,
        prepared: PreparedOwnedEffects,
    ) {
        let Some(identity) = owner.cell.observation.get() else {
            return;
        };
        let mut state = self.authority.state.borrow_mut();
        let Some(scope) = state
            .scopes
            .iter_mut()
            .find(|scope| scope.binding.stream == identity.stream)
        else {
            return;
        };
        if let Some(ScheduleUpdate {
            epoch,
            generation,
            kind,
            deadline,
        }) = prepared.schedule
        {
            scope.revoke_ping();
            scope.schedule_generation = generation;
            scope.schedule = Some((kind, epoch, deadline));
            scope.queued_timer = None;
            if let Some(retained) = prepared.retained_ping {
                scope.pending_ping = Some((retained, generation));
                let mut timer = owner.cell.timer.get().expect("prepared original Ping");
                timer.ping = PingTransfer::Retained;
                owner.cell.timer.set(Some(timer));
            }
        }
        if matches!(
            progress,
            Some(OwnedProgress::Received(
                ReceivedProgress::Down | ReceivedProgress::TimerDown
            ))
        ) {
            scope.disable_schedule();
            if owner
                .cell
                .timer
                .get()
                .is_some_and(|timer| timer.plan == TimerPlan::Timeout)
            {
                scope.frozen_timeout = None;
                if scope.close.identity.get().is_some_and(|close| {
                    close.storage == CloseStorage::WorkOwner(owner.id())
                        && close.epoch == identity.epoch
                }) {
                    scope.close.ready.set(true);
                }
            }
        }
        if matches!(
            progress,
            Some(OwnedProgress::Received(ReceivedProgress::Timer))
        ) && owner
            .cell
            .timer
            .get()
            .is_some_and(|timer| timer.plan != TimerPlan::Timeout)
            && scope.queued_timer
                == Some((
                    owner.id(),
                    owner.cell.timer.get().expect("original Timer").generation,
                ))
        {
            scope.queued_timer = None;
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
            .validate_turn(turn)
            .map_err(PersistBoundaryError::Authority)?;
        self.authority
            .ensure_storage_overlay_writable()
            .map_err(PersistBoundaryError::Authority)?;
        self.validate_frame_binding(frame, gate)
            .map_err(PersistBoundaryError::Authority)?;
        if let Some(owner) = owner {
            self.next_owned_progress(frame, owner)
                .map_err(PersistBoundaryError::Authority)?;
        }
        self.writer
            .checked_memory_profile()
            .and_then(|profile| profile.validate())
            .map_err(PersistBoundaryError::Authority)?;
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
        let owned_progress = match owner {
            Some(owner) => self
                .next_owned_progress(frame, owner)
                .map_err(PersistBoundaryError::Authority)?,
            None => None,
        };
        let prepared_effects = match owner {
            Some(owner) => self.prepare_owned_effects(turn, frame, owner, &owned_progress)?,
            None => PreparedOwnedEffects::default(),
        };
        if let Some(owner) = owner
            && owner.cell.obligation_origin.get() == ObligationOrigin::Generated
            && owner.cell.generated_progress.get() == GeneratedProgress::Unconfirmed
            && frame
                .record_no
                .checked_next()
                .and_then(RecordNo::checked_next)
                .and_then(RecordNo::checked_next)
                .is_err()
        {
            return Err(PersistBoundaryError::Authority(
                self.authority.stop_counter(turn, "RecordNo"),
            ));
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
                owner
                    .cell
                    .confirmed_records
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| {
                        PersistBoundaryError::Authority(
                            self.authority.stop_counter(turn, "WorkReceipt"),
                        )
                    })
            })
            .transpose()?;
        if let Some(owner) = owner
            && owner.cell.obligation_origin.get() == ObligationOrigin::Generated
            && owner.cell.generated_stage_order.get().is_none()
        {
            let next_order = self
                .authority
                .state
                .borrow()
                .record_admission_counter
                .checked_add(1);
            let next_order = next_order.ok_or_else(|| {
                PersistBoundaryError::Authority(
                    self.authority.stop_counter(turn, "RecordAdmissionOrder"),
                )
            })?;
            self.authority.state.borrow_mut().record_admission_counter = next_order;
            owner.cell.generated_stage_order.set(Some(next_order));
        }
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
            match &owned_progress {
                Some(OwnedProgress::Received(progress)) => {
                    owner.cell.received_progress.set(*progress)
                }
                Some(OwnedProgress::Generated(progress)) => {
                    owner.cell.generated_progress.set(*progress);
                    owner.cell.generated_stage_order.set(None);
                }
                None => {}
            }
            self.commit_owned_effects(owner, &owned_progress, prepared_effects);
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
    schedule: Option<(TimerKind, ConnectionEpoch, u64)>,
    schedule_generation: u64,
    timer_id: u64,
    queued_timer: Option<(u64, u64)>,
    frozen_timeout: Option<u64>,
    pending_ping: Option<(WorkOwner, u64)>,
}

impl ScopeState {
    fn revoke_ping(&mut self) {
        if let Some((owner, _)) = self.pending_ping.take()
            && let Some(mut timer) = owner.cell.timer.get()
        {
            timer.ping = PingTransfer::Revoked;
            owner.cell.timer.set(Some(timer));
        }
    }

    fn disable_schedule(&mut self) {
        self.schedule = None;
        self.queued_timer = None;
        self.revoke_ping();
    }
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
    record_admission_counter: u64,
    heartbeat_policy: HeartbeatPolicy,
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
    pub fn admit_due_timer(
        &self,
        turn: &mut SessionTurn,
        stream: StreamId,
        observed_stamp: ReceiveStamp,
    ) -> Result<TimerAdmission, AuthorityError> {
        self.authority.validate_turn(turn)?;
        {
            let state = self.authority.state.borrow();
            if !state.registered {
                return Err(AuthorityError::NotRegistered);
            }
            if !state
                .scopes
                .iter()
                .any(|scope| scope.binding.stream == stream)
            {
                return Err(AuthorityError::InvalidBinding);
            }
        }
        self.authority.synchronize_obligations(turn)?;
        self.authority.ensure_admission_open(turn)?;
        let mut state = self.authority.state.borrow_mut();
        let index = state
            .scopes
            .iter()
            .position(|scope| scope.binding.stream == stream)
            .expect("validated scope");
        let scope = &state.scopes[index];
        if scope.failure.is_some() {
            return Err(AuthorityError::CommandRevoked);
        }
        let Some((kind, epoch, deadline_ns)) = scope.schedule else {
            return Ok(TimerAdmission::NotDue);
        };
        if observed_stamp.monotonic_ns < deadline_ns {
            return Ok(TimerAdmission::NotDue);
        }
        if let Some((original_work_id, generation)) = scope.queued_timer
            && generation == scope.schedule_generation
        {
            return Ok(TimerAdmission::AlreadyQueued { original_work_id });
        }
        let cell = state
            .work
            .iter()
            .find(|cell| !cell.is_retained())
            .cloned()
            .ok_or(AuthorityError::WorkExhausted)?;
        let generation = scope.schedule_generation;
        let checked = (
            state.sequence.checked_add(1),
            state.record_admission_counter.checked_add(1),
            scope.timer_id.checked_add(1),
        );
        let (sequence, order, timer_id) = match checked {
            (Some(sequence), Some(order), Some(timer_id)) => (sequence, order, timer_id),
            _ => {
                let counter = if checked.0.is_none() {
                    "AdmissionOrder"
                } else if checked.1.is_none() {
                    "RecordAdmissionOrder"
                } else {
                    "TimerId"
                };
                drop(state);
                return Err(self.authority.stop_counter(turn, counter));
            }
        };
        let identity = ObservationIdentity {
            stream,
            epoch,
            stamp: observed_stamp,
            class: ObservationClass::Timer {
                timer_id,
                deadline_ns,
            },
            tag: None,
            attempts: None,
            loss_count: None,
        };
        state.sequence = sequence;
        state.record_admission_counter = order;
        state.scopes[index].timer_id = timer_id;
        state.scopes[index].queued_timer = Some((sequence, generation));
        cell.sequence.set(sequence);
        cell.close_scope.set(None);
        cell.references.set(1);
        cell.kind.set(WorkKind::QueuedObservation);
        cell.cut_side.set(if state.failed {
            CutSide::PostCut
        } else {
            CutSide::BeforeFailure
        });
        cell.obligation.set(ObservationObligation::Pending);
        cell.obligation_origin.set(ObligationOrigin::Received);
        cell.observation.set(Some(identity));
        cell.confirmed_records.set(0);
        cell.received_progress.set(ReceivedProgress::Unconfirmed);
        cell.generated_plan.set(None);
        cell.generated_progress.set(GeneratedProgress::Unconfirmed);
        cell.abandoned_kind.set(None);
        cell.record_admission_order.set(Some(order));
        cell.generated_stage_order.set(None);
        cell.timer.set(Some(TimerOriginal {
            kind,
            generation,
            plan: TimerPlan::Unselected,
            ping: PingTransfer::None,
        }));
        Ok(TimerAdmission::Admitted(AdmittedTimer {
            owner: WorkOwner {
                authority: Rc::downgrade(&self.authority.state),
                cell,
                sequence,
            },
        }))
    }

    pub fn timer_progress(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
    ) -> Result<TimerProgressView, AuthorityError> {
        self.authority.validate_turn(turn)?;
        self.validate_work(owner)?;
        let timer = owner
            .cell
            .timer
            .get()
            .ok_or(AuthorityError::TimerAuthorityRequired)?;
        let progress = owner.cell.received_progress.get();
        let timer_confirmed = matches!(
            progress,
            ReceivedProgress::Timer | ReceivedProgress::TimerDown
        );
        Ok(match timer.plan {
            TimerPlan::Unselected => TimerProgressView::Unselected,
            TimerPlan::Ping { .. } => TimerProgressView::TimerOnlyPing {
                timer_confirmed,
                ping_taken: timer.ping == PingTransfer::Taken,
            },
            TimerPlan::Obsolete => TimerProgressView::TimerOnlyObsolete { timer_confirmed },
            TimerPlan::Timeout => {
                let state = self.authority.state.borrow();
                let identity = owner
                    .cell
                    .observation
                    .get()
                    .ok_or(AuthorityError::InvalidOwner)?;
                let scope = state
                    .scopes
                    .iter()
                    .find(|scope| scope.binding.stream == identity.stream)
                    .ok_or(AuthorityError::InvalidBinding)?;
                let close = scope
                    .close
                    .identity
                    .get()
                    .filter(|close| {
                        close.storage == CloseStorage::WorkOwner(owner.id())
                            && close.epoch == identity.epoch
                    })
                    .ok_or(AuthorityError::InvalidOwner)?;
                TimerProgressView::TimerThenDown {
                    timer_confirmed,
                    down_confirmed: progress == ReceivedProgress::TimerDown,
                    close: CloseOwnerView {
                        owner: CloseOwnerRef {
                            authority: self.authority.clone(),
                            identity: close,
                        },
                        state: scope.close.state.get(),
                        ready: scope.close.ready.get(),
                    },
                }
            }
        })
    }

    pub fn take_timer_ping(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
    ) -> Result<CommandLease, AuthorityError> {
        self.authority.validate_turn(turn)?;
        self.validate_work(owner)?;
        let mut timer = owner
            .cell
            .timer
            .get()
            .ok_or(AuthorityError::TimerAuthorityRequired)?;
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        if timer.ping == PingTransfer::Taken {
            return Err(AuthorityError::PingAlreadyTaken);
        }
        {
            let state = self.authority.state.borrow();
            if state.storage_stopped.is_some()
                || !matches!(
                    state.lifecycle,
                    SessionLifecycle::Open | SessionLifecycle::FailedDiagnostic
                )
                || state
                    .scopes
                    .iter()
                    .find(|scope| scope.binding.stream == identity.stream)
                    .is_none_or(|scope| scope.failure.is_some())
            {
                return Err(AuthorityError::CommandRevoked);
            }
        }
        if timer.ping == PingTransfer::Revoked
            || timer.plan == TimerPlan::Obsolete
            || timer.kind != TimerKind::Ping
        {
            return Err(AuthorityError::CommandRevoked);
        }
        if timer.ping != PingTransfer::Retained
            || owner.cell.received_progress.get() != ReceivedProgress::Timer
        {
            return Err(AuthorityError::PingNotReady);
        }
        let TimerPlan::Ping {
            next_generation, ..
        } = timer.plan
        else {
            return Err(AuthorityError::PingNotReady);
        };
        let mut state = self.authority.state.borrow_mut();
        let permitted = state.storage_stopped.is_none()
            && matches!(
                state.lifecycle,
                SessionLifecycle::Open | SessionLifecycle::FailedDiagnostic
            );
        let scope = state
            .scopes
            .iter_mut()
            .find(|scope| scope.binding.stream == identity.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        if !permitted
            || scope.failure.is_some()
            || scope.binding.epoch != identity.epoch
            || scope.schedule_generation != next_generation
            || !matches!(scope.schedule, Some((TimerKind::Timeout, epoch, _)) if epoch == identity.epoch)
            || scope.frozen_timeout.is_some()
        {
            return Err(AuthorityError::CommandRevoked);
        }
        let Some((retained, generation)) = scope.pending_ping.as_ref() else {
            return Err(AuthorityError::CommandRevoked);
        };
        if retained.id() != owner.id() || *generation != next_generation {
            return Err(AuthorityError::InvalidOwner);
        }
        let (retained, _) = scope
            .pending_ping
            .take()
            .expect("validated retained entitlement");
        timer.ping = PingTransfer::Taken;
        owner.cell.timer.set(Some(timer));
        Ok(CommandLease {
            authority: self.authority.clone(),
            stream: identity.stream,
            connection: scope.binding.connection,
            epoch: identity.epoch,
            kind: CommandKind::SendText {
                text: "ping".to_owned(),
            },
            work: Some(retained),
            close: None,
            timer_ping: Some((owner.id(), next_generation)),
        })
    }
    /// Only this non-clonable registered supervisor handle can admit and
    /// complete its jobs; generic authority kind changes cannot settle them.
    pub fn admit_observation(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
        identity: ObservationIdentity,
    ) -> Result<(), AuthorityError> {
        self.authority.validate_turn(turn)?;
        self.validate_work(owner)?;
        if matches!(identity.class, ObservationClass::Timer { .. }) {
            return Err(AuthorityError::TimerAuthorityRequired);
        }
        {
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
        }
        self.authority.synchronize_obligations(turn)?;
        self.authority.ensure_admission_open(turn)?;
        let mut state = self.authority.state.borrow_mut();
        if owner.cell.obligation.get() != ObservationObligation::None
            || owner.cell.kind.get() != WorkKind::QueuedObservation
            || !state
                .scopes
                .iter()
                .any(|scope| scope.binding.stream == identity.stream)
        {
            return Err(AuthorityError::InvalidOwner);
        }
        let Some(order) = state.record_admission_counter.checked_add(1) else {
            drop(state);
            self.authority.received_order_exhausted(turn, identity)?;
            return Err(AuthorityError::CounterExhausted("RecordAdmissionOrder"));
        };
        state.record_admission_counter = order;
        owner.cell.record_admission_order.set(Some(order));
        owner.cell.observation.set(Some(identity));
        owner.cell.confirmed_records.set(0);
        owner.cell.generated_plan.set(None);
        owner
            .cell
            .generated_progress
            .set(GeneratedProgress::Unconfirmed);
        owner
            .cell
            .received_progress
            .set(ReceivedProgress::Unconfirmed);
        owner.cell.obligation.set(ObservationObligation::Pending);
        owner.cell.obligation_origin.set(ObligationOrigin::Received);
        Ok(())
    }

    /// F2 may extend only the last actually admitted archive observation.
    /// Reservation, retained aliases and cell reuse cannot move this frontier.
    /// This preflight is read-only; extension rechecks it under the same turn.
    pub fn gap_extension_eligible(
        &self,
        turn: &SessionTurn,
        owner: &WorkOwner,
    ) -> Result<bool, AuthorityError> {
        self.authority.validate_turn(turn)?;
        self.validate_work(owner)?;
        let state = self.authority.state.borrow();
        Ok(matches!(
            state.lifecycle,
            SessionLifecycle::Open | SessionLifecycle::FailedDiagnostic
        ) && state.storage_stopped.is_none()
            && owner.cell.kind.get() == WorkKind::QueuedObservation
            && owner.cell.obligation_origin.get() == ObligationOrigin::Received
            && owner.cell.obligation.get() == ObservationObligation::Pending
            && owner.cell.received_progress.get() == ReceivedProgress::Unconfirmed
            && owner.cell.confirmed_records.get() == 0
            && owner.cell.observation.get().is_some_and(|identity| {
                identity.class == ObservationClass::Gap
                    && state.scopes.iter().any(|scope| {
                        scope.binding.stream == identity.stream && scope.failure.is_none()
                    })
            })
            && owner.cell.record_admission_order.get() == Some(state.record_admission_counter)
            && (!state.failed || owner.cut_side() == CutSide::PostCut))
    }

    pub fn extend_gap_observation(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
        identity: ObservationIdentity,
    ) -> Result<(), AuthorityError> {
        self.authority.validate_turn(turn)?;
        self.validate_work(owner)?;
        // Reject incompatible identity/order without reconciling another lost
        // steward or changing the failure cut. No I/O is involved in F2.
        if !self.gap_extension_eligible(turn, owner)? {
            return Err(AuthorityError::InvalidOwner);
        }
        let old = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        let state = self.authority.state.borrow();
        if owner.cell.obligation.get() != ObservationObligation::Pending
            || owner.cell.received_progress.get() != ReceivedProgress::Unconfirmed
            || owner.cell.confirmed_records.get() != 0
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

    fn validate_received_down_close(
        &self,
        owner: &WorkOwner,
        identity: ObservationIdentity,
    ) -> Result<(), AuthorityError> {
        {
            let state = self.authority.state.borrow();
            let scope = state
                .scopes
                .iter()
                .find(|scope| scope.binding.stream == identity.stream)
                .ok_or(AuthorityError::InvalidBinding)?;
            let installs_close = scope.close.identity.get().is_none_or(|close| {
                close.epoch != identity.epoch && scope.close.state.get() == CloseState::Settled
            });
            if installs_close && owner.cell.references.get() >= MAX_WORK_SHARES {
                return Err(AuthorityError::WorkShareExhausted);
            }
            if let Some(close) = scope.close.identity.get()
                && close.epoch == identity.epoch
            {
                if close.stream != identity.stream || close.connection != scope.binding.connection {
                    return Err(AuthorityError::InvalidOwner);
                }
                if let CloseStorage::WorkOwner(work_id) = close.storage
                    && work_id != owner.id()
                    && scope.close.state.get() != CloseState::Settled
                {
                    // An unrelated live work owner's Close cannot account this
                    // Down merely because its connection/epoch numbers match.
                    // A genuine earlier Down keeps its existing R2 ownership.
                    let retained = scope.close.work.borrow();
                    let prior = retained.as_ref().ok_or(AuthorityError::InvalidOwner)?;
                    self.validate_work(prior)?;
                    let original = prior
                        .cell
                        .observation
                        .get()
                        .ok_or(AuthorityError::InvalidOwner)?;
                    let received_down = matches!(
                        (original.class, prior.cell.received_progress.get()),
                        (ObservationClass::Disconnected, ReceivedProgress::Down)
                            | (ObservationClass::Timer { .. }, ReceivedProgress::TimerDown)
                    );
                    let generated_down = prior.cell.generated_plan.get().is_some_and(|plan| {
                        prior.cell.obligation_origin.get() == ObligationOrigin::Generated
                            && plan.connection == close.connection
                            && plan.original.connection == close.epoch
                            && matches!(
                                original.class,
                                ObservationClass::Disconnected | ObservationClass::Timer { .. }
                            )
                    });
                    if prior.id() != work_id
                        || prior.cell.close_scope.get() != Some(identity.stream)
                        || original.stream != identity.stream
                        || original.epoch != identity.epoch
                        || !(received_down || generated_down)
                    {
                        return Err(AuthorityError::InvalidOwner);
                    }
                }
            }
        }
        Ok(())
    }

    fn retain_received_down_close(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
        identity: ObservationIdentity,
    ) -> Result<(), AuthorityError> {
        self.validate_received_down_close(owner, identity)?;
        // This existing serialized R2 transition installs the first Close on
        // the original W or reuses a valid prior/terminal owner. All fallible
        // checks precede its mutation; a pending observation stays retryable.
        self.authority
            .mandatory_close(turn, identity.stream, identity.epoch, Some(owner))?;
        Ok(())
    }

    /// Complete the canonical job after all its required writes/dispositions.
    /// Receipts are authenticated by the bound sink. A no-write obsolete Up or
    /// Pong is accepted only after actual same-epoch Down was gate-confirmed.
    /// Received Down retains its mandatory Close before settling this job;
    /// that Close may remain Pending/Leased until the caller dispatches it.
    pub fn complete_observation(
        &self,
        turn: &mut SessionTurn,
        sink: &BoundRecordSink,
        owner: &WorkOwner,
        obsolete: Option<(StreamId, ConnectionEpoch)>,
    ) -> Result<(), AuthorityError> {
        self.authority.validate_turn(turn)?;
        self.validate_work(owner)?;
        if !self.authority.same_authority(&sink.authority) {
            return Err(AuthorityError::AuthorityMismatch);
        }
        if owner.cell.obligation.get() != ObservationObligation::Pending {
            return Err(AuthorityError::InvalidOwner);
        }
        self.authority.ensure_storage_overlay_writable()?;
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        if matches!(identity.class, ObservationClass::Timer { .. })
            && owner.cell.timer.get().is_none()
        {
            return Err(AuthorityError::TimerAuthorityRequired);
        }
        if owner.cell.obligation_origin.get() == ObligationOrigin::Received
            && matches!(
                identity.class,
                ObservationClass::Timer { .. }
                    | ObservationClass::Connected
                    | ObservationClass::Pong
                    | ObservationClass::Disconnected
            )
        {
            self.authority.validate_record_order(owner)?;
        }
        let generated = owner.cell.obligation_origin.get() == ObligationOrigin::Generated;
        let authenticated = if generated {
            owner.cell.generated_progress.get() == GeneratedProgress::Book
        } else {
            match identity.class {
                ObservationClass::Raw => matches!(
                    owner.cell.received_progress.get(),
                    ReceivedProgress::Raw | ReceivedProgress::RawDiagnostic
                ),
                ObservationClass::RejectedStaleRaw => {
                    owner.cell.received_progress.get() == ReceivedProgress::StaleDiagnostic
                }
                ObservationClass::Gap => {
                    owner.cell.received_progress.get() == ReceivedProgress::Gap
                }
                ObservationClass::Connected | ObservationClass::Pong => {
                    owner.cell.received_progress.get() == ReceivedProgress::Up
                }
                ObservationClass::Disconnected => {
                    owner.cell.received_progress.get() == ReceivedProgress::Down
                }
                ObservationClass::Timer { .. } => match owner
                    .cell
                    .timer
                    .get()
                    .expect("validated original Timer")
                    .plan
                {
                    TimerPlan::Ping { .. } | TimerPlan::Obsolete => {
                        owner.cell.received_progress.get() == ReceivedProgress::Timer
                    }
                    TimerPlan::Timeout => {
                        owner.cell.received_progress.get() == ReceivedProgress::TimerDown
                            && self.authority.state.borrow().scopes.iter().any(|scope| {
                                scope.close.identity.get().is_some_and(|close| {
                                    close.storage == CloseStorage::WorkOwner(owner.id())
                                        && close.stream == identity.stream
                                        && close.epoch == identity.epoch
                                })
                            })
                    }
                    TimerPlan::Unselected => false,
                },
            }
        };
        if !authenticated
            && (!matches!(
                identity.class,
                ObservationClass::Connected | ObservationClass::Pong
            ) || generated
                || owner.cell.received_progress.get() != ReceivedProgress::Unconfirmed
                || owner.cell.confirmed_records.get() != 0
                || identity.tag.is_some()
                || identity.attempts.is_some()
                || identity.loss_count.is_some()
                || obsolete != Some((identity.stream, identity.epoch))
                || !self.authority.state.borrow().scopes.iter().any(|scope| {
                    scope.binding.stream == identity.stream
                        && scope.confirmed_down == Some(identity.epoch)
                }))
        {
            return Err(AuthorityError::NotQuiescent);
        }
        if !generated && identity.class == ObservationClass::Disconnected {
            self.validate_received_down_close(owner, identity)?;
        }
        self.authority.synchronize_obligations(turn)?;
        self.authority.ensure_storage_writable()?;
        if !generated && identity.class == ObservationClass::Disconnected {
            // Reuse the existing R2 owner if this epoch already has a Close.
            // A first ordinary Down transfers retention into the same W; a
            // rejected installation leaves the admitted obligation Pending.
            self.retain_received_down_close(turn, owner, identity)?;
        }
        owner.cell.obligation.set(ObservationObligation::Settled);
        Ok(())
    }

    pub fn retain_generated_plan(
        &self,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
    ) -> Result<(), AuthorityError> {
        self.authority.validate_turn(turn)?;
        self.validate_work(owner)?;
        if owner.cell.obligation.get() != ObservationObligation::Settled
            || owner.cell.kind.get() != WorkKind::PendingPlan
            || owner.cell.obligation_origin.get() != ObligationOrigin::Received
        {
            return Err(AuthorityError::InvalidOwner);
        }
        {
            let state = self.authority.state.borrow();
            if !matches!(
                state.lifecycle,
                SessionLifecycle::Open | SessionLifecycle::FailedDiagnostic
            ) {
                return Err(AuthorityError::SessionClosing);
            }
            if state.storage_stopped.is_some() {
                return Err(AuthorityError::StorageStopped);
            }
        }
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        if !matches!(
            (identity.class, owner.cell.received_progress.get()),
            (ObservationClass::Disconnected, ReceivedProgress::Down)
                | (ObservationClass::Timer { .. }, ReceivedProgress::TimerDown)
        ) {
            return Err(AuthorityError::InvalidOwner);
        }
        let state = self.authority.state.borrow();
        let scope = state
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == identity.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        let original = scope.current_tag.ok_or(AuthorityError::InvalidBinding)?;
        if scope.binding.epoch != identity.epoch
            || original.connection != identity.epoch
            || !scope.close.identity.get().is_some_and(|close| {
                close.stream == identity.stream
                    && close.connection == scope.binding.connection
                    && close.epoch == identity.epoch
                    && close.storage == CloseStorage::WorkOwner(owner.id())
            })
        {
            return Err(AuthorityError::InvalidBinding);
        }
        let book = state
            .accepted_bindings
            .iter()
            .find(|binding| binding.id == identity.stream)
            .and_then(|binding| binding.book_id)
            .ok_or(AuthorityError::InvalidBinding)?;
        let plan = GeneratedPlan {
            connection: scope.binding.connection,
            book,
            original,
            next_connection: original
                .connection
                .checked_next()
                .map_err(|_| AuthorityError::CounterExhausted("ConnectionEpoch"))?,
            next_subscription: original
                .subscription
                .checked_next()
                .map_err(|_| AuthorityError::CounterExhausted("SubscriptionEpoch"))?,
            next_book: original
                .book
                .ok_or(AuthorityError::InvalidBinding)?
                .checked_next()
                .map_err(|_| AuthorityError::CounterExhausted("BookEpoch"))?,
        };
        drop(state);
        self.authority.synchronize_obligations(turn)?;
        self.authority.ensure_storage_writable()?;
        owner.cell.obligation.set(ObservationObligation::Pending);
        owner
            .cell
            .obligation_origin
            .set(ObligationOrigin::Generated);
        owner.cell.confirmed_records.set(0);
        owner.cell.generated_plan.set(Some(plan));
        owner.cell.generated_stage_order.set(None);
        owner
            .cell
            .generated_progress
            .set(GeneratedProgress::Unconfirmed);
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
    /// Received record jobs still requiring their settlement boundary. Close
    /// retention and generated-plan reporting remain separate obligations.
    pub record_jobs: usize,
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
    fn stop_counter(&self, turn: &mut SessionTurn, name: &'static str) -> AuthorityError {
        let _ = self.storage_stopped(turn, PersistError::typed(PersistErrorKind::Counter, name));
        AuthorityError::CounterExhausted(name)
    }

    fn stop_time(&self, turn: &mut SessionTurn) -> AuthorityError {
        let _ = self.storage_stopped(
            turn,
            PersistError::typed(PersistErrorKind::Counter, "Timer deadline overflow"),
        );
        AuthorityError::TimeOverflow
    }

    fn received_order_exhausted(
        &self,
        turn: &mut SessionTurn,
        identity: ObservationIdentity,
    ) -> Result<(), AuthorityError> {
        let state = self.state.borrow();
        let scope = state
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == identity.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        let Some(mut observed_tag) = identity.tag.or(scope.current_tag) else {
            // An epoch-only pure harness has no accepted full tag to invent.
            // Preserve the original typed terminal boundary rather than guessing.
            drop(state);
            let _ = self.stop_counter(turn, "RecordAdmissionOrder");
            return Ok(());
        };
        if identity.tag.is_none() {
            observed_tag.connection = identity.epoch;
        }
        let failure = TerminalFailure {
            stream: identity.stream,
            connection: scope.binding.connection,
            observed_tag,
            current_epoch: scope.binding.epoch,
            context: state.prefix.ok_or(AuthorityError::NotRegistered)?.context,
            stamp: identity.stamp,
            input_class: match identity.class {
                ObservationClass::Connected => InputClass::Connected,
                ObservationClass::Pong => InputClass::Pong,
                ObservationClass::Disconnected => InputClass::Disconnected,
                _ => InputClass::Raw,
            },
            attempt: identity
                .attempts
                .map_or(AttemptIdentity::NotRaw, |(_, last)| {
                    AttemptIdentity::Candidate(last)
                }),
            cause: FailureCause::CounterExhausted("RecordAdmissionOrder"),
        };
        drop(state);
        self.terminate(turn, failure)?;
        Ok(())
    }

    fn validate_record_order(&self, owner: &WorkOwner) -> Result<(), AuthorityError> {
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        let state = self.state.borrow();
        let target = owner.cell.active_record_order();
        let earlier = state
            .work
            .iter()
            .filter(|cell| {
                cell.sequence.get() != owner.id()
                    && cell.required_record_pending()
                    && cell
                        .observation
                        .get()
                        .is_some_and(|original| original.stream == identity.stream)
            })
            .filter_map(|cell| {
                cell.active_record_order()
                    .filter(|order| target.is_none_or(|target| *order < target))
                    .map(|order| (order, cell.sequence.get()))
            })
            .min_by_key(|(order, _)| *order);
        if let Some((_, earlier_work_id)) = earlier {
            return Err(AuthorityError::TimerOrderBlocked { earlier_work_id });
        }
        let scope = state
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == identity.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        if let Some(work_id) = scope.frozen_timeout
            && work_id != owner.id()
        {
            return Err(AuthorityError::TimerPlanInProgress { work_id });
        }
        Ok(())
    }

    fn validate_scope_progress(&self, stream: StreamId) -> Result<(), AuthorityError> {
        let state = self.state.borrow();
        if let Some((_, earlier_work_id)) = state
            .work
            .iter()
            .filter(|cell| {
                cell.required_record_pending()
                    && cell
                        .observation
                        .get()
                        .is_some_and(|original| original.stream == stream)
            })
            .filter_map(|cell| {
                cell.active_record_order()
                    .map(|order| (order, cell.sequence.get()))
            })
            .min_by_key(|(order, _)| *order)
        {
            return Err(AuthorityError::TimerOrderBlocked { earlier_work_id });
        }
        let scope = state
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        if let Some(work_id) = scope.frozen_timeout {
            return Err(AuthorityError::TimerPlanInProgress { work_id });
        }
        Ok(())
    }

    fn timer_active(
        &self,
        owner: &WorkOwner,
        timer: TimerOriginal,
    ) -> Result<bool, AuthorityError> {
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        let ObservationClass::Timer { deadline_ns, .. } = identity.class else {
            return Err(AuthorityError::TimerAuthorityRequired);
        };
        let state = self.state.borrow();
        let scope = state
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == identity.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        Ok(state.storage_stopped.is_none()
            && matches!(
                state.lifecycle,
                SessionLifecycle::Open | SessionLifecycle::FailedDiagnostic
            )
            && scope.failure.is_none()
            && scope.binding.epoch == identity.epoch
            && scope.confirmed_down != Some(identity.epoch)
            && scope.schedule_generation == timer.generation
            && scope.schedule == Some((timer.kind, identity.epoch, deadline_ns))
            && scope.queued_timer == Some((owner.id(), timer.generation)))
    }

    fn validate_timer_close(&self, owner: &WorkOwner) -> Result<(), AuthorityError> {
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        let state = self.state.borrow();
        let scope = state
            .scopes
            .iter()
            .find(|scope| scope.binding.stream == identity.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        let installs_close = match scope.close.identity.get() {
            None => true,
            Some(close)
                if close.stream == identity.stream
                    && close.connection == scope.binding.connection
                    && close.epoch == identity.epoch
                    && close.storage == CloseStorage::WorkOwner(owner.id()) =>
            {
                false
            }
            Some(close)
                if close.stream == identity.stream
                    && close.connection == scope.binding.connection
                    && close.epoch < identity.epoch
                    && scope.close.state.get() == CloseState::Settled =>
            {
                true
            }
            Some(_) => return Err(AuthorityError::TimerCloseConflict),
        };
        if installs_close && owner.cell.references.get() >= MAX_WORK_SHARES {
            return Err(AuthorityError::WorkShareExhausted);
        }
        Ok(())
    }

    fn reserve_timer_close(&self, owner: &WorkOwner) -> Result<(), AuthorityError> {
        self.validate_timer_close(owner)?;
        let identity = owner
            .cell
            .observation
            .get()
            .ok_or(AuthorityError::InvalidOwner)?;
        let mut state = self.state.borrow_mut();
        let scope = state
            .scopes
            .iter_mut()
            .find(|scope| scope.binding.stream == identity.stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        if scope
            .close
            .identity
            .get()
            .is_none_or(|close| close.epoch < identity.epoch)
        {
            let retained = owner.share()?;
            scope.close.identity.set(Some(CloseIdentity {
                stream: identity.stream,
                connection: scope.binding.connection,
                epoch: identity.epoch,
                storage: CloseStorage::WorkOwner(owner.id()),
            }));
            scope.close.state.set(CloseState::Pending);
            scope.close.ready.set(false);
            *scope.close.work.borrow_mut() = Some(retained);
            owner.cell.close_scope.set(Some(identity.stream));
        }
        scope.frozen_timeout = Some(owner.id());
        scope.revoke_ping();
        Ok(())
    }

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
                record_admission_counter: 0,
                heartbeat_policy: HeartbeatPolicy::SupervisorV2,
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
        // Successful finalization fixes the reports. A readonly synchronization
        // remains a no-op, preserving consumed ticket/proof error priority.
        if state.lifecycle == SessionLifecycle::Finalized {
            return Ok(());
        }
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
        for scope in &mut state.scopes {
            scope.disable_schedule();
            if scope.frozen_timeout.is_some() {
                scope.close.ready.set(true);
            }
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
                scope.close.ready.set(true);
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
        self.validate_turn(turn)?;
        {
            let state = self.state.borrow();
            let scope = state
                .scopes
                .iter()
                .find(|scope| scope.binding.stream == stream)
                .ok_or(AuthorityError::InvalidBinding)?;
            if scope.binding.epoch != expected {
                return Err(AuthorityError::CommandRevoked);
            }
            if state.storage_stopped.is_some() {
                return Err(AuthorityError::StorageStopped);
            }
            if matches!(
                state.lifecycle,
                SessionLifecycle::DiagnosticClosed | SessionLifecycle::Finalized
            ) {
                return Err(AuthorityError::SessionClosed);
            }
            if let Some(work_id) = scope.frozen_timeout {
                return Err(AuthorityError::TimerPlanInProgress { work_id });
            }
        }
        self.validate_scope_progress(stream)?;
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
        let next_tag = EpochTag {
            connection: next,
            subscription: subscription_next,
            book: book_next,
            ..tag
        };
        drop(state);
        self.synchronize_obligations(turn)?;
        self.ensure_storage_writable()?;
        let mut state = self.state.borrow_mut();
        let scope = state
            .scopes
            .iter_mut()
            .find(|scope| scope.binding.stream == stream)
            .ok_or(AuthorityError::InvalidBinding)?;
        scope.current_tag = Some(next_tag);
        scope.binding.epoch = next;
        scope.disable_schedule();
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
        heartbeat_policy: HeartbeatPolicy,
    ) -> Result<SupervisorSessionHandle, AuthorityError> {
        self.validate_turn(turn)?;
        if self.state.borrow().registered {
            return Err(AuthorityError::AlreadyRegistered);
        }
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
                schedule: None,
                schedule_generation: 0,
                timer_id: 0,
                queued_timer: None,
                frozen_timeout: None,
                pending_ping: None,
                close: Rc::new(CloseCell {
                    identity: Cell::new(None),
                    state: Cell::new(CloseState::Pending),
                    work: RefCell::new(None),
                    ready: Cell::new(true),
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
                    received_progress: Cell::new(ReceivedProgress::Unconfirmed),
                    generated_plan: Cell::new(None),
                    generated_progress: Cell::new(GeneratedProgress::Unconfirmed),
                    abandoned_kind: Cell::new(None),
                    obligation_origin: Cell::new(ObligationOrigin::Received),
                    record_admission_order: Cell::new(None),
                    generated_stage_order: Cell::new(None),
                    timer: Cell::new(None),
                })
            })
            .collect();
        s.prefix = Some(prefix);
        s.heartbeat_policy = heartbeat_policy;
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
    fn ensure_storage_overlay_writable(&self) -> Result<(), AuthorityError> {
        let s = self.state.borrow();
        if s.storage_stopped.is_some() {
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
        cell.received_progress.set(ReceivedProgress::Unconfirmed);
        cell.generated_plan.set(None);
        cell.generated_progress.set(GeneratedProgress::Unconfirmed);
        cell.abandoned_kind.set(None);
        cell.obligation_origin.set(ObligationOrigin::Received);
        cell.record_admission_order.set(None);
        cell.generated_stage_order.set(None);
        cell.timer.set(None);
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

    fn ensure_terminal_mutation_allowed(&self) -> Result<(), AuthorityError> {
        if self.state.borrow().lifecycle == SessionLifecycle::Finalized {
            return Err(AuthorityError::SessionClosed);
        }
        Ok(())
    }

    pub fn terminate(
        &self,
        turn: &mut SessionTurn,
        failure: TerminalFailure,
    ) -> Result<TerminationReport, AuthorityError> {
        self.validate_turn(turn)?;
        if !self.state.borrow().scopes.iter().any(|scope| {
            scope.binding.stream == failure.stream && scope.binding.connection == failure.connection
        }) {
            return Err(AuthorityError::InvalidBinding);
        }
        self.ensure_terminal_mutation_allowed()?;
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
                s.scopes[index].disable_schedule();
                if s.scopes[index].frozen_timeout.is_some() {
                    s.scopes[index].close.ready.set(true);
                }
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
        self.validate_turn(turn)?;
        self.ensure_terminal_mutation_allowed()?;
        self.synchronize_obligations(turn)?;
        let mut s = self.state.borrow_mut();
        if s.storage_stopped.is_none() {
            s.storage_stopped = Some(error);
        }
        for scope in &mut s.scopes {
            scope.disable_schedule();
            if scope.frozen_timeout.is_some() {
                scope.close.ready.set(true);
            }
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

    fn ensure_close_service(&self) -> Result<(), AuthorityError> {
        if self.state.borrow().lifecycle == SessionLifecycle::Finalized {
            return Err(AuthorityError::SessionClosed);
        }
        Ok(())
    }

    pub fn mandatory_close(
        &self,
        turn: &mut SessionTurn,
        stream: StreamId,
        epoch: ConnectionEpoch,
        work: Option<&WorkOwner>,
    ) -> Result<CloseOwnerRef, AuthorityError> {
        self.validate_turn(turn)?;
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
        self.ensure_close_service()?;
        if let Some(work) = work
            && let Some(timer) = work.cell.timer.get()
        {
            if timer.plan != TimerPlan::Timeout {
                return Err(AuthorityError::TimerAuthorityRequired);
            }
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
                .filter(|close| {
                    close.stream == stream
                        && close.epoch == epoch
                        && close.storage == CloseStorage::WorkOwner(work.id())
                })
                .ok_or(AuthorityError::TimerCloseConflict)?;
            return Ok(CloseOwnerRef {
                authority: self.clone(),
                identity,
            });
        }
        self.synchronize_obligations(turn)?;
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
        scope.close.ready.set(true);
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
                            ready: scope.close.ready.get(),
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
        if let Err(error) = self.ensure_close_service() {
            return CloseLeaseReport::Rejected(error);
        }
        if cell.state.get() == CloseState::Settled {
            return CloseLeaseReport::AlreadySettled;
        }
        if !cell.ready.get() {
            return CloseLeaseReport::Rejected(AuthorityError::CloseNotReady);
        }
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
        if let Err(error) = self.ensure_close_service() {
            return CloseSettlementReport::Rejected(error);
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
        if work.cell.sequence.get() != work.sequence || work.cell.references.get() == 0 {
            return Err(AuthorityError::OwnerRetired);
        }
        if matches!(&kind, CommandKind::SendText { text } if text == "ping") {
            return Err(AuthorityError::TimerAuthorityRequired);
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
            timer_ping: None,
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
        if let Some(close) = command.close.as_ref() {
            if let Err(reason) = self.close_cell(&close.owner) {
                return DispatchReport::Denied { reason, command };
            }
            if let Err(reason) = self.ensure_close_service() {
                return DispatchReport::Denied { reason, command };
            }
            if !close.cell.ready.get() {
                return DispatchReport::Denied {
                    reason: AuthorityError::CloseNotReady,
                    command,
                };
            }
        }
        if let Some((work_id, generation)) = command.timer_ping {
            let state = self.state.borrow();
            let scope = state
                .scopes
                .iter()
                .find(|scope| scope.binding.stream == command.stream);
            if command.work.as_ref().is_none_or(|work| work.id() != work_id || work.cell.sequence.get() != work_id || work.cell.timer.get().is_none_or(|timer| !matches!(timer.plan, TimerPlan::Ping { next_generation, .. } if next_generation == generation)))
                || scope.is_none_or(|scope| scope.schedule_generation != generation || !matches!(scope.schedule, Some((TimerKind::Timeout, epoch, _)) if epoch == command.epoch) || scope.frozen_timeout.is_some()) {
                return DispatchReport::Revoked(AuthorityError::CommandRevoked);
            }
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
            record_jobs: s.work.iter().filter(|cell| cell.blocks_marker()).count(),
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
        for scope in &mut s.scopes {
            scope.disable_schedule();
        }
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
                for scope in &mut s.scopes {
                    scope.disable_schedule();
                }
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
                HeartbeatPolicy::SupervisorV2,
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

    // Private arithmetic modeling: MAX cannot be reached by a bounded-duration
    // public test. The actual admitted ordinal is set by admit_observation.
    #[test]
    fn p2_gap_current_tail_extends_at_admission_max_without_new_order_or_work() {
        let (authority, mut turn, handle) = session(10);
        authority.state.borrow_mut().record_admission_counter = u64::MAX - 1;
        let first = CaptureAttemptNo::new(1).unwrap();
        let identity = ObservationIdentity {
            stream: StreamId::new(1).unwrap(),
            epoch: ConnectionEpoch::new(1).unwrap(),
            stamp: ReceiveStamp {
                unix_ns: 5,
                monotonic_ns: 5,
            },
            class: ObservationClass::Gap,
            tag: Some(accepted_binding().tag),
            attempts: Some((first, first)),
            loss_count: Some(1),
        };
        let reserved_first = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        let gap = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        handle.admit_observation(&mut turn, &gap, identity).unwrap();
        assert_eq!(gap.cell.record_admission_order.get(), Some(u64::MAX));
        let report = authority.ownership_report();
        let cut = gap.cut_side();
        for last in 2..=33 {
            let extended = ObservationIdentity {
                attempts: Some((first, CaptureAttemptNo::new(last).unwrap())),
                loss_count: Some(last),
                ..identity
            };
            assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(true));
            handle
                .extend_gap_observation(&mut turn, &gap, extended)
                .unwrap();
            assert_eq!(gap.cell.observation.get(), Some(extended));
            assert_eq!(gap.cell.record_admission_order.get(), Some(u64::MAX));
            assert_eq!(authority.state.borrow().record_admission_counter, u64::MAX);
            assert_eq!(gap.cut_side(), cut);
            assert_eq!(authority.ownership_report(), report);
            assert_eq!(reserved_first.cell.record_admission_order.get(), None);
        }
    }

    #[test]
    fn p2_gap_barrier_survives_receipt_settlement_drop_and_work_cell_reuse() {
        let (authority, mut turn, handle) = session(10);
        let first = CaptureAttemptNo::new(1).unwrap();
        let identity = ObservationIdentity {
            stream: StreamId::new(1).unwrap(),
            epoch: ConnectionEpoch::new(1).unwrap(),
            stamp: ReceiveStamp {
                unix_ns: 5,
                monotonic_ns: 5,
            },
            class: ObservationClass::Gap,
            tag: Some(accepted_binding().tag),
            attempts: Some((first, first)),
            loss_count: Some(1),
        };
        let gap = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        handle.admit_observation(&mut turn, &gap, identity).unwrap();
        let barrier = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        handle
            .admit_observation(
                &mut turn,
                &barrier,
                ObservationIdentity {
                    class: ObservationClass::Raw,
                    stamp: ReceiveStamp {
                        unix_ns: 6,
                        monotonic_ns: 6,
                    },
                    loss_count: None,
                    ..identity
                },
            )
            .unwrap();
        let expanded = ObservationIdentity {
            attempts: Some((first, CaptureAttemptNo::new(2).unwrap())),
            loss_count: Some(2),
            ..identity
        };
        let old_order = gap.cell.record_admission_order.get();
        // Modeling only: a public two-scope concrete Durable regression below
        // actually writes and settles the neighbor before reusing its cell.
        barrier.cell.received_progress.set(ReceivedProgress::Raw);
        barrier.cell.obligation.set(ObservationObligation::Settled);
        let barrier_cell = barrier.cell.clone();
        drop(barrier);
        let reused = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        assert!(Rc::ptr_eq(&barrier_cell, &reused.cell));
        assert_eq!(reused.cell.record_admission_order.get(), None);
        assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(false));
        let status = authority.status();
        let report = authority.ownership_report();
        for _ in 0..100 {
            assert_eq!(
                handle.extend_gap_observation(&mut turn, &gap, expanded),
                Err(AuthorityError::InvalidOwner)
            );
            assert_eq!(gap.cell.observation.get(), Some(identity));
            assert_eq!(gap.cell.record_admission_order.get(), old_order);
            assert_eq!(authority.state.borrow().record_admission_counter, 2);
            assert_eq!(authority.status(), status);
            assert_eq!(authority.ownership_report(), report);
        }
    }

    fn admit_control(
        handle: &SupervisorSessionHandle,
        turn: &mut SessionTurn,
        owner: &WorkOwner,
        class: ObservationClass,
        stamp: ReceiveStamp,
    ) {
        handle
            .admit_observation(
                turn,
                owner,
                ObservationIdentity {
                    stream: StreamId::new(1).unwrap(),
                    epoch: ConnectionEpoch::new(1).unwrap(),
                    tag: None,
                    class,
                    stamp,
                    attempts: None,
                    loss_count: None,
                },
            )
            .unwrap();
        owner.set_kind(turn, WorkKind::InFlightObservation).unwrap();
    }

    fn observation_frame(
        authority: &CaptureSessionAuthority,
        identity: ObservationIdentity,
        value: Control,
    ) -> RecordFrame {
        let prefix = authority.prefix().unwrap();
        RecordFrame {
            record_no: prefix.next_record,
            segment_no: prefix.segment,
            value: Record::Control(ControlRecord {
                context: WireContext {
                    unix_ns: LocalUnixNs::new(identity.stamp.unix_ns),
                    monotonic_ns: MonotonicNs::new(identity.stamp.monotonic_ns),
                    context: InputContext::Active(prefix.context),
                },
                value,
            }),
        }
    }

    fn timer_frame(authority: &CaptureSessionAuthority, owner: &WorkOwner) -> RecordFrame {
        let identity = owner.cell.observation.get().unwrap();
        let ObservationClass::Timer {
            timer_id,
            deadline_ns,
        } = identity.class
        else {
            panic!("original Timer");
        };
        observation_frame(
            authority,
            identity,
            Control::Timer {
                stream: identity.stream,
                timer_id,
                deadline_ns,
            },
        )
    }

    fn timer_fixture() -> (
        CaptureSessionAuthority,
        SessionTurn,
        SupervisorSessionHandle,
        BoundRecordSink,
        Rc<Cell<usize>>,
    ) {
        let (authority, mut turn, handle) = session(10);
        authority
            .set_accepted_stream_bindings(&mut turn, &[accepted_binding()])
            .unwrap();
        let calls = Rc::new(Cell::new(0));
        let mut sink = authority
            .bind_sink(
                &mut turn,
                Box::new(Writer {
                    calls: calls.clone(),
                    mode: 0,
                }),
            )
            .unwrap();
        let up = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        admit_control(
            &handle,
            &mut turn,
            &up,
            ObservationClass::Connected,
            ReceiveStamp {
                unix_ns: 1,
                monotonic_ns: 2,
            },
        );
        let frame = observation_frame(
            &authority,
            up.cell.observation.get().unwrap(),
            Control::Transport {
                connection: ConnectionId::new(1).unwrap(),
                epoch: ConnectionEpoch::new(1).unwrap(),
                value: Transport::Up,
            },
        );
        sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &up)
            .unwrap();
        handle
            .complete_observation(&mut turn, &sink, &up, None)
            .unwrap();
        drop(up);
        (authority, turn, handle, sink, calls)
    }

    fn due_timer(
        authority: &CaptureSessionAuthority,
        turn: &mut SessionTurn,
        handle: &SupervisorSessionHandle,
    ) -> WorkOwner {
        let deadline = authority.state.borrow().scopes[0].schedule.unwrap().2;
        let TimerAdmission::Admitted(timer) = handle
            .admit_due_timer(
                turn,
                StreamId::new(1).unwrap(),
                ReceiveStamp {
                    unix_ns: 3,
                    monotonic_ns: deadline,
                },
            )
            .unwrap()
        else {
            panic!("original due Timer");
        };
        let owner = timer.into_owner();
        owner.set_kind(turn, WorkKind::InFlightObservation).unwrap();
        owner
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

    #[test]
    fn timer_generated_admission_max_preserves_frontiers_without_received_failure() {
        for counter in ["AdmissionOrder", "RecordAdmissionOrder", "TimerId"] {
            let (authority, mut turn, handle, _sink, calls) = timer_fixture();
            {
                let mut state = authority.state.borrow_mut();
                match counter {
                    "AdmissionOrder" => state.sequence = u64::MAX,
                    "RecordAdmissionOrder" => state.record_admission_counter = u64::MAX,
                    _ => state.scopes[0].timer_id = u64::MAX,
                }
            }
            let frontiers = {
                let state = authority.state.borrow();
                (
                    state.sequence,
                    state.record_admission_counter,
                    state.scopes[0].timer_id,
                    state.scopes[0].schedule_generation,
                )
            };
            assert_eq!(
                handle
                    .admit_due_timer(
                        &mut turn,
                        StreamId::new(1).unwrap(),
                        ReceiveStamp {
                            unix_ns: 8,
                            monotonic_ns: 30_000_000_002
                        }
                    )
                    .unwrap_err(),
                AuthorityError::CounterExhausted(counter)
            );
            let state = authority.state.borrow();
            assert_eq!(
                (
                    state.sequence,
                    state.record_admission_counter,
                    state.scopes[0].timer_id,
                    state.scopes[0].schedule_generation
                ),
                frontiers
            );
            assert!(state.scopes[0].queued_timer.is_none());
            assert!(state.scopes[0].schedule.is_none());
            assert!(state.storage_stopped.is_some());
            assert!(state.first_failure.is_none());
            assert!(state.scopes[0].failure.is_none());
            assert_eq!(calls.get(), 1);
            drop(state);
            assert_eq!(authority.ownership_report().work_used, 0);
        }
    }

    #[test]
    fn received_record_order_max_retains_exact_terminal_original_without_fake_order() {
        let (authority, mut turn, handle, _sink, calls) = timer_fixture();
        let work = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        authority.state.borrow_mut().record_admission_counter = u64::MAX;
        let identity = ObservationIdentity {
            stream: StreamId::new(1).unwrap(),
            epoch: ConnectionEpoch::new(1).unwrap(),
            class: ObservationClass::Raw,
            tag: Some(accepted_binding().tag),
            stamp: ReceiveStamp {
                unix_ns: 71,
                monotonic_ns: 72,
            },
            attempts: Some((
                CaptureAttemptNo::new(9).unwrap(),
                CaptureAttemptNo::new(9).unwrap(),
            )),
            loss_count: None,
        };
        assert_eq!(
            handle.admit_observation(&mut turn, &work, identity),
            Err(AuthorityError::CounterExhausted("RecordAdmissionOrder"))
        );
        assert_eq!(work.cell.observation.get(), None);
        assert_eq!(work.cell.record_admission_order.get(), None);
        assert_eq!(work.cell.obligation.get(), ObservationObligation::None);
        let failure = authority.terminal_failure(identity.stream).unwrap();
        assert_eq!(failure.stamp, identity.stamp);
        assert_eq!(failure.observed_tag, identity.tag.unwrap());
        assert_eq!(
            failure.attempt,
            AttemptIdentity::Candidate(CaptureAttemptNo::new(9).unwrap())
        );
        assert_eq!(
            failure.cause,
            FailureCause::CounterExhausted("RecordAdmissionOrder")
        );
        assert_eq!(failure.input_class, InputClass::Raw);
        assert_eq!(authority.state.borrow().record_admission_counter, u64::MAX);
        assert!(authority.status().storage_stopped.is_none());
        assert_eq!(
            authority.state.borrow().scopes[0]
                .close
                .identity
                .get()
                .unwrap()
                .storage,
            CloseStorage::ReservedTerminal
        );
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn received_control_order_max_with_accepted_registry_keeps_exact_terminal_provenance() {
        for (class, input_class) in [
            (ObservationClass::Connected, InputClass::Connected),
            (ObservationClass::Pong, InputClass::Pong),
            (ObservationClass::Disconnected, InputClass::Disconnected),
        ] {
            let (authority, mut turn, handle, _sink, calls) = timer_fixture();
            let work = handle
                .reserve_work(&mut turn, WorkKind::QueuedObservation)
                .unwrap();
            authority.state.borrow_mut().record_admission_counter = u64::MAX;
            let identity = ObservationIdentity {
                stream: StreamId::new(1).unwrap(),
                epoch: ConnectionEpoch::new(1).unwrap(),
                class,
                tag: None,
                stamp: ReceiveStamp {
                    unix_ns: 81,
                    monotonic_ns: 82,
                },
                attempts: None,
                loss_count: None,
            };
            let prefix = authority.prefix().unwrap();
            let watermark = authority.trusted_watermark(WatermarkKind::Durable);
            assert_eq!(
                handle.admit_observation(&mut turn, &work, identity),
                Err(AuthorityError::CounterExhausted("RecordAdmissionOrder"))
            );
            assert_eq!(work.cell.observation.get(), None);
            assert_eq!(work.cell.record_admission_order.get(), None);
            assert_eq!(work.cell.obligation.get(), ObservationObligation::None);
            let failure = authority.terminal_failure(identity.stream).unwrap();
            assert_eq!(failure.input_class, input_class);
            assert_eq!(failure.stamp, identity.stamp);
            assert_eq!(failure.observed_tag, accepted_binding().tag);
            assert_eq!(failure.current_epoch, identity.epoch);
            assert_eq!(failure.context, prefix.context);
            assert_eq!(failure.attempt, AttemptIdentity::NotRaw);
            assert_eq!(
                failure.cause,
                FailureCause::CounterExhausted("RecordAdmissionOrder")
            );
            assert_eq!(
                authority.state.borrow().scopes[0]
                    .close
                    .identity
                    .get()
                    .unwrap()
                    .storage,
                CloseStorage::ReservedTerminal
            );
            assert_eq!(authority.state.borrow().record_admission_counter, u64::MAX);
            assert_eq!(authority.prefix().unwrap(), prefix);
            assert_eq!(
                authority.trusted_watermark(WatermarkKind::Durable),
                watermark
            );
            assert_eq!(authority.status().storage_stopped, None);
            assert_eq!(calls.get(), 1);
        }
    }

    #[test]
    fn timer_capacity_rejection_and_duplicate_due_preserve_scheduler_frontiers() {
        let (authority, mut turn, handle, _sink, calls) = timer_fixture();
        let blockers: Vec<_> = (0..authority.ownership_report().work_limit)
            .map(|_| {
                handle
                    .reserve_work(&mut turn, WorkKind::PendingPlan)
                    .unwrap()
            })
            .collect();
        let before = authority.ownership_report();
        let frontiers = {
            let state = authority.state.borrow();
            (
                state.sequence,
                state.record_admission_counter,
                state.scopes[0].timer_id,
                state.scopes[0].schedule_generation,
                state.scopes[0].schedule,
            )
        };
        let stamp = ReceiveStamp {
            unix_ns: 80,
            monotonic_ns: 30_000_000_002,
        };
        assert_eq!(
            handle
                .admit_due_timer(&mut turn, StreamId::new(1).unwrap(), stamp)
                .unwrap_err(),
            AuthorityError::WorkExhausted
        );
        assert_eq!(authority.ownership_report(), before);
        assert_eq!(
            {
                let state = authority.state.borrow();
                (
                    state.sequence,
                    state.record_admission_counter,
                    state.scopes[0].timer_id,
                    state.scopes[0].schedule_generation,
                    state.scopes[0].schedule,
                )
            },
            frontiers
        );
        assert_eq!(authority.status().storage_stopped, None);
        drop(blockers);
        let TimerAdmission::Admitted(timer) = handle
            .admit_due_timer(&mut turn, StreamId::new(1).unwrap(), stamp)
            .unwrap()
        else {
            panic!("retry due original");
        };
        let after = authority.ownership_report();
        assert!(
            matches!(handle.admit_due_timer(&mut turn, timer.identity().stream, stamp).unwrap(), TimerAdmission::AlreadyQueued { original_work_id } if original_work_id == timer.owner().id())
        );
        assert_eq!(authority.ownership_report(), after);
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn timer_ping_schedule_and_receipt_max_stop_before_backend_without_entitlement() {
        for counter in ["TimerScheduleGeneration", "WorkReceipt", "RecordNo"] {
            let (authority, mut turn, handle, mut sink, calls) = timer_fixture();
            if counter == "TimerScheduleGeneration" {
                authority.state.borrow_mut().scopes[0].schedule_generation = u64::MAX;
            }
            let owner = due_timer(&authority, &mut turn, &handle);
            if counter == "WorkReceipt" {
                owner.cell.confirmed_records.set(usize::MAX);
            }
            if counter == "RecordNo" {
                authority
                    .state
                    .borrow_mut()
                    .prefix
                    .as_mut()
                    .unwrap()
                    .next_record = RecordNo::new(u64::MAX).unwrap();
            }
            let before = authority.prefix().unwrap();
            let frame = timer_frame(&authority, &owner);
            assert_eq!(
                sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &owner),
                Err(PersistBoundaryError::Authority(
                    AuthorityError::CounterExhausted(counter)
                ))
            );
            assert_eq!(authority.prefix().unwrap(), before);
            assert_eq!(calls.get(), 1);
            assert_eq!(
                owner.cell.received_progress.get(),
                ReceivedProgress::Unconfirmed
            );
            assert_eq!(owner.cell.obligation.get(), ObservationObligation::Pending);
            assert!(authority.status().storage_stopped.is_some());
            assert_eq!(
                handle.take_timer_ping(&mut turn, &owner).unwrap_err(),
                AuthorityError::CommandRevoked
            );
            assert!(authority.state.borrow().scopes[0].pending_ping.is_none());
            assert!(authority.state.borrow().scopes[0].schedule.is_none());
        }
    }

    #[test]
    fn timer_timeout_record_max_retains_same_ready_close_without_any_receipt() {
        let (authority, mut turn, handle, mut sink, calls) = timer_fixture();
        let ping = due_timer(&authority, &mut turn, &handle);
        let frame = timer_frame(&authority, &ping);
        sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &ping)
            .unwrap();
        handle
            .complete_observation(&mut turn, &sink, &ping, None)
            .unwrap();
        let lease = handle.take_timer_ping(&mut turn, &ping).unwrap();
        drop(lease);
        drop(ping);
        let timeout = due_timer(&authority, &mut turn, &handle);
        authority
            .state
            .borrow_mut()
            .prefix
            .as_mut()
            .unwrap()
            .next_record = RecordNo::new(u64::MAX - 1).unwrap();
        let before = authority.prefix().unwrap();
        let frame = timer_frame(&authority, &timeout);
        assert_eq!(
            sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &timeout),
            Err(PersistBoundaryError::Authority(
                AuthorityError::CounterExhausted("RecordNo")
            ))
        );
        assert_eq!(calls.get(), 2);
        assert_eq!(authority.prefix().unwrap(), before);
        assert_eq!(
            timeout.cell.received_progress.get(),
            ReceivedProgress::Unconfirmed
        );
        let TimerProgressView::TimerThenDown {
            timer_confirmed,
            down_confirmed,
            close,
        } = handle.timer_progress(&mut turn, &timeout).unwrap()
        else {
            panic!("retained timeout plan");
        };
        assert!(!timer_confirmed && !down_confirmed && close.ready);
        assert_eq!(close.owner.storage(), CloseStorage::WorkOwner(timeout.id()));
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, &timeout, None),
            Err(AuthorityError::StorageStopped)
        );
        let CloseLeaseReport::Leased(lease) = authority.reclaim_close(&mut turn, close.owner)
        else {
            panic!("same terminal close ready");
        };
        assert!(matches!(
            authority.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        assert_eq!(
            timeout.cell.obligation.get(),
            ObservationObligation::Pending
        );
    }

    #[test]
    fn timer_alias_preflight_rejection_is_retryable_without_plan_or_schedule_mutation() {
        let (authority, mut turn, handle, mut sink, calls) = timer_fixture();
        let owner = due_timer(&authority, &mut turn, &handle);
        let aliases: Vec<_> = (1..MAX_WORK_SHARES)
            .map(|_| owner.share().unwrap())
            .collect();
        let original_schedule = authority.state.borrow().scopes[0].schedule;
        let before = authority.ownership_report();
        let frame = timer_frame(&authority, &owner);
        assert_eq!(
            sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &owner),
            Err(PersistBoundaryError::Authority(
                AuthorityError::WorkShareExhausted
            ))
        );
        assert_eq!(authority.ownership_report(), before);
        assert_eq!(
            authority.state.borrow().scopes[0].schedule,
            original_schedule
        );
        assert_eq!(
            handle.timer_progress(&mut turn, &owner).unwrap(),
            TimerProgressView::Unselected
        );
        assert_eq!(authority.status().storage_stopped, None);
        assert_eq!(calls.get(), 1);
        drop(aliases);
        sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &owner)
            .unwrap();
        assert_eq!(calls.get(), 2);
        assert!(matches!(
            handle.timer_progress(&mut turn, &owner).unwrap(),
            TimerProgressView::TimerOnlyPing {
                timer_confirmed: true,
                ping_taken: false
            }
        ));
        assert!(handle.take_timer_ping(&mut turn, &owner).is_ok());
    }

    #[test]
    fn timer_pure_bad_frame_and_incomplete_completion_preserve_unrelated_abandonment() {
        let (authority, mut turn, handle, mut sink, calls) = timer_fixture();
        let timer = due_timer(&authority, &mut turn, &handle);
        let later = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        handle
            .admit_observation(
                &mut turn,
                &later,
                ObservationIdentity {
                    stream: StreamId::new(1).unwrap(),
                    epoch: ConnectionEpoch::new(1).unwrap(),
                    tag: Some(accepted_binding().tag),
                    class: ObservationClass::Raw,
                    stamp: ReceiveStamp {
                        unix_ns: 7,
                        monotonic_ns: 8,
                    },
                    attempts: Some((
                        CaptureAttemptNo::new(1).unwrap(),
                        CaptureAttemptNo::new(1).unwrap(),
                    )),
                    loss_count: None,
                },
            )
            .unwrap();
        drop(later);
        let before = authority.status();
        let ledger = authority.ownership_report();
        let mut wrong = timer_frame(&authority, &timer);
        if let Record::Control(record) = &mut wrong.value {
            record.context.unix_ns = LocalUnixNs::new(999);
        }
        assert_eq!(
            sink.persist_owned(&mut turn, &wrong, RecordingGate::Durable, &timer),
            Err(PersistBoundaryError::Authority(
                AuthorityError::InvalidBinding
            ))
        );
        assert_eq!(
            handle.complete_observation(&mut turn, &sink, &timer, None),
            Err(AuthorityError::NotQuiescent)
        );
        assert_eq!(authority.status(), before);
        assert_eq!(authority.ownership_report(), ledger);
        assert_eq!(calls.get(), 1);
        assert_eq!(
            handle.timer_progress(&mut turn, &timer).unwrap(),
            TimerProgressView::Unselected
        );
        authority.synchronize_obligations(&mut turn).unwrap();
        assert!(authority.status().storage_stopped.is_some());
        assert_eq!(authority.state.borrow().scopes[0].schedule, None);
    }

    fn retained_timeout_plan(
        authority: &CaptureSessionAuthority,
        turn: &mut SessionTurn,
        handle: &SupervisorSessionHandle,
        sink: &mut BoundRecordSink,
    ) -> WorkOwner {
        let (timeout, ping) = retained_timeout_plan_with_ping(authority, turn, handle, sink);
        drop(ping);
        timeout
    }

    fn retained_timeout_plan_with_ping(
        authority: &CaptureSessionAuthority,
        turn: &mut SessionTurn,
        handle: &SupervisorSessionHandle,
        sink: &mut BoundRecordSink,
    ) -> (WorkOwner, CommandLease) {
        let ping = due_timer(authority, turn, handle);
        sink.persist_owned(
            turn,
            &timer_frame(authority, &ping),
            RecordingGate::Durable,
            &ping,
        )
        .unwrap();
        handle
            .complete_observation(turn, sink, &ping, None)
            .unwrap();
        let ping_lease = handle.take_timer_ping(turn, &ping).unwrap();
        drop(ping);
        let timeout = due_timer(authority, turn, handle);
        sink.persist_owned(
            turn,
            &timer_frame(authority, &timeout),
            RecordingGate::Durable,
            &timeout,
        )
        .unwrap();
        let identity = timeout.cell.observation.get().unwrap();
        sink.persist_owned(
            turn,
            &observation_frame(
                authority,
                identity,
                Control::Transport {
                    connection: ConnectionId::new(1).unwrap(),
                    epoch: identity.epoch,
                    value: Transport::Down,
                },
            ),
            RecordingGate::Durable,
            &timeout,
        )
        .unwrap();
        handle
            .complete_observation(turn, sink, &timeout, None)
            .unwrap();
        timeout.set_kind(turn, WorkKind::PendingPlan).unwrap();
        handle.retain_generated_plan(turn, &timeout).unwrap();
        (timeout, ping_lease)
    }

    #[test]
    fn generated_timer_h1_order_max_activates_only_after_original_close_and_keeps_provenance() {
        let (authority, mut turn, handle, mut sink, calls) = timer_fixture();
        let owner = retained_timeout_plan(&authority, &mut turn, &handle, &mut sink);
        let identity = owner.cell.observation.get().unwrap();
        let plan = owner.cell.generated_plan.get().unwrap();
        let frame = observation_frame(
            &authority,
            identity,
            Control::EpochAdvance {
                change: EpochChange::Connection {
                    owner: plan.connection,
                    expected: plan.original.connection,
                    next: plan.next_connection,
                },
                reason: Reason::Reconnect,
            },
        );
        authority.state.borrow_mut().record_admission_counter = u64::MAX;
        let before = authority.ownership_report();
        assert_eq!(
            sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &owner),
            Err(PersistBoundaryError::Authority(
                AuthorityError::NotQuiescent
            ))
        );
        assert_eq!(authority.ownership_report(), before);
        assert_eq!(owner.cell.generated_stage_order.get(), None);
        assert_eq!(authority.status().storage_stopped, None);
        let close = authority
            .mandatory_close(&mut turn, identity.stream, identity.epoch, Some(&owner))
            .unwrap();
        let CloseLeaseReport::Leased(lease) = authority.reclaim_close(&mut turn, close) else {
            panic!("original Close");
        };
        assert!(matches!(
            authority.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        assert_eq!(
            sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &owner),
            Err(PersistBoundaryError::Authority(
                AuthorityError::CounterExhausted("RecordAdmissionOrder")
            ))
        );
        assert_eq!(calls.get(), 4);
        assert_eq!(
            owner.cell.generated_progress.get(),
            GeneratedProgress::Unconfirmed
        );
        assert_eq!(owner.cell.generated_stage_order.get(), None);
        assert_eq!(
            owner.cell.received_progress.get(),
            ReceivedProgress::TimerDown
        );
        assert!(authority.status().storage_stopped.is_some());
        assert!(authority.status().first_failure.is_none());
        assert_eq!(
            handle.cancel_generated_plan(&mut turn, &owner),
            Err(AuthorityError::StorageStopped)
        );
    }

    #[test]
    fn timer_frontiers_survive_three_fresh_h1_receipts_and_actual_epoch_replacement() {
        let (authority, mut turn, handle, mut sink, calls) = timer_fixture();
        let (owner, old_ping) =
            retained_timeout_plan_with_ping(&authority, &mut turn, &handle, &mut sink);
        let identity = owner.cell.observation.get().unwrap();
        let plan = owner.cell.generated_plan.get().unwrap();
        let close = authority
            .mandatory_close(&mut turn, identity.stream, identity.epoch, Some(&owner))
            .unwrap();
        let retired_close = close.clone();
        let CloseLeaseReport::Leased(lease) = authority.reclaim_close(&mut turn, close) else {
            panic!("original Close");
        };
        assert!(matches!(
            authority.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        let generation = authority.state.borrow().scopes[0].schedule_generation;
        let original_timer_id = authority.state.borrow().scopes[0].timer_id;
        let first_order = authority.state.borrow().record_admission_counter;
        for (index, change) in [
            EpochChange::Connection {
                owner: plan.connection,
                expected: plan.original.connection,
                next: plan.next_connection,
            },
            EpochChange::Subscription {
                owner: identity.stream,
                expected: plan.original.subscription,
                next: plan.next_subscription,
            },
            EpochChange::Book {
                owner: plan.book,
                expected: plan.original.book.unwrap(),
                next: plan.next_book,
            },
        ]
        .into_iter()
        .enumerate()
        {
            let frame = observation_frame(
                &authority,
                identity,
                Control::EpochAdvance {
                    change,
                    reason: Reason::Reconnect,
                },
            );
            sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &owner)
                .unwrap();
            assert_eq!(
                authority.state.borrow().record_admission_counter,
                first_order + index as u64 + 1
            );
            assert_eq!(owner.cell.generated_stage_order.get(), None);
        }
        handle
            .complete_observation(&mut turn, &sink, &owner, None)
            .unwrap();
        authority
            .advance_epoch(
                &mut turn,
                identity.stream,
                identity.epoch,
                plan.next_connection,
            )
            .unwrap();
        assert_eq!(calls.get(), 7);
        assert_eq!(
            authority.state.borrow().scopes[0].schedule_generation,
            generation
        );
        assert_eq!(
            authority.state.borrow().scopes[0].timer_id,
            original_timer_id
        );
        assert!(authority.state.borrow().scopes[0].schedule.is_none());
        assert_eq!(
            owner.cell.received_progress.get(),
            ReceivedProgress::TimerDown
        );
        let before_old_ping = authority.ownership_report();
        let prefix_before_old_ping = authority.prefix().unwrap();
        let status_before_old_ping = authority.status();
        let mut ping_callbacks = 0;
        assert!(matches!(
            authority.dispatch(&mut turn, old_ping, |_| {
                ping_callbacks += 1;
                Ok::<_, ()>(())
            }),
            DispatchReport::Revoked(AuthorityError::CommandRevoked)
        ));
        assert_eq!(ping_callbacks, 0);
        assert_eq!(
            authority.ownership_report().work_used,
            before_old_ping.work_used - 1
        );
        assert_eq!(
            authority.ownership_report().work_references,
            before_old_ping.work_references - 1
        );
        assert_eq!(authority.prefix().unwrap(), prefix_before_old_ping);
        assert_eq!(authority.status(), status_before_old_ping);
        assert_eq!(calls.get(), 7);
        drop(owner);
        let up = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        let new_identity = ObservationIdentity {
            epoch: plan.next_connection,
            class: ObservationClass::Connected,
            stamp: ReceiveStamp {
                unix_ns: 90,
                monotonic_ns: 91,
            },
            tag: None,
            attempts: None,
            loss_count: None,
            stream: identity.stream,
        };
        handle
            .admit_observation(&mut turn, &up, new_identity)
            .unwrap();
        up.set_kind(&mut turn, WorkKind::InFlightObservation)
            .unwrap();
        let frame = observation_frame(
            &authority,
            new_identity,
            Control::Transport {
                connection: plan.connection,
                epoch: plan.next_connection,
                value: Transport::Up,
            },
        );
        sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &up)
            .unwrap();
        handle
            .complete_observation(&mut turn, &sink, &up, None)
            .unwrap();
        drop(up);
        let TimerAdmission::Admitted(next) = handle
            .admit_due_timer(
                &mut turn,
                identity.stream,
                ReceiveStamp {
                    unix_ns: 92,
                    monotonic_ns: 30_000_000_091,
                },
            )
            .unwrap()
        else {
            panic!("next epoch original");
        };
        assert!(
            matches!(next.identity().class, ObservationClass::Timer { timer_id, .. } if timer_id == original_timer_id + 1)
        );
        assert_eq!(
            authority.state.borrow().scopes[0].schedule_generation,
            generation + 1
        );
        assert_eq!(next.identity().epoch, plan.next_connection);
        assert_eq!(
            authority.state.borrow().record_admission_counter,
            first_order + 5
        );
        let ping = next.into_owner();
        ping.set_kind(&mut turn, WorkKind::InFlightObservation)
            .unwrap();
        sink.persist_owned(
            &mut turn,
            &timer_frame(&authority, &ping),
            RecordingGate::Durable,
            &ping,
        )
        .unwrap();
        handle
            .complete_observation(&mut turn, &sink, &ping, None)
            .unwrap();
        let current_ping = handle.take_timer_ping(&mut turn, &ping).unwrap();
        let mut current_ping_callbacks = 0;
        assert!(matches!(
            authority.dispatch(&mut turn, current_ping, |command| {
                assert_eq!(command.epoch, plan.next_connection);
                assert!(matches!(command.kind, CommandKind::SendText { text } if text == "ping"));
                current_ping_callbacks += 1;
                Ok::<_, ()>(())
            }),
            DispatchReport::Dispatched
        ));
        assert_eq!(current_ping_callbacks, 1);
        drop(ping);
        let second = due_timer(&authority, &mut turn, &handle);
        sink.persist_owned(
            &mut turn,
            &timer_frame(&authority, &second),
            RecordingGate::Durable,
            &second,
        )
        .unwrap();
        let TimerProgressView::TimerThenDown {
            close,
            down_confirmed,
            ..
        } = handle.timer_progress(&mut turn, &second).unwrap()
        else {
            panic!("second active Timeout");
        };
        assert!(!close.ready && !down_confirmed);
        assert_eq!(close.owner.storage(), CloseStorage::WorkOwner(second.id()));
        assert_eq!(close.owner.epoch(), plan.next_connection);
        assert_ne!(close.owner, retired_close);
        assert!(matches!(
            authority.reclaim_close(&mut turn, retired_close),
            CloseLeaseReport::Rejected(AuthorityError::OwnerRetired)
        ));
        let second_identity = second.cell.observation.get().unwrap();
        let down = observation_frame(
            &authority,
            second_identity,
            Control::Transport {
                connection: plan.connection,
                epoch: plan.next_connection,
                value: Transport::Down,
            },
        );
        sink.persist_owned(&mut turn, &down, RecordingGate::Durable, &second)
            .unwrap();
        handle
            .complete_observation(&mut turn, &sink, &second, None)
            .unwrap();
        second.set_kind(&mut turn, WorkKind::PendingPlan).unwrap();
        handle.retain_generated_plan(&mut turn, &second).unwrap();
        let CloseLeaseReport::Leased(lease) = authority.reclaim_close(&mut turn, close.owner)
        else {
            panic!("second original ready Close");
        };
        assert!(matches!(
            authority.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        let second_plan = second.cell.generated_plan.get().unwrap();
        for change in [
            EpochChange::Connection {
                owner: second_plan.connection,
                expected: second_plan.original.connection,
                next: second_plan.next_connection,
            },
            EpochChange::Subscription {
                owner: second_identity.stream,
                expected: second_plan.original.subscription,
                next: second_plan.next_subscription,
            },
            EpochChange::Book {
                owner: second_plan.book,
                expected: second_plan.original.book.unwrap(),
                next: second_plan.next_book,
            },
        ] {
            let frame = observation_frame(
                &authority,
                second_identity,
                Control::EpochAdvance {
                    change,
                    reason: Reason::Reconnect,
                },
            );
            sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &second)
                .unwrap();
        }
        handle
            .complete_observation(&mut turn, &sink, &second, None)
            .unwrap();
        authority
            .advance_epoch(
                &mut turn,
                second_identity.stream,
                second_identity.epoch,
                second_plan.next_connection,
            )
            .unwrap();
        assert_eq!(second_plan.next_connection.get(), 3);
        assert_eq!(calls.get(), 14);
        assert_eq!(
            authority.state.borrow().scopes[0].timer_id,
            original_timer_id + 2
        );
        assert_eq!(
            authority.state.borrow().scopes[0].schedule_generation,
            generation + 2
        );
        assert_eq!(
            authority.state.borrow().record_admission_counter,
            first_order + 9
        );
        assert_eq!(authority.unsettled_summary().record_jobs, 0);
        assert_eq!(authority.status().storage_stopped, None);
    }

    #[test]
    fn timer_closing_and_scoped_revocation_at_max_confirm_only_original_obsolete_timer() {
        for closing in [false, true] {
            let (authority, mut turn, handle, mut sink, calls) = timer_fixture();
            {
                let mut state = authority.state.borrow_mut();
                state.scopes[0].schedule_generation = u64::MAX;
                state.scopes[0].timer_id = u64::MAX - 1;
            }
            let owner = due_timer(&authority, &mut turn, &handle);
            assert!(matches!(
                owner.cell.observation.get().unwrap().class,
                ObservationClass::Timer {
                    timer_id: u64::MAX,
                    ..
                }
            ));
            assert_eq!(owner.cell.timer.get().unwrap().generation, u64::MAX);
            if closing {
                let _ticket = authority.begin_finalization(&mut turn).unwrap();
            } else {
                let failure = TerminalFailure {
                    stream: StreamId::new(1).unwrap(),
                    connection: ConnectionId::new(1).unwrap(),
                    observed_tag: accepted_binding().tag,
                    current_epoch: ConnectionEpoch::new(1).unwrap(),
                    context: handle.prefix().context,
                    stamp: ReceiveStamp {
                        unix_ns: 101,
                        monotonic_ns: 102,
                    },
                    input_class: InputClass::Raw,
                    attempt: AttemptIdentity::Candidate(CaptureAttemptNo::new(1).unwrap()),
                    cause: FailureCause::QueueOverflow,
                };
                drop(handle.terminate(&mut turn, failure).unwrap().close);
            }
            let frame = timer_frame(&authority, &owner);
            sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &owner)
                .unwrap();
            handle
                .complete_observation(&mut turn, &sink, &owner, None)
                .unwrap();
            assert!(matches!(
                handle.timer_progress(&mut turn, &owner).unwrap(),
                TimerProgressView::TimerOnlyObsolete {
                    timer_confirmed: true
                }
            ));
            assert_eq!(
                handle.take_timer_ping(&mut turn, &owner).unwrap_err(),
                AuthorityError::CommandRevoked
            );
            assert_eq!(
                authority.state.borrow().scopes[0].schedule_generation,
                u64::MAX
            );
            assert_eq!(authority.state.borrow().scopes[0].timer_id, u64::MAX);
            assert_eq!(authority.status().storage_stopped, None);
            assert_eq!(calls.get(), 2);
            assert_eq!(owner.cell.received_progress.get(), ReceivedProgress::Timer);
            assert_eq!(owner.cell.obligation.get(), ObservationObligation::Settled);
        }
    }

    #[test]
    fn timer_backend_observes_same_work_unready_close_and_conversion_cannot_bypass_down() {
        struct Observer {
            authority: CaptureSessionAuthority,
            calls: Rc<Cell<usize>>,
            observed: Rc<Cell<Option<u64>>>,
        }
        impl SessionRecordWriter for Observer {
            fn persist(
                &mut self,
                frame: &RecordFrame,
                gate: RecordingGate,
            ) -> Result<PersistenceReceipt, PersistError> {
                self.calls.set(self.calls.get() + 1);
                let state = self.authority.state.borrow();
                let scope = &state.scopes[0];
                let work_id = scope
                    .frozen_timeout
                    .expect("Timeout selected before backend");
                let close = scope
                    .close
                    .identity
                    .get()
                    .expect("Close reserved before backend");
                assert_eq!(close.storage, CloseStorage::WorkOwner(work_id));
                assert_eq!(scope.close.state.get(), CloseState::Pending);
                assert!(!scope.close.ready.get());
                assert_eq!(scope.close.work.borrow().as_ref().unwrap().id(), work_id);
                let cell = state
                    .work
                    .iter()
                    .find(|cell| cell.sequence.get() == work_id)
                    .unwrap();
                assert_eq!(cell.received_progress.get(), ReceivedProgress::Unconfirmed);
                assert!(
                    matches!(&frame.value, Record::Control(record) if matches!(record.value, Control::Timer { .. }))
                );
                self.observed.set(Some(work_id));
                Ok(PersistenceReceipt {
                    through: frame.record_no,
                    achieved_gate: gate,
                })
            }
        }
        let (authority, mut turn, handle, mut sink, calls) = timer_fixture();
        let ping = due_timer(&authority, &mut turn, &handle);
        sink.persist_owned(
            &mut turn,
            &timer_frame(&authority, &ping),
            RecordingGate::Durable,
            &ping,
        )
        .unwrap();
        handle
            .complete_observation(&mut turn, &sink, &ping, None)
            .unwrap();
        drop(handle.take_timer_ping(&mut turn, &ping).unwrap());
        drop(ping);
        let timeout = due_timer(&authority, &mut turn, &handle);
        let observed = Rc::new(Cell::new(None));
        sink.writer = Box::new(Observer {
            authority: authority.clone(),
            calls: calls.clone(),
            observed: observed.clone(),
        });
        sink.persist_owned(
            &mut turn,
            &timer_frame(&authority, &timeout),
            RecordingGate::Durable,
            &timeout,
        )
        .unwrap();
        assert_eq!(observed.get(), Some(timeout.id()));
        let TimerProgressView::TimerThenDown {
            timer_confirmed,
            down_confirmed,
            close,
        } = handle.timer_progress(&mut turn, &timeout).unwrap()
        else {
            panic!("selected Timeout");
        };
        assert!(timer_confirmed && !down_confirmed && !close.ready);
        assert!(matches!(
            authority.reclaim_close(&mut turn, close.owner.clone()),
            CloseLeaseReport::Rejected(AuthorityError::CloseNotReady)
        ));
        // A trusted private fixture exercises the conversion guard separately;
        // public issuance cannot produce this unready lease.
        let cell = authority.close_cell(&close.owner).unwrap();
        let lease = CloseLease {
            owner: close.owner,
            cell,
            work: None,
            armed: false,
        };
        let before = authority.ownership_report();
        assert_eq!(
            lease.into_command().unwrap_err(),
            AuthorityError::CloseNotReady
        );
        assert_eq!(authority.ownership_report(), before);
        assert_eq!(calls.get(), 3);
        assert_eq!(
            timeout.cell.received_progress.get(),
            ReceivedProgress::Timer
        );
        assert_eq!(
            timeout.cell.obligation.get(),
            ObservationObligation::Pending
        );
        assert_eq!(authority.status().storage_stopped, None);
    }

    #[test]
    fn activated_generated_stage_order_blocks_later_control_then_receipt_removes_only_that_barrier()
    {
        let (authority, mut turn, handle, mut sink, calls) = timer_fixture();
        let owner = retained_timeout_plan(&authority, &mut turn, &handle, &mut sink);
        let identity = owner.cell.observation.get().unwrap();
        let plan = owner.cell.generated_plan.get().unwrap();
        let close = authority
            .mandatory_close(&mut turn, identity.stream, identity.epoch, Some(&owner))
            .unwrap();
        let CloseLeaseReport::Leased(lease) = authority.reclaim_close(&mut turn, close) else {
            panic!("same settled dependency");
        };
        assert!(matches!(
            authority.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
        assert!(!owner.cell.required_record_pending());
        assert_eq!(owner.cell.active_record_order(), None);
        // Represent the private in-flight activation point. SessionTurn makes
        // this point unobservable to a concurrent public admission.
        let activation = authority.state.borrow().record_admission_counter + 1;
        authority.state.borrow_mut().record_admission_counter = activation;
        owner.cell.generated_stage_order.set(Some(activation));
        let later = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        admit_control(
            &handle,
            &mut turn,
            &later,
            ObservationClass::Pong,
            ReceiveStamp {
                unix_ns: 80,
                monotonic_ns: 81,
            },
        );
        assert_eq!(
            authority.validate_record_order(&later),
            Err(AuthorityError::TimerOrderBlocked {
                earlier_work_id: owner.id()
            })
        );
        assert_eq!(owner.cell.active_record_order(), Some(activation));
        let frame = observation_frame(
            &authority,
            identity,
            Control::EpochAdvance {
                change: EpochChange::Connection {
                    owner: plan.connection,
                    expected: plan.original.connection,
                    next: plan.next_connection,
                },
                reason: Reason::Reconnect,
            },
        );
        sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &owner)
            .unwrap();
        assert_eq!(owner.cell.generated_stage_order.get(), None);
        assert!(!owner.cell.required_record_pending());
        assert_eq!(authority.validate_record_order(&later), Ok(()));
        handle
            .complete_observation(
                &mut turn,
                &sink,
                &later,
                Some((identity.stream, identity.epoch)),
            )
            .unwrap();
        assert_eq!(calls.get(), 5);
        assert_eq!(
            owner.cell.generated_progress.get(),
            GeneratedProgress::Connection
        );
        assert_eq!(owner.cell.obligation.get(), ObservationObligation::Pending);
        assert_eq!(authority.status().storage_stopped, None);
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
            authority.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| -> Result<(), ()> { panic!("settled Close effect") }
            ),
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
                .reserve_work(&mut turn, WorkKind::QueuedObservation)
                .unwrap();
            admit_control(
                &handle,
                &mut turn,
                &work,
                ObservationClass::Connected,
                ReceiveStamp {
                    unix_ns: 1,
                    monotonic_ns: 2,
                },
            );
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
            handle
                .complete_observation(&mut turn, &sink, &work, None)
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
    fn finalized_close_mint_and_reclaim_preserve_immutable_authority_state() {
        // Pure authority finalization; recording regressions independently
        // require actual Durable final seals and physical Complete bytes.
        for prior_close in [false, true] {
            let (authority, mut turn, handle) = session(10);
            let (foreign, mut foreign_turn, _) = session(10);
            let stream = StreamId::new(1).unwrap();
            let epoch = ConnectionEpoch::new(1).unwrap();
            let close = prior_close.then(|| {
                let close = handle
                    .mandatory_close(&mut turn, stream, epoch, None)
                    .unwrap();
                let CloseLeaseReport::Leased(lease) =
                    authority.reclaim_close(&mut turn, close.clone())
                else {
                    panic!("original Close");
                };
                assert!(matches!(
                    authority.dispatch(&mut turn, lease.into_command().unwrap(), |_| {
                        Ok::<_, ()>(())
                    }),
                    DispatchReport::Dispatched
                ));
                close
            });
            let ticket = authority.begin_finalization(&mut turn).unwrap();
            let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
                panic!("all original ownership settled");
            };
            authority.consume_proof(&mut turn, &mut proof).unwrap();
            authority.finalization_finished(&mut turn).unwrap();
            let snapshot = || {
                let state = authority.state.borrow();
                let scope = &state.scopes[0];
                (
                    authority.status(),
                    authority.ownership_report(),
                    authority.outstanding_close_owners(),
                    authority.prefix().unwrap(),
                    (
                        state.sequence,
                        state.record_admission_counter,
                        scope.schedule_generation,
                        scope.timer_id,
                    ),
                    (
                        scope.close.identity.get(),
                        scope.close.state.get(),
                        scope.close.ready.get(),
                    ),
                )
            };
            let before = snapshot();
            assert_eq!(before.0.lifecycle, SessionLifecycle::Finalized);
            assert_eq!(before.1.work_used, 0);
            assert_eq!(before.2.iter().count(), 0);
            for _ in 0..100 {
                assert_eq!(
                    handle
                        .mandatory_close(&mut turn, stream, epoch, None)
                        .unwrap_err(),
                    AuthorityError::SessionClosed
                );
                assert_eq!(
                    authority
                        .mandatory_close(&mut turn, stream, epoch, None)
                        .unwrap_err(),
                    AuthorityError::SessionClosed
                );
                if let Some(close) = &close {
                    assert!(matches!(
                        authority.reclaim_close(&mut turn, close.clone()),
                        CloseLeaseReport::Rejected(AuthorityError::SessionClosed)
                    ));
                    assert_eq!(
                        authority.close_state(stream, epoch).unwrap(),
                        CloseState::Settled
                    );
                }
                assert_eq!(snapshot(), before);
            }
            assert_eq!(
                authority
                    .mandatory_close(&mut foreign_turn, stream, epoch, None)
                    .unwrap_err(),
                AuthorityError::AuthorityMismatch
            );
            let foreign_close = foreign
                .mandatory_close(&mut foreign_turn, stream, epoch, None)
                .unwrap();
            assert!(matches!(
                authority.reclaim_close(&mut turn, foreign_close),
                CloseLeaseReport::Rejected(AuthorityError::AuthorityMismatch)
            ));
            assert_eq!(snapshot(), before);
            assert!(matches!(
                handle.quiesce(&mut turn, &ticket),
                QuiescenceReport::TicketConsumed
            ));
            assert_eq!(
                authority.consume_proof(&mut turn, &mut proof).unwrap_err(),
                AuthorityError::ProofConsumed
            );
            assert_eq!(snapshot(), before);
        }
    }

    #[test]
    fn finalized_close_defensive_conversion_and_dispatch_cannot_create_effect() {
        // Private defensive modeling: genuine quiescence cannot finalize while
        // a Pending/Leased Close exists. Only this fixture installs Finalized
        // beside that retained obligation; it is not public reachability.
        for leased in [false, true] {
            let (authority, mut turn, _) = session(10);
            let (foreign, mut foreign_turn, _) = session(10);
            let stream = StreamId::new(1).unwrap();
            let epoch = ConnectionEpoch::new(1).unwrap();
            let close = authority
                .mandatory_close(&mut turn, stream, epoch, None)
                .unwrap();
            let cell = authority.close_cell(&close).unwrap();
            let command = leased.then(|| {
                let CloseLeaseReport::Leased(lease) =
                    authority.reclaim_close(&mut turn, close.clone())
                else {
                    panic!("one genuine lease before modeled Finalized");
                };
                lease.into_command().unwrap()
            });
            authority.state.borrow_mut().lifecycle = SessionLifecycle::Finalized;
            let snapshot = || {
                (
                    authority.status(),
                    authority.ownership_report(),
                    authority.outstanding_close_owners(),
                    cell.identity.get(),
                    cell.state.get(),
                    cell.ready.get(),
                )
            };
            let before = snapshot();
            assert!(matches!(
                authority.reclaim_close(&mut turn, close.clone()),
                CloseLeaseReport::Rejected(AuthorityError::SessionClosed)
            ));
            // A disarmed private fixture isolates conversion from affine Drop
            // housekeeping. No public constructor can issue this second lease.
            let modeled_lease = CloseLease {
                owner: close.clone(),
                cell: Rc::clone(&cell),
                work: None,
                armed: false,
            };
            assert_eq!(
                modeled_lease.into_command().unwrap_err(),
                AuthorityError::SessionClosed
            );
            assert_eq!(snapshot(), before);
            let mut effects = 0;
            if let Some(command) = command {
                let DispatchReport::Denied { reason, command } =
                    authority.dispatch(&mut turn, command, |_| {
                        effects += 1;
                        Ok::<_, ()>(())
                    })
                else {
                    panic!("Finalized preserves and denies the held command");
                };
                assert_eq!(reason, AuthorityError::SessionClosed);
                assert_eq!(command.close_owner(), Some(&close));
                assert_eq!(effects, 0);
                assert_eq!(snapshot(), before);
                let DispatchReport::Denied { reason, command } =
                    foreign.dispatch(&mut foreign_turn, command, |_| {
                        effects += 1;
                        Ok::<_, ()>(())
                    })
                else {
                    panic!("foreign authority preserves the same command");
                };
                assert_eq!(reason, AuthorityError::AuthorityMismatch);
                assert_eq!(effects, 0);
                assert_eq!(snapshot(), before);
                // Return only the modeled lifecycle to its actual prior Open
                // value; the same returned genuine command remains lawful.
                authority.state.borrow_mut().lifecycle = SessionLifecycle::Open;
                assert!(matches!(
                    authority.dispatch(&mut turn, command, |_| {
                        effects += 1;
                        Ok::<_, ()>(())
                    }),
                    DispatchReport::Dispatched
                ));
                assert_eq!(effects, 1);
                assert_eq!(cell.state.get(), CloseState::Settled);
            }
        }
    }

    fn qa42_terminal_failure(handle: &SupervisorSessionHandle) -> TerminalFailure {
        TerminalFailure {
            stream: StreamId::new(1).unwrap(),
            connection: ConnectionId::new(1).unwrap(),
            current_epoch: ConnectionEpoch::new(1).unwrap(),
            observed_tag: accepted_binding().tag,
            context: handle.prefix().context,
            stamp: ReceiveStamp {
                unix_ns: 700,
                monotonic_ns: 700,
            },
            input_class: InputClass::Raw,
            attempt: AttemptIdentity::Candidate(CaptureAttemptNo::new(2).unwrap()),
            cause: FailureCause::QueueOverflow,
        }
    }

    #[test]
    fn qa42_finalized_terminal_routes_preserve_reports_and_consumed_authorization() {
        // Actual pure-authority finalization, not a filesystem/seal fixture.
        // Recording covers the corresponding concrete Durable public routes.
        let (authority, mut turn, handle) = session(10);
        let (foreign, mut foreign_turn, _) = session(10);
        let failure = qa42_terminal_failure(&handle);
        let error = PersistError::typed(PersistErrorKind::Io, "late terminal stop");
        let ticket = authority.begin_finalization(&mut turn).unwrap();
        let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
            panic!("genuine settled authority must issue its sole proof");
        };
        authority.consume_proof(&mut turn, &mut proof).unwrap();
        authority.finalization_finished(&mut turn).unwrap();
        let snapshot = || {
            let state = authority.state.borrow();
            let scope = &state.scopes[0];
            (
                authority.status(),
                authority.ownership_report(),
                authority.unsettled_summary(),
                authority.prefix().unwrap(),
                state.trusted_watermarks,
                (
                    state.sequence,
                    state.record_admission_counter,
                    state.ticket,
                    state.proof_issued,
                    state.proof_consumed,
                    state.finalization_authorized,
                ),
                (
                    scope.failure,
                    scope.schedule,
                    scope.schedule_generation,
                    scope.timer_id,
                    scope.queued_timer,
                    scope.frozen_timeout,
                ),
            )
        };
        let before = snapshot();
        let foreign_before = foreign.status();
        for _ in 0..100 {
            assert_eq!(
                handle.terminate(&mut turn, failure).unwrap_err(),
                AuthorityError::SessionClosed
            );
            assert_eq!(snapshot(), before);
            assert_eq!(
                authority.terminate(&mut turn, failure).unwrap_err(),
                AuthorityError::SessionClosed
            );
            assert_eq!(snapshot(), before);
            assert_eq!(
                authority.storage_stopped(&mut turn, error),
                Err(AuthorityError::SessionClosed)
            );
            assert_eq!(snapshot(), before);
            assert_eq!(
                authority.hard_stop(&mut turn, error),
                Err(AuthorityError::SessionClosed)
            );
            assert_eq!(authority.synchronize_obligations(&mut turn), Ok(()));
            assert_eq!(snapshot(), before);
        }
        let mut wrong = failure;
        wrong.connection = ConnectionId::new(2).unwrap();
        assert_eq!(
            handle.terminate(&mut turn, wrong).unwrap_err(),
            AuthorityError::InvalidBinding
        );
        assert_eq!(
            handle.terminate(&mut foreign_turn, wrong).unwrap_err(),
            AuthorityError::AuthorityMismatch
        );
        assert_eq!(
            authority.storage_stopped(&mut foreign_turn, error),
            Err(AuthorityError::AuthorityMismatch)
        );
        assert_eq!(
            authority.hard_stop(&mut foreign_turn, error),
            Err(AuthorityError::AuthorityMismatch)
        );
        assert_eq!(
            authority.synchronize_obligations(&mut foreign_turn),
            Err(AuthorityError::AuthorityMismatch)
        );
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::TicketConsumed
        ));
        assert_eq!(
            authority.consume_proof(&mut turn, &mut proof),
            Err(AuthorityError::ProofConsumed)
        );
        assert_eq!(snapshot(), before);
        assert_eq!(foreign.status(), foreign_before);
    }

    #[test]
    fn qa42_finalized_sync_cannot_latch_modeled_abandonment_or_change_error_priority() {
        let (authority, mut turn, handle) = session(10);
        let identity = ObservationIdentity {
            stream: StreamId::new(1).unwrap(),
            epoch: ConnectionEpoch::new(1).unwrap(),
            stamp: ReceiveStamp {
                unix_ns: 5,
                monotonic_ns: 5,
            },
            class: ObservationClass::Raw,
            tag: Some(accepted_binding().tag),
            attempts: Some((
                CaptureAttemptNo::new(1).unwrap(),
                CaptureAttemptNo::new(1).unwrap(),
            )),
            loss_count: None,
        };
        let work = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        handle
            .admit_observation(&mut turn, &work, identity)
            .unwrap();
        drop(work);
        // Private defensive modeling only: lawful public quiescence cannot
        // finalize an abandoned received obligation. Preserve that obligation
        // without letting a late sync or rejected terminal call rewrite reports.
        authority.state.borrow_mut().lifecycle = SessionLifecycle::Finalized;
        let before = (
            authority.status(),
            authority.ownership_report(),
            authority.unsettled_summary(),
            authority.prefix().unwrap(),
        );
        assert!(!before.0.failed);
        assert!(before.0.first_abandonment.is_none());
        assert_eq!(before.1.work_used, 1);
        let mut wrong = qa42_terminal_failure(&handle);
        wrong.stream = StreamId::new(2).unwrap();
        for _ in 0..100 {
            assert_eq!(authority.synchronize_obligations(&mut turn), Ok(()));
            assert_eq!(
                authority.terminate(&mut turn, wrong).unwrap_err(),
                AuthorityError::InvalidBinding
            );
            assert_eq!(
                handle
                    .terminate(&mut turn, qa42_terminal_failure(&handle))
                    .unwrap_err(),
                AuthorityError::SessionClosed
            );
            assert_eq!(
                (
                    authority.status(),
                    authority.ownership_report(),
                    authority.unsettled_summary(),
                    authority.prefix().unwrap()
                ),
                before
            );
        }
    }

    #[test]
    fn qa42_closing_failure_before_and_after_ready_retains_original_close_and_revokes_seals() {
        for phase in 0..3 {
            let (authority, mut turn, handle) = session(10);
            let ticket = authority.begin_finalization(&mut turn).unwrap();
            let mut proof = if phase > 0 {
                let QuiescenceReport::Ready(proof) = handle.quiesce(&mut turn, &ticket) else {
                    panic!("healthy original ready control");
                };
                Some(proof)
            } else {
                None
            };
            if phase == 2 {
                authority
                    .consume_proof(&mut turn, proof.as_mut().unwrap())
                    .unwrap();
                authority.ensure_finalization_authorized().unwrap();
            }
            let original = qa42_terminal_failure(&handle);
            let terminal = handle.terminate(&mut turn, original).unwrap();
            assert!(terminal.first);
            assert_eq!(authority.terminal_failure(original.stream), Some(original));
            assert_eq!(
                authority.status().lifecycle,
                SessionLifecycle::DiagnosticClosing
            );
            assert_eq!(authority.status().storage_stopped, None);
            assert_eq!(
                authority.ensure_finalization_authorized(),
                Err(AuthorityError::ArchiveFailed)
            );
            assert!(matches!(
                handle.quiesce(&mut turn, &ticket),
                QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
            ));
            if let Some(proof) = proof.as_mut() {
                assert_eq!(
                    authority.consume_proof(&mut turn, proof),
                    Err(AuthorityError::ArchiveFailed)
                );
            }
            assert_eq!(
                authority.finalization_finished(&mut turn),
                Err(AuthorityError::ArchiveFailed)
            );
            assert_eq!(
                authority.ensure_admission_open(&turn),
                Err(AuthorityError::SessionClosing)
            );
            assert_eq!(
                authority.state.borrow().ticket,
                if phase == 0 {
                    TicketState::Invalidated
                } else {
                    TicketState::Consumed
                }
            );
            let close = terminal.close_owner.clone();
            drop(terminal.close);
            let CloseLeaseReport::Leased(lease) = authority.reclaim_close(&mut turn, close.clone())
            else {
                panic!("original fail-safe Close survives Closing failure")
            };
            assert!(matches!(
                authority.dispatch(&mut turn, lease.into_command().unwrap(), |_| Err::<(), _>(
                    "ambiguous original Close"
                )),
                DispatchReport::DispatchFailed {
                    effect: AmbiguousEffect::Unknown,
                    ..
                }
            ));
            let CloseLeaseReport::Leased(lease) = authority.reclaim_close(&mut turn, close.clone())
            else {
                panic!("same original Close remains retryable")
            };
            let mut effects = 0;
            assert!(matches!(
                authority.dispatch(&mut turn, lease.into_command().unwrap(), |_| {
                    effects += 1;
                    Ok::<_, ()>(())
                }),
                DispatchReport::Dispatched
            ));
            assert_eq!(effects, 1);
            assert_eq!(
                authority.close_state(original.stream, original.current_epoch),
                Ok(CloseState::Settled)
            );
            assert_eq!(authority.ownership_report().work_used, 0);
            let again = handle.terminate(&mut turn, original).unwrap();
            assert!(!again.first);
            assert!(again.close.is_none());
            assert_eq!(again.close_owner, close);
        }
    }

    #[test]
    fn qa42_failed_scope_gap_keeps_original_identity_while_healthy_postcut_tail_extends_at_max() {
        for fail_gap_scope in [false, true] {
            // Epoch-only pure domain registry and trusted receipt writer. The
            // public recording counterparts own full accepted WAL bindings.
            let (base, base_turn, base_handle) = session(10);
            let binding = base.binding();
            let prefix = base_handle.prefix();
            drop((base, base_turn, base_handle));
            let (authority, mut turn) = CaptureSessionAuthority::new(binding);
            let scopes: [ScopeBinding; 2] = std::array::from_fn(|index| ScopeBinding {
                stream: StreamId::new(index as u32 + 1).unwrap(),
                connection: ConnectionId::new(index as u32 + 1).unwrap(),
                epoch: ConnectionEpoch::new(1).unwrap(),
            });
            let handle = authority
                .register_supervisor(
                    &mut turn,
                    &scopes,
                    RetentionBudget {
                        item_cap: 9,
                        raw_frame_limit: 1,
                        raw_byte_limit: 64,
                        max_message_bytes: 64,
                    },
                    prefix,
                    HeartbeatPolicy::SupervisorV2,
                )
                .unwrap();
            let mut first_failure = qa42_terminal_failure(&handle);
            first_failure.attempt = AttemptIdentity::Candidate(CaptureAttemptNo::new(1).unwrap());
            let first = handle.terminate(&mut turn, first_failure).unwrap();
            assert!(matches!(
                authority.dispatch(
                    &mut turn,
                    first.close.unwrap().into_command().unwrap(),
                    |_| Ok::<_, ()>(())
                ),
                DispatchReport::Dispatched
            ));
            // Private arithmetic modeling only; MAX is not publicly reached in
            // this bounded test. Actual successful GAP admission sets its order.
            authority.state.borrow_mut().record_admission_counter = u64::MAX - 1;
            let one = CaptureAttemptNo::new(1).unwrap();
            let original = ObservationIdentity {
                stream: scopes[1].stream,
                epoch: scopes[1].epoch,
                stamp: ReceiveStamp {
                    unix_ns: 701,
                    monotonic_ns: 701,
                },
                class: ObservationClass::Gap,
                tag: Some(accepted_binding().tag),
                attempts: Some((one, one)),
                loss_count: Some(1),
            };
            let gap = handle
                .reserve_work(&mut turn, WorkKind::QueuedObservation)
                .unwrap();
            handle.admit_observation(&mut turn, &gap, original).unwrap();
            assert_eq!(gap.cell.record_admission_order.get(), Some(u64::MAX));
            assert_eq!(gap.cut_side(), CutSide::PostCut);
            let mut second_failure = qa42_terminal_failure(&handle);
            second_failure.stream = scopes[1].stream;
            second_failure.connection = scopes[1].connection;
            second_failure.stamp = ReceiveStamp {
                unix_ns: 702,
                monotonic_ns: 702,
            };
            if fail_gap_scope {
                let second = handle.terminate(&mut turn, second_failure).unwrap();
                assert!(second.first);
                assert!(matches!(
                    authority.dispatch(
                        &mut turn,
                        second.close.unwrap().into_command().unwrap(),
                        |_| Ok::<_, ()>(())
                    ),
                    DispatchReport::Dispatched
                ));
            }
            let snapshot = || {
                let state = authority.state.borrow();
                (
                    authority.status(),
                    authority.ownership_report(),
                    authority.unsettled_summary(),
                    authority.prefix().unwrap(),
                    (state.sequence, state.record_admission_counter),
                    (
                        gap.id(),
                        gap.cut_side(),
                        gap.cell.record_admission_order.get(),
                    ),
                )
            };
            let before = snapshot();
            let mut expected = original;
            if fail_gap_scope {
                let expanded = ObservationIdentity {
                    attempts: Some((one, CaptureAttemptNo::new(2).unwrap())),
                    loss_count: Some(2),
                    ..original
                };
                for _ in 0..100 {
                    assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(false));
                    assert_eq!(
                        handle.extend_gap_observation(&mut turn, &gap, expanded),
                        Err(AuthorityError::InvalidOwner)
                    );
                    assert_eq!(gap.cell.observation.get(), Some(original));
                    assert_eq!(snapshot(), before);
                }
                assert_eq!(
                    authority.terminal_failure(scopes[1].stream),
                    Some(second_failure)
                );
            } else {
                for last in 2..=33 {
                    expected = ObservationIdentity {
                        attempts: Some((one, CaptureAttemptNo::new(last).unwrap())),
                        loss_count: Some(last),
                        ..original
                    };
                    assert_eq!(handle.gap_extension_eligible(&turn, &gap), Ok(true));
                    handle
                        .extend_gap_observation(&mut turn, &gap, expected)
                        .unwrap();
                    assert_eq!(gap.cell.observation.get(), Some(expected));
                    assert_eq!(snapshot(), before);
                }
                assert_eq!(authority.terminal_failure(scopes[1].stream), None);
            }
            assert_eq!(
                authority.terminal_failure(scopes[0].stream),
                Some(first_failure)
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
            let descriptor = authority.archive_failure_observation().unwrap();
            let marker = RecordFrame {
                record_no: handle.prefix().next_record,
                segment_no: handle.prefix().segment,
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
            sink.persist_marker(&mut turn, &marker, RecordingGate::Durable)
                .unwrap();
            gap.set_kind(&mut turn, WorkKind::InFlightObservation)
                .unwrap();
            let frame = RecordFrame {
                record_no: handle.prefix().next_record,
                segment_no: handle.prefix().segment,
                value: Record::Gap(crate::record::Gap {
                    context: WireContext {
                        unix_ns: LocalUnixNs::new(expected.stamp.unix_ns),
                        monotonic_ns: MonotonicNs::new(expected.stamp.monotonic_ns),
                        context: InputContext::Active(handle.prefix().context),
                    },
                    scope: crate::record::GapScope::ExplicitTargets(vec![
                        crate::record::GapTarget {
                            stream: expected.stream,
                            tag: expected.tag.unwrap(),
                            range: expected.attempts,
                            loss_count: expected.loss_count,
                        },
                    ]),
                    reason: Reason::QueueOverflow,
                }),
            };
            sink.persist_owned(&mut turn, &frame, RecordingGate::Durable, &gap)
                .unwrap();
            handle
                .complete_observation(&mut turn, &sink, &gap, None)
                .unwrap();
            assert_eq!(
                handle.complete_observation(&mut turn, &sink, &gap, None),
                Err(AuthorityError::InvalidOwner)
            );
            assert_eq!(gap.cell.observation.get(), Some(expected));
            assert_eq!(calls.get(), 2);
            drop(gap);
            assert_eq!(authority.ownership_report().work_used, 0);
            assert_eq!(authority.state.borrow().record_admission_counter, u64::MAX);
        }
    }

    #[test]
    fn qa42_actual_requested_layouts() {
        // Current worker private requested-layout observation, adapted from the
        // preserved independent layout probe. This is not public behavior/RSS.
        let actual = (
            std::mem::size_of::<WorkCell>(),
            std::mem::size_of::<ScopeState>(),
            std::mem::size_of::<AuthorityState>(),
            std::mem::size_of::<TimerOriginal>(),
            std::mem::size_of::<CloseCell>(),
            std::mem::size_of::<CommandLease>(),
            std::mem::size_of::<WorkOwner>(),
        );
        eprintln!(
            "WORKER_QA42_LAYOUT WorkCell={} ScopeState={} AuthorityState={} TimerOriginal={} CloseCell={} CommandLease={} WorkOwner={} pointer_bytes={}",
            actual.0,
            actual.1,
            actual.2,
            actual.3,
            actual.4,
            actual.5,
            actual.6,
            std::mem::size_of::<usize>()
        );
        assert_eq!(
            std::mem::size_of::<usize>(),
            8,
            "reported x64 layout profile"
        );
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
                HeartbeatPolicy::SupervisorV2,
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
            authority.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| -> Result<(), ()> { panic!("settled Close effect") }
            ),
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
            authority.dispatch(
                &mut turn,
                lease.into_command().unwrap(),
                |_| Ok::<_, ()>(())
            ),
            DispatchReport::Dispatched
        ));
    }
    #[test]
    fn external_failed_observation_owns_cut_and_original_descriptor_until_prefix_drain() {
        let (authority, mut turn, handle) = session(10);
        let work = handle
            .reserve_work(&mut turn, WorkKind::QueuedObservation)
            .unwrap();
        admit_control(
            &handle,
            &mut turn,
            &work,
            ObservationClass::Connected,
            ReceiveStamp {
                unix_ns: 3,
                monotonic_ns: 4,
            },
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
        handle
            .complete_observation(&mut turn, &sink, &work, None)
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
