use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;

use domain::event::{ClockScope, MonotonicSample};
use domain::identity::{
    Channel, ConnectionEpoch, ConnectionId, EpochTag, IdentityError, RecordNo, StreamBinding,
    StreamId,
};
use domain::policy::{DurabilityMode, HealthPolicy, PolicyError, SilenceRule};
use domain::record::{
    BookEvidenceKind, EpochChange, Freshness, GapScope, Reason, Transport, VerificationEvidence,
    WarmupEvidence,
};

use crate::{Books50Frame, ContinuityClassifier, ContinuityOutcome};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HealthError {
    Identity(IdentityError),
    Policy(PolicyError),
    RecordOrder {
        expected: RecordNo,
        actual: RecordNo,
    },
    IncomparableClock,
    UnknownStream(StreamId),
    UnknownConnection(ConnectionId),
    InvalidObservation(&'static str),
}

impl From<IdentityError> for HealthError {
    fn from(value: IdentityError) -> Self {
        Self::Identity(value)
    }
}

impl From<PolicyError> for HealthError {
    fn from(value: PolicyError) -> Self {
        Self::Policy(value)
    }
}

impl fmt::Display for HealthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for HealthError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingLimit {
    Frames,
    RawBytes,
    Outputs,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BookInvalidReason {
    Gap(Reason),
    TransportDown,
    ContinuityGap,
    ResetOrDiscontinuity,
    SnapshotIntervalMismatch,
    NeedsSnapshot,
    UnexpectedSnapshot,
    PendingOverflow(PendingLimit),
    PendingTimeout,
    PendingDeadlineOverflow,
    ProofConflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BookValidity {
    NoSnapshot,
    Warming,
    Usable,
    Invalid(BookInvalidReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HealthDiagnostic {
    DuplicateObservation {
        stream: StreamId,
        raw: RecordNo,
    },
    AlreadyVerified {
        stream: StreamId,
        raw: RecordNo,
    },
    AlreadyApplied {
        stream: StreamId,
        raw: RecordNo,
    },
    PreBarrier {
        stream: StreamId,
        referenced: RecordNo,
        barrier: RecordNo,
    },
    ObsoleteScope {
        stream: StreamId,
    },
    ObsoleteConnection {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
    },
    DuplicateTransport {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        value: Transport,
    },
    WitnessMismatch {
        stream: StreamId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HealthEffect {
    StreamRegistered {
        stream: StreamId,
    },
    TransportChanged {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        value: Transport,
    },
    StreamInvalidated {
        stream: StreamId,
        barrier: RecordNo,
        reason: BookInvalidReason,
    },
    StreamEpochReset {
        stream: StreamId,
        barrier: RecordNo,
        tag: EpochTag,
    },
    FramePending {
        stream: StreamId,
        raw: RecordNo,
    },
    SnapshotReleased {
        stream: StreamId,
        raw: RecordNo,
    },
    UpdateReleased {
        stream: StreamId,
        raw: RecordNo,
        outputs: u32,
    },
    BookBecameUsable {
        stream: StreamId,
        witness: RecordNo,
    },
    FreshnessChanged {
        stream: StreamId,
        from: Freshness,
        to: Freshness,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinuityReport {
    pub stream: StreamId,
    pub outcome: ContinuityOutcome,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StepResult {
    pub effects: Vec<HealthEffect>,
    pub diagnostics: Vec<HealthDiagnostic>,
    pub continuity: Vec<ContinuityReport>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookFrameObservation {
    pub stream: StreamId,
    pub tag: EpochTag,
    pub raw_bytes: u32,
    pub candidate_outputs: u32,
    pub frame: Books50Frame,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HealthObservation {
    RegisterStream(StreamBinding),
    Transport {
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        value: Transport,
    },
    EpochAdvance(EpochChange),
    Gap {
        scope: GapScope,
        reason: Reason,
    },
    BookFrame(BookFrameObservation),
    /// The evidence has already passed the accepted artifact/body verification
    /// boundary. This reducer still enforces current scope, barrier, ordering,
    /// evidence kind and ordered pending release.
    VerifiedFrame(VerificationEvidence),
    /// Explicit post-verifier outcome for contradictory current-scope frame
    /// evidence. Old/pre-barrier scope is diagnosed before invalidation.
    ProofConflict {
        stream: StreamId,
        tag: EpochTag,
        raw: RecordNo,
    },
    /// The evidence has already passed the accepted artifact/body verification
    /// boundary. Parsed proof references alone must not be converted into this
    /// observation.
    VerifiedWarmup(WarmupEvidence),
    Timer {
        stream: StreamId,
    },
    Noop,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedHealthObservation {
    pub record: RecordNo,
    pub sample: MonotonicSample,
    pub value: HealthObservation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingSummary {
    pub frames: u32,
    pub raw_bytes: u64,
    pub outputs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamHealthSnapshot {
    pub binding: StreamBinding,
    pub transport: Transport,
    pub freshness: Freshness,
    pub book: Option<BookValidity>,
    pub barrier: RecordNo,
    pub pending: PendingSummary,
    pub anchor: Option<RecordNo>,
    pub progress: u32,
    pub witness: Option<RecordNo>,
    pub last_valid_sample_ns: Option<u64>,
    pub last_applied_raw: Option<RecordNo>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionTransportSnapshot {
    pub connection: ConnectionId,
    pub epoch: ConnectionEpoch,
    pub transport: Transport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataHealthSnapshot {
    pub last_record: Option<RecordNo>,
    pub evaluation_ns: u64,
    pub connections: Vec<ConnectionTransportSnapshot>,
    pub streams: Vec<StreamHealthSnapshot>,
}

#[derive(Clone, Debug)]
struct PendingFrame {
    raw: RecordNo,
    kind: BookEvidenceKind,
    original_sample_ns: u64,
    raw_bytes: u32,
    outputs: u32,
    deadline_ns: u64,
    verified: bool,
}

#[derive(Clone, Copy, Debug)]
struct Anchor {
    raw: RecordNo,
    original_sample_ns: u64,
}

#[derive(Clone, Copy, Debug)]
struct FrozenWitness {
    record: RecordNo,
    anchor: RecordNo,
    update_count: u32,
    elapsed_ns: u64,
}

#[derive(Clone, Debug)]
struct StreamRuntime {
    binding: StreamBinding,
    freshness: Freshness,
    book: Option<BookValidity>,
    barrier: RecordNo,
    pending: VecDeque<PendingFrame>,
    anchor: Option<Anchor>,
    progress: u32,
    witness: Option<FrozenWitness>,
    last_valid_sample_ns: Option<u64>,
    last_applied_raw: Option<RecordNo>,
    continuity: ContinuityClassifier,
}

impl StreamRuntime {
    fn clear_continuity(&mut self, at: RecordNo) {
        self.barrier = at;
        self.pending.clear();
        self.anchor = None;
        self.progress = 0;
        self.witness = None;
        self.last_valid_sample_ns = None;
        self.freshness = Freshness::Unknown;
        self.continuity.clear_for_new_generation();
    }

    fn invalidate(&mut self, at: RecordNo, reason: BookInvalidReason) {
        self.clear_continuity(at);
        if self.book.is_some() {
            self.book = Some(BookValidity::Invalid(reason));
        }
    }

    fn reset_epoch(&mut self, at: RecordNo) {
        self.clear_continuity(at);
        if self.book.is_some() {
            self.book = Some(BookValidity::NoSnapshot);
        }
    }

    fn pending_summary(&self) -> PendingSummary {
        PendingSummary {
            frames: u32::try_from(self.pending.len()).unwrap_or(u32::MAX),
            raw_bytes: self
                .pending
                .iter()
                .map(|frame| u64::from(frame.raw_bytes))
                .sum(),
            outputs: self
                .pending
                .iter()
                .map(|frame| u64::from(frame.outputs))
                .sum(),
        }
    }
}

/// Pure deterministic continuity-health reducer for one recorded clock scope.
///
/// The caller supplies recorded order and monotonic samples. The reducer owns no
/// live clock, connection, storage writer, canonical level quantities or price
/// levels. Frame/warm-up evidence observations are post-verifier semantic inputs;
/// artifact loading and authenticity remain outside this type.
#[derive(Clone, Debug)]
pub struct DataHealthReducer {
    clock: ClockScope,
    policy: HealthPolicy,
    last_record: Option<RecordNo>,
    evaluation_ns: u64,
    transport: BTreeMap<(ConnectionId, ConnectionEpoch), Transport>,
    streams: BTreeMap<StreamId, StreamRuntime>,
}

impl DataHealthReducer {
    pub fn new(
        clock: ClockScope,
        policy: HealthPolicy,
        mode: DurabilityMode,
    ) -> Result<Self, HealthError> {
        policy.validate(mode)?;
        Ok(Self {
            clock,
            policy,
            last_record: None,
            evaluation_ns: 0,
            transport: BTreeMap::new(),
            streams: BTreeMap::new(),
        })
    }

    pub const fn evaluation_ns(&self) -> u64 {
        self.evaluation_ns
    }

    pub const fn last_record(&self) -> Option<RecordNo> {
        self.last_record
    }

    pub fn transport_state(
        &self,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
    ) -> Transport {
        self.transport
            .get(&(connection, epoch))
            .copied()
            .unwrap_or(Transport::Unknown)
    }

    pub fn stream_state(&self, stream: StreamId) -> Option<StreamHealthSnapshot> {
        let state = self.streams.get(&stream)?;
        let transport =
            self.transport_state(state.binding.connection_id, state.binding.tag.connection);
        Some(StreamHealthSnapshot {
            binding: state.binding.clone(),
            transport,
            freshness: state.freshness,
            book: state.book.clone(),
            barrier: state.barrier,
            pending: state.pending_summary(),
            anchor: state.anchor.map(|anchor| anchor.raw),
            progress: state.progress,
            witness: state.witness.map(|witness| witness.record),
            last_valid_sample_ns: state.last_valid_sample_ns,
            last_applied_raw: state.last_applied_raw,
        })
    }

    pub fn usable_data(&self, stream: StreamId) -> bool {
        let Some(state) = self.streams.get(&stream) else {
            return false;
        };
        let Some(anchor) = state.anchor else {
            return false;
        };
        let Some(witness) = state.witness else {
            return false;
        };
        self.transport_state(state.binding.connection_id, state.binding.tag.connection)
            == Transport::Up
            && matches!(
                state.freshness,
                Freshness::Fresh | Freshness::QuietVerified
            )
            && state.book == Some(BookValidity::Usable)
            && anchor.raw > state.barrier
            && witness.anchor == anchor.raw
    }

    pub fn snapshot(&self) -> DataHealthSnapshot {
        let connections = self
            .transport
            .iter()
            .map(
                |(&(connection, epoch), &transport)| ConnectionTransportSnapshot {
                    connection,
                    epoch,
                    transport,
                },
            )
            .collect();
        let streams = self
            .streams
            .keys()
            .filter_map(|stream| self.stream_state(*stream))
            .collect();
        DataHealthSnapshot {
            last_record: self.last_record,
            evaluation_ns: self.evaluation_ns,
            connections,
            streams,
        }
    }

    pub fn step(
        &mut self,
        observation: RecordedHealthObservation,
    ) -> Result<StepResult, HealthError> {
        let mut trial = self.clone();
        let result = trial.step_inner(&observation)?;
        *self = trial;
        Ok(result)
    }

    fn step_inner(
        &mut self,
        observation: &RecordedHealthObservation,
    ) -> Result<StepResult, HealthError> {
        if let Some(last) = self.last_record {
            let expected = last.checked_next()?;
            if observation.record != expected {
                return Err(HealthError::RecordOrder {
                    expected,
                    actual: observation.record,
                });
            }
        }
        if observation.sample.scope != self.clock {
            return Err(HealthError::IncomparableClock);
        }

        let original_sample_ns = observation.sample.ns.get();
        self.evaluation_ns = self.evaluation_ns.max(original_sample_ns);
        let mut result = StepResult::default();
        self.expire_before(observation.record, &mut result)?;
        self.dispatch(
            observation.record,
            original_sample_ns,
            &observation.value,
            &mut result,
        )?;
        self.last_record = Some(observation.record);
        Ok(result)
    }

    fn expire_before(
        &mut self,
        at: RecordNo,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        let streams: Vec<_> = self.streams.keys().copied().collect();
        for stream in streams {
            let pending_expired = self
                .streams
                .get(&stream)
                .ok_or(HealthError::UnknownStream(stream))?
                .pending
                .iter()
                .any(|frame| self.evaluation_ns >= frame.deadline_ns);
            if pending_expired {
                self.invalidate_stream(stream, at, BookInvalidReason::PendingTimeout, out)?;
            } else {
                self.refresh_freshness(stream, out)?;
            }
        }
        Ok(())
    }

    fn dispatch(
        &mut self,
        at: RecordNo,
        original_sample_ns: u64,
        observation: &HealthObservation,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        match observation {
            HealthObservation::RegisterStream(binding) => {
                self.register_stream(at, binding.clone(), out)
            }
            HealthObservation::Transport {
                connection,
                epoch,
                value,
            } => self.transport_observation(at, *connection, *epoch, *value, out),
            HealthObservation::EpochAdvance(change) => self.advance_epoch(at, change, out),
            HealthObservation::Gap { scope, reason } => self.gap(at, scope, *reason, out),
            HealthObservation::BookFrame(frame) => {
                self.book_frame(at, original_sample_ns, frame, out)
            }
            HealthObservation::VerifiedFrame(evidence) => self.verify_frame(at, evidence, out),
            HealthObservation::ProofConflict { stream, tag, raw } => {
                self.proof_conflict(at, *stream, *tag, *raw, out)
            }
            HealthObservation::VerifiedWarmup(evidence) => self.warmup(at, evidence, out),
            HealthObservation::Timer { stream } => {
                if self.streams.contains_key(stream) {
                    Ok(())
                } else {
                    Err(HealthError::UnknownStream(*stream))
                }
            }
            HealthObservation::Noop => Ok(()),
        }
    }

    fn register_stream(
        &mut self,
        at: RecordNo,
        binding: StreamBinding,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        let existing: Vec<_> = self
            .streams
            .values()
            .map(|state| state.binding.clone())
            .collect();
        binding.validate_registration(&existing)?;
        let book = (binding.channel != Channel::Trades).then_some(BookValidity::NoSnapshot);
        self.transport
            .entry((binding.connection_id, binding.tag.connection))
            .or_insert(Transport::Unknown);
        let id = binding.id;
        self.streams.insert(
            id,
            StreamRuntime {
                binding,
                freshness: Freshness::Unknown,
                book,
                barrier: at,
                pending: VecDeque::new(),
                anchor: None,
                progress: 0,
                witness: None,
                last_valid_sample_ns: None,
                last_applied_raw: None,
                continuity: ContinuityClassifier::new(),
            },
        );
        out.effects
            .push(HealthEffect::StreamRegistered { stream: id });
        Ok(())
    }

    fn transport_observation(
        &mut self,
        at: RecordNo,
        connection: ConnectionId,
        epoch: ConnectionEpoch,
        value: Transport,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        let affected: Vec<_> = self
            .streams
            .values()
            .filter(|state| {
                state.binding.connection_id == connection && state.binding.tag.connection == epoch
            })
            .map(|state| state.binding.id)
            .collect();

        if affected.is_empty() {
            if self
                .streams
                .values()
                .any(|state| state.binding.connection_id == connection)
            {
                out.diagnostics.push(HealthDiagnostic::ObsoleteConnection {
                    connection,
                    epoch,
                });
                return Ok(());
            }
            return Err(HealthError::UnknownConnection(connection));
        }

        let previous = self.transport_state(connection, epoch);
        if previous == value {
            out.diagnostics.push(HealthDiagnostic::DuplicateTransport {
                connection,
                epoch,
                value,
            });
            return Ok(());
        }

        self.transport.insert((connection, epoch), value);
        out.effects.push(HealthEffect::TransportChanged {
            connection,
            epoch,
            value,
        });
        if value == Transport::Down {
            for stream in affected {
                self.invalidate_stream(stream, at, BookInvalidReason::TransportDown, out)?;
            }
        }
        Ok(())
    }

    fn advance_epoch(
        &mut self,
        at: RecordNo,
        change: &EpochChange,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        match *change {
            EpochChange::Connection {
                owner,
                expected,
                next,
            } => {
                let affected: Vec<_> = self
                    .streams
                    .values()
                    .filter(|state| state.binding.connection_id == owner)
                    .map(|state| state.binding.id)
                    .collect();
                if affected.is_empty() {
                    return Err(HealthError::UnknownConnection(owner));
                }
                for stream in &affected {
                    self.streams
                        .get(stream)
                        .ok_or(HealthError::UnknownStream(*stream))?
                        .binding
                        .tag
                        .connection
                        .advance(expected, next)?;
                }

                self.transport.remove(&(owner, expected));
                self.transport.insert((owner, next), Transport::Unknown);
                out.effects.push(HealthEffect::TransportChanged {
                    connection: owner,
                    epoch: next,
                    value: Transport::Unknown,
                });

                for stream in affected {
                    let state = self
                        .streams
                        .get_mut(&stream)
                        .ok_or(HealthError::UnknownStream(stream))?;
                    state.binding.tag.connection = next;
                    state.reset_epoch(at);
                    out.effects.push(HealthEffect::StreamEpochReset {
                        stream,
                        barrier: at,
                        tag: state.binding.tag,
                    });
                }
                Ok(())
            }
            EpochChange::Subscription {
                owner,
                expected,
                next,
            } => {
                let state = self
                    .streams
                    .get_mut(&owner)
                    .ok_or(HealthError::UnknownStream(owner))?;
                state.binding.tag.subscription.advance(expected, next)?;
                state.binding.tag.subscription = next;
                state.reset_epoch(at);
                out.effects.push(HealthEffect::StreamEpochReset {
                    stream: owner,
                    barrier: at,
                    tag: state.binding.tag,
                });
                Ok(())
            }
            EpochChange::Book {
                owner,
                expected,
                next,
            } => {
                let stream = self
                    .streams
                    .values()
                    .find(|state| state.binding.book_id == Some(owner))
                    .map(|state| state.binding.id)
                    .ok_or(HealthError::InvalidObservation("EpochAdvance.book_owner"))?;
                let state = self
                    .streams
                    .get_mut(&stream)
                    .ok_or(HealthError::UnknownStream(stream))?;
                let current = state
                    .binding
                    .tag
                    .book
                    .ok_or(HealthError::InvalidObservation("EpochAdvance.book_epoch"))?;
                current.advance(expected, next)?;
                state.binding.tag.book = Some(next);
                state.reset_epoch(at);
                out.effects.push(HealthEffect::StreamEpochReset {
                    stream,
                    barrier: at,
                    tag: state.binding.tag,
                });
                Ok(())
            }
        }
    }

    fn gap(
        &mut self,
        at: RecordNo,
        scope: &GapScope,
        reason: Reason,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        if reason == Reason::NoFault {
            return Err(HealthError::InvalidObservation("Gap.reason"));
        }
        let targets: Vec<_> = match scope {
            GapScope::AllDeclaredStreams => self
                .streams
                .values()
                .map(|state| (state.binding.id, state.binding.tag))
                .collect(),
            GapScope::ExplicitTargets(targets) => targets
                .iter()
                .map(|target| (target.stream, target.tag))
                .collect(),
        };

        for (stream, tag) in targets {
            let current = self
                .streams
                .get(&stream)
                .ok_or(HealthError::UnknownStream(stream))?
                .binding
                .tag;
            if tag != current {
                out.diagnostics
                    .push(HealthDiagnostic::ObsoleteScope { stream });
                continue;
            }
            self.invalidate_stream(stream, at, BookInvalidReason::Gap(reason), out)?;
        }
        Ok(())
    }

    fn book_frame(
        &mut self,
        at: RecordNo,
        original_sample_ns: u64,
        observation: &BookFrameObservation,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        if observation.raw_bytes == 0 {
            return Err(HealthError::InvalidObservation("BookFrame.raw_bytes"));
        }
        if observation.candidate_outputs == 0 {
            return Err(HealthError::InvalidObservation(
                "BookFrame.candidate_outputs",
            ));
        }

        let stream = observation.stream;
        let mut state = self
            .streams
            .remove(&stream)
            .ok_or(HealthError::UnknownStream(stream))?;

        if observation.tag != state.binding.tag {
            out.diagnostics
                .push(HealthDiagnostic::ObsoleteScope { stream });
            self.streams.insert(stream, state);
            return Ok(());
        }
        if state.binding.channel != Channel::BookNormal {
            return Err(HealthError::InvalidObservation("BookFrame.channel"));
        }
        if at <= state.barrier {
            out.diagnostics.push(HealthDiagnostic::PreBarrier {
                stream,
                referenced: at,
                barrier: state.barrier,
            });
            self.streams.insert(stream, state);
            return Ok(());
        }

        let outcome = state.continuity.observe(&observation.frame);
        out.continuity.push(ContinuityReport { stream, outcome });

        match outcome {
            ContinuityOutcome::AnchorCandidate { .. } => {
                self.admit_pending(
                    &mut state,
                    at,
                    original_sample_ns,
                    BookEvidenceKind::Snapshot,
                    observation,
                    out,
                )?;
            }
            ContinuityOutcome::Continuous { .. } => {
                self.admit_pending(
                    &mut state,
                    at,
                    original_sample_ns,
                    BookEvidenceKind::Delta,
                    observation,
                    out,
                )?;
            }
            ContinuityOutcome::DuplicateDiagnostic { .. } => {
                out.diagnostics
                    .push(HealthDiagnostic::DuplicateObservation { stream, raw: at });
            }
            ContinuityOutcome::Gap { .. } => {
                Self::invalidate_state(
                    &mut state,
                    at,
                    BookInvalidReason::ContinuityGap,
                    out,
                );
            }
            ContinuityOutcome::ResetOrDiscontinuity { .. } => {
                Self::invalidate_state(
                    &mut state,
                    at,
                    BookInvalidReason::ResetOrDiscontinuity,
                    out,
                );
            }
            ContinuityOutcome::SnapshotIntervalMismatch { .. } => {
                Self::invalidate_state(
                    &mut state,
                    at,
                    BookInvalidReason::SnapshotIntervalMismatch,
                    out,
                );
            }
            ContinuityOutcome::NeedsSnapshot { .. } => {
                Self::invalidate_state(&mut state, at, BookInvalidReason::NeedsSnapshot, out);
            }
            ContinuityOutcome::UnexpectedSnapshot { .. } => {
                Self::invalidate_state(
                    &mut state,
                    at,
                    BookInvalidReason::UnexpectedSnapshot,
                    out,
                );
            }
        }

        self.streams.insert(stream, state);
        Ok(())
    }

    fn admit_pending(
        &self,
        state: &mut StreamRuntime,
        at: RecordNo,
        original_sample_ns: u64,
        kind: BookEvidenceKind,
        observation: &BookFrameObservation,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        let frames = u64::try_from(state.pending.len())
            .map_err(|_| HealthError::InvalidObservation("pending.frames"))?
            .checked_add(1)
            .ok_or(HealthError::InvalidObservation("pending.frames"))?;
        if frames > u64::from(self.policy.pending_max_frames) {
            Self::invalidate_state(
                state,
                at,
                BookInvalidReason::PendingOverflow(PendingLimit::Frames),
                out,
            );
            return Ok(());
        }

        let current_bytes: u64 = state
            .pending
            .iter()
            .map(|frame| u64::from(frame.raw_bytes))
            .sum();
        let bytes = current_bytes
            .checked_add(u64::from(observation.raw_bytes))
            .ok_or(HealthError::InvalidObservation("pending.raw_bytes"))?;
        if bytes > self.policy.pending_max_raw_bytes {
            Self::invalidate_state(
                state,
                at,
                BookInvalidReason::PendingOverflow(PendingLimit::RawBytes),
                out,
            );
            return Ok(());
        }

        let current_outputs: u64 = state
            .pending
            .iter()
            .map(|frame| u64::from(frame.outputs))
            .sum();
        let outputs = current_outputs
            .checked_add(u64::from(observation.candidate_outputs))
            .ok_or(HealthError::InvalidObservation("pending.outputs"))?;
        if outputs > u64::from(self.policy.pending_max_outputs) {
            Self::invalidate_state(
                state,
                at,
                BookInvalidReason::PendingOverflow(PendingLimit::Outputs),
                out,
            );
            return Ok(());
        }

        let Some(deadline_ns) = original_sample_ns.checked_add(self.policy.pending_wait_ns) else {
            Self::invalidate_state(
                state,
                at,
                BookInvalidReason::PendingDeadlineOverflow,
                out,
            );
            return Ok(());
        };
        if self.evaluation_ns >= deadline_ns {
            Self::invalidate_state(state, at, BookInvalidReason::PendingTimeout, out);
            return Ok(());
        }

        state.pending.push_back(PendingFrame {
            raw: at,
            kind,
            original_sample_ns,
            raw_bytes: observation.raw_bytes,
            outputs: observation.candidate_outputs,
            deadline_ns,
            verified: false,
        });
        out.effects.push(HealthEffect::FramePending {
            stream: state.binding.id,
            raw: at,
        });
        Ok(())
    }

    fn verify_frame(
        &mut self,
        at: RecordNo,
        evidence: &VerificationEvidence,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        let stream = evidence.stream;
        let mut state = self
            .streams
            .remove(&stream)
            .ok_or(HealthError::UnknownStream(stream))?;

        if evidence.raw <= state.barrier {
            out.diagnostics.push(HealthDiagnostic::PreBarrier {
                stream,
                referenced: evidence.raw,
                barrier: state.barrier,
            });
            self.streams.insert(stream, state);
            return Ok(());
        }
        if evidence.tag != state.binding.tag || evidence.profile != state.binding.feed_profile {
            out.diagnostics
                .push(HealthDiagnostic::ObsoleteScope { stream });
            self.streams.insert(stream, state);
            return Ok(());
        }
        if state
            .last_applied_raw
            .is_some_and(|last| evidence.raw <= last)
        {
            out.diagnostics.push(HealthDiagnostic::AlreadyApplied {
                stream,
                raw: evidence.raw,
            });
            self.streams.insert(stream, state);
            return Ok(());
        }

        let Some(position) = state
            .pending
            .iter()
            .position(|pending| pending.raw == evidence.raw)
        else {
            Self::invalidate_state(&mut state, at, BookInvalidReason::ProofConflict, out);
            self.streams.insert(stream, state);
            return Ok(());
        };
        if state.pending[position].kind != evidence.kind {
            Self::invalidate_state(&mut state, at, BookInvalidReason::ProofConflict, out);
            self.streams.insert(stream, state);
            return Ok(());
        }
        if state.pending[position].verified {
            out.diagnostics.push(HealthDiagnostic::AlreadyVerified {
                stream,
                raw: evidence.raw,
            });
            self.streams.insert(stream, state);
            return Ok(());
        }

        state.pending[position].verified = true;
        self.release_ready(&mut state, at, out)?;
        self.streams.insert(stream, state);
        Ok(())
    }

    fn proof_conflict(
        &mut self,
        at: RecordNo,
        stream: StreamId,
        tag: EpochTag,
        raw: RecordNo,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        let state = self
            .streams
            .get(&stream)
            .ok_or(HealthError::UnknownStream(stream))?;
        if raw <= state.barrier {
            out.diagnostics.push(HealthDiagnostic::PreBarrier {
                stream,
                referenced: raw,
                barrier: state.barrier,
            });
            return Ok(());
        }
        if tag != state.binding.tag {
            out.diagnostics
                .push(HealthDiagnostic::ObsoleteScope { stream });
            return Ok(());
        }
        self.invalidate_stream(stream, at, BookInvalidReason::ProofConflict, out)
    }

    fn release_ready(
        &self,
        state: &mut StreamRuntime,
        at: RecordNo,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        while state.pending.front().is_some_and(|pending| pending.verified) {
            let pending = state
                .pending
                .pop_front()
                .ok_or(HealthError::InvalidObservation("pending.front"))?;
            match pending.kind {
                BookEvidenceKind::Snapshot => {
                    state.book = Some(BookValidity::Warming);
                    state.anchor = Some(Anchor {
                        raw: pending.raw,
                        original_sample_ns: pending.original_sample_ns,
                    });
                    state.progress = 0;
                    state.witness = None;
                    out.effects.push(HealthEffect::SnapshotReleased {
                        stream: state.binding.id,
                        raw: pending.raw,
                    });
                }
                BookEvidenceKind::Delta => {
                    if state.anchor.is_none()
                        || !matches!(
                            state.book,
                            Some(BookValidity::Warming | BookValidity::Usable)
                        )
                    {
                        Self::invalidate_state(
                            state,
                            at,
                            BookInvalidReason::NeedsSnapshot,
                            out,
                        );
                        break;
                    }
                    if state.book == Some(BookValidity::Warming) {
                        let threshold = self.policy.fields.warmup_min_updates.unwrap_or(0);
                        let next = u64::from(state.progress)
                            .checked_add(u64::from(pending.outputs))
                            .ok_or(HealthError::InvalidObservation("warmup.progress"))?;
                        let capped = next.min(u64::from(threshold));
                        state.progress = u32::try_from(capped)
                            .map_err(|_| HealthError::InvalidObservation("warmup.progress"))?;
                    }
                    out.effects.push(HealthEffect::UpdateReleased {
                        stream: state.binding.id,
                        raw: pending.raw,
                        outputs: pending.outputs,
                    });
                }
            }

            let sample = pending.original_sample_ns;
            state.last_valid_sample_ns = Some(
                state
                    .last_valid_sample_ns
                    .map_or(sample, |previous| previous.max(sample)),
            );
            state.last_applied_raw = Some(pending.raw);
            Self::refresh_freshness_state(state, self.evaluation_ns, self.policy, out);
        }
        Ok(())
    }

    fn warmup(
        &mut self,
        at: RecordNo,
        evidence: &WarmupEvidence,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        let stream = evidence.stream;
        let mut state = self
            .streams
            .remove(&stream)
            .ok_or(HealthError::UnknownStream(stream))?;

        if evidence.anchor <= state.barrier {
            out.diagnostics.push(HealthDiagnostic::PreBarrier {
                stream,
                referenced: evidence.anchor,
                barrier: state.barrier,
            });
            self.streams.insert(stream, state);
            return Ok(());
        }
        if evidence.tag != state.binding.tag {
            out.diagnostics
                .push(HealthDiagnostic::ObsoleteScope { stream });
            self.streams.insert(stream, state);
            return Ok(());
        }

        if let Some(witness) = state.witness {
            if witness.anchor == evidence.anchor
                && witness.update_count == evidence.update_count
                && witness.elapsed_ns == evidence.elapsed_ns
            {
                out.diagnostics.push(HealthDiagnostic::AlreadyApplied {
                    stream,
                    raw: evidence.anchor,
                });
            } else {
                out.diagnostics
                    .push(HealthDiagnostic::WitnessMismatch { stream });
            }
            self.streams.insert(stream, state);
            return Ok(());
        }

        let Some(anchor) = state.anchor else {
            out.diagnostics
                .push(HealthDiagnostic::WitnessMismatch { stream });
            self.streams.insert(stream, state);
            return Ok(());
        };
        let elapsed = self
            .evaluation_ns
            .checked_sub(anchor.original_sample_ns)
            .ok_or(HealthError::InvalidObservation("Warmup.elapsed_ns"))?;
        let valid = state.book == Some(BookValidity::Warming)
            && anchor.raw == evidence.anchor
            && state.progress == evidence.update_count
            && elapsed == evidence.elapsed_ns
            && self
                .policy
                .fields
                .warmup_min_updates
                .is_none_or(|minimum| state.progress >= minimum)
            && self
                .policy
                .fields
                .warmup_min_elapsed_ns
                .is_none_or(|minimum| elapsed >= minimum);

        if valid {
            state.book = Some(BookValidity::Usable);
            state.witness = Some(FrozenWitness {
                record: at,
                anchor: evidence.anchor,
                update_count: evidence.update_count,
                elapsed_ns: evidence.elapsed_ns,
            });
            out.effects.push(HealthEffect::BookBecameUsable {
                stream,
                witness: at,
            });
        } else {
            out.diagnostics
                .push(HealthDiagnostic::WitnessMismatch { stream });
        }
        self.streams.insert(stream, state);
        Ok(())
    }

    fn invalidate_stream(
        &mut self,
        stream: StreamId,
        at: RecordNo,
        reason: BookInvalidReason,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        let state = self
            .streams
            .get_mut(&stream)
            .ok_or(HealthError::UnknownStream(stream))?;
        Self::invalidate_state(state, at, reason, out);
        Ok(())
    }

    fn invalidate_state(
        state: &mut StreamRuntime,
        at: RecordNo,
        reason: BookInvalidReason,
        out: &mut StepResult,
    ) {
        state.invalidate(at, reason.clone());
        out.effects.push(HealthEffect::StreamInvalidated {
            stream: state.binding.id,
            barrier: at,
            reason,
        });
    }

    fn refresh_freshness(
        &mut self,
        stream: StreamId,
        out: &mut StepResult,
    ) -> Result<(), HealthError> {
        let state = self
            .streams
            .get_mut(&stream)
            .ok_or(HealthError::UnknownStream(stream))?;
        Self::refresh_freshness_state(state, self.evaluation_ns, self.policy, out);
        Ok(())
    }

    fn refresh_freshness_state(
        state: &mut StreamRuntime,
        evaluation_ns: u64,
        policy: HealthPolicy,
        out: &mut StepResult,
    ) {
        let next = ordinary_freshness(state.last_valid_sample_ns, evaluation_ns, policy);
        if next != state.freshness {
            let previous = state.freshness;
            state.freshness = next;
            out.effects.push(HealthEffect::FreshnessChanged {
                stream: state.binding.id,
                from: previous,
                to: next,
            });
        }
    }
}

fn ordinary_freshness(
    sample: Option<u64>,
    evaluation_ns: u64,
    policy: HealthPolicy,
) -> Freshness {
    let (Some(sample), Some(deadline)) = (sample, policy.fields.freshness_deadline_ns) else {
        return Freshness::Unknown;
    };
    let Some(expiry) = sample.checked_add(deadline) else {
        return Freshness::Unknown;
    };
    if evaluation_ns < sample {
        return Freshness::Unknown;
    }
    if evaluation_ns < expiry {
        return Freshness::Fresh;
    }
    match policy.fields.silence_rule {
        SilenceRule::UnknownOnSilence => Freshness::Unknown,
        SilenceRule::StaleAfterDeadline => Freshness::Stale,
    }
}
