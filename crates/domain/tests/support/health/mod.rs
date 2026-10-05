//! Pure recorded-input DataHealth reference model, compiled only in tests.
//! It holds no order-book levels, clock service, runtime queue or publisher.
//! Caller-owned history and explicitly synthetic artifact/storage relations
//! allow deterministic assertions without claiming real feed or I/O guarantees.

mod application;
mod evidence;
mod transitions;

use std::collections::BTreeMap;

use domain::artifact::{ArtifactError, ArtifactRef};
use domain::event::*;
use domain::identity::*;
use domain::policy::{HealthPolicy, PolicyError, SilenceRule};
use domain::record::*;

use super::accounting::{LossError, LossState};
use super::bodies::*;
use super::model_env::{AcceptedProof, ModelEnv};
use super::publication::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelError {
    Record(RecordError),
    Event(EventError),
    Identity(IdentityError),
    Policy(PolicyError),
    Artifact(ArtifactError),
    Loss(LossError),
    Receipt(ReceiptError),
    UnknownDefinition,
    AlreadyBlocked,
}

macro_rules! error_from {
    ($source:ty, $variant:ident) => {
        impl From<$source> for ModelError {
            fn from(value: $source) -> Self {
                Self::$variant(value)
            }
        }
    };
}

error_from!(RecordError, Record);
error_from!(EventError, Event);
error_from!(IdentityError, Identity);
error_from!(PolicyError, Policy);
error_from!(ArtifactError, Artifact);
error_from!(LossError, Loss);
error_from!(ReceiptError, Receipt);

pub type Result<T> = std::result::Result<T, ModelError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Fault {
    Gap(Reason),
    TransportDown,
    ContextChanged,
    NeedsSnapshot,
    ProofConflict,
    ProofExpired,
    MixedFrameUnsupported,
    Structural(EventError),
    PendingOverflow(&'static str),
    PendingTimeout,
    PendingDeadlineOverflow,
    SnapshotNotTwoSided,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BookValidity {
    NoSnapshot,
    Warming,
    Usable,
    Invalid(Fault),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiagnosticCode {
    PreBarrier,
    ObsoleteScope,
    AlreadyApplied,
    AlreadyVerified,
    WitnessMismatch,
    Fault(Fault),
    FreshnessDeadlineOverflow,
    IncomparableClock,
    MissingFreshnessBasis,
    FreshnessAssertionMismatch,
    InvalidQuietBounds,
    QuietPolicyDenied,
    QuietNotYetValid,
    QuietExpired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    pub record: RecordRef,
    pub stream: StreamId,
    pub code: DiagnosticCode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingFrame {
    pub raw: RawFrameId,
    pub raw_bytes: u32,
    pub outputs: u32,
    pub deadline_ns: u64,
    pub output_sha256: [u8; 32],
    pub post_barrier_membership: bool,
    pub proof: Option<AcceptedProof>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Anchor {
    pub raw: RawFrameId,
    pub original_sample_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenWitness {
    pub record: RecordNo,
    pub reference: ArtifactRef,
    pub body: WarmupBody,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuietWitness {
    pub body: FreshnessBody,
    pub expires_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamState {
    pub binding: StreamBinding,
    pub profile_ref: ArtifactRef,
    pub barrier: u64,
    pub barrier_evaluation_ns: u64,
    pub freshness: Freshness,
    pub book: Option<BookValidity>,
    pub pending: Vec<PendingFrame>,
    pub anchor: Option<Anchor>,
    pub progress: u32,
    pub witness: Option<FrozenWitness>,
    pub last_valid_sample_ns: Option<u64>,
    pub last_applied_raw: Option<RecordNo>,
    pub last_event_cursor: Option<EventCursor>,
    pub last_batch_effects: Vec<EventId>,
    pub quiet: Option<QuietWitness>,
    pub loss: LossState,
    pub candidate: Option<PublicationCandidate>,
}

impl StreamState {
    pub fn revoke(&mut self) {
        if let Some(candidate) = &mut self.candidate {
            candidate.revoked = true;
        }
    }

    fn clear_continuity(&mut self, at: RecordNo, evaluation: u64) {
        self.barrier = at.get();
        self.barrier_evaluation_ns = evaluation;
        self.pending.clear();
        self.anchor = None;
        self.progress = 0;
        self.witness = None;
        self.last_valid_sample_ns = None;
        self.freshness = Freshness::Unknown;
        self.quiet = None;
        self.last_batch_effects.clear();
        self.revoke();
        // Historical cursors/frontiers are retained for identity/audit. The new
        // barrier makes all pre-barrier inputs inapplicable without replaying them.
    }

    pub fn invalidate(&mut self, fault: Fault, at: RecordNo, evaluation: u64) {
        self.clear_continuity(at, evaluation);
        if self.book.is_some() {
            self.book = Some(BookValidity::Invalid(fault));
        }
    }

    pub fn new_epoch(&mut self, at: RecordNo, evaluation: u64) {
        self.clear_continuity(at, evaluation);
        if self.book.is_some() {
            self.book = Some(BookValidity::NoSnapshot);
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StepResult {
    pub effects: Vec<EventEnvelope>,
    pub diagnostics: Vec<Diagnostic>,
    pub candidates_created: Vec<CandidateId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthModel {
    pub start: ArchiveStart,
    pub last_record: RecordNo,
    pub evaluation_ns: u64,
    pub timeline: ContextTimeline,
    pub config: Option<(ArtifactRef, ConfigBody)>,
    pub active_specs: BTreeMap<InstrumentSlot, SpecVersion>,
    pub streams: BTreeMap<StreamId, StreamState>,
    pub transport: BTreeMap<(ConnectionId, ConnectionEpoch), Transport>,
    pub recording: RecordingSnapshot,
    pub blocked: Option<ModelError>,
}

impl HealthModel {
    pub fn new(start: ArchiveStart, env: &mut ModelEnv) -> Result<Self> {
        let frame = RecordFrame {
            record_no: RecordNo::new(1)?,
            segment_no: SegmentNo::new(0),
            value: Record::ArchiveStart(start.clone()),
        };
        frame.validate_shape()?;
        env.accept(start.archive, &frame, &[]);
        Ok(Self {
            start,
            last_record: frame.record_no,
            evaluation_ns: 0,
            timeline: ContextTimeline::default(),
            config: None,
            active_specs: BTreeMap::new(),
            streams: BTreeMap::new(),
            transport: BTreeMap::new(),
            recording: RecordingSnapshot {
                health: RecordingHealth::Unknown,
                reason: Reason::Unknown,
                watermarks: Watermarks::default(),
                last_receipt: None,
            },
            blocked: None,
        })
    }

    pub fn clock(&self) -> ClockScope {
        ClockScope {
            session: self.start.session,
            clock: self.start.clock,
        }
    }

    pub fn policy(&self) -> Result<HealthPolicy> {
        self.config
            .as_ref()
            .map(|(_, body)| body.policy)
            .ok_or(ModelError::UnknownDefinition)
    }

    pub fn context(&self) -> Result<ActiveContext> {
        self.timeline.active().ok_or(ModelError::UnknownDefinition)
    }

    pub fn scope(&self, stream: &StreamState) -> Result<EvidenceScope> {
        Ok(EvidenceScope {
            archive: self.start.archive,
            clock: self.clock(),
            stream: stream.binding.id,
            slot: stream.binding.instrument_slot,
            tag: stream.binding.tag,
            context: self.context()?,
            profile: stream.binding.feed_profile,
            barrier: stream.barrier,
        })
    }

    pub fn transport_for(&self, stream: &StreamState) -> Transport {
        self.transport
            .get(&(stream.binding.connection_id, stream.binding.tag.connection))
            .copied()
            .unwrap_or(Transport::Unknown)
    }

    pub fn usable_data(&self, id: StreamId) -> bool {
        let Some(stream) = self.streams.get(&id) else {
            return false;
        };
        let Ok(policy) = self.policy() else {
            return false;
        };
        let Ok(scope) = self.scope(stream) else {
            return false;
        };
        if self.blocked.is_some() || self.transport_for(stream) != Transport::Up {
            return false;
        }
        let freshness_ok = stream.freshness == Freshness::Fresh
            || (stream.freshness == Freshness::QuietVerified
                && policy.fields.allow_quiet_with_proof
                && stream.quiet.as_ref().is_some_and(|quiet| {
                    quiet.body.scope == scope && self.evaluation_ns < quiet.expires_ns
                }));
        let (Some(anchor), Some(witness)) = (&stream.anchor, &stream.witness) else {
            return false;
        };
        freshness_ok
            && stream.book == Some(BookValidity::Usable)
            && anchor.raw.record.get() > stream.barrier
            && witness.body.scope == scope
            && witness.body.anchor == anchor.raw.record
    }

    /// Transactional semantic input. External immutable test history is cloned
    /// with the trial state, so a failed input cannot leave partial registration,
    /// application, clock advancement or artifact-identity binding behind.
    pub fn step(&mut self, input: &RecordFrame, env: &mut ModelEnv) -> Result<StepResult> {
        if self.blocked.is_some() {
            return Err(ModelError::AlreadyBlocked);
        }
        let mut trial = self.clone();
        let mut history = env.clone();
        match trial.step_inner(input, &mut history) {
            Ok(result) => {
                *self = trial;
                *env = history;
                Ok(result)
            }
            Err(error) => {
                self.blocked = Some(error.clone());
                Err(error)
            }
        }
    }

    fn step_inner(&mut self, input: &RecordFrame, env: &mut ModelEnv) -> Result<StepResult> {
        if input.record_no != self.last_record.checked_next()? {
            return Err(EventError::RecordOrderError.into());
        }
        input.validate_shape()?;
        if let Some(context) = input.value.context() {
            self.timeline.check(input.record_no, context.context)?;
        }
        self.preflight(input, env)?;
        let mut result = StepResult::default();
        if let Some(context) = input.value.context() {
            self.evaluation_ns = self.evaluation_ns.max(context.monotonic_ns.get());
            self.expire(input.record_no, &mut result)?;
        }
        self.dispatch(input, env, &mut result)?;
        self.last_record = input.record_no;
        env.accept(self.start.archive, input, &result.effects);
        self.update_candidates(input.record_no, &mut result)?;
        validate_dense_outputs(self.at(input.record_no), &result.effects)?;
        Ok(result)
    }

    fn at(&self, record: RecordNo) -> RecordRef {
        RecordRef {
            archive: self.start.archive,
            record,
        }
    }

    fn diagnostic(
        &self,
        at: RecordNo,
        stream: StreamId,
        code: DiagnosticCode,
        out: &mut StepResult,
    ) {
        out.diagnostics.push(Diagnostic {
            record: self.at(at),
            stream,
            code,
        });
    }

    fn fault(&self, state: &mut StreamState, at: RecordNo, fault: Fault, out: &mut StepResult) {
        state.invalidate(fault.clone(), at, self.evaluation_ns);
        self.diagnostic(at, state.binding.id, DiagnosticCode::Fault(fault), out);
    }

    fn expire(&mut self, at: RecordNo, out: &mut StepResult) -> Result<()> {
        let Some((_, config)) = &self.config else {
            return Ok(());
        };
        let policy = config.policy;
        let ids: Vec<_> = self.streams.keys().copied().collect();
        for id in ids {
            let mut state = self
                .streams
                .remove(&id)
                .ok_or(ModelError::UnknownDefinition)?;
            if state
                .pending
                .iter()
                .any(|pending| self.evaluation_ns >= pending.deadline_ns)
            {
                self.fault(&mut state, at, Fault::PendingTimeout, out);
            } else {
                self.refresh_freshness(&mut state, policy, at, out);
            }
            self.streams.insert(id, state);
        }
        Ok(())
    }

    fn refresh_freshness(
        &self,
        state: &mut StreamState,
        policy: HealthPolicy,
        at: RecordNo,
        out: &mut StepResult,
    ) {
        if let Some(quiet) = &state.quiet
            && self.evaluation_ns < quiet.expires_ns
        {
            state.freshness = Freshness::QuietVerified;
            return;
        }
        state.quiet = None;
        let (value, diagnostic) =
            ordinary_freshness(state.last_valid_sample_ns, self.evaluation_ns, policy);
        state.freshness = value;
        if let Some(code) = diagnostic {
            self.diagnostic(at, state.binding.id, code, out);
        }
    }

    fn update_candidates(&mut self, at: RecordNo, out: &mut StepResult) -> Result<()> {
        let Some((_, config)) = &self.config else {
            return Ok(());
        };
        let policy = config.policy;
        let ids: Vec<_> = self.streams.keys().copied().collect();
        for id in ids {
            let eligible =
                self.usable_data(id) && self.recording.health == RecordingHealth::Healthy;
            let mut stream = self
                .streams
                .remove(&id)
                .ok_or(ModelError::UnknownDefinition)?;
            if eligible {
                let anchor = stream
                    .anchor
                    .as_ref()
                    .ok_or(ModelError::UnknownDefinition)?;
                let witness = stream
                    .witness
                    .as_ref()
                    .ok_or(ModelError::UnknownDefinition)?;
                let projection = CandidateProjection {
                    scope: self.scope(&stream)?,
                    gate: policy.fields.recording_gate,
                    anchor: anchor.raw.record,
                    witness_record: witness.record,
                    ordered_effects: stream.last_batch_effects.clone(),
                    freshness: stream.freshness,
                    recording: self.recording.clone(),
                    evaluation_ns: self.evaluation_ns,
                };
                let changed = stream
                    .candidate
                    .as_ref()
                    .is_none_or(|previous| previous.revoked || previous.projection != projection);
                if changed {
                    let candidate = PublicationCandidate::freeze(self.at(at), projection);
                    out.candidates_created.push(candidate.id);
                    stream.candidate = Some(candidate);
                }
            } else {
                stream.revoke();
            }
            self.streams.insert(id, stream);
        }
        Ok(())
    }

    pub fn permit(
        &self,
        candidate: &PublicationCandidate,
        completion: Option<&SyntheticCompletion>,
    ) -> std::result::Result<(), PermitError> {
        let stream = self
            .streams
            .get(&candidate.id.stream)
            .ok_or(PermitError::CandidateRevoked)?;
        let scope = self
            .scope(stream)
            .map_err(|_| PermitError::CanonicalBlocked)?;
        publication_permit(
            candidate,
            &PermitState {
                canonical_running: self.blocked.is_none(),
                usable_data: self.usable_data(candidate.id.stream),
                recording: self.recording.health,
                mode: self.start.mode,
                scope: &scope,
                current_candidate: stream.candidate.as_ref(),
            },
            completion,
        )
    }
}

pub fn ordinary_freshness(
    sample: Option<u64>,
    evaluation: u64,
    policy: HealthPolicy,
) -> (Freshness, Option<DiagnosticCode>) {
    let (Some(sample), Some(deadline)) = (sample, policy.fields.freshness_deadline_ns) else {
        return (Freshness::Unknown, None);
    };
    let Some(expiry) = sample.checked_add(deadline) else {
        return (
            Freshness::Unknown,
            Some(DiagnosticCode::FreshnessDeadlineOverflow),
        );
    };
    if evaluation < sample {
        return (Freshness::Unknown, None);
    }
    if evaluation < expiry {
        return (Freshness::Fresh, None);
    }
    let value = match policy.fields.silence_rule {
        SilenceRule::UnknownOnSilence => Freshness::Unknown,
        SilenceRule::StaleAfterDeadline => Freshness::Stale,
    };
    (value, None)
}
