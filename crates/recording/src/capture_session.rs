//! Exclusive, fresh, single-segment capture-session writer ownership.
//!
//! This boundary emits no publication permit and exports no appendable writer.
//! The session turn serializes sink use, Close dispatch and finalization.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::mem::size_of;
use std::path::Path;
use std::rc::Rc;

use domain::capture_session::*;
use domain::event::{ActiveContext, InputContext};
use domain::identity::{
    ConfigVersion, InstrumentRef, InstrumentSlot, RecordNo, SpecVersion, StreamBinding, StreamId,
};
use domain::policy::RecordingGate;
use domain::record::{
    ArchiveSeal, InputQuality, InstrumentSpecRecord, Record, RecordFrame, SegmentSeal,
};

use crate::{MAX_FRAME_LEN, StorageWatermarks, WalWriter, WriterError, encode_frame};

pub const MAX_CAPTURE_PATH_BYTES: usize = 4096;
pub const MAX_BOOTSTRAP_RECORDS: usize = 64;
pub const MAX_BOOTSTRAP_BYTES: usize = 65_536;

/// Borrowed definitions are validated once, persisted, then released. No
/// caller-owned Vec capacity or bootstrap history becomes retained ownership.
#[derive(Clone, Copy, Debug)]
pub struct BoundedCaptureProfile<'a> {
    pub bootstrap: &'a [RecordFrame],
    pub max_frame_len: usize,
}

impl<'a> BoundedCaptureProfile<'a> {
    pub const fn new(bootstrap: &'a [RecordFrame]) -> Self {
        Self {
            bootstrap,
            max_frame_len: MAX_FRAME_LEN,
        }
    }
}

#[derive(Debug)]
pub enum OwnerError {
    InvalidProfile(&'static str),
    Authority(AuthorityError),
    Writer(WriterError),
    Create(std::io::Error),
    CounterExhausted(&'static str),
}

impl fmt::Display for OwnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for OwnerError {}
impl From<AuthorityError> for OwnerError {
    fn from(error: AuthorityError) -> Self {
        Self::Authority(error)
    }
}
impl From<WriterError> for OwnerError {
    fn from(error: WriterError) -> Self {
        Self::Writer(error)
    }
}

/// Negative-only offline fault injection. A fault cannot fabricate a stronger
/// successful gate, skip validation, or change the owner binding. Mismatch and
/// weak-gate faults deliberately retain ambiguity after a real write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SinkFaultKind {
    BeforeWrite(PersistError),
    ReceiptMismatch { through: RecordNo },
    WeakGate { achieved: RecordingGate },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SinkFault {
    pub at: RecordNo,
    pub kind: SinkFaultKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnerMemoryReport {
    pub registry_record_count: usize,
    pub registry_encoded_bytes: usize,
    pub registry_metadata_bound: usize,
    /// Exact known owner/writer/Vec/Rc storage. Private standard-library BTree
    /// nodes are covered separately by registry_metadata_bound, not called measured.
    pub known_metadata_backing_bytes: usize,
    pub encoder_workspace_bound: usize,
    pub backend_allocated_bytes: usize,
    pub max_frame_len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticCloseState {
    Closing,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LivePhysicalReport {
    pub descriptor_closed: bool,
    pub confirmed_written: Option<RecordNo>,
    pub known_physical_bytes: u64,
    pub unconfirmed_suffix_possible: bool,
    /// No eager reader scan, and no runtime relabeling of physical bytes.
    pub recovered_input_quality: Option<InputQuality>,
}

#[derive(Debug)]
pub struct DiagnosticCloseReport {
    pub outcome: Result<DiagnosticCloseState, OwnerError>,
    pub input_completeness: InputQuality,
    pub physical_report: LivePhysicalReport,
    pub watermarks: StorageWatermarks,
    pub session: SessionStatus,
    pub outstanding_close_owners: CloseOwnerSnapshot,
    pub undrained_owners: UnsettledSummary,
    pub close_error: Option<PersistError>,
}

/// An owner-issued observation of completed, synced finalization. Its private
/// binding is not a reusable permission to publish or append to the archive.
pub struct FinalizedArchive {
    binding: SessionBinding,
    watermarks: StorageWatermarks,
    input_quality: InputQuality,
}
impl FinalizedArchive {
    pub const fn binding(&self) -> SessionBinding {
        self.binding
    }
    pub const fn watermarks(&self) -> StorageWatermarks {
        self.watermarks
    }
    pub const fn input_quality(&self) -> InputQuality {
        self.input_quality
    }
}
impl fmt::Debug for FinalizedArchive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FinalizedArchive")
            .field("binding", &self.binding)
            .field("watermarks", &self.watermarks)
            .field("input_quality", &self.input_quality)
            .finish()
    }
}

/// Exclusive writer ownership cannot be exported, and a callback cannot
/// reenter capture while its unique turn is borrowed by dispatch.
///
/// ```compile_fail
/// use recording::CaptureSessionOwner;
/// fn export(owner: CaptureSessionOwner) {
///     let _writer = owner.into_writer();
/// }
/// ```
///
/// ```compile_fail
/// use recording::CaptureSessionOwner;
/// use domain::capture_session::{CommandLease, SessionTurn, SupervisorSessionHandle, WorkKind};
/// fn reenter(owner: &mut CaptureSessionOwner, turn: &mut SessionTurn,
///            handle: &SupervisorSessionHandle, command: CommandLease) {
///     owner.dispatch(turn, command, |_| {
///         handle.reserve_work(turn, WorkKind::QueuedObservation).unwrap();
///         Ok::<_, ()>(())
///     });
/// }
/// ```
///
/// ```compile_fail
/// use domain::capture_session::QuiescenceProof;
/// fn duplicate(proof: QuiescenceProof) {
///     let _second = proof.clone();
/// }
/// ```
///
/// ```compile_fail
/// use recording::CaptureSessionOwner;
/// use domain::capture_session::{QuiescenceProof, SessionTurn};
/// fn reuse(owner: &mut CaptureSessionOwner, turn: &mut SessionTurn,
///          proof: QuiescenceProof) {
///     let mut transferred = proof;
///     owner.finalize(turn, &mut transferred).unwrap();
///     let mut duplicate = proof;
///     owner.finalize(turn, &mut duplicate).unwrap();
/// }
/// ```
pub struct CaptureSessionOwner {
    authority: CaptureSessionAuthority,
    writer: Rc<RefCell<WalWriter>>,
    prefix: PrefixBinding,
    accepted_scopes: Vec<ScopeBinding>,
    accepted_bindings: Vec<StreamBinding>,
    memory: OwnerMemoryReport,
    sink_fault: Rc<Cell<Option<SinkFault>>>,
    sink_persist_calls: Rc<Cell<u64>>,
    storage_memory_authority: Option<StorageMemoryAuthority>,
    diagnostic_close_error: Option<PersistError>,
}

impl CaptureSessionOwner {
    pub fn create_new(
        path: impl AsRef<Path>,
        accepted_start: &RecordFrame,
        profile: BoundedCaptureProfile<'_>,
    ) -> Result<(Self, SessionTurn), OwnerError> {
        let path = path.as_ref();
        let path_text = path
            .to_str()
            .ok_or(OwnerError::InvalidProfile("path must be UTF-8"))?;
        if path_text.len() > MAX_CAPTURE_PATH_BYTES
            || profile.bootstrap.len() > MAX_BOOTSTRAP_RECORDS
            || profile.max_frame_len < 36
            || profile.max_frame_len > MAX_FRAME_LEN
        {
            return Err(OwnerError::InvalidProfile(
                "bounded path/bootstrap/frame profile",
            ));
        }
        accepted_start
            .validate_shape()
            .map_err(|_| OwnerError::InvalidProfile("ArchiveStart shape"))?;
        let Record::ArchiveStart(start) = &accepted_start.value else {
            return Err(OwnerError::InvalidProfile("fresh ArchiveStart required"));
        };
        let binding = SessionBinding {
            archive: start.archive,
            session: start.session,
            clock: start.clock,
        };
        let mut context: Option<ActiveContext> = None;
        let mut gate = None;
        let mut accepted_scopes = Vec::with_capacity(4);
        let mut accepted_bindings = Vec::with_capacity(4);
        let mut encoded_bytes = encode_frame(accepted_start)
            .map_err(WriterError::from)?
            .len();
        let mut prior = accepted_start.record_no;
        for frame in profile.bootstrap {
            if frame.segment_no != accepted_start.segment_no
                || prior.checked_next().ok() != Some(frame.record_no)
                || frame
                    .value
                    .context()
                    .is_none_or(|context| context.context != InputContext::Bootstrap)
            {
                return Err(OwnerError::InvalidProfile("dense frozen bootstrap"));
            }
            match &frame.value {
                Record::InstrumentSpec(_) => {}
                Record::StreamDefinition(definition) => {
                    if accepted_scopes.len() == 4
                        || accepted_scopes
                            .iter()
                            .any(|scope: &ScopeBinding| scope.stream == definition.binding.id)
                    {
                        return Err(OwnerError::InvalidProfile(
                            "one to four unique stream definitions",
                        ));
                    }
                    accepted_scopes.push(scope_binding(&definition.binding));
                    accepted_bindings.push(definition.binding.clone());
                }
                Record::ConfigDefinition(definition) => {
                    if context.is_some() {
                        return Err(OwnerError::InvalidProfile("one frozen ConfigDefinition"));
                    }
                    context = Some(definition.next);
                    gate = Some(definition.fields.recording_gate);
                }
                _ => return Err(OwnerError::InvalidProfile("bootstrap definitions only")),
            }
            let encoded = encode_frame(frame).map_err(WriterError::from)?;
            if encoded.len() > profile.max_frame_len {
                return Err(OwnerError::InvalidProfile("bootstrap frame bound"));
            }
            encoded_bytes = encoded_bytes
                .checked_add(encoded.len())
                .filter(|bytes| *bytes <= MAX_BOOTSTRAP_BYTES)
                .ok_or(OwnerError::InvalidProfile("bootstrap byte bound"))?;
            prior = frame.record_no;
        }
        if accepted_scopes.is_empty() {
            return Err(OwnerError::InvalidProfile("stream definition required"));
        }
        let context = context.ok_or(OwnerError::InvalidProfile("ConfigDefinition required"))?;
        let recording_gate = gate.ok_or(OwnerError::InvalidProfile("recording gate required"))?;
        start
            .mode
            .validate_gate(recording_gate)
            .map_err(|_| OwnerError::InvalidProfile("accepted mode/gate"))?;
        let next_record = prior
            .checked_next()
            .map_err(|_| OwnerError::CounterExhausted("RecordNo"))?;
        let registry_metadata_bound = registry_allocation_ceiling(profile.bootstrap)?;
        let binding_text_bytes = accepted_bindings
            .iter()
            .try_fold(0usize, |total, binding| {
                let text = checked_budget_sum(&[
                    binding.spec.instrument.venue.as_str().len(),
                    binding.spec.instrument.product_namespace.as_str().len(),
                    binding.spec.instrument.native_symbol.as_str().len(),
                ])?;
                checked_budget_sum(&[total, text])
            })?;
        let mut memory = OwnerMemoryReport {
            registry_record_count: profile.bootstrap.len() + 1,
            registry_encoded_bytes: encoded_bytes,
            registry_metadata_bound,
            known_metadata_backing_bytes: checked_budget_sum(&[
                size_of::<Self>(),
                size_of::<RefCell<WalWriter>>(),
                checked_budget_product(2, size_of::<usize>())?,
                size_of::<Cell<Option<SinkFault>>>(),
                checked_budget_product(2, size_of::<usize>())?,
                size_of::<Cell<u64>>(),
                checked_budget_product(2, size_of::<usize>())?,
                checked_budget_product(accepted_scopes.capacity(), size_of::<ScopeBinding>())?,
                checked_budget_product(accepted_bindings.capacity(), size_of::<StreamBinding>())?,
                binding_text_bytes,
                size_of::<FileSink>(),
            ])?,
            // encode_frame has payload and frame Vecs; geometric growth is at
            // most twice each codec cap. Include input/decoder copies and the
            // entire cloned validator registry. A smaller advertised frame
            // limit still uses the codec's global workspace before rejection.
            encoder_workspace_bound: checked_budget_sum(&[
                checked_budget_product(MAX_FRAME_LEN, 8)?,
                registry_metadata_bound,
            ])?,
            backend_allocated_bytes: 0,
            max_frame_len: profile.max_frame_len,
        };
        storage_memory(memory)?.validate()?;
        let mut writer = WalWriter::create(path).map_err(OwnerError::Create)?;
        writer.append(accepted_start)?;
        for frame in profile.bootstrap {
            writer.append(frame)?;
        }
        // Always flush Written bootstrap/records. Flushed is a truthful stronger
        // receipt and makes diagnostic prefixes inspectable; only Durable syncs.
        if recording_gate == RecordingGate::Durable {
            writer.sync_all()?;
        } else {
            writer.flush()?;
        }
        let (authority, turn) = CaptureSessionAuthority::new(binding);
        writer.bind_capture_session(authority.clone());
        memory.backend_allocated_bytes = writer.backend_capacity();
        storage_memory(memory)?.validate()?;
        Ok((
            Self {
                authority,
                writer: Rc::new(RefCell::new(writer)),
                prefix: PrefixBinding {
                    context,
                    recording_gate,
                    segment: accepted_start.segment_no,
                    next_record,
                },
                accepted_scopes,
                accepted_bindings,
                memory,
                sink_fault: Rc::new(Cell::new(None)),
                sink_persist_calls: Rc::new(Cell::new(0)),
                storage_memory_authority: None,
                diagnostic_close_error: None,
            },
            turn,
        ))
    }

    pub fn path(&self) -> std::path::PathBuf {
        self.writer.borrow().path().to_path_buf()
    }
    pub fn session_status(&self) -> SessionStatus {
        self.authority.status()
    }
    pub fn watermarks(&self) -> StorageWatermarks {
        self.writer.borrow().watermarks()
    }
    pub fn memory_report(&self) -> OwnerMemoryReport {
        OwnerMemoryReport {
            backend_allocated_bytes: self.writer.borrow().backend_capacity(),
            ..self.memory
        }
    }

    /// Install one bounded negative fault on the existing sink. This cannot
    /// replace its writer, owner, prefix or gate, and a stopped sink stays stopped.
    pub fn set_sink_fault(
        &mut self,
        turn: &mut SessionTurn,
        fault: Option<SinkFault>,
    ) -> Result<(), OwnerError> {
        self.authority.validate_turn(turn)?;
        self.authority.ensure_storage_writable()?;
        self.sink_fault.set(fault);
        Ok(())
    }

    pub fn register_publication_guard(
        &mut self,
        turn: &mut SessionTurn,
        guard: OwnerBoundPublicationGuard,
    ) -> Result<(), OwnerError> {
        Ok(self.authority.register_publication_guard(turn, guard)?)
    }

    pub fn publish<E>(
        &mut self,
        turn: &mut SessionTurn,
        candidate: PublicationCandidate,
        fence: PublicationFence,
        consumer: impl FnOnce(PublicationView) -> Result<(), E>,
    ) -> PublicationReport<E> {
        self.authority.publish(turn, candidate, fence, consumer)
    }

    pub fn register_supervisor(
        &mut self,
        turn: &mut SessionTurn,
        bindings: &[ScopeBinding],
        budget: RetentionBudget,
        heartbeat_policy: HeartbeatPolicy,
    ) -> Result<(SupervisorSessionHandle, BoundRecordSink), OwnerError> {
        self.register_supervisor_with_policy_and_fault(
            turn,
            bindings,
            budget,
            heartbeat_policy,
            None,
        )
    }

    pub fn register_supervisor_with_fault(
        &mut self,
        turn: &mut SessionTurn,
        bindings: &[ScopeBinding],
        budget: RetentionBudget,
        fault: Option<SinkFault>,
    ) -> Result<(SupervisorSessionHandle, BoundRecordSink), OwnerError> {
        self.register_supervisor_with_policy_and_fault(
            turn,
            bindings,
            budget,
            HeartbeatPolicy::SupervisorV2,
            fault,
        )
    }

    fn register_supervisor_with_policy_and_fault(
        &mut self,
        turn: &mut SessionTurn,
        bindings: &[ScopeBinding],
        budget: RetentionBudget,
        heartbeat_policy: HeartbeatPolicy,
        fault: Option<SinkFault>,
    ) -> Result<(SupervisorSessionHandle, BoundRecordSink), OwnerError> {
        if bindings != self.accepted_scopes.as_slice() {
            return Err(OwnerError::InvalidProfile(
                "registry must match accepted stream definitions",
            ));
        }
        let handle = self.authority.register_supervisor(
            turn,
            bindings,
            budget,
            self.prefix,
            heartbeat_policy,
        )?;
        self.authority
            .set_accepted_stream_bindings(turn, &self.accepted_bindings)?;
        self.sink_fault.set(fault);
        let writer = Box::new(FileSink {
            writer: Rc::clone(&self.writer),
            max_frame_len: self.memory.max_frame_len,
            fault: Rc::clone(&self.sink_fault),
            persist_calls: Rc::clone(&self.sink_persist_calls),
            memory: self.memory,
        });
        let (sink, storage_memory_authority) = self
            .authority
            .bind_sink_with_memory_authority(turn, writer)?;
        self.storage_memory_authority = Some(storage_memory_authority);
        Ok((handle, sink))
    }

    pub fn outstanding_close_owners(&self) -> CloseOwnerSnapshot {
        self.authority.outstanding_close_owners()
    }

    /// Diagnostic count of concrete sink invocations, including rejected backend writes.
    pub fn sink_persist_calls(&self) -> u64 {
        self.sink_persist_calls.get()
    }
    pub fn reclaim_close(
        &mut self,
        turn: &mut SessionTurn,
        owner: CloseOwnerRef,
    ) -> CloseLeaseReport {
        self.authority.reclaim_close(turn, owner)
    }
    pub fn confirm_closed(
        &mut self,
        turn: &mut SessionTurn,
        owner: CloseOwnerRef,
        closure: AuthenticatedClosure,
    ) -> CloseSettlementReport {
        self.authority.confirm_closed(turn, owner, closure)
    }
    pub fn dispatch<E>(
        &mut self,
        turn: &mut SessionTurn,
        command: CommandLease,
        effect: impl FnOnce(CommandView<'_>) -> Result<(), E>,
    ) -> DispatchReport<E> {
        self.authority.dispatch(turn, command, effect)
    }
    pub fn begin_finalization(
        &mut self,
        turn: &mut SessionTurn,
    ) -> Result<CloseTicket, OwnerError> {
        Ok(self.authority.begin_finalization(turn)?)
    }

    /// Foreign owner/turn rejection preserves the caller's exact affine proof.
    /// Once validated, the proof is spent before any seal I/O and cannot be
    /// reused after a storage failure or successful finalization.
    pub fn finalize(
        &mut self,
        turn: &mut SessionTurn,
        proof: &mut QuiescenceProof,
    ) -> Result<FinalizedArchive, OwnerError> {
        self.authority.consume_proof(turn, proof)?;
        let result = self.finalize_writer();
        match result {
            Ok(watermarks) => {
                self.authority.finalization_finished(turn)?;
                Ok(FinalizedArchive {
                    binding: self.authority.binding(),
                    watermarks,
                    input_quality: InputQuality::Unknown,
                })
            }
            Err(error) => {
                self.authority
                    .storage_stopped(turn, persist_error(&error))?;
                Err(error)
            }
        }
    }

    fn finalize_writer(&mut self) -> Result<StorageWatermarks, OwnerError> {
        self.authority.ensure_finalization_authorized()?;
        let mut writer = self.writer.borrow_mut();
        writer.authorize_owner_seals()?;
        let summary = writer.prefix_summary();
        let prior = summary
            .prior_record
            .ok_or(OwnerError::InvalidProfile("empty accepted writer"))?;
        let segment_record = prior
            .checked_next()
            .map_err(|_| OwnerError::CounterExhausted("RecordNo"))?;
        let archive_record = segment_record
            .checked_next()
            .map_err(|_| OwnerError::CounterExhausted("RecordNo"))?;
        self.authority.ensure_finalization_authorized()?;
        writer.append(&RecordFrame {
            record_no: segment_record,
            segment_no: self.prefix.segment,
            value: Record::SegmentSeal(SegmentSeal {
                prefix_frame_count: summary.frame_count,
                prefix_physical_len: summary.physical_bytes,
                prefix_crc32: summary.crc32,
                prior_record: prior,
                has_gap: summary.has_gap,
                is_final: true,
            }),
        })?;
        let summary = writer.prefix_summary();
        self.authority.ensure_finalization_authorized()?;
        writer.append(&RecordFrame {
            record_no: archive_record,
            segment_no: self.prefix.segment,
            value: Record::ArchiveSeal(ArchiveSeal {
                expected_segment_count: 1,
                prior_frame_count: summary.frame_count,
                total_prefix_physical_bytes: summary.physical_bytes,
                prefix_crc32: summary.crc32,
                prior_record: segment_record,
                input_quality: InputQuality::Unknown,
            }),
        })?;
        self.authority.ensure_finalization_authorized()?;
        Ok(writer.finish()?)
    }

    pub fn close_diagnostic(&mut self, turn: &mut SessionTurn) -> DiagnosticCloseReport {
        let outcome = match self.authority.begin_diagnostic_close(turn) {
            Err(error) => Err(OwnerError::Authority(error)),
            Ok(SessionLifecycle::DiagnosticClosed) => Ok(DiagnosticCloseState::Closed),
            Ok(_) => {
                let unsettled = self.authority.unsettled_summary();
                if self.authority.ensure_storage_writable().is_ok()
                    && (unsettled.record_jobs != 0 || unsettled.marker == MarkerState::Pending)
                {
                    Ok(DiagnosticCloseState::Closing)
                } else {
                    if let Err(error) = self.writer.borrow_mut().close_diagnostic() {
                        let error = persist_error(&OwnerError::Writer(error));
                        self.diagnostic_close_error = Some(error);
                        // The authority preserves an earlier stop error. This
                        // additional closure failure remains a separate report.
                        let _ = self.authority.storage_stopped(turn, error);
                    }
                    match self.authority.diagnostic_closed(turn) {
                        Ok(()) => Ok(DiagnosticCloseState::Closed),
                        Err(error) => Err(OwnerError::Authority(error)),
                    }
                }
            }
        };
        if outcome.is_err() {
            return self.diagnostic_report(outcome);
        }
        // Descriptor closure releases its userspace buffer. Refresh actual
        // known backing even when no later sink receipt can occur.
        let profile = match storage_memory(self.memory_report()) {
            Ok(profile) => profile,
            Err(error) => return self.diagnostic_report(Err(error)),
        };
        let update = match self.storage_memory_authority.as_mut() {
            Some(authority) => authority.update(turn, profile),
            None => self.authority.set_storage_memory_profile(turn, profile),
        };
        if let Err(error) = update {
            return self.diagnostic_report(Err(OwnerError::Authority(error)));
        }
        self.diagnostic_report(outcome)
    }

    fn diagnostic_report(
        &self,
        outcome: Result<DiagnosticCloseState, OwnerError>,
    ) -> DiagnosticCloseReport {
        let writer = self.writer.borrow();
        let summary = writer.prefix_summary();
        DiagnosticCloseReport {
            outcome,
            input_completeness: InputQuality::Unknown,
            physical_report: LivePhysicalReport {
                descriptor_closed: writer.is_closed(),
                confirmed_written: writer.watermarks().written,
                known_physical_bytes: summary.physical_bytes,
                unconfirmed_suffix_possible: self.authority.status().storage_stopped.is_some()
                    || writer.is_poisoned(),
                recovered_input_quality: None,
            },
            watermarks: writer.watermarks(),
            session: self.authority.status(),
            outstanding_close_owners: self.authority.outstanding_close_owners(),
            undrained_owners: self.authority.unsettled_summary(),
            close_error: self.diagnostic_close_error,
        }
    }
}

fn scope_binding(binding: &StreamBinding) -> ScopeBinding {
    ScopeBinding {
        stream: binding.id,
        connection: binding.connection_id,
        epoch: binding.tag.connection,
    }
}

struct FileSink {
    writer: Rc<RefCell<WalWriter>>,
    max_frame_len: usize,
    fault: Rc<Cell<Option<SinkFault>>>,
    persist_calls: Rc<Cell<u64>>,
    memory: OwnerMemoryReport,
}
impl SessionRecordWriter for FileSink {
    fn checked_memory_profile(&self) -> Result<StorageMemoryProfile, AuthorityError> {
        let profile = storage_memory(OwnerMemoryReport {
            backend_allocated_bytes: self.writer.borrow().backend_capacity(),
            ..self.memory
        })
        .map_err(|_| AuthorityError::InvalidBudget)?;
        profile.validate()?;
        Ok(profile)
    }
    fn persist(
        &mut self,
        frame: &RecordFrame,
        required_gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistError> {
        let next_call = self
            .persist_calls
            .get()
            .checked_add(1)
            .ok_or(PersistError {
                kind: PersistErrorKind::Validation,
                detail: "sink invocation counter exhausted",
            })?;
        self.persist_calls.set(next_call);
        if let Some(SinkFault {
            at,
            kind: SinkFaultKind::BeforeWrite(error),
        }) = self.fault.get()
            && at == frame.record_no
        {
            return Err(error);
        }
        let bytes = encode_frame(frame)
            .map_err(|error| persist_error(&OwnerError::Writer(WriterError::Codec(error))))?;
        if bytes.len() > self.max_frame_len {
            return Err(PersistError {
                kind: PersistErrorKind::Validation,
                detail: "bounded frame length",
            });
        }
        drop(bytes);
        let mut writer = self.writer.borrow_mut();
        writer
            .append(frame)
            .map_err(|error| persist_error(&OwnerError::Writer(error)))?;
        let achieved_gate = if required_gate == RecordingGate::Durable {
            writer
                .sync_all()
                .map_err(|error| persist_error(&OwnerError::Writer(error)))?;
            RecordingGate::Durable
        } else {
            writer
                .flush()
                .map_err(|error| persist_error(&OwnerError::Writer(error)))?;
            RecordingGate::Flushed
        };
        let mut receipt = PersistenceReceipt {
            through: frame.record_no,
            achieved_gate,
        };
        if let Some(fault) = self.fault.get()
            && fault.at == frame.record_no
        {
            match fault.kind {
                SinkFaultKind::BeforeWrite(_) => unreachable!("handled before append"),
                SinkFaultKind::ReceiptMismatch { through } => receipt.through = through,
                SinkFaultKind::WeakGate { achieved } => {
                    if !achieved.covers(achieved_gate) {
                        receipt.achieved_gate = achieved;
                    }
                }
            }
        }
        Ok(receipt)
    }
}

fn persist_error(error: &OwnerError) -> PersistError {
    let (kind, detail) = match error {
        OwnerError::Writer(WriterError::Io { operation, .. }) => (PersistErrorKind::Io, *operation),
        OwnerError::Writer(WriterError::Codec(_)) => (PersistErrorKind::Validation, "WAL codec"),
        OwnerError::Writer(WriterError::Validation(_)) => {
            (PersistErrorKind::Validation, "WAL validation")
        }
        OwnerError::Writer(WriterError::Session(_)) | OwnerError::Authority(_) => {
            (PersistErrorKind::Authority, "capture authority")
        }
        OwnerError::CounterExhausted(counter) => (PersistErrorKind::Counter, *counter),
        OwnerError::Writer(WriterError::OffsetOverflow) => {
            (PersistErrorKind::Counter, "WAL offset")
        }
        _ => (PersistErrorKind::Io, "WAL storage boundary"),
    };
    PersistError { kind, detail }
}

fn checked_budget_sum(values: &[usize]) -> Result<usize, OwnerError> {
    values
        .iter()
        .try_fold(0usize, |sum, value| sum.checked_add(*value))
        .ok_or(OwnerError::InvalidProfile("unrepresentable memory budget"))
}

fn checked_budget_product(a: usize, b: usize) -> Result<usize, OwnerError> {
    a.checked_mul(b)
        .ok_or(OwnerError::InvalidProfile("unrepresentable memory budget"))
}

fn storage_memory(memory: OwnerMemoryReport) -> Result<StorageMemoryProfile, OwnerError> {
    let profile = StorageMemoryProfile {
        metadata_backing_bytes: memory.known_metadata_backing_bytes,
        metadata_ceiling_bytes: checked_budget_sum(&[
            memory.known_metadata_backing_bytes,
            memory.registry_metadata_bound,
        ])?,
        workspace_backing_bytes: 0,
        workspace_ceiling_bytes: memory.encoder_workspace_bound,
        backend_backing_bytes: memory.backend_allocated_bytes,
        backend_ceiling_bytes: checked_budget_sum(&[MAX_CAPTURE_PATH_BYTES, 8192])?,
    };
    profile.validate()?;
    Ok(profile)
}

/// Conservative allocation proof for the frozen validator on the pinned Rust
/// toolchain. It owns six BTree collections and one binding Vec: configs,
/// specs, slot->identity, identity->slot, active-spec, streams, bindings. A
/// node has at most 11 key/value pairs and 12 child edges; reserve 16 pairs,
/// 32 pointer words and 64 alignment bytes per node. At most 2*n+1 nodes for
/// n entries overbounds even an empty root and every internal/leaf split.
/// This deliberately reports a ceiling, not private-node allocation as measured.
fn registry_allocation_ceiling(bootstrap: &[RecordFrame]) -> Result<usize, OwnerError> {
    fn tree(entries: usize, entry_size: usize) -> Result<usize, OwnerError> {
        if entries == 0 {
            return Ok(0);
        }
        let nodes = checked_budget_sum(&[checked_budget_product(2, entries)?, 1])?;
        let bytes = checked_budget_sum(&[
            checked_budget_product(16, entry_size)?,
            checked_budget_product(32, size_of::<usize>())?,
            64,
        ])?;
        checked_budget_product(nodes, bytes)
    }
    let specs = bootstrap
        .iter()
        .filter(|frame| matches!(frame.value, Record::InstrumentSpec(_)))
        .count();
    let streams = bootstrap
        .iter()
        .filter(|frame| matches!(frame.value, Record::StreamDefinition(_)))
        .count();
    let configs = bootstrap
        .iter()
        .filter(|frame| matches!(frame.value, Record::ConfigDefinition(_)))
        .count();
    checked_budget_sum(&[
        tree(configs, size_of::<ConfigVersion>())?,
        tree(
            specs,
            size_of::<((InstrumentSlot, SpecVersion), InstrumentSpecRecord)>(),
        )?,
        tree(specs, size_of::<(InstrumentSlot, InstrumentRef)>())?,
        tree(specs, size_of::<(InstrumentRef, InstrumentSlot)>())?,
        tree(specs, size_of::<(InstrumentSlot, SpecVersion)>())?,
        // StreamState is StreamBinding plus fixed LossState (two u64s,
        // EpochTag and Options); 128 bytes covers that non-allocating state.
        tree(
            streams,
            checked_budget_sum(&[size_of::<(StreamId, StreamBinding)>(), 128])?,
        )?,
        // All cloned Token capacities equal their string lengths. Numeric
        // spec has 256 token bytes; each identity-map copy has <=128 bytes.
        checked_budget_product(specs, checked_budget_sum(&[256, 128, 128])?)?,
        checked_budget_product(streams, checked_budget_product(2, 128)?)?,
        checked_budget_product(streams.max(4), size_of::<StreamBinding>())?,
    ])
}

#[cfg(test)]
mod storage_memory_budget_conformance {
    use super::*;

    #[test]
    fn storage_memory_component_intermediate_and_aggregate_overflow_is_typed() {
        let original = OwnerMemoryReport {
            registry_record_count: 1,
            registry_encoded_bytes: 1,
            registry_metadata_bound: 7,
            known_metadata_backing_bytes: 3,
            encoder_workspace_bound: 11,
            backend_allocated_bytes: 19,
            max_frame_len: MAX_FRAME_LEN,
        };
        assert!(storage_memory(original).is_ok());
        for invalid in [
            OwnerMemoryReport {
                registry_metadata_bound: usize::MAX,
                ..original
            },
            OwnerMemoryReport {
                known_metadata_backing_bytes: usize::MAX,
                ..original
            },
            OwnerMemoryReport {
                encoder_workspace_bound: usize::MAX,
                ..original
            },
            OwnerMemoryReport {
                backend_allocated_bytes: usize::MAX,
                ..original
            },
            OwnerMemoryReport {
                known_metadata_backing_bytes: usize::MAX / 2,
                registry_metadata_bound: 0,
                encoder_workspace_bound: usize::MAX / 2,
                ..original
            },
        ] {
            assert!(storage_memory(invalid).is_err());
        }
        assert!(checked_budget_product(usize::MAX, 2).is_err());
        assert!(checked_budget_sum(&[usize::MAX, 1]).is_err());
        assert_eq!(storage_memory(original).unwrap().metadata_ceiling_bytes, 10);
    }
}

#[cfg(test)]
mod finalization_tests {
    use super::*;
    use crate::{ArchiveStatus, WalReader};
    use domain::artifact::ArtifactRef;
    use domain::event::{ActiveContext, InputContext};
    use domain::identity::*;
    use domain::numeric::ExactDecimal;
    use domain::policy::{DurabilityMode, PolicyFields, SilenceRule};
    use domain::qualified::{NumericSpec, NumericSpecFields, PriceUnits};
    use domain::record::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    fn positive<T>(result: Result<T, IdentityError>) -> T {
        result.expect("valid test identity")
    }

    fn record(value: u64) -> RecordNo {
        positive(RecordNo::new(value))
    }

    fn active() -> ActiveContext {
        ActiveContext {
            config: positive(ConfigVersion::new(1)),
            normalizer: positive(NormalizerVersion::new(1)),
        }
    }

    fn context(number: u64, bootstrap: bool) -> WireContext {
        WireContext {
            unix_ns: LocalUnixNs::new(number as i64),
            monotonic_ns: MonotonicNs::new(number),
            context: if bootstrap {
                InputContext::Bootstrap
            } else {
                InputContext::Active(active())
            },
        }
    }

    fn frame(number: u64, value: Record) -> RecordFrame {
        RecordFrame {
            record_no: record(number),
            segment_no: SegmentNo::new(0),
            value,
        }
    }

    fn start() -> RecordFrame {
        frame(
            1,
            Record::ArchiveStart(ArchiveStart {
                archive: positive(ArchiveId::new([1; 16])),
                session: positive(CaptureSessionId::new([2; 16])),
                clock: positive(ClockId::new(1)),
                mode: DurabilityMode::Buffered,
                previous_archive: None,
            }),
        )
    }

    fn provenance() -> ArtifactRef {
        "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .expect("valid artifact")
    }

    fn binding() -> StreamBinding {
        let spec = positive(SpecVersion::new(1));
        StreamBinding {
            id: positive(StreamId::new(1)),
            instrument_slot: positive(InstrumentSlot::new(1)),
            spec: SpecRef {
                instrument: InstrumentRef {
                    venue: positive(Token::new("bitget")),
                    market: MarketKind::Perpetual,
                    product_namespace: positive(Token::new("usdt-futures")),
                    native_symbol: positive(Token::new("BTCUSDT")),
                },
                version: spec,
            },
            connection_id: positive(ConnectionId::new(1)),
            channel: Channel::BookNormal,
            book_id: Some(positive(BookId::new(1))),
            tag: EpochTag {
                spec,
                connection: positive(ConnectionEpoch::new(1)),
                subscription: positive(SubscriptionEpoch::new(1)),
                book: Some(positive(BookEpoch::new(1))),
            },
            feed_profile: positive(FeedProfileVersion::new(1)),
        }
    }

    fn bootstrap() -> Vec<RecordFrame> {
        let stream = binding();
        vec![
            frame(
                2,
                Record::InstrumentSpec(InstrumentSpecRecord {
                    context: context(2, true),
                    slot: stream.instrument_slot,
                    numeric: NumericSpec::new(NumericSpecFields {
                        reference: stream.spec.clone(),
                        price_units: PriceUnits {
                            quote: positive(Token::new("USDT")),
                            basis: positive(Token::new("BASE")),
                        },
                        quantity_unit: positive(Token::new("BTC")),
                        base_asset: positive(Token::new("BTC")),
                        price_increment: ExactDecimal::ONE,
                        quantity_increment: ExactDecimal::ONE,
                        quantity_to_base_multiplier: Some(ExactDecimal::ONE),
                    })
                    .expect("numeric spec"),
                    provenance: provenance(),
                }),
            ),
            frame(
                3,
                Record::StreamDefinition(StreamDefinition {
                    context: context(3, true),
                    binding: stream,
                    provenance: provenance(),
                }),
            ),
            frame(
                4,
                Record::ConfigDefinition(ConfigDefinition {
                    context: context(4, true),
                    next: active(),
                    provenance_kind: ProvenanceKind::Synthetic,
                    evidence: provenance(),
                    fields: PolicyFields {
                        silence_rule: SilenceRule::UnknownOnSilence,
                        freshness_deadline_ns: Some(100),
                        warmup_min_updates: Some(1),
                        warmup_min_elapsed_ns: Some(0),
                        allow_quiet_with_proof: false,
                        require_two_sided_snapshot: true,
                        recording_gate: RecordingGate::Written,
                    },
                }),
            ),
        ]
    }

    fn budget() -> RetentionBudget {
        RetentionBudget {
            item_cap: 5,
            raw_frame_limit: 1,
            raw_byte_limit: 1024,
            max_message_bytes: 1024,
        }
    }

    fn scope() -> ScopeBinding {
        let binding = binding();
        ScopeBinding {
            stream: binding.id,
            connection: binding.connection_id,
            epoch: binding.tag.connection,
        }
    }

    #[test]
    fn validated_proof_stays_spent_after_real_backend_closure_failure() {
        let serial = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "proscalping-proof-storage-error-{}-{serial}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let definitions = bootstrap();
        let (mut owner, mut turn) = CaptureSessionOwner::create_new(
            &path,
            &start(),
            BoundedCaptureProfile::new(&definitions),
        )
        .unwrap();
        let (handle, _sink) = owner
            .register_supervisor(
                &mut turn,
                &[scope()],
                budget(),
                HeartbeatPolicy::SupervisorV2,
            )
            .unwrap();
        let ticket = owner.begin_finalization(&mut turn).unwrap();
        let QuiescenceReport::Ready(mut proof) = handle.quiesce(&mut turn, &ticket) else {
            panic!("healthy owner must issue its one proof")
        };
        // Only a private conformance fixture can close the concrete descriptor
        // without changing the authority. The public owner exposes no writer.
        owner.writer.borrow_mut().close_diagnostic().unwrap();
        let before_bytes = fs::read(&path).unwrap();
        let before_watermarks = owner.watermarks();
        assert!(matches!(
            owner.finalize(&mut turn, &mut proof),
            Err(OwnerError::Writer(WriterError::Closed))
        ));
        assert!(owner.session_status().failed);
        assert!(owner.session_status().storage_stopped.is_some());
        assert_eq!(owner.watermarks(), before_watermarks);
        assert_eq!(fs::read(&path).unwrap(), before_bytes);
        // The same object remains available, but the validated consumption is
        // irreversible. No replacement proof or seal retry becomes possible.
        assert!(matches!(
            owner.finalize(&mut turn, &mut proof),
            Err(OwnerError::Authority(AuthorityError::ArchiveFailed))
        ));
        assert!(matches!(
            handle.quiesce(&mut turn, &ticket),
            QuiescenceReport::FinalizationInvalidated(AuthorityError::ArchiveFailed)
        ));
        assert!(matches!(
            owner.authority.consume_proof(&mut turn, &mut proof),
            Err(AuthorityError::ArchiveFailed)
        ));
        assert_eq!(fs::read(&path).unwrap(), before_bytes);
        drop(owner);
        let mut reader = WalReader::open(&path).unwrap();
        while let Some(frame) = reader.next_record().unwrap() {
            assert!(!matches!(
                frame.value,
                Record::SegmentSeal(_) | Record::ArchiveSeal(_)
            ));
        }
        assert_eq!(reader.report().status, ArchiveStatus::ValidPrefixIncomplete);
        assert_eq!(reader.report().input_quality, None);
        drop(reader);
        fs::remove_file(path).unwrap();
    }
}
