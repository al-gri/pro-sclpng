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
///     owner.finalize(turn, proof).unwrap();
///     owner.finalize(turn, proof).unwrap();
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
        let memory = OwnerMemoryReport {
            registry_record_count: profile.bootstrap.len() + 1,
            registry_encoded_bytes: encoded_bytes,
            registry_metadata_bound: registry_allocation_ceiling(profile.bootstrap),
            known_metadata_backing_bytes: size_of::<Self>()
                + size_of::<RefCell<WalWriter>>()
                + 2 * size_of::<usize>()
                + size_of::<Cell<Option<SinkFault>>>()
                + 2 * size_of::<usize>()
                + accepted_scopes.capacity() * size_of::<ScopeBinding>()
                + accepted_bindings.capacity() * size_of::<StreamBinding>()
                + accepted_bindings
                    .iter()
                    .map(|binding| {
                        binding.spec.instrument.venue.as_str().len()
                            + binding.spec.instrument.product_namespace.as_str().len()
                            + binding.spec.instrument.native_symbol.as_str().len()
                    })
                    .sum::<usize>()
                + size_of::<FileSink>(),
            // encode_frame has payload and frame Vecs; geometric growth is at
            // most twice each codec cap. Include input/decoder copies and the
            // entire cloned validator registry. A smaller advertised frame
            // limit still uses the codec's global workspace before rejection.
            encoder_workspace_bound: MAX_FRAME_LEN * 8
                + registry_allocation_ceiling(profile.bootstrap),
            backend_allocated_bytes: writer.backend_capacity(),
            max_frame_len: profile.max_frame_len,
        };
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
    ) -> Result<(SupervisorSessionHandle, BoundRecordSink), OwnerError> {
        self.register_supervisor_with_fault(turn, bindings, budget, None)
    }

    pub fn register_supervisor_with_fault(
        &mut self,
        turn: &mut SessionTurn,
        bindings: &[ScopeBinding],
        budget: RetentionBudget,
        fault: Option<SinkFault>,
    ) -> Result<(SupervisorSessionHandle, BoundRecordSink), OwnerError> {
        if bindings != self.accepted_scopes.as_slice() {
            return Err(OwnerError::InvalidProfile(
                "registry must match accepted stream definitions",
            ));
        }
        let handle = self
            .authority
            .register_supervisor(turn, bindings, budget, self.prefix)?;
        self.authority
            .set_accepted_stream_bindings(turn, &self.accepted_bindings)?;
        self.authority
            .set_storage_memory_profile(turn, storage_memory(self.memory))?;
        self.sink_fault.set(fault);
        let writer = Box::new(FileSink {
            writer: Rc::clone(&self.writer),
            max_frame_len: self.memory.max_frame_len,
            fault: Rc::clone(&self.sink_fault),
            memory: self.memory,
        });
        let sink = self.authority.bind_sink(turn, writer)?;
        Ok((handle, sink))
    }

    pub fn outstanding_close_owners(&self) -> CloseOwnerSnapshot {
        self.authority.outstanding_close_owners()
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

    pub fn finalize(
        &mut self,
        turn: &mut SessionTurn,
        proof: QuiescenceProof,
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
                    && (unsettled.queued + unsettled.in_flight != 0
                        || unsettled.marker == MarkerState::Pending)
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
        if let Err(error) = self
            .authority
            .set_storage_memory_profile(turn, storage_memory(self.memory_report()))
        {
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
    memory: OwnerMemoryReport,
}
impl SessionRecordWriter for FileSink {
    fn memory_profile(&self) -> StorageMemoryProfile {
        storage_memory(OwnerMemoryReport {
            backend_allocated_bytes: self.writer.borrow().backend_capacity(),
            ..self.memory
        })
    }
    fn persist(
        &mut self,
        frame: &RecordFrame,
        required_gate: RecordingGate,
    ) -> Result<PersistenceReceipt, PersistError> {
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

fn storage_memory(memory: OwnerMemoryReport) -> StorageMemoryProfile {
    StorageMemoryProfile {
        metadata_backing_bytes: memory.known_metadata_backing_bytes,
        metadata_ceiling_bytes: memory.known_metadata_backing_bytes
            + memory.registry_metadata_bound,
        workspace_backing_bytes: 0,
        workspace_ceiling_bytes: memory.encoder_workspace_bound,
        backend_backing_bytes: memory.backend_allocated_bytes,
        backend_ceiling_bytes: MAX_CAPTURE_PATH_BYTES + 8192,
    }
}

/// Conservative allocation proof for the frozen validator on the pinned Rust
/// toolchain. It owns six BTree collections and one binding Vec: configs,
/// specs, slot->identity, identity->slot, active-spec, streams, bindings. A
/// node has at most 11 key/value pairs and 12 child edges; reserve 16 pairs,
/// 32 pointer words and 64 alignment bytes per node. At most 2*n+1 nodes for
/// n entries overbounds even an empty root and every internal/leaf split.
/// This deliberately reports a ceiling, not private-node allocation as measured.
fn registry_allocation_ceiling(bootstrap: &[RecordFrame]) -> usize {
    fn tree(entries: usize, entry_size: usize) -> usize {
        if entries == 0 {
            return 0;
        }
        (2 * entries + 1) * (16 * entry_size + 32 * size_of::<usize>() + 64)
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
    tree(configs, size_of::<ConfigVersion>())
        + tree(specs, size_of::<((InstrumentSlot, SpecVersion), InstrumentSpecRecord)>())
        + tree(specs, size_of::<(InstrumentSlot, InstrumentRef)>())
        + tree(specs, size_of::<(InstrumentRef, InstrumentSlot)>())
        + tree(specs, size_of::<(InstrumentSlot, SpecVersion)>())
        // StreamState is StreamBinding plus fixed LossState (two u64s,
        // EpochTag and Options); 128 bytes covers that non-allocating state.
        + tree(streams, size_of::<(StreamId, StreamBinding)>() + 128)
        // All cloned Token capacities equal their string lengths. Numeric
        // spec has 256 token bytes; each identity-map copy has <=128 bytes.
        + specs * (256 + 128 + 128)
        + streams * 2 * 128
        + streams.max(4) * size_of::<StreamBinding>()
}
